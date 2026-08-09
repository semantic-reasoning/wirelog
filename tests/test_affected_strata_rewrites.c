/*
 * test_affected_strata_rewrites.c - Affected-strata detection across plan
 * rewrites (Issue #1019)
 *
 * rewrite_multiway_delta() collapses a recursive relation plan to a single
 * K_FUSION op and rewrite_lftj_chains() collapses an EDB-only join chain to a
 * single LFTJ op.  Both move the operators that name the relations being read
 * into wl_plan_op_t.opaque_data, where col_compute_affected_strata()'s
 * operator scan cannot see them.  The stratum then looks like it reads
 * nothing: incremental inserts derive no tuples and, because
 * col_session_step() intersects the removal mask into UINT64_MAX, retracted
 * tuples are retained.
 *
 * Every case here builds a REAL plan through wl_plan_from_program() and
 * asserts its own premise -- that the blinding operator it targets is
 * actually present -- before asserting the mask, so the case cannot pass
 * vacuously if a future rewrite changes shape.
 *
 * Coverage note: because every plan here comes from wl_plan_from_program(),
 * every stratum carries a recorded wl_plan_stratum_t.rule_refs, so this file
 * exercises only the refset arm of rule_references_relation().  The
 * operator-scan fallback (rule_refs == NULL) is covered by the hand-built
 * plans in tests/test_affected_strata.c, tests/test_affected_rules.c and
 * tests/test_rule_level_frontier.c, which memset their strata on the stack
 * and never call wl_plan_from_program().  Both arms need to keep passing.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/cli/driver.h"
#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-easy.h"
#include "../wirelog/wirelog.h"

#include "test_tmpdir.h"

/* ======================================================================== */
/* TEST HARNESS MACROS                                                       */
/* ======================================================================== */

#define TEST(name)                       \
        do {                                 \
            printf("  [TEST] %-62s ", name); \
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

static int tests_passed = 0;
static int tests_failed = 0;

/* ======================================================================== */
/* DATALOG PROGRAMS                                                          */
/* ======================================================================== */

/* Two IDB body atoms in one rule -> rewrite_multiway_delta fuses. */
static const char *const SRC_FUSED_TC =
    ".decl Edge(x: int64, y: int64)\n"
    ".decl Path(x: int64, y: int64)\n"
    ".output Path\n"
    "Path(x,y) :- Edge(x,y).\n"
    "Path(x,y) :- Path(x,z), Path(z,y).\n";

/* Three rules for P, each with at most ONE IDB body atom, still fuse:
 * count_delta_positions() counts delta positions across the whole relation
 * plan (the union of every rule for the head), not per rule. */
static const char *const SRC_CROSS_RULE_FUSION =
    ".decl E(x: int64, y: int64)\n"
    ".decl P(x: int64, y: int64)\n"
    ".decl Q(x: int64, y: int64)\n"
    ".decl S(x: int64, y: int64)\n"
    ".output P\n"
    "P(x,y) :- E(x,y).\n"
    "P(x,y) :- Q(x,z), E(z,y).\n"
    "P(x,y) :- S(x,z), E(z,y).\n"
    "Q(x,y) :- P(x,y).\n"
    "S(x,y) :- P(x,y).\n";

/* Non-recursive star join.  Without the optimizer passes this lowers to a
 * three-way EDB chain and rewrite_lftj_chains() collapses it to LFTJ. */
static const char *const SRC_STAR_JOIN =
    ".decl A(x: int64, y: int64)\n"
    ".decl B(x: int64, z: int64)\n"
    ".decl C(x: int64, w: int64)\n"
    ".decl R(x: int64)\n"
    ".output R\n"
    "R(x) :- A(x,y), B(x,z), C(x,w).\n";

/* One IDB body atom per rule and only one rule that recurses: no fusion. */
static const char *const SRC_UNFUSED_TC =
    ".decl Edge(x: int64, y: int64)\n"
    ".decl Path(x: int64, y: int64)\n"
    ".output Path\n"
    "Path(x,y) :- Edge(x,y).\n"
    "Path(x,y) :- Path(x,z), Edge(z,y).\n";

/* Fused stratum 0, a stratum that depends on it, and a wholly independent
 * stratum that must stay unmarked. */
static const char *const SRC_FUSED_PLUS_INDEPENDENT =
    ".decl Edge(x: int64, y: int64)\n"
    ".decl Path(x: int64, y: int64)\n"
    ".decl Reach(x: int64, y: int64)\n"
    ".decl Other(x: int64, y: int64)\n"
    ".decl Ind(x: int64, y: int64)\n"
    ".output Reach\n"
    ".output Ind\n"
    "Path(x,y) :- Edge(x,y).\n"
    "Path(x,y) :- Path(x,z), Path(z,y).\n"
    "Reach(x,y) :- Path(x,y).\n"
    "Ind(x,y) :- Other(x,y).\n";

/* Edge -> Path (fused) -> Reach -> Far: propagation has to cross the fused
 * stratum twice. */
static const char *const SRC_FUSED_CHAIN =
    ".decl Edge(x: int64, y: int64)\n"
    ".decl Path(x: int64, y: int64)\n"
    ".decl Reach(x: int64, y: int64)\n"
    ".decl Far(x: int64, y: int64)\n"
    ".output Far\n"
    "Path(x,y) :- Edge(x,y).\n"
    "Path(x,y) :- Path(x,z), Path(z,y).\n"
    "Reach(x,y) :- Path(x,y).\n"
    "Far(x,y) :- Reach(x,y).\n";

/* ======================================================================== */
/* PLAN HELPERS                                                              */
/* ======================================================================== */

/*
 * build_plan - Lower @src to a wl_plan_t.
 *
 * @optimize selects which optimizer passes run, and that choice is what
 * decides K_FUSION versus LFTJ, so every caller states it explicitly:
 *   true  - fusion + JPP + SIP, the shape wirelog_optimize() produces.
 *   false - no passes at all, the mode wirelog-advanced.h documents for
 *           hosts that call wirelog_session_create() directly.
 * The K-fusion rewrite runs in wl_plan_from_program() either way.
 */
static wl_plan_t *
build_plan(const char *src, bool optimize)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    if (optimize) {
        wl_fusion_apply(prog, NULL);
        wl_jpp_apply(prog, NULL);
        wl_sip_apply(prog, NULL);
    }

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    return (rc == 0) ? plan : NULL;
}

/*
 * find_stratum_of - Index of the stratum producing relation @name, or
 * UINT32_MAX.
 */
static uint32_t
find_stratum_of(const wl_plan_t *plan, const char *name)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ri++) {
            const char *rn = plan->strata[si].relations[ri].name;
            if (rn && strcmp(rn, name) == 0)
                return si;
        }
    }
    return UINT32_MAX;
}

/*
 * relation_leading_op - Operator kind of the first op of relation @name, or
 * -1 when the relation is absent or has no operators.  Returns int rather
 * than wl_plan_op_type_t because the enum has no "none" member.
 */
static int
relation_leading_op(const wl_plan_t *plan, const char *name)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ri++) {
            const wl_plan_relation_t *pr = &plan->strata[si].relations[ri];
            if (!pr->name || strcmp(pr->name, name) != 0)
                continue;
            if (pr->op_count == 0 || !pr->ops)
                return -1;
            return (int)pr->ops[0].op;
        }
    }
    return -1;
}

/*
 * strata_mask - col_compute_affected_strata() over @plan for @rel.
 * Returns 0 and sets *ok = false when the session harness itself fails.
 */
static uint64_t
strata_mask(wl_plan_t *plan, const char *rel, bool *ok)
{
    wl_session_t *sess = NULL;
    *ok = false;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0 || !sess)
        return 0;
    uint64_t mask = col_compute_affected_strata(sess, rel);
    wl_session_destroy(sess);
    *ok = true;
    return mask;
}

/*
 * rules_mask - col_compute_affected_rules() over @plan for @rel.
 */
static uint64_t
rules_mask(wl_plan_t *plan, const char *rel, bool *ok)
{
    wl_session_t *sess = NULL;
    *ok = false;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0 || !sess)
        return 0;
    uint64_t mask = col_compute_affected_rules(sess, rel);
    wl_session_destroy(sess);
    *ok = true;
    return mask;
}

/* ======================================================================== */
/* TC1: a fused two-IDB-atom rule still marks its stratum                     */
/* ======================================================================== */

static void
test_fused_rule_marks_stratum(void)
{
    TEST("K-fusion: two IDB body atoms still mark the stratum");

    wl_plan_t *plan = build_plan(SRC_FUSED_TC, false);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t si = find_stratum_of(plan, "Path");
    if (si == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("no stratum produces Path");
        return;
    }

    /* Premise: the blinding op this case targets is really there. */
    if (relation_leading_op(plan, "Path") != WL_PLAN_OP_K_FUSION) {
        wl_plan_free(plan);
        FAIL("premise: Path was not rewritten to K_FUSION");
        return;
    }

    bool ok = false;
    uint64_t mask = strata_mask(plan, "Edge", &ok);
    wl_plan_free(plan);
    if (!ok) {
        FAIL("wl_session_create failed");
        return;
    }

    if ((mask & ((uint64_t)1 << si)) != 0) {
        PASS;
    } else {
        char buf[96];
        snprintf(buf, sizeof(buf),
            "Edge must mark stratum %u; mask=0x%" PRIx64, si, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TC2: cross-rule fusion (each rule has at most one IDB atom)                */
/* ======================================================================== */

static void
test_cross_rule_fusion_marks_stratum(void)
{
    TEST("K-fusion: three rules with <=1 IDB atom each still fuse");

    wl_plan_t *plan = build_plan(SRC_CROSS_RULE_FUSION, false);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t si = find_stratum_of(plan, "P");
    if (si == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("no stratum produces P");
        return;
    }

    if (relation_leading_op(plan, "P") != WL_PLAN_OP_K_FUSION) {
        wl_plan_free(plan);
        FAIL("premise: P was not rewritten to K_FUSION");
        return;
    }

    bool ok = false;
    uint64_t mask = strata_mask(plan, "E", &ok);
    wl_plan_free(plan);
    if (!ok) {
        FAIL("wl_session_create failed");
        return;
    }

    if ((mask & ((uint64_t)1 << si)) != 0) {
        PASS;
    } else {
        char buf[96];
        snprintf(buf, sizeof(buf), "E must mark stratum %u; mask=0x%" PRIx64,
            si, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TC3: LFTJ star join, no optimizer passes                                  */
/* ======================================================================== */

static void
test_lftj_star_join_marks_stratum(void)
{
    TEST("LFTJ: non-recursive star join without wirelog_optimize");

    /* Pin the pass dependency: with SIP the chain becomes four ops --
     * VARIABLE, JOIN, SEMIJOIN, JOIN -- which no longer matches
     * lftj_chain_len()'s all-JOIN run, so nothing is hidden and this case
     * only means anything on the un-optimized plan. */
    wl_plan_t *opt = build_plan(SRC_STAR_JOIN, true);
    if (!opt) {
        FAIL("build_plan (optimized) failed");
        return;
    }
    int opt_lead = relation_leading_op(opt, "R");
    wl_plan_free(opt);
    if (opt_lead == WL_PLAN_OP_LFTJ) {
        FAIL("premise: optimized plan also uses LFTJ; case is mis-scoped");
        return;
    }

    wl_plan_t *plan = build_plan(SRC_STAR_JOIN, false);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t si = find_stratum_of(plan, "R");
    if (si == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("no stratum produces R");
        return;
    }

    if (relation_leading_op(plan, "R") != WL_PLAN_OP_LFTJ) {
        wl_plan_free(plan);
        FAIL("premise: R was not rewritten to LFTJ");
        return;
    }

    const char *edbs[] = { "A", "B", "C" };
    for (uint32_t i = 0; i < 3; i++) {
        bool ok = false;
        uint64_t mask = strata_mask(plan, edbs[i], &ok);
        if (!ok) {
            wl_plan_free(plan);
            FAIL("wl_session_create failed");
            return;
        }
        if ((mask & ((uint64_t)1 << si)) == 0) {
            char buf[96];
            snprintf(buf, sizeof(buf),
                "%s must mark stratum %u; mask=0x%" PRIx64, edbs[i], si, mask);
            wl_plan_free(plan);
            FAIL(buf);
            return;
        }
    }

    wl_plan_free(plan);
    PASS;
}

/* ======================================================================== */
/* TC4: unfused control -- the op scan alone already answers                 */
/* ======================================================================== */

/*
 * This case is a control, not fault-detecting coverage: no mutation of the
 * #1019 fix kills it, because the pre-#1019 operator scan already answered it
 * correctly and the new refset arm answers it the same way.  It earns its
 * place by asserting the INVERTED premise -- that the leading op is neither
 * K_FUSION nor LFTJ -- so if a future rewrite starts collapsing this shape the
 * case fails loudly instead of quietly turning into a duplicate of TC1/TC3.
 */
static void
test_unfused_control(void)
{
    TEST("control: unfused recursion is marked and has no blinding op");

    wl_plan_t *plan = build_plan(SRC_UNFUSED_TC, false);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t si = find_stratum_of(plan, "Path");
    if (si == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("no stratum produces Path");
        return;
    }

    /* Premise, inverted: this plan must NOT be rewritten, otherwise the case
     * is not a control at all. */
    int lead = relation_leading_op(plan, "Path");
    if (lead == WL_PLAN_OP_K_FUSION || lead == WL_PLAN_OP_LFTJ) {
        wl_plan_free(plan);
        FAIL("premise: control plan was rewritten after all");
        return;
    }

    bool ok = false;
    uint64_t mask = strata_mask(plan, "Edge", &ok);
    wl_plan_free(plan);
    if (!ok) {
        FAIL("wl_session_create failed");
        return;
    }

    if ((mask & ((uint64_t)1 << si)) != 0) {
        PASS;
    } else {
        char buf[96];
        snprintf(buf, sizeof(buf),
            "Edge must mark stratum %u; mask=0x%" PRIx64, si, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TC5: precision -- an independent stratum stays unmarked                   */
/* ======================================================================== */

static void
test_independent_stratum_stays_clear(void)
{
    TEST("precision: independent stratum is not marked by a fused insert");

    wl_plan_t *plan = build_plan(SRC_FUSED_PLUS_INDEPENDENT, false);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t si_path = find_stratum_of(plan, "Path");
    uint32_t si_reach = find_stratum_of(plan, "Reach");
    uint32_t si_ind = find_stratum_of(plan, "Ind");
    if (si_path == UINT32_MAX || si_reach == UINT32_MAX
        || si_ind == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("expected separate strata for Path, Reach and Ind");
        return;
    }
    if (si_path == si_ind || si_reach == si_ind) {
        wl_plan_free(plan);
        FAIL("premise: Ind shares a stratum, so precision is untestable");
        return;
    }
    if (relation_leading_op(plan, "Path") != WL_PLAN_OP_K_FUSION) {
        wl_plan_free(plan);
        FAIL("premise: Path was not rewritten to K_FUSION");
        return;
    }

    bool ok = false;
    uint64_t mask = strata_mask(plan, "Edge", &ok);
    wl_plan_free(plan);
    if (!ok) {
        FAIL("wl_session_create failed");
        return;
    }

    if ((mask & ((uint64_t)1 << si_path)) == 0) {
        FAIL("Path stratum not marked");
        return;
    }
    if ((mask & ((uint64_t)1 << si_reach)) == 0) {
        FAIL("Reach stratum not marked");
        return;
    }
    if ((mask & ((uint64_t)1 << si_ind)) != 0) {
        char buf[96];
        snprintf(buf, sizeof(buf),
            "Ind stratum %u must stay clear; mask=0x%" PRIx64, si_ind, mask);
        FAIL(buf);
        return;
    }
    PASS;
}

/* ======================================================================== */
/* TC6: transitive propagation through a fused stratum                       */
/* ======================================================================== */

static void
test_transitive_propagation_through_fusion(void)
{
    TEST("transitive: Edge -> Path(fused) -> Reach -> Far");

    wl_plan_t *plan = build_plan(SRC_FUSED_CHAIN, false);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    if (relation_leading_op(plan, "Path") != WL_PLAN_OP_K_FUSION) {
        wl_plan_free(plan);
        FAIL("premise: Path was not rewritten to K_FUSION");
        return;
    }

    const char *heads[] = { "Path", "Reach", "Far" };
    uint32_t sidx[3];
    for (uint32_t i = 0; i < 3; i++) {
        sidx[i] = find_stratum_of(plan, heads[i]);
        if (sidx[i] == UINT32_MAX) {
            wl_plan_free(plan);
            FAIL("missing stratum for a head relation");
            return;
        }
    }

    bool ok = false;
    uint64_t mask = strata_mask(plan, "Edge", &ok);
    wl_plan_free(plan);
    if (!ok) {
        FAIL("wl_session_create failed");
        return;
    }

    for (uint32_t i = 0; i < 3; i++) {
        if ((mask & ((uint64_t)1 << sidx[i])) == 0) {
            char buf[96];
            snprintf(buf, sizeof(buf),
                "%s stratum %u not marked; mask=0x%" PRIx64, heads[i],
                sidx[i], mask);
            FAIL(buf);
            return;
        }
    }
    PASS;
}

/* ======================================================================== */
/* TC7: rule-level mask                                                      */
/* ======================================================================== */

/*
 * col_compute_affected_rules() reads the same per-rule reference sets and is
 * blinded the same way.  It has no end-to-end observable of its own today
 * (see #1030), so it is asserted directly.
 */
static void
test_rule_level_mask_over_fusion(void)
{
    TEST("rule level: fused rule marked, independent rule left clear");

    wl_plan_t *plan = build_plan(SRC_FUSED_PLUS_INDEPENDENT, false);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    if (relation_leading_op(plan, "Path") != WL_PLAN_OP_K_FUSION) {
        wl_plan_free(plan);
        FAIL("premise: Path was not rewritten to K_FUSION");
        return;
    }

    /* Rules are enumerated stratum-major, relation-minor. */
    uint32_t idx = 0;
    uint32_t rule_path = UINT32_MAX;
    uint32_t rule_reach = UINT32_MAX;
    uint32_t rule_ind = UINT32_MAX;
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ri++) {
            const char *rn = plan->strata[si].relations[ri].name;
            if (rn && strcmp(rn, "Path") == 0)
                rule_path = idx;
            else if (rn && strcmp(rn, "Reach") == 0)
                rule_reach = idx;
            else if (rn && strcmp(rn, "Ind") == 0)
                rule_ind = idx;
            idx++;
        }
    }
    if (rule_path == UINT32_MAX || rule_reach == UINT32_MAX
        || rule_ind == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("could not locate all rule indices");
        return;
    }

    bool ok = false;
    uint64_t mask = rules_mask(plan, "Edge", &ok);
    wl_plan_free(plan);
    if (!ok) {
        FAIL("wl_session_create failed");
        return;
    }

    if ((mask & ((uint64_t)1 << rule_path)) == 0) {
        FAIL("fused rule Path not marked");
        return;
    }
    if ((mask & ((uint64_t)1 << rule_reach)) == 0) {
        FAIL("downstream rule Reach not marked");
        return;
    }
    if ((mask & ((uint64_t)1 << rule_ind)) != 0) {
        FAIL("independent rule Ind must stay clear");
        return;
    }
    PASS;
}

/* ======================================================================== */
/* END-TO-END HELPERS                                                        */
/* ======================================================================== */

#define MAX_TUPLES 256

typedef struct {
    int64_t x;
    int64_t y;
    int32_t mult;
} tuple_ent_t;

typedef struct {
    tuple_ent_t ent[MAX_TUPLES];
    uint32_t count;
    uint32_t overflow;
} tuple_set_t;

static void
tuple_set_bump(tuple_set_t *ts, int64_t x, int64_t y, int32_t diff)
{
    for (uint32_t i = 0; i < ts->count; i++) {
        if (ts->ent[i].x == x && ts->ent[i].y == y) {
            ts->ent[i].mult += diff;
            return;
        }
    }
    if (ts->count == MAX_TUPLES) {
        ts->overflow++;
        return;
    }
    ts->ent[ts->count].x = x;
    ts->ent[ts->count].y = y;
    ts->ent[ts->count].mult = diff;
    ts->count++;
}

static bool
tuple_set_live(const tuple_set_t *ts, int64_t x, int64_t y)
{
    for (uint32_t i = 0; i < ts->count; i++) {
        if (ts->ent[i].x == x && ts->ent[i].y == y)
            return ts->ent[i].mult > 0;
    }
    return false;
}

static uint32_t
tuple_set_live_count(const tuple_set_t *ts)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < ts->count; i++) {
        if (ts->ent[i].mult > 0)
            n++;
    }
    return n;
}

/* True when both sets have exactly the same live tuples. */
static bool
tuple_sets_equal(const tuple_set_t *a, const tuple_set_t *b)
{
    if (tuple_set_live_count(a) != tuple_set_live_count(b))
        return false;
    for (uint32_t i = 0; i < a->count; i++) {
        if (a->ent[i].mult > 0 && !tuple_set_live(b, a->ent[i].x, a->ent[i].y))
            return false;
    }
    return true;
}

static void
path_delta_cb(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    if (!relation || strcmp(relation, "Path") != 0 || ncols != 2)
        return;
    tuple_set_bump((tuple_set_t *)user_data, row[0], row[1], diff);
}

static void
path_tuple_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    if (!relation || strcmp(relation, "Path") != 0 || ncols != 2)
        return;
    tuple_set_bump((tuple_set_t *)user_data, row[0], row[1], 1);
}

/* A 5-cycle plus two chords; the retraction below breaks the cycle. */
static const int64_t SEED_EDGES[][2] = { { 1, 2 }, { 2, 3 } };
static const int64_t INSERT_EDGES[][2]
    = { { 3, 4 }, { 4, 5 }, { 5, 1 }, { 2, 5 }, { 6, 1 } };
static const int64_t RETRACT_EDGE[2] = { 5, 1 };

#define SEED_COUNT   (sizeof(SEED_EDGES) / sizeof(SEED_EDGES[0]))
#define INSERT_COUNT (sizeof(INSERT_EDGES) / sizeof(INSERT_EDGES[0]))

/*
 * full_eval_paths - Path relation of a fresh session holding the final EDB,
 * derived by a single non-incremental snapshot.
 */
static bool
full_eval_paths(uint32_t workers, tuple_set_t *out)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts.num_workers = workers;
    wirelog_easy_session_t *s = NULL;

    memset(out, 0, sizeof(*out));
    if (wirelog_easy_open_opts(SRC_FUSED_TC, &opts, &s) != WIRELOG_OK || !s)
        return false;

    for (size_t i = 0; i < SEED_COUNT; i++) {
        if (wirelog_easy_insert(s, "Edge", SEED_EDGES[i], 2) != WIRELOG_OK) {
            wirelog_easy_close(s);
            return false;
        }
    }
    for (size_t i = 0; i < INSERT_COUNT; i++) {
        if (INSERT_EDGES[i][0] == RETRACT_EDGE[0]
            && INSERT_EDGES[i][1] == RETRACT_EDGE[1])
            continue;
        if (wirelog_easy_insert(s, "Edge", INSERT_EDGES[i], 2) != WIRELOG_OK) {
            wirelog_easy_close(s);
            return false;
        }
    }

    bool ok = wirelog_easy_snapshot(s, "Path", path_tuple_cb, out)
        == WIRELOG_OK;
    wirelog_easy_close(s);
    return ok;
}

/*
 * incremental_paths - Drive the same EDB through delta callback + step, one
 * edge at a time, and fold the deltas back into a tuple set.  When
 * @before_retraction is non-NULL it receives the state just before the
 * retraction step.
 */
static bool
incremental_paths(uint32_t workers, tuple_set_t *out,
    tuple_set_t *before_retraction)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts.num_workers = workers;
    wirelog_easy_session_t *s = NULL;

    memset(out, 0, sizeof(*out));
    if (wirelog_easy_open_opts(SRC_FUSED_TC, &opts, &s) != WIRELOG_OK || !s)
        return false;
    if (wirelog_easy_set_delta_cb(s, path_delta_cb, out) != WIRELOG_OK) {
        wirelog_easy_close(s);
        return false;
    }

    for (size_t i = 0; i < SEED_COUNT; i++) {
        if (wirelog_easy_insert(s, "Edge", SEED_EDGES[i], 2) != WIRELOG_OK) {
            wirelog_easy_close(s);
            return false;
        }
    }
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        return false;
    }

    for (size_t i = 0; i < INSERT_COUNT; i++) {
        if (wirelog_easy_insert(s, "Edge", INSERT_EDGES[i], 2) != WIRELOG_OK
            || wirelog_easy_step(s) != WIRELOG_OK) {
            wirelog_easy_close(s);
            return false;
        }
    }

    if (before_retraction)
        *before_retraction = *out;

    if (wirelog_easy_remove(s, "Edge", RETRACT_EDGE, 2) != WIRELOG_OK
        || wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        return false;
    }

    wirelog_easy_close(s);
    return out->overflow == 0;
}

/* ======================================================================== */
/* TC8: incremental result equals a from-scratch evaluation                  */
/* ======================================================================== */

static void
test_incremental_matches_full_eval(void)
{
    const uint32_t worker_counts[] = { 1, 2, 4, 8 };
    const uint32_t worker_count_n
        = (uint32_t)(sizeof(worker_counts) / sizeof(worker_counts[0]));

    for (uint32_t wi = 0; wi < worker_count_n; wi++) {
        uint32_t w = worker_counts[wi];
        char name[80];
        snprintf(name, sizeof(name),
            "end to end: incremental == full evaluation (W=%u)", w);
        TEST(name);

        tuple_set_t inc;
        tuple_set_t full;
        if (!incremental_paths(w, &inc, NULL)) {
            FAIL("incremental run failed");
            continue;
        }
        if (!full_eval_paths(w, &full)) {
            FAIL("reference full evaluation failed");
            continue;
        }
        if (tuple_set_live_count(&full) == 0) {
            FAIL("premise: reference evaluation derived nothing");
            continue;
        }
        if (!tuple_sets_equal(&inc, &full)) {
            char buf[96];
            snprintf(buf, sizeof(buf), "incremental %u tuples, full %u tuples",
                tuple_set_live_count(&inc), tuple_set_live_count(&full));
            FAIL(buf);
            continue;
        }
        PASS;
    }
}

/* ======================================================================== */
/* TC9: retraction emits the exact negative deltas                           */
/* ======================================================================== */

/*
 * col_session_step() starts from UINT64_MAX and *intersects* the removal
 * mask, so a blinded 0 zeroes the whole mask and every retracted tuple is
 * retained.  This is the only case that exercises that line.
 */
static void
test_retraction_emits_exact_negative_deltas(void)
{
    TEST("retraction: exact -Path deltas and post-retraction state");

    tuple_set_t before;
    tuple_set_t after;
    tuple_set_t full;

    if (!incremental_paths(1, &after, &before)) {
        FAIL("incremental run failed");
        return;
    }
    if (!full_eval_paths(1, &full)) {
        FAIL("reference full evaluation failed");
        return;
    }

    if (tuple_set_live_count(&before) <= tuple_set_live_count(&full)) {
        FAIL("premise: retraction did not have to remove anything");
        return;
    }

    /* Every tuple live before and not live after must be exactly the set of
     * tuples the reference evaluation no longer derives -- no more, no
     * fewer. */
    for (uint32_t i = 0; i < before.count; i++) {
        if (before.ent[i].mult <= 0)
            continue;
        int64_t x = before.ent[i].x;
        int64_t y = before.ent[i].y;
        bool still_live = tuple_set_live(&after, x, y);
        bool should_live = tuple_set_live(&full, x, y);
        if (still_live != should_live) {
            char buf[112];
            snprintf(buf, sizeof(buf),
                "Path(%" PRId64 ",%" PRId64 ") live=%d expected=%d after "
                "retraction", x, y, (int)still_live, (int)should_live);
            FAIL(buf);
            return;
        }
    }

    if (!tuple_sets_equal(&after, &full)) {
        char buf[96];
        snprintf(buf, sizeof(buf), "post-retraction %u tuples, expected %u",
            tuple_set_live_count(&after), tuple_set_live_count(&full));
        FAIL(buf);
        return;
    }
    PASS;
}

/* ======================================================================== */
/* TC10: --watch reproduction from the issue                                 */
/* ======================================================================== */

/*
 * The original report: run the fused transitive closure under --watch, feed
 * "Edge 3 4" on stdin, and observe that nothing is emitted.  wl_run_pipeline()
 * always applies wirelog_optimize(), and the fusion rewrite is inside
 * wl_plan_from_program(), so the shape is K_FUSION either way.
 */
static void
test_watch_mode_emits_new_derivations(void)
{
    TEST("--watch: piped fact still derives new tuples");

    char inpath[512];
    char outpath[512];
    test_tmppath(inpath, sizeof(inpath), "wirelog_test_1019_watch.in");
    test_tmppath(outpath, sizeof(outpath), "wirelog_test_1019_watch.out");

    FILE *in = fopen(inpath, "w");
    if (!in) {
        FAIL("cannot create watch input file");
        return;
    }
    fprintf(in, "Edge 3 4\n");
    fclose(in);

    if (!freopen(inpath, "r", stdin)) {
        remove(inpath);
        FAIL("cannot redirect stdin");
        return;
    }

    FILE *out = fopen(outpath, "w");
    if (!out) {
        remove(inpath);
        FAIL("cannot create watch output file");
        return;
    }

    const char *src = ".decl Edge(x: int64, y: int64)\n"
        ".decl Path(x: int64, y: int64)\n"
        ".output Path\n"
        "Edge(1,2).\n"
        "Edge(2,3).\n"
        "Path(x,y) :- Edge(x,y).\n"
        "Path(x,y) :- Path(x,z), Path(z,y).\n";

    int rc = wl_run_pipeline(src, 1, false, true, 0, out);
    fclose(out);
    if (rc != 0) {
        remove(inpath);
        remove(outpath);
        FAIL("wl_run_pipeline returned non-zero");
        return;
    }

    char *output = wl_read_file(outpath);
    remove(inpath);
    remove(outpath);
    if (!output) {
        FAIL("cannot read watch output");
        return;
    }

    /* Adding Edge(3,4) to Path(1,2),Path(2,3),Path(1,3) must derive the three
     * new paths that end at 4. */
    const char *expected[] = { "+Path(3, 4)", "+Path(2, 4)", "+Path(1, 4)" };
    for (uint32_t i = 0; i < 3; i++) {
        if (!strstr(output, expected[i])) {
            char buf[96];
            snprintf(buf, sizeof(buf), "missing %s in watch output",
                expected[i]);
            free(output);
            FAIL(buf);
            return;
        }
    }
    free(output);
    PASS;
}

/* ======================================================================== */
/* MAIN                                                                      */
/* ======================================================================== */

int
main(void)
{
    printf("=== Affected-strata detection across plan rewrites (#1019) ===\n");

    test_fused_rule_marks_stratum();
    test_cross_rule_fusion_marks_stratum();
    test_lftj_star_join_marks_stratum();
    test_unfused_control();
    test_independent_stratum_stays_clear();
    test_transitive_propagation_through_fusion();
    test_rule_level_mask_over_fusion();
    test_incremental_matches_full_eval();
    test_retraction_emits_exact_negative_deltas();

    /* Last: this one replaces the process stdin. */
    test_watch_mode_emits_new_derivations();

    printf("\n  passed: %d, failed: %d\n", tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
