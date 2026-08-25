/*
 * test_plan_gen.c - Unit tests for wl_plan_from_program() and wl_plan_free()
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/backend.h"
#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/intern.h"
#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework
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
 * Snapshot counting callback
 * ---------------------------------------------------------------- */

struct count_ctx {
    int64_t count;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct count_ctx *ctx = (struct count_ctx *)user_data;
    ctx->count++;
    (void)relation;
    (void)row;
    (void)ncols;
}

/* ----------------------------------------------------------------
 * Test: Simple TC program generates valid plan
 * ---------------------------------------------------------------- */

static void
test_tc_plan_generation(void)
{
    TEST("TC plan generation");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "wl_plan_from_program failed");
    ASSERT(plan != NULL, "plan is NULL");

    /* Should have at least 1 stratum */
    ASSERT(plan->stratum_count >= 1, "no strata");

    /* Should have at least 1 relation (tc) in some stratum */
    int found_tc = 0;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            if (plan->strata[s].relations[r].name
                && strcmp(plan->strata[s].relations[r].name, "tc") == 0) {
                found_tc = 1;
                ASSERT(plan->strata[s].relations[r].op_count > 0,
                    "tc has no ops");
            }
        }
    }
    ASSERT(found_tc, "tc relation not found in plan");

    /* edge should be an EDB */
    int found_edge_edb = 0;
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        if (plan->edb_relations[i]
            && strcmp(plan->edb_relations[i], "edge") == 0)
            found_edge_edb = 1;
    }
    ASSERT(found_edge_edb, "edge not in EDB list");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_generation_preinterns_static_string_literals(void)
{
    TEST("plan generation pre-interns static string literals");

    const char *src = ".decl trigger(value: symbol)\n"
        ".decl unused(value: symbol, plane: symbol)\n"
        "unused(X, \"data\") :- trigger(X).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");
    ASSERT(wl_intern_get(prog->intern, "data") < 0,
        "parser unexpectedly interned projection literal");

    ASSERT(wl_fusion_apply(prog, NULL) == 0, "fusion failed");
    ASSERT(wl_jpp_apply(prog, NULL) == 0, "JPP failed");
    ASSERT(wl_sip_apply(prog, NULL) == 0, "SIP failed");

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");
    ASSERT(wl_intern_get(prog->intern, "data") >= 0,
        "plan generation did not intern projection literal");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: TC end-to-end via columnar session
 * ---------------------------------------------------------------- */

static void
test_tc_end_to_end(void)
{
    TEST("TC end-to-end columnar session");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");
    ASSERT(ctx.count > 0, "no tuples produced");

    /* TC on 1->2->3->4 should produce 6 tuples:
     * tc(1,2), tc(2,3), tc(3,4), tc(1,3), tc(2,4), tc(1,4) */
    ASSERT(ctx.count == 6, "expected 6 TC tuples");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_join_project_fusion_plan_shape(void)
{
    TEST("JOIN project fusion plan shape");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl r(x: int32, z: int32)\n"
        "r(x, z) :- a(x, y), b(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    const wl_plan_relation_t *rp = NULL;
    for (uint32_t s = 0; s < plan->stratum_count && !rp; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *cand = &plan->strata[s].relations[r];
            if (cand->name && strcmp(cand->name, "r") == 0) {
                rp = cand;
                break;
            }
        }
    }
    ASSERT(rp != NULL, "relation r not found");

    const wl_plan_op_t *join = NULL;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        ASSERT(rp->ops[i].op != WL_PLAN_OP_MAP,
            "pure join projection should not leave MAP");
        if (rp->ops[i].op == WL_PLAN_OP_JOIN)
            join = &rp->ops[i];
    }
    ASSERT(join != NULL, "JOIN not found");
    ASSERT(join->project_count == 2, "JOIN should carry projection width");
    ASSERT(join->project_indices != NULL, "JOIN projection indices missing");
    ASSERT(join->project_indices[0] == 0, "first projected column should be x");
    ASSERT(join->project_indices[1] == 3,
        "second projected column should be z");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_join_project_fusion_keeps_computed_map(void)
{
    TEST("JOIN project fusion keeps computed MAP");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl r(x: int32, z: int32)\n"
        "r(x + 1, z) :- a(x, y), b(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    bool found_map = false;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *rp = &plan->strata[s].relations[r];
            if (!rp->name || strcmp(rp->name, "r") != 0)
                continue;
            for (uint32_t i = 0; i < rp->op_count; i++)
                if (rp->ops[i].op == WL_PLAN_OP_MAP)
                    found_map = true;
        }
    }
    ASSERT(found_map, "computed projection should keep MAP");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_k_fusion_sequences_fuse_join_project(void)
{
    TEST("K_FUSION sequences fuse JOIN project");

    const char *src = ".decl p(x: int32, y: int32)\n"
        "p(1, 2).\n"
        "p(x, z) :- p(x, y), p(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    const wl_plan_op_k_fusion_t *kf = NULL;
    for (uint32_t s = 0; s < plan->stratum_count && !kf; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count && !kf; r++) {
            const wl_plan_relation_t *rel = &plan->strata[s].relations[r];
            if (!rel->name || strcmp(rel->name, "p") != 0)
                continue;
            for (uint32_t i = 0; i < rel->op_count; i++) {
                if (rel->ops[i].op == WL_PLAN_OP_K_FUSION) {
                    kf = (const wl_plan_op_k_fusion_t *)
                        rel->ops[i].opaque_data;
                    break;
                }
            }
        }
    }
    ASSERT(kf != NULL, "K_FUSION not found");
    ASSERT(kf->k >= 2, "K_FUSION should contain delta copies");

    for (uint32_t d = 0; d < kf->k; d++) {
        bool projected_join = false;
        for (uint32_t i = 0; i < kf->k_op_counts[d]; i++) {
            const wl_plan_op_t *op = &kf->k_ops[d][i];
            ASSERT(op->op != WL_PLAN_OP_MAP,
                "pure projection MAP should be fused inside K_FUSION");
            if (op->op == WL_PLAN_OP_JOIN && op->project_count == 2
                && op->project_indices)
                projected_join = true;
        }
        ASSERT(projected_join, "projected JOIN not found in K_FUSION copy");
    }

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: SIP-inserted SEMIJOINs must not shift column indices (#955)
 *
 * A SEMIJOIN filters its left input and emits the left columns only.
 * When the plan generator resolved join keys and projections against a
 * column layout that also counted the SEMIJOIN's right side, every index
 * past the first SEMIJOIN was shifted; out-of-range "colN" names then fell
 * back to column 0 in the evaluator, silently dropping derivations.
 *
 * The chain below is the smallest shape that exposes it: five atoms, all
 * variables live in the head (so no projection re-bases the layout), each
 * atom introducing a fresh variable.
 * ---------------------------------------------------------------- */

static const char *k_semijoin_chain_src
    = ".decl a(p1: int64, p2: int64)\n"
    "a(1, 2).\n"
    ".decl b(q1: int64, q2: int64)\n"
    "b(2, 3).\n"
    ".decl c(r1: int64, r2: int64)\n"
    "c(3, 4).\n"
    ".decl d(s1: int64, s2: int64)\n"
    "d(4, 5).\n"
    ".decl e(t1: int64, t2: int64)\n"
    "e(5, 6).\n"
    ".decl chain(c1: int64, c2: int64, c3: int64, c4: int64, c5: int64,"
    " c6: int64)\n"
    "chain(v1, v2, v3, v4, v5, v6) :- a(v1, v2), b(v2, v3), c(v3, v4),"
    " d(v4, v5), e(v5, v6).\n";

/* Declared arity of the relations in k_semijoin_chain_src. */
static uint32_t
chain_rel_width(const char *name)
{
    if (!name)
        return 0;
    if (strcmp(name, "chain") == 0)
        return 6;
    return 2; /* a, b, c, d, e are all binary */
}

static void
test_semijoin_does_not_shift_column_indices(void)
{
    TEST("SEMIJOIN does not shift join key/projection indices");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(k_semijoin_chain_src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    const wl_plan_relation_t *rp = NULL;
    for (uint32_t s = 0; s < plan->stratum_count && !rp; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *cand = &plan->strata[s].relations[r];
            if (cand->name && strcmp(cand->name, "chain") == 0) {
                rp = cand;
                break;
            }
        }
    }
    ASSERT(rp != NULL, "relation chain not found");

    /* Replay the operator sequence, tracking the width of the relation on
     * top of the evaluator stack, and check every resolved index against
     * the operand widths it will actually see at run time. */
    uint32_t width = 0;
    int saw_semijoin = 0;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        const wl_plan_op_t *op = &rp->ops[i];
        uint32_t rw = op->right_relation ? chain_rel_width(op->right_relation)
                                         : 0;
        if (op->op == WL_PLAN_OP_SEMIJOIN)
            saw_semijoin = 1;
        if (op->op == WL_PLAN_OP_JOIN || op->op == WL_PLAN_OP_SEMIJOIN
            || op->op == WL_PLAN_OP_ANTIJOIN) {
            for (uint32_t k = 0; k < op->key_count; k++) {
                const char *lk = op->left_keys ? op->left_keys[k] : NULL;
                ASSERT(lk != NULL, "missing left key");
                ASSERT(strncmp(lk, "col", 3) == 0, "left key is not colN");
                ASSERT((uint32_t)atoi(lk + 3) < width,
                    "left join key column index out of range");
            }
            uint32_t concat = (op->op == WL_PLAN_OP_JOIN) ? width + rw : width;
            for (uint32_t p = 0; p < op->project_count; p++) {
                if (!op->project_indices)
                    break;
                ASSERT(op->project_indices[p] < concat,
                    "projection index out of range");
            }
        }
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
            width = chain_rel_width(op->relation_name);
            break;
        case WL_PLAN_OP_JOIN:
            width = op->project_count ? op->project_count : width + rw;
            break;
        case WL_PLAN_OP_MAP:
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
            if (op->project_count)
                width = op->project_count;
            break;
        default:
            break;
        }
    }
    ASSERT(saw_semijoin, "SIP inserted no SEMIJOIN -- test no longer covers "
        "the regression");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

struct chain_ctx {
    int64_t count;
    int64_t row[8];
    uint32_t ncols;
};

static void
chain_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct chain_ctx *ctx = (struct chain_ctx *)user_data;
    if (!relation || strcmp(relation, "chain") != 0)
        return;
    ctx->count++;
    if (ctx->count == 1) {
        ctx->ncols = ncols < 8 ? ncols : 8;
        for (uint32_t i = 0; i < ctx->ncols; i++)
            ctx->row[i] = row[i];
    }
}

static void
test_semijoin_chain_end_to_end(void)
{
    TEST("five-atom chain with SEMIJOINs derives its one tuple");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(k_semijoin_chain_src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    struct chain_ctx ctx = { 0, { 0 }, 0 };
    rc = wl_session_snapshot(sess, chain_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");

    ASSERT(ctx.count == 1, "expected exactly one chain tuple");
    ASSERT(ctx.ncols == 6, "expected six columns");
    for (uint32_t i = 0; i < 6; i++)
        ASSERT(ctx.row[i] == (int64_t)(i + 1), "unexpected chain tuple value");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* Same chain shape, but with a negated atom so the head projection stays a
 * separate MAP instead of being fused into the last JOIN.  The shifted
 * layout corrupted that projection too: the tuple was still derived, with
 * an out-of-range index silently yielding 0 in the last column. */
static const char *k_semijoin_negation_src
    = ".decl a(p1: int64, p2: int64)\n"
    "a(1, 2).\n"
    ".decl b(q1: int64, q2: int64)\n"
    "b(2, 3).\n"
    ".decl n(z1: int64)\n"
    "n(99).\n"
    ".decl c(r1: int64, r2: int64)\n"
    "c(3, 4).\n"
    ".decl d(s1: int64, s2: int64)\n"
    "d(4, 5).\n"
    ".decl chain(c1: int64, c2: int64, c3: int64, c4: int64, c5: int64)\n"
    "chain(v1, v2, v3, v4, v5) :- a(v1, v2), b(v2, v3), !n(v3),"
    " c(v3, v4), d(v4, v5).\n";

static void
test_semijoin_head_projection_values(void)
{
    TEST("SEMIJOIN chain keeps head projection values intact");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(k_semijoin_negation_src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    struct chain_ctx ctx = { 0, { 0 }, 0 };
    rc = wl_session_snapshot(sess, chain_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");

    ASSERT(ctx.count == 1, "expected exactly one chain tuple");
    ASSERT(ctx.ncols == 5, "expected five columns");
    for (uint32_t i = 0; i < 5; i++)
        ASSERT(ctx.row[i] == (int64_t)(i + 1), "unexpected chain tuple value");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: a JOIN whose right child is not a relation is rejected
 *
 * wl_plan_op_t.right_relation is a relation name, not a subtree.  A JOIN
 * whose right child is itself a composite node cannot be represented; if
 * plan generation emits it anyway the operator silently matches nothing
 * (Issue #989).
 * ---------------------------------------------------------------- */

static wirelog_ir_node_t *
find_right_deep_candidate(wirelog_ir_node_t *node)
{
    if (!node)
        return NULL;
    if (node->type == WIRELOG_IR_JOIN && node->child_count == 2
        && node->children[0]
        && node->children[0]->type == WIRELOG_IR_JOIN)
        return node;
    for (uint32_t i = 0; i < node->child_count; i++) {
        wirelog_ir_node_t *hit = find_right_deep_candidate(node->children[i]);
        if (hit)
            return hit;
    }
    return NULL;
}

static wirelog_ir_node_t *
find_composite_left_joinlike(wirelog_ir_node_t *node,
    wirelog_ir_node_type_t type)
{
    if (!node)
        return NULL;
    if (node->type == type && node->child_count == 2
        && node->children[0]
        && node->children[0]->type == WIRELOG_IR_JOIN
        && node->children[1] && node->children[1]->relation_name)
        return node;
    for (uint32_t i = 0; i < node->child_count; i++) {
        wirelog_ir_node_t *hit
            = find_composite_left_joinlike(node->children[i], type);
        if (hit)
            return hit;
    }
    return NULL;
}

static void
test_join_with_composite_right_child_rejected(void)
{
    TEST("JOIN with composite right child is rejected");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl c(z: int32, w: int32)\n"
        ".decl out(x: int32, w: int32)\n"
        "a(1, 2). b(2, 3). c(3, 4).\n"
        "out(x, w) :- a(x, y), b(y, z), c(z, w).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    /* The parser builds JOIN(JOIN(a, b), c).  Swap the outer JOIN's
     * children to obtain the right-deep JOIN(c, JOIN(a, b)). */
    wirelog_ir_node_t *outer = NULL;
    for (uint32_t r = 0; r < prog->rule_count && !outer; r++) {
        if (prog->rules[r].head_relation
            && strcmp(prog->rules[r].head_relation, "out") == 0)
            outer = find_right_deep_candidate(prog->rules[r].ir_root);
    }
    if (!outer) {
        wirelog_program_free(prog);
        FAIL("no left-deep JOIN chain to invert");
        return;
    }

    wirelog_ir_node_t *tmp = outer->children[0];
    outer->children[0] = outer->children[1];
    outer->children[1] = tmp;

    if (wl_ir_program_rebuild_relation_irs(prog) != 0) {
        wirelog_program_free(prog);
        FAIL("rebuild_relation_irs failed");
        return;
    }

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc == 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("plan generation accepted an unrepresentable JOIN right child");
        return;
    }
    ASSERT(plan == NULL, "plan must not be returned on error");

    wirelog_program_free(prog);
    PASS();
}

static void
test_antijoin_with_composite_right_child_rejected(void)
{
    TEST("ANTIJOIN with composite right child is rejected (#993)");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl blocked(z: int32)\n"
        ".decl out(x: int32)\n"
        "out(x) :- a(x, y), b(y, z), !blocked(z).\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_plan_t *control = NULL;
    int rc = wl_plan_from_program(prog, &control);
    ASSERT(rc == 0 && control != NULL,
        "unmodified ANTIJOIN plan should be valid");
    wl_plan_free(control);

    wirelog_ir_node_t *anti = NULL;
    for (uint32_t r = 0; r < prog->rule_count && !anti; r++) {
        if (prog->rules[r].head_relation
            && strcmp(prog->rules[r].head_relation, "out") == 0)
            anti = find_composite_left_joinlike(prog->rules[r].ir_root,
                    WIRELOG_IR_ANTIJOIN);
    }
    ASSERT(anti != NULL, "no composite-left ANTIJOIN candidate found");

    uint32_t saved_child_count = anti->child_count;
    anti->child_count = 1;
    ASSERT(wl_ir_program_rebuild_relation_irs(prog) == 0,
        "rebuild_relation_irs failed for missing child");
    wl_plan_t *missing_plan = NULL;
    rc = wl_plan_from_program(prog, &missing_plan);
    ASSERT(rc != 0 && missing_plan == NULL,
        "plan generation accepted a missing ANTIJOIN child");
    anti->child_count = saved_child_count;

    wirelog_ir_node_t *tmp = anti->children[0];
    anti->children[0] = anti->children[1];
    anti->children[1] = tmp;
    ASSERT(wl_ir_program_rebuild_relation_irs(prog) == 0,
        "rebuild_relation_irs failed");

    wl_plan_t *plan = NULL;
    rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc != 0 && plan == NULL,
        "plan generation accepted an unrepresentable ANTIJOIN");
    wirelog_program_free(prog);
    PASS();
}

static void
test_semijoin_with_composite_right_child_rejected(void)
{
    TEST("SEMIJOIN with composite right child is rejected (#993)");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(k_semijoin_chain_src, &err);
    ASSERT(prog != NULL, "parse failed");
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *control = NULL;
    int rc = wl_plan_from_program(prog, &control);
    ASSERT(rc == 0 && control != NULL,
        "unmodified SEMIJOIN plan should be valid");
    wl_plan_free(control);

    wirelog_ir_node_t *semi = NULL;
    for (uint32_t r = 0; r < prog->rule_count && !semi; r++) {
        if (prog->rules[r].head_relation
            && strcmp(prog->rules[r].head_relation, "chain") == 0)
            semi = find_composite_left_joinlike(prog->rules[r].ir_root,
                    WIRELOG_IR_SEMIJOIN);
    }
    ASSERT(semi != NULL, "no composite-left SEMIJOIN candidate found");

    wirelog_ir_node_t *tmp = semi->children[0];
    semi->children[0] = semi->children[1];
    semi->children[1] = tmp;
    ASSERT(wl_ir_program_rebuild_relation_irs(prog) == 0,
        "rebuild_relation_irs failed");

    wl_plan_t *plan = NULL;
    rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc != 0 && plan == NULL,
        "plan generation accepted an unrepresentable SEMIJOIN");
    wirelog_program_free(prog);
    PASS();
}

static const wl_plan_relation_t *
find_plan_relation(const wl_plan_t *plan, const char *name)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *rel = &plan->strata[s].relations[r];
            if (rel->name && strcmp(rel->name, name) == 0)
                return rel;
        }
    }
    return NULL;
}

/* Issue #994: side-compound joins must lower from either atom position. */
static void
test_side_compound_lowers_in_either_atom_position(void)
{
    TEST("side compound lowers from any body-atom position (#994)");

    const char *src = ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl gate(id: int64)\n"
        ".decl hot(id: int64, r: int64)\n"
        "hot(ID, R) :- gate(ID), event(ID, metadata(_, _, _, R)).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0 || !plan) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("side compound in a non-first atom must lower");
        return;
    }
    const wl_plan_relation_t *hot = find_plan_relation(plan, "hot");
    ASSERT(hot != NULL, "hot relation plan not found");

    const wl_plan_op_t *chain = NULL;
    const wl_plan_op_t *side = NULL;
    for (uint32_t i = 0; i < hot->op_count; i++) {
        const wl_plan_op_t *op = &hot->ops[i];
        if (op->op != WL_PLAN_OP_JOIN || !op->right_relation)
            continue;
        if (strcmp(op->right_relation, "event") == 0)
            chain = op;
        if (strcmp(op->right_relation, "__compound_metadata_4") == 0)
            side = op;
    }
    ASSERT(chain != NULL, "event join not found");
    ASSERT(side != NULL, "side-compound join not found");
    ASSERT(side > chain, "side join must follow the chain join");
    ASSERT(side->key_count == 1, "side join must have one key");
    ASSERT(strcmp(side->right_keys[0], "col0") == 0,
        "side join must use side handle column");
    wirelog_program_free(prog);
    wl_plan_free(plan);

    /* Control: the same rule with the compound atom first must also lower;
     * both spellings are accepted. */
    const char *ok_src = ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl gate(id: int64)\n"
        ".decl hot(id: int64, r: int64)\n"
        "hot(ID, R) :- event(ID, metadata(_, _, _, R)), gate(ID).\n";

    wirelog_program_t *ok = wirelog_parse_string(ok_src, &err);
    ASSERT(ok != NULL, "control parse failed");

    wl_plan_t *ok_plan = NULL;
    if (wl_plan_from_program(ok, &ok_plan) != 0) {
        wirelog_program_free(ok);
        FAIL("control: compound-first rule must still lower");
        return;
    }
    wl_plan_free(ok_plan);
    wirelog_program_free(ok);

    PASS();
}

/* ----------------------------------------------------------------
 * Test: wl_plan_free NULL-safe
 * ---------------------------------------------------------------- */

static void
test_plan_free_null(void)
{
    TEST("wl_plan_free NULL-safe");
    wl_plan_free(NULL); /* should not crash */
    PASS();
}

/* ----------------------------------------------------------------
 * Test: wl_session_load_facts NULL-safe
 * ---------------------------------------------------------------- */

static void
test_load_facts_null_safe(void)
{
    TEST("wl_session_load_facts NULL-safe");
    int rc = wl_session_load_facts(NULL, NULL);
    ASSERT(rc == -1, "expected -1 for NULL args");
    PASS();
}

/* ----------------------------------------------------------------
 * Main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Plan Generator Tests ===\n");

    test_tc_plan_generation();
    test_plan_generation_preinterns_static_string_literals();
    test_tc_end_to_end();
    test_join_project_fusion_plan_shape();
    test_join_project_fusion_keeps_computed_map();
    test_k_fusion_sequences_fuse_join_project();
    test_semijoin_does_not_shift_column_indices();
    test_semijoin_chain_end_to_end();
    test_semijoin_head_projection_values();
    test_join_with_composite_right_child_rejected();
    test_antijoin_with_composite_right_child_rejected();
    test_semijoin_with_composite_right_child_rejected();
    test_side_compound_lowers_in_either_atom_position();
    test_plan_free_null();
    test_load_facts_null_safe();

    printf("\n%d tests: %d passed, %d failed\n", test_count, pass_count,
        fail_count);
    return fail_count > 0 ? 1 : 0;
}
