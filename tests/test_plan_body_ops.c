/*
 * test_plan_body_ops.c - Issue #2114: the plan keeps each relation's body.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * wl_plan_from_program() rewrites a relation's operators after lowering:
 * join chains become LFTJ, multi-way recursive rules become K_FUSION, and
 * exchanges are inserted.  An evaluator that binds each body occurrence to
 * a delta or to a full relation needs the operators as lowered, so the plan
 * records a copy of every relation's body before those rewrites, in
 * wl_plan_stratum_t.bodies.
 *
 * The expectations below come from the Datalog source, not from the
 * lowering: which relations each body reads and how often, that a body
 * holds none of the rewrite operators, and that an arithmetic head keeps
 * its expressions.
 */

#include "../wirelog/exec_plan.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

typedef struct {
    const char *name;
    uint32_t count;
} read_t;

static wl_plan_t *
build(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    wl_plan_t *plan = NULL;

    if (!prog)
        return NULL;
    plan_fixture_hold(prog);
    if (wl_plan_from_program(prog, &plan) != 0)
        return NULL;
    return plan;
}

/* The stratum and index of the relation named @name. */
static bool
find_relation(const wl_plan_t *plan, const char *name, uint32_t *si_out,
    uint32_t *ri_out)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            if (sp->relations[ri].name
                && strcmp(sp->relations[ri].name, name) == 0) {
                *si_out = si;
                *ri_out = ri;
                return true;
            }
        }
    }
    return false;
}

/* The relation an operator reads, if any. */
static const char *
op_reads(const wl_plan_op_t *op)
{
    switch (op->op) {
    case WL_PLAN_OP_VARIABLE:
        return op->relation_name;
    case WL_PLAN_OP_JOIN:
    case WL_PLAN_OP_ANTIJOIN:
    case WL_PLAN_OP_SEMIJOIN:
        return op->right_relation;
    default:
        return NULL;
    }
}

static bool
has_op(const wl_plan_op_t *ops, uint32_t n, wl_plan_op_type_t kind)
{
    for (uint32_t i = 0; i < n; i++)
        if (ops[i].op == kind)
            return true;
    return false;
}

static char message[256];

/* Check the body of @head against the reads @want (each name with the
 * number of body occurrences that read it, and nothing else read). */
static const char *
check_body(const wl_plan_t *plan, const char *head, const read_t *want,
    uint32_t nwant)
{
    uint32_t si, ri;

    if (!find_relation(plan, head, &si, &ri))
        return "head relation not in the plan";
    const wl_plan_stratum_t *sp = &plan->strata[si];
    if (!sp->bodies)
        return "stratum records no bodies";
    const wl_plan_body_t *body = &sp->bodies[ri];
    if (body->op_count == 0 || !body->ops)
        return "body is empty";
    if (body->ops == sp->relations[ri].ops)
        return "body aliases the rewritten operators";
    for (uint32_t i = 0; i < body->op_count; i++) {
        wl_plan_op_type_t kind = body->ops[i].op;
        if (kind == WL_PLAN_OP_K_FUSION || kind == WL_PLAN_OP_LFTJ
            || kind == WL_PLAN_OP_EXCHANGE)
            return "body holds a rewrite operator";
        if (body->ops[i].opaque_data)
            return "body operator carries opaque_data";
    }
    uint32_t total = 0;
    for (uint32_t i = 0; i < body->op_count; i++)
        if (op_reads(&body->ops[i]))
            total++;
    uint32_t expected_total = 0;
    for (uint32_t w = 0; w < nwant; w++) {
        uint32_t seen = 0;
        for (uint32_t i = 0; i < body->op_count; i++) {
            const char *r = op_reads(&body->ops[i]);
            if (r && strcmp(r, want[w].name) == 0)
                seen++;
        }
        if (seen != want[w].count) {
            snprintf(message, sizeof(message),
                "%s body reads %s %u times, want %u", head, want[w].name,
                seen, want[w].count);
            return message;
        }
        expected_total += want[w].count;
    }
    if (total != expected_total) {
        snprintf(message, sizeof(message),
            "%s body reads %u occurrences, want %u", head, total,
            expected_total);
        return message;
    }
    return NULL;
}

static void
run(const char *name, const char *src, const char *head, const read_t *want,
    uint32_t nwant)
{
    const char *why;
    wl_plan_t *plan;

    tests_run++;
    printf("  [%d] %s", tests_run, name);
    plan = build(src);
    if (!plan) {
        FAIL("plan");
        return;
    }
    why = check_body(plan, head, want, nwant);
    wl_plan_free(plan);
    if (why) {
        FAIL(why);
        return;
    }
    tests_passed++;
    printf(" ... PASS\n");
}

/* Shape A: a right-recursive closure with its base rule. */
static void
test_shape_a(void)
{
    static const read_t want[] = { { "e", 2 }, { "a", 1 } };
    run("right-recursive closure",
        ".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
        "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n",
        "a", want, 2);
}

/* Shape B: a base rule joining the input with itself. */
static void
test_shape_b(void)
{
    static const read_t want[] = { { "e", 2 }, { "a", 1 }, { "s", 1 } };
    run("base rule joining its input twice",
        ".decl e(x: int64, y: int64)\n.decl s(x: int64, y: int64)\n"
        ".decl a(x: int64, y: int64)\n"
        "a(x, z) :- e(x, y), e(y, z).\na(x, z) :- a(x, y), s(y, z).\n",
        "a", want, 3);
}

/* Shape C: a rule with two recursive atoms, which the multi-way rewrite
 * expands and, in a K-fusion build, fuses. */
static void
test_shape_c(void)
{
    static const read_t want[] = { { "f", 1 }, { "e", 1 }, { "a", 2 } };
    run("rule with two recursive atoms",
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
        ".decl a(x: int64, y: int64)\n"
        "a(x, y) :- f(x, y).\na(x, z) :- e(x, y), a(y, w), a(w, z).\n",
        "a", want, 3);
}

/* A join chain over one key, the shape the join-chain rewrite replaces with
 * an LFTJ operator, freeing the joins it absorbs.  The rewritten operators
 * must hold that LFTJ, or the case would stop covering the rewrite. */
static void
test_chain(void)
{
    static const read_t want[] = {
        { "a", 1 }, { "b", 1 }, { "c", 1 }, { "guard", 1 }
    };
    static const char *const src =
        ".decl a(x: int64, value: int64)\n"
        ".decl b(x: int64, value: int64)\n"
        ".decl c(x: int64, value: int64)\n"
        ".decl guard(q: int64)\n"
        ".decl out(x: int64, av: int64, bv: int64, cv: int64)\n"
        "out(x, av, bv, cv) :- a(x, av), b(x, bv), c(x, cv), guard(7).\n";
    uint32_t si, ri;
    wl_plan_t *plan;
    const char *why;

    tests_run++;
    printf("  [%d] join chain rewritten to LFTJ", tests_run);
    plan = build(src);
    if (!plan) {
        FAIL("plan");
        return;
    }
    why = check_body(plan, "out", want, 4);
    if (!why && find_relation(plan, "out", &si, &ri)) {
        const wl_plan_relation_t *rp = &plan->strata[si].relations[ri];
        if (!has_op(rp->ops, rp->op_count, WL_PLAN_OP_LFTJ))
            why = "the chain was not rewritten to LFTJ";
    }
    wl_plan_free(plan);
    if (why) {
        FAIL(why);
        return;
    }
    tests_passed++;
    printf(" ... PASS\n");
}

/* An arithmetic head keeps its expressions in the body. */
static void
test_arithmetic_head(void)
{
    static const read_t want[] = { { "e", 1 } };
    uint32_t si, ri;
    wl_plan_t *plan;
    const char *why;

    tests_run++;
    printf("  [%d] arithmetic head keeps its expressions", tests_run);
    plan = build(".decl e(x: int64, y: int64)\n.decl b(x: int64, y: int64)\n"
            "b(x, y + 1) :- e(x, y).\n");
    if (!plan) {
        FAIL("plan");
        return;
    }
    why = check_body(plan, "b", want, 1);
    if (!why && find_relation(plan, "b", &si, &ri)) {
        const wl_plan_body_t *body = &plan->strata[si].bodies[ri];
        bool exprs = false;
        for (uint32_t i = 0; i < body->op_count; i++)
            if (body->ops[i].op == WL_PLAN_OP_MAP
                && body->ops[i].map_expr_count > 0
                && body->ops[i].map_exprs)
                exprs = true;
        if (!exprs)
            why = "no MAP with expressions in the body";
    }
    wl_plan_free(plan);
    if (why) {
        FAIL(why);
        return;
    }
    tests_passed++;
    printf(" ... PASS\n");
}

/* The rewrites still run on the operators the evaluator uses: the rule with
 * two recursive atoms is fused, or at least gets an exchange, there.
 * Without this the body tests above could pass on a plan that never
 * rewrote anything. */
static void
test_rewrites_still_apply(void)
{
    uint32_t si, ri;
    wl_plan_t *plan;
    const char *why = NULL;

    tests_run++;
    printf("  [%d] rewritten operators differ from the body", tests_run);
    plan = build(".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
            ".decl a(x: int64, y: int64)\n"
            "a(x, y) :- f(x, y).\na(x, z) :- e(x, y), a(y, w), a(w, z).\n");
    if (!plan) {
        FAIL("plan");
        return;
    }
    if (!find_relation(plan, "a", &si, &ri)) {
        why = "a not in the plan";
    } else {
        const wl_plan_relation_t *rp = &plan->strata[si].relations[ri];
        if (!has_op(rp->ops, rp->op_count, WL_PLAN_OP_K_FUSION)
            && !has_op(rp->ops, rp->op_count, WL_PLAN_OP_LFTJ)
            && !has_op(rp->ops, rp->op_count, WL_PLAN_OP_EXCHANGE))
            why = "the recursive rule was not rewritten";
    }
    wl_plan_free(plan);
    if (why) {
        FAIL(why);
        return;
    }
    tests_passed++;
    printf(" ... PASS\n");
}

/* Existence opcodes must survive in actual preserved lowered bodies even if
 * the backend later rewrites the relation to another execution strategy. */
static void
test_existence_bodies(void)
{
    const char *sources[] = {
        ".decl a(x:int64,y:int64)\n.decl b(y:int64,w:int64)\n"
        ".decl c(w:int64,z:int64)\n.decl output(x:int64,z:int64)\n"
        "output(x,z) :- a(x,y),b(y,w),c(w,z).\n",
        ".decl a(x:int64)\n.decl b(x:int64)\n.decl output(x:int64)\n"
        "output(x) :- a(x),!b(x).\n"
    };
    for (unsigned anti = 0; anti < 2; ++anti) {
        ++tests_run;
        printf("  preserved %s body", anti ? "ANTIJOIN" : "SIP SEMIJOIN");
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(sources[anti],
                &error);
        wl_plan_t *plan = NULL;
        if (!program) {
            FAIL("parse failed"); continue;
        }
        plan_fixture_hold(program);
        if (!anti) {
            wl_sip_stats_t stats = {0};
            if (wl_sip_apply(program,
                &stats) || stats.semijoins_inserted != 1) {
                FAIL("actual SIP insertion missing"); continue;
            }
        }
        if (wl_plan_from_program(program, &plan)) {
            FAIL("lowering failed"); continue;
        }
        uint32_t si, ri;
        bool valid = find_relation(plan, "output", &si, &ri);
        if (valid) {
            const wl_plan_body_t *body = &plan->strata[si].bodies[ri];
            valid = body->op_count > 2 &&
                body->ops[0].op == WL_PLAN_OP_VARIABLE &&
                body->ops[body->op_count - 1].op == WL_PLAN_OP_CONSOLIDATE &&
                has_op(body->ops, body->op_count,
                    anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN);
        }
        wl_plan_free(plan);
        if (!valid) {
            FAIL("preserved full existence body missing"); continue;
        }
        ++tests_passed;
        printf(" ... PASS\n");
    }
}

/* COUNT describes its input domain separately from integer cardinalities,
 * including when the backend clones a lowered aggregate after a join rewrite. */
static void
test_count_result_domains(void)
{
    const char *sources[] = {
        ".decl input(g:int64,v:float)\n.decl output(g:int64,n:int64)\n"
        "output(g,count(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl b(g:int64)\n.decl c(g:int64)\n"
        ".decl output(g:int64,n:int64)\n"
        "output(g,count(v)) :- input(g,v),b(g),c(g).\n"
    };
    for (unsigned rewritten = 0; rewritten < 2; ++rewritten) {
        ++tests_run;
        printf("  COUNT operand/result domains, rewrite=%u", rewritten);
        wl_plan_t *plan = build(sources[rewritten]);
        if (!plan) {
            FAIL("COUNT plan generation failed"); continue;
        }
        uint32_t si, ri;
        const char *why = NULL;
        if (!find_relation(plan, "output", &si, &ri)) why = "output missing";
        else {
            const wl_plan_relation_t *rp = &plan->strata[si].relations[ri];
            const wl_plan_body_t *body = &plan->strata[si].bodies[ri];
            if (!body->op_count || body->ops[0].op != WL_PLAN_OP_VARIABLE ||
                body->ops[body->op_count - 1].op != WL_PLAN_OP_CONSOLIDATE)
                why = "full preserved body missing";
            for (unsigned variant = 0; variant < 2; ++variant) {
                const wl_plan_op_t *ops = variant ? rp->ops : body->ops;
                uint32_t count = variant ? rp->op_count : body->op_count;
                unsigned reductions = 0;
                for (uint32_t i = 0; i < count; ++i)
                    if (ops[i].op == WL_PLAN_OP_REDUCE) {
                        ++reductions;
                        if (ops[i].agg_fn != WIRELOG_AGG_COUNT ||
                            ops[i].agg_operand_type !=
                            WL_PLAN_AGG_OPERAND_FLOAT ||
                            ops[i].agg_result_type !=
                            WL_PLAN_AGG_OPERAND_SCALAR)
                            why = "COUNT operand/result descriptor changed";
                    }
                if (reductions != 1) why = "actual COUNT missing";
            }
            if (rewritten && !has_op(rp->ops, rp->op_count, WL_PLAN_OP_LFTJ))
                why = "join clone path not reached";
        }
        wl_plan_free(plan);
        if (why) {
            FAIL(why); continue;
        }
        ++tests_passed; printf(" ... PASS\n");
    }
}

int
main(void)
{
    test_count_result_domains();
    test_existence_bodies();
    printf("test_plan_body_ops (Issue #2114)\n");
    test_shape_a();
    test_shape_b();
    test_shape_c();
    test_chain();
    test_arithmetic_head();
    test_rewrites_still_apply();
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
