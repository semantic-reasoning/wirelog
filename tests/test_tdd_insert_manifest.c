/*
 * test_tdd_insert_manifest.c - Issue #2114: insertion bindings over a body.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * After an insert, a stratum's new derivations are the ones that use at
 * least one new row.  The occurrence-bound evaluator finds them by running
 * each rule once per occurrence that can hold such a row, with that
 * occurrence bound to the new rows (DELTA) and every other occurrence bound
 * to the whole relation (FULL):
 *   - round 0 drives from occurrences of the changed relations;
 *   - every later round drives from occurrences of the stratum's own heads.
 * wl_columnar_eval_tdd_plan_insert_bindings() lists those instances over
 * the body the plan kept as lowered (wl_plan_stratum_t.bodies).
 *
 * The expected instances below are counted from the Datalog source: one per
 * (rule, driving occurrence).
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <errno.h>
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

static wl_plan_t *
build(const char *src, bool passes)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    wl_plan_t *plan = NULL;

    if (!prog)
        return NULL;
    plan_fixture_hold(prog);
    if (passes) {
        wl_fusion_apply(prog, NULL);
        wl_jpp_apply(prog, NULL);
        wl_sip_apply(prog, NULL);
    }
    if (wl_plan_from_program(prog, &plan) != 0)
        return NULL;
    return plan;
}

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

/* What one round of bindings for @head should hold: @instances instances,
 * and @per[d] of them driven by an occurrence of @driver[d]. */
typedef struct {
    const char *driver;
    uint32_t instances;
} drive_t;

static char message[256];

/* Every instance has exactly one DELTA read, which is its driver; every
 * other read is FULL or, for a SEMIJOIN, PREFILTER; the driver of round 0
 * names a changed relation and the driver of a later round names a head of
 * the stratum. */
static const char *
check_shape(const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *m, const char *const *changed,
    uint32_t nchanged, bool round0)
{
    for (uint32_t s = 0; s < m->slice_count; s++) {
        const wl_columnar_eval_tdd_plan_slice_t *slice = &m->slices[s];
        uint32_t deltas = 0;
        if (slice->seed || slice->inactive || slice->read_count == 0)
            return "instance is a seed, inactive or reads nothing";
        if (slice->driver < slice->start
            || slice->driver >= slice->start + slice->count)
            return "instance driver lies outside its rule slice";
        /* The reads are exactly the slice's operands, in order: a missing
         * FULL read would leave an occurrence unbound. */
        uint32_t nread = 0;
        for (uint32_t i = slice->start; i < slice->start + slice->count; i++) {
            const wl_plan_op_t *op = &m->owner_ops[i];
            const char *name = op->op == WL_PLAN_OP_VARIABLE
                ? op->relation_name
                : op->op == WL_PLAN_OP_JOIN || op->op == WL_PLAN_OP_SEMIJOIN
                ? op->right_relation : NULL;
            if (!name)
                continue;
            if (nread >= slice->read_count)
                return "instance lists fewer reads than its slice has operands";
            const wl_columnar_eval_tdd_plan_read_t *read =
                &m->reads[slice->read_start + nread++];
            wl_columnar_eval_tdd_plan_read_kind_t kind = i == slice->driver
                ? WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
                : op->op == WL_PLAN_OP_SEMIJOIN
                ? WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER
                : WL_COLUMNAR_EVAL_TDD_PLAN_FULL;
            if (read->op_index != i || read->source_index != i - slice->start
                || !read->relation_name || strcmp(read->relation_name, name)
                || read->right_operand != (op->op != WL_PLAN_OP_VARIABLE)
                || read->kind != kind)
                return "a read does not match the operand it binds";
        }
        if (nread != slice->read_count)
            return "instance lists more reads than its slice has operands";
        for (uint32_t r = 0; r < slice->read_count; r++) {
            const wl_columnar_eval_tdd_plan_read_t *read =
                &m->reads[slice->read_start + r];
            const wl_plan_op_t *op = &m->owner_ops[read->op_index];
            if (read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA) {
                deltas++;
                if (read->op_index != slice->driver)
                    return "a DELTA read is not the instance driver";
                if (op->op == WL_PLAN_OP_SEMIJOIN)
                    return "a SEMIJOIN drives an instance";
                bool ok = false;
                if (round0) {
                    for (uint32_t c = 0; c < nchanged; c++)
                        ok |= strcmp(read->relation_name, changed[c]) == 0;
                } else {
                    for (uint32_t h = 0; h < sp->relation_count; h++)
                        ok |= strcmp(read->relation_name,
                                sp->relations[h].name) == 0;
                }
                if (!ok)
                    return "driver reads neither a changed relation nor a head";
            } else if (read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER) {
                if (op->op != WL_PLAN_OP_SEMIJOIN)
                    return "a PREFILTER read is not a SEMIJOIN";
            } else if (op->op == WL_PLAN_OP_SEMIJOIN) {
                return "a SEMIJOIN read is not PREFILTER";
            }
        }
        if (deltas != 1)
            return "instance does not have exactly one DELTA read";
    }
    return NULL;
}

static const char *
check_round(const wl_plan_t *plan, const char *head,
    const char *const *changed, uint32_t nchanged, bool round0,
    uint32_t want_instances, const drive_t *per, uint32_t nper)
{
    uint32_t si, ri;
    wl_columnar_eval_tdd_plan_manifest_t m;
    const char *why = NULL;

    if (!find_relation(plan, head, &si, &ri))
        return "head not in the plan";
    memset(&m, 0, sizeof(m));
    int rc = wl_columnar_eval_tdd_plan_insert_bindings(&plan->strata[si], ri,
            changed, nchanged, round0, &m);
    if (rc != 0) {
        snprintf(message, sizeof(message), "%s round %s: rc %d", head,
            round0 ? "0" : "n", rc);
        return message;
    }
    if (m.form != WL_COLUMNAR_EVAL_TDD_PLAN_INSERT
        || m.owner_ops != plan->strata[si].bodies[ri].ops
        || m.owner_op_count != plan->strata[si].bodies[ri].op_count) {
        why = "manifest is not bound to the body";
        goto done;
    }
    if (m.slice_count != want_instances) {
        snprintf(message, sizeof(message), "%s round %s: %u instances, want %u",
            head, round0 ? "0" : "n", m.slice_count, want_instances);
        why = message;
        goto done;
    }
    for (uint32_t p = 0; p < nper; p++) {
        uint32_t seen = 0;
        for (uint32_t s = 0; s < m.slice_count; s++) {
            const wl_plan_op_t *op = &m.owner_ops[m.slices[s].driver];
            const char *name = op->op == WL_PLAN_OP_VARIABLE
                ? op->relation_name : op->right_relation;
            if (name && strcmp(name, per[p].driver) == 0)
                seen++;
        }
        if (seen != per[p].instances) {
            snprintf(message, sizeof(message),
                "%s round %s: %u instances driven by %s, want %u", head,
                round0 ? "0" : "n", seen, per[p].driver, per[p].instances);
            why = message;
            goto done;
        }
    }
    why = check_shape(&plan->strata[si], &m, changed, nchanged, round0);
done:
    wl_columnar_eval_tdd_plan_bindings_free(&m);
    return why;
}

#define BEGIN(name)                             \
        do {                                    \
            tests_run++;                        \
            printf("  [%d] %s", tests_run, (name)); \
        } while (0)

#define CHECK(expr)                \
        do {                       \
            const char *why_ = (expr); \
            if (why_) {            \
                FAIL(why_);        \
                goto done;         \
            }                      \
        } while (0)

static const char *const E[] = { "e" };

static void
test_shape_a(void)
{
    static const drive_t r0[] = { { "e", 2 } };
    static const drive_t rn[] = { { "a", 1 } };
    wl_plan_t *plan;

    BEGIN("right-recursive closure");
    plan = build(".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
            "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n", false);
    if (!plan) {
        FAIL("plan");
        return;
    }
    /* Round 0: both rules read e once.  Later rounds: only the recursive
     * rule reads a. */
    CHECK(check_round(plan, "a", E, 1, true, 2, r0, 1));
    CHECK(check_round(plan, "a", E, 1, false, 1, rn, 1));
    tests_passed++;
    printf(" ... PASS\n");
done:
    wl_plan_free(plan);
}

/* Shape B round 0, written out: e(x, y) is a VARIABLE and e(y, z) the right
 * operand of a JOIN; one instance reads the first as DELTA and the second
 * FULL, the other the reverse. */
static const char *
check_shape_b_reads(const wl_plan_t *plan)
{
    uint32_t si, ri;
    wl_columnar_eval_tdd_plan_manifest_t m;
    bool left_driven = false, right_driven = false;
    const char *why = NULL;

    if (!find_relation(plan, "a", &si, &ri))
        return "a not in the plan";
    if (wl_columnar_eval_tdd_plan_insert_bindings(&plan->strata[si], ri, E, 1,
        true, &m) != 0)
        return "round 0 bindings failed";
    for (uint32_t s = 0; s < m.slice_count && !why; s++) {
        const wl_columnar_eval_tdd_plan_slice_t *slice = &m.slices[s];
        const wl_columnar_eval_tdd_plan_read_t *rd =
            &m.reads[slice->read_start];
        if (slice->read_count != 2) {
            why = "an instance of e(x, y), e(y, z) does not read twice";
            break;
        }
        if (strcmp(rd[0].relation_name, "e") || rd[0].right_operand
            || rd[0].source_index != 0 || strcmp(rd[1].relation_name, "e")
            || !rd[1].right_operand || rd[1].source_index == 0) {
            why = "the reads are not e as VARIABLE then e as JOIN operand";
            break;
        }
        if (rd[0].kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
            && rd[1].kind == WL_COLUMNAR_EVAL_TDD_PLAN_FULL)
            left_driven = true;
        else if (rd[0].kind == WL_COLUMNAR_EVAL_TDD_PLAN_FULL
            && rd[1].kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA)
            right_driven = true;
        else
            why = "an instance is neither DELTA,FULL nor FULL,DELTA";
    }
    if (!why && !(left_driven && right_driven))
        why = "both occurrences of e do not each drive an instance";
    wl_columnar_eval_tdd_plan_bindings_free(&m);
    return why;
}

static void
test_shape_b(void)
{
    static const drive_t r0[] = { { "e", 2 } };
    static const drive_t rn[] = { { "a", 1 } };
    wl_plan_t *plan;

    BEGIN("base rule joining its input twice");
    plan = build(".decl e(x: int64, y: int64)\n.decl s(x: int64, y: int64)\n"
            ".decl a(x: int64, y: int64)\n"
            "a(x, z) :- e(x, y), e(y, z).\na(x, z) :- a(x, y), s(y, z).\n",
            false);
    if (!plan) {
        FAIL("plan");
        return;
    }
    /* Round 0: one instance per occurrence of e in the first rule. */
    CHECK(check_round(plan, "a", E, 1, true, 2, r0, 1));
    CHECK(check_shape_b_reads(plan));
    CHECK(check_round(plan, "a", E, 1, false, 1, rn, 1));
    tests_passed++;
    printf(" ... PASS\n");
done:
    wl_plan_free(plan);
}

static void
test_shape_c(void)
{
    static const drive_t r0[] = { { "e", 1 } };
    static const drive_t rn[] = { { "a", 2 } };
    wl_plan_t *plan;

    BEGIN("rule with two recursive atoms");
    plan = build(".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
            ".decl a(x: int64, y: int64)\n"
            "a(x, y) :- f(x, y).\na(x, z) :- e(x, y), a(y, w), a(w, z).\n",
            false);
    if (!plan) {
        FAIL("plan");
        return;
    }
    /* The base rule reads f, which did not change. */
    CHECK(check_round(plan, "a", E, 1, true, 1, r0, 1));
    CHECK(check_round(plan, "a", E, 1, false, 2, rn, 1));
    tests_passed++;
    printf(" ... PASS\n");
done:
    wl_plan_free(plan);
}

/* The number of PREFILTER reads in @head's bindings for the round, or
 * UINT32_MAX when the call fails. */
static uint32_t
prefilter_reads(const wl_plan_t *plan, const char *head,
    const char *const *changed, uint32_t nchanged, bool round0)
{
    uint32_t si, ri, n = 0;
    wl_columnar_eval_tdd_plan_manifest_t m;

    if (!find_relation(plan, head, &si, &ri))
        return UINT32_MAX;
    if (wl_columnar_eval_tdd_plan_insert_bindings(&plan->strata[si], ri,
        changed, nchanged, round0, &m) != 0)
        return UINT32_MAX;
    for (uint32_t r = 0; r < m.read_count; r++)
        n += m.reads[r].kind == WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER;
    wl_columnar_eval_tdd_plan_bindings_free(&m);
    return n;
}

/* CSPA after the passes the session pipeline applies, SIP among them.  All
 * three relations share one recursive stratum. */
static void
test_cspa(void)
{
    static const char *const assign[] = { "assign" };
    static const drive_t vf0[] = { { "assign", 4 } };
    static const drive_t vfn[] = { { "valueFlow", 2 }, { "memoryAlias", 1 } };
    static const drive_t ma0[] = { { "assign", 2 } };
    static const drive_t man[] = { { "valueAlias", 1 } };
    static const drive_t van[] = { { "valueFlow", 4 }, { "memoryAlias", 1 } };
    wl_plan_t *plan;

    BEGIN("CSPA with fusion, JPP and SIP");
    plan = build(
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
        "valueFlow(w, y).\n", true);
    if (!plan) {
        FAIL("plan");
        return;
    }
    CHECK(check_round(plan, "valueFlow", assign, 1, true, 4, vf0, 1));
    CHECK(check_round(plan, "valueFlow", assign, 1, false, 3, vfn, 2));
    CHECK(check_round(plan, "memoryAlias", assign, 1, true, 2, ma0, 1));
    CHECK(check_round(plan, "memoryAlias", assign, 1, false, 1, man, 1));
    /* valueAlias reads no assign: round 0 has nothing to drive. */
    CHECK(check_round(plan, "valueAlias", assign, 1, true, 0, NULL, 0));
    CHECK(check_round(plan, "valueAlias", assign, 1, false, 5, van, 2));
    /* SIP put a SEMIJOIN prefilter before a join in memoryAlias and in
     * valueAlias; without it this case would not cover PREFILTER reads. */
    {
        uint32_t ma = prefilter_reads(plan, "memoryAlias", assign, 1, false);
        uint32_t va = prefilter_reads(plan, "valueAlias", assign, 1, false);
        if (ma == 0 || ma == UINT32_MAX || va == 0 || va == UINT32_MAX) {
            FAIL("the CSPA bindings hold no PREFILTER read");
            goto done;
        }
    }
    tests_passed++;
    printf(" ... PASS\n");
done:
    wl_plan_free(plan);
}

/* A rule the binder cannot bind: the call fails and leaves @m empty. */
static const char *
expect_refusal(const wl_plan_t *plan, const char *head, int want_rc,
    const char *const *changed, uint32_t nchanged, bool round0)
{
    uint32_t si, ri;
    wl_columnar_eval_tdd_plan_manifest_t m;

    if (!find_relation(plan, head, &si, &ri))
        return "head not in the plan";
    memset(&m, 0xa5, sizeof(m));
    int rc = wl_columnar_eval_tdd_plan_insert_bindings(&plan->strata[si], ri,
            changed, nchanged, round0, &m);
    if (rc != want_rc) {
        snprintf(message, sizeof(message), "%s: rc %d, want %d", head, rc,
            want_rc);
        return message;
    }
    if (m.slices || m.reads || m.slice_count || m.read_count)
        return "a refused call left a partial manifest";
    return NULL;
}

/* A one-relation stratum named h whose body is @ops, which the binder must
 * refuse with ENOTSUP in round 0 with e changed, leaving the manifest
 * empty.  These bodies are built by hand because the planner does not
 * produce them today. */
static const char *
expect_body_refused(const wl_plan_op_t *ops, uint32_t op_count)
{
    wl_plan_relation_t rel = { .name = "h" };
    wl_plan_body_t body = { .ops = ops, .op_count = op_count };
    wl_plan_stratum_t sp = { .relations = &rel, .relation_count = 1,
                             .bodies = &body };
    wl_columnar_eval_tdd_plan_manifest_t m;

    memset(&m, 0xa5, sizeof(m));
    int rc = wl_columnar_eval_tdd_plan_insert_bindings(&sp, 0, E, 1, true, &m);
    if (rc != ENOTSUP) {
        snprintf(message, sizeof(message), "rc %d, want ENOTSUP", rc);
        return message;
    }
    if (m.slices || m.reads || m.slice_count || m.read_count)
        return "a refused call left a partial manifest";
    return NULL;
}

/* Bodies the binder cannot split into rule slices it can bind. */
static void
test_refused_bodies(void)
{
    static const char *const x[] = { "x" };
    static int opaque;
    /* A MAP after the CONCAT applies to the union, not to one rule. */
    const wl_plan_op_t after_union[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "e" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "f" },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_MAP },
        { .op = WL_PLAN_OP_CONSOLIDATE },
    };
    /* A SEMIJOIN not followed by a JOIN over the same relation and key is
     * an existential test of its own, not a redundant prefilter. */
    const wl_plan_op_t lone_semijoin[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "e" },
        { .op = WL_PLAN_OP_SEMIJOIN, .right_relation = "e", .left_keys = x,
          .right_keys = x, .key_count = 1 },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "f", .left_keys = x,
          .right_keys = x, .key_count = 1 },
        { .op = WL_PLAN_OP_CONSOLIDATE },
    };
    /* Backend metadata belongs to rewritten operators, never to a body. */
    const wl_plan_op_t with_opaque[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "e",
          .opaque_data = &opaque },
        { .op = WL_PLAN_OP_CONSOLIDATE },
    };
    /* Two rules never combined by a CONCAT. */
    const wl_plan_op_t unbalanced[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "e" },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "f" },
        { .op = WL_PLAN_OP_MAP },
    };
    const char *why;

    BEGIN("bodies it cannot split into bindable rules are refused");
    if ((why = expect_body_refused(after_union, 5)) != NULL
        || (why = expect_body_refused(lone_semijoin, 4)) != NULL
        || (why = expect_body_refused(with_opaque, 2)) != NULL) {
        FAIL(why);
        return;
    }
    {
        /* An unbalanced body is malformed rather than unsupported. */
        wl_plan_relation_t rel = { .name = "h" };
        wl_plan_body_t body = { .ops = unbalanced, .op_count = 3 };
        wl_plan_stratum_t sp = { .relations = &rel, .relation_count = 1,
                                 .bodies = &body };
        wl_columnar_eval_tdd_plan_manifest_t m;
        if (wl_columnar_eval_tdd_plan_insert_bindings(&sp, 0, E, 1, true, &m)
            != EINVAL || m.slices || m.reads) {
            FAIL("an unbalanced body was not refused with EINVAL");
            return;
        }
    }
    tests_passed++;
    printf(" ... PASS\n");
}

static void
test_refusals(void)
{
    wl_plan_t *neg = NULL, *agg = NULL, *bad = NULL;
    wl_plan_stratum_t hand;

    BEGIN("negation, aggregates, hand-built strata and heads are refused");
    neg = build(".decl e(x: int64)\n.decl s(x: int64)\n.decl p(x: int64)\n"
            "p(x) :- s(x), !e(x).\n", false);
    agg = build(".decl e(x: int64)\n.decl n(k: int64)\n"
            "n(sum(x)) :- e(x).\n", false);
    bad = build(".decl e(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
            "a(x, y) :- e(x, y).\na(x, z) :- e(x, y), a(y, z).\n", false);
    if (!neg || !agg || !bad) {
        FAIL("plan");
        goto done;
    }
    CHECK(expect_refusal(neg, "p", ENOTSUP, E, 1, true));
    CHECK(expect_refusal(agg, "n", ENOTSUP, E, 1, true));
    {
        /* A head of the stratum cannot be a changed relation in round 0. */
        static const char *const head[] = { "a" };
        CHECK(expect_refusal(bad, "a", EINVAL, head, 1, true));
    }
    {
        /* A stratum without recorded bodies has nothing to bind. */
        uint32_t si, ri;
        wl_columnar_eval_tdd_plan_manifest_t m;
        if (!find_relation(bad, "a", &si, &ri)) {
            FAIL("a not in the plan");
            goto done;
        }
        hand = bad->strata[si];
        hand.bodies = NULL;
        memset(&m, 0, sizeof(m));
        if (wl_columnar_eval_tdd_plan_insert_bindings(&hand, ri, E, 1, true,
            &m) != ENOTSUP || m.slices || m.reads) {
            FAIL("a stratum without bodies was not refused");
            goto done;
        }
    }
    tests_passed++;
    printf(" ... PASS\n");
done:
    wl_plan_free(neg);
    wl_plan_free(agg);
    wl_plan_free(bad);
}

int
main(void)
{
    printf("test_tdd_insert_manifest (Issue #2114)\n");
    test_shape_a();
    test_shape_b();
    test_shape_c();
    test_cspa();
    test_refusals();
    test_refused_bodies();
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
