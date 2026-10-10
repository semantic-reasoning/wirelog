/*
 * test_tdd_partition_metadata.c - Recursive strata keep their model at
 * every worker count, with and without the planning passes (Issue #2118)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Each case evaluates one program over fixed data at W=1, 2 and 4, once
 * from a plan built without the fusion, JPP and SIP passes and once with
 * them, and requires every run to succeed with the same model as the
 * optimised plan at W=1.  Every case except antijoin_negated_edb and
 * bdx_joined_base_rule failed on the planner before Issue #2118, with
 * EINVAL or with a model that differed from W=1.  Those two, and
 * bdx_filtered_base_rule, guard against partitioning rules that were
 * tried while fixing it and lost rows.  A case marked `parallel` also
 * checks that the stratum really ran on more than one worker through
 * hybrid initialisation, so a sequential fallback cannot make it pass.
 * With the passes, most of these plans are evaluated sequentially at any
 * worker count; only the mutual-recursion case requires them to run in
 * parallel.
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _MSC_VER
static int
setenv(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, value);
}
#endif

/* Order-independent digest of a snapshot, as in bench_flowlog. */
struct digest {
    int64_t count;
    uint64_t sum;
};

static uint64_t
mix64(uint64_t x)
{
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    x ^= x >> 31;
    return x;
}

static void
digest_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct digest *d = (struct digest *)user_data;
    uint64_t h = UINT64_C(0xcbf29ce484222325);
    for (const char *c = relation; *c; c++)
        h = (h ^ (uint64_t)(unsigned char)*c) * UINT64_C(0x100000001b3);
    h = mix64(h ^ ncols);
    for (uint32_t i = 0; i < ncols; i++)
        h = mix64(h ^ (uint64_t)row[i]);
    d->sum += h;
    d->count++;
}

struct test_case {
    const char *name;
    const char *src;
    uint32_t rows;           /* random rows per EDB; 0 = facts in src */
    uint32_t nodes;
    uint64_t seed;
    /* Which plans must run on more than one worker through hybrid
     * initialisation: bit 0 = without passes, bit 1 = with passes. */
    unsigned parallel;
};

static uint64_t
next_rand(uint64_t *state)
{
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static int
evaluate(const struct test_case *tc, bool passes, uint32_t workers,
    struct digest *out, const char **strategy, uint32_t *selected)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(tc->src, &err);
    if (!prog)
        return -1;
    if (passes) {
        wl_fusion_apply(prog, NULL);
        wl_jpp_apply(prog, NULL);
        wl_sip_apply(prog, NULL);
    }
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wl_session_t *sess = NULL;
    if (rc == 0)
        rc = wl_session_create(wl_backend_columnar(), plan, workers, &sess);
    if (rc == 0)
        rc = wl_session_load_facts(sess, prog);
    uint64_t state = tc->seed;
    for (uint32_t e = 0; rc == 0 && tc->rows > 0 && e < plan->edb_count;
        e++) {
        uint32_t width = plan->edb_declared_width[e];
        if (width == 0 || width > 4)
            continue;
        for (uint32_t i = 0; rc == 0 && i < tc->rows; i++) {
            int64_t row[4];
            for (uint32_t c = 0; c < width; c++)
                row[c] = (int64_t)(next_rand(&state) % tc->nodes);
            rc = wl_session_insert(sess, plan->edb_relations[e], row, 1,
                    width);
        }
    }
    memset(out, 0, sizeof(*out));
    if (rc == 0)
        rc = wl_session_snapshot(sess, digest_cb, out);
    if (sess) {
        *strategy = COL_SESSION(sess)->tdd_audit.strategy;
        *selected = COL_SESSION(sess)->tdd_audit.selected_workers;
        wl_session_destroy(sess);
    }
    if (plan)
        wl_plan_free(plan);
    /* The plan borrows the program's intern table (Issue #1431). */
    plan_fixture_hold(prog);
    return rc;
}

static bool
hybrid_strategy(const char *strategy)
{
    return strategy
           && (strcmp(strategy, "owner") == 0 || strcmp(strategy, "bdx") == 0
           || strcmp(strategy, "aligned") == 0);
}

static int
run_case(const struct test_case *tc)
{
    int failures = 0;
    struct digest ref;
    const char *strategy = NULL;
    uint32_t selected = 0;
    int rc = evaluate(tc, true, 1, &ref, &strategy, &selected);
    if (rc != 0 || ref.count == 0) {
        printf("FAIL %s: reference rc=%d rows=%lld\n", tc->name, rc,
            (long long)ref.count);
        return 1;
    }
    for (int p = 0; p <= 1; p++) {
        for (uint32_t w = 1; w <= 4; w *= 2) {
            struct digest got;
            strategy = NULL;
            selected = 0;
            rc = evaluate(tc, p == 1, w, &got, &strategy, &selected);
            bool same = rc == 0 && got.count == ref.count
                && got.sum == ref.sum;
            bool want_parallel = w > 1 && (tc->parallel & (1u << p));
            bool parallel_ok = !want_parallel
                || (selected > 1 && hybrid_strategy(strategy));
            if (!same || !parallel_ok) {
                printf("FAIL %s passes=%d W=%u: rc=%d rows=%lld/%lld %s "
                    "strategy=%s workers=%u\n",
                    tc->name, p, w, rc, (long long)got.count,
                    (long long)ref.count, same ? "match" : "MISMATCH",
                    strategy ? strategy : "-", selected);
                failures++;
            }
        }
    }
    if (failures == 0)
        printf("ok   %s (%lld rows)\n", tc->name, (long long)ref.count);
    return failures;
}

#define DECL_EFA                                                         \
        ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n" \
        ".decl a(x: int64, y: int64)\n"

#define FACTS_ANTIJOIN \
        "e(0, 100).\n" "e(1, 101).\n" "e(2, 102).\n" "e(3, 103).\n" \
        "e(4, 104).\n" "e(5, 105).\n" "e(6, 106).\n" "e(7, 107).\n" \
        "e(8, 108).\n" "e(9, 109).\n" "e(10, 110).\n" \
        "e(11, 111).\n" "e(12, 112).\n" "e(13, 113).\n" \
        "e(14, 114).\n" "e(15, 115).\n" "e(16, 116).\n" \
        "e(17, 117).\n" "e(18, 118).\n" "e(19, 119).\n" \
        "n(0, 100).\n" "n(2, 102).\n" "n(4, 104).\n" "n(6, 106).\n" \
        "n(8, 108).\n" "n(10, 110).\n" "n(12, 112).\n" \
        "n(14, 114).\n" "n(16, 116).\n" "n(18, 118).\n"

static const struct test_case cases[] = {
    /* The issue's program and facts. */
    { "issue_e_a_a",
      DECL_EFA "a(x, y) :- f(x, y).\n"
      "a(x, z) :- e(x, y), a(y, w), a(w, z).\n"
      "f(2, 3).\nf(3, 4).\ne(9, 9).\ne(1, 2).\n",
      0, 1, 0, 1u },
    { "e_a_a",
      DECL_EFA "a(x, y) :- f(x, y).\n"
      "a(x, z) :- e(x, y), a(y, w), a(w, z).\n",
      60, 40, 1, 1u },
    { "e_f_a",
      DECL_EFA "a(x, y) :- f(x, y).\n"
      "a(x, z) :- e(x, y), f(y, w), a(w, z).\n",
      60, 40, 1, 1u },
    { "a_e_a",
      DECL_EFA "a(x, y) :- f(x, y).\n"
      "a(x, z) :- a(x, y), e(y, w), a(w, z).\n",
      60, 40, 1, 1u },
    /* Two relations of one stratum co-partition one EDB through different
     * columns; this failed with and without the passes. */
    { "mutual_e_two_keys",
      DECL_EFA ".decl b(x: int64, y: int64)\n"
      "a(x, y) :- f(x, y).\na(x, z) :- e(x, y), a(y, z).\n"
      "b(x, y) :- a(y, x).\nb(x, z) :- e(z, x), b(z, y).\n"
      "a(x, y) :- b(x, y).\n",
      60, 40, 1, 3u },
    /* The negated relation is read whole by every row; partitioning it
     * would let rows through whose blocking tuple sits on another
     * worker. */
    { "antijoin_negated_edb",
      ".decl e(x: int64, y: int64)\n.decl n(x: int64, y: int64)\n"
      ".decl a(x: int64, y: int64)\n"
      "a(x, y) :- e(x, y), !n(x, y).\na(x, y) :- e(y, x).\n"
      "a(x, z) :- a(x, y), a(y, z).\n"
      FACTS_ANTIJOIN,
      0, 1, 0, 1u },
    /* A relation joined with an EDB through another relation of the
     * stratum partitioned by the same columns in a different order; this
     * failed with and without the passes. */
    { "cross_relation_key_order",
      ".decl e(x: int64, y: int64)\n.decl f(x: int64, y: int64)\n"
      ".decl g(x: int64, y: int64)\n.decl a(x: int64, y: int64)\n"
      ".decl b(x: int64, y: int64)\n"
      "a(x, y) :- f(x, y).\na(x, y) :- e(x, y), b(x, y).\n"
      "b(u, v) :- g(u, v), b(v, u).\nb(u, v) :- a(u, v).\n",
      60, 20, 1, 1u },
    /* An EDB read only by a rule body that reads no relation of the
     * stratum must stay replicated: partitioning it lost rows under the
     * bdx strategy, whether the body filters it or joins it. */
    { "bdx_filtered_base_rule",
      ".decl e(c0: int64, c1: int64)\n.decl g(c0: int64, c1: int64)\n"
      ".decl h(c0: int64, c1: int64)\n.decl a(c0: int64, c1: int64)\n"
      "a(x, y) :- g(x, y), x < 2.\n"
      "a(x0, x3) :- a(x0, x1), h(x1, x2), a(x3, x2).\n",
      80, 25, 3, 1u },
    { "bdx_joined_base_rule",
      ".decl e0(c0: int64, c1: int64)\n"
      ".decl e1(c0: int64, c1: int64, c2: int64)\n"
      ".decl e2(c0: int64, c1: int64)\n.decl a(c0: int64, c1: int64)\n"
      "a(x2, x2) :- e2(0, x1), e1(x1, x2, y1).\n"
      "a(x0, x3) :- a(x0, x1), e2(x1, x2), a(x3, x2).\n",
      60, 20, 3, 1u },
    /* The relation's EXCHANGE key came from another relation's columns
     * or from an earlier join's result, past the relation's own width:
     * EINVAL at W >= 2, with and without the passes. */
    { "key_from_wider_relation",
      ".decl e0(c0: int64, c1: int64)\n"
      ".decl a(c0: int64, c1: int64, c2: int64)\n"
      ".decl b(c0: int64, c1: int64)\n"
      "a(x2, x0, x0) :- e0(x0, x1), e0(x1, x2), !e0(x2, x1).\n"
      "b(x2, x0) :- a(x0, x1, 2), a(y1, x2, x1), !e0(y1, x2).\n"
      "a(x2, x0, x2) :- b(x0, x1), e0(x2, x1).\n",
      80, 25, 1, 3u },
    { "key_on_unary_relation",
      ".decl e0(c0: int64, c1: int64)\n.decl e1(c0: int64)\n"
      ".decl e2(c0: int64, c1: int64)\n.decl a(c0: int64, c1: int64)\n"
      ".decl b(c0: int64)\n"
      "a(x2, x0) :- e2(x0, x1), e1(x2), x0 < x1.\n"
      "a(x3, x3) :- e1(x1), e1(x2), e2(x2, x3).\n"
      "b(x2) :- e1(x0), e2(x2, x1), !e0(x0, x2).\n"
      "b(x2) :- b(x1), a(x2, x1), x2 < x1.\n"
      "a(x1, x2) :- e0(x0, x1), b(x1), e1(x2), !e2(x0, x0).\n",
      80, 25, 1, 3u },
    { "key_from_earlier_join",
      DECL_EFA ".decl g(x: int64, y: int64)\n.decl b(x: int64, y: int64)\n"
      "a(x, y) :- f(x, y).\na(x, z) :- a(x, y), b(y, z).\n"
      "b(x, y) :- a(x, y).\n"
      "b(v0, v3) :- b(v1, v0), e(v1, v2), g(v3, v2).\n",
      60, 20, 1, 1u },
    { "andersen",
      ".decl addressOf(v: int64, o: int64)\n.decl assign(v1: int64, v2: int64)\n"
      ".decl load(v1: int64, v2: int64)\n.decl store(v1: int64, v2: int64)\n"
      ".decl pointsTo(v: int64, o: int64)\n"
      "pointsTo(v, o) :- addressOf(v, o).\n"
      "pointsTo(v1, o) :- assign(v1, v2), pointsTo(v2, o).\n"
      "pointsTo(v1, o) :- load(v1, v2), pointsTo(v2, p), pointsTo(p, o).\n"
      "pointsTo(v1, o) :- store(v1, v2), pointsTo(v1, p), "
      "pointsTo(v2, o).\n",
      60, 40, 6, 1u },
};

int
main(void)
{
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
        failures += run_case(&cases[i]);
    printf("%s: %d failure(s)\n", failures ? "FAILED" : "PASSED", failures);
    return failures ? 1 : 0;
}
