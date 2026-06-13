/*
 * test_issue914_nonrec_after_recursive.c - Tests for issue #914
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Regression test for issue #914: sess->current_iteration leaks across strata.
 *
 * A recursive stratum's semi-naive loop sets sess->current_iteration = eff_iter
 * and never resets it to 0. When a NON-recursive stratum runs AFTER a recursive
 * one, it evaluates with a stale current_iteration > 0, which wrongly triggers
 * the "base-case EDB VARIABLE skip" optimization (col_eval_relation_plan). The
 * affected rule's body becomes empty and the head derives 0 rows.
 *
 * The #914 program: a non-recursive rule (requires_review) reads a static EDB
 * (relation) that is NOT followed by a JOIN and is NOT used as a JOIN right
 * relation. It is stratified after the recursive path/edge SCC, so the stale
 * iteration leaks into it and drops its head tuples.
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/intern.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
        do {                                                \
            test_count++;                                   \
            printf("TEST %d: %s ... ", test_count, (name)); \
        } while (0)

#define PASS()            \
        do {                  \
            pass_count++;     \
            printf("PASS\n"); \
        } while (0)

#define FAIL(msg)                    \
        do {                             \
            fail_count++;                \
            printf("FAIL: %s\n", (msg)); \
            return;                      \
        } while (0)

#define ASSERT(cond, msg) \
        do {                  \
            if (!(cond))      \
            FAIL(msg);    \
        } while (0)

/* ----------------------------------------------------------------
 * Helpers
 * ---------------------------------------------------------------- */

struct rel_ctx {
    const char *target;
    int64_t count;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct rel_ctx *ctx = (struct rel_ctx *)user_data;
    if (relation && ctx->target && strcmp(relation, ctx->target) == 0)
        ctx->count++;
    (void)row;
    (void)ncols;
}

/* Delta collector: counts +diffs for requires_review("a","r") rows whose
 * interned column IDs match the expected (entity, reason) pair. */
struct delta_ctx {
    int64_t entity_id;
    int64_t reason_id;
    int64_t count; /* net +1 deltas matching requires_review("a","r") */
};

static void
delta_cb(const char *relation, const int64_t *row, uint32_t ncols, int32_t diff,
    void *user_data)
{
    struct delta_ctx *ctx = (struct delta_ctx *)user_data;
    if (!relation || strcmp(relation, "requires_review") != 0)
        return;
    if (ncols != 2 || !row)
        return;
    if (row[0] == ctx->entity_id && row[1] == ctx->reason_id)
        ctx->count += diff;
}

/* Run a fresh snapshot evaluation and return tuple count for target_rel. */
static int
run_fresh(const char *src, const char *target_rel, int64_t *out_count)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct rel_ctx ctx = { target_rel, 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_count)
        *out_count = ctx.count;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ================================================================
 * Test 1: The exact #914 program.
 *
 * A non-recursive rule (requires_review) is stratified after the
 * recursive path/edge SCC. Before the fix, the stale current_iteration
 * from the recursive stratum triggers the base-case EDB VARIABLE skip
 * on requires_review's body, dropping its single head tuple.
 *
 * Expected: requires_review == 1, edge == 1, path == 1.
 * ================================================================ */
static void
test_issue914_exact_program(void)
{
    TEST("non-recursive stratum after recursive derives head tuples (#914)");

    const char *src =
        ".decl relation(subject: symbol, rel: symbol, object: symbol)\n"
        ".decl edge(start: symbol, target: symbol)\n"
        ".decl path(start: symbol, target: symbol)\n"
        "edge(S, O) :- relation(S, R, O).\n"
        "path(S, O) :- edge(S, O).\n"
        "path(S, O) :- edge(S, M), path(M, O).\n"
        ".decl requires_review(entity: symbol, reason: symbol)\n"
        "relation(\"a\", \"contains_private_data\", \"b\").\n"
        "requires_review(X, \"r\") :- "
        "relation(X, \"contains_private_data\", O1).\n";

    int64_t rr_count = 0;
    int rc = run_fresh(src, "requires_review", &rr_count);
    ASSERT(rc == 0, "fresh eval failed (requires_review)");
    printf("(requires_review=%" PRId64 ") ", rr_count);

    int64_t edge_count = 0;
    rc = run_fresh(src, "edge", &edge_count);
    ASSERT(rc == 0, "fresh eval failed (edge)");

    int64_t path_count = 0;
    rc = run_fresh(src, "path", &path_count);
    ASSERT(rc == 0, "fresh eval failed (path)");

    /* Regression guards: these stay green before and after the fix. */
    ASSERT(edge_count == 1, "expected exactly 1 edge tuple");
    ASSERT(path_count == 1, "expected exactly 1 path tuple");

    /* The #914 assertion: FAILS before the fix (count 0), PASSES after. */
    ASSERT(rr_count == 1,
        "expected exactly 1 requires_review tuple (#914 leak)");

    PASS();
}

/* ================================================================
 * Test 2: Working variant cross-check.
 *
 * Same program minus the edge/path recursive rules. With no recursive
 * stratum running before it, requires_review must derive its tuple both
 * before and after the fix (variant that already works).
 * ================================================================ */
static void
test_issue914_no_recursion_variant(void)
{
    TEST("non-recursive stratum without preceding recursion (#914 variant)");

    const char *src =
        ".decl relation(subject: symbol, rel: symbol, object: symbol)\n"
        ".decl requires_review(entity: symbol, reason: symbol)\n"
        "relation(\"a\", \"contains_private_data\", \"b\").\n"
        "requires_review(X, \"r\") :- "
        "relation(X, \"contains_private_data\", O1).\n";

    int64_t rr_count = 0;
    int rc = run_fresh(src, "requires_review", &rr_count);
    ASSERT(rc == 0, "fresh eval failed (requires_review)");
    printf("(requires_review=%" PRId64 ") ", rr_count);

    ASSERT(rr_count == 1,
        "expected exactly 1 requires_review tuple (working variant)");

    PASS();
}

/* ================================================================
 * Test 3: Real incremental-delta surface (the originally-reported
 * --delta / PyreWire#165 path).
 *
 * Start a session on the #914 program (recursive edge/path SCC plus the
 * single-body requires_review rule) WITHOUT the relation("a",...) fact
 * preloaded. After an initial step (relation empty -> requires_review
 * derives nothing), insert relation("a","contains_private_data","b") via
 * the incremental API and run a delta step with a delta callback, mirroring
 * test_delta_propagation.c.
 *
 * The recursive path/edge stratum runs first and leaks current_iteration > 0
 * into the non-recursive requires_review stratum. Without the eval.c reset,
 * the base-case EDB VARIABLE skip drops requires_review's head and the delta
 * callback never fires +requires_review("a","r"). With the fix, the delta
 * step emits requires_review("a","r") exactly once.
 * ================================================================ */
static void
test_issue914_delta_insert(void)
{
    TEST("incremental delta insert derives non-recursive head (#914)");

    /* Same program as Test 1, but the relation("a",...) fact is NOT
     * preloaded: it is supplied later via the incremental insert API. */
    const char *src =
        ".decl relation(subject: symbol, rel: symbol, object: symbol)\n"
        ".decl edge(start: symbol, target: symbol)\n"
        ".decl path(start: symbol, target: symbol)\n"
        "edge(S, O) :- relation(S, R, O).\n"
        "path(S, O) :- edge(S, O).\n"
        "path(S, O) :- edge(S, M), path(M, O).\n"
        ".decl requires_review(entity: symbol, reason: symbol)\n"
        "requires_review(X, \"r\") :- "
        "relation(X, \"contains_private_data\", O1).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    /* Intern the fact's symbols. "contains_private_data" and "r" are already
     * interned via the rule literals; "a" and "b" appear only in the fact we
     * insert incrementally, so intern them here. The session shares the
     * program's intern table, so these IDs are valid for the insert. */
    wl_intern_t *intern =
        (wl_intern_t *)wirelog_program_get_intern(prog);
    ASSERT(intern != NULL, "intern table is NULL");
    int64_t a_id = wl_intern_put(intern, "a");
    int64_t cpd_id = wl_intern_put(intern, "contains_private_data");
    int64_t b_id = wl_intern_put(intern, "b");
    int64_t r_id = wl_intern_put(intern, "r");
    ASSERT(a_id >= 0 && cpd_id >= 0 && b_id >= 0 && r_id >= 0,
        "interning fact symbols failed");

    /* Register the delta callback BEFORE stepping so it sees +deltas. */
    struct delta_ctx dctx = { a_id, r_id, 0 };
    wl_session_set_delta_cb(sess, delta_cb, &dctx);

    /* Initial step: relation is empty, so requires_review derives nothing. */
    rc = wl_session_step(sess);
    ASSERT(rc == 0, "initial step failed");

    struct rel_ctx ctx0 = { "requires_review", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx0);
    ASSERT(rc == 0, "initial snapshot failed");
    ASSERT(ctx0.count == 0,
        "requires_review must be empty before the delta insert");

    /* Incremental delta insert: relation("a","contains_private_data","b").
     * Mirrors col_session_insert_incremental(...) in test_delta_propagation.c. */
    int64_t fact[3] = { a_id, cpd_id, b_id };
    rc = col_session_insert_incremental(sess, "relation", fact, 1, 3);
    ASSERT(rc == 0, "insert_incremental failed");

    /* Delta step: drives the recursive edge/path stratum (leaks
     * current_iteration) then the non-recursive requires_review stratum. */
    rc = wl_session_step(sess);
    ASSERT(rc == 0, "delta step failed");

    printf("(delta +requires_review=%" PRId64 ") ", dctx.count);

    /* The #914 delta assertion: the delta callback must fire
     * requires_review("a","r") exactly once. FAILS before the fix (the
     * base-case skip drops the head, so the delta never fires), PASSES after. */
    ASSERT(dctx.count == 1,
        "expected exactly one +requires_review(\"a\",\"r\") delta (#914)");

    /* Snapshot regression guards on the delta state: requires_review must now
     * hold its single head tuple, with edge == 1 and path == 1 derived from
     * the incrementally-inserted relation fact. */
    struct rel_ctx rr = { "requires_review", 0 };
    rc = wl_session_snapshot(sess, count_cb, &rr);
    ASSERT(rc == 0, "post-delta requires_review snapshot failed");
    ASSERT(rr.count == 1,
        "expected exactly 1 requires_review tuple after delta insert");

    struct rel_ctx ec = { "edge", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ec);
    ASSERT(rc == 0, "post-delta edge snapshot failed");
    ASSERT(ec.count == 1, "expected exactly 1 edge tuple after delta insert");

    struct rel_ctx pc = { "path", 0 };
    rc = wl_session_snapshot(sess, count_cb, &pc);
    ASSERT(rc == 0, "post-delta path snapshot failed");
    ASSERT(pc.count == 1, "expected exactly 1 path tuple after delta insert");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Non-recursive-after-recursive Tests (Issue #914) ===\n\n");

    test_issue914_exact_program();
    test_issue914_no_recursion_variant();
    test_issue914_delta_insert();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)", fail_count);
    printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
