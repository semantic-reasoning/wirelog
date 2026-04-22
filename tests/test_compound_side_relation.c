/*
 * test_compound_side_relation.c - Side-relation auto-creation tests (Issue #533)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Integration tests exercising wl_compound_side_ensure() against a live
 * columnar session.  These tests validate Commit B of #533:
 *
 *   1. test_side_relation_auto_create
 *        Ensures __compound_<functor>_<arity> is created with the expected
 *        schema (handle, arg0, ..., arg{N-1}) and is idempotent.
 *
 *   2. test_side_relation_store_retrieve
 *        Inserts a compound handle + args into the side-relation and reads
 *        them back through the col_rel_t column accessors.
 *
 *   3. test_side_relation_nested
 *        Simulates f(g(a,b)) by registering two side-relations
 *        (__compound_g_2 and __compound_f_1) and verifying cross-reference
 *        via handle values in the outer relation's arg0 column.
 *
 *   4. test_side_relation_arena_gc_integration
 *        Allocates compound handles from the arena, stores them in the
 *        side-relation, releases them (multiplicity -> 0) and confirms the
 *        arena GC reclaims them at the next epoch boundary.
 */

#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/compound_side.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                              \
        do {                                        \
            tests_run++;                            \
            printf("  [%d] %s", tests_run, name);   \
        } while (0)

#define PASS()                                  \
        do {                                        \
            tests_passed++;                         \
            printf(" ... PASS\n");                  \
        } while (0)

#define FAIL(msg)                               \
        do {                                        \
            tests_failed++;                         \
            printf(" ... FAIL: %s\n", (msg));       \
            goto cleanup;                           \
        } while (0)

#define ASSERT(cond, msg)                       \
        do {                                        \
            if (!(cond)) {                          \
                FAIL(msg);                          \
            }                                       \
        } while (0)

/* ======================================================================== */
/* Session helpers                                                          */
/* ======================================================================== */

/*
 * Minimal-program harness: parses, plans, and creates a columnar session.
 * Uses the simplest viable program so the session has a valid plan but we
 * only exercise the rel[] map, not the evaluator.
 *
 * Caller must pass out_sess/out_plan/out_prog into free_session on exit.
 */
static int
make_session(wl_session_t **out_sess, wl_plan_t **out_plan,
    wirelog_program_t **out_prog)
{
    const char *src =
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;
    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }
    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }
    *out_sess = sess;
    *out_plan = plan;
    *out_prog = prog;
    return 0;
}

static void
free_session(wl_session_t *sess, wl_plan_t *plan, wirelog_program_t *prog)
{
    if (sess)
        wl_session_destroy(sess);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_side_relation_name_format(void)
{
    TEST("side-relation name format");
    char buf[64];
    int rc = wl_compound_side_name("metadata", 4, buf, sizeof(buf));
    if (rc != 0) {
        tests_failed++;
        printf(" ... FAIL: name format returned error\n");
        return;
    }
    if (strcmp(buf, "__compound_metadata_4") != 0) {
        tests_failed++;
        printf(" ... FAIL: unexpected name '%s'\n", buf);
        return;
    }
    /* Too-small buffer must fail. */
    char small[8];
    if (wl_compound_side_name("metadata", 4, small, sizeof(small)) == 0) {
        tests_failed++;
        printf(" ... FAIL: small buffer should have errored\n");
        return;
    }
    tests_passed++;
    printf(" ... PASS\n");
}

static void
test_side_relation_auto_create(void)
{
    TEST("side-relation auto-create (idempotent)");
    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    if (make_session(&sess, &plan, &prog) != 0) {
        tests_failed++;
        printf(" ... FAIL: make_session failed\n");
        return;
    }
    wl_col_session_t *cs = (wl_col_session_t *)sess;

    col_rel_t *rel1 = NULL;
    int rc = wl_compound_side_ensure(cs, "metadata", 4, &rel1);
    ASSERT(rc == 0, "ensure returned non-zero");
    ASSERT(rel1 != NULL, "ensure returned NULL rel");
    ASSERT(rel1->ncols == 5, "expected 5 cols (handle + 4 args)");
    ASSERT(strcmp(rel1->name, "__compound_metadata_4") == 0, "wrong name");
    ASSERT(rel1->col_names != NULL, "col_names missing");
    ASSERT(strcmp(rel1->col_names[0], "handle") == 0, "col 0 != handle");
    ASSERT(strcmp(rel1->col_names[1], "arg0") == 0, "col 1 != arg0");
    ASSERT(strcmp(rel1->col_names[4], "arg3") == 0, "col 4 != arg3");

    /* Idempotent: second call returns same pointer, does not duplicate. */
    uint32_t nrels_before = cs->nrels;
    col_rel_t *rel2 = NULL;
    rc = wl_compound_side_ensure(cs, "metadata", 4, &rel2);
    ASSERT(rc == 0, "second ensure failed");
    ASSERT(rel2 == rel1, "second ensure produced different pointer");
    ASSERT(cs->nrels == nrels_before, "ensure added duplicate");

    /* Different functor -> different relation. */
    col_rel_t *rel3 = NULL;
    rc = wl_compound_side_ensure(cs, "scope", 1, &rel3);
    ASSERT(rc == 0, "scope ensure failed");
    ASSERT(rel3 != NULL, "scope rel NULL");
    ASSERT(rel3 != rel1, "scope vs metadata collided");
    ASSERT(rel3->ncols == 2, "scope/1 should have 2 cols");
    ASSERT(strcmp(rel3->name, "__compound_scope_1") == 0, "wrong scope name");

    /* Invalid arguments */
    ASSERT(wl_compound_side_ensure(NULL, "x", 1, NULL) == EINVAL,
        "NULL session not rejected");
    ASSERT(wl_compound_side_ensure(cs, NULL, 1, NULL) == EINVAL,
        "NULL functor not rejected");
    ASSERT(wl_compound_side_ensure(cs, "x", 0, NULL) == EINVAL,
        "arity=0 not rejected");

    PASS();
cleanup:
    free_session(sess, plan, prog);
}

static void
test_side_relation_store_retrieve(void)
{
    TEST("side-relation store + retrieve");
    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    if (make_session(&sess, &plan, &prog) != 0) {
        tests_failed++;
        printf(" ... FAIL: make_session failed\n");
        return;
    }
    wl_col_session_t *cs = (wl_col_session_t *)sess;

    col_rel_t *rel = NULL;
    int rc = wl_compound_side_ensure(cs, "pair", 2, &rel);
    ASSERT(rc == 0 && rel, "ensure failed");

    /* Pretend we have a compound arena external to the session: register
     * two compound records and store their handles into the side-relation. */
    wl_compound_arena_t *arena = wl_compound_arena_create(0x55u, 256, 0);
    ASSERT(arena != NULL, "arena create failed");

    uint64_t h_a = wl_compound_arena_alloc(arena, 16);
    uint64_t h_b = wl_compound_arena_alloc(arena, 16);
    ASSERT(h_a && h_b, "handle alloc failed");

    /* Side-relation row: (handle, arg0, arg1) */
    int64_t row_a[3] = { (int64_t)h_a, 100, 200 };
    int64_t row_b[3] = { (int64_t)h_b, 300, 400 };
    ASSERT(col_rel_append_row(rel, row_a) == 0, "append row_a");
    ASSERT(col_rel_append_row(rel, row_b) == 0, "append row_b");
    ASSERT(rel->nrows == 2, "rel nrows != 2");

    /* Retrieve by column accessor */
    ASSERT(col_rel_get(rel, 0, 0) == (int64_t)h_a, "row 0 handle mismatch");
    ASSERT(col_rel_get(rel, 0, 1) == 100, "row 0 arg0 mismatch");
    ASSERT(col_rel_get(rel, 0, 2) == 200, "row 0 arg1 mismatch");
    ASSERT(col_rel_get(rel, 1, 0) == (int64_t)h_b, "row 1 handle mismatch");
    ASSERT(col_rel_get(rel, 1, 1) == 300, "row 1 arg0 mismatch");
    ASSERT(col_rel_get(rel, 1, 2) == 400, "row 1 arg1 mismatch");

    /* Arena lookup round-trip: the handle column in the relation resolves
     * back to the arena payload. */
    uint32_t sz = 0;
    const void *pa = wl_compound_arena_lookup(arena, h_a, &sz);
    const void *pb = wl_compound_arena_lookup(arena, h_b, &sz);
    ASSERT(pa != NULL && pb != NULL, "arena lookup failed");
    ASSERT(pa != pb, "distinct handles collided in arena");

    wl_compound_arena_free(arena);
    PASS();
cleanup:
    free_session(sess, plan, prog);
}

static void
test_side_relation_nested(void)
{
    TEST("side-relation nested f(g(a,b)) cross-reference");
    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    if (make_session(&sess, &plan, &prog) != 0) {
        tests_failed++;
        printf(" ... FAIL: make_session failed\n");
        return;
    }
    wl_col_session_t *cs = (wl_col_session_t *)sess;

    /* Inner compound: g(a, b) - 2 args */
    col_rel_t *rel_g = NULL;
    int rc = wl_compound_side_ensure(cs, "g", 2, &rel_g);
    ASSERT(rc == 0 && rel_g, "ensure g/2 failed");

    /* Outer compound: f(x) - 1 arg, where x is a handle into rel_g. */
    col_rel_t *rel_f = NULL;
    rc = wl_compound_side_ensure(cs, "f", 1, &rel_f);
    ASSERT(rc == 0 && rel_f, "ensure f/1 failed");
    ASSERT(rel_g != rel_f, "nested relations aliased");

    wl_compound_arena_t *arena = wl_compound_arena_create(0x99u, 256, 0);
    ASSERT(arena != NULL, "arena create failed");

    /* Inner g(10, 20) */
    uint64_t h_g = wl_compound_arena_alloc(arena, 24);
    ASSERT(h_g, "alloc h_g");
    int64_t row_g[3] = { (int64_t)h_g, 10, 20 };
    ASSERT(col_rel_append_row(rel_g, row_g) == 0, "append to g");

    /* Outer f(h_g): arg0 holds a handle into rel_g. */
    uint64_t h_f = wl_compound_arena_alloc(arena, 16);
    ASSERT(h_f, "alloc h_f");
    int64_t row_f[2] = { (int64_t)h_f, (int64_t)h_g };
    ASSERT(col_rel_append_row(rel_f, row_f) == 0, "append to f");

    /* Follow the cross-reference: rel_f.arg0 -> handle -> lookup rel_g row. */
    int64_t ref = col_rel_get(rel_f, 0, 1);
    ASSERT((uint64_t)ref == h_g, "cross-ref handle mismatch");

    /* Locate the rel_g row whose handle column equals `ref`. */
    bool found = false;
    for (uint32_t r = 0; r < rel_g->nrows; r++) {
        if (col_rel_get(rel_g, r, 0) == ref) {
            found = true;
            ASSERT(col_rel_get(rel_g, r, 1) == 10, "inner arg0 not 10");
            ASSERT(col_rel_get(rel_g, r, 2) == 20, "inner arg1 not 20");
            break;
        }
    }
    ASSERT(found, "cross-ref row not found in g/2");

    wl_compound_arena_free(arena);
    PASS();
cleanup:
    free_session(sess, plan, prog);
}

static void
test_side_relation_arena_gc_integration(void)
{
    TEST("side-relation + arena GC epoch boundary");
    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    if (make_session(&sess, &plan, &prog) != 0) {
        tests_failed++;
        printf(" ... FAIL: make_session failed\n");
        return;
    }
    wl_col_session_t *cs = (wl_col_session_t *)sess;

    col_rel_t *rel = NULL;
    int rc = wl_compound_side_ensure(cs, "tmp", 1, &rel);
    ASSERT(rc == 0 && rel, "ensure tmp/1 failed");

    wl_compound_arena_t *arena = wl_compound_arena_create(0x1u, 128, 4);
    ASSERT(arena != NULL, "arena create failed");

    /* Allocate 3 compounds; release 2 so their multiplicity hits 0. */
    uint64_t h0 = wl_compound_arena_alloc(arena, 8);
    uint64_t h1 = wl_compound_arena_alloc(arena, 8);
    uint64_t h2 = wl_compound_arena_alloc(arena, 8);
    ASSERT(h0 && h1 && h2, "alloc 3 handles");
    ASSERT(arena->live_handles == 3, "live != 3");

    wl_compound_arena_retain(arena, h0, -1);
    wl_compound_arena_retain(arena, h1, -1);
    ASSERT(arena->live_handles == 1, "live != 1 after 2 releases");

    /* Z-set frontier advance: the GC reclaims the zero-multiplicity entries. */
    uint32_t reclaimed = wl_compound_arena_gc_epoch_boundary(arena);
    ASSERT(reclaimed == 2, "GC should have reclaimed 2");
    ASSERT(arena->current_epoch == 1, "epoch should advance to 1");

    /* K-Fusion invariant: freeze blocks new allocations during execution. */
    wl_compound_arena_freeze(arena);
    ASSERT(wl_compound_arena_alloc(arena, 8) == WL_COMPOUND_HANDLE_NULL,
        "frozen arena accepted alloc");
    wl_compound_arena_unfreeze(arena);

    /* Post-unfreeze alloc lands in the new epoch. */
    uint64_t h_new = wl_compound_arena_alloc(arena, 8);
    ASSERT(h_new != WL_COMPOUND_HANDLE_NULL, "post-GC alloc failed");
    ASSERT(wl_compound_handle_epoch(h_new) == 1, "new handle not in epoch 1");

    wl_compound_arena_free(arena);
    PASS();
cleanup:
    free_session(sess, plan, prog);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_compound_side_relation (Issue #533)\n");
    printf("========================================\n");

    test_side_relation_name_format();
    test_side_relation_auto_create();
    test_side_relation_store_retrieve();
    test_side_relation_nested();
    test_side_relation_arena_gc_integration();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
