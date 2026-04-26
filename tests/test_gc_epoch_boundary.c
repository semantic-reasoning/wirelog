/*
 * test_gc_epoch_boundary.c - Issue #560 integration tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies that the rotation-strategy gc_epoch_boundary slot fires at
 * the integration points added by #560:
 *
 *   1. After each recursive sub-pass iteration in col_eval_stratum:
 *      a recursive transitive-closure evaluation that runs >= 2
 *      sub-pass iterations advances sess->compound_arena->current_epoch.
 *
 *   2. The dispatch is NULL-safe with respect to sess->compound_arena:
 *      if a session is mutated to drop its arena before evaluation,
 *      the eval path must not crash on the slot dispatch.
 */

#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test harness                                                             */
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
            return;                           \
        } while (0)

#define ASSERT(cond, msg)     \
        do {                      \
            if (!(cond)) {        \
                FAIL(msg);        \
            }                     \
        } while (0)

/* ======================================================================== */
/* Helper: build a minimal columnar plan                                    */
/* ======================================================================== */

static wl_plan_t *
build_plan(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0)
        return NULL;
    return plan;
}

/* ======================================================================== */
/* Test 1: gc_epoch_boundary fires per recursive sub-pass iteration         */
/* ======================================================================== */

static void
test_gc_epoch_advances_per_iteration(void)
{
    TEST("gc_epoch_boundary: advances current_epoch across recursive eval");

    /* Transitive-closure program: forces the recursive sub-pass loop to
     * run multiple iterations until fixed-point. */
    wl_plan_t *plan
        = build_plan(".decl edge(x: int32, y: int32)\n"
            ".decl tc(x: int32, y: int32)\n"
            "tc(X, Y) :- edge(X, Y).\n"
            "tc(X, Z) :- edge(X, Y), tc(Y, Z).\n");
    ASSERT(plan != NULL, "could not build TC plan");

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
    }

    wl_col_session_t *cs = COL_SESSION(session);
    ASSERT(cs->compound_arena != NULL,
        "compound_arena not allocated by col_session_create");

    /* Snapshot the epoch counter before evaluation. */
    uint32_t epoch_before = cs->compound_arena->current_epoch;

    /* Build a chain edge(1,2), edge(2,3), edge(3,4), edge(4,5) so the
     * recursive sub-pass loop has to iterate several times to derive
     * the full closure (the per-iteration GC dispatch fires once per
     * sub-pass). */
    int64_t edges[][2] = {
        { 1, 2 },
        { 2, 3 },
        { 3, 4 },
        { 4, 5 },
    };
    for (size_t i = 0; i < sizeof(edges) / sizeof(edges[0]); i++) {
        wl_session_insert(session, "edge", edges[i], 1, 2);
    }

    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("wl_session_step failed");
    }

    uint32_t epoch_after = cs->compound_arena->current_epoch;

    if (epoch_after <= epoch_before) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("compound_arena current_epoch did not advance after eval");
    }

    /* TC over a 4-edge chain takes >= 2 recursive sub-passes (and the
     * #560 dispatch fires once per sub-pass), so we expect strictly
     * more than one epoch advance.  Use >= 2 as the lower bound to
     * keep the assertion robust against future eval optimisations
     * that might collapse trivial sub-passes. */
    if (epoch_after - epoch_before < 2u) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("expected at least 2 epoch advances across recursive eval");
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Test 2: dispatch is NULL-safe when sess->compound_arena is NULL          */
/* ======================================================================== */

static void
test_gc_epoch_compound_arena_null_safe(void)
{
    TEST("gc_epoch_boundary: NULL compound_arena does not crash eval");

    /* Same recursive program as test 1 so the dispatch site fires. */
    wl_plan_t *plan
        = build_plan(".decl edge(x: int32, y: int32)\n"
            ".decl tc(x: int32, y: int32)\n"
            "tc(X, Y) :- edge(X, Y).\n"
            "tc(X, Z) :- edge(X, Y), tc(Y, Z).\n");
    ASSERT(plan != NULL, "could not build TC plan");

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
    }

    wl_col_session_t *cs = COL_SESSION(session);
    ASSERT(cs->compound_arena != NULL,
        "compound_arena not allocated by col_session_create");

    /* Free and detach the compound arena so the per-iteration
     * dispatch site has to take the NULL-guarded skip branch. */
    wl_compound_arena_free(cs->compound_arena);
    cs->compound_arena = NULL;

    int64_t edge_12[] = { 1, 2 };
    int64_t edge_23[] = { 2, 3 };
    wl_session_insert(session, "edge", edge_12, 1, 2);
    wl_session_insert(session, "edge", edge_23, 1, 2);

    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("wl_session_step failed with NULL compound_arena");
    }

    /* If we reached here without segfaulting, the NULL guards held. */
    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_gc_epoch_boundary (Issue #560)\n");
    test_gc_epoch_advances_per_iteration();
    test_gc_epoch_compound_arena_null_safe();
    printf("\n%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
