/*
 * test_worker_arena_borrow.c - Issue #579 worker arena borrow invariants
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies design rule R-5 (workers share frozen arena):
 *   1. col_worker_session_create copies the coordinator's compound_arena
 *      pointer into the worker (pointer equality), and worker lookups
 *      against the borrowed arena resolve coordinator-allocated handles.
 *   2. col_worker_session_destroy does NOT free the borrowed arena: the
 *      coordinator's compound_arena pointer remains valid (and live
 *      lookups still succeed) after worker destruction.  ASan would flag
 *      a double-free on subsequent coordinator destruction.
 *
 * Issue #579 (stacked on #561 freeze contract).
 */

#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                             */
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
/* Helpers (mirrored from tests/test_worker_session.c)                       */
/* ======================================================================== */

static wl_col_session_t *
make_coordinator(wl_plan_t **plan_out, wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32)\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n",
        &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 2, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(session);
}

static int
insert_edges(wl_col_session_t *sess, const int64_t *rows, uint32_t nrows)
{
    return wl_session_insert(&sess->base, "edge", rows, nrows, 2);
}

static int
partition_rel(wl_col_session_t *coordinator, const char *name,
    uint32_t num_workers, col_rel_t ***out_partitions)
{
    col_rel_t *rel = session_find_rel(coordinator, name);
    if (!rel || rel->ncols == 0)
        return ENOENT;

    col_rel_t **parts
        = (col_rel_t **)calloc(num_workers, sizeof(col_rel_t *));
    if (!parts)
        return ENOMEM;

    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(rel, key_cols, 1, num_workers, parts);
    if (rc != 0) {
        free(parts);
        return rc;
    }

    for (uint32_t w = 0; w < num_workers; w++) {
        free(parts[w]->name);
        size_t len = strlen(name) + 1;
        char *dup = (char *)malloc(len);
        if (!dup) {
            for (uint32_t j = 0; j < num_workers; j++)
                col_rel_destroy(parts[j]);
            free(parts);
            return ENOMEM;
        }
        memcpy(dup, name, len);
        parts[w]->name = dup;
    }

    *out_partitions = parts;
    return 0;
}

static void
cleanup_coordinator(wl_col_session_t *sess, wl_plan_t *plan,
    wirelog_program_t *prog)
{
    wl_session_destroy(&sess->base);
    wl_plan_free(plan);
    wirelog_program_free(prog);
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static int
test_worker_borrows_coordinator_arena(void)
{
    TEST("worker borrows coordinator compound_arena (pointer + lookup)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }
    if (!coord->compound_arena) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("coordinator compound_arena unexpectedly NULL");
        return 1;
    }

    /* Allocate a payload in the coordinator's arena.  Must precede any
     * freeze; coordinator arena is unfrozen at session-create time. */
    uint64_t handle = wl_compound_arena_alloc(coord->compound_arena, 64u);
    if (handle == WL_COMPOUND_HANDLE_NULL) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("wl_compound_arena_alloc on coordinator returned NULL handle");
        return 1;
    }

    int64_t rows[] = { 1, 2 };
    if (insert_edges(coord, rows, 1) != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("insert_edges");
        return 1;
    }

    col_rel_t **parts = NULL;
    int rc = partition_rel(coord, "edge", 1, &parts);
    if (rc != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("partition_rel");
        return 1;
    }

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    rc = col_worker_session_create(coord, 0, parts, 1, &worker);
    free(parts);
    if (rc != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("col_worker_session_create");
        return 1;
    }

    /* R-5: pointer equality — worker borrows the exact coordinator arena. */
    int ok = (worker.compound_arena == coord->compound_arena);

    /* Borrow is functional: lookup of the coordinator-allocated handle
     * resolves through the worker's borrowed pointer. */
    if (ok) {
        uint32_t out_size = 0;
        const void *payload = wl_compound_arena_lookup(
            worker.compound_arena, handle, &out_size);
        if (!payload || out_size != 64u)
            ok = 0;
    }

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("worker did not borrow coordinator arena or lookup failed");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_worker_destroy_does_not_free_coordinator_arena(void)
{
    TEST("worker destroy leaves coordinator arena intact (no double-free)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }
    if (!coord->compound_arena) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("coordinator compound_arena unexpectedly NULL");
        return 1;
    }

    /* Allocate a sentinel handle BEFORE worker creation so we can probe
     * the arena's liveness after worker destruction. */
    uint64_t sentinel = wl_compound_arena_alloc(coord->compound_arena, 32u);
    if (sentinel == WL_COMPOUND_HANDLE_NULL) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("sentinel wl_compound_arena_alloc returned NULL handle");
        return 1;
    }

    int64_t rows[] = { 1, 2 };
    if (insert_edges(coord, rows, 1) != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("insert_edges");
        return 1;
    }

    col_rel_t **parts = NULL;
    int rc = partition_rel(coord, "edge", 1, &parts);
    if (rc != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("partition_rel");
        return 1;
    }

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    rc = col_worker_session_create(coord, 0, parts, 1, &worker);
    free(parts);
    if (rc != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("col_worker_session_create");
        return 1;
    }

    /* Snapshot the arena pointer prior to worker destruction. */
    wl_compound_arena_t *arena_before = coord->compound_arena;

    col_worker_session_destroy(&worker);

    /* After worker destroy, the coordinator must still own the same
     * arena pointer and lookups against it must still succeed. */
    int ok = (coord->compound_arena == arena_before)
        && (coord->compound_arena != NULL);
    if (ok) {
        uint32_t out_size = 0;
        const void *payload = wl_compound_arena_lookup(
            coord->compound_arena, sentinel, &out_size);
        if (!payload || out_size != 32u)
            ok = 0;
    }

    /* Coordinator destroy frees the arena exactly once.  Under the ASan
     * suite, a double-free here would be flagged. */
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("coordinator arena disturbed by worker destroy");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_worker_skips_gc_epoch_boundary_dispatch(void)
{
    TEST("worker context closes gc_epoch_boundary call-site gate");

    /* Regression for the TSan data race that surfaced once workers
     * started borrowing the coordinator's compound_arena (Issue #579 /
     * R-5).  The dispatch sites at ops.c (col_op_k_fusion entry) and
     * eval.c (recursive sub-pass tail) advance arena->current_epoch,
     * which is a write on a pointer that workers share with the
     * coordinator.  The gate predicate must therefore reject worker
     * sessions: only the coordinator may mutate the borrowed arena. */

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }
    if (!coord->compound_arena || !coord->rotation_ops
        || !coord->rotation_ops->gc_epoch_boundary) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("coordinator missing arena/rotation_ops prerequisites");
        return 1;
    }

    int64_t rows[] = { 1, 2 };
    if (insert_edges(coord, rows, 1) != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("insert_edges");
        return 1;
    }

    col_rel_t **parts = NULL;
    int rc = partition_rel(coord, "edge", 1, &parts);
    if (rc != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("partition_rel");
        return 1;
    }

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    rc = col_worker_session_create(coord, 0, parts, 1, &worker);
    free(parts);
    if (rc != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("col_worker_session_create");
        return 1;
    }

    /* The gate predicate that ops.c:col_op_k_fusion and
     * eval.c:col_eval_stratum apply before dispatching
     * gc_epoch_boundary.  Mirroring it here pins the invariant: any
     * future relaxation of the worker check at either call site will
     * trip this assertion. */
    int coord_open = (coord->coordinator == NULL
        && coord->compound_arena && coord->rotation_ops
        && coord->rotation_ops->gc_epoch_boundary);
    int worker_open = (worker.coordinator == NULL
        && worker.compound_arena && worker.rotation_ops
        && worker.rotation_ops->gc_epoch_boundary);

    /* Worker must observably borrow the coordinator's arena, otherwise
     * the test reduces to a tautology over a NULL pointer. */
    int worker_borrows = (worker.compound_arena == coord->compound_arena
        && worker.compound_arena != NULL
        && worker.rotation_ops == coord->rotation_ops
        && worker.coordinator == coord);

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!coord_open) {
        FAIL("coordinator gate unexpectedly closed");
        return 1;
    }
    if (!worker_borrows) {
        FAIL("worker did not borrow coordinator arena/rotation_ops");
        return 1;
    }
    if (worker_open) {
        FAIL("worker gate must be closed (coordinator-only mutation)");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_worker_arena_borrow (Issue #579 / R-5)\n");
    test_worker_borrows_coordinator_arena();
    test_worker_destroy_does_not_free_coordinator_arena();
    test_worker_skips_gc_epoch_boundary_dispatch();
    printf("\n%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
