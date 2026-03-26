/*
 * test_worker_session.c - Unit tests for per-worker session state
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: worker session create/destroy lifecycle, relation isolation,
 * frontier independence, arena/pool independence, coordinator read-only.
 *
 * Issue #315: Per-Worker Session State
 */

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
/* Helpers                                                                  */
/* ======================================================================== */

/* Create a coordinator session with "edge(x,y)" EDB and "path(x,y)" IDB. */
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

/* Insert edge rows into coordinator session. */
static int
insert_edges(wl_col_session_t *sess, const int64_t *rows, uint32_t nrows)
{
    return wl_session_insert(&sess->base, "edge", rows, nrows, 2);
}

/* Build partition array for a named relation in the coordinator. */
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

    /* Partition on first column */
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(rel, key_cols, 1, num_workers, parts);
    if (rc != 0) {
        free(parts);
        return rc;
    }

    /* Rename partitions to original name for session_find_rel */
    for (uint32_t w = 0; w < num_workers; w++) {
        free(parts[w]->name);
        {
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
        if (!parts[w]->name) {
            for (uint32_t j = 0; j < num_workers; j++)
                col_rel_destroy(parts[j]);
            free(parts);
            return ENOMEM;
        }
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
test_create_destroy(void)
{
    TEST("worker session create and destroy (lifecycle, no leaks)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    if (insert_edges(coord, rows, 4) != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("insert");
        return 1;
    }

    col_rel_t **parts = NULL;
    int rc = partition_rel(coord, "edge", 2, &parts);
    if (rc != 0) {
        cleanup_coordinator(coord, plan, prog);
        FAIL("partition");
        return 1;
    }

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    rc = col_worker_session_create(coord, 0, parts, 2, &worker);
    if (rc != 0) {
        for (uint32_t i = 0; i < 2; i++) {
            if (parts[i])
                col_rel_destroy(parts[i]);
        }
        free(parts);
        cleanup_coordinator(coord, plan, prog);
        FAIL("worker create");
        return 1;
    }
    free(parts);

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);
    PASS();
    return 0;
}

static int
test_identity_fields(void)
{
    TEST("worker identity fields (worker_id, coordinator pointer)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 2, 3, 4 };
    insert_edges(coord, rows, 2);

    col_rel_t **parts = NULL;
    partition_rel(coord, "edge", 1, &parts);

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    col_worker_session_create(coord, 7, parts, 1, &worker);
    free(parts);

    int ok = (worker.worker_id == 7)
        && (worker.coordinator == coord)
        && (coord->coordinator == NULL);

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("identity mismatch");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_borrowed_fields(void)
{
    TEST("borrowed fields shared (plan, frontier_ops)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 2 };
    insert_edges(coord, rows, 1);

    col_rel_t **parts = NULL;
    partition_rel(coord, "edge", 1, &parts);

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    col_worker_session_create(coord, 0, parts, 1, &worker);
    free(parts);

    int ok = (worker.plan == coord->plan)
        && (worker.frontier_ops == coord->frontier_ops);

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("borrowed fields differ");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_rels_independent(void)
{
    TEST("worker rels[] independent from coordinator");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 10, 2, 20, 3, 30, 4, 40 };
    insert_edges(coord, rows, 4);

    col_rel_t *coord_edge = session_find_rel(coord, "edge");
    uint32_t coord_nrows = coord_edge ? coord_edge->nrows : 0;

    col_rel_t **parts = NULL;
    partition_rel(coord, "edge", 2, &parts);

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    col_worker_session_create(coord, 0, parts, 2, &worker);
    free(parts);

    int ok = (worker.rels != coord->rels);

    col_rel_t *coord_edge2 = session_find_rel(coord, "edge");
    if (!coord_edge2 || coord_edge2->nrows != coord_nrows)
        ok = 0;

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("rels not independent");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_wq_null(void)
{
    TEST("worker wq is NULL (prevents nested K-fusion)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 2 };
    insert_edges(coord, rows, 1);

    col_rel_t **parts = NULL;
    partition_rel(coord, "edge", 1, &parts);

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    col_worker_session_create(coord, 0, parts, 1, &worker);
    free(parts);

    int ok = (worker.wq == NULL);

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("wq not NULL");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_arena_pool_allocated(void)
{
    TEST("worker has independent eval_arena and delta_pool");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 2 };
    insert_edges(coord, rows, 1);

    col_rel_t **parts = NULL;
    partition_rel(coord, "edge", 1, &parts);

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    col_worker_session_create(coord, 0, parts, 1, &worker);
    free(parts);

    int ok = (worker.eval_arena != NULL)
        && (worker.delta_pool != NULL)
        && (worker.eval_arena != coord->eval_arena)
        && (worker.delta_pool != coord->delta_pool);

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("arena/pool not independent");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_mat_cache_empty(void)
{
    TEST("worker mat_cache starts empty");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 2 };
    insert_edges(coord, rows, 1);

    col_rel_t **parts = NULL;
    partition_rel(coord, "edge", 1, &parts);

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    col_worker_session_create(coord, 0, parts, 1, &worker);
    free(parts);

    int ok = (worker.mat_cache.count == 0);

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("mat_cache not empty");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_frontier_independence(void)
{
    TEST("worker frontiers independent from coordinator");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    coord->frontiers[0].outer_epoch = 1;
    coord->frontiers[0].iteration = 5;

    int64_t rows[] = { 1, 2 };
    insert_edges(coord, rows, 1);

    col_rel_t **parts = NULL;
    partition_rel(coord, "edge", 1, &parts);

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    col_worker_session_create(coord, 0, parts, 1, &worker);
    free(parts);

    int ok = (worker.frontiers[0].outer_epoch == 1)
        && (worker.frontiers[0].iteration == 5);

    worker.frontiers[0].outer_epoch = 99;
    worker.frontiers[0].iteration = 42;

    if (coord->frontiers[0].outer_epoch != 1
        || coord->frontiers[0].iteration != 5)
        ok = 0;

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("frontiers not independent");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_coordinator_readonly(void)
{
    TEST("coordinator rels unchanged after worker lifecycle");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 10, 2, 20, 3, 30, 4, 40,
                       5, 50, 6, 60, 7, 70, 8, 80 };
    insert_edges(coord, rows, 8);

    col_rel_t *coord_edge = session_find_rel(coord, "edge");
    uint32_t orig_nrows = coord_edge->nrows;

    int64_t *orig_data = (int64_t *)malloc(
        (size_t)orig_nrows * 2 * sizeof(int64_t));
    for (uint32_t i = 0; i < orig_nrows; i++)
        col_rel_row_copy_out(coord_edge, i, orig_data + (size_t)i * 2);

    for (uint32_t w = 0; w < 2; w++) {
        col_rel_t **parts = NULL;
        partition_rel(coord, "edge", 2, &parts);

        wl_col_session_t worker;
        memset(&worker, 0, sizeof(worker));
        col_worker_session_create(coord, w, parts, 2, &worker);
        free(parts);

        col_worker_session_destroy(&worker);
    }

    coord_edge = session_find_rel(coord, "edge");
    int ok = (coord_edge != NULL)
        && (coord_edge->nrows == orig_nrows);
    if (ok) {
        int64_t *cur_data = (int64_t *)malloc(
            (size_t)orig_nrows * 2 * sizeof(int64_t));
        if (cur_data) {
            for (uint32_t i = 0; i < orig_nrows; i++)
                col_rel_row_copy_out(coord_edge, i,
                    cur_data + (size_t)i * 2);
            ok = (memcmp(cur_data, orig_data,
                (size_t)orig_nrows * 2 * sizeof(int64_t)) == 0);
            free(cur_data);
        } else {
            ok = 0;
        }
    }

    free(orig_data);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("coordinator modified");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_find_rel_returns_partition(void)
{
    TEST("session_find_rel on worker returns partition data");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 10, 2, 20, 3, 30, 4, 40 };
    insert_edges(coord, rows, 4);

    col_rel_t *coord_edge = session_find_rel(coord, "edge");
    uint32_t total = coord_edge->nrows;

    col_rel_t **parts = NULL;
    partition_rel(coord, "edge", 2, &parts);

    wl_col_session_t workers[2];
    memset(workers, 0, sizeof(workers));

    col_rel_t *w0_parts[1] = { parts[0] };
    parts[0] = NULL;
    col_worker_session_create(coord, 0, w0_parts, 1, &workers[0]);

    col_rel_t *w1_parts[1] = { parts[1] };
    parts[1] = NULL;
    col_worker_session_create(coord, 1, w1_parts, 1, &workers[1]);
    free(parts);

    col_rel_t *w0_edge = session_find_rel(&workers[0], "edge");
    col_rel_t *w1_edge = session_find_rel(&workers[1], "edge");

    int ok = (w0_edge != NULL) && (w1_edge != NULL);
    if (ok)
        ok = (w0_edge->nrows + w1_edge->nrows == total);

    col_worker_session_destroy(&workers[0]);
    col_worker_session_destroy(&workers[1]);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("partition lookup failed");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_invalid_args(void)
{
    TEST("invalid args: NULL coordinator/out_worker returns EINVAL");

    wl_col_session_t dummy;
    memset(&dummy, 0, sizeof(dummy));
    col_rel_t *parts[1] = { NULL };

    int rc1 = col_worker_session_create(NULL, 0, parts, 1, &dummy);
    int rc2 = col_worker_session_create(&dummy, 0, parts, 1, NULL);
    int rc3 = col_worker_session_create(&dummy, 0, NULL, 1, &dummy);

    if (rc1 != EINVAL || rc2 != EINVAL || rc3 != EINVAL) {
        FAIL("expected EINVAL");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_hash_table_independent(void)
{
    TEST("worker hash table is independent (lazy rebuild)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *coord = make_coordinator(&plan, &prog);
    if (!coord) {
        FAIL("coordinator creation");
        return 1;
    }

    int64_t rows[] = { 1, 2 };
    insert_edges(coord, rows, 1);

    col_rel_t **parts = NULL;
    partition_rel(coord, "edge", 1, &parts);

    wl_col_session_t worker;
    memset(&worker, 0, sizeof(worker));
    col_worker_session_create(coord, 0, parts, 1, &worker);
    free(parts);

    int ok = (worker.rel_hash_nbuckets == 0);

    col_rel_t *found = session_find_rel(&worker, "edge");
    if (!found)
        ok = 0;

    /* After lazy build, hash should be worker-owned (not coordinator's) */
    if (worker.rel_hash_nbuckets > 0
        && worker.rel_hash_head == coord->rel_hash_head)
        ok = 0;

    col_worker_session_destroy(&worker);
    cleanup_coordinator(coord, plan, prog);

    if (!ok) {
        FAIL("hash table not independent");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("Per-Worker Session State Tests (Issue #315)\n");

    test_create_destroy();
    test_identity_fields();
    test_borrowed_fields();
    test_rels_independent();
    test_wq_null();
    test_arena_pool_allocated();
    test_mat_cache_empty();
    test_frontier_independence();
    test_coordinator_readonly();
    test_find_rel_returns_partition();
    test_invalid_args();
    test_hash_table_independent();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);
    return tests_failed > 0 ? 1 : 0;
}
