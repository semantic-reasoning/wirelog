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
test_join_output_limit_scaling(void)
{
    TEST("join_output_limit scaled 1/W per worker (Issue #426)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;

    /* --- W=1: worker gets full limit --- */
    wl_col_session_t *coord1 = make_coordinator(&plan, &prog);
    if (!coord1) {
        FAIL("coordinator creation W=1");
        return 1;
    }
    coord1->num_workers = 1;
    coord1->join_output_limit = 4000;

    int64_t rows1[] = { 1, 2 };
    insert_edges(coord1, rows1, 1);

    col_rel_t **parts1 = NULL;
    partition_rel(coord1, "edge", 1, &parts1);

    wl_col_session_t w1;
    memset(&w1, 0, sizeof(w1));
    col_worker_session_create(coord1, 0, parts1, 1, &w1);
    free(parts1);

    int ok = (w1.join_output_limit == 4000);
    col_worker_session_destroy(&w1);
    cleanup_coordinator(coord1, plan, prog);

    if (!ok) {
        FAIL("W=1: worker should get full limit");
        return 1;
    }

    /* --- W=4: worker gets 1/4 of limit --- */
    plan = NULL;
    prog = NULL;
    wl_col_session_t *coord4 = make_coordinator(&plan, &prog);
    if (!coord4) {
        FAIL("coordinator creation W=4");
        return 1;
    }
    coord4->num_workers = 4;
    coord4->join_output_limit = 8000;

    int64_t rows4[] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    insert_edges(coord4, rows4, 4);

    col_rel_t **parts4 = NULL;
    partition_rel(coord4, "edge", 4, &parts4);

    /* Create 4 workers; each should get join_output_limit = 8000/4 = 2000 */
    wl_col_session_t workers4[4];
    memset(workers4, 0, sizeof(workers4));
    for (uint32_t w = 0; w < 4; w++) {
        col_rel_t *wp[1] = { parts4[w] };
        parts4[w] = NULL;
        col_worker_session_create(coord4, w, wp, 1, &workers4[w]);
        if (workers4[w].join_output_limit != 2000)
            ok = 0;
    }
    free(parts4);

    for (uint32_t w = 0; w < 4; w++)
        col_worker_session_destroy(&workers4[w]);
    cleanup_coordinator(coord4, plan, prog);

    if (!ok) {
        FAIL("W=4: worker should get 1/4 of limit");
        return 1;
    }

    /* --- W=2: zero limit stays zero (disabled) --- */
    plan = NULL;
    prog = NULL;
    wl_col_session_t *coord2 = make_coordinator(&plan, &prog);
    if (!coord2) {
        FAIL("coordinator creation W=2 zero limit");
        return 1;
    }
    coord2->num_workers = 2;
    coord2->join_output_limit = 0; /* disabled */

    int64_t rows2[] = { 1, 2, 3, 4 };
    insert_edges(coord2, rows2, 2);

    col_rel_t **parts2 = NULL;
    partition_rel(coord2, "edge", 2, &parts2);

    wl_col_session_t w2a, w2b;
    memset(&w2a, 0, sizeof(w2a));
    memset(&w2b, 0, sizeof(w2b));
    col_rel_t *p2a[1] = { parts2[0] }; parts2[0] = NULL;
    col_rel_t *p2b[1] = { parts2[1] }; parts2[1] = NULL;
    col_worker_session_create(coord2, 0, p2a, 1, &w2a);
    col_worker_session_create(coord2, 1, p2b, 1, &w2b);
    free(parts2);

    if (w2a.join_output_limit != 0 || w2b.join_output_limit != 0)
        ok = 0;

    col_worker_session_destroy(&w2a);
    col_worker_session_destroy(&w2b);
    cleanup_coordinator(coord2, plan, prog);

    if (!ok) {
        FAIL("zero limit should stay zero (disabled)");
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
/* Issue #535: RDF Named-Graph column propagation tests                     */
/* ======================================================================== */

/*
 * Helper: parse src, generate plan, create columnar session.
 * Caller must call wl_session_destroy + wl_plan_free + wirelog_program_free.
 * Returns the wl_col_session_t* (cast of wl_session_t*) or NULL on error.
 */
static wl_col_session_t *
make_session_from_src(const char *src, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(session);
}

/*
 * test_rdf_graph_column_propagates_to_col_rel:
 * A relation declared with __graph_id as third column must have
 * has_graph_column == true and graph_col_idx == 2 on its col_rel_t
 * after col_session_create.
 */
static int
test_rdf_graph_column_propagates_to_col_rel(void)
{
    TEST("rdf: __graph_id propagates has_graph_column=true, graph_col_idx=2");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session_from_src(
        ".decl edge(a: int64, b: int64, __graph_id: int64)\n",
        &plan, &prog);
    if (!sess) {
        FAIL("session creation failed");
        return 1;
    }

    col_rel_t *r = session_find_rel(sess, "edge");
    if (!r) {
        wl_session_destroy(&sess->base);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("edge relation not found in session");
        return 1;
    }

    if (!r->has_graph_column) {
        wl_session_destroy(&sess->base);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("has_graph_column should be true");
        return 1;
    }

    if (r->graph_col_idx != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected graph_col_idx=2, got %u",
            r->graph_col_idx);
        wl_session_destroy(&sess->base);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL(msg);
        return 1;
    }

    wl_session_destroy(&sess->base);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
    return 0;
}

/*
 * test_rdf_no_graph_column_defaults_to_false:
 * A relation without __graph_id must have has_graph_column == false
 * on its col_rel_t after col_session_create.
 */
static int
test_rdf_no_graph_column_defaults_to_false(void)
{
    TEST("rdf: relation without __graph_id has has_graph_column=false");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session_from_src(
        ".decl edge(a: int64, b: int64)\n",
        &plan, &prog);
    if (!sess) {
        FAIL("session creation failed");
        return 1;
    }

    col_rel_t *r = session_find_rel(sess, "edge");
    if (!r) {
        wl_session_destroy(&sess->base);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("edge relation not found in session");
        return 1;
    }

    if (r->has_graph_column) {
        wl_session_destroy(&sess->base);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("has_graph_column should be false when __graph_id absent");
        return 1;
    }

    wl_session_destroy(&sess->base);
    wl_plan_free(plan);
    wirelog_program_free(prog);
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
    test_join_output_limit_scaling();
    test_rdf_graph_column_propagates_to_col_rel();
    test_rdf_no_graph_column_defaults_to_false();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);
    return tests_failed > 0 ? 1 : 0;
}
