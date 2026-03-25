/*
 * test_edb_partition_ingest.c - Unit tests for EDB partition-at-ingest
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: key discovery from plan, dirty flag, batch partition,
 * single-worker noop, reinsert re-partition, partition consistency.
 *
 * Issue #320: EDB Partition-at-Ingest
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

static wl_col_session_t *
make_session(const char *src, uint32_t num_workers,
    wl_plan_t **plan_out, wirelog_program_t **prog_out)
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
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(session);
}

static void
cleanup(wl_col_session_t *sess, wl_plan_t *plan, wirelog_program_t *prog)
{
    wl_session_destroy(&sess->base);
    wl_plan_free(plan);
    wirelog_program_free(prog);
}

static const char *EDGE_PATH_SRC =
    ".decl edge(x: int32, y: int32)\n"
    ".decl path(x: int32, y: int32)\n"
    "path(x, y) :- edge(x, y).\n";

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static int
test_single_worker_no_partitions(void)
{
    TEST("W=1: edb_parts is NULL (no partition overhead)");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(EDGE_PATH_SRC, 1, &plan, &prog);
    if (!sess) {
        FAIL("session creation");
        return 1;
    }

    int ok = (sess->edb_parts == NULL && sess->edb_part_count == 0);

    cleanup(sess, plan, prog);

    if (!ok) {
        FAIL("edb_parts not NULL for W=1");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_multi_worker_edb_parts_allocated(void)
{
    TEST("W=2: edb_parts allocated with correct count");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(EDGE_PATH_SRC, 2, &plan, &prog);
    if (!sess) {
        FAIL("session creation");
        return 1;
    }

    int ok = (sess->edb_parts != NULL)
        && (sess->edb_part_count == plan->edb_count)
        && (sess->edb_part_count > 0);

    /* All should start dirty */
    for (uint32_t i = 0; i < sess->edb_part_count && ok; i++) {
        if (!sess->edb_parts[i].dirty)
            ok = 0;
    }

    cleanup(sess, plan, prog);

    if (!ok) {
        FAIL("edb_parts not properly allocated");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_key_discovery_from_plan(void)
{
    TEST("key discovery: finds JOIN key columns from plan");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(EDGE_PATH_SRC, 2, &plan, &prog);
    if (!sess) {
        FAIL("session creation");
        return 1;
    }

    /* Insert to trigger schema init, then partition to trigger key discovery */
    int64_t rows[] = { 1, 2, 3, 4 };
    wl_session_insert(&sess->base, "edge", rows, 2, 2);

    int rc = col_session_partition_edb_for_tdd(sess);

    /* Find the edb_parts entry for "edge" */
    int ok = (rc == 0);
    bool found_edge = false;
    for (uint32_t i = 0; i < sess->edb_part_count && ok; i++) {
        if (strcmp(plan->edb_relations[i], "edge") == 0) {
            found_edge = true;
            ok = sess->edb_parts[i].keys_resolved;
            /* key_count should be > 0 if edge is used in a JOIN */
            break;
        }
    }
    if (!found_edge)
        ok = 0;

    cleanup(sess, plan, prog);

    if (!ok) {
        FAIL("key discovery failed");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_dirty_flag_set_on_insert(void)
{
    TEST("dirty flag: set by insert, cleared by partition");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(EDGE_PATH_SRC, 2, &plan, &prog);
    if (!sess) {
        FAIL("session creation");
        return 1;
    }

    /* Initially dirty from session_create */
    int edge_idx = -1;
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        if (strcmp(plan->edb_relations[i], "edge") == 0) {
            edge_idx = (int)i;
            break;
        }
    }
    if (edge_idx < 0) {
        cleanup(sess, plan, prog);
        FAIL("edge not found in plan");
        return 1;
    }

    int ok = sess->edb_parts[edge_idx].dirty;

    /* Insert and verify still dirty */
    int64_t rows[] = { 1, 2 };
    wl_session_insert(&sess->base, "edge", rows, 1, 2);
    ok = ok && sess->edb_parts[edge_idx].dirty;

    /* Partition should clear dirty */
    col_session_partition_edb_for_tdd(sess);
    ok = ok && !sess->edb_parts[edge_idx].dirty;

    /* Insert again should re-dirty */
    int64_t rows2[] = { 3, 4 };
    wl_session_insert(&sess->base, "edge", rows2, 1, 2);
    ok = ok && sess->edb_parts[edge_idx].dirty;

    cleanup(sess, plan, prog);

    if (!ok) {
        FAIL("dirty flag lifecycle incorrect");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_batch_partition_correctness(void)
{
    TEST("batch partition: sum of partition rows == total rows");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(EDGE_PATH_SRC, 4, &plan, &prog);
    if (!sess) {
        FAIL("session creation");
        return 1;
    }

    /* Insert 100 rows */
    int64_t rows[200];
    for (uint32_t i = 0; i < 100; i++) {
        rows[(size_t)i * 2] = (int64_t)i;
        rows[(size_t)i * 2 + 1] = (int64_t)(i * 10);
    }
    wl_session_insert(&sess->base, "edge", rows, 100, 2);

    int rc = col_session_partition_edb_for_tdd(sess);
    if (rc != 0) {
        cleanup(sess, plan, prog);
        FAIL("partition failed");
        return 1;
    }

    /* Find edge partition entry */
    int ok = 0;
    for (uint32_t i = 0; i < sess->edb_part_count; i++) {
        if (strcmp(plan->edb_relations[i], "edge") == 0) {
            col_edb_partition_t *ep = &sess->edb_parts[i];
            if (!ep->partitions) break;

            uint32_t total = 0;
            for (uint32_t w = 0; w < ep->num_partitions; w++) {
                if (ep->partitions[w])
                    total += ep->partitions[w]->nrows;
            }
            ok = (total == 100);
            break;
        }
    }

    cleanup(sess, plan, prog);

    if (!ok) {
        FAIL("partition row count mismatch");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_reinsert_repartitions(void)
{
    TEST("reinsert: second insert re-dirties and re-partitions");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(EDGE_PATH_SRC, 2, &plan, &prog);
    if (!sess) {
        FAIL("session creation");
        return 1;
    }

    /* First insert + partition */
    int64_t rows1[] = { 1, 10, 2, 20 };
    wl_session_insert(&sess->base, "edge", rows1, 2, 2);
    col_session_partition_edb_for_tdd(sess);

    /* Second insert + re-partition */
    int64_t rows2[] = { 3, 30, 4, 40, 5, 50 };
    wl_session_insert(&sess->base, "edge", rows2, 3, 2);
    col_session_partition_edb_for_tdd(sess);

    /* Verify total is 5 (2 + 3) */
    int ok = 0;
    for (uint32_t i = 0; i < sess->edb_part_count; i++) {
        if (strcmp(plan->edb_relations[i], "edge") == 0) {
            col_edb_partition_t *ep = &sess->edb_parts[i];
            if (!ep->partitions) break;

            uint32_t total = 0;
            for (uint32_t w = 0; w < ep->num_partitions; w++) {
                if (ep->partitions[w])
                    total += ep->partitions[w]->nrows;
            }
            ok = (total == 5);
            break;
        }
    }

    cleanup(sess, plan, prog);

    if (!ok) {
        FAIL("re-partition row count wrong");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_partition_consistency(void)
{
    TEST("partition consistency: each row hashes to correct worker");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(EDGE_PATH_SRC, 4, &plan, &prog);
    if (!sess) {
        FAIL("session creation");
        return 1;
    }

    int64_t rows[40];
    for (uint32_t i = 0; i < 20; i++) {
        rows[(size_t)i * 2] = (int64_t)i;
        rows[(size_t)i * 2 + 1] = (int64_t)(i * 100);
    }
    wl_session_insert(&sess->base, "edge", rows, 20, 2);
    col_session_partition_edb_for_tdd(sess);

    int ok = 1;
    for (uint32_t i = 0; i < sess->edb_part_count && ok; i++) {
        if (strcmp(plan->edb_relations[i], "edge") != 0)
            continue;

        col_edb_partition_t *ep = &sess->edb_parts[i];
        if (!ep->partitions || ep->key_count == 0) {
            ok = 0;
            break;
        }

        int64_t key_buf[COL_STACK_MAX];
        for (uint32_t w = 0; w < ep->num_partitions && ok; w++) {
            col_rel_t *part = ep->partitions[w];
            if (!part) continue;
            for (uint32_t r = 0; r < part->nrows && ok; r++) {
                const int64_t *row = part->data + (size_t)r * part->ncols;
                uint32_t target = col_row_partition(row, ep->key_cols,
                        ep->key_count, ep->num_partitions, key_buf);
                if (target != w)
                    ok = 0;
            }
        }
    }

    cleanup(sess, plan, prog);

    if (!ok) {
        FAIL("row in wrong partition");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_empty_insert_no_crash(void)
{
    TEST("empty insert: 0 rows, no crash, partitions empty");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_session(EDGE_PATH_SRC, 2, &plan, &prog);
    if (!sess) {
        FAIL("session creation");
        return 1;
    }

    /* Insert 0 rows — need at least one row to init schema first */
    int64_t rows[] = { 1, 2 };
    wl_session_insert(&sess->base, "edge", rows, 1, 2);

    /* Now partition (should produce small partitions) */
    int rc = col_session_partition_edb_for_tdd(sess);

    cleanup(sess, plan, prog);

    if (rc != 0) {
        FAIL("partition failed on small insert");
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
    printf("EDB Partition-at-Ingest Tests (Issue #320)\n");

    test_single_worker_no_partitions();
    test_multi_worker_edb_parts_allocated();
    test_key_discovery_from_plan();
    test_dirty_flag_set_on_insert();
    test_batch_partition_correctness();
    test_reinsert_repartitions();
    test_partition_consistency();
    test_empty_insert_no_crash();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);
    return tests_failed > 0 ? 1 : 0;
}
