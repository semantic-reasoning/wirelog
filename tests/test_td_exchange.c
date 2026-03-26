/*
 * test_td_exchange.c - Unit tests for col_op_exchange (Issue #316)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: single-worker no-op, correctness (W=4), empty input,
 * multi-column preservation, timestamp preservation, skewed keys,
 * invalid arguments, large relation.
 *
 * Issue #316: Exchange Operator - Redistribute tuples by hash(key) % W
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan.h"
#include "../wirelog/columnar/columnar_nanoarrow.h"

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

/* Build a relation with ncols columns from a flat row-major array. */
static col_rel_t *
make_rel(uint32_t ncols, const int64_t *rows, uint32_t nrows)
{
    col_rel_t *r = col_rel_new_auto("test", ncols);

    if (!r)
        return NULL;
    for (uint32_t i = 0; i < nrows; i++) {
        if (col_rel_append_row(r, rows + (size_t)i * ncols) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

/*
 * Allocate a minimal coordinator session for exchange tests.
 * Only exchange_bufs / exchange_num_workers are set; all other
 * fields remain zero.  exchange_bufs[0][0..W-1] is allocated
 * for worker 0 (the coordinator acting as sole scatter worker).
 *
 * Caller must free exchange_bufs[0] then exchange_bufs then the
 * struct itself.  Use free_exchange_coord() for safe teardown.
 */
static wl_col_session_t *
make_exchange_coord(uint32_t num_workers)
{
    wl_col_session_t *coord
        = (wl_col_session_t *)calloc(1, sizeof(wl_col_session_t));

    if (!coord)
        return NULL;

    coord->exchange_bufs = (col_rel_t ***)( /* NOLINT */
        calloc(num_workers, sizeof(col_rel_t **)));
    if (!coord->exchange_bufs) {
        free(coord);
        return NULL;
    }

    coord->exchange_bufs[0] = (col_rel_t **)(  /* NOLINT */
        calloc(num_workers, sizeof(col_rel_t *)));
    if (!coord->exchange_bufs[0]) {
        free(coord->exchange_bufs);
        free(coord);
        return NULL;
    }

    coord->exchange_num_workers = num_workers;
    /* coordinator = NULL: coord IS the coordinator; worker_id = 0 */
    return coord;
}

/* Tear down a coordinator session created by make_exchange_coord. */
static void
free_exchange_coord(wl_col_session_t *coord)
{
    if (!coord)
        return;
    if (coord->exchange_bufs) {
        uint32_t w = coord->exchange_num_workers;
        for (uint32_t i = 0; i < w; i++) {
            if (coord->exchange_bufs[i]) {
                for (uint32_t j = 0; j < w; j++)
                    col_rel_destroy(coord->exchange_bufs[i][j]);
                free(coord->exchange_bufs[i]);
            }
        }
        free(coord->exchange_bufs);
    }
    free(coord);
}

/* Build a wl_plan_op_exchange_t + wl_plan_op_t for the given key columns. */
static wl_plan_op_t
make_exchange_op(uint32_t num_workers, const uint32_t *key_cols,
    uint32_t key_count)
{
    wl_plan_op_exchange_t *meta
        = (wl_plan_op_exchange_t *)calloc(1, sizeof(wl_plan_op_exchange_t));

    meta->num_workers = num_workers;
    meta->key_col_count = key_count;
    meta->key_col_idxs
        = (uint32_t *)malloc(key_count * sizeof(uint32_t));
    memcpy(meta->key_col_idxs, key_cols, key_count * sizeof(uint32_t));

    wl_plan_op_t op;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_EXCHANGE;
    op.opaque_data = meta;
    return op;
}

/* Free opaque_data allocated by make_exchange_op. */
static void
free_exchange_op_data(wl_plan_op_t *op)
{
    if (!op->opaque_data)
        return;
    wl_plan_op_exchange_t *meta = (wl_plan_op_exchange_t *)op->opaque_data;

    free(meta->key_col_idxs);
    free(meta);
    op->opaque_data = NULL;
}

/* Sum nrows across exchange_bufs[0][0..W-1]. */
static uint64_t
exchange_total_rows(wl_col_session_t *coord)
{
    uint64_t total = 0;
    uint32_t w = coord->exchange_num_workers;

    for (uint32_t i = 0; i < w; i++) {
        if (coord->exchange_bufs[0][i])
            total += coord->exchange_bufs[0][i]->nrows;
    }
    return total;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/*
 * test_exchange_single_worker_noop:
 * W=1 must leave the eval stack completely unchanged (no scatter).
 */
static void
test_exchange_single_worker_noop(void)
{
    TEST("single_worker_noop");

    /* W=1 exchange op */
    uint32_t key_cols[] = { 0 };
    wl_plan_op_t op = make_exchange_op(1, key_cols, 1);

    /* Build a 1-column relation with 5 rows */
    int64_t data[] = { 1, 2, 3, 4, 5 };
    col_rel_t *rel = make_rel(1, data, 5);

    if (!rel) {
        FAIL("make_rel returned NULL");
        free_exchange_op_data(&op);
        return;
    }

    /* Session: coordinator=NULL, no exchange_bufs needed for W=1 */
    wl_col_session_t sess;
    memset(&sess, 0, sizeof(sess));

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_exchange(&op, &stack, &sess);

    if (rc != 0) {
        FAIL("col_op_exchange returned non-zero for W=1");
        eval_stack_drain(&stack);
        free_exchange_op_data(&op);
        return;
    }

    /* Stack must still have exactly 1 item (the original rel, unchanged) */
    if (stack.top != 1) {
        FAIL("stack top changed for W=1 no-op");
        eval_stack_drain(&stack);
        free_exchange_op_data(&op);
        return;
    }

    eval_entry_t e = eval_stack_pop(&stack);

    if (e.rel != rel || e.rel->nrows != 5) {
        FAIL("stack item modified for W=1 no-op");
        if (e.owned)
            col_rel_destroy(e.rel);
        free_exchange_op_data(&op);
        return;
    }

    if (e.owned)
        col_rel_destroy(e.rel);
    free_exchange_op_data(&op);
    PASS();
}

/*
 * test_exchange_w4_correctness:
 * W=4, 100 rows: total rows across all partitions == 100,
 * no row lost or duplicated.
 */
static void
test_exchange_w4_correctness(void)
{
    TEST("w4_correctness");

    const uint32_t nrows = 100;
    const uint32_t W = 4;

    int64_t data[100];

    for (uint32_t i = 0; i < nrows; i++)
        data[i] = (int64_t)(i + 1);

    col_rel_t *rel = make_rel(1, data, nrows);

    if (!rel) {
        FAIL("make_rel returned NULL");
        return;
    }

    wl_col_session_t *coord = make_exchange_coord(W);

    if (!coord) {
        col_rel_destroy(rel);
        FAIL("make_exchange_coord returned NULL");
        return;
    }

    uint32_t key_cols[] = { 0 };
    wl_plan_op_t op = make_exchange_op(W, key_cols, 1);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_exchange(&op, &stack, coord);

    if (rc != 0) {
        FAIL("col_op_exchange failed");
        eval_stack_drain(&stack);
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    /* Stack must be empty (input consumed) */
    if (stack.top != 0) {
        FAIL("stack not empty after W>1 exchange");
        eval_stack_drain(&stack);
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    uint64_t total = exchange_total_rows(coord);

    if (total != nrows) {
        FAIL("row count mismatch");
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    free_exchange_coord(coord);
    free_exchange_op_data(&op);
    PASS();
}

/*
 * test_exchange_empty_input:
 * 0-row relation: no crash, all partitions empty, returns 0.
 */
static void
test_exchange_empty_input(void)
{
    TEST("empty_input");

    const uint32_t W = 4;
    col_rel_t *rel = make_rel(1, NULL, 0);

    if (!rel) {
        FAIL("make_rel returned NULL");
        return;
    }

    wl_col_session_t *coord = make_exchange_coord(W);

    if (!coord) {
        col_rel_destroy(rel);
        FAIL("make_exchange_coord returned NULL");
        return;
    }

    uint32_t key_cols[] = { 0 };
    wl_plan_op_t op = make_exchange_op(W, key_cols, 1);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_exchange(&op, &stack, coord);

    if (rc != 0) {
        FAIL("col_op_exchange failed on empty input");
        eval_stack_drain(&stack);
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    uint64_t total = exchange_total_rows(coord);

    if (total != 0) {
        FAIL("non-zero rows from empty input");
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    free_exchange_coord(coord);
    free_exchange_op_data(&op);
    PASS();
}

/*
 * test_exchange_preserves_columns:
 * 3-column relation: all 3 columns are present in every partition.
 */
static void
test_exchange_preserves_columns(void)
{
    TEST("preserves_columns");

    const uint32_t W = 2;
    const uint32_t ncols = 3;
    const uint32_t nrows = 20;

    int64_t data[60]; /* 20 rows × 3 cols */

    for (uint32_t i = 0; i < nrows; i++) {
        data[i * ncols + 0] = (int64_t)i;
        data[i * ncols + 1] = (int64_t)(i * 10);
        data[i * ncols + 2] = (int64_t)(i * 100);
    }

    col_rel_t *rel = make_rel(ncols, data, nrows);

    if (!rel) {
        FAIL("make_rel returned NULL");
        return;
    }

    wl_col_session_t *coord = make_exchange_coord(W);

    if (!coord) {
        col_rel_destroy(rel);
        FAIL("make_exchange_coord returned NULL");
        return;
    }

    uint32_t key_cols[] = { 0 };
    wl_plan_op_t op = make_exchange_op(W, key_cols, 1);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_exchange(&op, &stack, coord);

    if (rc != 0) {
        FAIL("col_op_exchange failed");
        eval_stack_drain(&stack);
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    /* Every non-empty partition must have ncols columns */
    int ok = 1;

    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *part = coord->exchange_bufs[0][w];

        if (part && part->nrows > 0 && part->ncols != ncols) {
            ok = 0;
            break;
        }
    }

    if (!ok) {
        FAIL("partition has wrong ncols");
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    /* Total rows preserved */
    if (exchange_total_rows(coord) != nrows) {
        FAIL("row count mismatch");
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    free_exchange_coord(coord);
    free_exchange_op_data(&op);
    PASS();
}

/*
 * test_exchange_preserves_timestamps:
 * Relation with timestamps: timestamps must follow rows into partitions.
 */
static void
test_exchange_preserves_timestamps(void)
{
    TEST("preserves_timestamps");

    const uint32_t W = 2;
    const uint32_t nrows = 10;

    col_rel_t *rel = make_rel(1, NULL, 0);

    if (!rel) {
        FAIL("make_rel returned NULL");
        return;
    }

    /* Manually append rows and timestamps */
    for (uint32_t i = 0; i < nrows; i++) {
        int64_t val = (int64_t)(i + 1);

        if (col_rel_append_row(rel, &val) != 0) {
            col_rel_destroy(rel);
            FAIL("col_rel_append_row failed");
            return;
        }
    }

    rel->timestamps = (col_delta_timestamp_t *)calloc(
        nrows, sizeof(col_delta_timestamp_t));
    if (!rel->timestamps) {
        col_rel_destroy(rel);
        FAIL("calloc timestamps failed");
        return;
    }

    for (uint32_t i = 0; i < nrows; i++) {
        rel->timestamps[i].iteration = i;
        rel->timestamps[i].stratum = 0;
    }

    wl_col_session_t *coord = make_exchange_coord(W);

    if (!coord) {
        col_rel_destroy(rel);
        FAIL("make_exchange_coord returned NULL");
        return;
    }

    uint32_t key_cols[] = { 0 };
    wl_plan_op_t op = make_exchange_op(W, key_cols, 1);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_exchange(&op, &stack, coord);

    if (rc != 0) {
        FAIL("col_op_exchange failed");
        eval_stack_drain(&stack);
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    /* Every non-empty partition must have timestamps */
    int ok = 1;

    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *part = coord->exchange_bufs[0][w];

        if (part && part->nrows > 0 && !part->timestamps) {
            ok = 0;
            break;
        }
    }

    if (!ok) {
        FAIL("partition missing timestamps");
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    if (exchange_total_rows(coord) != nrows) {
        FAIL("row count mismatch");
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    free_exchange_coord(coord);
    free_exchange_op_data(&op);
    PASS();
}

/*
 * test_exchange_all_same_key:
 * All rows have the same key value → all land in the same partition,
 * the other W-1 partitions are empty (0 rows).
 */
static void
test_exchange_all_same_key(void)
{
    TEST("all_same_key");

    const uint32_t W = 4;
    const uint32_t nrows = 50;

    int64_t data[50];

    for (uint32_t i = 0; i < nrows; i++)
        data[i] = 42LL; /* all identical */

    col_rel_t *rel = make_rel(1, data, nrows);

    if (!rel) {
        FAIL("make_rel returned NULL");
        return;
    }

    wl_col_session_t *coord = make_exchange_coord(W);

    if (!coord) {
        col_rel_destroy(rel);
        FAIL("make_exchange_coord returned NULL");
        return;
    }

    uint32_t key_cols[] = { 0 };
    wl_plan_op_t op = make_exchange_op(W, key_cols, 1);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_exchange(&op, &stack, coord);

    if (rc != 0) {
        FAIL("col_op_exchange failed");
        eval_stack_drain(&stack);
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    /* Exactly one partition must have all nrows rows */
    uint32_t full_count = 0;

    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *part = coord->exchange_bufs[0][w];

        if (part && part->nrows == nrows)
            full_count++;
        else if (part && part->nrows != 0) {
            FAIL("unexpected split of identical-key rows");
            free_exchange_coord(coord);
            free_exchange_op_data(&op);
            return;
        }
    }

    if (full_count != 1) {
        FAIL("all-same-key rows not in exactly one partition");
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    free_exchange_coord(coord);
    free_exchange_op_data(&op);
    PASS();
}

/*
 * test_exchange_invalid_args:
 * NULL opaque_data and key_col_idx out-of-range must return EINVAL.
 */
static void
test_exchange_invalid_args(void)
{
    TEST("invalid_args");

    wl_col_session_t sess;
    memset(&sess, 0, sizeof(sess));

    eval_stack_t stack;
    eval_stack_init(&stack);

    /* NULL opaque_data */
    wl_plan_op_t op;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_EXCHANGE;
    op.opaque_data = NULL;

    int64_t val = 1;
    col_rel_t *rel = make_rel(1, &val, 1);

    if (!rel) {
        FAIL("make_rel returned NULL");
        return;
    }

    eval_stack_push(&stack, rel, true);
    int rc = col_op_exchange(&op, &stack, &sess);

    if (rc != EINVAL) {
        FAIL("expected EINVAL for NULL opaque_data");
        eval_stack_drain(&stack);
        col_rel_destroy(rel);
        return;
    }
    /* rel stays on stack (was not popped for W=1-ish or NULL meta) */
    eval_stack_drain(&stack);

    /* key_col_idx out of range for W=2 */
    col_rel_t *rel2 = make_rel(1, &val, 1);

    if (!rel2) {
        FAIL("make_rel returned NULL");
        return;
    }

    wl_col_session_t *coord = make_exchange_coord(2);

    if (!coord) {
        col_rel_destroy(rel2);
        FAIL("make_exchange_coord returned NULL");
        return;
    }

    uint32_t bad_key = 99; /* col 99 doesn't exist in a 1-col relation */
    wl_plan_op_t op2 = make_exchange_op(2, &bad_key, 1);

    eval_stack_init(&stack);
    eval_stack_push(&stack, rel2, true);
    rc = col_op_exchange(&op2, &stack, coord);

    if (rc != EINVAL) {
        FAIL("expected EINVAL for out-of-range key col");
        eval_stack_drain(&stack);
        free_exchange_coord(coord);
        free_exchange_op_data(&op2);
        return;
    }

    eval_stack_drain(&stack);
    free_exchange_coord(coord);
    free_exchange_op_data(&op2);
    PASS();
}

/*
 * test_exchange_large_relation:
 * 1000 rows, W=8: total rows preserved after scatter.
 */
static void
test_exchange_large_relation(void)
{
    TEST("large_relation");

    const uint32_t nrows = 1000;
    const uint32_t W = 8;

    int64_t *data = (int64_t *)malloc(nrows * sizeof(int64_t));

    if (!data) {
        FAIL("malloc failed");
        return;
    }

    for (uint32_t i = 0; i < nrows; i++)
        data[i] = (int64_t)(i * 7 + 3); /* varied keys */

    col_rel_t *rel = make_rel(1, data, nrows);
    free(data);

    if (!rel) {
        FAIL("make_rel returned NULL");
        return;
    }

    wl_col_session_t *coord = make_exchange_coord(W);

    if (!coord) {
        col_rel_destroy(rel);
        FAIL("make_exchange_coord returned NULL");
        return;
    }

    uint32_t key_cols[] = { 0 };
    wl_plan_op_t op = make_exchange_op(W, key_cols, 1);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_exchange(&op, &stack, coord);

    if (rc != 0) {
        FAIL("col_op_exchange failed");
        eval_stack_drain(&stack);
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    uint64_t total = exchange_total_rows(coord);

    if (total != nrows) {
        FAIL("row count mismatch after large scatter");
        free_exchange_coord(coord);
        free_exchange_op_data(&op);
        return;
    }

    free_exchange_coord(coord);
    free_exchange_op_data(&op);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("Exchange Operator Tests\n");
    printf("=======================\n");

    test_exchange_single_worker_noop();
    test_exchange_w4_correctness();
    test_exchange_empty_input();
    test_exchange_preserves_columns();
    test_exchange_preserves_timestamps();
    test_exchange_all_same_key();
    test_exchange_invalid_args();
    test_exchange_large_relation();

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
