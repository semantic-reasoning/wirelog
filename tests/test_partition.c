/*
 * test_partition.c - Unit tests for hash-partitioned relation storage
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: empty relation, single worker, disjoint partitions, roundtrip,
 * balance, skewed keys, schema propagation, multi-column keys,
 * key subset, invalid arguments.
 *
 * Issue #314: Hash-Partitioned Relation Storage
 */

#include "../wirelog/columnar/internal.h"

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

/* Build a relation with named columns. */
static col_rel_t *
make_rel_named(uint32_t ncols, const char *const *col_names,
    const int64_t *rows, uint32_t nrows)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, "test") != 0)
        return NULL;
    if (col_rel_set_schema(r, ncols, col_names) != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    for (uint32_t i = 0; i < nrows; i++) {
        if (col_rel_append_row(r, rows + (size_t)i * ncols) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

/* Sum nrows across all partitions. */
static uint32_t
partition_total_rows(col_rel_t **parts, uint32_t num_workers)
{
    uint32_t total = 0;
    for (uint32_t w = 0; w < num_workers; w++)
        total += parts[w]->nrows;
    return total;
}

/* Check that every row in src appears in exactly one partition.
 * Uses sorted comparison: sort a copy of src and merged partitions,
 * then memcmp. Returns 1 on success. */
static int
verify_partition_coverage(const col_rel_t *src, col_rel_t **parts,
    uint32_t num_workers)
{
    col_rel_t *merged = NULL;
    if (col_rel_merge_partitions(parts, num_workers, &merged) != 0)
        return 0;

    /* Create sorted copies for order-independent comparison */
    col_rel_t *src_copy = make_rel(src->ncols, src->data, src->nrows);
    if (!src_copy) {
        col_rel_destroy(merged);
        return 0;
    }

    col_rel_radix_sort_int64(src_copy);
    col_rel_radix_sort_int64(merged);

    int ok = (src_copy->nrows == merged->nrows)
        && (memcmp(src_copy->data, merged->data,
        (size_t)src_copy->nrows * src_copy->ncols * sizeof(int64_t))
        == 0);

    col_rel_destroy(src_copy);
    col_rel_destroy(merged);
    return ok;
}

/* Destroy all partitions in array. */
static void
destroy_parts(col_rel_t **parts, uint32_t num_workers)
{
    for (uint32_t w = 0; w < num_workers; w++) {
        if (parts[w]) {
            col_rel_destroy(parts[w]);
            parts[w] = NULL;
        }
    }
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static int
test_empty_relation(void)
{
    TEST("partition empty relation (0 rows, W=4)");

    col_rel_t *src = col_rel_new_auto("empty", 3);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[4] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 4, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    for (uint32_t w = 0; w < 4; w++) {
        if (parts[w]->nrows != 0) {
            FAIL("non-empty partition from empty source");
            destroy_parts(parts, 4);
            col_rel_destroy(src);
            return 1;
        }
    }

    destroy_parts(parts, 4);
    col_rel_destroy(src);
    PASS();
    return 0;
}

static int
test_single_worker(void)
{
    TEST("partition with single worker (W=1)");

    int64_t rows[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    col_rel_t *src = make_rel(3, rows, 4);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[1] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 1, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    if (parts[0]->nrows != 4) {
        FAIL("nrows mismatch");
        destroy_parts(parts, 1);
        col_rel_destroy(src);
        return 1;
    }

    if (memcmp(parts[0]->data, src->data, 12 * sizeof(int64_t)) != 0) {
        FAIL("data mismatch");
        destroy_parts(parts, 1);
        col_rel_destroy(src);
        return 1;
    }

    destroy_parts(parts, 1);
    col_rel_destroy(src);
    PASS();
    return 0;
}

static int
test_two_partitions_disjoint(void)
{
    TEST("two partitions are disjoint");

    /* 8 rows, 2 columns: key in col 0 */
    int64_t rows[] = {
        0, 100, 1, 101, 2, 102, 3, 103,
        4, 104, 5, 105, 6, 106, 7, 107,
    };
    col_rel_t *src = make_rel(2, rows, 8);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[2] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 2, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    if (partition_total_rows(parts, 2) != 8) {
        FAIL("total rows mismatch");
        destroy_parts(parts, 2);
        col_rel_destroy(src);
        return 1;
    }

    if (!verify_partition_coverage(src, parts, 2)) {
        FAIL("coverage check failed");
        destroy_parts(parts, 2);
        col_rel_destroy(src);
        return 1;
    }

    destroy_parts(parts, 2);
    col_rel_destroy(src);
    PASS();
    return 0;
}

static int
test_four_partitions_coverage(void)
{
    TEST("four partitions cover all rows (100 rows)");

    /* Generate 100 rows with 3 columns */
    int64_t rows[300];
    for (uint32_t i = 0; i < 100; i++) {
        rows[(size_t)i * 3 + 0] = (int64_t)i;
        rows[(size_t)i * 3 + 1] = (int64_t)i * 10;
        rows[(size_t)i * 3 + 2] = (int64_t)i * 100;
    }

    col_rel_t *src = make_rel(3, rows, 100);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[4] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 4, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    if (partition_total_rows(parts, 4) != 100) {
        char msg[128];
        snprintf(msg, sizeof(msg), "total %u != 100",
            partition_total_rows(parts, 4));
        FAIL(msg);
        destroy_parts(parts, 4);
        col_rel_destroy(src);
        return 1;
    }

    if (!verify_partition_coverage(src, parts, 4)) {
        FAIL("coverage check failed");
        destroy_parts(parts, 4);
        col_rel_destroy(src);
        return 1;
    }

    destroy_parts(parts, 4);
    col_rel_destroy(src);
    PASS();
    return 0;
}

static int
test_deterministic_hash(void)
{
    TEST("partition is deterministic (same input -> same output)");

    int64_t rows[] = { 10, 20, 30, 40, 50, 60, 70, 80, 90 };
    col_rel_t *src = make_rel(3, rows, 3);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts1[4] = { NULL };
    col_rel_t *parts2[4] = { NULL };
    uint32_t key_cols[] = { 0 };

    col_rel_partition_by_key(src, key_cols, 1, 4, parts1);
    col_rel_partition_by_key(src, key_cols, 1, 4, parts2);

    int ok = 1;
    for (uint32_t w = 0; w < 4; w++) {
        if (parts1[w]->nrows != parts2[w]->nrows) {
            ok = 0;
            break;
        }
        if (parts1[w]->nrows > 0
            && memcmp(parts1[w]->data, parts2[w]->data,
            (size_t)parts1[w]->nrows * 3 * sizeof(int64_t))
            != 0) {
            ok = 0;
            break;
        }
    }

    destroy_parts(parts1, 4);
    destroy_parts(parts2, 4);
    col_rel_destroy(src);

    if (!ok) {
        FAIL("non-deterministic partition");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_schema_propagation(void)
{
    TEST("partitions inherit column names from source");

    const char *names[] = { "x", "y", "z" };
    int64_t rows[] = { 1, 2, 3, 4, 5, 6 };
    col_rel_t *src = make_rel_named(3, names, rows, 2);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[2] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 2, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    int ok = 1;
    for (uint32_t w = 0; w < 2; w++) {
        if (parts[w]->ncols != 3) {
            ok = 0;
            break;
        }
        for (uint32_t c = 0; c < 3; c++) {
            if (!parts[w]->col_names[c]
                || strcmp(parts[w]->col_names[c], names[c]) != 0) {
                ok = 0;
                break;
            }
        }
    }

    destroy_parts(parts, 2);
    col_rel_destroy(src);

    if (!ok) {
        FAIL("col_names mismatch");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_multi_column_key(void)
{
    TEST("multi-column key (cols 0,2 of 3-col relation)");

    /* Rows with same (col0, col2) must land in same partition */
    int64_t rows[] = {
        1, 10, 100,   /* key = (1, 100) */
        1, 20, 100,   /* key = (1, 100) -- same */
        2, 30, 200,   /* key = (2, 200) */
        2, 40, 200,   /* key = (2, 200) -- same */
        3, 50, 300,   /* key = (3, 300) */
    };
    col_rel_t *src = make_rel(3, rows, 5);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[4] = { NULL };
    uint32_t key_cols[] = { 0, 2 };
    int rc = col_rel_partition_by_key(src, key_cols, 2, 4, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    /* Verify rows with same key (col0, col2) are in the same partition */
    int ok = 1;
    /* Rows 0 and 1 share key (1, 100) */
    /* Rows 2 and 3 share key (2, 200) */
    /* Find which partition row 0 is in, verify row 1 is also there */
    for (uint32_t w = 0; w < 4; w++) {
        for (uint32_t i = 0; i < parts[w]->nrows; i++) {
            int64_t *r = parts[w]->data + (size_t)i * 3;
            /* If we find key (1, 100), the partner row (col1 differs) must
             * also be in this partition */
            if (r[0] == 1 && r[2] == 100) {
                int found_partner = 0;
                for (uint32_t j = 0; j < parts[w]->nrows; j++) {
                    if (j == i)
                        continue;
                    int64_t *r2 = parts[w]->data + (size_t)j * 3;
                    if (r2[0] == 1 && r2[2] == 100) {
                        found_partner = 1;
                        break;
                    }
                }
                if (!found_partner) {
                    ok = 0;
                    break;
                }
            }
        }
    }

    if (!verify_partition_coverage(src, parts, 4))
        ok = 0;

    destroy_parts(parts, 4);
    col_rel_destroy(src);

    if (!ok) {
        FAIL("same-key rows in different partitions");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_merge_roundtrip(void)
{
    TEST("partition then merge produces identical tuple set");

    /* 20 rows, 4 columns */
    int64_t rows[80];
    for (uint32_t i = 0; i < 20; i++) {
        rows[(size_t)i * 4 + 0] = (int64_t)i;
        rows[(size_t)i * 4 + 1] = (int64_t)i * 2;
        rows[(size_t)i * 4 + 2] = (int64_t)i * 3;
        rows[(size_t)i * 4 + 3] = (int64_t)i * 4;
    }

    col_rel_t *src = make_rel(4, rows, 20);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[4] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 4, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    col_rel_t *merged = NULL;
    rc = col_rel_merge_partitions(parts, 4, &merged);
    if (rc != 0) {
        FAIL("merge returned error");
        destroy_parts(parts, 4);
        col_rel_destroy(src);
        return 1;
    }

    if (merged->nrows != src->nrows) {
        char msg[128];
        snprintf(msg, sizeof(msg), "nrows %u != %u", merged->nrows,
            src->nrows);
        FAIL(msg);
        col_rel_destroy(merged);
        destroy_parts(parts, 4);
        col_rel_destroy(src);
        return 1;
    }

    /* Sort both for order-independent comparison */
    col_rel_t *src_copy = make_rel(4, src->data, src->nrows);
    col_rel_radix_sort_int64(src_copy);
    col_rel_radix_sort_int64(merged);

    int ok = memcmp(src_copy->data, merged->data,
            (size_t)src_copy->nrows * 4 * sizeof(int64_t))
        == 0;

    col_rel_destroy(src_copy);
    col_rel_destroy(merged);
    destroy_parts(parts, 4);
    col_rel_destroy(src);

    if (!ok) {
        FAIL("data mismatch after roundtrip");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_partition_balance(void)
{
    TEST("uniform keys produce balanced partitions (within 10%%)");

    /* 1000 rows with sequential keys -> expect ~250 per partition (W=4) */
    int64_t rows[2000];
    for (uint32_t i = 0; i < 1000; i++) {
        rows[(size_t)i * 2 + 0] = (int64_t)i;
        rows[(size_t)i * 2 + 1] = (int64_t)i * 10;
    }

    col_rel_t *src = make_rel(2, rows, 1000);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[4] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 4, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    uint32_t expected = 1000 / 4; /* 250 */
    uint32_t margin = expected / 10; /* 25 = 10% */
    int ok = 1;
    for (uint32_t w = 0; w < 4; w++) {
        if (parts[w]->nrows < expected - margin
            || parts[w]->nrows > expected + margin) {
            char msg[128];
            snprintf(msg, sizeof(msg),
                "partition %u has %u rows (expected ~%u)",
                w, parts[w]->nrows, expected);
            FAIL(msg);
            ok = 0;
            break;
        }
    }

    destroy_parts(parts, 4);
    col_rel_destroy(src);

    if (ok)
        PASS();
    return ok ? 0 : 1;
}

static int
test_skewed_keys(void)
{
    TEST("skewed keys (all same key -> all in one partition)");

    /* All rows have the same key value */
    int64_t rows[] = { 42, 1, 42, 2, 42, 3, 42, 4, 42, 5 };
    col_rel_t *src = make_rel(2, rows, 5);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[4] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 4, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    /* Exactly one partition should have all 5 rows */
    uint32_t non_empty = 0;
    uint32_t total = 0;
    for (uint32_t w = 0; w < 4; w++) {
        if (parts[w]->nrows > 0)
            non_empty++;
        total += parts[w]->nrows;
    }

    int ok = (non_empty == 1 && total == 5);

    if (!ok)
        FAIL("skewed partition incorrect");

    if (ok && !verify_partition_coverage(src, parts, 4)) {
        FAIL("coverage check failed");
        ok = 0;
    }

    destroy_parts(parts, 4);
    col_rel_destroy(src);

    if (ok)
        PASS();
    return ok ? 0 : 1;
}

static int
test_preserves_all_columns(void)
{
    TEST("partition preserves all columns (5-col relation)");

    int64_t rows[] = {
        1, 2, 3, 4, 5,
        6, 7, 8, 9, 10,
        11, 12, 13, 14, 15,
        16, 17, 18, 19, 20,
    };
    col_rel_t *src = make_rel(5, rows, 4);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[2] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 2, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    /* Verify all 5 columns are intact in every partition row */
    int ok = 1;
    for (uint32_t w = 0; w < 2 && ok; w++) {
        if (parts[w]->ncols != 5) {
            ok = 0;
            break;
        }
        for (uint32_t i = 0; i < parts[w]->nrows && ok; i++) {
            int64_t *pr = parts[w]->data + (size_t)i * 5;
            /* Find this row in src */
            int found = 0;
            for (uint32_t j = 0; j < 4; j++) {
                if (memcmp(pr, src->data + (size_t)j * 5,
                    5 * sizeof(int64_t))
                    == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found)
                ok = 0;
        }
    }

    destroy_parts(parts, 2);
    col_rel_destroy(src);

    if (!ok) {
        FAIL("column data corrupted");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_many_workers_few_rows(void)
{
    TEST("more workers than rows (W=8, nrows=3)");

    int64_t rows[] = { 1, 10, 2, 20, 3, 30 };
    col_rel_t *src = make_rel(2, rows, 3);
    if (!src) {
        FAIL("alloc");
        return 1;
    }

    col_rel_t *parts[8] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 8, parts);
    if (rc != 0) {
        FAIL("partition returned error");
        col_rel_destroy(src);
        return 1;
    }

    if (partition_total_rows(parts, 8) != 3) {
        FAIL("total rows mismatch");
        destroy_parts(parts, 8);
        col_rel_destroy(src);
        return 1;
    }

    if (!verify_partition_coverage(src, parts, 8)) {
        FAIL("coverage check failed");
        destroy_parts(parts, 8);
        col_rel_destroy(src);
        return 1;
    }

    destroy_parts(parts, 8);
    col_rel_destroy(src);
    PASS();
    return 0;
}

static int
test_invalid_null_src(void)
{
    TEST("invalid args: NULL src returns EINVAL");

    col_rel_t *parts[2] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(NULL, key_cols, 1, 2, parts);
    if (rc != EINVAL) {
        FAIL("expected EINVAL");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_invalid_zero_key_count(void)
{
    TEST("invalid args: key_count=0 returns EINVAL");

    col_rel_t *src = col_rel_new_auto("test", 2);
    col_rel_t *parts[2] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 0, 2, parts);
    col_rel_destroy(src);

    if (rc != EINVAL) {
        FAIL("expected EINVAL");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_invalid_key_out_of_bounds(void)
{
    TEST("invalid args: key_col >= ncols returns EINVAL");

    col_rel_t *src = col_rel_new_auto("test", 3);
    col_rel_t *parts[2] = { NULL };
    uint32_t key_cols[] = { 5 }; /* out of bounds for 3-col relation */
    int rc = col_rel_partition_by_key(src, key_cols, 1, 2, parts);
    col_rel_destroy(src);

    if (rc != EINVAL) {
        FAIL("expected EINVAL");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_invalid_zero_workers(void)
{
    TEST("invalid args: num_workers=0 returns EINVAL");

    col_rel_t *src = col_rel_new_auto("test", 2);
    col_rel_t *parts[1] = { NULL };
    uint32_t key_cols[] = { 0 };
    int rc = col_rel_partition_by_key(src, key_cols, 1, 0, parts);
    col_rel_destroy(src);

    if (rc != EINVAL) {
        FAIL("expected EINVAL");
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
    printf("Hash-Partitioned Relation Storage Tests (Issue #314)\n");

    test_empty_relation();
    test_single_worker();
    test_two_partitions_disjoint();
    test_four_partitions_coverage();
    test_deterministic_hash();
    test_schema_propagation();
    test_multi_column_key();
    test_merge_roundtrip();
    test_partition_balance();
    test_skewed_keys();
    test_preserves_all_columns();
    test_many_workers_few_rows();
    test_invalid_null_src();
    test_invalid_zero_key_count();
    test_invalid_key_out_of_bounds();
    test_invalid_zero_workers();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);
    return tests_failed > 0 ? 1 : 0;
}
