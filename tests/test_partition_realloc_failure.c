/* A failed column resize must discard the unpublished partition/merge result. */

#include "../wirelog/columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

void *__real_realloc(void *ptr, size_t size);

static size_t fail_size;
static uint32_t fail_ordinal;
static uint32_t matching_calls;

void *
__wrap_realloc(void *ptr, size_t size)
{
    if (fail_ordinal != 0 && size == fail_size
        && ++matching_calls == fail_ordinal)
        return NULL;
    return __real_realloc(ptr, size);
}

static void
arm_failure(uint32_t capacity, uint32_t ordinal)
{
    matching_calls = 0;
    fail_size = (size_t)capacity * sizeof(int64_t);
    fail_ordinal = ordinal;
}

static void
disarm_failure(void)
{
    fail_ordinal = 0;
}

static int64_t
value_at(uint32_t row, uint32_t col)
{
    return (int64_t)(1000u * col + row);
}

static col_rel_t *
make_relation(uint32_t ncols, uint32_t start, uint32_t nrows)
{
    col_rel_t *rel = col_rel_new_auto("source", ncols);
    if (!rel)
        return NULL;
    int64_t values[8];
    for (uint32_t row = start; row < start + nrows; row++) {
        for (uint32_t col = 0; col < ncols; col++)
            values[col] = value_at(row, col);
        if (col_rel_append_row(rel, values) != 0) {
            col_rel_destroy(rel);
            return NULL;
        }
    }
    return rel;
}

static int
source_intact(const col_rel_t *rel, uint32_t start, uint32_t nrows,
    uint32_t capacity, int64_t *const *outer,
    int64_t *const column_ptrs[8])
{
    if (rel->nrows != nrows || rel->capacity != capacity
        || rel->columns != outer)
        return 0;
    for (uint32_t col = 0; col < rel->ncols; col++) {
        if (rel->columns[col] != column_ptrs[col])
            return 0;
        for (uint32_t row = 0; row < nrows; row++)
            if (rel->columns[col][row] != value_at(start + row, col))
                return 0;
    }
    return 1;
}

static int
rows_match(const col_rel_t *rel, uint32_t start, uint32_t nrows)
{
    if (!rel || rel->nrows != nrows)
        return 0;
    for (uint32_t col = 0; col < rel->ncols; col++)
        for (uint32_t row = 0; row < nrows; row++)
            if (rel->columns[col][row] != value_at(start + row, col))
                return 0;
    return 1;
}

static int
check_partition(uint32_t ncols, uint32_t nrows, uint32_t ordinal)
{
    col_rel_t *src = make_relation(ncols, 0, nrows);
    if (!src)
        return 0;
    const uint32_t source_capacity = src->capacity;
    int64_t **source_columns = src->columns;
    int64_t *source_column_ptrs[8];
    for (uint32_t col = 0; col < ncols; col++)
        source_column_ptrs[col] = src->columns[col];
    const uint32_t key = 0;
    col_rel_t *parts[1] = { (col_rel_t *)(uintptr_t)1 };

    arm_failure(nrows, ordinal);
    int rc = col_rel_partition_by_key(src, &key, 1, 1, parts);
    disarm_failure();
    int ok = rc == ENOMEM && matching_calls == ordinal
        && parts[0] == NULL
        && source_intact(src, 0, nrows, source_capacity, source_columns,
            source_column_ptrs);

    /* A fresh call must still construct the complete partition. */
    if (ok) {
        rc = col_rel_partition_by_key(src, &key, 1, 1, parts);
        ok = rc == 0 && rows_match(parts[0], 0, nrows)
            && source_intact(src, 0, nrows, source_capacity,
                source_columns, source_column_ptrs);
    }
    if (parts[0] && parts[0] != (col_rel_t *)(uintptr_t)1)
        col_rel_destroy(parts[0]);
    col_rel_destroy(src);
    return ok;
}

static int
check_merge(uint32_t ncols, uint32_t total, uint32_t ordinal)
{
    uint32_t first_rows = total / 2;
    col_rel_t *parts[2] = {
        make_relation(ncols, 0, first_rows),
        make_relation(ncols, first_rows, total - first_rows),
    };
    if (!parts[0] || !parts[1]) {
        col_rel_destroy(parts[0]);
        col_rel_destroy(parts[1]);
        return 0;
    }
    const uint32_t cap0 = parts[0]->capacity;
    const uint32_t cap1 = parts[1]->capacity;
    int64_t **cols0 = parts[0]->columns;
    int64_t **cols1 = parts[1]->columns;
    int64_t *column_ptrs0[8];
    int64_t *column_ptrs1[8];
    for (uint32_t col = 0; col < ncols; col++) {
        column_ptrs0[col] = cols0[col];
        column_ptrs1[col] = cols1[col];
    }
    col_rel_t *merged = (col_rel_t *)(uintptr_t)1;

    arm_failure(total, ordinal);
    int rc = col_rel_merge_partitions(parts, 2, &merged);
    disarm_failure();
    int ok = rc == ENOMEM && matching_calls == ordinal
        && merged == (col_rel_t *)(uintptr_t)1
        && source_intact(parts[0], 0, first_rows, cap0, cols0,
            column_ptrs0)
        && source_intact(parts[1], first_rows, total - first_rows,
            cap1, cols1, column_ptrs1);

    if (ok) {
        rc = col_rel_merge_partitions(parts, 2, &merged);
        ok = rc == 0 && rows_match(merged, 0, total)
            && source_intact(parts[0], 0, first_rows, cap0, cols0,
                column_ptrs0)
            && source_intact(parts[1], first_rows, total - first_rows,
                cap1, cols1, column_ptrs1);
    }
    if (merged && merged != (col_rel_t *)(uintptr_t)1)
        col_rel_destroy(merged);
    col_rel_destroy(parts[0]);
    col_rel_destroy(parts[1]);
    return ok;
}

int
main(void)
{
    const uint32_t column_counts[] = { 1, 3, 8 };
    const uint32_t partition_rows[] = { 3, 65 };
    const uint32_t merge_rows[] = { 4, 66 };
    int failures = 0;
    for (size_t c = 0; c < sizeof(column_counts) / sizeof(column_counts[0]);
        c++) {
        for (size_t shape = 0; shape < 2; shape++) {
            for (uint32_t ordinal = 1; ordinal <= column_counts[c];
                ordinal++) {
                if (!check_partition(column_counts[c],
                    partition_rows[shape], ordinal)) {
                    fprintf(stderr, "partition: %u columns, %u rows,"
                        " failure at column %u\n", column_counts[c],
                        partition_rows[shape], ordinal);
                    failures++;
                }
                if (!check_merge(column_counts[c], merge_rows[shape],
                    ordinal)) {
                    fprintf(stderr, "merge: %u columns, %u rows,"
                        " failure at column %u\n", column_counts[c],
                        merge_rows[shape], ordinal);
                    failures++;
                }
            }
        }
    }
    printf("partition realloc fault cases: %s\n",
        failures == 0 ? "PASS" : "FAIL");
    return failures == 0 ? 0 : 1;
}
