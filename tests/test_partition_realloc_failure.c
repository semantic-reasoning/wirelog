/* A failed column resize must discard the unpublished partition/merge result. */

#include "../wirelog/columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <xxhash.h>

void *__real_realloc(void *ptr, size_t size);

static size_t fail_size;
static uint32_t fail_ordinal;
static uint32_t matching_calls;
static col_rel_t **observed_parts;
static int saw_prior_worker_at_failure;
static uint32_t matching_worker1_calls;
static uint32_t matching_other_calls;

void *
__wrap_realloc(void *ptr, size_t size)
{
    if (fail_ordinal != 0 && size == fail_size) {
        if (observed_parts && observed_parts[0]
            && observed_parts[1] == NULL)
            matching_worker1_calls++;
        else
            matching_other_calls++;
        if (++matching_calls == fail_ordinal) {
            saw_prior_worker_at_failure
                = matching_worker1_calls == matching_calls;
            return NULL;
        }
    }
    return __real_realloc(ptr, size);
}

static void
arm_failure(uint32_t capacity, uint32_t ordinal)
{
    matching_calls = 0;
    matching_worker1_calls = 0;
    matching_other_calls = 0;
    fail_size = (size_t)capacity * sizeof(int64_t);
    fail_ordinal = ordinal;
}

static void
disarm_failure(void)
{
    fail_ordinal = 0;
    observed_parts = NULL;
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

static uint32_t
worker_for_row(uint32_t row)
{
    int64_t key = value_at(row, 0);
    return (uint32_t)(XXH3_64bits(&key, sizeof(key)) % 2u);
}

static uint32_t
choose_two_worker_rows(uint32_t counts[2])
{
    for (uint32_t nrows = 70; nrows <= 100; nrows++) {
        counts[0] = 0;
        counts[1] = 0;
        for (uint32_t row = 0; row < nrows; row++)
            counts[worker_for_row(row)]++;
        if (counts[0] > 0 && counts[1] > 0
            && counts[0] != counts[1]
            && counts[0] != COL_REL_INIT_CAP
            && counts[1] != COL_REL_INIT_CAP)
            return nrows;
    }
    return 0;
}

static int
partition_tuple_set(const col_rel_t *const parts[2], uint32_t nrows,
    const uint32_t counts[2])
{
    uint8_t seen[101] = { 0 };
    if (!parts[0] || !parts[1]
        || parts[0]->nrows != counts[0]
        || parts[1]->nrows != counts[1]
        || parts[0]->capacity != counts[0]
        || parts[1]->capacity != counts[1])
        return 0;
    for (uint32_t worker = 0; worker < 2; worker++) {
        for (uint32_t r = 0; r < parts[worker]->nrows; r++) {
            int64_t key = parts[worker]->columns[0][r];
            if (key < 0 || key >= (int64_t)nrows
                || worker_for_row((uint32_t)key) != worker
                || seen[key]++)
                return 0;
            for (uint32_t c = 1; c < 3; c++)
                if (parts[worker]->columns[c][r]
                    != value_at((uint32_t)key, c))
                    return 0;
        }
    }
    for (uint32_t row = 0; row < nrows; row++)
        if (seen[row] != 1)
            return 0;
    return 1;
}

static int
merged_tuple_set(const col_rel_t *merged, uint32_t nrows)
{
    uint8_t seen[101] = { 0 };
    if (!merged || merged->nrows != nrows || merged->ncols != 3)
        return 0;
    for (uint32_t r = 0; r < nrows; r++) {
        int64_t key = merged->columns[0][r];
        if (key < 0 || key >= (int64_t)nrows || seen[key]++)
            return 0;
        for (uint32_t c = 1; c < 3; c++)
            if (merged->columns[c][r] != value_at((uint32_t)key, c))
                return 0;
    }
    for (uint32_t row = 0; row < nrows; row++)
        if (seen[row] != 1)
            return 0;
    return 1;
}

static int
check_late_partition_dry_run(uint32_t nrows,
    const uint32_t counts[2])
{
    col_rel_t *src = make_relation(3, 0, nrows);
    if (!src)
        return 0;
    const uint32_t key = 0;
    col_rel_t *parts[2] = { NULL, NULL };
    arm_failure(counts[1], UINT32_MAX);
    observed_parts = parts;
    int rc = col_rel_partition_by_key(src, &key, 1, 2, parts);
    disarm_failure();
    int ok = rc == 0 && matching_calls == 3
        && matching_worker1_calls == 3 && matching_other_calls == 0
        && partition_tuple_set((const col_rel_t *const *)parts,
            nrows, counts);
    col_rel_destroy(parts[0]);
    col_rel_destroy(parts[1]);
    col_rel_destroy(src);
    return ok;
}

static int
check_late_partition_failure(uint32_t nrows, const uint32_t counts[2],
    uint32_t ordinal)
{
    col_rel_t *src = make_relation(3, 0, nrows);
    if (!src)
        return 0;
    const uint32_t source_capacity = src->capacity;
    const uint64_t source_view = src->view_generation;
    const uint64_t source_storage = src->storage_generation;
    int64_t **source_columns = src->columns;
    int64_t *source_column_ptrs[8] = {
        src->columns[0], src->columns[1], src->columns[2]
    };
    const uint32_t key = 0;
    col_rel_t *parts[2] = {
        (col_rel_t *)(uintptr_t)1, (col_rel_t *)(uintptr_t)1
    };

    arm_failure(counts[1], ordinal);
    observed_parts = parts;
    saw_prior_worker_at_failure = 0;
    int rc = col_rel_partition_by_key(src, &key, 1, 2, parts);
    disarm_failure();
    int ok = rc == ENOMEM && matching_calls == ordinal
        && saw_prior_worker_at_failure
        && parts[0] == NULL && parts[1] == NULL
        && source_intact(src, 0, nrows, source_capacity, source_columns,
            source_column_ptrs)
        && src->view_generation == source_view
        && src->storage_generation == source_storage;

    if (ok) {
        rc = col_rel_partition_by_key(src, &key, 1, 2, parts);
        ok = rc == 0
            && partition_tuple_set((const col_rel_t *const *)parts,
                nrows, counts)
            && source_intact(src, 0, nrows, source_capacity,
                source_columns, source_column_ptrs)
            && src->view_generation == source_view
            && src->storage_generation == source_storage;
    }
    if (ok) {
        col_rel_t *merged = NULL;
        rc = col_rel_merge_partitions(parts, 2, &merged);
        ok = rc == 0 && merged_tuple_set(merged, nrows)
            && source_intact(src, 0, nrows, source_capacity,
                source_columns, source_column_ptrs)
            && src->view_generation == source_view
            && src->storage_generation == source_storage;
        col_rel_destroy(merged);
    }
    for (uint32_t w = 0; w < 2; w++)
        if (parts[w] && parts[w] != (col_rel_t *)(uintptr_t)1)
            col_rel_destroy(parts[w]);
    col_rel_destroy(src);
    return ok;
}

int
main(void)
{
    const uint32_t column_counts[] = { 1, 3, 8 };
    const uint32_t partition_rows[] = { 3, 65 };
    const uint32_t merge_rows[] = { 4, 66 };
    int failures = 0;
    uint32_t counts[2];
    uint32_t two_worker_rows = choose_two_worker_rows(counts);
    if (two_worker_rows == 0) {
        fputs("no valid unequal two-worker partition fixture\n", stderr);
        return 1;
    }
    printf("late partition fixture: %u rows, buckets %u/%u\n",
        two_worker_rows, counts[0], counts[1]);
    if (!check_late_partition_dry_run(two_worker_rows, counts)) {
        fputs("late partition dry run did not isolate three worker1 "
            "column reallocations\n", stderr);
        return 1;
    }
    for (uint32_t ordinal = 1; ordinal <= 3; ordinal++) {
        if (!check_late_partition_failure(two_worker_rows, counts,
            ordinal)) {
            fprintf(stderr, "late partition: %u rows, buckets %u/%u,"
                " failure at column %u\n", two_worker_rows,
                counts[0], counts[1], ordinal);
            failures++;
        }
    }
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
