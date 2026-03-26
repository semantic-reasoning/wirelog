/*
 * columnar/partition.c - Hash-Partitioned Relation Storage
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements hash-based data partitioning for col_rel_t relations.
 * Each relation is split into W disjoint partitions by
 * hash(key_columns) % W, enabling data-parallel scaling where each
 * worker processes only 1/W of the data.
 *
 * Issue #314: Hash-Partitioned Relation Storage
 */

#include "wirelog/columnar/internal.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

/* Stack threshold for counts/offsets arrays. Above this, heap-allocate. */
#define PARTITION_STACK_MAX 64u

/* ---- internal helpers --------------------------------------------------- */

/*
 * Compute partition index for a single row by hashing the specified
 * key columns with XXH3_64bits.
 *
 * key_buf is a caller-provided scratch buffer of at least key_count
 * entries, used to concatenate the key column values before hashing.
 */
static uint32_t
row_partition(const int64_t *row, const uint32_t *key_cols,
    uint32_t key_count, uint32_t num_workers, int64_t *key_buf)
{
    if (num_workers == 1)
        return 0;

    for (uint32_t i = 0; i < key_count; i++)
        key_buf[i] = row[key_cols[i]];

    uint64_t hash = XXH3_64bits(key_buf, key_count * sizeof(int64_t));
    return (uint32_t)(hash % num_workers);
}

/* ---- public API --------------------------------------------------------- */

int
col_rel_partition_by_key(const col_rel_t *src,
    const uint32_t *key_cols, uint32_t key_count,
    uint32_t num_workers, col_rel_t **out_parts)
{
    if (!src || !key_cols || !out_parts || key_count == 0
        || num_workers == 0 || src->ncols == 0)
        return EINVAL;

    /* Validate key column indices */
    for (uint32_t i = 0; i < key_count; i++) {
        if (key_cols[i] >= src->ncols)
            return EINVAL;
    }

    /* Initialise output to NULL for safe cleanup on error */
    for (uint32_t w = 0; w < num_workers; w++)
        out_parts[w] = NULL;

    const uint32_t ncols = src->ncols;
    const uint32_t nrows = src->nrows;
    const size_t row_bytes = (size_t)ncols * sizeof(int64_t);

    /* Allocate counts and offsets arrays (stack if small, heap otherwise) */
    uint32_t counts_stack[PARTITION_STACK_MAX];
    uint32_t offsets_stack[PARTITION_STACK_MAX];
    uint32_t *counts = counts_stack;
    uint32_t *offsets = offsets_stack;

    if (num_workers > PARTITION_STACK_MAX) {
        counts = (uint32_t *)calloc(num_workers, sizeof(uint32_t));
        offsets = (uint32_t *)calloc(num_workers, sizeof(uint32_t));
        if (!counts || !offsets) {
            free(counts);
            free(offsets);
            return ENOMEM;
        }
    } else {
        memset(counts, 0, num_workers * sizeof(uint32_t));
        memset(offsets, 0, num_workers * sizeof(uint32_t));
    }

    /* Key buffer for hash computation (stack-allocated) */
    int64_t key_buf_stack[COL_STACK_MAX];
    int64_t *key_buf = key_buf_stack;
    if (key_count > COL_STACK_MAX) {
        key_buf = (int64_t *)malloc(key_count * sizeof(int64_t));
        if (!key_buf) {
            if (num_workers > PARTITION_STACK_MAX) {
                free(counts);
                free(offsets);
            }
            return ENOMEM;
        }
    }

    /* Pass 1: count rows per partition */
    for (uint32_t i = 0; i < nrows; i++) {
        const int64_t *row = col_rel_row(src, i);
        uint32_t p = row_partition(row, key_cols, key_count, num_workers,
                key_buf);
        counts[p]++;
    }

    /* Allocate partition relations with exact capacity */
    int rc = 0;
    for (uint32_t w = 0; w < num_workers; w++) {
        char name_buf[256];
        snprintf(name_buf, sizeof(name_buf), "%s_p%u",
            src->name ? src->name : "rel", w);

        col_rel_t *part = NULL;
        rc = col_rel_alloc(&part, name_buf);
        if (rc != 0)
            goto cleanup;

        rc = col_rel_set_schema(part, ncols,
                (const char *const *)src->col_names);
        if (rc != 0) {
            col_rel_destroy(part);
            goto cleanup;
        }

        /* Resize data buffer to exact partition size.  Safe because
         * col_rel_alloc + col_rel_set_schema always heap-allocates. */
        assert(!part->arena_owned);
        assert(!part->pool_owned);
        if (counts[w] != part->capacity && counts[w] > 0) {
            int64_t *exact = (int64_t *)realloc(
                part->data, (size_t)counts[w] * ncols * sizeof(int64_t));
            if (!exact) {
                col_rel_destroy(part);
                rc = ENOMEM;
                goto cleanup;
            }
            part->data = exact;
            part->capacity = counts[w];
        } else if (counts[w] == 0) {
            /* Empty partition: free the default allocation */
            free(part->data);
            part->data = NULL;
            part->capacity = 0;
        }

        out_parts[w] = part;
    }

    /* Pass 2: scatter rows into partitions */
    for (uint32_t i = 0; i < nrows; i++) {
        const int64_t *row = col_rel_row(src, i);
        uint32_t p = row_partition(row, key_cols, key_count, num_workers,
                key_buf);
        memcpy(col_rel_row_mut(out_parts[p], offsets[p]),
            row, row_bytes);
        offsets[p]++;
    }

    /* Set final row counts */
    for (uint32_t w = 0; w < num_workers; w++)
        out_parts[w]->nrows = counts[w];

    /* Cleanup temporaries */
    if (key_count > COL_STACK_MAX)
        free(key_buf);
    if (num_workers > PARTITION_STACK_MAX) {
        free(counts);
        free(offsets);
    }
    return 0;

cleanup:
    for (uint32_t w = 0; w < num_workers; w++) {
        if (out_parts[w]) {
            col_rel_destroy(out_parts[w]);
            out_parts[w] = NULL;
        }
    }
    if (key_count > COL_STACK_MAX)
        free(key_buf);
    if (num_workers > PARTITION_STACK_MAX) {
        free(counts);
        free(offsets);
    }
    return rc;
}

int
col_rel_merge_partitions(col_rel_t **parts, uint32_t num_workers,
    col_rel_t **out)
{
    if (!parts || !out || num_workers == 0)
        return EINVAL;

    /* Verify all partitions have the same ncols */
    uint32_t ncols = parts[0] ? parts[0]->ncols : 0;
    if (ncols == 0)
        return EINVAL;

    uint64_t total_rows_64 = 0;
    for (uint32_t w = 0; w < num_workers; w++) {
        if (!parts[w] || parts[w]->ncols != ncols)
            return EINVAL;
        total_rows_64 += parts[w]->nrows;
    }
    if (total_rows_64 > UINT32_MAX)
        return ENOMEM;
    uint32_t total_rows = (uint32_t)total_rows_64;

    /* Create output relation with schema from parts[0] */
    col_rel_t *merged = col_rel_new_like("merged", parts[0]);
    if (!merged)
        return ENOMEM;

    /* Pre-size to exact total.  Safe: col_rel_new_like heap-allocates. */
    assert(!merged->arena_owned);
    assert(!merged->pool_owned);
    if (total_rows > 0 && total_rows != merged->capacity) {
        int64_t *exact = (int64_t *)realloc(
            merged->data, (size_t)total_rows * ncols * sizeof(int64_t));
        if (!exact) {
            col_rel_destroy(merged);
            return ENOMEM;
        }
        merged->data = exact;
        merged->capacity = total_rows;
    } else if (total_rows == 0) {
        free(merged->data);
        merged->data = NULL;
        merged->capacity = 0;
    }

    /* Copy partition data contiguously */
    size_t offset = 0;
    for (uint32_t w = 0; w < num_workers; w++) {
        if (parts[w]->nrows > 0) {
            memcpy(merged->data + offset,
                parts[w]->data,
                (size_t)parts[w]->nrows * ncols * sizeof(int64_t));
            offset += (size_t)parts[w]->nrows * ncols;
        }
    }
    merged->nrows = total_rows;

    *out = merged;
    return 0;
}
