/*
 * test_relation_append.c - generic relation append overflow regression
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>

static int tests_failed;

static void
test_row_count_boundaries(void)
{
    col_rel_t dst = { 0 };
    col_rel_t src = { 0 };

    printf("  exact UINT32_MAX row total: ");
    dst.capacity = UINT32_MAX;
    dst.nrows = UINT32_MAX - 1u;
    src.nrows = 1;
    if (col_rel_append_all(&dst, &src, NULL) != 0
        || dst.nrows != UINT32_MAX) {
        printf("FAIL\n");
        tests_failed++;
    } else {
        printf("PASS\n");
    }

    printf("  overflowing row total leaves destination unchanged: ");
    dst.nrows = UINT32_MAX;
    src.nrows = 1;
    int64_t **columns = (int64_t **)(uintptr_t)0x1;
    col_delta_timestamp_t *timestamps
        = (col_delta_timestamp_t *)(uintptr_t)0x2;
    bool *col_shared = (bool *)(uintptr_t)0x3;
    wl_mem_ledger_t *mem_ledger = (wl_mem_ledger_t *)(uintptr_t)0x4;
    dst.columns = columns;
    dst.timestamps = timestamps;
    dst.col_shared = col_shared;
    dst.mem_ledger = mem_ledger;
    dst.arena_owned = true;
    if (col_rel_append_all(&dst, &src, NULL) != EOVERFLOW
        || dst.nrows != UINT32_MAX || dst.capacity != UINT32_MAX
        || dst.columns != columns || dst.timestamps != timestamps
        || dst.col_shared != col_shared || dst.mem_ledger != mem_ledger
        || !dst.arena_owned) {
        printf("FAIL\n");
        tests_failed++;
    } else {
        printf("PASS\n");
    }
}

int
main(void)
{
    printf("=== col_rel_append_all overflow tests (Issue #1200) ===\n");
    test_row_count_boundaries();
    return tests_failed != 0;
}
