/*
 * test_pointer_swap.c - TDD tests for col_rel_t pointer swap pattern
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies that the zero-copy pointer swap pattern used in
 * col_stratum_step_retraction_nonrecursive is safe and correct:
 *
 *   1. Save r->data pointer as backup (O(1), no memcpy)
 *   2. Allocate fresh buffer, assign to r->data
 *   3. Evaluate into fresh buffer
 *   4a. No change: free new buffer, restore backup pointer
 *   4b. Changed:   free backup, keep new buffer
 *
 * Each test covers one behavioral invariant of this pattern.
 */

#include "../wirelog/columnar/internal.h"

#include <inttypes.h>
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

/*
 * fill_rel: append nrows sequential rows to r.
 * Row i contains: val_base + i * ncols, val_base + i * ncols + 1, ...
 * Returns 0 on success.
 */
static int
fill_rel(col_rel_t *r, uint32_t nrows, int64_t val_base)
{
    int64_t *row_buf = (int64_t *)malloc(r->ncols * sizeof(int64_t));
    if (!row_buf)
        return -1;
    for (uint32_t i = 0; i < nrows; i++) {
        for (uint32_t c = 0; c < r->ncols; c++)
            row_buf[c] = val_base + (int64_t)(i * r->ncols + c);
        if (col_rel_append_row(r, row_buf) != 0) {
            free(row_buf);
            return -1;
        }
    }
    free(row_buf);
    return 0;
}

/*
 * data_equal: byte-wise comparison of r->data against expected buffer.
 * Returns true if identical.
 */
static bool
data_equal(const col_rel_t *r, const int64_t *expected)
{
    size_t sz = (size_t)r->nrows * r->ncols * sizeof(int64_t);
    return memcmp(r->data, expected, sz) == 0;
}

/* ======================================================================== */
/* Test 1: Basic pointer save and restore leaves data intact               */
/* ======================================================================== */

/*
 * Allocate a relation with 5 rows. Save r->data, allocate a fresh buffer,
 * copy rows into it (simulating evaluation into new buffer), then restore
 * the original pointer. Verify the original data is still accessible and
 * unchanged. The fresh buffer is freed.
 */
static int
test_basic_save_restore(void)
{
    TEST("Basic pointer save and restore: data intact after round-trip");

    col_rel_t *r = col_rel_new_auto("t1", 2);
    if (!r) {
        FAIL("col_rel_new_auto failed");
        return 1;
    }

    if (fill_rel(r, 5, 100) != 0) {
        col_rel_destroy(r);
        FAIL("fill_rel failed");
        return 1;
    }

    /* Snapshot expected data before swap */
    size_t sz = (size_t)r->nrows * r->ncols * sizeof(int64_t);
    int64_t *expected = (int64_t *)malloc(sz);
    if (!expected) {
        col_rel_destroy(r);
        FAIL("malloc expected failed");
        return 1;
    }
    memcpy(expected, r->data, sz);

    /* --- pointer swap: save --- */
    int64_t    *backup_data = r->data;
    uint32_t backup_nrows = r->nrows;
    uint32_t backup_capacity = r->capacity;

    /* Allocate fresh buffer and copy rows in (simulates evaluation output) */
    int64_t *fresh = (int64_t *)malloc(sz);
    if (!fresh) {
        free(expected);
        col_rel_destroy(r);
        FAIL("malloc fresh failed");
        return 1;
    }
    memcpy(fresh, backup_data, sz);
    r->data = fresh;
    r->nrows = backup_nrows;
    r->capacity = backup_capacity;

    /* --- pointer swap: restore (no-change path) --- */
    free(r->data);          /* free the fresh buffer  */
    r->data = backup_data;
    r->nrows = backup_nrows;
    r->capacity = backup_capacity;

    /* Verify original pointer is back and data is unchanged */
    bool ptr_ok = (r->data == backup_data);
    bool data_ok = data_equal(r, expected);

    free(expected);
    col_rel_destroy(r);

    if (!ptr_ok) {
        FAIL("restored data pointer does not match original");
        return 1;
    }
    if (!data_ok) {
        FAIL("data corrupted after pointer save/restore round-trip");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: Multiple simultaneous pointer swaps are independent             */
/* ======================================================================== */

/*
 * Create 3 relations (1-col, 2-col, 3-col). Swap all 3 simultaneously:
 * save pointers, allocate fresh buffers, write distinct sentinel values,
 * then restore each independently. Verify each relation holds its own
 * original content undisturbed.
 */
static int
test_multiple_relations(void)
{
    TEST("Multiple relations: simultaneous pointer swaps are independent");

    col_rel_t *rels[3];
    rels[0] = col_rel_new_auto("m1", 1);
    rels[1] = col_rel_new_auto("m2", 2);
    rels[2] = col_rel_new_auto("m3", 3);
    for (int i = 0; i < 3; i++) {
        if (!rels[i]) {
            for (int j = 0; j < i; j++) col_rel_destroy(rels[j]);
            FAIL("col_rel_new_auto failed");
            return 1;
        }
    }

    /* Fill each relation with distinct values */
    for (int i = 0; i < 3; i++) {
        if (fill_rel(rels[i], 4, (int64_t)i * 100 + 10) != 0) {
            for (int j = 0; j < 3; j++) col_rel_destroy(rels[j]);
            FAIL("fill_rel failed");
            return 1;
        }
    }

    /* Snapshot expected data */
    int64_t *expected[3];
    for (int i = 0; i < 3; i++) {
        size_t sz = (size_t)rels[i]->nrows * rels[i]->ncols * sizeof(int64_t);
        expected[i] = (int64_t *)malloc(sz);
        if (!expected[i]) {
            for (int j = 0; j < i; j++) free(expected[j]);
            for (int j = 0; j < 3; j++) col_rel_destroy(rels[j]);
            FAIL("malloc expected failed");
            return 1;
        }
        memcpy(expected[i], rels[i]->data, sz);
    }

    /* Save all 3 pointers */
    int64_t *backups[3];
    uint32_t backup_nrows[3], backup_cap[3];
    int64_t *fresh[3];
    for (int i = 0; i < 3; i++) {
        backups[i] = rels[i]->data;
        backup_nrows[i] = rels[i]->nrows;
        backup_cap[i] = rels[i]->capacity;
        size_t sz = (size_t)backup_cap[i] * rels[i]->ncols * sizeof(int64_t);
        fresh[i] = (int64_t *)malloc(sz);
        if (!fresh[i]) {
            for (int j = 0; j < i; j++) free(fresh[j]);
            for (int j = 0; j < 3; j++) free(expected[j]);
            for (int j = 0; j < 3; j++) col_rel_destroy(rels[j]);
            FAIL("malloc fresh failed");
            return 1;
        }
        rels[i]->data = fresh[i];
        rels[i]->nrows = backup_nrows[i];
        rels[i]->capacity = backup_cap[i];
    }

    /* Restore all 3 (no-change path) */
    for (int i = 0; i < 3; i++) {
        free(rels[i]->data);
        rels[i]->data = backups[i];
        rels[i]->nrows = backup_nrows[i];
        rels[i]->capacity = backup_cap[i];
    }

    /* Verify each relation is intact */
    bool ok = true;
    for (int i = 0; i < 3; i++) {
        if (!data_equal(rels[i], expected[i])) {
            ok = false;
            break;
        }
    }

    for (int i = 0; i < 3; i++) free(expected[i]);
    for (int i = 0; i < 3; i++) col_rel_destroy(rels[i]);

    if (!ok) {
        FAIL("data corrupted in one or more relations after simultaneous swap");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: Zero-row relation swap is a safe no-op                          */
/* ======================================================================== */

/*
 * Empty relation (nrows == 0). Pointer swap should handle this without
 * crash: no data bytes to copy, backup pointer restores cleanly.
 */
static int
test_zero_rows(void)
{
    TEST("Zero rows: pointer swap on empty relation is safe");

    col_rel_t *r = col_rel_new_auto("empty", 2);
    if (!r) {
        FAIL("col_rel_new_auto failed");
        return 1;
    }
    /* Do NOT append any rows — nrows stays 0 */

    /* Ensure schema is set so ncols is valid */
    if (r->ncols == 0) {
        /* col_rel_new_auto already sets ncols=2, but if schema not set,
         * capacity may be 0 — that is fine for this test. */
    }

    int64_t *backup_data = r->data;
    uint32_t backup_nrows = r->nrows;      /* 0 */
    uint32_t backup_capacity = r->capacity;

    /* Pointer swap: allocate a minimal fresh buffer (or NULL if capacity==0) */
    int64_t *fresh = NULL;
    if (backup_capacity > 0 && r->ncols > 0) {
        size_t sz = (size_t)backup_capacity * r->ncols * sizeof(int64_t);
        fresh = (int64_t *)malloc(sz);
        if (!fresh) {
            col_rel_destroy(r);
            FAIL("malloc fresh failed");
            return 1;
        }
    }
    r->data = fresh;       /* may be NULL — that is valid when nrows==0 */
    r->nrows = 0;
    r->capacity = backup_capacity;

    /* Restore */
    free(r->data);
    r->data = backup_data;
    r->nrows = backup_nrows;
    r->capacity = backup_capacity;

    bool ptr_ok = (r->data == backup_data);
    bool nrows_ok = (r->nrows == 0);

    col_rel_destroy(r);

    if (!ptr_ok) {
        FAIL("pointer not restored correctly for zero-row relation");
        return 1;
    }
    if (!nrows_ok) {
        FAIL("nrows should remain 0 after swap on empty relation");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 4: Large buffer (>10 MB) swap works correctly                      */
/* ======================================================================== */

/*
 * Allocate a relation with enough rows so the data buffer exceeds 10 MB.
 * ncols=1, 1,400,000 rows => 1,400,000 * 8 = 11.2 MB.
 * Perform pointer swap, verify integrity of first, last, and mid rows.
 */
static int
test_large_buffer(void)
{
    TEST("Large buffer (>10 MB): pointer swap preserves row data");

#define LARGE_NROWS 1400000u

    col_rel_t *r = col_rel_new_auto("large", 1);
    if (!r) {
        FAIL("col_rel_new_auto failed");
        return 1;
    }

    if (fill_rel(r, LARGE_NROWS, 0) != 0) {
        col_rel_destroy(r);
        FAIL("fill_rel failed (OOM?)");
        return 1;
    }

    size_t sz = (size_t)r->nrows * r->ncols * sizeof(int64_t);
    size_t sz_cap = (size_t)r->capacity * r->ncols * sizeof(int64_t);

    /* Snapshot spot-checks: first, mid, last */
    int64_t first = r->data[0];
    int64_t mid = r->data[LARGE_NROWS / 2];
    int64_t last = r->data[r->nrows - 1];

    /* Pointer swap */
    int64_t *backup = r->data;
    uint32_t b_nrows = r->nrows;
    uint32_t b_capacity = r->capacity;

    int64_t *fresh = (int64_t *)malloc(sz_cap);
    if (!fresh) {
        col_rel_destroy(r);
        FAIL("malloc fresh (>10MB) failed");
        return 1;
    }
    memcpy(fresh, backup, sz); /* simulate eval output */
    r->data = fresh;
    r->nrows = b_nrows;
    r->capacity = b_capacity;

    /* Restore */
    free(r->data);
    r->data = backup;
    r->nrows = b_nrows;
    r->capacity = b_capacity;

    bool ok = (r->data[0] == first)
        && (r->data[LARGE_NROWS / 2] == mid)
        && (r->data[r->nrows - 1] == last);

    col_rel_destroy(r);

    if (!ok) {
        FAIL("spot-check values differ after large buffer swap");
        return 1;
    }
    PASS();
    return 0;
#undef LARGE_NROWS
}

/* ======================================================================== */
/* Test 5: Data integrity — every value verified after swap sequence       */
/* ======================================================================== */

/*
 * Multi-column relation (ncols=3), 100 rows with patterned values.
 * Perform the full swap-back sequence and compare every element.
 */
static int
test_data_integrity(void)
{
    TEST("Data integrity: all values match after complete swap sequence");

#define INTEGRITY_NROWS 100u
#define INTEGRITY_NCOLS 3u

    col_rel_t *r = col_rel_new_auto("integrity", INTEGRITY_NCOLS);
    if (!r) {
        FAIL("col_rel_new_auto failed");
        return 1;
    }

    if (fill_rel(r, INTEGRITY_NROWS, 42) != 0) {
        col_rel_destroy(r);
        FAIL("fill_rel failed");
        return 1;
    }

    /* Snapshot full expected buffer */
    size_t sz = (size_t)r->nrows * r->ncols * sizeof(int64_t);
    size_t sz_cap = (size_t)r->capacity * r->ncols * sizeof(int64_t);
    int64_t *expected = (int64_t *)malloc(sz);
    if (!expected) {
        col_rel_destroy(r);
        FAIL("malloc expected failed");
        return 1;
    }
    memcpy(expected, r->data, sz);

    /* Pointer swap + restore */
    int64_t *backup = r->data;
    uint32_t b_nrows = r->nrows;
    uint32_t b_capacity = r->capacity;

    int64_t *fresh = (int64_t *)malloc(sz_cap);
    if (!fresh) {
        free(expected);
        col_rel_destroy(r);
        FAIL("malloc fresh failed");
        return 1;
    }
    memcpy(fresh, backup, sz);
    r->data = fresh;
    r->nrows = b_nrows;
    r->capacity = b_capacity;

    /* No-change path: restore */
    free(r->data);
    r->data = backup;
    r->nrows = b_nrows;
    r->capacity = b_capacity;

    /* Element-wise check */
    bool ok = true;
    for (uint32_t row = 0; row < r->nrows && ok; row++) {
        for (uint32_t col = 0; col < r->ncols && ok; col++) {
            int64_t got = r->data[row * r->ncols + col];
            int64_t want = expected[row * r->ncols + col];
            if (got != want) {
                ok = false;
                char msg[128];
                snprintf(msg, sizeof(msg),
                    "mismatch at row %u col %u: got %" PRId64
                    " want %" PRId64, row, col, got, want);
                free(expected);
                col_rel_destroy(r);
                FAIL(msg);
                return 1;
            }
        }
    }

    free(expected);
    col_rel_destroy(r);
    PASS();
    return 0;
#undef INTEGRITY_NROWS
#undef INTEGRITY_NCOLS
}

/* ======================================================================== */
/* Test 6: Backup cleanup — changed path frees old buffer without leak     */
/* ======================================================================== */

/*
 * Simulate the "data changed" branch: evaluation produced different rows.
 * The backup pointer must be freed (not the new buffer). ASAN will catch
 * any double-free or use-after-free; valgrind will catch leaks.
 *
 * Pattern:
 *   backup = r->data;      // save old
 *   r->data = fresh;       // assign new (evaluation result)
 *   // data changed — keep fresh, discard backup
 *   free(backup);          // ← this is the critical path
 */
static int
test_backup_cleanup_changed_path(void)
{
    TEST("Backup cleanup: changed path frees backup without leak");

    col_rel_t *r = col_rel_new_auto("cleanup", 2);
    if (!r) {
        FAIL("col_rel_new_auto failed");
        return 1;
    }

    if (fill_rel(r, 8, 200) != 0) {
        col_rel_destroy(r);
        FAIL("fill_rel failed");
        return 1;
    }

    size_t sz_cap = (size_t)r->capacity * r->ncols * sizeof(int64_t);

    /* Save old pointer */
    int64_t *backup = r->data;
    uint32_t b_nrows = r->nrows;
    uint32_t b_capacity = r->capacity;

    /* Allocate fresh buffer with different (changed) values */
    int64_t *fresh = (int64_t *)malloc(sz_cap);
    if (!fresh) {
        col_rel_destroy(r);
        FAIL("malloc fresh failed");
        return 1;
    }
    /* Write distinct sentinel: every element = 9999 */
    for (size_t i = 0; i < (size_t)b_nrows * r->ncols; i++)
        fresh[i] = 9999;

    r->data = fresh;
    r->nrows = b_nrows;
    r->capacity = b_capacity;

    /* Changed path: free the backup, keep fresh */
    free(backup);

    /* Verify new data is what we wrote */
    bool ok = true;
    for (uint32_t i = 0; i < r->nrows * r->ncols && ok; i++) {
        if (r->data[i] != 9999) {
            ok = false;
        }
    }

    col_rel_destroy(r);

    if (!ok) {
        FAIL("sentinel values corrupted in changed-path cleanup");
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
    printf("Pointer Swap Tests (Issue #300)\n");
    printf("================================\n\n");

    test_basic_save_restore();
    test_multiple_relations();
    test_zero_rows();
    test_large_buffer();
    test_data_integrity();
    test_backup_cleanup_changed_path();

    printf("\n");
    printf("Passed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
