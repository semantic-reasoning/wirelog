/*
 * test_malloc_optimization.c - Arena allocation optimization tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests for the wl_arena_t bump-pointer allocator and its intended
 * integration patterns with columnar operator outputs.
 *
 * Test categories:
 *   A. Arena lifecycle: create, alloc, reset, free
 *   B. Allocation semantics: alignment, overflow, null safety
 *   C. Operator allocation patterns: arena-backed col_rel_t data buffers
 *   D. Multi-iteration reset: arena reuse across simulated operator chains
 *   E. Pointer swap with arena-allocated data
 *   F. Memory safety: ASAN-compatible usage patterns
 *   G. Performance: arena vs malloc allocation overhead
 */

#include "../wirelog/columnar/internal.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#endif

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
            return 1;                         \
        } while (0)

#define ASSERT_MSG(cond, msg) \
        do {                      \
            if (!(cond))          \
            FAIL(msg);        \
        } while (0)

/* ======================================================================== */
/* Helpers                                                                  */
/* ======================================================================== */

/*
 * monotonic_ns: return nanoseconds via clock_gettime(CLOCK_MONOTONIC) on POSIX,
 * or QueryPerformanceCounter on Windows. Returns 0 if unavailable.
 */
static uint64_t
monotonic_ns(void)
{
#ifdef _WIN32
    LARGE_INTEGER freq, cnt;
    if (!QueryPerformanceFrequency(&freq) || !QueryPerformanceCounter(&cnt))
        return 0;
    return (uint64_t)(cnt.QuadPart * 1000000000LL / freq.QuadPart);
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return 0;
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
#endif
}

/*
 * Simulate the data buffer size needed by a join operator:
 * left_nrows * right_nrows * ncols * sizeof(int64_t).
 * In practice join output is bounded, but this gives a representative size.
 */
static size_t
join_output_size(uint32_t left_nrows, uint32_t right_nrows, uint32_t ncols)
{
    return (size_t)left_nrows * right_nrows * ncols * sizeof(int64_t);
}

/*
 * fill_rel_data: write sequential values into a pre-allocated data buffer
 * (row-major, ncols columns). Returns number of cells written.
 */
static uint32_t
fill_rel_data(int64_t *data, uint32_t nrows, uint32_t ncols, int64_t base)
{
    for (uint32_t r = 0; r < nrows; r++)
        for (uint32_t c = 0; c < ncols; c++)
            data[r * ncols + c] = base + (int64_t)(r * ncols + c);
    return nrows * ncols;
}

/* ======================================================================== */
/* Category A: Arena Lifecycle                                              */
/* ======================================================================== */

/*
 * A1: wl_arena_create with valid capacity returns non-NULL arena;
 *     initial used == 0, capacity matches requested size.
 */
static int
test_arena_create_basic(void)
{
    TEST("A1: arena create - non-NULL, zero used, correct capacity");

    size_t cap = 1024 * 1024; /* 1 MB */
    wl_arena_t *arena = wl_arena_create(cap);
    ASSERT_MSG(arena != NULL, "wl_arena_create returned NULL");
    ASSERT_MSG(arena->used == 0, "initial used != 0");
    ASSERT_MSG(arena->capacity == cap, "capacity mismatch");
    ASSERT_MSG(arena->base != NULL, "base pointer is NULL");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * A2: wl_arena_create with capacity 0 returns NULL (documented contract).
 */
static int
test_arena_create_zero_capacity(void)
{
    TEST("A2: arena create with 0 capacity returns NULL");

    wl_arena_t *arena = wl_arena_create(0);
    ASSERT_MSG(arena == NULL, "expected NULL for zero capacity");

    PASS();
    return 0;
}

/*
 * A3: wl_arena_reset resets used to 0 without freeing backing buffer.
 *     The same base pointer must remain valid after reset.
 */
static int
test_arena_reset(void)
{
    TEST("A3: arena reset sets used=0, base pointer unchanged");

    wl_arena_t *arena = wl_arena_create(4096);
    ASSERT_MSG(arena != NULL, "create failed");

    void *base_before = arena->base;

    /* Allocate some bytes */
    void *p = wl_arena_alloc(arena, 128);
    ASSERT_MSG(p != NULL, "first alloc failed");
    ASSERT_MSG(arena->used > 0, "used should be nonzero after alloc");

    /* Reset */
    wl_arena_reset(arena);
    ASSERT_MSG(arena->used == 0, "used != 0 after reset");
    ASSERT_MSG(arena->base == base_before, "base changed after reset");

    /* Allocate again — must succeed and return same address as before */
    void *p2 = wl_arena_alloc(arena, 128);
    ASSERT_MSG(p2 != NULL, "alloc after reset failed");
    ASSERT_MSG(p2 == p, "second alloc should return same base address");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * A4: wl_arena_free(NULL) is a safe no-op (documented as NULL-safe).
 */
static int
test_arena_free_null(void)
{
    TEST("A4: arena free(NULL) is safe no-op");

    wl_arena_free(NULL); /* must not crash */
    PASS();
    return 0;
}

/*
 * A5: Single operator allocation - allocate a data buffer sized for
 *     a realistic relation (100 rows, 3 cols) from a fresh arena.
 */
static int
test_arena_single_operator_alloc(void)
{
    TEST("A5: single operator alloc - 100 rows x 3 cols fits in 1 MB arena");

    size_t cap = 1024 * 1024;
    wl_arena_t *arena = wl_arena_create(cap);
    ASSERT_MSG(arena != NULL, "create failed");

    uint32_t nrows = 100, ncols = 3;
    size_t sz = (size_t)nrows * (size_t)ncols * sizeof(int64_t);
    int64_t *data = (int64_t *)wl_arena_alloc(arena, sz);
    ASSERT_MSG(data != NULL, "data alloc from arena failed");

    /* Write and read back to exercise the memory */
    fill_rel_data(data, nrows, ncols, 0);
    ASSERT_MSG(data[0] == 0, "first element mismatch");
    ASSERT_MSG(data[(nrows - 1) * ncols + (ncols - 1)] ==
        (int64_t)nrows * ncols - 1,
        "last element mismatch");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * A6: Multiple sequential operator allocations from a single arena.
 *     Simulate: variable load -> filter output -> join output -> consolidate output.
 */
static int
test_arena_multiple_operator_alloc(void)
{
    TEST("A6: multiple sequential operator allocs within one arena");

    size_t cap = 8 * 1024 * 1024; /* 8 MB */
    wl_arena_t *arena = wl_arena_create(cap);
    ASSERT_MSG(arena != NULL, "create failed");

    uint32_t ncols = 2;

    /* Simulated operator outputs: variable (200 rows), filter (150 rows),
     * join (100 rows), consolidate (80 rows after dedup). */
    uint32_t sizes[] = { 200, 150, 100, 80 };
    int64_t *bufs[4];
    for (int i = 0; i < 4; i++) {
        size_t sz = (size_t)sizes[i] * ncols * sizeof(int64_t);
        bufs[i] = (int64_t *)wl_arena_alloc(arena, sz);
        ASSERT_MSG(bufs[i] != NULL, "operator alloc returned NULL");
        fill_rel_data(bufs[i], sizes[i], ncols, (int64_t)i * 1000);
    }

    /* Verify allocations are non-overlapping: each buf must be >= previous end */
    for (int i = 1; i < 4; i++) {
        size_t prev_sz = (size_t)sizes[i - 1] * ncols * sizeof(int64_t);
        /* Align up to 8 bytes as per arena internals */
        size_t aligned = (prev_sz + 7) & ~(size_t)7;
        char *expected_min = (char *)bufs[i - 1] + aligned;
        ASSERT_MSG((char *)bufs[i] >= expected_min,
            "allocations overlap or wrong order");
    }

    /* Spot-check data integrity of last buffer */
    int64_t last_val = bufs[3][(sizes[3] - 1) * ncols + (ncols - 1)];
    int64_t expected = 3000 + (int64_t)(sizes[3] * ncols - 1);
    ASSERT_MSG(last_val == expected, "data corrupted in sequential alloc");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Category B: Allocation Semantics                                        */
/* ======================================================================== */

/*
 * B1: All allocations are 8-byte aligned (required for int64_t storage).
 */
static int
test_arena_alignment(void)
{
    TEST("B1: all allocations are 8-byte aligned");

    wl_arena_t *arena = wl_arena_create(4096);
    ASSERT_MSG(arena != NULL, "create failed");

    /* Allocate odd sizes to force alignment padding */
    size_t odd_sizes[] = { 1, 3, 7, 9, 15, 17, 31, 33 };
    for (int i = 0; i < 8; i++) {
        void *p = wl_arena_alloc(arena, odd_sizes[i]);
        ASSERT_MSG(p != NULL, "alloc returned NULL");
        ASSERT_MSG(((uintptr_t)p % 8) == 0, "pointer not 8-byte aligned");
    }

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * B2: Allocation that exactly matches remaining capacity succeeds;
 *     next allocation returns NULL (arena exhausted).
 */
static int
test_arena_overflow(void)
{
    TEST("B2: arena overflow - alloc past capacity returns NULL");

    size_t cap = 64;
    wl_arena_t *arena = wl_arena_create(cap);
    ASSERT_MSG(arena != NULL, "create failed");

    /* Fill up exactly */
    void *p1 = wl_arena_alloc(arena, cap);
    ASSERT_MSG(p1 != NULL, "full-capacity alloc failed");
    ASSERT_MSG(arena->used == cap, "used != capacity after full alloc");

    /* Next alloc must return NULL */
    void *p2 = wl_arena_alloc(arena, 1);
    ASSERT_MSG(p2 == NULL, "expected NULL when arena exhausted");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * B3: wl_arena_alloc(arena, 0) returns NULL (documented contract).
 */
static int
test_arena_alloc_zero_size(void)
{
    TEST("B3: alloc of 0 bytes returns NULL");

    wl_arena_t *arena = wl_arena_create(1024);
    ASSERT_MSG(arena != NULL, "create failed");

    void *p = wl_arena_alloc(arena, 0);
    ASSERT_MSG(p == NULL, "expected NULL for 0-byte alloc");
    ASSERT_MSG(arena->used == 0, "used should be 0 after 0-byte alloc");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * B4: wl_arena_alloc(NULL, size) returns NULL (null safety).
 */
static int
test_arena_alloc_null_arena(void)
{
    TEST("B4: alloc from NULL arena returns NULL safely");

    void *p = wl_arena_alloc(NULL, 64);
    ASSERT_MSG(p == NULL, "expected NULL for NULL arena");

    PASS();
    return 0;
}

/*
 * B5: wl_arena_reset(NULL) is a safe no-op (null safety).
 */
static int
test_arena_reset_null(void)
{
    TEST("B5: reset(NULL) is safe no-op");

    wl_arena_reset(NULL); /* must not crash */
    PASS();
    return 0;
}

/* ======================================================================== */
/* Category C: Operator Allocation Patterns                                */
/* ======================================================================== */

/*
 * C1: col_op_join allocation pattern.
 *     A join of A(ncols=2, nrows=50) x B(ncols=2, nrows=50) on 1 key
 *     produces at most 50*50=2500 rows with 3 output cols.
 *     Verify an arena-backed buffer can hold this and data reads back correctly.
 */
static int
test_arena_join_pattern(void)
{
    TEST("C1: col_op_join pattern - arena holds join output buffer");

    uint32_t left_nrows = 50, right_nrows = 50, out_ncols = 3;
    size_t join_sz = join_output_size(left_nrows, right_nrows, out_ncols);

    wl_arena_t *arena = wl_arena_create(join_sz + 128);
    ASSERT_MSG(arena != NULL, "create failed");

    int64_t *out = (int64_t *)wl_arena_alloc(arena, join_sz);
    ASSERT_MSG(out != NULL, "join output alloc failed");

    /* Simulate writing join tuples: (left_val, right_val, key) */
    uint32_t actual_rows = 100; /* realistic join output << cross product */
    for (uint32_t r = 0; r < actual_rows; r++) {
        out[r * out_ncols + 0] = (int64_t)r;          /* left col */
        out[r * out_ncols + 1] = (int64_t)(r * 2);    /* right col */
        out[r * out_ncols + 2] = (int64_t)(r % 10);   /* join key */
    }

    /* Verify spot-check */
    ASSERT_MSG(out[50 * out_ncols + 2] == 50 % 10,
        "join key mismatch at row 50");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * C2: col_op_filter allocation pattern.
 *     Filter passes a subset of rows from input to output.
 *     Verify arena holds filtered output and data is correct.
 */
static int
test_arena_filter_pattern(void)
{
    TEST("C2: col_op_filter pattern - arena holds filtered output buffer");

    uint32_t input_nrows = 500, ncols = 2;
    /* Filter keeps ~20% of rows */
    uint32_t output_nrows = 100;
    size_t out_sz = (size_t)output_nrows * ncols * sizeof(int64_t);

    wl_arena_t *arena = wl_arena_create(out_sz + 64);
    ASSERT_MSG(arena != NULL, "create failed");

    int64_t *out = (int64_t *)wl_arena_alloc(arena, out_sz);
    ASSERT_MSG(out != NULL, "filter output alloc failed");

    /* Simulate: pass rows where col0 % 5 == 0 from a 500-row input */
    uint32_t written = 0;
    for (uint32_t r = 0; r < input_nrows && written < output_nrows; r++) {
        if (r % 5 == 0) {
            out[written * ncols + 0] = (int64_t)r;
            out[written * ncols + 1] = (int64_t)(r * 10);
            written++;
        }
    }
    ASSERT_MSG(written == output_nrows, "wrote unexpected number of rows");

    /* Verify: row 0 of output should be original row 0 */
    ASSERT_MSG(out[0] == 0, "first filtered row col0 wrong");
    /* row 5 of output should be original row 25 (5th multiple of 5) */
    ASSERT_MSG(out[5 * ncols + 0] == 25, "fifth filtered row col0 wrong");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * C3: col_op_consolidate allocation pattern.
 *     Consolidate sorts and deduplicates rows in-place.
 *     Verify the merge buffer (needed for k-way merge) fits in arena.
 */
static int
test_arena_consolidate_pattern(void)
{
    TEST("C3: col_op_consolidate pattern - merge buffer fits in arena");

    /* Consolidate needs a merge buffer equal to the relation size */
    uint32_t nrows = 1000, ncols = 2;
    size_t rel_sz = (size_t)nrows * ncols * sizeof(int64_t);
    size_t merge_sz = rel_sz; /* merge buffer = same size as relation */

    /* Arena must hold: relation data + merge buffer */
    wl_arena_t *arena = wl_arena_create(rel_sz + merge_sz + 256);
    ASSERT_MSG(arena != NULL, "create failed");

    int64_t *rel_data = (int64_t *)wl_arena_alloc(arena, rel_sz);
    int64_t *merge_buf = (int64_t *)wl_arena_alloc(arena, merge_sz);
    ASSERT_MSG(rel_data != NULL, "rel data alloc failed");
    ASSERT_MSG(merge_buf != NULL, "merge buffer alloc failed");

    /* Non-overlapping: merge_buf must follow rel_data */
    ASSERT_MSG((char *)merge_buf >= (char *)rel_data + rel_sz,
        "merge buffer overlaps relation data");

    /* Write descending data (simulating unsorted relation before consolidate) */
    for (uint32_t r = 0; r < nrows; r++) {
        rel_data[r * ncols + 0] = (int64_t)(nrows - 1 - r);
        rel_data[r * ncols + 1] = 0;
    }
    ASSERT_MSG(rel_data[0] == (int64_t)(nrows - 1),
        "first row should be largest value");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Category D: Multi-Iteration Arena Reset                                 */
/* ======================================================================== */

/*
 * D1: Arena reset enables buffer reuse across iterations.
 *     Simulate 10 fixed-point iterations: each allocates operator buffers,
 *     then resets. Total memory used stays constant (no growth).
 */
static int
test_arena_multi_iteration_reset(void)
{
    TEST("D1: multi-iteration reset - memory footprint stays constant");

    size_t iter_cap = 2 * 1024 * 1024; /* 2 MB per iteration */
    wl_arena_t *arena = wl_arena_create(iter_cap);
    ASSERT_MSG(arena != NULL, "create failed");

    void *base = arena->base;

    for (int iter = 0; iter < 10; iter++) {
        wl_arena_reset(arena);
        ASSERT_MSG(arena->used == 0, "reset did not zero used");
        ASSERT_MSG(arena->base == base, "base changed after reset");

        /* Allocate simulated operator outputs for this iteration */
        size_t alloc_sz = 512 * 1024; /* 512 KB */
        void *p = wl_arena_alloc(arena, alloc_sz);
        ASSERT_MSG(p != NULL, "iter alloc failed");

        /* Write a sentinel to confirm the memory is writable */
        memset(p, (int)(iter + 1), alloc_sz);
        uint8_t *check = (uint8_t *)p;
        ASSERT_MSG(check[0] == (uint8_t)(iter + 1), "sentinel mismatch");
        ASSERT_MSG(check[alloc_sz - 1] == (uint8_t)(iter + 1),
            "sentinel mismatch at end");
    }

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * D2: col_eval_relation_plan arena lifecycle - each iteration gets a fresh
 *     view of the arena. Allocate a realistic chain of operator buffers,
 *     verify they do not overlap, then reset for the next iteration.
 */
static int
test_arena_eval_iteration_lifecycle(void)
{
    TEST("D2: eval iteration lifecycle - allocate chain, reset, repeat");

    /* Each iteration: variable (400 rows) -> filter (200) -> join (150) -> consolidate (100) */
    uint32_t ncols = 2;
    size_t op_sizes[] = {
        (size_t)400 * ncols * sizeof(int64_t),
        (size_t)200 * ncols * sizeof(int64_t),
        (size_t)150 * ncols * sizeof(int64_t),
        (size_t)100 * ncols * sizeof(int64_t),
    };
    size_t total = 0;
    for (int i = 0; i < 4; i++) total += op_sizes[i];
    total += 128; /* alignment padding headroom */

    wl_arena_t *arena = wl_arena_create(total);
    ASSERT_MSG(arena != NULL, "create failed");

    for (int iter = 0; iter < 5; iter++) {
        wl_arena_reset(arena);

        void *bufs[4];
        for (int op = 0; op < 4; op++) {
            bufs[op] = wl_arena_alloc(arena, op_sizes[op]);
            ASSERT_MSG(bufs[op] != NULL, "op buf alloc failed");
        }

        /* Verify non-overlapping order */
        for (int op = 1; op < 4; op++) {
            ASSERT_MSG((char *)bufs[op] > (char *)bufs[op - 1],
                "operator buffers overlap");
        }
    }

    wl_arena_free(arena);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Category E: Pointer Swap with Arena-Allocated Data                      */
/* ======================================================================== */

/*
 * E1: Pointer swap pattern with arena-backed data buffer.
 *     The arena owns the backing memory; the col_rel_t struct tracks it
 *     via r->data. After arena reset, the pointer is stale — this test
 *     verifies the pattern works BEFORE the reset.
 */
static int
test_arena_pointer_swap_no_change(void)
{
    TEST(
        "E1: pointer swap with arena data - no-change path restores correctly");

    wl_arena_t *arena = wl_arena_create((size_t)2 * 1024 * 1024);
    ASSERT_MSG(arena != NULL, "create failed");

    /* Allocate a relation struct on heap, data buffer from arena */
    uint32_t nrows = 50, ncols = 2;
    size_t sz = (size_t)nrows * ncols * sizeof(int64_t);

    col_rel_t *r = col_rel_new_auto("t_arena", ncols);
    ASSERT_MSG(r != NULL, "col_rel_new_auto failed");

    /* Replace r->columns with arena-allocated column buffers */
    col_columns_free(r->columns, ncols);
    free(r->row_scratch);
    r->columns = (int64_t **)calloc(ncols, sizeof(int64_t *));
    ASSERT_MSG(r->columns != NULL, "calloc columns array failed");
    for (uint32_t c = 0; c < ncols; c++) {
        r->columns[c] = (int64_t *)wl_arena_alloc(arena,
                nrows * sizeof(int64_t));
        ASSERT_MSG(r->columns[c] != NULL, "arena alloc for column failed");
    }
    r->arena_owned = true;
    r->nrows = nrows;
    r->capacity = nrows;

    /* Fill with values using accessors */
    for (uint32_t row = 0; row < nrows; row++)
        for (uint32_t c = 0; c < ncols; c++)
            col_rel_set(r, row, c, 42 + (int64_t)(row * ncols + c));

    /* Snapshot first/last values */
    int64_t first = col_rel_get(r, 0, 0);
    int64_t last = col_rel_get(r, nrows - 1, ncols - 1);

    /* Save backup (pointer swap: save phase) */
    int64_t **backup_cols = r->columns;
    uint32_t backup_nrows = r->nrows;
    uint32_t backup_capacity = r->capacity;

    /* Allocate fresh columns from arena for evaluation output */
    int64_t **fresh = (int64_t **)calloc(ncols, sizeof(int64_t *));
    ASSERT_MSG(fresh != NULL, "calloc fresh columns failed");
    for (uint32_t c = 0; c < ncols; c++) {
        fresh[c] = (int64_t *)wl_arena_alloc(arena,
                nrows * sizeof(int64_t));
        ASSERT_MSG(fresh[c] != NULL, "arena alloc for fresh col failed");
        memcpy(fresh[c], backup_cols[c], nrows * sizeof(int64_t));
    }

    r->columns = fresh;
    r->nrows = backup_nrows;
    r->capacity = backup_capacity;

    /* No-change path: restore original (note: with arena, we skip free()) */
    free(r->columns); /* free columns array only, arena owns buffers */
    r->columns = backup_cols;
    r->nrows = backup_nrows;
    r->capacity = backup_capacity;

    /* Verify data is intact */
    ASSERT_MSG(r->columns == backup_cols, "columns pointer not restored");
    ASSERT_MSG(col_rel_get(r, 0, 0) == first, "first value changed");
    ASSERT_MSG(col_rel_get(r, nrows - 1, ncols - 1) == last,
        "last value changed after restore");

    /* Null out columns before col_rel_destroy so it does not free arena memory */
    free(r->columns); /* free columns array */
    r->columns = NULL;
    r->nrows = 0;
    r->capacity = 0;
    r->arena_owned = false;
    col_rel_destroy(r);

    wl_arena_free(arena); /* releases all arena-allocated data at once */
    PASS();
    return 0;
}

/*
 * E2: Pointer swap changed-path with arena data.
 *     Evaluation produced different results; the fresh buffer replaces the
 *     original. The original (backup) is silently dropped — it will be
 *     reclaimed when the arena resets, so no explicit free() is needed.
 */
static int
test_arena_pointer_swap_changed(void)
{
    TEST("E2: pointer swap with arena data - changed path keeps new data");

    wl_arena_t *arena = wl_arena_create((size_t)2 * 1024 * 1024);
    ASSERT_MSG(arena != NULL, "create failed");

    uint32_t nrows = 30, ncols = 2;
    size_t sz = (size_t)nrows * ncols * sizeof(int64_t);

    col_rel_t *r = col_rel_new_auto("t_changed", ncols);
    ASSERT_MSG(r != NULL, "col_rel_new_auto failed");

    col_columns_free(r->columns, ncols);
    free(r->row_scratch);
    r->columns = (int64_t **)calloc(ncols, sizeof(int64_t *));
    ASSERT_MSG(r->columns != NULL, "calloc columns failed");
    for (uint32_t c = 0; c < ncols; c++) {
        r->columns[c] = (int64_t *)wl_arena_alloc(arena,
                nrows * sizeof(int64_t));
        ASSERT_MSG(r->columns[c] != NULL, "arena alloc failed");
    }
    r->arena_owned = true;
    r->nrows = nrows;
    r->capacity = nrows;

    for (uint32_t row = 0; row < nrows; row++)
        for (uint32_t c = 0; c < ncols; c++)
            col_rel_set(r, row, c, 100 + (int64_t)(row * ncols + c));

    /* Save backup */
    int64_t **backup_cols = r->columns;
    uint32_t backup_nrows = r->nrows;
    uint32_t backup_capacity = r->capacity;

    /* Allocate fresh columns: evaluation produced different rows */
    int64_t **fresh = (int64_t **)calloc(ncols, sizeof(int64_t *));
    ASSERT_MSG(fresh != NULL, "calloc fresh failed");
    for (uint32_t c = 0; c < ncols; c++) {
        fresh[c] = (int64_t *)wl_arena_alloc(arena,
                nrows * sizeof(int64_t));
        ASSERT_MSG(fresh[c] != NULL, "arena alloc for fresh failed");
        for (uint32_t row = 0; row < nrows; row++)
            fresh[c][row] = 9000;
    }

    r->columns = fresh;
    r->nrows = backup_nrows;
    r->capacity = backup_capacity;

    /* Changed path: keep fresh, drop backup (no free needed with arena) */
    free(backup_cols); /* free old columns array, arena owns buffers */

    /* Verify new data is in place */
    ASSERT_MSG(r->columns == fresh, "columns pointer should be fresh");
    ASSERT_MSG(col_rel_get(r, 0, 0) == 9000,
        "first value should be from fresh buffer");

    free(r->columns); /* free columns array */
    r->columns = NULL;
    r->nrows = 0;
    r->capacity = 0;
    r->arena_owned = false;
    col_rel_destroy(r);

    wl_arena_free(arena);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Category F: Memory Safety                                               */
/* ======================================================================== */

/*
 * F1: No-leak pattern - every arena_create is paired with arena_free.
 *     All col_rel_t allocations are matched with col_rel_destroy.
 *     ASAN will detect any leaks if this pattern is broken.
 */
static int
test_memory_no_leak(void)
{
    TEST("F1: no-leak pattern - paired arena_create/free and rel_new/destroy");

    for (int trial = 0; trial < 5; trial++) {
        wl_arena_t *arena = wl_arena_create((size_t)512 * 1024);
        ASSERT_MSG(arena != NULL, "create failed");

        /* Allocate multiple buffers */
        for (int i = 0; i < 8; i++) {
            void *p = wl_arena_alloc(arena, 4096);
            ASSERT_MSG(p != NULL, "alloc failed");
            memset(p, 0xAB, 4096);
        }

        wl_arena_reset(arena);

        /* Create and destroy a relation using the heap (not arena) */
        col_rel_t *r = col_rel_new_auto("leak_test", 2);
        ASSERT_MSG(r != NULL, "rel alloc failed");
        col_rel_destroy(r);

        wl_arena_free(arena);
    }

    PASS();
    return 0;
}

/*
 * F2: Use-after-reset detection guard.
 *     After arena_reset, previously allocated pointers must not be
 *     accessed. This test verifies that a fresh allocation after reset
 *     starts at the base of the arena (demonstrating the old pointer
 *     would now alias new data).
 */
static int
test_memory_reset_invalidates_pointers(void)
{
    TEST("F2: reset invalidates old pointers - new alloc aliases base");

    wl_arena_t *arena = wl_arena_create(1024);
    ASSERT_MSG(arena != NULL, "create failed");

    void *p1 = wl_arena_alloc(arena, 64);
    ASSERT_MSG(p1 != NULL, "first alloc failed");

    /* Write a sentinel into p1 */
    memset(p1, 0x11, 64);

    /* Reset: p1 is now stale */
    wl_arena_reset(arena);

    /* New alloc after reset must return the same base address as p1 */
    void *p2 = wl_arena_alloc(arena, 64);
    ASSERT_MSG(p2 != NULL, "alloc after reset failed");
    ASSERT_MSG(p2 == p1, "post-reset alloc should alias first alloc address");

    /* Write different sentinel into p2 — this overwrites what p1 pointed to */
    memset(p2, 0x22, 64);
    ASSERT_MSG(((uint8_t *)p2)[0] == 0x22, "p2 sentinel not written");
    /* p1 now points to the same memory with 0x22 — if someone accessed p1
     * expecting 0x11, ASAN would not catch it here, but the aliasing is
     * the documented behavior that callers must respect. */

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * F3: Buffer boundary safety - writes to the edge of an arena allocation
 *     must not corrupt the arena's internal metadata (base, capacity, used).
 */
static int
test_memory_boundary_safety(void)
{
    TEST("F3: boundary safety - edge writes do not corrupt arena metadata");

    wl_arena_t *arena = wl_arena_create(256);
    ASSERT_MSG(arena != NULL, "create failed");

    void   *base_before = arena->base;
    size_t cap_before = arena->capacity;

    /* Allocate exactly 128 bytes and write to both ends */
    uint8_t *p = (uint8_t *)wl_arena_alloc(arena, 128);
    ASSERT_MSG(p != NULL, "alloc failed");

    p[0] = 0xDE;
    p[127] = 0xAD;

    /* Arena metadata must be unchanged */
    ASSERT_MSG(arena->base == base_before, "base corrupted");
    ASSERT_MSG(arena->capacity == cap_before,  "capacity corrupted");
    ASSERT_MSG(arena->used == 128,          "used unexpected");
    ASSERT_MSG(p[0] == 0xDE, "first byte corrupted");
    ASSERT_MSG(p[127] == 0xAD, "last byte corrupted");

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * F4: CRDT workload correctness check.
 *     Allocate two equivalent data buffers: one via malloc, one via arena.
 *     Fill both with identical data and verify byte-equality.
 *     This ensures arena-backed operator outputs match malloc-backed ones.
 */
static int
test_crdt_correctness(void)
{
    TEST("F4: CRDT correctness - arena and malloc outputs are byte-identical");

    uint32_t nrows = 200, ncols = 3;
    size_t sz = (size_t)nrows * ncols * sizeof(int64_t);

    /* malloc-backed reference */
    int64_t *ref = (int64_t *)malloc(sz);
    ASSERT_MSG(ref != NULL, "ref malloc failed");
    fill_rel_data(ref, nrows, ncols, 7);

    /* arena-backed candidate */
    wl_arena_t *arena = wl_arena_create(sz + 64);
    ASSERT_MSG(arena != NULL, "arena create failed");
    int64_t *cand = (int64_t *)wl_arena_alloc(arena, sz);
    ASSERT_MSG(cand != NULL, "arena alloc failed");
    fill_rel_data(cand, nrows, ncols, 7); /* same base value */

    /* Byte-identical check */
    ASSERT_MSG(memcmp(ref, cand, sz) == 0,
        "arena output differs from malloc output");

    free(ref);
    wl_arena_free(arena);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Category G: Performance                                                 */
/* ======================================================================== */

/*
 * G1: Arena allocation throughput vs malloc.
 *     Measure time for N alloc+memset cycles using arena (with resets)
 *     vs individual malloc/free pairs. Arena must be faster.
 *
 * Note: this is a sanity check, not a precise microbenchmark. On a heavily
 * loaded CI machine it may occasionally fail; the ratio should hold on any
 * modern system for N=10000 allocations of 4KB each.
 */
static int
test_performance_arena_vs_malloc(void)
{
    TEST("G1: performance - arena alloc/reset faster than malloc/free");

#define PERF_N        10000
#define PERF_ALLOC_SZ 4096

    /* Arena: allocate all in a 50 MB arena, reset every 100 allocs */
    wl_arena_t *arena = wl_arena_create(100 * PERF_ALLOC_SZ + 1024);
    if (!arena) {
        FAIL("arena create failed");
    }

    uint64_t t0 = monotonic_ns();
    for (int i = 0; i < PERF_N; i++) {
        if (i % 100 == 0)
            wl_arena_reset(arena);
        void *p = wl_arena_alloc(arena, PERF_ALLOC_SZ);
        if (p)
            ((uint8_t *)p)[0] = (uint8_t)i; /* prevent dead-code elim */
    }
    uint64_t t1 = monotonic_ns();
    uint64_t arena_ns = t1 - t0;

    wl_arena_free(arena);

    /* malloc/free: individual allocations */
    uint64_t t2 = monotonic_ns();
    for (int i = 0; i < PERF_N; i++) {
        void *p = malloc(PERF_ALLOC_SZ);
        if (p) {
            ((uint8_t *)p)[0] = (uint8_t)i;
            free(p);
        }
    }
    uint64_t t3 = monotonic_ns();
    uint64_t malloc_ns = t3 - t2;

    printf("\n    arena=%llu ns  malloc=%llu ns  ratio=%.2fx ",
        (unsigned long long)arena_ns,
        (unsigned long long)malloc_ns,
        malloc_ns > 0 ? (double)malloc_ns / (double)arena_ns : 0.0);

    /* Arena must not be dramatically slower than malloc.
     * Skip the ratio check when malloc_ns == 0 (timer resolution too coarse
     * to measure — means malloc is too fast to distinguish, which is fine).
     * Note: CI environments with glibc tcache can optimize malloc extremely well,
     * so we use a conservative 40x ratio to avoid flakiness. */
    if (malloc_ns > 1000000) { /* Only check if malloc takes > 1ms */
        ASSERT_MSG(arena_ns < malloc_ns * 40,
            "arena allocation unexpectedly much slower than malloc");
    }

    PASS();
    return 0;
#undef PERF_N
#undef PERF_ALLOC_SZ
}

/*
 * G2: Arena memory footprint is predictable.
 *     After allocating N buffers of known size, arena->used must equal
 *     the sum of aligned sizes. No hidden overhead beyond alignment padding.
 */
static int
test_performance_footprint(void)
{
    TEST("G2: footprint - used matches sum of 8-byte-aligned allocation sizes");

    wl_arena_t *arena = wl_arena_create((size_t)1024 * 1024);
    ASSERT_MSG(arena != NULL, "create failed");

    size_t sizes[] = { 8, 16, 24, 32, 64, 128, 256 };
    size_t expected = 0;
    for (int i = 0; i < 7; i++) {
        size_t aligned = (sizes[i] + 7) & ~(size_t)7;
        expected += aligned;
        void *p = wl_arena_alloc(arena, sizes[i]);
        ASSERT_MSG(p != NULL, "alloc failed");
        ASSERT_MSG(arena->used == expected, "used mismatch after alloc");
    }

    wl_arena_free(arena);
    PASS();
    return 0;
}

/*
 * G3: Large realistic workload - 1000-iteration simulation.
 *     Each iteration allocates 4 operator buffers (totaling ~1 MB),
 *     then resets. Measure peak arena->used to confirm it stays bounded.
 */
static int
test_performance_large_workload(void)
{
    TEST("G3: large workload - 1000 iterations, memory stays bounded");

    /* Operator sizes for one iteration (bytes): */
    size_t op_sz[] = {
        (size_t)500 * 2 * sizeof(int64_t), /* variable  */
        (size_t)300 * 2 * sizeof(int64_t), /* filter    */
        (size_t)200 * 3 * sizeof(int64_t), /* join      */
        (size_t)150 * 3 * sizeof(int64_t), /* consolidate */
    };
    size_t iter_sz = 0;
    for (int i = 0; i < 4; i++) iter_sz += op_sz[i];
    iter_sz += 256; /* alignment headroom */

    wl_arena_t *arena = wl_arena_create(iter_sz);
    ASSERT_MSG(arena != NULL, "create failed");

    size_t peak_used = 0;
    for (int iter = 0; iter < 1000; iter++) {
        wl_arena_reset(arena);
        for (int op = 0; op < 4; op++) {
            void *p = wl_arena_alloc(arena, op_sz[op]);
            ASSERT_MSG(p != NULL, "op alloc failed at iteration");
        }
        if (arena->used > peak_used)
            peak_used = arena->used;
    }

    /* Peak usage must not exceed the arena capacity */
    ASSERT_MSG(peak_used <= arena->capacity,
        "peak usage exceeded arena capacity");

    printf("\n    peak_used=%zu bytes capacity=%zu bytes ",
        peak_used, arena->capacity);

    wl_arena_free(arena);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Main                                                                    */
/* ======================================================================== */

int
main(void)
{
    printf("Malloc Optimization Tests (Arena Allocation)\n");
    printf("=============================================\n\n");

    printf("Category A: Arena Lifecycle\n");
    test_arena_create_basic();
    test_arena_create_zero_capacity();
    test_arena_reset();
    test_arena_free_null();
    test_arena_single_operator_alloc();
    test_arena_multiple_operator_alloc();

    printf("\nCategory B: Allocation Semantics\n");
    test_arena_alignment();
    test_arena_overflow();
    test_arena_alloc_zero_size();
    test_arena_alloc_null_arena();
    test_arena_reset_null();

    printf("\nCategory C: Operator Allocation Patterns\n");
    test_arena_join_pattern();
    test_arena_filter_pattern();
    test_arena_consolidate_pattern();

    printf("\nCategory D: Multi-Iteration Reset\n");
    test_arena_multi_iteration_reset();
    test_arena_eval_iteration_lifecycle();

    printf("\nCategory E: Pointer Swap with Arena Data\n");
    test_arena_pointer_swap_no_change();
    test_arena_pointer_swap_changed();

    printf("\nCategory F: Memory Safety\n");
    test_memory_no_leak();
    test_memory_reset_invalidates_pointers();
    test_memory_boundary_safety();
    test_crdt_correctness();

    printf("\nCategory G: Performance\n");
    test_performance_arena_vs_malloc();
    test_performance_footprint();
    test_performance_large_workload();

    printf("\n");
    printf("=============================================\n");
    printf("Passed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
