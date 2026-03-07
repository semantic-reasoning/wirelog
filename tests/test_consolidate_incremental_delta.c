#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

/*
 * TDD RED Phase: Test harness for col_op_consolidate_incremental_delta
 * All tests FAIL initially (function doesn't exist - that's expected in RED phase)
 * When US-004 implements the function, these tests should PASS (GREEN phase)
 */

/* ArrowSchema stub (opaque type from nanoarrow) */
typedef struct {
    void *release; /* function pointer */
    void *private_data;
} ArrowSchema;

/* Internal structure definition (matching columnar_nanoarrow.c) */
typedef struct col_rel {
    char *name;         /* owned, null-terminated                */
    uint32_t ncols;     /* columns per tuple (0 = unset)         */
    int64_t *data;      /* owned, row-major int64 buffer         */
    uint32_t nrows;     /* current row count                     */
    uint32_t capacity;  /* allocated row capacity                */
    char **col_names;   /* owned array of ncols owned strings    */
    ArrowSchema schema; /* owned Arrow schema (lazy-inited)      */
    bool schema_ok;     /* true after schema is initialised      */
} col_rel_t;

/* Forward declaration - function under test (doesn't exist yet in RED phase) */
int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
                                     col_rel_t *delta_out);

/* Test utility macros */
#define PASS(msg) printf("  ✓ %s\n", msg)
#define FAIL(msg)                      \
    do {                               \
        printf("  ✗ FAIL: %s\n", msg); \
        exit(1);                       \
    } while (0)

/* Helper: Create test relation */
static col_rel_t *
test_rel_create(const char *name, uint32_t ncols)
{
    col_rel_t *rel = (col_rel_t *)calloc(1, sizeof(col_rel_t));
    rel->name = (char *)malloc(strlen(name) + 1);
    strcpy(rel->name, name);
    rel->ncols = ncols;
    rel->nrows = 0;
    rel->capacity = 200;
    rel->data = (int64_t *)calloc(rel->capacity * ncols, sizeof(int64_t));
    return rel;
}

/* Helper: Append row */
static void
test_rel_append_row(col_rel_t *rel, int64_t *row)
{
    if (rel->nrows >= rel->capacity) {
        rel->capacity *= 2;
        rel->data = (int64_t *)realloc(rel->data, rel->capacity * rel->ncols
                                                      * sizeof(int64_t));
    }
    memcpy(&rel->data[rel->nrows * rel->ncols], row,
           rel->ncols * sizeof(int64_t));
    rel->nrows++;
}

/* Helper: Check if rows are sorted */
static int
is_sorted(col_rel_t *rel)
{
    for (uint32_t i = 1; i < rel->nrows; i++) {
        int64_t *prev = &rel->data[(i - 1) * rel->ncols];
        int64_t *curr = &rel->data[i * rel->ncols];
        for (uint32_t j = 0; j < rel->ncols; j++) {
            if (prev[j] > curr[j])
                return 0;
            if (prev[j] < curr[j])
                break;
        }
    }
    return 1;
}

/* Helper: Check for duplicates */
static int
has_dups(col_rel_t *rel)
{
    for (uint32_t i = 1; i < rel->nrows; i++) {
        int64_t *prev = &rel->data[(i - 1) * rel->ncols];
        int64_t *curr = &rel->data[i * rel->ncols];
        if (memcmp(prev, curr, rel->ncols * sizeof(int64_t)) == 0)
            return 1;
    }
    return 0;
}

/* Helper: Free relation */
static void
test_rel_free(col_rel_t *rel)
{
    if (rel) {
        free(rel->data);
        free(rel->name);
        free(rel);
    }
}

/* ===== TEST CASES ===== */

/* Test 1: Empty old + small delta -> all rows new */
static void
test_case_1(void)
{
    printf("Test 1: Empty old + small delta\n");

    col_rel_t *rel = test_rel_create("rel", 2);
    col_rel_t *delta = test_rel_create("delta", 2);

    int64_t row1[2] = { 1, 2 };
    int64_t row2[2] = { 3, 4 };
    test_rel_append_row(rel, row1);
    test_rel_append_row(rel, row2);

    int rc = col_op_consolidate_incremental_delta(rel, 0, delta);
    if (rc != 0)
        FAIL("function returned error");
    if (delta->nrows != 2)
        FAIL("delta_out should have 2 rows");
    if (!is_sorted(rel))
        FAIL("result not sorted");
    if (has_dups(rel))
        FAIL("result has duplicates");

    test_rel_free(rel);
    test_rel_free(delta);
    PASS("all checks passed");
}

/* Test 2: Old + all-duplicate delta -> no change */
static void
test_case_2(void)
{
    printf("Test 2: Old + all-duplicate delta\n");

    col_rel_t *rel = test_rel_create("rel", 2);
    col_rel_t *delta = test_rel_create("delta", 2);

    int64_t row1[2] = { 1, 2 };
    int64_t row2[2] = { 3, 4 };
    test_rel_append_row(rel, row1);
    test_rel_append_row(rel, row2);
    uint32_t old_nrows = rel->nrows;

    test_rel_append_row(rel, row1);
    test_rel_append_row(rel, row2);

    int rc = col_op_consolidate_incremental_delta(rel, old_nrows, delta);
    if (rc != 0)
        FAIL("function returned error");
    if (delta->nrows != 0)
        FAIL("delta_out should be empty");
    if (rel->nrows != 2)
        FAIL("merged should still have 2 rows");

    test_rel_free(rel);
    test_rel_free(delta);
    PASS("all checks passed");
}

/* Test 3: Old + unique delta -> merged union */
static void
test_case_3(void)
{
    printf("Test 3: Old + unique delta\n");

    col_rel_t *rel = test_rel_create("rel", 2);
    col_rel_t *delta = test_rel_create("delta", 2);

    int64_t row1[2] = { 1, 2 };
    int64_t row2[2] = { 3, 4 };
    test_rel_append_row(rel, row1);
    test_rel_append_row(rel, row2);
    uint32_t old_nrows = rel->nrows;

    int64_t row3[2] = { 2, 3 };
    int64_t row4[2] = { 5, 6 };
    test_rel_append_row(rel, row3);
    test_rel_append_row(rel, row4);

    int rc = col_op_consolidate_incremental_delta(rel, old_nrows, delta);
    if (rc != 0)
        FAIL("function returned error");
    if (rel->nrows != 4)
        FAIL("merged should have 4 rows");
    if (delta->nrows != 2)
        FAIL("delta_out should have 2 rows");
    if (!is_sorted(rel))
        FAIL("result not sorted");
    if (has_dups(rel))
        FAIL("result has duplicates");

    test_rel_free(rel);
    test_rel_free(delta);
    PASS("all checks passed");
}

/* Test 4: First iteration (old_nrows == 0) */
static void
test_case_4(void)
{
    printf("Test 4: First iteration edge case\n");

    col_rel_t *rel = test_rel_create("rel", 2);
    col_rel_t *delta = test_rel_create("delta", 2);

    int64_t row1[2] = { 5, 6 };
    int64_t row2[2] = { 1, 2 };
    int64_t row3[2] = { 3, 4 };
    test_rel_append_row(rel, row1);
    test_rel_append_row(rel, row2);
    test_rel_append_row(rel, row3);

    int rc = col_op_consolidate_incremental_delta(rel, 0, delta);
    if (rc != 0)
        FAIL("function returned error");
    if (delta->nrows != rel->nrows)
        FAIL("all rows should be new");
    if (!is_sorted(rel))
        FAIL("result not sorted");
    if (has_dups(rel))
        FAIL("result has duplicates");

    test_rel_free(rel);
    test_rel_free(delta);
    PASS("all checks passed");
}

/* Test 5: No new rows (delta_count == 0) */
static void
test_case_5(void)
{
    printf("Test 5: No new rows edge case\n");

    col_rel_t *rel = test_rel_create("rel", 2);
    col_rel_t *delta = test_rel_create("delta", 2);

    int64_t row1[2] = { 1, 2 };
    test_rel_append_row(rel, row1);
    uint32_t old_nrows = rel->nrows;

    int rc = col_op_consolidate_incremental_delta(rel, old_nrows, delta);
    if (rc != 0)
        FAIL("function returned error");
    if (delta->nrows != 0)
        FAIL("delta_out should be empty");
    if (rel->nrows != old_nrows)
        FAIL("merged should be unchanged");

    test_rel_free(rel);
    test_rel_free(delta);
    PASS("all checks passed");
}

/* ===== MAIN ===== */
int
main(void)
{
    printf("\n=== TDD RED Phase: col_op_consolidate_incremental_delta ===\n");
    printf("Expected: All tests FAIL (function doesn't exist yet)\n");
    printf(
        "When GREEN phase implements the function, all tests should PASS.\n\n");

    test_case_1();
    test_case_2();
    test_case_3();
    test_case_4();
    test_case_5();

    printf("\n✓ All test cases executed\n");
    return 0;
}
