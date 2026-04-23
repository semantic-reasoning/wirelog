/*
 * test_col_rel_inline_storage.c - inline compound store/retrieve/retract
 * (Issue #532 Task 3)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies the inline compound storage ops added in eval.c:
 *
 *   test_inline_store_retrieve
 *       Schema (scalar, inline/2, scalar, inline/3). Append a row, store
 *       args into each inline column via wl_col_rel_store_inline_compound,
 *       retrieve them back via wl_col_rel_retrieve_inline_compound, and
 *       confirm the raw physical slots line up with the Task #2 prefix-sum
 *       offset map (1, 3 for the two inline columns).
 *
 *   test_inline_retract_sync
 *       Store, then retract. Physical slots must remain intact so delta
 *       machinery and join matching can still see the original tuple
 *       during retraction-seeded evaluation. Retract with multiplicity
 *       zero is rejected as a precondition violation.
 *
 *   test_inline_multiarg
 *       Two independent inline columns of different arities in the same
 *       row round-trip without cross-talk; a second row writes different
 *       values and leaves the first row unchanged.
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/wirelog-types.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                              \
        do {                                        \
            tests_run++;                            \
            printf("  [%d] %s", tests_run, name);   \
        } while (0)

#define PASS()                                  \
        do {                                        \
            tests_passed++;                         \
            printf(" ... PASS\n");                  \
        } while (0)

#define FAIL(msg)                               \
        do {                                        \
            tests_failed++;                         \
            printf(" ... FAIL: %s\n", (msg));       \
            goto cleanup;                           \
        } while (0)

#define ASSERT(cond, msg)                       \
        do {                                        \
            if (!(cond)) {                          \
                FAIL(msg);                          \
            }                                       \
        } while (0)

/* Build the (scalar, inline/2, scalar, inline/3) fixture shared by each
 * test: allocates a relation, applies the compound schema, materialises
 * the 7-column physical schema, and appends one row of zeros so row 0 is
 * addressable by the storage ops.  Returns 0 on success; caller owns the
 * returned relation and must col_rel_destroy it. */
static int
build_inline_fixture(col_rel_t **out_rel)
{
    col_rel_t *r = NULL;
    int rc = col_rel_alloc(&r, "inline_fixture");
    if (rc != 0)
        return rc;

    const col_rel_logical_col_t logical[4] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
        { WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u },
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
        { WIRELOG_COMPOUND_KIND_INLINE, 3u, 1u },
    };
    rc = col_rel_apply_compound_schema(r, logical, 4u);
    if (rc != 0) {
        col_rel_destroy(r);
        return rc;
    }
    /* Physical schema: 1 + 2 + 1 + 3 = 7 slots. */
    rc = col_rel_set_schema(r, 7u, NULL);
    if (rc != 0) {
        col_rel_destroy(r);
        return rc;
    }

    const int64_t zeros[7] = { 0, 0, 0, 0, 0, 0, 0 };
    rc = col_rel_append_row(r, zeros);
    if (rc != 0) {
        col_rel_destroy(r);
        return rc;
    }
    *out_rel = r;
    return 0;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_inline_store_retrieve(void)
{
    TEST("wl_col_rel_store/retrieve_inline_compound round-trip");
    col_rel_t *r = NULL;
    int rc = build_inline_fixture(&r);
    if (rc != 0 || r == NULL) {
        tests_failed++;
        printf(" ... FAIL: fixture rc=%d\n", rc);
        return;
    }

    const int64_t args2[2] = { 1001, 1002 };
    ASSERT(wl_col_rel_store_inline_compound(r, 0u, 1u, args2, 2u) == 0,
        "store inline/2 failed");

    /* Raw physical slots must line up with the Task #2 offset map: logical
     * column 1 begins at physical offset 1, so args2[0]..[1] land in
     * columns 1, 2. */
    ASSERT(col_rel_get(r, 0u, 1u) == 1001, "physical slot[1] mismatch");
    ASSERT(col_rel_get(r, 0u, 2u) == 1002, "physical slot[2] mismatch");

    int64_t out2[2] = { 0 };
    ASSERT(wl_col_rel_retrieve_inline_compound(r, 0u, 1u, out2, 2u) == 0,
        "retrieve inline/2 failed");
    ASSERT(out2[0] == 1001 && out2[1] == 1002,
        "retrieve inline/2 data mismatch");

    /* Arity mismatch against the compound_arity_map must be rejected. */
    ASSERT(wl_col_rel_store_inline_compound(r, 0u, 1u, args2, 3u) == EINVAL,
        "store accepted arity mismatch");
    ASSERT(wl_col_rel_retrieve_inline_compound(r, 0u, 1u, out2, 3u) == EINVAL,
        "retrieve accepted arity mismatch");

    /* Row out of bounds is rejected. */
    ASSERT(wl_col_rel_store_inline_compound(r, 99u, 1u, args2, 2u) == EINVAL,
        "store accepted out-of-range row");

    PASS();
cleanup:
    col_rel_destroy(r);
}

static void
test_inline_retract_sync(void)
{
    TEST("wl_col_rel_retract_inline_compound preserves physical slots");
    col_rel_t *r = NULL;
    int rc = build_inline_fixture(&r);
    if (rc != 0 || r == NULL) {
        tests_failed++;
        printf(" ... FAIL: fixture rc=%d\n", rc);
        return;
    }

    const int64_t args[3] = { 7, 11, 13 };
    ASSERT(wl_col_rel_store_inline_compound(r, 0u, 3u, args, 3u) == 0,
        "store inline/3 failed");

    /* Multiplicity 0 is rejected: retraction must carry a signed count. */
    ASSERT(wl_col_rel_retract_inline_compound(r, 0u, 3u, 0) == EINVAL,
        "retract accepted multiplicity == 0");

    /* A valid retraction reports success and MUST NOT mutate physical
     * slots: Z-set multiplicity is tracked at the delta layer, and join
     * matching during retraction-seeded evaluation still needs to see
     * the original tuple. */
    ASSERT(wl_col_rel_retract_inline_compound(r, 0u, 3u, -1) == 0,
        "retract inline/3 failed");

    int64_t out3[3] = { 0 };
    ASSERT(wl_col_rel_retrieve_inline_compound(r, 0u, 3u, out3, 3u) == 0,
        "retrieve after retract failed");
    ASSERT(out3[0] == 7 && out3[1] == 11 && out3[2] == 13,
        "retract mutated physical slots");

    PASS();
cleanup:
    col_rel_destroy(r);
}

static void
test_inline_multiarg(void)
{
    TEST("multiple inline columns round-trip independently");
    col_rel_t *r = NULL;
    int rc = build_inline_fixture(&r);
    if (rc != 0 || r == NULL) {
        tests_failed++;
        printf(" ... FAIL: fixture rc=%d\n", rc);
        return;
    }

    /* Append a second row so we can exercise two-row independence. */
    const int64_t zeros[7] = { 0 };
    ASSERT(col_rel_append_row(r, zeros) == 0, "append second row failed");

    const int64_t r0_inline2[2] = { 21, 22 };
    const int64_t r0_inline3[3] = { 31, 32, 33 };
    ASSERT(wl_col_rel_store_inline_compound(r, 0u, 1u, r0_inline2, 2u) == 0,
        "row0 inline/2 store failed");
    ASSERT(wl_col_rel_store_inline_compound(r, 0u, 3u, r0_inline3, 3u) == 0,
        "row0 inline/3 store failed");

    const int64_t r1_inline2[2] = { 41, 42 };
    const int64_t r1_inline3[3] = { 51, 52, 53 };
    ASSERT(wl_col_rel_store_inline_compound(r, 1u, 1u, r1_inline2, 2u) == 0,
        "row1 inline/2 store failed");
    ASSERT(wl_col_rel_store_inline_compound(r, 1u, 3u, r1_inline3, 3u) == 0,
        "row1 inline/3 store failed");

    /* Row 0 must round-trip both inline columns unchanged after row 1's
     * writes — no cross-talk in the column-major layout. */
    int64_t out2[2] = { 0 };
    int64_t out3[3] = { 0 };
    ASSERT(wl_col_rel_retrieve_inline_compound(r, 0u, 1u, out2, 2u) == 0,
        "row0 inline/2 retrieve failed");
    ASSERT(out2[0] == 21 && out2[1] == 22, "row0 inline/2 corrupted");
    ASSERT(wl_col_rel_retrieve_inline_compound(r, 0u, 3u, out3, 3u) == 0,
        "row0 inline/3 retrieve failed");
    ASSERT(out3[0] == 31 && out3[1] == 32 && out3[2] == 33,
        "row0 inline/3 corrupted");

    /* And row 1 carries its own distinct values. */
    ASSERT(wl_col_rel_retrieve_inline_compound(r, 1u, 1u, out2, 2u) == 0,
        "row1 inline/2 retrieve failed");
    ASSERT(out2[0] == 41 && out2[1] == 42, "row1 inline/2 corrupted");
    ASSERT(wl_col_rel_retrieve_inline_compound(r, 1u, 3u, out3, 3u) == 0,
        "row1 inline/3 retrieve failed");
    ASSERT(out3[0] == 51 && out3[1] == 52 && out3[2] == 53,
        "row1 inline/3 corrupted");

    /* A logical column index beyond the schema is rejected. */
    ASSERT(wl_col_rel_store_inline_compound(r, 0u, 99u, r0_inline2, 2u)
        == EINVAL, "store accepted logical_col out of range");

    PASS();
cleanup:
    col_rel_destroy(r);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_col_rel_inline_storage (Issue #532 Task 3)\n");
    printf("===============================================\n");

    test_inline_store_retrieve();
    test_inline_retract_sync();
    test_inline_multiarg();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
