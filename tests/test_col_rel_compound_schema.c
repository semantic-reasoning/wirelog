/*
 * test_col_rel_compound_schema.c - col_rel_t compound metadata (Issue #532 Task 1)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Unit tests for the compound-column metadata fields added to col_rel_t as
 * part of the inline-tier schema extension (Issue #532, Commit 1).  These
 * tests exercise data-structure additions only -- no logic that populates
 * the fields lands in this commit.
 *
 *   test_col_rel_side_default
 *       Freshly allocated relation has compound metadata in its zero
 *       default (kind == NONE, count == 0, arity_map == NULL,
 *       inline_physical_offset == 0), so SIDE-tier consumers that never
 *       opt into inline expansion observe a clean slate.
 *
 *   test_col_rel_inline_schema
 *       Manually populates the compound metadata on a relation to assert
 *       that the struct layout accepts INLINE-tier values and that the
 *       owned compound_arity_map is released on destroy without leaks.
 *
 *   test_col_rel_backward_compat
 *       Confirms the new fields are safely zero-initialised by the
 *       normal allocation paths (col_rel_alloc, col_rel_new_auto), so
 *       legacy call sites that never touch compound metadata continue
 *       to behave identically.
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/wirelog-types.h"

#include <errno.h>
#include <stddef.h>
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

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_col_rel_side_default(void)
{
    TEST("col_rel_t compound defaults to SIDE-clean state");
    col_rel_t *r = NULL;
    int rc = col_rel_alloc(&r, "side_default");
    if (rc != 0 || r == NULL) {
        tests_failed++;
        printf(" ... FAIL: col_rel_alloc failed rc=%d\n", rc);
        return;
    }

    /* A relation that never opts into inline expansion must present
     * compound_kind == NONE with no owned side-state.  The SIDE-tier
     * storage path keys off this invariant. */
    ASSERT(r->compound_kind == WIRELOG_COMPOUND_KIND_NONE,
        "compound_kind not NONE by default");
    ASSERT(r->compound_count == 0u, "compound_count not zero by default");
    ASSERT(r->compound_arity_map == NULL,
        "compound_arity_map not NULL by default");
    ASSERT(r->inline_physical_offset == 0u,
        "inline_physical_offset not zero by default");

    /* Even with a schema applied, unchanged compound metadata must stay
     * at its zero default -- set_schema does not touch these fields. */
    const char *names[2] = { "a", "b" };
    ASSERT(col_rel_set_schema(r, 2, names) == 0, "set_schema failed");
    ASSERT(r->compound_kind == WIRELOG_COMPOUND_KIND_NONE,
        "compound_kind perturbed by set_schema");
    ASSERT(r->compound_arity_map == NULL,
        "compound_arity_map perturbed by set_schema");

    PASS();
cleanup:
    col_rel_destroy(r);
}

static void
test_col_rel_inline_schema(void)
{
    TEST("col_rel_t accepts INLINE compound metadata");
    col_rel_t *r = NULL;
    uint32_t *arity_map = NULL;
    int rc = col_rel_alloc(&r, "inline_schema");
    if (rc != 0 || r == NULL) {
        tests_failed++;
        printf(" ... FAIL: col_rel_alloc failed rc=%d\n", rc);
        return;
    }

    /* Logical schema: (scalar, inline/2, scalar, inline/3).  Phase 2B will
     * synthesise this map automatically; here we populate it directly to
     * prove the struct can carry it and the destroy path frees it. */
    const uint32_t logical_ncols = 4u;
    arity_map = (uint32_t *)calloc(logical_ncols, sizeof(uint32_t));
    ASSERT(arity_map != NULL, "calloc arity_map failed");
    arity_map[0] = 1u;
    arity_map[1] = 2u;
    arity_map[2] = 1u;
    arity_map[3] = 3u;

    r->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    r->compound_count = 2u;
    r->compound_arity_map = arity_map;
    /* col 0 takes one physical slot; the first inline compound starts at
     * physical offset 1. */
    r->inline_physical_offset = 1u;
    arity_map = NULL; /* ownership transferred to r */

    /* Round-trip reads: the fields hold what we wrote without truncation. */
    ASSERT(r->compound_kind == WIRELOG_COMPOUND_KIND_INLINE,
        "compound_kind round-trip mismatch");
    ASSERT(r->compound_count == 2u, "compound_count round-trip mismatch");
    ASSERT(r->compound_arity_map != NULL, "compound_arity_map lost");
    ASSERT(r->compound_arity_map[0] == 1u, "arity_map[0] mismatch");
    ASSERT(r->compound_arity_map[1] == 2u, "arity_map[1] mismatch");
    ASSERT(r->compound_arity_map[2] == 1u, "arity_map[2] mismatch");
    ASSERT(r->compound_arity_map[3] == 3u, "arity_map[3] mismatch");
    ASSERT(r->inline_physical_offset == 1u,
        "inline_physical_offset round-trip mismatch");

    /* Prefix-sum sanity: sum(arity_map) == logical base (4 slots for
     * logical columns 0..3) + expansion delta; compound_count of 2
     * matches the two entries with arity > 1. */
    uint32_t compound_seen = 0u;
    uint32_t physical_total = 0u;
    for (uint32_t i = 0; i < logical_ncols; i++) {
        physical_total += r->compound_arity_map[i];
        if (r->compound_arity_map[i] > 1u)
            compound_seen++;
    }
    ASSERT(compound_seen == r->compound_count,
        "compound_count inconsistent with arity_map");
    ASSERT(physical_total == 7u, "physical column count mismatch");

    PASS();
cleanup:
    /* If we failed before transferring ownership, free the local buffer
     * so ASAN stays quiet. */
    free(arity_map);
    col_rel_destroy(r); /* frees r->compound_arity_map */
}

static void
test_col_rel_backward_compat(void)
{
    TEST("col_rel_t legacy allocation paths zero compound metadata");
    col_rel_t *via_alloc = NULL;
    col_rel_t *via_auto = NULL;

    ASSERT(col_rel_alloc(&via_alloc, "via_alloc") == 0, "col_rel_alloc failed");
    ASSERT(via_alloc->compound_kind == WIRELOG_COMPOUND_KIND_NONE,
        "col_rel_alloc did not zero compound_kind");
    ASSERT(via_alloc->compound_count == 0u,
        "col_rel_alloc did not zero compound_count");
    ASSERT(via_alloc->compound_arity_map == NULL,
        "col_rel_alloc did not zero compound_arity_map");
    ASSERT(via_alloc->inline_physical_offset == 0u,
        "col_rel_alloc did not zero inline_physical_offset");

    via_auto = col_rel_new_auto("via_auto", 3u);
    ASSERT(via_auto != NULL, "col_rel_new_auto failed");
    ASSERT(via_auto->compound_kind == WIRELOG_COMPOUND_KIND_NONE,
        "col_rel_new_auto did not zero compound_kind");
    ASSERT(via_auto->compound_count == 0u,
        "col_rel_new_auto did not zero compound_count");
    ASSERT(via_auto->compound_arity_map == NULL,
        "col_rel_new_auto did not zero compound_arity_map");
    ASSERT(via_auto->inline_physical_offset == 0u,
        "col_rel_new_auto did not zero inline_physical_offset");

    /* Appending rows through the legacy row-oriented API on a plain
     * relation must not touch compound metadata either. */
    int64_t row[3] = { 1, 2, 3 };
    ASSERT(col_rel_append_row(via_auto, row) == 0, "append_row failed");
    ASSERT(via_auto->compound_kind == WIRELOG_COMPOUND_KIND_NONE,
        "append_row perturbed compound_kind");
    ASSERT(via_auto->compound_arity_map == NULL,
        "append_row perturbed compound_arity_map");

    PASS();
cleanup:
    col_rel_destroy(via_alloc);
    col_rel_destroy(via_auto);
}

/* ======================================================================== */
/* Physical column layout (Issue #532 Task 2)                               */
/* ======================================================================== */

static void
test_physical_column_expansion(void)
{
    TEST("physical-column expansion with mixed INLINE/SIDE/scalar");

    /* Logical schema: [scalar, INLINE/2, scalar, INLINE/3, SIDE/5].
     * Expected physical layout:
     *   col 0  scalar     -> 1 slot  at offset 0
     *   col 1  INLINE/2   -> 2 slots at offset 1..2
     *   col 2  scalar     -> 1 slot  at offset 3
     *   col 3  INLINE/3   -> 3 slots at offset 4..6
     *   col 4  SIDE/5     -> 1 slot  at offset 7  (handle only)
     * physical_ncols         = 8
     * inline_physical_offset = 1  (first INLINE column)
     * compound_count         = 2  (INLINE columns only). */
    const col_rel_logical_col_t cols[5] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
        { WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u },
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
        { WIRELOG_COMPOUND_KIND_INLINE, 3u, 1u },
        { WIRELOG_COMPOUND_KIND_SIDE,   5u, 1u },
    };
    uint32_t offsets[5] = { 0 };
    uint32_t physical = 0u;
    uint32_t first_inline = 0u;
    uint32_t compound_count = 0u;
    col_rel_t *r = NULL;

    int rc = col_rel_compute_physical_layout(cols, 5u, &physical, offsets,
            &first_inline, &compound_count);
    if (rc != 0) {
        tests_failed++;
        printf(" ... FAIL: compute_physical_layout rc=%d\n", rc);
        return;
    }

    ASSERT(physical == 8u, "physical_ncols != 8");
    ASSERT(offsets[0] == 0u, "offsets[0] != 0");
    ASSERT(offsets[1] == 1u, "offsets[1] != 1");
    ASSERT(offsets[2] == 3u, "offsets[2] != 3");
    ASSERT(offsets[3] == 4u, "offsets[3] != 4");
    ASSERT(offsets[4] == 7u, "offsets[4] != 7");
    ASSERT(first_inline == 1u, "inline_physical_offset != 1");
    ASSERT(compound_count == 2u, "compound_count != 2");

    /* All-scalar schema degenerates: physical == logical, no inline offset. */
    const col_rel_logical_col_t scalar_cols[3] = {
        { WIRELOG_COMPOUND_KIND_NONE, 0u, 0u },
        { WIRELOG_COMPOUND_KIND_NONE, 0u, 0u },
        { WIRELOG_COMPOUND_KIND_NONE, 0u, 0u },
    };
    physical = 99u;
    first_inline = 99u;
    compound_count = 99u;
    ASSERT(col_rel_compute_physical_layout(scalar_cols, 3u, &physical, NULL,
        &first_inline, &compound_count)
        == 0,
        "scalar layout rejected");
    ASSERT(physical == 3u, "scalar physical_ncols != 3");
    ASSERT(first_inline == 0u, "scalar first_inline != 0");
    ASSERT(compound_count == 0u, "scalar compound_count != 0");

    /* apply_compound_schema: populates the owned map; destroy releases it. */
    ASSERT(col_rel_alloc(&r, "phys_expand") == 0, "col_rel_alloc failed");
    ASSERT(col_rel_apply_compound_schema(r, cols, 5u) == 0,
        "apply_compound_schema failed");
    ASSERT(r->compound_kind == WIRELOG_COMPOUND_KIND_INLINE,
        "compound_kind not INLINE after apply");
    ASSERT(r->compound_count == 2u, "compound_count on rel != 2");
    ASSERT(r->inline_physical_offset == 1u,
        "inline_physical_offset on rel != 1");
    ASSERT(r->compound_arity_map != NULL,
        "compound_arity_map not allocated");
    ASSERT(r->compound_arity_map[0] == 1u, "rel arity_map[0] != 1");
    ASSERT(r->compound_arity_map[1] == 2u, "rel arity_map[1] != 2");
    ASSERT(r->compound_arity_map[2] == 1u, "rel arity_map[2] != 1");
    ASSERT(r->compound_arity_map[3] == 3u, "rel arity_map[3] != 3");
    ASSERT(r->compound_arity_map[4] == 1u,
        "rel arity_map[4] != 1 (SIDE is 1 slot)");

    /* Re-apply with a different schema: the previous arity_map is freed and
     * replaced, not leaked. Switch to a SIDE-only schema to also verify the
     * relation-level kind flips accordingly. */
    const col_rel_logical_col_t side_only[2] = {
        { WIRELOG_COMPOUND_KIND_SIDE, 3u, 1u },
        { WIRELOG_COMPOUND_KIND_NONE, 0u, 0u },
    };
    ASSERT(col_rel_apply_compound_schema(r, side_only, 2u) == 0,
        "reapply with SIDE schema failed");
    ASSERT(r->compound_kind == WIRELOG_COMPOUND_KIND_SIDE,
        "compound_kind not SIDE after reapply");
    ASSERT(r->compound_count == 0u,
        "compound_count should be 0 (no INLINE cols) after reapply");
    ASSERT(r->inline_physical_offset == 0u,
        "inline_physical_offset should be 0 on SIDE-only schema");
    ASSERT(r->compound_arity_map[0] == 1u, "SIDE col width != 1");
    ASSERT(r->compound_arity_map[1] == 1u, "scalar col width != 1");

    PASS();
cleanup:
    col_rel_destroy(r);
}

static void
test_arity_overflow(void)
{
    TEST("arity overflow rejected (INLINE arity > MAX_ARITY)");

    /* Spot-check the boundary: arity == MAX_ARITY must succeed, arity ==
     * MAX_ARITY + 1 must fail. Also verify arity == 0 is rejected because
     * an inline compound with no arguments is degenerate. */
    col_rel_logical_col_t col = {
        WIRELOG_COMPOUND_KIND_INLINE,
        WL_COMPOUND_INLINE_MAX_ARITY, /* == 4, acceptable boundary */
        1u,
    };
    uint32_t physical = 0u;
    ASSERT(col_rel_compute_physical_layout(&col, 1u, &physical, NULL, NULL,
        NULL)
        == 0,
        "boundary arity rejected");
    ASSERT(physical == WL_COMPOUND_INLINE_MAX_ARITY,
        "boundary physical_ncols mismatch");

    col.arity = WL_COMPOUND_INLINE_MAX_ARITY + 1u; /* == 5, overflow */
    ASSERT(col_rel_compute_physical_layout(&col, 1u, NULL, NULL, NULL, NULL)
        == EINVAL,
        "overflow arity not rejected");

    col.arity = 0u;
    ASSERT(col_rel_compute_physical_layout(&col, 1u, NULL, NULL, NULL, NULL)
        == EINVAL,
        "zero-arity INLINE not rejected");

    /* Overflow leaves relation state untouched (no partial write). */
    col_rel_t *r = NULL;
    ASSERT(col_rel_alloc(&r, "overflow") == 0, "col_rel_alloc failed");
    col.arity = WL_COMPOUND_INLINE_MAX_ARITY + 2u;
    ASSERT(col_rel_apply_compound_schema(r, &col, 1u) == EINVAL,
        "overflow not rejected via apply");
    ASSERT(r->compound_kind == WIRELOG_COMPOUND_KIND_NONE,
        "compound_kind mutated on failure");
    ASSERT(r->compound_arity_map == NULL,
        "compound_arity_map allocated on failure");
    col_rel_destroy(r);

    /* SIDE-kind arity is NOT capped by the inline limit -- SIDE columns
     * hold only a handle in the row, regardless of functor arity. */
    col_rel_logical_col_t side = {
        WIRELOG_COMPOUND_KIND_SIDE,
        16u, /* intentionally above WL_COMPOUND_INLINE_MAX_ARITY */
        1u,
    };
    ASSERT(col_rel_compute_physical_layout(&side, 1u, &physical, NULL, NULL,
        NULL)
        == 0,
        "SIDE arity incorrectly capped");
    ASSERT(physical == 1u, "SIDE col should occupy exactly 1 physical slot");

    PASS();
cleanup:
    return;
}

static void
test_depth_validation(void)
{
    TEST("nesting depth validation (INLINE depth > MAX_DEPTH rejected)");
    col_rel_t *r = NULL;

    /* depth == MAX_DEPTH (= 1) is the acceptable boundary. */
    col_rel_logical_col_t col = {
        WIRELOG_COMPOUND_KIND_INLINE,
        2u,
        WL_COMPOUND_INLINE_MAX_DEPTH,
    };
    ASSERT(col_rel_compute_physical_layout(&col, 1u, NULL, NULL, NULL, NULL)
        == 0,
        "boundary depth rejected");

    col.depth = WL_COMPOUND_INLINE_MAX_DEPTH + 1u; /* nested; must fail */
    ASSERT(col_rel_compute_physical_layout(&col, 1u, NULL, NULL, NULL, NULL)
        == EINVAL,
        "depth overflow not rejected");

    /* Mixing a depth-valid and depth-invalid column: function must still
     * fail and must not partially commit to a physical count. */
    const col_rel_logical_col_t mixed[2] = {
        { WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u },
        { WIRELOG_COMPOUND_KIND_INLINE, 3u, 2u }, /* too deep */
    };
    uint32_t offsets[2] = { 99u, 99u };
    uint32_t physical = 99u;
    ASSERT(col_rel_compute_physical_layout(mixed, 2u, &physical, offsets,
        NULL, NULL)
        == EINVAL,
        "mixed depth overflow not rejected");
    ASSERT(physical == 99u, "physical_ncols modified on EINVAL");
    ASSERT(offsets[0] == 99u && offsets[1] == 99u,
        "offsets modified on EINVAL");

    /* apply_compound_schema must refuse and preserve rel state on EINVAL. */
    ASSERT(col_rel_alloc(&r, "depth_validation") == 0,
        "col_rel_alloc failed");
    ASSERT(col_rel_apply_compound_schema(r, mixed, 2u) == EINVAL,
        "apply did not reject depth overflow");
    ASSERT(r->compound_kind == WIRELOG_COMPOUND_KIND_NONE,
        "compound_kind mutated by failed apply");
    ASSERT(r->compound_arity_map == NULL,
        "compound_arity_map allocated on failed apply");

    /* SIDE columns are not subject to the inline depth limit (the side-
     * relation path represents nesting via handle chains, not expansion). */
    col_rel_logical_col_t side = {
        WIRELOG_COMPOUND_KIND_SIDE,
        3u,
        5u, /* arbitrary: ignored for SIDE */
    };
    ASSERT(col_rel_compute_physical_layout(&side, 1u, NULL, NULL, NULL, NULL)
        == 0,
        "SIDE depth incorrectly validated");

    /* NULL / zero-length inputs */
    ASSERT(col_rel_compute_physical_layout(NULL, 0u, NULL, NULL, NULL, NULL)
        == EINVAL,
        "NULL input not rejected");
    ASSERT(col_rel_compute_physical_layout(&col, 0u, NULL, NULL, NULL, NULL)
        == EINVAL,
        "zero logical_ncols not rejected");

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
    printf("test_col_rel_compound_schema (Issue #532 Tasks 1 + 2)\n");
    printf("=====================================================\n");

    test_col_rel_side_default();
    test_col_rel_inline_schema();
    test_col_rel_backward_compat();
    test_physical_column_expansion();
    test_arity_overflow();
    test_depth_validation();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
