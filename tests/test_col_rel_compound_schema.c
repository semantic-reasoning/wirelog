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
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_col_rel_compound_schema (Issue #532 Task 1)\n");
    printf("================================================\n");

    test_col_rel_side_default();
    test_col_rel_inline_schema();
    test_col_rel_backward_compat();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
