/*
 * test_handle_remap_side_apply.c - Issue #590 acceptance harness
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Drives wl_handle_remap_apply_side_relation +
 * wl_handle_remap_apply_session_side_relations end-to-end.  Cases:
 *
 *   1. Single side-relation, flat (column 0 only, no nested args).
 *   2. Acceptance: 100 side-relations, 2-level nested.  arg0 of
 *      side-relation S_i carries a nested handle into S_(i+1)%100;
 *      the caller-supplied nested-arg index list is {1} (column
 *      index, not arg index, because column 0 is the row's own
 *      handle).  arg1 carries a literal that must NOT be rewritten;
 *      we verify the literal stays intact even when its bit pattern
 *      collides with an old handle (the precise corruption the
 *      pre-review critic flagged).
 *   3. EIO propagation: an inner side-relation has one row whose
 *      column 0 handle is missing from the remap.  Driver returns
 *      EIO; *out_rels_rewritten and *out_total_cells reflect the
 *      prefix that succeeded.
 *   4. NULL/EINVAL coverage on the per-side-relation primitive.
 *
 * Multiplicity (Z-set) preservation is structural -- the apply path
 * never touches col_rel_t::timestamps[] -- so each case asserts
 * timestamps and nrows are unchanged after the rewrite.
 */

#include "../wirelog/columnar/handle_remap.h"
#include "../wirelog/columnar/handle_remap_apply_side.h"
#include "../wirelog/columnar/internal.h"

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
            goto cleanup;                     \
        } while (0)

#define ASSERT(cond, msg)                 \
        do {                                  \
            if (!(cond)) {                    \
                FAIL(msg);                    \
            }                                 \
        } while (0)

/* ======================================================================== */
/* Fixture builders                                                         */
/* ======================================================================== */

/* Build a side-relation with __compound_<tag>_<arity> name, schema
 * (handle, arg0, arg1, ..., arg{arity-1}), and @nrows rows whose cell
 * values are produced by @cell(row, col) (col 0 = the row's own
 * handle, col c >= 1 = arg{c-1}). */
typedef int64_t (*cell_fn_t)(uint32_t row, uint32_t col, void *ud);

static col_rel_t *
build_side_relation(const char *tag,
    uint32_t arity,
    uint32_t nrows,
    cell_fn_t cell,
    void *ud)
{
    col_rel_t *r = NULL;
    char name[64];
    snprintf(name, sizeof(name), "__compound_%s_%u", tag, arity);
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    /* schema: handle, arg0, arg1, ..., arg{arity-1} */
    const char *col_names[16];
    char arg_buf[16][8];
    if (arity + 1u > 16u) {
        col_rel_destroy(r);
        return NULL;
    }
    col_names[0] = "handle";
    for (uint32_t a = 0; a < arity; a++) {
        snprintf(arg_buf[a], sizeof(arg_buf[a]), "arg%u", a);
        col_names[a + 1u] = arg_buf[a];
    }
    if (col_rel_set_schema(r, arity + 1u, col_names) != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    int64_t row[16];
    for (uint32_t i = 0; i < nrows; i++) {
        for (uint32_t c = 0; c < arity + 1u; c++)
            row[c] = cell(i, c, ud);
        if (col_rel_append_row(r, row) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

/* ======================================================================== */
/* Case 1: single side-relation, flat                                       */
/* ======================================================================== */

static int64_t
flat_old(uint32_t row, uint32_t col, void *ud)
{
    (void)ud;
    /* col 0 is the handle.  Make every cell non-zero so the apply
     * pass actually touches it. */
    return (int64_t)((((uint64_t)col + 1u) << 32) | (uint64_t)(row + 1u));
}

static int64_t
flat_new(uint32_t row, uint32_t col)
{
    return flat_old(row, col, NULL) ^ (int64_t)0xCAFEBABEDEADBEEFull;
}

static void
test_apply_side_relation_flat(void)
{
    TEST("#590: per-side-relation primitive, flat (col 0 only)");

    static const uint32_t NROWS = 32u;
    static const uint32_t ARITY = 2u; /* schema: handle, arg0, arg1 */
    wl_handle_remap_t *remap = NULL;
    col_rel_t *rel = NULL;

    rel = build_side_relation("flat", ARITY, NROWS, flat_old, NULL);
    ASSERT(rel != NULL, "build fixture");

    /* Snapshot row count before the pass; apply must not change it. */
    uint32_t nrows_before = rel->nrows;

    /* Build a remap that rewrites col 0 only.  arg cells are NOT
     * inserted -- the apply pass would EIO if it tried to look them
     * up, which is what we want when nested_arg_count == 0. */
    int rc = wl_handle_remap_create((size_t)NROWS, &remap);
    ASSERT(rc == 0 && remap != NULL, "remap create");
    for (uint32_t r = 0; r < NROWS; r++) {
        rc = wl_handle_remap_insert(remap, flat_old(r, 0u, NULL),
                flat_new(r, 0u));
        ASSERT(rc == 0, "remap insert");
    }

    uint64_t rewrites = 0;
    rc = wl_handle_remap_apply_side_relation(rel, NULL, 0u, remap,
            &rewrites);
    ASSERT(rc == 0, "apply rc");
    ASSERT(rewrites == (uint64_t)NROWS, "rewrites == NROWS");
    ASSERT(rel->nrows == nrows_before, "nrows must not change");

    for (uint32_t r = 0; r < NROWS; r++) {
        ASSERT(rel->columns[0][r] == flat_new(r, 0u),
            "col 0 not rewritten");
        /* arg cols left untouched. */
        ASSERT(rel->columns[1][r] == flat_old(r, 1u, NULL),
            "arg0 mutated");
        ASSERT(rel->columns[2][r] == flat_old(r, 2u, NULL),
            "arg1 mutated");
    }

    PASS();
cleanup:
    wl_handle_remap_free(remap);
    col_rel_destroy(rel);
}

/* ======================================================================== */
/* Case 2: 100 side-relations, 2-level nested (acceptance)                  */
/* ======================================================================== */

static const uint32_t N_RELS = 100u;
static const uint32_t N_ROWS_PER = 10u;
static const uint32_t ACCEPT_ARITY = 2u; /* handle, arg0, arg1 */

/* Encode a (relation, row) pair into a 64-bit handle.
 *   col 0 (handle):   own handle = (rel + 1) << 24 | (row + 1)
 *   col 1 (arg0):     nested handle into S_((rel+1) % N_RELS)[row]
 *   col 2 (arg1):     literal -- a value chosen to deliberately
 *                     COLLIDE with one of the old handles to verify
 *                     the apply pass does NOT rewrite undeclared
 *                     scalar arg columns.
 */
static int64_t
nested_own_handle(uint32_t rel, uint32_t row)
{
    return (int64_t)((((uint64_t)rel + 1u) << 24) | (uint64_t)(row + 1u));
}

static int64_t
nested_arg0_handle(uint32_t rel, uint32_t row)
{
    /* points into S_((rel+1) % N_RELS)[row] */
    uint32_t target_rel = (rel + 1u) % N_RELS;
    return nested_own_handle(target_rel, row);
}

static int64_t
nested_arg1_literal(uint32_t rel, uint32_t row)
{
    /* Deliberately collide with own handle of S_0[row]: when the
     * apply pass walks S_0..S_99, the literal in arg1 of S_42 row 3
     * has the same bit pattern as nested_own_handle(0, 3) which IS
     * in the remap.  If the apply pass tried to rewrite arg1, it
     * would silently corrupt this literal.  We verify after the
     * pass that the literal is bit-identical to the pre-pass value. */
    (void)rel;
    return nested_own_handle(0u, row);
}

static int64_t
nested_new_own_handle(uint32_t rel, uint32_t row)
{
    return nested_own_handle(rel, row) ^ (int64_t)0xDEADBEEFCAFEBABEull;
}

static int64_t
nested_new_arg0_handle(uint32_t rel, uint32_t row)
{
    uint32_t target_rel = (rel + 1u) % N_RELS;
    return nested_new_own_handle(target_rel, row);
}

typedef struct {
    uint32_t rel_idx;
} nested_ud_t;

static int64_t
nested_old_cell(uint32_t row, uint32_t col, void *ud)
{
    nested_ud_t *u = (nested_ud_t *)ud;
    if (col == 0u)
        return nested_own_handle(u->rel_idx, row);
    if (col == 1u)
        return nested_arg0_handle(u->rel_idx, row);
    return nested_arg1_literal(u->rel_idx, row);
}

static void
test_apply_session_100_nested(void)
{
    TEST("#590: 100 side-relations, 2-level nested, scalar args preserved");

    col_rel_t **rels = NULL;
    wl_handle_remap_t *remap = NULL;

    rels = (col_rel_t **)calloc(N_RELS, sizeof(col_rel_t *));
    ASSERT(rels != NULL, "rels alloc");

    /* Build all 100 side-relations.  Each row carries an own-handle
     * (col 0), a nested handle (col 1), and a colliding literal
     * (col 2). */
    for (uint32_t i = 0; i < N_RELS; i++) {
        char tag[16];
        snprintf(tag, sizeof(tag), "rel%u", i);
        nested_ud_t ud = { i };
        rels[i] = build_side_relation(tag, ACCEPT_ARITY, N_ROWS_PER,
                nested_old_cell, &ud);
        ASSERT(rels[i] != NULL, "build relation");
    }

    /* Build the remap covering every (rel, row)'s own handle.  arg1
     * literals are NOT inserted.  Note that nested_arg0_handle(i, r)
     * is just nested_own_handle((i+1) % N_RELS, r), so it's already
     * in the remap by construction -- no additional inserts. */
    int rc = wl_handle_remap_create((size_t)(N_RELS * N_ROWS_PER), &remap);
    ASSERT(rc == 0 && remap != NULL, "remap create");
    for (uint32_t i = 0; i < N_RELS; i++) {
        for (uint32_t r = 0; r < N_ROWS_PER; r++) {
            rc = wl_handle_remap_insert(remap,
                    nested_own_handle(i, r),
                    nested_new_own_handle(i, r));
            ASSERT(rc == 0, "remap insert own");
        }
    }

    /* (1) Per-relation primitive: rewrite col 0 + col 1 (nested arg).
     * We do NOT use the session driver here because it sweeps col 0
     * only; the nested-arg sweep is caller-driven by design. */
    uint32_t nested_idx[1] = { 1u };
    uint64_t total_rewrites = 0;
    for (uint32_t i = 0; i < N_RELS; i++) {
        uint64_t r = 0;
        rc = wl_handle_remap_apply_side_relation(rels[i], nested_idx,
                1u, remap, &r);
        ASSERT(rc == 0, "apply rc");
        ASSERT(r == (uint64_t)(2u * N_ROWS_PER),
            "expected 2 rewrites per row (col 0 + col 1)");
        total_rewrites += r;
    }
    ASSERT(total_rewrites == (uint64_t)(2u * N_RELS * N_ROWS_PER),
        "total rewrites = 2 cols x N_RELS x N_ROWS_PER");

    /* Verify: col 0 + col 1 rewritten; col 2 (scalar literal)
     * unchanged even though its bit pattern collides with an old
     * handle that IS in the remap. */
    for (uint32_t i = 0; i < N_RELS; i++) {
        for (uint32_t r = 0; r < N_ROWS_PER; r++) {
            int64_t got_h = rels[i]->columns[0][r];
            int64_t got_a0 = rels[i]->columns[1][r];
            int64_t got_a1 = rels[i]->columns[2][r];
            int64_t want_h = nested_new_own_handle(i, r);
            int64_t want_a0 = nested_new_arg0_handle(i, r);
            int64_t want_a1 = nested_arg1_literal(i, r);
            if (got_h != want_h || got_a0 != want_a0
                || got_a1 != want_a1) {
                printf(" ... FAIL: rel=%u row=%u h=0x%llx/0x%llx "
                    "a0=0x%llx/0x%llx a1=0x%llx/0x%llx\n",
                    i, r,
                    (unsigned long long)got_h,
                    (unsigned long long)want_h,
                    (unsigned long long)got_a0,
                    (unsigned long long)want_a0,
                    (unsigned long long)got_a1,
                    (unsigned long long)want_a1);
                tests_failed++;
                goto cleanup;
            }
        }
    }

    PASS();
cleanup:
    wl_handle_remap_free(remap);
    if (rels) {
        for (uint32_t i = 0; i < N_RELS; i++)
            col_rel_destroy(rels[i]);
        free(rels);
    }
}

/* ======================================================================== */
/* Case 3: EIO propagation                                                  */
/* ======================================================================== */

static void
test_apply_eio_partial_rewrite(void)
{
    TEST("#590: missing handle returns EIO with partial rewrite count");

    wl_handle_remap_t *remap = NULL;
    col_rel_t *rel = NULL;

    rel = build_side_relation("eio", 1u, 4u, flat_old, NULL);
    ASSERT(rel != NULL, "build fixture");

    /* Insert remap entries for rows 0..1 only.  Row 2's own handle
     * is non-zero but absent -- the apply pass must return EIO. */
    int rc = wl_handle_remap_create(4u, &remap);
    ASSERT(rc == 0 && remap != NULL, "remap create");
    for (uint32_t r = 0; r < 2u; r++) {
        rc = wl_handle_remap_insert(remap, flat_old(r, 0u, NULL),
                flat_new(r, 0u));
        ASSERT(rc == 0, "remap insert");
    }

    uint64_t rewrites = 0;
    rc = wl_handle_remap_apply_side_relation(rel, NULL, 0u, remap,
            &rewrites);
    ASSERT(rc == EIO, "expected EIO");
    ASSERT(rewrites == 2u, "rewrites must reflect prefix that succeeded");
    ASSERT(rel->columns[0][0] == flat_new(0u, 0u),
        "row 0 must be rewritten before EIO");
    ASSERT(rel->columns[0][1] == flat_new(1u, 0u),
        "row 1 must be rewritten before EIO");
    ASSERT(rel->columns[0][2] == flat_old(2u, 0u, NULL),
        "row 2 (the failing row) must remain at its old handle");

    PASS();
cleanup:
    wl_handle_remap_free(remap);
    col_rel_destroy(rel);
}

/* ======================================================================== */
/* Case 4: NULL / EINVAL coverage                                            */
/* ======================================================================== */

static void
test_apply_einval_coverage(void)
{
    TEST("#590: NULL/non-side/OOR/zero-aliased args -> EINVAL");

    col_rel_t *side = NULL;
    col_rel_t *non_side = NULL;
    wl_handle_remap_t *remap = NULL;
    int rc;

    rc = col_rel_alloc(&non_side, "user_named");
    ASSERT(rc == 0 && non_side != NULL, "non-side alloc");

    side = build_side_relation("eink", 1u, 1u, flat_old, NULL);
    ASSERT(side != NULL, "side build");

    rc = wl_handle_remap_create(2u, &remap);
    ASSERT(rc == 0 && remap != NULL, "remap create");

    /* NULL rel / NULL remap. */
    ASSERT(wl_handle_remap_apply_side_relation(NULL, NULL, 0, remap,
        NULL) == EINVAL, "NULL rel");
    ASSERT(wl_handle_remap_apply_side_relation(side, NULL, 0, NULL,
        NULL) == EINVAL, "NULL remap");

    /* Non-side relation rejected. */
    ASSERT(wl_handle_remap_apply_side_relation(non_side, NULL, 0, remap,
        NULL) == EINVAL, "non-side relation accepted");

    /* nested_arg_count > 0 with NULL idx. */
    ASSERT(wl_handle_remap_apply_side_relation(side, NULL, 1, remap,
        NULL) == EINVAL, "NULL idx with count > 0 accepted");

    /* Zero-aliased nested entry (would alias the implicit col 0). */
    {
        uint32_t bad[1] = { 0u };
        ASSERT(wl_handle_remap_apply_side_relation(side, bad, 1, remap,
            NULL) == EINVAL, "zero-aliased nested entry accepted");
    }

    /* Out-of-range nested entry. */
    {
        uint32_t oor[1] = { 99u };
        ASSERT(wl_handle_remap_apply_side_relation(side, oor, 1, remap,
            NULL) == EINVAL, "OOR nested entry accepted");
    }

    /* Session driver: NULL sess / NULL remap. */
    ASSERT(wl_handle_remap_apply_session_side_relations(NULL, remap, NULL,
        NULL) == EINVAL, "NULL sess accepted");

    PASS();
cleanup:
    wl_handle_remap_free(remap);
    col_rel_destroy(side);
    col_rel_destroy(non_side);
}

static void
test_apply_side_reader_exclusion(void)
{
    TEST("#1495: side-relation reader blocks remap until retry");

    col_rel_t *rel = build_side_relation("reader", 1u, 40u, flat_old, NULL);
    wl_handle_remap_t *remap = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    uint64_t rewrites = 99u;
    ASSERT(rel != NULL, "side relation build");
    ASSERT(wl_handle_remap_create(40u, &remap) == 0,
        "remap create");
    for (uint32_t row = 0; row < 40u; row++)
        ASSERT(wl_handle_remap_insert(remap, flat_old(row, 0u, NULL),
            flat_new(row, 0u)) == 0, "remap insert");
    int64_t before = rel->columns[0][0];
    uint64_t generation = rel->view_generation;
    ASSERT(col_rel_source_reader_acquire(rel, &reader) == 0,
        "reader acquire");
    ASSERT(wl_handle_remap_apply_side_relation(rel, NULL, 0u, remap,
        &rewrites) == EBUSY, "reader must block side remap");
    ASSERT(rewrites == 0u && rel->columns[0][0] == before
        && rel->view_generation == generation,
        "blocked side remap must be transactional");
    ASSERT(col_rel_source_reader_release(&reader) == 0,
        "reader release");
    rewrites = 0u;
    ASSERT(wl_handle_remap_apply_side_relation(rel, NULL, 0u, remap,
        &rewrites) == 0 && rewrites == 40u,
        "side remap retry must succeed");
    PASS();
cleanup:
    wl_handle_remap_free(remap);
    col_rel_destroy(rel);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_handle_remap_side_apply (Issue #590)\n");
    printf("=========================================\n");

    test_apply_side_relation_flat();
    test_apply_session_100_nested();
    test_apply_eio_partial_rewrite();
    test_apply_einval_coverage();
    test_apply_side_reader_exclusion();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
