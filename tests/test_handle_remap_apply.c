/*
 * test_handle_remap_apply.c - Issue #589 acceptance harness
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Drives wl_handle_remap_apply_columns end-to-end against a 10K-row x
 * 3-compound-column fixture, the size #589 pins as the acceptance
 * test.  The fixture builds the remap table by hand (the rotation
 * helper that would normally populate it is #550 Option C, not yet
 * implemented), so this is unit-style coverage of the apply pass +
 * the underlying SplitMix64 / open-addressing table together.
 *
 * Acceptance bullets the test pins:
 *   - All EDB compound handles remapped (every cell rewritten)
 *   - Handle lookup always succeeds (no EIO miss path)
 *   - 10K rows, 3 compound columns
 *   - TSan clean on hash lookup
 *   - ASAN clean
 *
 * The test is single-threaded; the project's TSan suite runs the
 * binary under instrumentation but no concurrent thread is involved
 * here -- the W=2 alloc-vs-gc race is covered by #584's harness.
 * What this test verifies under TSan is the *absence* of any
 * concurrent state inside wl_handle_remap_apply_columns itself
 * (e.g. accidental thread-local globals).  The default sanitizer
 * build also catches out-of-bounds, use-after-free, and signed
 * overflow inside the row scan.
 */

#include "../wirelog/columnar/handle_remap.h"
#include "../wirelog/columnar/handle_remap_apply.h"
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
/* Fixture builder                                                          */
/* ======================================================================== */

/* Synthesise a deterministic old handle per (row, column) pair.  We
 * spread bits across all 64 so the SplitMix64 finalizer in
 * wl_handle_remap_hash actually sees varied input. */
static int64_t
mk_old_handle(uint32_t row, uint32_t col)
{
    /* +1 to keep handles non-zero; high bits encode column to spread
     * across the keyspace; low bits encode row monotonically. */
    return (int64_t)(((uint64_t)(col + 1u) << 40) ^ ((uint64_t)row + 1u));
}

/* Deterministic post-rotation handle.  The remap is mk_old_handle(r,c)
 * -> mk_new_handle(r,c).  Choosing a different transform from
 * mk_old_handle makes round-trip mistakes obvious. */
static int64_t
mk_new_handle(uint32_t row, uint32_t col)
{
    return (int64_t)(((uint64_t)(col + 1u) << 40)
           ^ ((uint64_t)row + 1u)
           ^ 0xDEADBEEFCAFEBABEull);
}

static col_rel_t *
build_relation(uint32_t nrows, uint32_t ncompound_cols)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, "edb_remap_fixture") != 0)
        return NULL;
    /* Build a flat 3-column schema; col_rel_set_schema also sizes the
     * physical columns array. */
    const char *names[3] = { "h0", "h1", "h2" };
    if (ncompound_cols > 3u) {
        col_rel_destroy(r);
        return NULL;
    }
    if (col_rel_set_schema(r, ncompound_cols, names) != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    /* Populate row-by-row using append_row -- this matches how the
     * EDB ingest path fills relations (one row at a time, growing
     * capacity as needed) so the apply pass exercises the same
     * column-major buffer the production path does. */
    int64_t row[3];
    for (uint32_t i = 0; i < nrows; i++) {
        for (uint32_t c = 0; c < ncompound_cols; c++)
            row[c] = mk_old_handle(i, c);
        if (col_rel_append_row(r, row) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_apply_10k_x_3_columns(void)
{
    TEST("#589: 10K rows x 3 compound columns -- all handles remapped");

    static const uint32_t NROWS = 10000u;
    static const uint32_t NCOLS = 3u;

    wl_handle_remap_t *remap = NULL;
    col_rel_t *rel = NULL;

    /* (1) Build the fixture relation.  Every cell carries a
     * deterministic non-zero old handle. */
    rel = build_relation(NROWS, NCOLS);
    ASSERT(rel != NULL, "build_relation");

    /* (2) Build the remap table sized for the full population.  The
     * caller passes the raw expected count; the constructor applies
     * the load-factor headroom (per #586 §2.8 / handle_remap.c). */
    int rc = wl_handle_remap_create((size_t)(NROWS * NCOLS), &remap);
    ASSERT(rc == 0 && remap != NULL, "remap create");
    for (uint32_t r = 0; r < NROWS; r++) {
        for (uint32_t c = 0; c < NCOLS; c++) {
            rc = wl_handle_remap_insert(remap,
                    mk_old_handle(r, c),
                    mk_new_handle(r, c));
            if (rc != 0)
                FAIL("remap insert mid-build");
        }
    }

    /* (3) Apply the pass.  Acceptance: every cell rewritten. */
    uint32_t handle_cols[3] = { 0u, 1u, 2u };
    uint64_t rewrites = 0u;
    rc = wl_handle_remap_apply_columns(rel, handle_cols, NCOLS, remap,
            &rewrites);
    ASSERT(rc == 0, "apply_columns returned non-zero");
    ASSERT(rewrites == (uint64_t)NROWS * (uint64_t)NCOLS,
        "rewrites count must equal nrows * ncols");

    /* (4) Spot-check a stratified sample: first row, last row, and a
     * mid sample.  Verifying every cell would just re-implement the
     * apply loop; stratified sampling catches off-by-one and
     * cross-column corruption.  Probe values are bare literals (not
     * NROWS - 1) because MSVC's default C dialect rejects
     * static-storage initializers that reference `static const`
     * locals (C2099). */
    static const uint32_t probe_rows[] = {
        0u, 1u, 4999u, 5000u, 5001u, 9999u
    };
    for (size_t i = 0; i < sizeof(probe_rows) / sizeof(probe_rows[0]);
        i++) {
        uint32_t pr = probe_rows[i];
        for (uint32_t c = 0; c < NCOLS; c++) {
            int64_t got = rel->columns[c][pr];
            int64_t want = mk_new_handle(pr, c);
            if (got != want) {
                printf(" ... FAIL: row=%u col=%u got=0x%llx want=0x%llx\n",
                    pr, c, (unsigned long long)got,
                    (unsigned long long)want);
                tests_failed++;
                goto cleanup;
            }
        }
    }

    PASS();
cleanup:
    wl_handle_remap_free(remap);
    col_rel_destroy(rel);
}

static void
test_apply_zero_handles_unchanged(void)
{
    TEST("#589: zero (NULL) handles are skipped, not flagged as miss");

    wl_handle_remap_t *remap = NULL;
    col_rel_t *rel = NULL;

    rel = build_relation(8u, 1u);
    ASSERT(rel != NULL, "build_relation");

    /* Zero out every other cell so the apply pass walks past genuine
     * NULL handles without entering the EIO miss path. */
    for (uint32_t r = 0; r < rel->nrows; r += 2u)
        rel->columns[0][r] = 0;

    int rc = wl_handle_remap_create(8u, &remap);
    ASSERT(rc == 0 && remap != NULL, "remap create");
    for (uint32_t r = 1u; r < rel->nrows; r += 2u) {
        rc = wl_handle_remap_insert(remap,
                mk_old_handle(r, 0u),
                mk_new_handle(r, 0u));
        ASSERT(rc == 0, "remap insert");
    }

    uint32_t handle_cols[1] = { 0u };
    uint64_t rewrites = 0u;
    rc = wl_handle_remap_apply_columns(rel, handle_cols, 1u, remap,
            &rewrites);
    ASSERT(rc == 0, "apply_columns rc != 0");
    ASSERT(rewrites == 4u, "exactly half of 8 cells should rewrite");
    /* The zero cells stayed zero; the rewritten cells got new handles. */
    for (uint32_t r = 0; r < rel->nrows; r++) {
        if (r % 2u == 0u) {
            if (rel->columns[0][r] != 0)
                FAIL("zero cell mutated");
        } else {
            if (rel->columns[0][r] != mk_new_handle(r, 0u))
                FAIL("non-zero cell not rewritten");
        }
    }

    PASS();
cleanup:
    wl_handle_remap_free(remap);
    col_rel_destroy(rel);
}

static void
test_apply_missing_handle_returns_eio(void)
{
    TEST("#589: a non-zero handle missing from remap returns EIO");

    wl_handle_remap_t *remap = NULL;
    col_rel_t *rel = NULL;

    rel = build_relation(4u, 1u);
    ASSERT(rel != NULL, "build_relation");

    int rc = wl_handle_remap_create(4u, &remap);
    ASSERT(rc == 0 && remap != NULL, "remap create");
    /* Only insert the FIRST row's handle.  The second row's handle is
     * non-zero but absent -- the apply pass must return EIO. */
    rc = wl_handle_remap_insert(remap,
            mk_old_handle(0u, 0u),
            mk_new_handle(0u, 0u));
    ASSERT(rc == 0, "remap insert");

    uint32_t handle_cols[1] = { 0u };
    uint64_t rewrites = 0u;
    rc = wl_handle_remap_apply_columns(rel, handle_cols, 1u, remap,
            &rewrites);
    ASSERT(rc == EIO, "apply must return EIO on missing handle");
    /* Only the first row should have been rewritten before the miss. */
    ASSERT(rewrites == 1u, "rewrites must reflect prefix that succeeded");
    ASSERT(rel->columns[0][0] == mk_new_handle(0u, 0u),
        "first row should have been rewritten");
    ASSERT(rel->columns[0][1] == mk_old_handle(1u, 0u),
        "row that triggered EIO must remain at its old handle");

    PASS();
cleanup:
    wl_handle_remap_free(remap);
    col_rel_destroy(rel);
}

static void
test_apply_invalid_args_einval(void)
{
    TEST("#589: NULL/OOR arguments are rejected with EINVAL");

    col_rel_t *rel = build_relation(4u, 1u);
    ASSERT(rel != NULL, "build_relation");
    wl_handle_remap_t *remap = NULL;
    int rc = wl_handle_remap_create(4u, &remap);
    ASSERT(rc == 0, "remap create");

    uint32_t cols_ok[1] = { 0u };
    uint32_t cols_oor[1] = { 99u };
    uint64_t out = 0;

    ASSERT(wl_handle_remap_apply_columns(NULL, cols_ok, 1u, remap, &out)
        == EINVAL, "NULL rel not rejected");
    ASSERT(wl_handle_remap_apply_columns(rel, cols_ok, 1u, NULL, &out)
        == EINVAL, "NULL remap not rejected");
    ASSERT(wl_handle_remap_apply_columns(rel, NULL, 1u, remap, &out)
        == EINVAL, "NULL idx with count > 0 not rejected");
    ASSERT(wl_handle_remap_apply_columns(rel, cols_oor, 1u, remap, &out)
        == EINVAL, "out-of-range column index not rejected");
    /* count == 0 with NULL idx is a no-op success. */
    ASSERT(wl_handle_remap_apply_columns(rel, NULL, 0u, remap, &out)
        == 0, "count=0 should be a no-op success");
    ASSERT(out == 0, "no-op must report zero rewrites");

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
    printf("test_handle_remap_apply (Issue #589)\n");
    printf("====================================\n");

    test_apply_10k_x_3_columns();
    test_apply_zero_handles_unchanged();
    test_apply_missing_handle_returns_eio();
    test_apply_invalid_args_einval();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
