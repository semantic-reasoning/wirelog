/*
 * test_col_rel_deep_copy.c - col_rel_deep_copy contract (Issue #553)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Unit tests for col_rel_deep_copy.  The function produces a fully owned
 * deep copy of a source relation; subsequent commits expand the surface
 * area covered (columns, schema, timestamps, merge buffer, retract
 * backup, compound metadata).  Each test in this file is the smallest
 * proof that one slice of the contract holds end-to-end.
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
/* Group H/I/J + NULL-arg scaffold (Issue #553 Commit 1)                    */
/* ======================================================================== */

static void
test_null_args_rejected(void)
{
    TEST("NULL src and NULL out are rejected with EINVAL");
    col_rel_t *out = NULL;
    col_rel_t *dummy = NULL;

    /* NULL src */
    ASSERT(col_rel_deep_copy(NULL, &out, NULL) == EINVAL,
        "NULL src not rejected");
    ASSERT(out == NULL, "out perturbed on NULL-src failure");

    /* NULL out */
    int rc = col_rel_alloc(&dummy, "src");
    ASSERT(rc == 0 && dummy != NULL, "col_rel_alloc failed");
    ASSERT(col_rel_deep_copy(dummy, NULL, NULL) == EINVAL,
        "NULL out not rejected");

    /* Both NULL */
    ASSERT(col_rel_deep_copy(NULL, NULL, NULL) == EINVAL,
        "double NULL not rejected");

    PASS();
cleanup:
    col_rel_destroy(dummy);
}

static void
test_empty_0x0_relation(void)
{
    TEST("deep-copy of an empty 0x0 relation succeeds and is destroyable");
    col_rel_t *src = NULL;
    col_rel_t *dst = NULL;
    int rc = col_rel_alloc(&src, "empty");
    ASSERT(rc == 0 && src != NULL, "col_rel_alloc src failed");

    rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0, "col_rel_deep_copy returned non-zero");
    ASSERT(dst != NULL, "dst not populated on success");
    ASSERT(dst != src, "dst aliased src pointer");
    ASSERT(dst->ncols == 0u, "ncols not zero on empty copy");
    ASSERT(dst->nrows == 0u, "nrows not zero on empty copy");
    ASSERT(dst->capacity == 0u, "capacity not zero on empty copy");
    ASSERT(dst->name != NULL && strcmp(dst->name, "empty") == 0,
        "name not deep-copied");
    ASSERT(dst->name != src->name, "name buffer aliased between src and dst");

    PASS();
cleanup:
    col_rel_destroy(src);
    col_rel_destroy(dst);
}

static void
test_design_invariants_zero_state(void)
{
    TEST("Group I/J zero-state invariants on a copy");
    col_rel_t *src = NULL;
    col_rel_t *dst = NULL;
    int rc = col_rel_alloc(&src, "src");
    ASSERT(rc == 0 && src != NULL, "col_rel_alloc src failed");

    /* Manually flip Group I bits on src; the copy must not inherit them. */
    src->pool_owned = true;
    src->arena_owned = true;
    /* mem_ledger left NULL -- we cannot construct a real ledger here, but
     * the contract is "always reset", so a NULL src->mem_ledger still
     * proves the dst path doesn't dereference it. */

    rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0 && dst != NULL, "deep-copy failed");

    /* Group I: ownership flags reset, ledger NULL. */
    ASSERT(dst->pool_owned == false, "pool_owned leaked from src");
    ASSERT(dst->arena_owned == false, "arena_owned leaked from src");
    ASSERT(dst->mem_ledger == NULL, "mem_ledger not reset");

    /* Group J: caches cleared. */
    ASSERT(dst->dedup_slots == NULL, "dedup_slots not reset");
    ASSERT(dst->dedup_cap == 0u, "dedup_cap not reset");
    ASSERT(dst->dedup_count == 0u, "dedup_count not reset");
    ASSERT(dst->col_shared == NULL, "col_shared not reset");
    ASSERT(dst->row_scratch == NULL, "row_scratch not reset");

    /* Restore src ownership flags so destroy paths behave correctly. */
    src->pool_owned = false;
    src->arena_owned = false;

    PASS();
cleanup:
    if (src) {
        src->pool_owned = false;
        src->arena_owned = false;
    }
    col_rel_destroy(src);
    col_rel_destroy(dst);
}

static void
test_graph_metadata_round_trip(void)
{
    TEST("Group H graph-column metadata round-trips");
    col_rel_t *src = NULL;
    col_rel_t *dst = NULL;
    int rc = col_rel_alloc(&src, "graph_src");
    ASSERT(rc == 0 && src != NULL, "col_rel_alloc src failed");

    src->has_graph_column = true;
    src->graph_col_idx = 7u;

    rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0 && dst != NULL, "deep-copy failed");
    ASSERT(dst->has_graph_column == true, "has_graph_column not copied");
    ASSERT(dst->graph_col_idx == 7u, "graph_col_idx not copied");

    /* Mutate src and confirm dst is independent. */
    src->graph_col_idx = 99u;
    ASSERT(dst->graph_col_idx == 7u, "graph_col_idx aliased through src");

    PASS();
cleanup:
    col_rel_destroy(src);
    col_rel_destroy(dst);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_col_rel_deep_copy (Issue #553)\n");
    printf("===================================\n");

    test_null_args_rejected();
    test_empty_0x0_relation();
    test_design_invariants_zero_state();
    test_graph_metadata_round_trip();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
