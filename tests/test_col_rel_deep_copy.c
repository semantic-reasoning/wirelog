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
/* Group A + Group B: columns, schema, col_names (Issue #553 Commit 2)      */
/* ======================================================================== */

/* Helper: build a 3-col, N-row relation with deterministic content. */
static col_rel_t *
build_populated(const char *name, uint32_t nrows)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    const char *names[3] = { "alpha", "beta", "gamma" };
    if (col_rel_set_schema(r, 3u, names) != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    for (uint32_t i = 0; i < nrows; i++) {
        int64_t row[3] = {
            (int64_t)i,
            (int64_t)(i * 10),
            (int64_t)(i * 100),
        };
        if (col_rel_append_row(r, row) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

static void
test_columns_independent_after_modify(void)
{
    TEST("Group A: column buffers are independent after mutation");
    col_rel_t *src = build_populated("src", 5u);
    col_rel_t *dst = NULL;
    ASSERT(src != NULL, "build_populated failed");

    int rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0 && dst != NULL, "deep-copy failed");
    ASSERT(dst->columns != src->columns,
        "columns array pointer aliased between src and dst");
    for (uint32_t c = 0; c < src->ncols; c++) {
        ASSERT(dst->columns[c] != src->columns[c],
            "per-column buffer aliased");
    }

    /* Mutate every cell on dst and verify src is unchanged. */
    for (uint32_t r0 = 0; r0 < dst->nrows; r0++) {
        for (uint32_t c = 0; c < dst->ncols; c++) {
            col_rel_set(dst, r0, c, (int64_t)-1);
        }
    }
    for (uint32_t r0 = 0; r0 < src->nrows; r0++) {
        ASSERT(col_rel_get(src, r0, 0) == (int64_t)r0,
            "src col 0 perturbed by dst mutation");
        ASSERT(col_rel_get(src, r0, 1) == (int64_t)(r0 * 10),
            "src col 1 perturbed by dst mutation");
        ASSERT(col_rel_get(src, r0, 2) == (int64_t)(r0 * 100),
            "src col 2 perturbed by dst mutation");
    }

    PASS();
cleanup:
    col_rel_destroy(src);
    col_rel_destroy(dst);
}

static void
test_col_names_independent(void)
{
    TEST("Group B: col_names strings are independent");
    col_rel_t *src = build_populated("src", 1u);
    col_rel_t *dst = NULL;
    ASSERT(src != NULL, "build_populated failed");

    int rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0 && dst != NULL, "deep-copy failed");
    ASSERT(dst->col_names != NULL, "dst col_names not allocated");
    ASSERT(dst->col_names != src->col_names,
        "col_names array pointer aliased");
    for (uint32_t i = 0; i < src->ncols; i++) {
        ASSERT(dst->col_names[i] != src->col_names[i],
            "per-name buffer aliased");
        ASSERT(strcmp(dst->col_names[i], src->col_names[i]) == 0,
            "col_names content mismatch");
    }

    PASS();
cleanup:
    col_rel_destroy(src);
    col_rel_destroy(dst);
}

static void
test_schema_round_trip(void)
{
    TEST("Group B: ArrowSchema is independently reinit'd");
    col_rel_t *src = build_populated("src", 1u);
    col_rel_t *dst = NULL;
    ASSERT(src != NULL, "build_populated failed");
    ASSERT(src->schema_ok == true, "src schema_ok unexpectedly false");

    int rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0 && dst != NULL, "deep-copy failed");
    ASSERT(dst->schema_ok == true, "dst schema_ok not set");
    ASSERT(dst->schema.release != NULL, "dst schema release callback NULL");
    /* The format string ("+s" for struct) is independently allocated by
     * ArrowSchemaSetTypeStruct -- if it were aliased, release of either
     * schema would corrupt the other.  This is the cheapest single-check
     * proof that the schemas are not bit-copies. */
    ASSERT(dst->schema.format != src->schema.format,
        "ArrowSchema format buffer aliased between src and dst");
    ASSERT(dst->schema.children != src->schema.children,
        "ArrowSchema children array aliased between src and dst");
    ASSERT(dst->schema.n_children == src->schema.n_children,
        "n_children mismatch");
    for (int64_t i = 0; i < src->schema.n_children; i++) {
        ASSERT(dst->schema.children[i] != src->schema.children[i],
            "per-child schema aliased");
        ASSERT(dst->schema.children[i]->name != NULL,
            "per-child name not set");
        ASSERT(strcmp(dst->schema.children[i]->name,
            src->schema.children[i]->name) == 0,
            "per-child schema name mismatch");
    }

    PASS();
cleanup:
    col_rel_destroy(src);
    col_rel_destroy(dst);
}

static void
test_destroy_source_copy_still_valid(void)
{
    TEST("destroying src after deep-copy leaves dst fully usable");
    col_rel_t *src = build_populated("src", 4u);
    col_rel_t *dst = NULL;
    ASSERT(src != NULL, "build_populated failed");

    int rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0 && dst != NULL, "deep-copy failed");

    /* Destroy src first so any aliased pointer would dangle. */
    col_rel_destroy(src);
    src = NULL;

    /* Confirm dst still reports correct content. */
    ASSERT(dst->ncols == 3u, "dst ncols corrupted post-src-destroy");
    ASSERT(dst->nrows == 4u, "dst nrows corrupted post-src-destroy");
    for (uint32_t r0 = 0; r0 < dst->nrows; r0++) {
        ASSERT(col_rel_get(dst, r0, 0) == (int64_t)r0,
            "dst col 0 corrupted post-src-destroy");
        ASSERT(col_rel_get(dst, r0, 1) == (int64_t)(r0 * 10),
            "dst col 1 corrupted post-src-destroy");
        ASSERT(col_rel_get(dst, r0, 2) == (int64_t)(r0 * 100),
            "dst col 2 corrupted post-src-destroy");
    }
    ASSERT(strcmp(dst->name, "src") == 0,
        "dst name corrupted post-src-destroy");

    PASS();
cleanup:
    col_rel_destroy(src);
    col_rel_destroy(dst);
}

/* ======================================================================== */
/* Group C: timestamps (Issue #553 Commit 3)                                */
/* ======================================================================== */

static void
test_timestamps_round_trip(void)
{
    TEST("Group C: timestamps array is deep-copied (capacity-sized)");
    col_rel_t *src = build_populated("ts_src", 3u);
    col_rel_t *dst = NULL;
    ASSERT(src != NULL, "build_populated failed");

    /* Manually attach a timestamps buffer matching src->capacity.  In
     * production col_eval_stratum / append-with-ts paths populate this;
     * here we wire it up directly to keep the test self-contained. */
    src->timestamps = (col_delta_timestamp_t *)calloc(src->capacity,
            sizeof(col_delta_timestamp_t));
    ASSERT(src->timestamps != NULL, "calloc src timestamps failed");
    for (uint32_t i = 0; i < src->nrows; i++) {
        src->timestamps[i].iteration = i + 1u;
        src->timestamps[i].stratum = 2u;
        src->timestamps[i].worker = 0u;
        src->timestamps[i].multiplicity = 1;
    }

    int rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0 && dst != NULL, "deep-copy failed");
    ASSERT(dst->timestamps != NULL, "dst timestamps not allocated");
    ASSERT(dst->timestamps != src->timestamps,
        "timestamps buffer aliased between src and dst");
    for (uint32_t i = 0; i < src->nrows; i++) {
        ASSERT(dst->timestamps[i].iteration == src->timestamps[i].iteration,
            "iteration mismatch");
        ASSERT(dst->timestamps[i].stratum == src->timestamps[i].stratum,
            "stratum mismatch");
        ASSERT(dst->timestamps[i].multiplicity
            == src->timestamps[i].multiplicity,
            "multiplicity mismatch");
    }

    /* Mutate src timestamps and confirm dst stays unchanged. */
    src->timestamps[0].iteration = 9999u;
    ASSERT(dst->timestamps[0].iteration == 1u,
        "dst timestamps perturbed by src mutation");

    PASS();
cleanup:
    col_rel_destroy(src);
    col_rel_destroy(dst);
}

static void
test_timestamps_null_when_src_null(void)
{
    TEST("Group C: NULL src timestamps -> NULL dst timestamps");
    col_rel_t *src = build_populated("no_ts_src", 2u);
    col_rel_t *dst = NULL;
    ASSERT(src != NULL, "build_populated failed");
    ASSERT(src->timestamps == NULL,
        "src timestamps unexpectedly non-NULL by default");

    int rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0 && dst != NULL, "deep-copy failed");
    ASSERT(dst->timestamps == NULL,
        "dst timestamps allocated despite src being NULL");

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
    test_columns_independent_after_modify();
    test_col_names_independent();
    test_schema_round_trip();
    test_destroy_source_copy_still_valid();
    test_timestamps_round_trip();
    test_timestamps_null_when_src_null();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
