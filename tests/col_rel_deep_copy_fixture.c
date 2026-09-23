/*
 * col_rel_deep_copy_fixture.c - Reusable test fixtures for deep-copy validation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Issue #556: deep-copy validation framework.  See the header for the
 * contract documented per helper.
 */

#include "col_rel_deep_copy_fixture.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Maximum supported column count from this fixture.  Tests that need
* wider relations should extend or fork this helper rather than relax
* the limit -- the column-name buffer width is bounded for safety. */
#define DEEP_COPY_FIXTURE_MAX_COLS 1024u

/* Width of "col_NNN" + NUL.  Matches DEEP_COPY_FIXTURE_MAX_COLS digits. */
#define DEEP_COPY_FIXTURE_COL_NAME_BUF 16u

col_rel_t *
deep_copy_fixture_make_relation(const char *name, uint32_t nrows,
    uint32_t ncols)
{
    if (!name)
        return NULL;
    if (ncols > DEEP_COPY_FIXTURE_MAX_COLS)
        return NULL;

    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;

    if (ncols == 0u) {
        /* 0-col relation: no schema, no rows.  Mirrors the empty 0x0
         * relation already covered by the original test scaffolding. */
        return r;
    }

    /* Build a stable set of column names.  Buffers are owned by this
     * helper through col_rel_set_schema (which deep-copies them). */
    char name_storage[DEEP_COPY_FIXTURE_MAX_COLS][DEEP_COPY_FIXTURE_COL_NAME_BUF];
    const char *names[DEEP_COPY_FIXTURE_MAX_COLS];
    for (uint32_t c = 0; c < ncols; c++) {
        snprintf(name_storage[c], DEEP_COPY_FIXTURE_COL_NAME_BUF,
            "col_%u", c);
        names[c] = name_storage[c];
    }
    if (col_rel_set_schema(r, ncols, names) != 0) {
        col_rel_destroy(r);
        return NULL;
    }

    /* Append rows with deterministic content. */
    int64_t row[DEEP_COPY_FIXTURE_MAX_COLS];
    for (uint32_t i = 0; i < nrows; i++) {
        for (uint32_t c = 0; c < ncols; c++) {
            row[c] = (int64_t)((uint64_t)i * (uint64_t)ncols + (uint64_t)c);
        }
        if (col_rel_append_row(r, row) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

int
deep_copy_fixture_assert_design_invariants(const col_rel_t *dst)
{
    if (!dst) {
        fprintf(stderr, "FIXTURE: dst is NULL\n");
        return 0;
    }
    if (dst->pool_owned != false) {
        fprintf(stderr, "FIXTURE: pool_owned should be false\n");
        return 0;
    }
    if (dst->arena_owned != false) {
        fprintf(stderr, "FIXTURE: arena_owned should be false\n");
        return 0;
    }
    if (dst->mem_ledger != NULL) {
        fprintf(stderr, "FIXTURE: mem_ledger should be NULL\n");
        return 0;
    }
    if (dst->col_shared != NULL) {
        fprintf(stderr, "FIXTURE: col_shared should be NULL\n");
        return 0;
    }
    if (dst->dedup_slots != NULL) {
        fprintf(stderr, "FIXTURE: dedup_slots should be NULL\n");
        return 0;
    }
    if (dst->dedup_cap != 0u) {
        fprintf(stderr, "FIXTURE: dedup_cap should be 0\n");
        return 0;
    }
    if (dst->dedup_count != 0u) {
        fprintf(stderr, "FIXTURE: dedup_count should be 0\n");
        return 0;
    }
    if (dst->row_scratch != NULL) {
        fprintf(stderr, "FIXTURE: row_scratch should be NULL\n");
        return 0;
    }
    return 1;
}

int
deep_copy_fixture_assert_relations_equal(const col_rel_t *a,
    const col_rel_t *b)
{
    if (!a || !b) {
        fprintf(stderr, "FIXTURE: NULL relation passed to equal-check\n");
        return 0;
    }
    if (a == b) {
        fprintf(stderr, "FIXTURE: aliased relation pointers (a == b)\n");
        return 0;
    }

    /* Scalar metadata. */
    if (a->ncols != b->ncols) {
        fprintf(stderr, "FIXTURE: ncols mismatch (%u vs %u)\n",
            a->ncols, b->ncols);
        return 0;
    }
    if (a->nrows != b->nrows) {
        fprintf(stderr, "FIXTURE: nrows mismatch (%u vs %u)\n",
            a->nrows, b->nrows);
        return 0;
    }
    if (a->capacity != b->capacity) {
        fprintf(stderr, "FIXTURE: capacity mismatch (%u vs %u)\n",
            a->capacity, b->capacity);
        return 0;
    }
    if (a->sorted_nrows != b->sorted_nrows) {
        fprintf(stderr, "FIXTURE: sorted_nrows mismatch (%u vs %u)\n",
            a->sorted_nrows, b->sorted_nrows);
        return 0;
    }
    if (a->base_nrows != b->base_nrows) {
        fprintf(stderr, "FIXTURE: base_nrows mismatch (%u vs %u)\n",
            a->base_nrows, b->base_nrows);
        return 0;
    }

    /* Name string. */
    if ((a->name == NULL) != (b->name == NULL)) {
        fprintf(stderr, "FIXTURE: name NULL-ness mismatch\n");
        return 0;
    }
    if (a->name && b->name) {
        if (a->name == b->name) {
            fprintf(stderr, "FIXTURE: name pointer aliased\n");
            return 0;
        }
        if (strcmp(a->name, b->name) != 0) {
            fprintf(stderr, "FIXTURE: name mismatch (%s vs %s)\n",
                a->name, b->name);
            return 0;
        }
    }

    /* Column-major data. */
    if (a->ncols > 0u) {
        if (a->columns == NULL || b->columns == NULL) {
            fprintf(stderr, "FIXTURE: columns array NULL with ncols>0\n");
            return 0;
        }
        if (a->columns == b->columns) {
            fprintf(stderr, "FIXTURE: columns array pointer aliased\n");
            return 0;
        }
        for (uint32_t c = 0; c < a->ncols; c++) {
            if (a->columns[c] == NULL || b->columns[c] == NULL) {
                fprintf(stderr,
                    "FIXTURE: column %u buffer NULL\n", c);
                return 0;
            }
            if (a->columns[c] == b->columns[c]) {
                fprintf(stderr,
                    "FIXTURE: column %u buffer aliased\n", c);
                return 0;
            }
            for (uint32_t r0 = 0; r0 < a->nrows; r0++) {
                if (a->columns[c][r0] != b->columns[c][r0]) {
                    fprintf(stderr,
                        "FIXTURE: cell (col=%u row=%u) mismatch "
                        "(%lld vs %lld)\n",
                        c, r0,
                        (long long)a->columns[c][r0],
                        (long long)b->columns[c][r0]);
                    return 0;
                }
            }
        }
    }

    /* Column names. */
    if ((a->col_names == NULL) != (b->col_names == NULL)) {
        fprintf(stderr, "FIXTURE: col_names NULL-ness mismatch\n");
        return 0;
    }
    if (a->col_names && b->col_names) {
        if (a->col_names == b->col_names) {
            fprintf(stderr, "FIXTURE: col_names array aliased\n");
            return 0;
        }
        for (uint32_t c = 0; c < a->ncols; c++) {
            if (a->col_names[c] == NULL || b->col_names[c] == NULL) {
                fprintf(stderr,
                    "FIXTURE: col_names[%u] NULL\n", c);
                return 0;
            }
            if (a->col_names[c] == b->col_names[c]) {
                fprintf(stderr,
                    "FIXTURE: col_names[%u] pointer aliased\n", c);
                return 0;
            }
            if (strcmp(a->col_names[c], b->col_names[c]) != 0) {
                fprintf(stderr,
                    "FIXTURE: col_names[%u] mismatch (%s vs %s)\n",
                    c, a->col_names[c], b->col_names[c]);
                return 0;
            }
        }
    }

    /* Compound metadata. */
    if (a->compound_kind != b->compound_kind) {
        fprintf(stderr, "FIXTURE: compound_kind mismatch\n");
        return 0;
    }
    if (a->compound_count != b->compound_count) {
        fprintf(stderr, "FIXTURE: compound_count mismatch\n");
        return 0;
    }
    if (a->compound_arity_len != b->compound_arity_len) {
        fprintf(stderr, "FIXTURE: compound_arity_len mismatch\n");
        return 0;
    }
    if ((a->compound_arity_map == NULL)
        != (b->compound_arity_map == NULL)) {
        fprintf(stderr,
            "FIXTURE: compound_arity_map NULL-ness mismatch\n");
        return 0;
    }
    if (a->compound_arity_map && b->compound_arity_map) {
        if (a->compound_arity_map == b->compound_arity_map) {
            fprintf(stderr,
                "FIXTURE: compound_arity_map aliased\n");
            return 0;
        }
        for (uint32_t i = 0; i < a->compound_arity_len; i++) {
            if (a->compound_arity_map[i] != b->compound_arity_map[i]) {
                fprintf(stderr,
                    "FIXTURE: compound_arity_map[%u] mismatch\n", i);
                return 0;
            }
        }
    }

    /* Graph-column metadata. */
    if (a->has_graph_column != b->has_graph_column) {
        fprintf(stderr, "FIXTURE: has_graph_column mismatch\n");
        return 0;
    }
    if (a->graph_col_idx != b->graph_col_idx) {
        fprintf(stderr, "FIXTURE: graph_col_idx mismatch\n");
        return 0;
    }

    /* Run tracking. */
    if (a->run_count != b->run_count) {
        fprintf(stderr, "FIXTURE: run_count mismatch\n");
        return 0;
    }
    for (uint32_t i = 0; i < COL_MAX_RUNS; i++) {
        if (a->run_ends[i] != b->run_ends[i]) {
            fprintf(stderr, "FIXTURE: run_ends[%u] mismatch\n", i);
            return 0;
        }
    }

    /* Schema flag. */
    if (a->schema_ok != b->schema_ok) {
        fprintf(stderr, "FIXTURE: schema_ok mismatch\n");
        return 0;
    }

    return 1;
}
