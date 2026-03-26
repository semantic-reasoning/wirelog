/*
 * columnar/relation.c - wirelog Columnar Relation Storage
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Relation lifecycle, schema, and row-append operations.
 * Extracted from backend/columnar_nanoarrow.c.
 */

#include "columnar/internal.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- lifecycle ---------------------------------------------------------- */

void
col_rel_free_contents(col_rel_t *r)
{
    if (!r)
        return;
    /* Report data buffer deallocation to ledger before memset zeroes fields */
    if (r->mem_ledger && r->columns && r->capacity > 0 && r->ncols > 0)
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_RELATION,
            (uint64_t)r->capacity * r->ncols * sizeof(int64_t));
    free(r->name);
    if (!r->arena_owned) {
        if (r->col_shared && r->columns) {
            /* Free only non-shared columns (6B zero-copy sharing) */
            for (uint32_t c = 0; c < r->ncols; c++) {
                if (!r->col_shared[c])
                    free(r->columns[c]);
            }
            free(r->columns);
        } else {
            col_columns_free(r->columns, r->ncols);
        }
    } else {
        /* Arena owns column buffers; only free the columns array itself */
        free(r->columns);
    }
    free(r->col_shared);
    col_columns_free(r->retract_backup_columns, r->ncols);
    col_columns_free(r->merge_columns, r->ncols);
    free(r->row_scratch);
    free(r->timestamps);
    if (r->col_names) {
        for (uint32_t i = 0; i < r->ncols; i++)
            free(r->col_names[i]);
        free(r->col_names);
    }
    if (r->schema_ok)
        ArrowSchemaRelease(&r->schema);
    memset(r, 0, sizeof(*r));
}

/*
 * col_rel_destroy:
 * Free contents and, if heap-allocated (pool_owned == false), the struct
 * itself.  Pool-owned structs have their memory reclaimed on pool_reset();
 * calling free() on them would corrupt the pool allocator.
 */
void
col_rel_destroy(col_rel_t *r)
{
    if (!r)
        return;
    bool from_pool = r->pool_owned;
    col_rel_free_contents(r); /* memset zeroes pool_owned */
    if (!from_pool)
        free(r);
    /* If pool_owned: struct memory freed on pool_reset(), skip free(). */
}

/*
 * col_rel_set_schema:
 * Initialise ncols, col_names[], data buffer, and ArrowSchema.
 * Called lazily on first insert (EDB) or when relation is first produced.
 * Returns 0 on success, ENOMEM/EINVAL on failure.
 */
int
col_rel_set_schema(col_rel_t *r, uint32_t ncols, const char *const *col_names)
{
    if (r->ncols != 0)
        return 0; /* already initialised */

    r->ncols = ncols;

    if (ncols > 0) {
        r->capacity = COL_REL_INIT_CAP;
        r->columns = col_columns_alloc(ncols, r->capacity);
        if (!r->columns)
            return ENOMEM;

        r->col_names = (char **)calloc(ncols, sizeof(char *));
        if (!r->col_names) {
            col_columns_free(r->columns, ncols);
            r->columns = NULL;
            return ENOMEM;
        }
        for (uint32_t i = 0; i < ncols; i++) {
            if (col_names && col_names[i]) {
                r->col_names[i] = wl_strdup(col_names[i]);
            } else {
                char buf[32];
                snprintf(buf, sizeof(buf), "col%u", i);
                r->col_names[i] = wl_strdup(buf);
            }
            if (!r->col_names[i]) {
                for (uint32_t j = 0; j < i; j++)
                    free(r->col_names[j]);
                free(r->col_names);
                col_columns_free(r->columns, ncols);
                r->col_names = NULL;
                r->columns = NULL;
                return ENOMEM;
            }
        }
    }

    /* Arrow schema: struct<col0:i64, col1:i64, ...> */
    ArrowSchemaInit(&r->schema);
    if (ArrowSchemaSetTypeStruct(&r->schema, (int64_t)ncols) != NANOARROW_OK) {
        /* cleanup names/data done by caller via col_rel_free_contents */
        return EINVAL;
    }
    for (uint32_t i = 0; i < ncols; i++) {
        if (ArrowSchemaInitFromType(r->schema.children[i], NANOARROW_TYPE_INT64)
            != NANOARROW_OK) {
            ArrowSchemaRelease(&r->schema);
            return EINVAL;
        }
        const char *cname
            = (r->col_names && r->col_names[i]) ? r->col_names[i] : "";
        ArrowSchemaSetName(r->schema.children[i], cname);
    }
    r->schema_ok = true;
    return 0;
}

int
col_rel_alloc(col_rel_t **out, const char *name)
{
    col_rel_t *r = (col_rel_t *)calloc(1, sizeof(col_rel_t));
    if (!r)
        return ENOMEM;
    r->name = wl_strdup(name);
    if (!r->name) {
        free(r);
        return ENOMEM;
    }
    r->pool_owned = false;
    *out = r;
    return 0;
}

int
col_rel_append_row(col_rel_t *r, const int64_t *row)
{
    if (r->nrows >= r->capacity) {
        uint32_t new_cap = r->capacity ? r->capacity * 2 : COL_REL_INIT_CAP;
        if (new_cap <= r->capacity) /* overflow guard */
            return ENOMEM;
        /* Grow timestamps first (if tracking) so we can roll back cleanly
         * on a subsequent data realloc failure. */
        if (r->timestamps) {
            col_delta_timestamp_t *new_ts = (col_delta_timestamp_t *)realloc(
                r->timestamps, new_cap * sizeof(col_delta_timestamp_t));
            if (!new_ts)
                return ENOMEM;
            r->timestamps = new_ts;
        }
        if (r->arena_owned) {
            /* Arena data cannot be realloc'd; migrate to heap */
            int64_t **new_cols = col_columns_alloc(r->ncols, new_cap);
            if (!new_cols)
                return ENOMEM;
            for (uint32_t c = 0; c < r->ncols; c++)
                memcpy(new_cols[c], r->columns[c],
                    sizeof(int64_t) * r->nrows);
            /* Don't free arena columns; just free the columns array */
            free(r->columns);
            r->columns = new_cols;
            r->arena_owned = false;
        } else if (r->columns) {
            if (col_columns_realloc(r->columns, r->ncols, new_cap) != 0)
                return ENOMEM;
        } else {
            r->columns = col_columns_alloc(r->ncols, new_cap);
            if (!r->columns)
                return ENOMEM;
        }
        /* Track capacity growth in ledger (Issue #224): only the delta bytes
        * added by this growth event.  r->capacity is still the old value. */
        if (r->mem_ledger && r->ncols > 0) {
            uint64_t delta = (uint64_t)(new_cap - r->capacity) * r->ncols
                * sizeof(int64_t);
            wl_mem_ledger_alloc(r->mem_ledger, WL_MEM_SUBSYS_RELATION, delta);
        }
        r->capacity = new_cap;
    }
    if (r->timestamps)
        memset(&r->timestamps[r->nrows], 0, sizeof(col_delta_timestamp_t));
    col_rel_row_copy_in(r, r->nrows, row);
    r->nrows++;
    return 0;
}

/* Copy all rows from src into dst (must have same ncols).
 * If src has timestamps and dst has timestamp tracking enabled, the source
 * timestamps are propagated to the newly appended rows.
 * Optimized (Issue #300): bulk memcpy instead of per-row append. */
int
col_rel_append_all(col_rel_t *dst, const col_rel_t *src, wl_arena_t *arena)
{
    if (src->nrows == 0)
        return 0;

    uint32_t dst_base = dst->nrows;
    uint32_t new_nrows = dst->nrows + src->nrows;

    /* Ensure dst has sufficient capacity */
    if (new_nrows > dst->capacity) {
        uint32_t new_cap = dst->capacity ? dst->capacity * 2 : COL_REL_INIT_CAP;
        while (new_cap < new_nrows) {
            uint32_t next_cap = new_cap * 2;
            if (next_cap <= new_cap) /* overflow guard */
                return ENOMEM;
            new_cap = next_cap;
        }

        /* Grow timestamps first (if tracking) */
        if (dst->timestamps) {
            col_delta_timestamp_t *new_ts = (col_delta_timestamp_t *)realloc(
                dst->timestamps, new_cap * sizeof(col_delta_timestamp_t));
            if (!new_ts)
                return ENOMEM;
            dst->timestamps = new_ts;
        }

        if (dst->arena_owned) {
            /* Arena data: allocate from same arena, preserve arena_owned flag */
            if (arena) {
                int64_t **new_cols
                    = (int64_t **)calloc(dst->ncols, sizeof(int64_t *));
                if (!new_cols)
                    return ENOMEM;
                bool ok = true;
                for (uint32_t c = 0; c < dst->ncols; c++) {
                    new_cols[c] = (int64_t *)wl_arena_alloc(arena,
                            (size_t)new_cap * sizeof(int64_t));
                    if (!new_cols[c]) {
                        ok = false;
                        break;
                    }
                    memcpy(new_cols[c], dst->columns[c],
                        sizeof(int64_t) * dst->nrows);
                }
                if (!ok) {
                    /* Arena alloc failed; don't free arena columns */
                    free(new_cols);
                    return ENOMEM;
                }
                free(dst->columns); /* free old columns array only */
                dst->columns = new_cols;
            } else {
                /* Fallback to heap if arena unavailable */
                int64_t **new_cols
                    = col_columns_alloc(dst->ncols, new_cap);
                if (!new_cols)
                    return ENOMEM;
                for (uint32_t c = 0; c < dst->ncols; c++)
                    memcpy(new_cols[c], dst->columns[c],
                        sizeof(int64_t) * dst->nrows);
                free(dst->columns);
                dst->columns = new_cols;
                dst->arena_owned = false;
            }
        } else if (dst->columns) {
            if (col_columns_realloc(dst->columns, dst->ncols, new_cap) != 0)
                return ENOMEM;
        } else {
            dst->columns = col_columns_alloc(dst->ncols, new_cap);
            if (!dst->columns)
                return ENOMEM;
        }

        /* Track capacity growth in ledger */
        if (dst->mem_ledger && dst->ncols > 0) {
            uint64_t delta = (uint64_t)(new_cap - dst->capacity) * dst->ncols
                * sizeof(int64_t);
            wl_mem_ledger_alloc(dst->mem_ledger, WL_MEM_SUBSYS_RELATION, delta);
        }
        dst->capacity = new_cap;
    }

    /* Bulk copy all rows per-column */
    for (uint32_t c = 0; c < dst->ncols; c++)
        memcpy(dst->columns[c] + dst_base, src->columns[c],
            (size_t)src->nrows * sizeof(int64_t));
    dst->nrows = new_nrows;

    /* Copy timestamps if both have tracking enabled */
    if (src->timestamps && dst->timestamps)
        memcpy(&dst->timestamps[dst_base], src->timestamps,
            src->nrows * sizeof(col_delta_timestamp_t));

    return 0;
}

/* ---- compaction ---------------------------------------------------------- */

/*
 * col_rel_compact:
 * Shrink oversized data and timestamps buffers after bulk retraction.
 *
 * Guards:
 *   - NULL or ncols==0: no-op
 *   - nrows==0: release data, merge_buf, timestamps; zero capacities
 *   - capacity <= nrows*4: already tight enough; skip
 *
 * On success the capacity is reduced to max(nrows*2, COL_REL_INIT_CAP).
 * merge_buf is always freed (it will be re-allocated on next consolidation).
 * Allocation failures are non-fatal: the relation remains valid.
 * sorted_nrows is clamped to nrows if it drifted above.
 */
void
col_rel_compact(col_rel_t *r)
{
    if (!r || r->ncols == 0)
        return;

    if (r->nrows == 0) {
        if (!r->arena_owned)
            col_columns_free(r->columns, r->ncols);
        else
            free(r->columns);
        r->columns = NULL;
        r->capacity = 0;
        r->arena_owned = false;
        col_columns_free(r->merge_columns, r->ncols);
        r->merge_columns = NULL;
        r->merge_buf_cap = 0;
        free(r->timestamps);
        r->timestamps = NULL;
        r->sorted_nrows = 0;
        r->base_nrows = 0;
        return;
    }

    /* Only compact when buffer is more than 4x oversized.
     * Cast to uint64_t to prevent overflow when nrows > UINT32_MAX/4. */
    if (r->capacity <= (uint64_t)r->nrows * 4)
        goto free_merge_buf;

    {
        uint32_t tight = r->nrows * 2;
        if (tight < r->nrows) /* overflow guard */
            tight = UINT32_MAX;
        if (tight < COL_REL_INIT_CAP)
            tight = COL_REL_INIT_CAP;

        if (r->arena_owned) {
            /* Arena data: allocate new heap columns and copy */
            int64_t **new_cols = col_columns_alloc(r->ncols, tight);
            if (!new_cols)
                goto free_merge_buf;
            for (uint32_t c = 0; c < r->ncols; c++)
                memcpy(new_cols[c], r->columns[c],
                    (size_t)r->nrows * sizeof(int64_t));
            free(r->columns);
            r->columns = new_cols;
            r->arena_owned = false;
        } else {
            if (col_columns_realloc(r->columns, r->ncols, tight) != 0)
                goto free_merge_buf;
        }
        r->capacity = tight;

        /* Shrink timestamps to match new capacity (non-fatal on failure). */
        if (r->timestamps) {
            col_delta_timestamp_t *nt = (col_delta_timestamp_t *)realloc(
                r->timestamps, (size_t)tight * sizeof(col_delta_timestamp_t));
            if (nt)
                r->timestamps = nt;
            /* failure: timestamps oversized but valid (accessed up to nrows) */
        }
    }

free_merge_buf:
    col_columns_free(r->merge_columns, r->ncols);
    r->merge_columns = NULL;
    r->merge_buf_cap = 0;

    if (r->sorted_nrows > r->nrows)
        r->sorted_nrows = r->nrows;
    if (r->base_nrows > r->nrows)
        r->base_nrows = r->nrows;
}

/* ---- column name lookup ------------------------------------------------- */

int
col_rel_col_idx(const col_rel_t *r, const char *name)
{
    if (!r->col_names || !name)
        return -1;
    for (uint32_t i = 0; i < r->ncols; i++) {
        if (r->col_names[i] && strcmp(r->col_names[i], name) == 0)
            return (int)i;
    }
    /* fallback: "col<N>" convention */
    if (name[0] == 'c' && name[1] == 'o' && name[2] == 'l') {
        char *end;
        long v = strtol(name + 3, &end, 10);
        if (*end == '\0' && v >= 0 && (uint32_t)v < r->ncols)
            return (int)v;
    }
    return -1;
}

/* ---- convenience constructors ------------------------------------------- */

/* Helper: create a new owned relation with given ncols and auto-named cols. */
col_rel_t *
col_rel_new_auto(const char *name, uint32_t ncols)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    if (col_rel_set_schema(r, ncols, NULL) != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    return r;
}

/* Helper: create owned relation copying col_names from src. */
col_rel_t *
col_rel_new_like(const char *name, const col_rel_t *src)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    if (col_rel_set_schema(r, src->ncols, (const char *const *)src->col_names)
        != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    return r;
}

/* Pool-aware col_rel constructor wrappers.
 *
 * The struct slot is allocated from the pool slab (O(1), no free needed).
 * Data buffers and col_names are still heap-allocated so that realloc in
 * col_rel_append_row remains safe and col_names are available for column
 * lookup (col_rel_col_idx).  The pool_owned flag tells col_rel_destroy to
 * skip free() on the struct itself while still freeing heap-allocated
 * internals. */
col_rel_t *
col_rel_pool_new_like(delta_pool_t *pool, const char *name,
    const col_rel_t *like)
{
    if (!pool)
        return col_rel_new_like(name, like); /* Fallback to malloc */
    col_rel_t *r = (col_rel_t *)delta_pool_alloc_slot(pool);
    if (!r)
        return col_rel_new_like(name, like); /* Pool exhausted, fallback */
    r->pool_owned = true;
    r->name = wl_strdup(name);
    if (!r->name) {
        memset(r, 0, sizeof(*r));
        return col_rel_new_like(name, like);
    }
    r->ncols = like->ncols;
    r->capacity = COL_REL_INIT_CAP;
    if (like->ncols > 0) {
        r->columns = col_columns_alloc(like->ncols, r->capacity);
        if (!r->columns) {
            free(r->name);
            memset(r, 0, sizeof(*r));
            return col_rel_new_like(name, like); /* Fallback */
        }
    }
    /* Copy col_names so col_rel_col_idx works for downstream operators */
    if (like->col_names && like->ncols > 0) {
        r->col_names = (char **)calloc(like->ncols, sizeof(char *));
        if (r->col_names) {
            for (uint32_t i = 0; i < like->ncols; i++) {
                if (like->col_names[i])
                    r->col_names[i] = wl_strdup(like->col_names[i]);
            }
        }
    }
    r->nrows = 0;
    return r;
}

col_rel_t *
col_rel_pool_new_auto(delta_pool_t *pool, wl_arena_t *arena,
    const char *name, uint32_t ncols)
{
    if (!pool)
        return col_rel_new_auto(name, ncols); /* Fallback */
    col_rel_t *r = (col_rel_t *)delta_pool_alloc_slot(pool);
    if (!r)
        return col_rel_new_auto(name, ncols); /* Pool exhausted */
    r->pool_owned = true;
    r->name = wl_strdup(name);
    if (!r->name) {
        memset(r, 0, sizeof(*r));
        return col_rel_new_auto(name, ncols);
    }
    r->ncols = ncols;
    r->capacity = COL_REL_INIT_CAP;
    if (ncols > 0) {
        /* Try arena allocation first; fall back to malloc.
         * The columns array itself is always heap-allocated.
         * Individual column buffers may be from arena. */
        bool use_arena = false;
        if (arena) {
            r->columns = (int64_t **)calloc(ncols, sizeof(int64_t *));
            if (r->columns) {
                bool ok = true;
                for (uint32_t c = 0; c < ncols; c++) {
                    r->columns[c] = (int64_t *)wl_arena_alloc(arena,
                            (size_t)r->capacity * sizeof(int64_t));
                    if (!r->columns[c]) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    use_arena = true;
                    r->arena_owned = true;
                } else {
                    /* Arena alloc failed for some columns; free and retry */
                    free(r->columns);
                    r->columns = NULL;
                }
            }
        }
        if (!use_arena) {
            r->columns = col_columns_alloc(ncols, r->capacity);
            r->arena_owned = false;
        }
        if (!r->columns) {
            free(r->name);
            memset(r, 0, sizeof(*r));
            return col_rel_new_auto(name, ncols); /* Fallback */
        }
        /* Auto-generate col_names (col0, col1, ...) matching set_schema */
        r->col_names = (char **)calloc(ncols, sizeof(char *));
        if (r->col_names) {
            for (uint32_t i = 0; i < ncols; i++) {
                char buf[32];
                snprintf(buf, sizeof(buf), "col%u", i);
                r->col_names[i] = wl_strdup(buf);
            }
        }
    }
    r->nrows = 0;
    return r;
}

/* ---- radix sort ---------------------------------------------------------- */

/*
 * col_radix_sort_rows: sort nrows rows of ncols int64_t columns in-place
 * using LSD radix sort.  Works on a raw pointer (no col_rel_t needed).
 *
 * Falls back to qsort_r on allocation failure.
 * Returns 0 on success, -1 on fallback (still sorted, just via qsort).
 */
int
col_radix_sort_rows(int64_t *data, uint32_t nrows, uint32_t ncols)
{
    if (!data || ncols == 0 || nrows <= 1)
        return 0;

    uint32_t nr = nrows;
    uint32_t nc = ncols;
    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    int64_t *tmp = (int64_t *)malloc((size_t)nr * row_bytes);
    if (!tmp) {
        /* Fallback to comparison sort on allocation failure */
        QSORT_R_CALL(data, nr, row_bytes, &nc, row_cmp_fn);
        return -1;
    }

    int64_t *src = data;
    int64_t *dst = tmp;

    /* count[b] = frequency of byte value b in current pass.
     * prefix[b] = output index for next element with byte value b. */
    uint32_t count[256];
    uint32_t prefix[256];

    /*
     * LSD radix sort: process from least significant sort key to most
     * significant.  Column nc-1 is least significant, column 0 is most
     * significant.  Within each column, byte 0 (LSB) is processed first
     * and byte 7 (MSB) last.  The sign bit (bit 7 of byte 7) is flipped
     * so that unsigned bucket ordering matches signed int64 ordering.
     */
    for (int c = (int)nc - 1; c >= 0; c--) {
        for (int b = 0; b < 8; b++) {
            int shift = b * 8;
            int is_sign_byte = (b == 7);

            memset(count, 0, sizeof(count));
            for (uint32_t row = 0; row < nr; row++) {
                uint8_t bv = (uint8_t)((uint64_t)src[(size_t)row * nc + c] >>
                    shift);
                if (is_sign_byte)
                    bv ^= 0x80u;
                count[bv]++;
            }

            prefix[0] = 0;
            for (int i = 1; i < 256; i++)
                prefix[i] = prefix[i - 1] + count[i - 1];

            for (uint32_t row = 0; row < nr; row++) {
                uint8_t bv = (uint8_t)((uint64_t)src[(size_t)row * nc + c] >>
                    shift);
                if (is_sign_byte)
                    bv ^= 0x80u;
                uint32_t out_pos = prefix[bv]++;
                memcpy(dst + (size_t)out_pos * nc,
                    src + (size_t)row * nc,
                    row_bytes);
            }

            /* Swap buffers: output of this pass is input of next */
            int64_t *t = src;
            src = dst;
            dst = t;
        }
    }

    /* After nc*8 passes the result is in src.  Copy back if needed. */
    if (src != data)
        memcpy(data, src, (size_t)nr * row_bytes);

    free(tmp);
    return 0;
}

/*
 * col_rel_insertion_sort: insertion sort for small segments (Issue #343).
 *
 * Sort sub-range [start_row, start_row + nrows) of r in-place.
 * O(n^2) but low constant overhead — faster than radix sort for small N.
 */
static int
col_rel_insertion_sort(col_rel_t *r, uint32_t start_row, uint32_t nrows)
{
    uint32_t nc = r->ncols;

    for (uint32_t i = 1; i < nrows; i++) {
        int64_t tmp[COL_STACK_MAX];
        int64_t *tbuf = nc <= COL_STACK_MAX ? tmp
            : (int64_t *)malloc((size_t)nc * sizeof(int64_t));
        if (!tbuf)
            return ENOMEM;
        col_rel_row_copy_out(r, start_row + i, tbuf);
        uint32_t j = i;
        while (j > 0) {
            /* Compare r[start_row + j - 1] against saved key
             * in tbuf (not the relation -- row i is overwritten
             * after the first shift). */
            int cmp = 0;
            for (uint32_t c = 0; c < nc; c++) {
                int64_t va = col_rel_get(r, start_row + j - 1, c);
                int64_t vb = tbuf[c];
                if (va > vb) {
                    cmp = 1;
                    break;
                }
                if (va < vb) {
                    cmp = -1;
                    break;
                }
            }
            if (cmp <= 0)
                break;
            col_rel_row_move(r, start_row + j, start_row + j - 1);
            j--;
        }
        col_rel_row_copy_in(r, start_row + j, tbuf);
        if (tbuf != tmp)
            free(tbuf);
    }
    return 0;
}

/*
 * col_rel_radix_sort: index-permutation LSD radix sort (Phase B, Issue #330).
 *
 * Sort sub-range [start_row, start_row + nrows) of r in-place.
 * Uses col_rel_get() for key extraction (layout-independent).
 * Sorts a permutation array instead of scattering full rows.
 * Permutation is applied once at the end via col_rel_row_copy_out/in.
 *
 * Optimizations (Issue #343):
 *   - Hybrid threshold: insertion sort for nrows <= 32
 *   - Skip-pass: skip byte positions where all values have the same byte
 *   - Byte-value cache: read column data once per pass, reuse for scatter
 *
 * Falls back to insertion sort on allocation failure.
 */
int
col_rel_radix_sort(col_rel_t *r, uint32_t start_row, uint32_t nrows)
{
    if (nrows <= 1)
        return 0;

    /* Hybrid threshold: insertion sort for small segments (Issue #343) */
    if (nrows <= 32)
        return col_rel_insertion_sort(r, start_row, nrows);

    uint32_t nc = r->ncols;

    /* Allocate permutation arrays (double-buffered for radix scatter)
     * and byte-value cache (Issue #343) */
    uint32_t *perm_a = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint32_t *perm_b = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint8_t *bv_cache = (uint8_t *)malloc(nrows);
    if (!perm_a || !perm_b || !bv_cache) {
        /* Fallback: insertion sort */
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        return col_rel_insertion_sort(r, start_row, nrows);
    }

    /* Initialize identity permutation */
    for (uint32_t i = 0; i < nrows; i++)
        perm_a[i] = i;

    uint32_t *src = perm_a;
    uint32_t *dst = perm_b;
    uint32_t count[256];
    uint32_t prefix[256];

    /* LSD radix sort: column nc-1 (LSB) to column 0 (MSB),
     * byte 0 (LSB) to byte 7 (MSB) within each column.
     * Optimization (Issue #334): direct column pointer avoids accessor
     * overhead in the hot loop. */
    for (int c = (int)nc - 1; c >= 0; c--) {
        const int64_t *col_data = r->columns[c]; /* contiguous column */
        for (int b = 0; b < 8; b++) {
            int shift = b * 8;
            int is_sign_byte = (b == 7);

            /* Skip-pass optimization (Issue #343): check if all values
             * produce the same byte at this position.  For typical Datalog
             * data (small integers), upper bytes are all zero, allowing
             * 50-75% of passes to be skipped entirely. */
            uint8_t first_bv;
            {
                int64_t val = col_data[start_row + src[0]];
                first_bv = (uint8_t)((uint64_t)val >> shift);
                if (is_sign_byte)
                    first_bv ^= 0x80u;
            }
            bool uniform = true;
            for (uint32_t i = 1; i < nrows && uniform; i++) {
                int64_t val = col_data[start_row + src[i]];
                uint8_t bv = (uint8_t)((uint64_t)val >> shift);
                if (is_sign_byte)
                    bv ^= 0x80u;
                if (bv != first_bv)
                    uniform = false;
            }
            if (uniform)
                continue; /* all values identical at this byte — skip */

            /* Count pass: read once, cache byte value (Issue #343) */
            memset(count, 0, sizeof(count));
            for (uint32_t i = 0; i < nrows; i++) {
                int64_t val = col_data[start_row + src[i]];
                uint8_t bv = (uint8_t)((uint64_t)val >> shift);
                if (is_sign_byte)
                    bv ^= 0x80u;
                bv_cache[i] = bv;
                count[bv]++;
            }

            prefix[0] = 0;
            for (int i = 1; i < 256; i++)
                prefix[i] = prefix[i - 1] + count[i - 1];

            /* Scatter pass: use cached byte values (Issue #343) */
            for (uint32_t i = 0; i < nrows; i++)
                dst[prefix[bv_cache[i]]++] = src[i];

            uint32_t *t = src;
            src = dst;
            dst = t;
        }
    }

    /* Apply permutation per-column (Issue #334): contiguous access pattern.
     * Each column is gathered independently, which is cache-friendly for
     * column-major layout. */
    int64_t *temp_col = (int64_t *)malloc(nrows * sizeof(int64_t));
    if (!temp_col) {
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        return ENOMEM;
    }

    for (uint32_t c = 0; c < nc; c++) {
        int64_t *col = r->columns[c];
        /* Gather: copy in permuted order */
        for (uint32_t i = 0; i < nrows; i++)
            temp_col[i] = col[start_row + src[i]];
        /* Scatter back */
        memcpy(col + start_row, temp_col, nrows * sizeof(int64_t));
    }

    free(temp_col);
    free(perm_a);
    free(perm_b);
    free(bv_cache);
    return 0;
}

/*
 * col_rel_radix_sort_int64: sort all rows of r in-place using LSD radix sort.
 *
 * Sorts lexicographically by all ncols columns (column 0 is most significant).
 * Handles signed int64_t by flipping the sign bit on the MSB of each column
 * so that unsigned byte comparison yields the correct signed ordering.
 *
 * Complexity: O(ncols * 8 * nrows) time, O(nrows * ncols) extra space.
 * Sets r->sorted_nrows = r->nrows on completion.
 * Falls back to insertion sort on allocation failure.
 */
void
col_rel_radix_sort_int64(col_rel_t *r)
{
    if (!r || r->ncols == 0) {
        if (r)
            r->sorted_nrows = r->nrows;
        return;
    }
    if (r->nrows <= 1) {
        r->sorted_nrows = r->nrows;
        return;
    }

    col_rel_radix_sort(r, 0, r->nrows);
    r->sorted_nrows = r->nrows;
}
