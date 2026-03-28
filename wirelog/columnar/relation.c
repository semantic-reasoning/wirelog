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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __ARM_NEON__
#include <arm_neon.h>
#endif

/*
 * Portable software-prefetch macros (Issue #363).
 *
 * WL_PREFETCH_R(addr) — read prefetch, L2 temporal (gather source).
 * WL_PREFETCH_W(addr) — write prefetch, L2 temporal (scatter destination).
 *
 * MSVC does not support __builtin_prefetch; map to _mm_prefetch with
 * _MM_HINT_T1 (L2 locality) for both read and write variants since MSVC
 * does not expose a write-intent hint via _mm_prefetch.
 */
#if defined(_MSC_VER)
#include <xmmintrin.h>
#define WL_PREFETCH_R(addr) _mm_prefetch((const char *)(addr), _MM_HINT_T1)
#define WL_PREFETCH_W(addr) _mm_prefetch((const char *)(addr), _MM_HINT_T1)
#elif defined(__GNUC__) || defined(__clang__)
#define WL_PREFETCH_R(addr) __builtin_prefetch((addr), 0, 1)
#define WL_PREFETCH_W(addr) __builtin_prefetch((addr), 1, 1)
#else
#define WL_PREFETCH_R(addr) ((void)(addr))
#define WL_PREFETCH_W(addr) ((void)(addr))
#endif

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
    /* Release any prior schema (handles ncols==0 → ncols>0 upgrade;
     * col_rel_new_auto with ncols=0 sets schema_ok=true with an empty
     * struct schema, and a later set_schema with ncols>0 must release
     * the old schema before reinitializing). */
    if (r->schema_ok) {
        ArrowSchemaRelease(&r->schema);
        r->schema_ok = false;
    }
    ArrowSchemaInit(&r->schema);
    if (ArrowSchemaSetTypeStruct(&r->schema, (int64_t)ncols) != NANOARROW_OK) {
        ArrowSchemaRelease(&r->schema);
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

/* ======================================================================== */
/* Fused Uniform Check + Count Pass (Issue #363 Phase 1)                    */
/* ======================================================================== */

/*
 * radix_uniform_count_fused: single gather pass that combines the uniform
 * check and the count pass into one traversal over col_data.
 *
 * Returns true  if all nrows bytes are identical — caller skips the pass.
 * Returns false if not uniform; bv_cache[] and count[] are populated and
 *               ready for the prefix-sum / scatter steps.
 *
 * Benefit over calling radix_uniform_check_fast then radix_count_pass_fast:
 * non-skipped passes pay for only one set of gather loads instead of two.
 */

#ifdef __AVX2__
static bool
radix_uniform_count_fused_avx2(const int64_t *col_data, uint32_t start_row,
    const uint32_t *src, uint32_t nrows, int shift, int is_sign_byte,
    uint8_t first_bv, uint8_t *bv_cache, uint32_t *count)
{
    uint8_t xmask = is_sign_byte ? 0x80u : 0u;
    __m256i vshift = _mm256_set1_epi64x(shift);
    uint8_t uniform = 1; /* bitwise-AND accumulator; 0 once any mismatch seen */
    uint32_t i = 0;

    /* SIMD gather + byte extract + bv_cache write + uniformity tracking.
     * Prefetch col_data 16 elements (2 iterations) ahead to hide L3 gather
     * latency (Issue #363 Phase 2). */
    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);
        __m128i vidx0 = _mm_loadu_si128((const __m128i *)(src + i));
        __m128i vidx1 = _mm_loadu_si128((const __m128i *)(src + i + 4));
        __m256i vals0 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx0, 8);
        __m256i vals1 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx1, 8);
        __m256i sh0 = _mm256_srlv_epi64(vals0, vshift);
        __m256i sh1 = _mm256_srlv_epi64(vals1, vshift);

        bv_cache[i + 0] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                0) ^ xmask;
        bv_cache[i + 1] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                1) ^ xmask;
        bv_cache[i + 2] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                2) ^ xmask;
        bv_cache[i + 3] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                3) ^ xmask;
        bv_cache[i + 4] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                0) ^ xmask;
        bv_cache[i + 5] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                1) ^ xmask;
        bv_cache[i + 6] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                2) ^ xmask;
        bv_cache[i + 7] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                3) ^ xmask;

        /* Branchless bitwise accumulation — avoids misprediction in hot loop */
        uniform &= (bv_cache[i + 0] == first_bv) & (bv_cache[i + 1] ==
            first_bv) &
            (bv_cache[i + 2] == first_bv) & (bv_cache[i + 3] == first_bv) &
            (bv_cache[i + 4] == first_bv) & (bv_cache[i + 5] == first_bv) &
            (bv_cache[i + 6] == first_bv) & (bv_cache[i + 7] == first_bv);
    }

    /* Scalar tail */
    for (; i < nrows; i++) {
        uint8_t bv = (uint8_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_byte)
            bv ^= 0x80u;
        bv_cache[i] = bv;
        uniform &= (uint8_t)(bv == first_bv);
    }

    if (uniform)
        return true;

    /* Not uniform: build histogram from bv_cache (sequential read) */
    memset(count, 0, 256 * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
static bool
radix_uniform_count_fused_neon(const int64_t *col_data, uint32_t start_row,
    const uint32_t *src, uint32_t nrows, int shift, int is_sign_byte,
    uint8_t first_bv, uint8_t *bv_cache, uint32_t *count)
{
    uint8_t xmask = is_sign_byte ? 0x80u : 0u;
    int64x2_t vneg_shift = vdupq_n_s64(-(int64_t)shift);
    uint8x8_t vfirst = vdup_n_u8(first_bv);
    /* Bit-AND mask: stays UINT64_MAX while all compared bytes equal first_bv */
    uint64_t uniform_mask = UINT64_MAX;
    uint32_t i = 0;

    /* NEON gather + byte extract + bv_cache write + uniformity tracking.
     * Prefetch col_data 16 elements (2 iterations) ahead to hide L3 gather
     * latency (Issue #363 Phase 2). */
    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);
        int64_t v0 = col_data[start_row + src[i + 0]];
        int64_t v1 = col_data[start_row + src[i + 1]];
        int64_t v2 = col_data[start_row + src[i + 2]];
        int64_t v3 = col_data[start_row + src[i + 3]];
        int64_t v4 = col_data[start_row + src[i + 4]];
        int64_t v5 = col_data[start_row + src[i + 5]];
        int64_t v6 = col_data[start_row + src[i + 6]];
        int64_t v7 = col_data[start_row + src[i + 7]];

        int64x2_t vec01 = vcombine_s64(
            vcreate_s64((uint64_t)v0), vcreate_s64((uint64_t)v1));
        int64x2_t vec23 = vcombine_s64(
            vcreate_s64((uint64_t)v2), vcreate_s64((uint64_t)v3));
        int64x2_t vec45 = vcombine_s64(
            vcreate_s64((uint64_t)v4), vcreate_s64((uint64_t)v5));
        int64x2_t vec67 = vcombine_s64(
            vcreate_s64((uint64_t)v6), vcreate_s64((uint64_t)v7));

        uint64x2_t sv01 = vshlq_u64(vreinterpretq_u64_s64(vec01), vneg_shift);
        uint64x2_t sv23 = vshlq_u64(vreinterpretq_u64_s64(vec23), vneg_shift);
        uint64x2_t sv45 = vshlq_u64(vreinterpretq_u64_s64(vec45), vneg_shift);
        uint64x2_t sv67 = vshlq_u64(vreinterpretq_u64_s64(vec67), vneg_shift);

        uint8_t bvals[8] = {
            (uint8_t)vgetq_lane_u64(sv01, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv01, 1) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv23, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv23, 1) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv45, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv45, 1) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv67, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv67, 1) ^ xmask,
        };
        uint8x8_t bvec = vld1_u8(bvals);
        vst1_u8(bv_cache + i, bvec);

        /* SIMD compare: all-0xFF if every byte equals first_bv; AND into mask */
        uint8x8_t cmp = vceq_u8(bvec, vfirst);
        uniform_mask &= vget_lane_u64(vreinterpret_u64_u8(cmp), 0);
    }

    /* Scalar tail */
    bool uniform_tail = true;
    for (; i < nrows; i++) {
        uint8_t bv = (uint8_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_byte)
            bv ^= 0x80u;
        bv_cache[i] = bv;
        if (bv != first_bv)
            uniform_tail = false;
    }

    if (uniform_mask == UINT64_MAX && uniform_tail)
        return true;

    /* Not uniform: build histogram from bv_cache (sequential read) */
    memset(count, 0, 256 * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __ARM_NEON__ */

#if !defined(__AVX2__) && !defined(__ARM_NEON__)
/*
 * Scalar fallback: single loop over all elements — builds bv_cache[] and
 * count[] in one pass and returns true if all bytes are equal to first_bv.
 * Only compiled on non-SIMD targets to avoid unused-function warnings.
 */
static bool
radix_uniform_count_fused_scalar(const int64_t *col_data, uint32_t start_row,
    const uint32_t *src, uint32_t nrows, int shift, int is_sign_byte,
    uint8_t first_bv, uint8_t *bv_cache, uint32_t *count)
{
    bool uniform = true;

    memset(count, 0, 256 * sizeof(uint32_t));
    for (uint32_t i = 0; i < nrows; i++) {
        uint8_t bv = (uint8_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_byte)
            bv ^= 0x80u;
        bv_cache[i] = bv;
        count[bv]++;
        if (bv != first_bv)
            uniform = false;
    }
    return uniform;
}
#endif /* !__AVX2__ && !__ARM_NEON__ */

/* Compile-time dispatch: AVX2 > NEON > scalar */
#ifdef __AVX2__
#define radix_uniform_count_fused_fast radix_uniform_count_fused_avx2
#elif defined(__ARM_NEON__)
#define radix_uniform_count_fused_fast radix_uniform_count_fused_neon
#else
#define radix_uniform_count_fused_fast radix_uniform_count_fused_scalar
#endif

/* ======================================================================== */
/* Fused Uniform Check + Count Pass for k=16 (Issue #363 Phase 5c ext.)    */
/* ======================================================================== */

/*
 * k=16 variants: same two-pass strategy as the k=8 functions above, but
 * operating on uint16_t bucket values and a 65536-entry count[] histogram.
 *
 * Pass 1 (SIMD): gather int64_t, extract 16-bit bucket, write bv_cache[],
 *   accumulate uniformity check.  Return true immediately if uniform.
 * Pass 2 (scalar, only when not uniform): memset(count, 0, 256KB) + histogram.
 */

#ifdef __AVX2__
static bool
radix_uniform_count_fused_k16_avx2(const int64_t *col_data,
    uint32_t start_row, const uint32_t *src, uint32_t nrows, int shift,
    int is_sign_pass, uint16_t first_bv, uint16_t *bv_cache, uint32_t *count)
{
    uint16_t xmask = is_sign_pass ? 0x8000u : 0u;
    __m256i vshift = _mm256_set1_epi64x(shift);
    __m256i vmask16 = _mm256_set1_epi64x(0xFFFF);
    uint8_t uniform = 1;
    uint32_t i = 0;

    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);
        __m128i vidx0 = _mm_loadu_si128((const __m128i *)(src + i));
        __m128i vidx1 = _mm_loadu_si128((const __m128i *)(src + i + 4));
        __m256i vals0 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx0, 8);
        __m256i vals1 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx1, 8);
        __m256i sh0 = _mm256_and_si256(
            _mm256_srlv_epi64(vals0, vshift), vmask16);
        __m256i sh1 = _mm256_and_si256(
            _mm256_srlv_epi64(vals1, vshift), vmask16);

        uint16_t b0 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                0) ^ xmask;
        uint16_t b1 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                1) ^ xmask;
        uint16_t b2 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                2) ^ xmask;
        uint16_t b3 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                3) ^ xmask;
        uint16_t b4 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                0) ^ xmask;
        uint16_t b5 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                1) ^ xmask;
        uint16_t b6 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                2) ^ xmask;
        uint16_t b7 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                3) ^ xmask;

        bv_cache[i + 0] = b0;
        bv_cache[i + 1] = b1;
        bv_cache[i + 2] = b2;
        bv_cache[i + 3] = b3;
        bv_cache[i + 4] = b4;
        bv_cache[i + 5] = b5;
        bv_cache[i + 6] = b6;
        bv_cache[i + 7] = b7;

        uniform &= (b0 == first_bv) & (b1 == first_bv) &
            (b2 == first_bv) & (b3 == first_bv) &
            (b4 == first_bv) & (b5 == first_bv) &
            (b6 == first_bv) & (b7 == first_bv);
    }

    for (; i < nrows; i++) {
        uint16_t bv =
            (uint16_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_pass)
            bv ^= 0x8000u;
        bv_cache[i] = bv;
        uniform &= (uint8_t)(bv == first_bv);
    }

    if (uniform)
        return true;

    memset(count, 0, 65536u * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
static bool
radix_uniform_count_fused_k16_neon(const int64_t *col_data,
    uint32_t start_row, const uint32_t *src, uint32_t nrows, int shift,
    int is_sign_pass, uint16_t first_bv, uint16_t *bv_cache, uint32_t *count)
{
    uint16_t xmask = is_sign_pass ? 0x8000u : 0u;
    int64x2_t vneg_shift = vdupq_n_s64(-(int64_t)shift);
    uint16x8_t vfirst = vdupq_n_u16(first_bv);
    uint64_t uniform_mask = UINT64_MAX;
    uint32_t i = 0;

    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);

        int64_t v0 = col_data[start_row + src[i + 0]];
        int64_t v1 = col_data[start_row + src[i + 1]];
        int64_t v2 = col_data[start_row + src[i + 2]];
        int64_t v3 = col_data[start_row + src[i + 3]];
        int64_t v4 = col_data[start_row + src[i + 4]];
        int64_t v5 = col_data[start_row + src[i + 5]];
        int64_t v6 = col_data[start_row + src[i + 6]];
        int64_t v7 = col_data[start_row + src[i + 7]];

        int64x2_t vec01 = vcombine_s64(
            vcreate_s64((uint64_t)v0), vcreate_s64((uint64_t)v1));
        int64x2_t vec23 = vcombine_s64(
            vcreate_s64((uint64_t)v2), vcreate_s64((uint64_t)v3));
        int64x2_t vec45 = vcombine_s64(
            vcreate_s64((uint64_t)v4), vcreate_s64((uint64_t)v5));
        int64x2_t vec67 = vcombine_s64(
            vcreate_s64((uint64_t)v6), vcreate_s64((uint64_t)v7));

        uint64x2_t sv01 = vshlq_u64(
            vreinterpretq_u64_s64(vec01), vneg_shift);
        uint64x2_t sv23 = vshlq_u64(
            vreinterpretq_u64_s64(vec23), vneg_shift);
        uint64x2_t sv45 = vshlq_u64(
            vreinterpretq_u64_s64(vec45), vneg_shift);
        uint64x2_t sv67 = vshlq_u64(
            vreinterpretq_u64_s64(vec67), vneg_shift);

        uint16_t bvals[8] = {
            (uint16_t)vgetq_lane_u64(sv01, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv01, 1) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv23, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv23, 1) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv45, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv45, 1) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv67, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv67, 1) ^ xmask,
        };
        uint16x8_t bvec = vld1q_u16(bvals);
        vst1q_u16(bv_cache + i, bvec);

        uint16x8_t cmp = vceqq_u16(bvec, vfirst);
        uint64x2_t c64 = vreinterpretq_u64_u16(cmp);
        uniform_mask &= vgetq_lane_u64(c64, 0) & vgetq_lane_u64(c64, 1);
    }

    bool uniform_tail = true;
    for (; i < nrows; i++) {
        uint16_t bv =
            (uint16_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_pass)
            bv ^= 0x8000u;
        bv_cache[i] = bv;
        if (bv != first_bv)
            uniform_tail = false;
    }

    if (uniform_mask == UINT64_MAX && uniform_tail)
        return true;

    memset(count, 0, 65536u * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __ARM_NEON__ */

#if !defined(__AVX2__) && !defined(__ARM_NEON__)
static bool
radix_uniform_count_fused_k16_scalar(const int64_t *col_data,
    uint32_t start_row, const uint32_t *src, uint32_t nrows, int shift,
    int is_sign_pass, uint16_t first_bv, uint16_t *bv_cache, uint32_t *count)
{
    uint16_t uacc = 0;

    memset(count, 0, 65536u * sizeof(uint32_t));
    for (uint32_t i = 0; i < nrows; i++) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16u]);
        int64_t v = col_data[start_row + src[i]];
        uint16_t bv = (uint16_t)((uint64_t)v >> shift);
        if (is_sign_pass)
            bv ^= 0x8000u;
        bv_cache[i] = bv;
        uacc |= (uint16_t)(bv ^ first_bv);
        count[bv]++;
    }
    return uacc == 0;
}
#endif /* !__AVX2__ && !__ARM_NEON__ */

/* Compile-time dispatch for k=16: AVX2 > NEON > scalar */
#ifdef __AVX2__
#define radix_uniform_count_fused_k16_fast radix_uniform_count_fused_k16_avx2
#elif defined(__ARM_NEON__)
#define radix_uniform_count_fused_k16_fast radix_uniform_count_fused_k16_neon
#else
#define radix_uniform_count_fused_k16_fast radix_uniform_count_fused_k16_scalar
#endif

/*
 * radix_sort_k16: LSD radix sort using 16-bit radix (Issue #363 Phase 5b/5c).
 *
 * 4 passes × 65536 buckets.  Scalar fused uniform+count loop with
 * WL_PREFETCH_R; in-place prefix sum avoids a separate 256KB prefix[].
 * Called for nrows >= 50000 where fewer passes justify the larger histogram.
 */
static int
radix_sort_k16(col_rel_t *r, uint32_t start_row, uint32_t nrows)
{
    uint32_t nc = r->ncols;

    const uint32_t radix_bits = 16u;
    const uint32_t num_passes = 64u / radix_bits;  /* 4 for k=16 */
    const uint32_t hist_size = 1u << radix_bits;   /* 65536 for k=16 */

    uint32_t *perm_a = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint32_t *perm_b = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint16_t *bv_cache = (uint16_t *)malloc(nrows * sizeof(uint16_t));
    uint32_t *count = (uint32_t *)malloc(hist_size * sizeof(uint32_t));
    if (!perm_a || !perm_b || !bv_cache || !count) {
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        free(count);
        return col_rel_insertion_sort(r, start_row, nrows);
    }

    for (uint32_t i = 0; i < nrows; i++)
        perm_a[i] = i;

    uint32_t *src = perm_a;
    uint32_t *dst = perm_b;

#ifdef WL_RADIX_BENCH
    uint64_t _tU = 0, _tS = 0, _tA = 0, _t0 = 0;
    uint32_t _nSk = 0, _nPs = 0;
#endif

    for (int c = (int)nc - 1; c >= 0; c--) {
        const int64_t *col_data = r->columns[c];
        for (uint32_t pass = 0; pass < num_passes; pass++) {
            int shift = (int)(pass * radix_bits);
            int is_sign_pass = (pass == num_passes - 1);

            uint16_t first_bv;
            {
                int64_t val = col_data[start_row + src[0]];
                first_bv = (uint16_t)((uint64_t)val >> shift);
                if (is_sign_pass)
                    first_bv ^= 0x8000u;
            }
#ifdef WL_RADIX_BENCH
            _t0 = now_ns();
#endif
            /* SIMD-dispatched fused uniform check + count pass for k=16
             * (Issue #363 Phase 5c extension). */
            if (radix_uniform_count_fused_k16_fast(col_data, start_row,
                src, nrows, shift, is_sign_pass, first_bv,
                bv_cache, count)) {
#ifdef WL_RADIX_BENCH
                _tU += now_ns() - _t0;
                _nSk++;
#endif
                continue;
            }
#ifdef WL_RADIX_BENCH
            _tU += now_ns() - _t0;
            _nPs++;
            _t0 = now_ns();
#endif
            {
                uint32_t running = 0;
                for (uint32_t i = 0; i < hist_size; i++) {
                    uint32_t cnt = count[i];
                    count[i] = running;
                    running += cnt;
                }
            }
            for (uint32_t i = 0; i < nrows; i++) {
                if (i + 16u < nrows)
                    WL_PREFETCH_W(dst + count[bv_cache[i + 16u]]);
                dst[count[bv_cache[i]]++] = src[i];
            }
            uint32_t *t = src;
            src = dst;
            dst = t;
#ifdef WL_RADIX_BENCH
            _tS += now_ns() - _t0;
#endif
        }
    }

#ifdef WL_RADIX_BENCH
    _t0 = now_ns();
#endif
    int64_t *temp_col = (int64_t *)malloc(nrows * sizeof(int64_t));
    if (!temp_col) {
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        free(count);
        return ENOMEM;
    }
    for (uint32_t c = 0; c < nc; c++) {
        int64_t *col = r->columns[c];
        for (uint32_t i = 0; i < nrows; i++) {
            if (i + 8u < nrows)
                WL_PREFETCH_R(col + start_row + src[i + 8u]);
            temp_col[i] = col[start_row + src[i]];
        }
        memcpy(col + start_row, temp_col, nrows * sizeof(int64_t));
    }
    free(temp_col);
    free(perm_a);
    free(perm_b);
    free(bv_cache);
    free(count);
#ifdef WL_RADIX_BENCH
    _tA = now_ns() - _t0;
    {
        uint64_t _tot = _tU + _tS + _tA;
        fprintf(stderr,
            "[radix-bench k=16] nrows=%u nc=%u pass=%u skip=%u "
            "uniform_+_count=%.0f%% scatter=%.0f%% apply=%.0f%% "
            "total_ms=%.3f\n",
            nrows, nc, _nPs, _nSk,
            100.0 * _tU / (_tot + 1),
            100.0 * _tS / (_tot + 1), 100.0 * _tA / (_tot + 1),
            (double)_tot * 1e-6);
    }
#endif
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
 * Adaptive radix width (Issue #363 Phase 5c):
 *   - nrows >= 50000: k=16 via radix_sort_k16() — 4 passes × 65536 buckets
 *   - nrows <  50000: k=8  with SIMD fused uniform+count — 8 passes × 256 buckets
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

    /* Adaptive radix width (Issue #363 Phase 5c): dispatch to k=16 for large
     * arrays where fewer passes outweigh the larger histogram cost.
     *
     * Empirical threshold (Apple M-series, 1-col, 64-bit uniform-random keys):
     *   nrows=10K: k8=0.66ms  k16=0.81ms  k8 faster by 1.23x
     *   nrows=20K: k8=0.81ms  k16=0.91ms  k8 faster by 1.12x
     *   nrows=30K: k8=0.83ms  k16=0.89ms  k8 faster by 1.08x
     *   nrows=40K: k8=0.81ms  k16=0.82ms  near parity
     *   nrows=50K: k8=0.79ms  k16=0.78ms  k16 faster by 1.01x
     *   nrows=60K: k8=0.82ms  k16=0.80ms  k16 faster by 1.02x
     *   nrows=100K: k8=1.41ms k16=1.32ms  k16 faster by 1.07x
     * Crossover at ~40-50K rows; 50000 is a conservative round boundary.
     *
     * k=16 uses a scalar fused loop (not SIMD): the 4-pass reduction
     * already yields fewer total iterations than 8-pass k=8+SIMD, and a
     * 256KB histogram makes SIMD gather impractical (cache pressure). */
    if (nrows >= 50000u)
        return radix_sort_k16(r, start_row, nrows);

    /* k=8 SIMD path: 8 passes × 256 buckets (1KB histogram, stack-allocated).
     * SIMD-dispatched fused uniform+count avoids a separate gather loop. */
    uint32_t nc = r->ncols;

    const uint32_t radix_bits = 8u;
    const uint32_t num_passes = 64u / radix_bits;  /* 8 for k=8 */

    uint32_t *perm_a = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint32_t *perm_b = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint8_t  *bv_cache = (uint8_t *)malloc(nrows);
    if (!perm_a || !perm_b || !bv_cache) {
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        return col_rel_insertion_sort(r, start_row, nrows);
    }

    for (uint32_t i = 0; i < nrows; i++)
        perm_a[i] = i;

    uint32_t *src = perm_a;
    uint32_t *dst = perm_b;
    uint32_t count[256];
    uint32_t prefix[256];

#ifdef WL_RADIX_BENCH
    /* _tU = fused uniform+count (radix_uniform_count_fused_fast)
     * _tS = scatter (prefix scan + scatter loop)
     * _tA = apply permutation per-column */
    uint64_t _tU = 0, _tS = 0, _tA = 0, _t0 = 0;
    uint32_t _nSk = 0, _nPs = 0;
#endif

    /* LSD radix sort: column nc-1 (LSB) to column 0 (MSB),
     * pass 0 (LSB) to pass num_passes-1 (MSB) within each column. */
    for (int c = (int)nc - 1; c >= 0; c--) {
        const int64_t *col_data = r->columns[c];
        for (uint32_t pass = 0; pass < num_passes; pass++) {
            int shift = (int)(pass * radix_bits);
            int is_sign_pass = (pass == num_passes - 1);

            /* Skip-pass: if all values identical at this byte position, skip. */
            uint8_t first_bv;
            {
                int64_t val = col_data[start_row + src[0]];
                first_bv = (uint8_t)((uint64_t)val >> shift);
                if (is_sign_pass)
                    first_bv ^= 0x80u;
            }
            /* Fused uniform check + count pass: single gather traversal
             * (Issue #363 Phase 1).  Returns true if all nrows bytes at
             * this position are identical (skip); returns false with
             * bv_cache[] and count[] populated when not uniform. */
#ifdef WL_RADIX_BENCH
            _t0 = now_ns();
#endif
            if (radix_uniform_count_fused_fast(col_data, start_row, src,
                nrows, shift, is_sign_pass, first_bv, bv_cache, count)) {
#ifdef WL_RADIX_BENCH
                _tU += now_ns() - _t0;
                _nSk++;
#endif
                continue;
            }
#ifdef WL_RADIX_BENCH
            _tU += now_ns() - _t0;
            _nPs++;
            _t0 = now_ns();
#endif
            prefix[0] = 0;
            for (int i = 1; i < 256; i++)
                prefix[i] = prefix[i - 1] + count[i - 1];

            /* Scatter pass with software prefetch (Issue #363 Phase 2). */
            for (uint32_t i = 0; i < nrows; i++) {
                if (i + 16u < nrows)
                    WL_PREFETCH_W(dst + prefix[bv_cache[i + 16u]]);
                dst[prefix[bv_cache[i]]++] = src[i];
            }
            uint32_t *t = src;
            src = dst;
            dst = t;
#ifdef WL_RADIX_BENCH
            _tS += now_ns() - _t0;
#endif
        }
    }

#ifdef WL_RADIX_BENCH
    _t0 = now_ns();
#endif
    /* Apply permutation per-column (Issue #334): contiguous access pattern. */
    int64_t *temp_col = (int64_t *)malloc(nrows * sizeof(int64_t));
    if (!temp_col) {
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        return ENOMEM;
    }
    for (uint32_t c = 0; c < nc; c++) {
        int64_t *col = r->columns[c];
        /* Gather: prefetch 8 elements ahead (Issue #363 Phase 3). */
        for (uint32_t i = 0; i < nrows; i++) {
            if (i + 8u < nrows)
                WL_PREFETCH_R(col + start_row + src[i + 8u]);
            temp_col[i] = col[start_row + src[i]];
        }
        memcpy(col + start_row, temp_col, nrows * sizeof(int64_t));
    }
    free(temp_col);
    free(perm_a);
    free(perm_b);
    free(bv_cache);
#ifdef WL_RADIX_BENCH
    _tA = now_ns() - _t0;
    {
        uint64_t _tot = _tU + _tS + _tA;
        fprintf(stderr,
            "[radix-bench k=8] nrows=%u nc=%u pass=%u skip=%u "
            "uniform_+_count=%.0f%% scatter=%.0f%% apply=%.0f%% "
            "total_ms=%.3f\n",
            nrows, nc, _nPs, _nSk,
            100.0 * _tU / (_tot + 1),
            100.0 * _tS / (_tot + 1), 100.0 * _tA / (_tot + 1),
            (double)_tot * 1e-6);
    }
#endif
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
