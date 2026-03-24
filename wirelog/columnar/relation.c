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
    if (r->mem_ledger && r->data && r->capacity > 0 && r->ncols > 0)
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_RELATION,
            (uint64_t)r->capacity * r->ncols * sizeof(int64_t));
    free(r->name);
    free(r->data);
    free(r->retract_backup); /* safety: non-NULL only if destroyed mid-retraction */
    free(r->merge_buf);
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
        r->data = (int64_t *)malloc(sizeof(int64_t) * r->capacity * ncols);
        if (!r->data)
            return ENOMEM;

        r->col_names = (char **)calloc(ncols, sizeof(char *));
        if (!r->col_names) {
            free(r->data);
            r->data = NULL;
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
                free(r->data);
                r->col_names = NULL;
                r->data = NULL;
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
        int64_t *nd = (int64_t *)realloc(
            r->data, sizeof(int64_t) * (size_t)new_cap * r->ncols);
        if (!nd)
            return ENOMEM;
        r->data = nd;
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
    memcpy(r->data + (size_t)r->nrows * r->ncols, row,
        sizeof(int64_t) * r->ncols);
    r->nrows++;
    return 0;
}

/* Copy all rows from src into dst (must have same ncols).
* If src has timestamps and dst has timestamp tracking enabled, the source
* timestamps are propagated to the newly appended rows. */
int
col_rel_append_all(col_rel_t *dst, const col_rel_t *src)
{
    uint32_t dst_base = dst->nrows;
    for (uint32_t i = 0; i < src->nrows; i++) {
        int rc = col_rel_append_row(dst, src->data + (size_t)i * src->ncols);
        if (rc != 0)
            return rc;
    }
    /* Overwrite the zero-initialized timestamps with src provenance. */
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
        free(r->data);
        r->data = NULL;
        r->capacity = 0;
        free(r->merge_buf);
        r->merge_buf = NULL;
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

        int64_t *nd = (int64_t *)realloc(r->data, (size_t)tight * r->ncols
                * sizeof(int64_t));
        if (!nd)
            goto free_merge_buf; /* data shrink failed; skip timestamps too */
        r->data = nd;
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
    free(r->merge_buf);
    r->merge_buf = NULL;
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
        r->data
            = (int64_t *)malloc(sizeof(int64_t) * r->capacity * like->ncols);
        if (!r->data) {
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
col_rel_pool_new_auto(delta_pool_t *pool, const char *name, uint32_t ncols)
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
        r->data = (int64_t *)malloc(sizeof(int64_t) * r->capacity * ncols);
        if (!r->data) {
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
