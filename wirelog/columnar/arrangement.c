/*
 * columnar/arrangement.c - wirelog Arrangement Layer (Phase 3C)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Hash-indexed arrangement registry for columnar relations.
 * Extracted from backend/columnar_nanoarrow.c for modular compilation.
 */

#include "columnar/internal.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Arrangement Layer (Phase 3C)                                             */
/* ======================================================================== */

/*
 * arr_hash_key: FNV-1a hash over key columns of a single row.
 * nbuckets MUST be a power of 2.
 */
static uint32_t
arr_hash_key(const int64_t *row, const uint32_t *key_cols, uint32_t key_count,
             uint32_t nbuckets)
{
    uint64_t h = 14695981039346656037ULL; /* FNV-1a basis */
    for (uint32_t k = 0; k < key_count; k++) {
        uint64_t v = (uint64_t)row[key_cols[k]];
        for (int b = 0; b < 8; b++) {
            h ^= v & 0xFFu;
            h *= 1099511628211ULL;
            v >>= 8;
        }
    }
    return (uint32_t)(h & (uint64_t)(nbuckets - 1));
}

/* Round n up to the next power of 2; minimum 16. */
static uint32_t
arr_next_pow2(uint32_t n)
{
    if (n < 16u)
        return 16u;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1u;
}

/*
 * arr_free_contents: free hash table arrays only.
 * key_cols is NOT freed here — it is always owned by the registry entry
 * (col_arr_entry_t.key_cols) and freed separately in col_session_destroy.
 */
void
arr_free_contents(col_arrangement_t *arr)
{
    if (!arr)
        return;
    free(arr->ht_head);
    free(arr->ht_next);
    arr->ht_head = NULL;
    arr->ht_next = NULL;
    arr->nbuckets = 0;
    arr->ht_cap = 0;
    arr->indexed_rows = 0;
}

/* Full rebuild: index all nrows rows in rel into arr. */
static int
arr_build_full(col_arrangement_t *arr, const col_rel_t *rel)
{
    uint32_t nrows = rel->nrows;
    uint32_t nbuckets = arr_next_pow2(nrows > 0 ? nrows * 2u : 16u);

    /* Reallocate bucket heads if size changed. */
    if (nbuckets != arr->nbuckets) {
        uint32_t *head = (uint32_t *)malloc(nbuckets * sizeof(uint32_t));
        if (!head)
            return ENOMEM;
        free(arr->ht_head);
        arr->ht_head = head;
        arr->nbuckets = nbuckets;
    }
    memset(arr->ht_head, 0xFF, nbuckets * sizeof(uint32_t)); /* UINT32_MAX */

    /* Grow chain array if needed. */
    if (nrows > arr->ht_cap) {
        uint32_t new_cap = nrows > arr->ht_cap * 2u ? nrows : arr->ht_cap * 2u;
        if (new_cap < 16u)
            new_cap = 16u;
        uint32_t *nxt = (uint32_t *)malloc(new_cap * sizeof(uint32_t));
        if (!nxt)
            return ENOMEM;
        free(arr->ht_next);
        arr->ht_next = nxt;
        arr->ht_cap = new_cap;
    }

    uint32_t nc = rel->ncols;
    for (uint32_t row = 0; row < nrows; row++) {
        const int64_t *rp = rel->data + (size_t)row * nc;
        uint32_t bucket
            = arr_hash_key(rp, arr->key_cols, arr->key_count, nbuckets);
        arr->ht_next[row] = arr->ht_head[bucket];
        arr->ht_head[bucket] = row;
    }
    arr->indexed_rows = nrows;
    return 0;
}

/* Incremental update: index only rows [old_nrows..rel->nrows). */
static int
arr_update_incremental(col_arrangement_t *arr, const col_rel_t *rel,
                       uint32_t old_nrows)
{
    uint32_t nrows = rel->nrows;
    if (old_nrows >= nrows)
        return 0;

    /* If load factor would exceed 50%, full rebuild needed. */
    uint32_t needed = arr_next_pow2(nrows * 2u);
    if (needed != arr->nbuckets)
        return arr_build_full(arr, rel);

    /* Grow chain array if needed. */
    if (nrows > arr->ht_cap) {
        uint32_t new_cap = nrows * 2u < 16u ? 16u : nrows * 2u;
        uint32_t *nxt
            = (uint32_t *)realloc(arr->ht_next, new_cap * sizeof(uint32_t));
        if (!nxt)
            return ENOMEM;
        arr->ht_next = nxt;
        arr->ht_cap = new_cap;
    }

    uint32_t nc = rel->ncols;
    uint32_t nb = arr->nbuckets;
    for (uint32_t row = old_nrows; row < nrows; row++) {
        const int64_t *rp = rel->data + (size_t)row * nc;
        uint32_t bucket = arr_hash_key(rp, arr->key_cols, arr->key_count, nb);
        arr->ht_next[row] = arr->ht_head[bucket];
        arr->ht_head[bucket] = row;
    }
    arr->indexed_rows = nrows;
    return 0;
}

/* ======================================================================== */
/* Arrangement Accessors (Phase 3C)                                         */
/* ======================================================================== */

col_arrangement_t *
col_session_get_arrangement(wl_session_t *sess, const char *rel_name,
                            const uint32_t *key_cols, uint32_t key_count)
{
    if (!sess || !rel_name || !key_cols || key_count == 0)
        return NULL;

    wl_col_session_t *cs = COL_SESSION(sess);

    /* Look up the relation. */
    col_rel_t *rel = NULL;
    for (uint32_t i = 0; i < cs->nrels; i++) {
        if (cs->rels[i] && cs->rels[i]->name
            && strcmp(cs->rels[i]->name, rel_name) == 0) {
            rel = cs->rels[i];
            break;
        }
    }
    if (!rel)
        return NULL;

    /* Search registry for matching (rel_name, key_cols) entry. */
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        col_arr_entry_t *e = &cs->arr_entries[i];
        if (e->key_count != key_count)
            continue;
        if (strcmp(e->rel_name, rel_name) != 0)
            continue;
        bool match = true;
        for (uint32_t k = 0; k < key_count; k++) {
            if (e->key_cols[k] != key_cols[k]) {
                match = false;
                break;
            }
        }
        if (!match)
            continue;

        /* Found: update if stale. */
        if (e->arr.indexed_rows == 0 && rel->nrows > 0) {
            if (arr_build_full(&e->arr, rel) != 0)
                return NULL;
        } else if (e->arr.indexed_rows < rel->nrows) {
            uint32_t old = e->arr.indexed_rows;
            if (arr_update_incremental(&e->arr, rel, old) != 0)
                return NULL;
        }
        return &e->arr;
    }

    /* Not found: grow registry and create new entry. */
    if (cs->arr_count >= cs->arr_cap) {
        uint32_t new_cap = cs->arr_cap ? cs->arr_cap * 2u : 8u;
        col_arr_entry_t *ne = (col_arr_entry_t *)realloc(
            cs->arr_entries, new_cap * sizeof(col_arr_entry_t));
        if (!ne)
            return NULL;
        cs->arr_entries = ne;
        cs->arr_cap = new_cap;
    }

    col_arr_entry_t *e = &cs->arr_entries[cs->arr_count];
    memset(e, 0, sizeof(*e));

    e->rel_name = wl_strdup(rel_name);
    if (!e->rel_name)
        return NULL;

    e->key_cols = (uint32_t *)malloc(key_count * sizeof(uint32_t));
    if (!e->key_cols) {
        free(e->rel_name);
        e->rel_name = NULL;
        return NULL;
    }
    memcpy(e->key_cols, key_cols, key_count * sizeof(uint32_t));
    e->key_count = key_count;
    e->arr.key_cols = e->key_cols; /* shared view; key_cols owned by entry */
    e->arr.key_count = key_count;
    cs->arr_count++;

    /* Initial build. */
    if (rel->nrows > 0 && arr_build_full(&e->arr, rel) != 0) {
        /* Roll back the entry we just added. */
        cs->arr_count--;
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    return &e->arr;
}

uint32_t
col_arrangement_find_first(const col_arrangement_t *arr,
                           const int64_t *rel_data, uint32_t rel_ncols,
                           const int64_t *key_row)
{
    if (!arr || !rel_data || !key_row || arr->nbuckets == 0)
        return UINT32_MAX;

    uint32_t bucket
        = arr_hash_key(key_row, arr->key_cols, arr->key_count, arr->nbuckets);
    uint32_t row = arr->ht_head[bucket];
    while (row != UINT32_MAX) {
        const int64_t *rp = rel_data + (size_t)row * rel_ncols;
        bool match = true;
        for (uint32_t k = 0; k < arr->key_count; k++) {
            if (rp[arr->key_cols[k]] != key_row[arr->key_cols[k]]) {
                match = false;
                break;
            }
        }
        if (match)
            return row;
        row = arr->ht_next[row];
    }
    return UINT32_MAX;
}

uint32_t
col_arrangement_find_next(const col_arrangement_t *arr, uint32_t row_idx)
{
    if (!arr || row_idx >= arr->ht_cap)
        return UINT32_MAX;
    return arr->ht_next[row_idx];
}

void
col_session_invalidate_arrangements(wl_session_t *sess, const char *rel_name)
{
    if (!sess || !rel_name)
        return;
    wl_col_session_t *cs = COL_SESSION(sess);
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (strcmp(cs->arr_entries[i].rel_name, rel_name) == 0)
            cs->arr_entries[i].arr.indexed_rows = 0; /* force full rebuild */
    }
}

/* ======================================================================== */
/* Delta Arrangement Cache (Phase 3C-001-Ext)                               */
/* ======================================================================== */

/*
 * col_session_free_delta_arrangements:
 *
 * Free all entries in the delta arrangement cache and reset the cache.
 * Called at the start of each semi-naive iteration (sequential path) and
 * by col_op_k_fusion after each dispatch to clean up worker copies.
 *
 * Safe to call on a zeroed session (darr_count == 0, darr_entries == NULL).
 */
void
col_session_free_delta_arrangements(wl_col_session_t *cs)
{
    for (uint32_t i = 0; i < cs->darr_count; i++) {
        col_arr_entry_t *e = &cs->darr_entries[i];
        free(e->rel_name);
        free(e->key_cols);
        arr_free_contents(&e->arr);
    }
    free(cs->darr_entries);
    cs->darr_entries = NULL;
    cs->darr_count = 0;
    cs->darr_cap = 0;
}

/*
 * col_session_get_delta_arrangement:
 *
 * Return (or lazily create) a delta arrangement for `delta_rel` keyed on
 * `key_cols[0..key_count)`.  Stored in cs->darr_entries (the per-worker
 * delta cache) and keyed by (rel_name, key_cols[]).
 *
 * Unlike full arrangements (which persist across iterations for EDB
 * relations), delta arrangements are rebuilt from scratch if stale
 * (indexed_rows != delta_rel->nrows).  This handles the case where the
 * same delta relation grows between iterations.
 *
 * Returns NULL on allocation failure or if key_count == 0.
 */
col_arrangement_t *
col_session_get_delta_arrangement(wl_col_session_t *cs, const char *rel_name,
                                  const col_rel_t *delta_rel,
                                  const uint32_t *key_cols, uint32_t key_count)
{
    if (!cs || !rel_name || !delta_rel || !key_cols || key_count == 0)
        return NULL;

    /* Search existing cache entries. */
    for (uint32_t i = 0; i < cs->darr_count; i++) {
        col_arr_entry_t *e = &cs->darr_entries[i];
        if (e->key_count != key_count)
            continue;
        if (strcmp(e->rel_name, rel_name) != 0)
            continue;
        bool match = true;
        for (uint32_t k = 0; k < key_count; k++) {
            if (e->key_cols[k] != key_cols[k]) {
                match = false;
                break;
            }
        }
        if (!match)
            continue;
        /* Found: rebuild if stale (delta changed size). */
        if (e->arr.indexed_rows != delta_rel->nrows) {
            arr_free_contents(&e->arr);
            if (delta_rel->nrows > 0 && arr_build_full(&e->arr, delta_rel) != 0)
                return NULL;
        }
        return &e->arr;
    }

    /* Not found: grow cache and create new entry. */
    if (cs->darr_count >= cs->darr_cap) {
        uint32_t new_cap = cs->darr_cap ? cs->darr_cap * 2u : 4u;
        col_arr_entry_t *ne = (col_arr_entry_t *)realloc(
            cs->darr_entries, new_cap * sizeof(col_arr_entry_t));
        if (!ne)
            return NULL;
        cs->darr_entries = ne;
        cs->darr_cap = new_cap;
    }

    col_arr_entry_t *e = &cs->darr_entries[cs->darr_count];
    memset(e, 0, sizeof(*e));

    e->rel_name = wl_strdup(rel_name);
    if (!e->rel_name)
        return NULL;

    e->key_cols = (uint32_t *)malloc(key_count * sizeof(uint32_t));
    if (!e->key_cols) {
        free(e->rel_name);
        e->rel_name = NULL;
        return NULL;
    }
    memcpy(e->key_cols, key_cols, key_count * sizeof(uint32_t));
    e->key_count = key_count;
    e->arr.key_cols = e->key_cols; /* shared view; owned by entry */
    e->arr.key_count = key_count;
    cs->darr_count++;

    /* Initial build. */
    if (delta_rel->nrows > 0 && arr_build_full(&e->arr, delta_rel) != 0) {
        cs->darr_count--;
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    return &e->arr;
}

/*
 * col_session_get_darr_count:
 *
 * Return the number of delta arrangement cache entries in the session.
 * Expected to be 0 on the main session after K-fusion evaluation
 * (delta caches are per-worker and freed after each dispatch).
 *
 * Used by tests to verify per-worker isolation invariant.
 */
uint32_t
col_session_get_darr_count(wl_session_t *sess)
{
    if (!sess)
        return 0;
    return COL_SESSION(sess)->darr_count;
}

/*
 * col_session_get_frontier:
 *
 * Copy frontiers[stratum_idx] into *out_frontier.
 * Returns EINVAL for NULL args or out-of-range stratum_idx.
 */
int
col_session_get_frontier(wl_session_t *session, uint32_t stratum_idx,
                         col_frontier_2d_t *out_frontier)
{
    if (!session || !out_frontier || stratum_idx >= MAX_STRATA)
        return EINVAL;
    *out_frontier = COL_SESSION(session)->frontiers[stratum_idx];
    return 0;
}

/* ======================================================================== */
/* Sorted Arrangement Cache (Issue #195)                                    */
/* ======================================================================== */

/*
 * sarr_row_cmp: qsort_r comparator that orders rows by a single key column.
 * Context points to a uint32_t key_col value (matching QSORT_R_CALL signature
 * in internal.h).
 */
#ifdef __GLIBC__
static int
sarr_row_cmp(const void *a, const void *b, void *ctx)
{
    const uint32_t kc = *(const uint32_t *)ctx;
    const int64_t ka = ((const int64_t *)a)[kc];
    const int64_t kb = ((const int64_t *)b)[kc];
    return (ka > kb) - (ka < kb);
}
#elif defined(_MSC_VER)
static int __cdecl sarr_row_cmp(void *ctx, const void *a, const void *b)
{
    const uint32_t kc = *(const uint32_t *)ctx;
    const int64_t ka = ((const int64_t *)a)[kc];
    const int64_t kb = ((const int64_t *)b)[kc];
    return (ka > kb) - (ka < kb);
}
#else
static int
sarr_row_cmp(void *ctx, const void *a, const void *b)
{
    const uint32_t kc = *(const uint32_t *)ctx;
    const int64_t ka = ((const int64_t *)a)[kc];
    const int64_t kb = ((const int64_t *)b)[kc];
    return (ka > kb) - (ka < kb);
}
#endif

/*
 * sarr_build: (re)build sorted copy from rel into sarr.
 * Frees any previous sorted buffer and allocates a fresh one.
 * Returns 0 on success, ENOMEM on allocation failure.
 */
static int
sarr_build(col_sorted_arr_t *sarr, const col_rel_t *rel, uint32_t key_col)
{
    free(sarr->sorted);
    sarr->sorted = NULL;
    sarr->nrows = 0;
    sarr->ncols = rel->ncols;
    sarr->key_col = key_col;
    sarr->indexed_rows = 0;

    if (rel->nrows == 0)
        return 0;

    size_t bytes = (size_t)rel->nrows * rel->ncols * sizeof(int64_t);
    sarr->sorted = (int64_t *)malloc(bytes);
    if (!sarr->sorted)
        return ENOMEM;

    memcpy(sarr->sorted, rel->data, bytes);
    uint32_t kc = key_col;
    QSORT_R_CALL(sarr->sorted, rel->nrows, rel->ncols * sizeof(int64_t), &kc,
                 sarr_row_cmp);
    sarr->nrows = rel->nrows;
    sarr->indexed_rows = rel->nrows;
    return 0;
}

/*
 * col_session_get_sorted_arrangement:
 *
 * Return (or lazily build) a sorted arrangement for `rel_name` keyed on
 * `key_col`.  Rebuilds the sorted copy when indexed_rows != rel->nrows
 * (data was appended since last build).
 *
 * Returns NULL on allocation failure or if the relation is not found.
 */
col_sorted_arr_t *
col_session_get_sorted_arrangement(wl_col_session_t *cs, const char *rel_name,
                                   uint32_t key_col)
{
    if (!cs || !rel_name)
        return NULL;

    /* Look up the source relation. */
    col_rel_t *rel = session_find_rel(cs, rel_name);
    if (!rel)
        return NULL;

    /* Search existing cache entries. */
    for (uint32_t i = 0; i < cs->sarr_count; i++) {
        col_sorted_arr_entry_t *e = &cs->sarr_entries[i];
        if (e->key_col != key_col)
            continue;
        if (strcmp(e->rel_name, rel_name) != 0)
            continue;
        /* Found: rebuild if stale. */
        if (e->sarr.indexed_rows != rel->nrows) {
            if (sarr_build(&e->sarr, rel, key_col) != 0)
                return NULL;
        }
        return &e->sarr;
    }

    /* Not found: grow cache and create new entry. */
    if (cs->sarr_count >= cs->sarr_cap) {
        uint32_t new_cap = cs->sarr_cap ? cs->sarr_cap * 2u : 4u;
        col_sorted_arr_entry_t *ne = (col_sorted_arr_entry_t *)realloc(
            cs->sarr_entries, new_cap * sizeof(col_sorted_arr_entry_t));
        if (!ne)
            return NULL;
        cs->sarr_entries = ne;
        cs->sarr_cap = new_cap;
    }

    col_sorted_arr_entry_t *e = &cs->sarr_entries[cs->sarr_count];
    memset(e, 0, sizeof(*e));

    e->rel_name = wl_strdup(rel_name);
    if (!e->rel_name)
        return NULL;

    e->key_col = key_col;
    e->sarr.key_col = key_col;
    cs->sarr_count++;

    if (sarr_build(&e->sarr, rel, key_col) != 0) {
        cs->sarr_count--;
        free(e->rel_name);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    return &e->sarr;
}

/*
 * col_session_free_sorted_arrangements:
 *
 * Free all entries in the sorted arrangement cache and reset it.
 * Called from col_session_destroy.
 */
void
col_session_free_sorted_arrangements(wl_col_session_t *cs)
{
    if (!cs)
        return;
    for (uint32_t i = 0; i < cs->sarr_count; i++) {
        free(cs->sarr_entries[i].rel_name);
        free(cs->sarr_entries[i].sarr.sorted);
    }
    free(cs->sarr_entries);
    cs->sarr_entries = NULL;
    cs->sarr_count = 0;
    cs->sarr_cap = 0;
}
