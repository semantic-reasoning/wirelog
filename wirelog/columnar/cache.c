/*
 * columnar/cache.c - wirelog Materialization Cache & Join Management
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * CSE materialized join management and materialization cache (US-006).
 * Extracted from backend/columnar_nanoarrow.c.
 */

#include "columnar/internal.h"

#include <errno.h>
#ifndef _MSC_VER
#include <stdatomic.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* CSE Materialized Join Management (US-003)                               */
/* ======================================================================== */

/**
 * col_materialized_join_create - Allocate and initialize a materialized join.
 * Memory limit defaults to 10MB if not specified.
 */
col_materialized_join_t *
col_materialized_join_create(uint32_t ncols, uint32_t memory_limit)
{
    col_materialized_join_t *mj
        = (col_materialized_join_t *)calloc(1, sizeof(*mj));
    if (!mj)
        return NULL;

    mj->ncols = ncols;
    mj->memory_limit = (memory_limit > 0) ? memory_limit : (10 * 1024 * 1024);
    mj->capacity = 64; /* start small, grow on demand */
    mj->nrows = 0;
    mj->is_valid = false;

    if (ncols > 0) {
        mj->data = (int64_t *)malloc(sizeof(int64_t) * mj->capacity * ncols);
        if (!mj->data) {
            free(mj);
            return NULL;
        }
    }

    return mj;
}

/**
 * col_materialized_join_append - Add a row to materialized join.
 * Returns 0 on success, ENOMEM if materialized join would exceed memory limit.
 */
int
col_materialized_join_append(col_materialized_join_t *mj, const int64_t *row)
{
    if (!mj || !mj->data || mj->ncols == 0)
        return EINVAL;

    /* Check memory limit */
    uint32_t row_bytes = sizeof(int64_t) * mj->ncols;
    if ((size_t)mj->nrows * row_bytes + row_bytes > mj->memory_limit)
        return ENOMEM; /* exceeds memory limit */

    /* Grow capacity if needed */
    if (mj->nrows >= mj->capacity) {
        uint32_t new_cap = mj->capacity * 2;
        int64_t *new_data = (int64_t *)realloc(
            mj->data, sizeof(int64_t) * new_cap * mj->ncols);
        if (!new_data)
            return ENOMEM;
        mj->data = new_data;
        mj->capacity = new_cap;
    }

    memcpy(mj->data + (size_t)mj->nrows * mj->ncols, row, row_bytes);
    mj->nrows++;
    return 0;
}

/**
 * col_materialized_join_free - Free materialized join and release data.
 */
void
col_materialized_join_free(col_materialized_join_t *mj)
{
    if (!mj)
        return;
    free(mj->data);
    memset(mj, 0, sizeof(*mj));
    free(mj);
}

/**
 * col_materialized_join_invalidate - Mark as invalid (expires at end of iteration).
 */
void
col_materialized_join_invalidate(col_materialized_join_t *mj)
{
    if (mj)
        mj->is_valid = false;
}

/* ======================================================================== */
/* Materialization Cache (US-006)                                            */
/* ======================================================================== */

/**
 * col_mat_cache_key_content - Content-based hash of a col_rel_t.
 *
 * FNV-1a over first min(100, nrows) rows + shape (nrows, ncols).
 * Deterministic: same sorted data layout -> same hash.
 * Bounded cost: O(min(100, nrows)) independent of relation size.
 */
uint64_t
col_mat_cache_key_content(const col_rel_t *rel)
{
    if (!rel || rel->nrows == 0)
        return 0;

    uint64_t hash = 14695981039346656037ULL; /* FNV-1a offset basis */
    uint32_t k = rel->nrows < 100 ? rel->nrows : 100;

    for (uint32_t i = 0; i < k; i++) {
        for (uint32_t j = 0; j < rel->ncols; j++) {
            uint64_t val = (uint64_t)rel->data[i * rel->ncols + j];
            hash ^= val;
            hash *= 1099511628211ULL; /* FNV-1a prime */
        }
    }

    /* Mix in shape to distinguish relations with identical prefixes */
    hash ^= (uint64_t)rel->nrows;
    hash *= 1099511628211ULL;
    hash ^= (uint64_t)rel->ncols;
    hash *= 1099511628211ULL;

    return hash;
}

void
col_mat_cache_clear(col_mat_cache_t *cache)
{
    /* Track freed bytes before clearing (Issue #224) */
    if (cache->ledger && cache->total_bytes > 0)
        wl_mem_ledger_free(cache->ledger, WL_MEM_SUBSYS_CACHE,
                           (uint64_t)cache->total_bytes);
    for (uint32_t i = 0; i < cache->count; i++)
        col_rel_destroy(cache->entries[i].result);
    cache->count = 0;
    cache->total_bytes = 0;
}

/**
 * col_mat_cache_evict_until - Evict LRU entries until cache size is below
 * target.
 *
 * Removes least recently used (oldest) entries one by one until the cache
 * total_bytes is strictly below the target_bytes threshold. Preserves
 * recently accessed cache entries that are still useful.
 *
 * @param cache    The materialization cache.
 * @param target_bytes  Target size threshold; eviction stops when
 *                      total_bytes < target_bytes.
 */
void
col_mat_cache_evict_until(col_mat_cache_t *cache, size_t target_bytes)
{
    while (cache->count > 0 && cache->total_bytes >= target_bytes) {
        uint32_t lru = 0;
        for (uint32_t i = 1; i < cache->count; i++) {
            if (cache->entries[i].lru_clock < cache->entries[lru].lru_clock)
                lru = i;
        }
        /* Track evicted bytes under CACHE subsystem (Issue #224) */
        if (cache->ledger && cache->entries[lru].mem_bytes > 0)
            wl_mem_ledger_free(cache->ledger, WL_MEM_SUBSYS_CACHE,
                               (uint64_t)cache->entries[lru].mem_bytes);
        cache->total_bytes -= cache->entries[lru].mem_bytes;
        col_rel_destroy(cache->entries[lru].result);
        memmove(&cache->entries[lru], &cache->entries[lru + 1],
                (cache->count - lru - 1) * sizeof(col_mat_entry_t));
        cache->count--;
    }
}

col_rel_t *
col_mat_cache_lookup(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right)
{
    uint64_t lh = col_mat_cache_key_content(left);
    uint64_t rh = col_mat_cache_key_content(right);
    for (uint32_t i = 0; i < cache->count; i++) {
        if (cache->entries[i].left_hash == lh
            && cache->entries[i].right_hash == rh) {
            cache->entries[i].lru_clock = ++cache->clock;
            cache->hits++;
            return cache->entries[i].result;
        }
    }
    cache->misses++;
    return NULL;
}

void
col_mat_cache_insert(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right, col_rel_t *result)
{
    size_t result_bytes
        = (result->nrows > 0 && result->ncols > 0)
              ? (size_t)result->nrows * result->ncols * sizeof(int64_t)
              : 0;

    /* Evict LRU entries until within memory limit */
    while (cache->count > 0
           && cache->total_bytes + result_bytes > COL_MAT_CACHE_LIMIT_BYTES) {
        uint32_t lru = 0;
        for (uint32_t i = 1; i < cache->count; i++) {
            if (cache->entries[i].lru_clock < cache->entries[lru].lru_clock)
                lru = i;
        }
        if (cache->ledger && cache->entries[lru].mem_bytes > 0)
            wl_mem_ledger_free(cache->ledger, WL_MEM_SUBSYS_CACHE,
                               (uint64_t)cache->entries[lru].mem_bytes);
        cache->total_bytes -= cache->entries[lru].mem_bytes;
        col_rel_destroy(cache->entries[lru].result);
        memmove(&cache->entries[lru], &cache->entries[lru + 1],
                (cache->count - lru - 1) * sizeof(col_mat_entry_t));
        cache->count--;
    }

    /* Evict oldest entry if array is full */
    if (cache->count >= COL_MAT_CACHE_MAX) {
        uint32_t lru = 0;
        for (uint32_t i = 1; i < cache->count; i++) {
            if (cache->entries[i].lru_clock < cache->entries[lru].lru_clock)
                lru = i;
        }
        if (cache->ledger && cache->entries[lru].mem_bytes > 0)
            wl_mem_ledger_free(cache->ledger, WL_MEM_SUBSYS_CACHE,
                               (uint64_t)cache->entries[lru].mem_bytes);
        cache->total_bytes -= cache->entries[lru].mem_bytes;
        col_rel_destroy(cache->entries[lru].result);
        memmove(&cache->entries[lru], &cache->entries[lru + 1],
                (cache->count - lru - 1) * sizeof(col_mat_entry_t));
        cache->count--;
    }

    col_mat_entry_t *e = &cache->entries[cache->count++];
    e->left_hash = col_mat_cache_key_content(left);
    e->right_hash = col_mat_cache_key_content(right);
    e->result = result;
    e->mem_bytes = result_bytes;
    e->lru_clock = ++cache->clock;
    cache->total_bytes += result_bytes;
    /* Track new cache entry under CACHE subsystem (Issue #224) */
    if (cache->ledger && result_bytes > 0)
        wl_mem_ledger_alloc(cache->ledger, WL_MEM_SUBSYS_CACHE,
                            (uint64_t)result_bytes);

    /* Backpressure: when CACHE subsystem > 80% cap, evict LRU to 50% cap
     * (Issue #224, Step 4). Extends existing COL_MAT_CACHE_LIMIT_BYTES
     * policy with budget-aware pressure response. */
    if (cache->ledger
        && wl_mem_ledger_should_backpressure(cache->ledger, WL_MEM_SUBSYS_CACHE,
                                             80)) {
        uint64_t budget = atomic_load_explicit(&cache->ledger->total_budget,
                                               memory_order_relaxed);
        if (budget > 0) {
            /* Target: 50% of this subsystem's cap */
            uint64_t cap
                = (budget * wl_mem_subsys_pct[WL_MEM_SUBSYS_CACHE]) / 100;
            size_t target = (size_t)(cap / 2);
            if (target < cache->total_bytes) {
                const char *mem_report_env = getenv("WL_MEM_REPORT");
                if (mem_report_env && mem_report_env[0] == '1')
                    fprintf(stderr,
                            "[wirelog mem] CACHE backpressure: evicting LRU "
                            "to 50%% of cap (%zu -> %zu bytes)\n",
                            cache->total_bytes, target);
                col_mat_cache_evict_until(cache, target);
            }
        }
    }
}
