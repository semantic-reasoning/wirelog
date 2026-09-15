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

#include <assert.h>
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
            uint64_t val = (uint64_t)col_rel_get(rel, i, j);
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

/*
 * mat_cache_entry_destroy: release one entry's result and its CACHE charge
 * (Issue #1380).  Does not touch the entries[] array or total_bytes.
 */
static void
mat_cache_entry_destroy(col_mat_cache_t *cache, col_mat_entry_t *e)
{
    if (cache->ledger && e->ledger_bytes > 0)
        wl_mem_ledger_free(cache->ledger, WL_MEM_SUBSYS_CACHE,
            e->ledger_bytes);
    e->ledger_bytes = 0;
    if (e->owns_result)
        col_rel_destroy(e->result);
    e->result = NULL;
    e->mem_bytes = 0;
    e->eviction_deferred = false;
    e->owner_alive = false;
    e->owns_result = false;
    e->epoch_pin_count = 0;
}

static bool
mat_cache_result_is_exclusive(const col_rel_t *result)
{
    if (!result || result->pool_owned || result->arena_owned
        || result->col_shared || result->storage_owner != result
        || result->storage_alias_borrows != 0)
        return false;
    return true;
}

static void
mat_cache_remove_at(col_mat_cache_t *cache, uint32_t index)
{
    col_mat_entry_t *entry = &cache->entries[index];
    assert(entry->pin_count == 0);
    cache->total_bytes -= entry->mem_bytes;
    mat_cache_entry_destroy(cache, entry);
    memmove(entry, entry + 1,
        (cache->count - index - 1) * sizeof(col_mat_entry_t));
    cache->count--;
}

/*
 * mat_cache_evict_lru: drop the least recently used entry.  Caller
 * guarantees cache->count > 0.  Pinned entries stay physically present and
 * charged to total_bytes/ledger_bytes.  Pinned entries are skipped by LRU;
 * when every entry is pinned, admission returns ENOSPC without changing
 * ownership.  Clear and truncate defer destruction of pinned entries (and
 * hide them from lookup); the final pin release removes them and their
 * charge.
 */
static bool
mat_cache_evict_lru(col_mat_cache_t *cache)
{
    uint32_t lru = UINT32_MAX;
    for (uint32_t i = 0; i < cache->count; i++) {
        if (!cache->entries[i].result || cache->entries[i].eviction_deferred)
            continue;
        if (cache->entries[i].pin_count > 0)
            continue;
        if (lru == UINT32_MAX
            || cache->entries[i].lru_clock < cache->entries[lru].lru_clock)
            lru = i;
    }
    if (lru != UINT32_MAX) {
        mat_cache_remove_at(cache, lru);
        return true;
    }

    /* All live entries are pinned.  Admission must fail without changing
     * ownership or evicting a still-borrowed result. */
    return false;
}

void
col_mat_cache_clear(col_mat_cache_t *cache)
{
    if (!cache)
        return;
    for (uint32_t i = 0; i < cache->count;) {
        if (cache->entries[i].pin_count > 0) {
            cache->entries[i].eviction_deferred = true;
            i++;
        } else {
            mat_cache_remove_at(cache, i);
        }
    }
}

void
col_mat_cache_release_pins(col_mat_cache_t *cache)
{
    if (!cache)
        return;
    for (uint32_t i = 0; i < cache->count;) {
        col_mat_entry_t *entry = &cache->entries[i];
        if (entry->epoch_pin_count > 0) {
            assert(entry->pin_count >= entry->epoch_pin_count);
            entry->pin_count -= entry->epoch_pin_count;
            assert(cache->active_pins >= entry->epoch_pin_count);
            cache->active_pins -= entry->epoch_pin_count;
            entry->epoch_pin_count = 0;
            entry->pin_epoch = 0;
        }
        if (entry->pin_count == 0 && entry->eviction_deferred)
            mat_cache_remove_at(cache, i);
        else
            i++;
    }
    cache->pin_epoch++;
    if (cache->pin_epoch == 0)
        cache->pin_epoch++;
}

void
col_mat_cache_truncate(col_mat_cache_t *cache, uint32_t keep_count)
{
    /* keep_count is a visible-prefix target.  Pinned suffix entries may keep
     * the physical count above it until their final borrowed reference ends,
     * but are deferred and cannot be looked up or replaced. */
    if (!cache || keep_count >= cache->count)
        return;
    for (uint32_t i = keep_count; i < cache->count;) {
        if (cache->entries[i].pin_count > 0) {
            cache->entries[i].eviction_deferred = true;
            i++;
        } else {
            mat_cache_remove_at(cache, i);
        }
    }
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
        if (!mat_cache_evict_lru(cache))
            break;
    }
}

col_rel_t *
col_mat_cache_lookup(col_mat_cache_t *cache, const col_rel_t *left,
    const col_rel_t *right)
{
    if (!cache)
        return NULL;
    if (cache->pin_epoch == 0)
        cache->pin_epoch = 1;
    uint64_t lh = col_mat_cache_key_content(left);
    uint64_t rh = col_mat_cache_key_content(right);
    for (uint32_t i = 0; i < cache->count; i++) {
        if (cache->entries[i].left_hash == lh
            && cache->entries[i].right_hash == rh
            && wl_columnar_relation_snapshot_equal(
                cache->entries[i].left_snapshot,
                wl_columnar_relation_snapshot(left))
            && wl_columnar_relation_snapshot_equal(
                cache->entries[i].right_snapshot,
                wl_columnar_relation_snapshot(right))
            && cache->entries[i].result != NULL
            && !cache->entries[i].eviction_deferred) {
            col_mat_entry_t *entry = &cache->entries[i];
            if (entry->owner_alive && entry->generation != 0
                && entry->pin_epoch != cache->pin_epoch) {
                entry->pin_count++;
                entry->epoch_pin_count++;
                entry->pin_epoch = cache->pin_epoch;
                assert(cache->active_pins < UINT32_MAX);
                cache->active_pins++;
            }
            cache->entries[i].lru_clock = ++cache->clock;
            cache->hits++;
            return entry->result;
        }
    }
    cache->misses++;
    return NULL;
}

col_rel_t *
col_mat_cache_lookup_pin(col_mat_cache_t *cache, const col_rel_t *left,
    const col_rel_t *right, col_mat_cache_pin_t *pin)
{
    if (!cache || !left || !right || !pin)
        return NULL;
    memset(pin, 0, sizeof(*pin));
    uint64_t lh = col_mat_cache_key_content(left);
    uint64_t rh = col_mat_cache_key_content(right);
    for (uint32_t i = 0; i < cache->count; i++) {
        col_mat_entry_t *entry = &cache->entries[i];
        if (entry->left_hash == lh && entry->right_hash == rh
            && wl_columnar_relation_snapshot_equal(entry->left_snapshot,
            wl_columnar_relation_snapshot(left))
            && wl_columnar_relation_snapshot_equal(entry->right_snapshot,
            wl_columnar_relation_snapshot(right))
            && entry->result != NULL && !entry->eviction_deferred) {
            entry->lru_clock = ++cache->clock;
            cache->hits++;
            entry->pin_count++;
            assert(cache->active_pins < UINT32_MAX);
            cache->active_pins++;
            if (pin) {
                pin->cache = cache;
                pin->identity = entry->identity;
                pin->active = true;
            }
            return entry->result;
        }
    }
    cache->misses++;
    return NULL;
}

void
col_mat_cache_pin_release(col_mat_cache_pin_t *pin)
{
    if (!pin || !pin->active || !pin->cache)
        return;
    col_mat_cache_t *cache = pin->cache;
    uint32_t index = UINT32_MAX;
    for (uint32_t i = 0; i < cache->count; i++) {
        if (cache->entries[i].identity == pin->identity) {
            index = i;
            break;
        }
    }
    assert(index != UINT32_MAX);
    if (index != UINT32_MAX) {
        col_mat_entry_t *entry = &cache->entries[index];
        assert(entry->pin_count > 0);
        if (entry->pin_count > 0)
            entry->pin_count--;
        if (entry->pin_count == 0 && entry->eviction_deferred)
            mat_cache_remove_at(cache, index);
    }
    assert(cache->active_pins > 0);
    if (cache->active_pins > 0)
        cache->active_pins--;
    pin->cache = NULL;
    pin->identity = 0;
    pin->active = false;
}

int
col_mat_cache_insert_pin(col_mat_cache_t *cache, const col_rel_t *left,
    const col_rel_t *right, col_rel_t *result, col_mat_cache_pin_t *pin)
{
    if (pin)
        memset(pin, 0, sizeof(*pin));
    if (!cache || !left || !right || !result)
        return EINVAL;
    /* The cache destroys entries on eviction.  Borrowed, arena-backed, or
     * aliased results must remain owned by their producer instead of being
     * reparented into the cache ledger. */
    if (!mat_cache_result_is_exclusive(result)
        || result == left || result == right)
        return EINVAL;
    for (uint32_t i = 0; i < cache->count; i++) {
        if (cache->entries[i].owner_alive
            && cache->entries[i].result == result)
            return EEXIST;
    }
    size_t result_bytes
        = (result->nrows > 0 && result->ncols > 0)
              ? (size_t)result->nrows * result->ncols * sizeof(int64_t)
              : 0;

    /* Evict LRU entries until within memory limit */
    while (cache->count > 0
        && cache->total_bytes + result_bytes > COL_MAT_CACHE_LIMIT_BYTES) {
        if (!mat_cache_evict_lru(cache))
            return ENOSPC;
    }

    /* Evict oldest entry if array is full */
    while (cache->count >= COL_MAT_CACHE_MAX) {
        if (!mat_cache_evict_lru(cache))
            return ENOSPC;
    }

    col_mat_entry_t *e = &cache->entries[cache->count++];
    e->left_hash = col_mat_cache_key_content(left);
    e->right_hash = col_mat_cache_key_content(right);
    e->left_snapshot = wl_columnar_relation_snapshot(left);
    e->right_snapshot = wl_columnar_relation_snapshot(right);
    e->result = result;
    e->mem_bytes = result_bytes;
    e->lru_clock = ++cache->clock;
    e->ledger_bytes = 0;
    e->identity = ++cache->next_identity;
    if (e->identity == 0)
        e->identity = ++cache->next_identity;
    e->pin_count = 0;
    e->epoch_pin_count = 0;
    e->eviction_deferred = false;
    e->pin_epoch = 0;
    e->generation = ++cache->next_generation;
    if (e->generation == 0)
        e->generation = ++cache->next_generation;
    e->owner_alive = true;
    e->owns_result = true;
    cache->total_bytes += result_bytes;

    /* Issue #1380: the cache now owns result, so its bytes move from
     * RELATION (charged while the join was producing it) to CACHE.  Detach
     * the relation's own ledger link so col_rel_destroy on eviction does
     * not credit RELATION a second time. */
    if (cache->ledger) {
        uint64_t bytes = col_rel_transport_bytes(result);
        if (result->mem_ledger) {
            col_rel_ledger_release(result);
            result->mem_ledger = NULL;
        }
        if (bytes > 0)
            wl_mem_ledger_alloc(cache->ledger, WL_MEM_SUBSYS_CACHE, bytes);
        e->ledger_bytes = bytes;
    }
    if (pin) {
        e->pin_count = 1;
        assert(cache->active_pins < UINT32_MAX);
        cache->active_pins++;
        pin->cache = cache;
        pin->identity = e->identity;
        pin->active = true;
    }
    return 0;
}

int
col_mat_cache_insert(col_mat_cache_t *cache, const col_rel_t *left,
    const col_rel_t *right, col_rel_t *result)
{
    return col_mat_cache_insert_pin(cache, left, right, result, NULL);
}

void
col_mat_cache_remove_result(col_mat_cache_t *cache, const col_rel_t *result)
{
    if (!cache || !result)
        return;
    for (uint32_t i = 0; i < cache->count; i++) {
        if (cache->entries[i].result == result) {
            if (cache->entries[i].pin_count == 0)
                mat_cache_remove_at(cache, i);
            return;
        }
    }
}

static wl_mem_reclaim_result_t
mat_cache_reclaimer(void *owner)
{
    col_mat_cache_t *cache = (col_mat_cache_t *)owner;
    wl_mem_reclaim_result_t total = { 0, 0 };
    if (!cache || !cache->reclaimer_owner_alive)
        return total;
    uint32_t index = 0;
    while (index < cache->count) {
        uint64_t generation = cache->entries[index].generation;
        wl_mem_reclaim_result_t one
            = col_mat_cache_reclaim_entry(cache, index, generation);
        if (one.candidates > 0) {
            total.bytes_released += one.bytes_released;
            total.candidates += one.candidates;
        } else {
            index++;
        }
    }
    return total;
}

int
col_mat_cache_attach_reclaimer(col_mat_cache_t *cache)
{
    if (!cache || !cache->ledger)
        return EINVAL;
    if (cache->reclaimer_handle != 0)
        return 0;
    int rc = wl_mem_ledger_register_reclaimer(cache->ledger,
            mat_cache_reclaimer, cache, &cache->reclaimer_handle);
    if (rc == 0)
        cache->reclaimer_owner_alive = true;
    return rc;
}

void
col_mat_cache_detach_reclaimer(col_mat_cache_t *cache)
{
    if (!cache)
        return;
    cache->reclaimer_owner_alive = false;
    if (cache->ledger && cache->reclaimer_handle != 0)
        wl_mem_ledger_unregister_reclaimer(cache->ledger,
            cache->reclaimer_handle);
    cache->reclaimer_handle = 0;
}

wl_mem_reclaim_result_t
col_mat_cache_reclaim_entry(col_mat_cache_t *cache, uint32_t index,
    uint64_t expected_generation)
{
    wl_mem_reclaim_result_t result = { 0, 0 };
    if (!cache || index >= cache->count)
        return result;
    col_mat_entry_t *entry = &cache->entries[index];
    if (entry->generation != expected_generation || !entry->owner_alive
        || !entry->owns_result || !entry->result || entry->pin_count != 0)
        return result;
    /* Re-read ownership and pin state immediately before removal. */
    if (entry->generation != expected_generation || !entry->owner_alive
        || entry->pin_count != 0)
        return result;
    uint64_t released = col_rel_transport_bytes(entry->result);
    mat_cache_remove_at(cache, index);
    result.bytes_released = released;
    result.candidates = 1;
    return result;
}
