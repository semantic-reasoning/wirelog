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

static uint64_t
mat_cache_fnv1a(uint64_t h, const void *data, size_t size)
{
    const unsigned char *p = (const unsigned char *)data;
    for (size_t i = 0; i < size; i++) {
        h ^= p[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

static uint64_t
mat_cache_fnv1a_key(uint64_t h, const char *key)
{
    static const unsigned char separator = 0xff;
    if (key)
        h = mat_cache_fnv1a(h, key, strlen(key));
    return mat_cache_fnv1a(h, &separator, 1);
}

uint64_t
col_mat_cache_join_signature(const wl_plan_op_t *op, bool diff_path)
{
    uint64_t h = 0xcbf29ce484222325ULL;
    if (!op)
        return 1;
    uint32_t kind = (uint32_t)op->op;
    unsigned char path = diff_path ? 1 : 0;
    h = mat_cache_fnv1a(h, &kind, sizeof(kind));
    h = mat_cache_fnv1a(h, &path, sizeof(path));
    h = mat_cache_fnv1a(h, &op->key_count, sizeof(op->key_count));
    for (uint32_t k = 0; k < op->key_count; k++) {
        h = mat_cache_fnv1a_key(h, op->left_keys ? op->left_keys[k] : NULL);
        h = mat_cache_fnv1a_key(h, op->right_keys ? op->right_keys[k] : NULL);
    }
    h = mat_cache_fnv1a(h, &op->right_filter_expr.size,
            sizeof(op->right_filter_expr.size));
    if (op->right_filter_expr.data && op->right_filter_expr.size > 0)
        h = mat_cache_fnv1a(h, op->right_filter_expr.data,
                op->right_filter_expr.size);
    return h != 0 ? h : 1;
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
            && cache->entries[i].op_signature == 0
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
    return col_mat_cache_lookup_pin_keyed(cache, left, right, 0, pin);
}

col_rel_t *
col_mat_cache_lookup_pin_keyed(col_mat_cache_t *cache, const col_rel_t *left,
    const col_rel_t *right, uint64_t op_signature, col_mat_cache_pin_t *pin)
{
    if (!cache || !left || !right || !pin)
        return NULL;
    memset(pin, 0, sizeof(*pin));
    uint64_t lh = col_mat_cache_key_content(left);
    uint64_t rh = col_mat_cache_key_content(right);
    for (uint32_t i = 0; i < cache->count; i++) {
        col_mat_entry_t *entry = &cache->entries[i];
        if (entry->left_hash == lh && entry->right_hash == rh
            && entry->op_signature == op_signature
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
    return col_mat_cache_insert_pin_keyed(cache, left, right, 0, result, pin);
}

int
col_mat_cache_insert_pin_keyed(col_mat_cache_t *cache, const col_rel_t *left,
    const col_rel_t *right, uint64_t op_signature, col_rel_t *result,
    col_mat_cache_pin_t *pin)
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
    e->op_signature = op_signature;
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
    return col_mat_cache_insert_pin_keyed(cache, left, right, 0, result,
               NULL);
}

int
col_mat_cache_insert_keyed(col_mat_cache_t *cache, const col_rel_t *left,
    const col_rel_t *right, uint64_t op_signature, col_rel_t *result)
{
    return col_mat_cache_insert_pin_keyed(cache, left, right, op_signature,
               result, NULL);
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

/* Exact pin-free recipe work. Each public progress call executes one paid
 * action: BEGIN256; acquire/cell/signature/latch/END/ACK128; source/final
 * validation192. No dynamic loop is inside an action. The meaningful BEGIN
 * initializer has 79 scalar stores including both readers and native scratch;
 * fixed preflight/ref/thread/caller work fits the remaining256 envelope.
 * END_CLEAR checks inert readers and erases six borrowed pointers (<=128).
 * Separately paid END_RESET has38 fixed stores+<=80 common work. ACK has20
 * final stores+<=96 native/inertness/funding/ref/caller work. No reset loop.
 * These are logical envelopes, not allocator/instrumentation latency bounds.
 * Fixed refusal bookkeeping and copied query are report/protocol boundaries. */
enum {
    WL_COLUMNAR_CACHE_DIGEST_BEGIN_WORK = 256,
    WL_COLUMNAR_CACHE_DIGEST_ACTION_WORK = 128,
    WL_COLUMNAR_CACHE_DIGEST_VALIDATE_WORK = 192
};
typedef char wl_columnar_cache_digest_record_bound[
        sizeof(wl_columnar_cache_digest_attempt_t) <= 768 ? 1 : -1];
typedef char wl_columnar_cache_digest_native_bound[
        sizeof(uint32_t) <= 8 ? 1 : -1];

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_cache_digest_test_checkpoint)(
    wl_columnar_cache_digest_attempt_t *, wl_evaluation_control_t *, unsigned);
#define WL_COLUMNAR_CACHE_DIGEST_HOOK(a, c, api) do { \
            if (wl_columnar_cache_digest_test_checkpoint) \
            wl_columnar_cache_digest_test_checkpoint(a, c, api); \
} while (0)
#else
#define WL_COLUMNAR_CACHE_DIGEST_HOOK(a, c, api) ((void)0)
#endif

static bool
wl_columnar_cache_digest_thread(const wl_columnar_cache_digest_attempt_t *a)
{
    if (!a->thread_valid) return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(a->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return a->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(a->owner_thread, pthread_self()) != 0;
#endif
}

static bool
wl_columnar_cache_digest_reader_inert(
    const wl_columnar_source_access_reader_t *r)
{
    return !r->owner && !r->secondary_owner && !r->identity
           && !r->thread_valid && !r->transferable && !r->acquire_stage
           && !r->release_stage;
}

static bool
wl_columnar_cache_digest_overlap(const void *a, size_t an,
    const void *b, size_t bn)
{
    if (!a || !b || !an || !bn) return false;
    uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
    if (an - 1 > UINTPTR_MAX - x || bn - 1 > UINTPTR_MAX - y) return true;
    return x <= y + bn - 1 && y <= x + an - 1;
}

static int
wl_columnar_cache_digest_enter(wl_columnar_cache_digest_attempt_t *a)
{
    if (!a) return EINVAL;
    if (a->busy) return EBUSY;
    if (a->identity != a || !a->control_ref || !a->control
        || !wl_columnar_cache_digest_thread(a)) return EINVAL;
    a->busy = true;
    return 0;
}

static int
wl_columnar_cache_digest_leave(wl_columnar_cache_digest_attempt_t *a, int rc)
{
    a->busy = false;
    return rc;
}

static void
wl_columnar_cache_digest_source_init(wl_columnar_cache_digest_source_t *s,
    const col_rel_t *relation)
{
    s->relation = relation;
    s->reader.owner = NULL; s->reader.secondary_owner = NULL;
    s->reader.identity = 0; s->reader.thread_valid = false;
    s->reader.transferable = false; s->reader.acquire_stage = 0;
    s->reader.release_stage = 0;
    s->snapshot = (col_relation_snapshot_t){0};
    s->rows = s->width = 0; s->held = false;
}

int
wl_columnar_cache_digest_begin(wl_columnar_cache_digest_attempt_t *a,
    const col_rel_t *left, const col_rel_t *right, const wl_plan_op_t *op,
    bool diff_path, wl_evaluation_control_t *control, uint64_t id)
{
    if (!a) return EINVAL;
    if (a->busy) return EBUSY;
    if (a->identity && a->identity != a) return EINVAL;
    if (a->identity || a->phase != WL_COLUMNAR_CACHE_DIGEST_INERT
        || a->control_ref || a->control || a->thread_valid
        || a->source[0].relation || a->source[1].relation || a->op
        || !wl_columnar_cache_digest_reader_inert(&a->source[0].reader)
        || !wl_columnar_cache_digest_reader_inert(&a->source[1].reader))
        return EBUSY;
    if (!control || !id) return EINVAL;
    a->busy = true;
    WL_COLUMNAR_CACHE_DIGEST_HOOK(a, control,
        WL_COLUMNAR_CACHE_DIGEST_TEST_BEGIN);
    if (wl_evaluation_control_active_attempt_id(control) != id)
        return wl_columnar_cache_digest_leave(a, EINVAL);
    int rc = wl_evaluation_control_charge_attempt(control, id,
            WL_COLUMNAR_CACHE_DIGEST_BEGIN_WORK);
    if (rc) return wl_columnar_cache_digest_leave(a, rc);
    rc = wl_evaluation_control_retain(control);
    if (rc) return wl_columnar_cache_digest_leave(a, rc);
    /* No callback/fallible operation between retention and recorded ownership. */
    a->identity = a; a->control = control; a->control_ref = true;
    a->control_identity = wl_evaluation_control_identity(control);
    a->attempt_id = id;
#if defined(WL_HAVE_C11_THREADS)
    a->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    a->owner_thread = GetCurrentThreadId();
#else
    a->owner_thread = pthread_self();
#endif
    a->thread_valid = true; a->result_saved = false;
    wl_columnar_cache_digest_source_init(&a->source[0], left);
    wl_columnar_cache_digest_source_init(&a->source[1], right);
    a->op = op;
    a->left_keys = op ? op->left_keys : NULL;
    a->right_keys = op ? op->right_keys : NULL;
    a->filter = op ? op->right_filter_expr.data : NULL;
    a->kind = op ? (uint32_t)op->op : 0;
    a->key_count = op ? op->key_count : 0;
    a->filter_size = op ? op->right_filter_expr.size : 0;
    a->path = diff_path ? 1 : 0;
    a->hash[0] = a->hash[1] = 0;
    a->signature = UINT64_C(14695981039346656037);
    a->side = a->row = a->column = a->key = a->key_side = 0;
    a->filter_byte = a->release_side = 0; a->name_byte = 0;
    for (unsigned i = 0; i < 8; ++i) a->native[i] = 0;
    a->native_count = a->native_byte = a->field = 0;
    a->semantic_result = 0;
    a->result = (wl_columnar_cache_digest_result_t){0};
    a->phase = WL_COLUMNAR_CACHE_DIGEST_ACQUIRE;
    return wl_columnar_cache_digest_leave(a, 0);
}

static int
wl_columnar_cache_digest_source_valid(
    const wl_columnar_cache_digest_source_t *s)
{
    const wl_columnar_source_access_reader_t *r = &s->reader;
    if (!s->held || !s->relation || r->identity != (uintptr_t)r
        || r->acquire_stage || r->release_stage || r->transferable
        || !wl_columnar_source_access_reader_thread_equal(r)
        || r->secondary_owner != &s->relation->descriptor_access
        || !wl_columnar_relation_snapshot_equal(s->snapshot,
        wl_columnar_relation_snapshot(s->relation))) return EINVAL;
    col_rel_t *owner = NULL;
    int rc = col_rel_storage_owner_resolve(s->relation, &owner);
    if (rc) return rc;
    if (r->owner != &owner->source_access) return EINVAL;
    uint64_t d = atomic_load_explicit(&r->secondary_owner->state,
            memory_order_acquire);
    uint64_t v = atomic_load_explicit(&r->owner->state, memory_order_acquire);
    return !d || d == WL_COLUMNAR_SOURCE_ACCESS_WRITER
           || !v || v == WL_COLUMNAR_SOURCE_ACCESS_WRITER ? EINVAL : 0;
}

static int
wl_columnar_cache_digest_fail(wl_columnar_cache_digest_attempt_t *a, int rc)
{
    if (!a->result_saved) {
        a->result_saved = true; a->semantic_result = rc;
    }
    a->phase = WL_COLUMNAR_CACHE_DIGEST_END;
    return rc;
}

static void
wl_columnar_cache_digest_next_source(wl_columnar_cache_digest_attempt_t *a)
{
    a->row = a->column = 0;
    if (!a->side) {
        a->side = 1; a->phase = WL_COLUMNAR_CACHE_DIGEST_ACQUIRE;
    } else a->phase = WL_COLUMNAR_CACHE_DIGEST_SIGNATURE_FIELD;
}

static void
wl_columnar_cache_digest_signature_byte(wl_columnar_cache_digest_attempt_t *a,
    unsigned char byte)
{
    a->signature ^= byte; a->signature *= UINT64_C(1099511628211);
}

static int
wl_columnar_cache_digest_action(wl_columnar_cache_digest_attempt_t *a)
{
    if (a->side > 1 || a->key_side > 1) return EINVAL;
    wl_columnar_cache_digest_source_t *s = &a->source[a->side];
    switch (a->phase) {
    case WL_COLUMNAR_CACHE_DIGEST_ACQUIRE: {
        if (!s->relation) {
            wl_columnar_cache_digest_next_source(a); return 0;
        }
        bool held = false;
        int rc = col_rel_source_reader_acquire_step(s->relation, &s->reader,
                &held);
        if (rc == EAGAIN) return 0;
        if (rc) return rc;
        if (held) {
            s->held = true;
            s->snapshot = wl_columnar_relation_snapshot(s->relation);
            a->phase = WL_COLUMNAR_CACHE_DIGEST_VALIDATE;
        }
        return 0;
    }
    case WL_COLUMNAR_CACHE_DIGEST_VALIDATE: {
        int rc = wl_columnar_cache_digest_source_valid(s);
        if (rc) return rc;
        s->rows = s->relation->nrows; s->width = s->relation->ncols;
        if (s->rows > s->relation->capacity
            || (s->rows && s->width && !s->relation->columns)) return EINVAL;
        if (!s->rows) {
            a->hash[a->side] = 0; wl_columnar_cache_digest_next_source(a);
        }else {
            a->hash[a->side] = UINT64_C(14695981039346656037);
            a->phase =
                s->width ? WL_COLUMNAR_CACHE_DIGEST_CELL :
                WL_COLUMNAR_CACHE_DIGEST_ROWS;
        }
        return 0;
    }
    case WL_COLUMNAR_CACHE_DIGEST_CELL: {
        uint32_t sample = s->rows < 100 ? s->rows : 100;
        if (a->row >= sample || a->column >= s->width
            || !s->relation->columns ||
            !s->relation->columns[a->column]) return EINVAL;
        a->hash[a->side] ^= (uint64_t)col_rel_get(s->relation, a->row,
                a->column);
        a->hash[a->side] *= UINT64_C(1099511628211);
        if (++a->column == s->width) {
            a->column = 0;
            if (++a->row == sample) a->phase = WL_COLUMNAR_CACHE_DIGEST_ROWS;
        }
        return 0;
    }
    case WL_COLUMNAR_CACHE_DIGEST_ROWS:
        a->hash[a->side] ^= s->rows;
        a->hash[a->side] *= UINT64_C(1099511628211);
        a->phase = WL_COLUMNAR_CACHE_DIGEST_COLUMNS; return 0;
    case WL_COLUMNAR_CACHE_DIGEST_COLUMNS:
        a->hash[a->side] ^= s->width;
        a->hash[a->side] *= UINT64_C(1099511628211);
        wl_columnar_cache_digest_next_source(a); return 0;
    case WL_COLUMNAR_CACHE_DIGEST_SIGNATURE_FIELD:
        if (!a->op) {
            a->signature = 1; a->phase = WL_COLUMNAR_CACHE_DIGEST_FINAL_LEFT;
            return 0;
        }
        switch (a->field) {
        case 0: memcpy(a->native, &a->kind, sizeof(a->kind));
            a->native_count = sizeof(a->kind); break;
        case 1: a->native[0] = a->path; a->native_count = 1; break;
        case 2: memcpy(a->native, &a->key_count, sizeof(a->key_count));
            a->native_count = sizeof(a->key_count); break;
        case 3: memcpy(a->native, &a->filter_size, sizeof(a->filter_size));
            a->native_count = sizeof(a->filter_size); break;
        default: return EINVAL;
        }
        a->native_byte = 0; a->phase = WL_COLUMNAR_CACHE_DIGEST_SIGNATURE_BYTE;
        return 0;
    case WL_COLUMNAR_CACHE_DIGEST_SIGNATURE_BYTE:
        if (a->native_byte >= a->native_count ||
            a->native_count > sizeof(a->native)) return EINVAL;
        wl_columnar_cache_digest_signature_byte(a, a->native[a->native_byte++]);
        if (a->native_byte == a->native_count) {
            if (a->field < 2) {
                ++a->field; a->phase = WL_COLUMNAR_CACHE_DIGEST_SIGNATURE_FIELD;
            }else a->phase = a->field ==
                    2 ? WL_COLUMNAR_CACHE_DIGEST_KEY :
                    WL_COLUMNAR_CACHE_DIGEST_FILTER;
        }
        return 0;
    case WL_COLUMNAR_CACHE_DIGEST_KEY: {
        if (a->key >= a->key_count) {
            a->field = 3; a->phase = WL_COLUMNAR_CACHE_DIGEST_SIGNATURE_FIELD;
            return 0;
        }
        const char *const *keys = a->key_side ? a->right_keys : a->left_keys;
        const char *name = keys ? keys[a->key] : NULL;
        if (name && (a->name_byte == SIZE_MAX
            || a->name_byte > UINTPTR_MAX - (uintptr_t)name)) return EOVERFLOW;
        unsigned char byte = name ? (unsigned char)name[a->name_byte] : 0;
        if (!byte) {
            a->phase = WL_COLUMNAR_CACHE_DIGEST_SEPARATOR; return 0;
        }
        wl_columnar_cache_digest_signature_byte(a, byte); ++a->name_byte;
        return 0;
    }
    case WL_COLUMNAR_CACHE_DIGEST_SEPARATOR:
        wl_columnar_cache_digest_signature_byte(a, 0xff);
        a->name_byte = 0;
        if (!a->key_side) a->key_side = 1;
        else {
            a->key_side = 0; ++a->key;
        }
        a->phase = WL_COLUMNAR_CACHE_DIGEST_KEY; return 0;
    case WL_COLUMNAR_CACHE_DIGEST_FILTER:
        if (!a->filter || a->filter_byte >= a->filter_size) {
            a->phase = WL_COLUMNAR_CACHE_DIGEST_FINAL_LEFT; return 0;
        }
        wl_columnar_cache_digest_signature_byte(a, a->filter[a->filter_byte++]);
        return 0;
    case WL_COLUMNAR_CACHE_DIGEST_FINAL_LEFT:
    case WL_COLUMNAR_CACHE_DIGEST_FINAL_RIGHT: {
        unsigned side = a->phase == WL_COLUMNAR_CACHE_DIGEST_FINAL_RIGHT;
        if (a->source[side].relation) {
            int rc = wl_columnar_cache_digest_source_valid(&a->source[side]);
            if (rc) return rc;
        }
        a->phase =
            side ? WL_COLUMNAR_CACHE_DIGEST_LATCH :
            WL_COLUMNAR_CACHE_DIGEST_FINAL_RIGHT;
        return 0;
    }
    case WL_COLUMNAR_CACHE_DIGEST_LATCH:
        a->result = (wl_columnar_cache_digest_result_t){
            .left_hash = a->hash[0], .right_hash = a->hash[1],
            .signature = a->signature ? a->signature : 1,
            .left_snapshot = a->source[0].snapshot,
            .right_snapshot = a->source[1].snapshot,
            .control_identity = a->control_identity, .attempt_id = a->attempt_id
        };
        a->phase = WL_COLUMNAR_CACHE_DIGEST_READY; return 0;
    default: return EINVAL;
    }
}

int
wl_columnar_cache_digest_step(wl_columnar_cache_digest_attempt_t *a,
    bool *ready)
{
    int rc = wl_columnar_cache_digest_enter(a);
    if (rc) return rc;
    if (!ready || wl_columnar_cache_digest_overlap(a, sizeof(*a), ready,
        sizeof(*ready)))
        return wl_columnar_cache_digest_leave(a, EINVAL);
    if (a->phase < WL_COLUMNAR_CACHE_DIGEST_ACQUIRE
        || a->phase >= WL_COLUMNAR_CACHE_DIGEST_READY)
        return wl_columnar_cache_digest_leave(a, EBUSY);
    if (wl_evaluation_control_active_attempt_id(a->control) != a->attempt_id)
        return wl_columnar_cache_digest_leave(a, EINVAL);
    *ready = false;
    WL_COLUMNAR_CACHE_DIGEST_HOOK(a, a->control,
        WL_COLUMNAR_CACHE_DIGEST_TEST_STEP);
    uint64_t work = a->phase == WL_COLUMNAR_CACHE_DIGEST_VALIDATE
        || (a->phase == WL_COLUMNAR_CACHE_DIGEST_FINAL_LEFT &&
        a->source[0].relation)
        || (a->phase == WL_COLUMNAR_CACHE_DIGEST_FINAL_RIGHT &&
        a->source[1].relation)
        ? WL_COLUMNAR_CACHE_DIGEST_VALIDATE_WORK :
        WL_COLUMNAR_CACHE_DIGEST_ACTION_WORK;
    rc = wl_evaluation_control_charge_attempt(a->control, a->attempt_id, work);
    if (!rc) rc = wl_columnar_cache_digest_action(a);
    if (rc) wl_columnar_cache_digest_fail(a, rc);
    else *ready = a->phase == WL_COLUMNAR_CACHE_DIGEST_READY;
    return wl_columnar_cache_digest_leave(a, rc);
}

static int
wl_columnar_cache_digest_fund(wl_columnar_cache_digest_attempt_t *a,
    wl_evaluation_control_t *funding, uint64_t id)
{
    if (!funding || !id ||
        wl_evaluation_control_identity(funding) == a->control_identity
        || wl_evaluation_control_active_attempt_id(funding) !=
        id) return EINVAL;
    return wl_evaluation_control_charge_attempt(funding, id,
               WL_COLUMNAR_CACHE_DIGEST_ACTION_WORK);
}

static int
wl_columnar_cache_digest_end(wl_columnar_cache_digest_attempt_t *a,
    int initiating, bool aborting, wl_evaluation_control_t *funding,
    uint64_t id, bool *done)
{
    if (!done || wl_columnar_cache_digest_overlap(a, sizeof(*a), done,
        sizeof(*done)))
        return EINVAL;
    if (a->phase == WL_COLUMNAR_CACHE_DIGEST_DONE) {
        *done = true; return 0;
    }
    if (!aborting && a->phase != WL_COLUMNAR_CACHE_DIGEST_READY
        && a->phase != WL_COLUMNAR_CACHE_DIGEST_END
        && a->phase != WL_COLUMNAR_CACHE_DIGEST_END_CLEAR
        && a->phase != WL_COLUMNAR_CACHE_DIGEST_END_RESET) return EBUSY;
    if (aborting && !initiating) return EINVAL;
    *done = false;
    WL_COLUMNAR_CACHE_DIGEST_HOOK(a, funding, aborting
        ? WL_COLUMNAR_CACHE_DIGEST_TEST_ABORT :
        WL_COLUMNAR_CACHE_DIGEST_TEST_FINISH);
    int rc = wl_columnar_cache_digest_fund(a, funding, id);
    if (rc) return rc;
    if (a->phase != WL_COLUMNAR_CACHE_DIGEST_END
        && a->phase != WL_COLUMNAR_CACHE_DIGEST_END_CLEAR
        && a->phase != WL_COLUMNAR_CACHE_DIGEST_END_RESET) {
        if (!a->result_saved) {
            a->semantic_result = aborting ? initiating : 0;
            a->result_saved = true;
        }
        a->phase = WL_COLUMNAR_CACHE_DIGEST_END; return 0;
    }
    if (a->phase == WL_COLUMNAR_CACHE_DIGEST_END_RESET) {
        /* END_CLEAR established inertness; no legal API can acquire a token
         * here. Leave token/held fields untouched, and retain copied result,
         * native identity, first semantic and control ownership through DONE. */
        a->source[0].snapshot = a->source[1].snapshot =
            (col_relation_snapshot_t){0};
        a->source[0].rows = a->source[0].width = a->source[1].rows =
            a->source[1].width = 0;
        a->hash[0] = a->hash[1] = a->signature = 0;
        a->kind = a->key_count = a->filter_size = 0; a->path = 0;
        a->side = a->row = a->column = a->key = a->key_side = 0;
        a->filter_byte = a->release_side = 0; a->name_byte = 0;
        a->native_count = a->native_byte = a->field = 0;
        a->native[0] = 0; a->native[1] = 0; a->native[2] = 0; a->native[3] = 0;
        a->native[4] = 0; a->native[5] = 0; a->native[6] = 0; a->native[7] = 0;
        a->phase = WL_COLUMNAR_CACHE_DIGEST_DONE; *done = true; return 0;
    }
    if (a->phase == WL_COLUMNAR_CACHE_DIGEST_END_CLEAR) {
        if (!wl_columnar_cache_digest_reader_inert(&a->source[0].reader)
            || !wl_columnar_cache_digest_reader_inert(&a->source[1].reader)
            || a->source[0].held || a->source[1].held || !a->result_saved)
            return EINVAL;
        a->source[0].relation = a->source[1].relation = NULL;
        a->op = NULL; a->left_keys = a->right_keys = NULL; a->filter = NULL;
        a->phase = WL_COLUMNAR_CACHE_DIGEST_END_RESET; return 0;
    }
    if (a->release_side >= 2) return EINVAL;
    wl_columnar_cache_digest_source_t *s = &a->source[a->release_side];
    if (s->reader.owner) {
        bool released = false;
        rc = wl_columnar_source_access_reader_release_step(&s->reader,
                &released);
        if (rc) return rc;
        if (!released) return 0;
        s->held = false;
    } else if (!wl_columnar_cache_digest_reader_inert(&s->reader) ||
        s->held) return EINVAL;
    if (++a->release_side == 2) a->phase = WL_COLUMNAR_CACHE_DIGEST_END_CLEAR;
    return 0;
}

int
wl_columnar_cache_digest_finish(wl_columnar_cache_digest_attempt_t *a,
    wl_evaluation_control_t *funding, uint64_t id, bool *done)
{
    int rc = wl_columnar_cache_digest_enter(a);
    if (rc) return rc;
    rc = wl_columnar_cache_digest_end(a, 0, false, funding, id, done);
    return wl_columnar_cache_digest_leave(a, rc);
}

int
wl_columnar_cache_digest_abort(wl_columnar_cache_digest_attempt_t *a,
    int initiating, wl_evaluation_control_t *funding, uint64_t id, bool *done)
{
    int rc = wl_columnar_cache_digest_enter(a);
    if (rc) return rc;
    rc = wl_columnar_cache_digest_end(a, initiating, true, funding, id, done);
    return wl_columnar_cache_digest_leave(a, rc);
}

int
wl_columnar_cache_digest_query(const wl_columnar_cache_digest_attempt_t *a,
    wl_columnar_cache_digest_result_t *result, int *semantic)
{
    if (!a) return EINVAL;
    if (a->busy) return EBUSY;
    if (a->identity != a || !a->control_ref ||
        !wl_columnar_cache_digest_thread(a))
        return EINVAL;
    if (a->phase != WL_COLUMNAR_CACHE_DIGEST_DONE) return EBUSY;
    if (!a->result_saved || a->source[0].held || a->source[1].held
        || a->source[0].relation || a->source[1].relation || a->op
        || !wl_columnar_cache_digest_reader_inert(&a->source[0].reader)
        || !wl_columnar_cache_digest_reader_inert(&a->source[1].reader))
        return EBUSY;
    if (!semantic || (!a->semantic_result && !result)
        || wl_columnar_cache_digest_overlap(a, sizeof(*a), semantic,
        sizeof(*semantic))
        || (result && (wl_columnar_cache_digest_overlap(a, sizeof(*a), result,
        sizeof(*result))
        || wl_columnar_cache_digest_overlap(result, sizeof(*result), semantic,
        sizeof(*semantic)))))
        return EINVAL;
    *semantic = a->semantic_result;
    if (!a->semantic_result) *result = a->result;
    return 0;
}

int
wl_columnar_cache_digest_ack(wl_columnar_cache_digest_attempt_t *a,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_cache_digest_enter(a);
    if (rc) return rc;
    if (a->phase != WL_COLUMNAR_CACHE_DIGEST_DONE
        || !a->result_saved || a->source[0].relation || a->source[1].relation ||
        a->op
        || !wl_columnar_cache_digest_reader_inert(&a->source[0].reader)
        || !wl_columnar_cache_digest_reader_inert(&a->source[1].reader)
        || a->source[0].held || a->source[1].held)
        return wl_columnar_cache_digest_leave(a, EBUSY);
    WL_COLUMNAR_CACHE_DIGEST_HOOK(a, funding,
        WL_COLUMNAR_CACHE_DIGEST_TEST_ACK);
    rc = wl_columnar_cache_digest_fund(a, funding, id);
    if (rc) return wl_columnar_cache_digest_leave(a, rc);
    wl_evaluation_control_t *owned = a->control;
    a->identity = NULL; a->control = NULL; a->control_ref = false;
    a->control_identity = a->attempt_id = 0; a->thread_valid = false;
    a->result_saved = false; a->semantic_result = 0;
    a->result = (wl_columnar_cache_digest_result_t){0};
    a->phase = WL_COLUMNAR_CACHE_DIGEST_INERT;
    wl_evaluation_control_release(owned);
    return wl_columnar_cache_digest_leave(a, 0);
}
#undef WL_COLUMNAR_CACHE_DIGEST_HOOK
