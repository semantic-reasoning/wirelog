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
#ifndef _MSC_VER
#include <stdatomic.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ARR_HEAD_ROW(value) ((uint32_t)((value)&UINT64_C(0xFFFFFFFF)))
#define ARR_HEAD_GENERATION(value) ((uint32_t)((value) >> 32))
#define ARR_MAKE_HEAD(generation, row) \
        (((uint64_t)(generation) << 32) | (uint64_t)(row))

/* ======================================================================== */
/* Arrangement Layer (Phase 3C)                                             */
/* ======================================================================== */

/*
 * arr_hash_key: legacy FNV-1a hash over key columns of a single row.
 *
 * This is the untyped arrangement API.  The 32-bit arithmetic is the
 * low-32 projection of the historical 64-bit FNV-1a chain: the low 32 bits
 * of its basis and prime are 0x84222325 and 435 respectively.
 * nbuckets MUST be a power of 2.
 */
static uint32_t
arr_hash_key(const int64_t *row, const uint32_t *key_cols, uint32_t key_count,
    uint32_t nbuckets)
{
    uint32_t h = (uint32_t)14695981039346656037ULL;
    for (uint32_t k = 0; k < key_count; k++) {
        uint64_t v = (uint64_t)row[key_cols[k]];
        for (uint32_t b = 0; b < sizeof(v); b++) {
            h ^= (uint32_t)(v & 0xffu);
            h *= 435u;
            v >>= 8;
        }
    }
    return h & (nbuckets - 1u);
}

static uint32_t
arr_hash_key_typed(const col_rel_t *rel, const int64_t *row,
    const uint32_t *key_cols, uint32_t key_count, uint32_t nbuckets)
{
    uint32_t h = (uint32_t)14695981039346656037ULL;
    for (uint32_t k = 0; k < key_count; k++) {
        uint64_t v = wl_columnar_value_key(rel, key_cols[k],
                row[key_cols[k]]);
        for (uint32_t b = 0; b < 8u; b++) {
            h ^= (uint32_t)(v & 0xffu);
            h *= 435u;
            v >>= 8;
        }
    }
    return h & (nbuckets - 1u);
}

/*
 * arr_hash_rows_batch:
 * Hash a contiguous run of rows in one call, for the arrangement build.
 *
 * Returns the low-32 value of the same legacy FNV-1a chain as arr_hash_key(),
 * after applying the typed value-key canonicalization used by
 * arr_hash_key_typed().  Typed arrangement probes must use
 * col_arrangement_find_first_typed(), so build and probe canonicalize keys
 * identically.  Callers apply `& (nbuckets - 1)` themselves.
 */
void
arr_hash_rows_batch(const col_rel_t *rel, uint32_t row_begin, uint32_t row_end,
    const uint32_t *key_cols, uint32_t key_count, uint32_t *out_hashes)
{
    if (row_begin >= row_end)
        return;

    const uint32_t n = row_end - row_begin;
    /* Keep one scalar implementation as the canonical typed path.  In
     * particular, use the low-32 legacy constants rather than the ordinary
     * 32-bit FNV constants: the arrangement contract is the low 32 bits of
     * the historical 64-bit chain. */
    for (uint32_t r = 0; r < n; r++) {
        uint32_t h = (uint32_t)14695981039346656037ULL;
        for (uint32_t k = 0; k < key_count; k++) {
            uint64_t v = wl_columnar_value_key(rel, key_cols[k],
                    rel->columns[key_cols[k]][row_begin + r]);
            for (uint32_t b = 0; b < 8u; b++) {
                h ^= (uint32_t)(v & 0xffu);
                h *= 435u;
                v >>= 8;
            }
        }
        out_hashes[r] = h;
    }
}

/*
 * arr_index_rows:
 * Hash rows [@begin, @nrows) in batches and prepend each to its bucket chain.
 *
 * Chains use a UINT32_MAX empty sentinel and store the row index in the low
 * half of a tagged bucket head.
 * Rows are inserted in ascending order exactly as the per-row loop did, so
 * chain order is preserved bucket for bucket.
 *
 * The scratch tile is on the stack (ARR_HASH_TILE * 4 bytes), so this adds no
 * allocation and no failure path to the arrangement build.
 */
static void
arr_index_rows(col_arrangement_t *arr, const col_rel_t *rel, uint32_t begin,
    uint32_t nrows, uint32_t nbuckets)
{
    uint32_t scratch[ARR_HASH_TILE];

    for (uint32_t base = begin; base < nrows;) {
        uint32_t chunk = nrows - base;
        if (chunk > ARR_HASH_TILE)
            chunk = ARR_HASH_TILE;

        arr_hash_rows_batch(rel, base, base + chunk, arr->key_cols,
            arr->key_count, scratch);

        for (uint32_t j = 0; j < chunk; j++) {
            uint32_t row = base + j;
            uint32_t bucket = scratch[j] & (nbuckets - 1);
            uint64_t head = arr->ht_head[bucket];
            if (ARR_HEAD_GENERATION(head) != arr->generation)
                head = ARR_MAKE_HEAD(arr->generation, UINT32_MAX);
            arr->ht_next[row] = ARR_HEAD_ROW(head);
            arr->ht_head[bucket] = ARR_MAKE_HEAD(arr->generation, row);
        }

        /* Advance by the clamped chunk so base cannot wrap past UINT32_MAX. */
        base += chunk;
    }
}

static bool
arr_key_cols_valid(const col_rel_t *rel, const uint32_t *key_cols,
    uint32_t key_count)
{
    if (!rel || !key_cols || key_count == 0)
        return false;
    for (uint32_t k = 0; k < key_count; k++) {
        if (key_cols[k] >= rel->ncols)
            return false;
    }
    return true;
}

/*
 * Round n up to the next power of 2; minimum 16.
 *
 * Returns 0 -- not a saturated value -- for every n above 0x80000000: the
 * smear fills to 0xFFFFFFFF and `n + 1u` wraps.  Callers must reject that
 * themselves; both in this file do, below.  The byte-identical
 * wl_columnar_filter_next_pow2 in filter.c has the same return and more
 * callers.  See follow-up issue #1192 for the remaining queue-capacity
 * arithmetic paths.
 */
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

static bool
arr_memory_bytes(const col_arrangement_t *arr, size_t *out)
{
    size_t head_bytes = (size_t)arr->nbuckets * sizeof(uint64_t);
    size_t next_bytes = (size_t)arr->ht_cap * sizeof(uint32_t);
    if ((arr->nbuckets != 0
        && head_bytes / sizeof(uint64_t) != arr->nbuckets)
        || (arr->ht_cap != 0
        && next_bytes / sizeof(uint32_t) != arr->ht_cap)
        || head_bytes > SIZE_MAX - next_bytes)
        return false;
    *out = head_bytes + next_bytes;
    return true;
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
    arr->generation = 0;
}

/* Full rebuild: index all nrows rows in rel into arr. */
static int
arr_build_full(col_arrangement_t *arr, const col_rel_t *rel)
{
    uint32_t nrows = rel->nrows;
    uint32_t nbuckets = arr_next_pow2(nrows > 0 ? nrows * 2u : 16u);

    /*
     * arr_next_pow2 returns 0 above 0x80000000, so nbuckets is 0 for nrows
     * in [0x40000001, 0x7FFFFFFF] and again in [0xC0000001, 0xFFFFFFFF] --
     * 2147483646 row counts, about half the 32-bit space.  Zero would then
     * pass the "size changed" test below against a zeroed arrangement, skip
     * the allocation, and leave arr_index_rows to load ht_head[hash & ~0u]
     * through a NULL pointer.
     *
     * Test the bucket count, not the row count.  Beyond being this file's
     * own invalid-arrangement predicate -- col_arrangement_find_first uses
     * `arr->nbuckets == 0`, where `< 16u` would invent a second and
     * unstated minimum -- it is the form the analyzer can use.  It clears
     * the memset below when non-zero-ness is established by a test on the
     * value flowing into it, or on its producer's output, and not when the
     * test is on the producer's input domain.  So the same guard written
     * against nrows here still reports the diagnostic, as does making
     * arr_next_pow2 saturate by rejecting its input; a saturation applied
     * after the smear, which constrains what it returns, does clear it.
     * A primitive-only fix was available; this one is preferred because it
     * rejects the relation rather than silently resizing it.
     *
     * The domain left open is deliberate.  Row counts in
     * [0x80000000, 0xC0000000] still build here, degenerately -- nbuckets
     * floors at 16 for two billion rows -- and that is memory-safe: the
     * chain sizing below takes max(nrows, ht_cap * 2), which never lands
     * under nrows, unlike the bare nrows * 2u in arr_update_incremental.
     * Unifying the two guards on row count measures one diagnostic, so the
     * asymmetry with that function is required rather than an oversight.
     *
     * ENOMEM is reused deliberately: the relation is unrepresentable rather
     * than out of memory, and EOVERFLOW would say so.  It is not that
     * EOVERFLOW is unavailable -- join.c, which consumes these
     * arrangements, returns it as rc=84 in nine places.  It is that all
     * seven call sites of this function and arr_update_incremental discard
     * the value and collapse to NULL, so the distinction could never reach
     * a caller.
     */
    if (nbuckets == 0)
        return ENOMEM;

    uint64_t *new_head = NULL;
    uint32_t *new_next = NULL;
    uint32_t new_cap = arr->ht_cap;

    /* Prepare every potentially failing allocation before publishing any
     * part of the new arrangement.  This keeps a failed rebuild usable. */
    if (nbuckets != arr->nbuckets) {
        size_t head_bytes = (size_t)nbuckets * sizeof(uint64_t);
        if (nbuckets != 0 && head_bytes / sizeof(uint64_t) != nbuckets)
            return ENOMEM;
        new_head = (uint64_t *)malloc(head_bytes);
        if (!new_head)
            return ENOMEM;
        /* Zero has generation zero, which cannot match a live generation. */
        memset(new_head, 0, head_bytes);
    }
    /* Grow chain array if needed. */
    if (nrows > arr->ht_cap) {
        uint32_t doubled = arr->ht_cap > UINT32_MAX / 2u
            ? UINT32_MAX : arr->ht_cap * 2u;
        new_cap = nrows > doubled ? nrows : doubled;
        if (new_cap < 16u)
            new_cap = 16u;
        size_t next_bytes = (size_t)new_cap * sizeof(uint32_t);
        if (new_cap != 0 && next_bytes / sizeof(uint32_t) != new_cap) {
            free(new_head);
            return ENOMEM;
        }
        new_next = (uint32_t *)malloc(next_bytes);
        if (!new_next) {
            free(new_head);
            return ENOMEM;
        }
    }

    if (new_head) {
        free(arr->ht_head);
        arr->ht_head = new_head;
        arr->nbuckets = nbuckets;
    }
    if (new_next) {
        free(arr->ht_next);
        arr->ht_next = new_next;
        arr->ht_cap = new_cap;
    }

    /* Start a new epoch.  Only wraparound needs a full clear; normal rebuilds
     * lazily replace each bucket head as that bucket receives its first row. */
    if (arr->generation == UINT32_MAX) {
        memset(arr->ht_head, 0, nbuckets * sizeof(uint64_t));
        arr->generation = 1;
    } else {
        arr->generation++;
        if (arr->generation == 0)
            arr->generation = 1;
    }

    arr_index_rows(arr, rel, 0, nrows, nbuckets);
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

    /*
     * Both sizes below are nrows * 2, which wraps for nrows >= 0x80000000 --
     * every row count in the top half of the range.  The chain array is the
     * dangerous one: the wrapped product is 0, the 16-entry floor rounds it
     * up, and arr_index_rows writes past a 64-byte allocation.  The
     * arr_build_full guard does not cover it, because arr_next_pow2(0) is 16
     * and matches the arrangement's existing bucket count, so the load-factor
     * test below never diverts to the rebuild.
     *
     * This refuses more than the unsafe range, deliberately.  Where nrows is
     * in [0x80000000, 0xC0000000] and `needed` differs from arr->nbuckets,
     * the call used to divert to arr_build_full and complete safely, and is
     * now rejected.  What that costs is a relation of at least 2^31 rows --
     * some 34 GB of backing store across two columns -- whose success was a
     * 16-bucket table with 134-million-entry chains.  One test on the row
     * count, ahead of both products, is worth more than preserving it.
     *
     * ENOMEM is reused for the reason given in arr_build_full.
     */
    if (nrows > UINT32_MAX / 2u)
        return ENOMEM;

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

    uint32_t nb = arr->nbuckets;
    /* Extends a cached arrangement in place: rows indexed here are probed
     * against rows indexed by earlier calls, so these buckets must match what
     * arr_hash_key() computes on the probe side.  See arr_hash_rows_batch. */
    arr_index_rows(arr, rel, old_nrows, nrows, nb);
    arr->indexed_rows = nrows;
    return 0;
}

/* ======================================================================== */
/* Arrangement Cache LRU Eviction (Issue #216)                              */
/* ======================================================================== */

/*
 * col_arr_cache_evict_lru: evict arrangement cache entries until arr_total_bytes
 * drops below target_bytes, or until no more evictable entries remain.
 *
 * Priority:
 *   1. indexed_rows == 0 (already tombstoned — zero memory cost, but frees slot)
 *   2. smallest lru_clock (least recently used)
 *
 * Eviction is a tombstone: arr_free_contents clears ht_head/ht_next and resets
 * indexed_rows to 0.  The slot (rel_name, key_cols) is retained so the next
 * access can rebuild without re-allocating the entry.
 */
static void
col_arr_cache_evict_lru(wl_col_session_t *cs, size_t target_bytes)
{
    /* Keep evicting until we meet the target or have nothing left to evict. */
    for (;;) {
        if (cs->arr_total_bytes <= target_bytes)
            break;

        /* Find the best eviction candidate. */
        int victim = -1;
        uint64_t min_clock = UINT64_MAX;

        for (uint32_t i = 0; i < cs->arr_count; i++) {
            col_arr_entry_t *e = &cs->arr_entries[i];
            if (e->mem_bytes == 0)
                continue; /* already tombstoned, nothing to free */

            /* Prefer indexed_rows == 0 (invalidated) entries first. */
            if (e->arr.indexed_rows == 0) {
                victim = (int)i;
                break;
            }

            if (e->lru_clock < min_clock) {
                min_clock = e->lru_clock;
                victim = (int)i;
            }
        }

        if (victim < 0)
            break; /* nothing evictable */

        col_arr_entry_t *e = &cs->arr_entries[victim];
        cs->arr_total_bytes -= e->mem_bytes;
        arr_free_contents(&e->arr);
        e->mem_bytes = 0;
    }
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
    if (!arr_key_cols_valid(rel, key_cols, key_count))
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
        if (e->arr.indexed_rows == 0) {
            /* Deduct stale bytes before rebuild; restore on failure. */
            cs->arr_total_bytes -= e->mem_bytes;
            if (arr_build_full(&e->arr, rel) != 0) {
                cs->arr_total_bytes += e->mem_bytes;
                return NULL;
            }
            if (!arr_memory_bytes(&e->arr, &e->mem_bytes)) {
                arr_free_contents(&e->arr);
                e->mem_bytes = 0;
                return NULL;
            }
            cs->arr_total_bytes += e->mem_bytes;
        } else if (e->arr.indexed_rows < rel->nrows) {
            uint32_t old = e->arr.indexed_rows;
            cs->arr_total_bytes -= e->mem_bytes;
            if (arr_update_incremental(&e->arr, rel, old) != 0) {
                cs->arr_total_bytes += e->mem_bytes; /* restore on failure */
                return NULL;
            }
            if (!arr_memory_bytes(&e->arr, &e->mem_bytes)) {
                arr_free_contents(&e->arr);
                e->mem_bytes = 0;
                return NULL;
            }
            cs->arr_total_bytes += e->mem_bytes;
        }
        /* Bump LRU clock on every access. */
        e->lru_clock = ++cs->arr_clock;
        return &e->arr;
    }

    /* Not found: evict if count or byte limits exceeded. */
    if (cs->arr_count >= COL_ARR_CACHE_MAX
        || cs->arr_total_bytes >= cs->arr_cache_limit_bytes) {
        /* Target: drop to 75% of limit to amortize eviction overhead. */
        size_t target = (cs->arr_cache_limit_bytes / 4u) * 3u;
        col_arr_cache_evict_lru(cs, target);
    }

    /* Prefer reusing a tombstone slot to keep arr_count bounded at
     * COL_ARR_CACHE_MAX.  A tombstone has indexed_rows == 0 and mem_bytes == 0
     * (hash tables freed); rel_name/key_cols are still valid and must be
     * replaced below. */
    uint32_t slot = cs->arr_count; /* default: append */
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (cs->arr_entries[i].arr.indexed_rows == 0
            && cs->arr_entries[i].mem_bytes == 0) {
            slot = i;
            break;
        }
    }

    if (slot == cs->arr_count) {
        /* No tombstone available: grow the registry. */
        if (cs->arr_count >= cs->arr_cap) {
            uint32_t new_cap = cs->arr_cap ? cs->arr_cap * 2u : 8u;
            col_arr_entry_t *ne = (col_arr_entry_t *)realloc(
                cs->arr_entries, new_cap * sizeof(col_arr_entry_t));
            if (!ne)
                return NULL;
            cs->arr_entries = ne;
            cs->arr_cap = new_cap;
        }
        cs->arr_count++;
    } else {
        /* Reuse tombstone: free its old ownership before overwriting. */
        free(cs->arr_entries[slot].rel_name);
        free(cs->arr_entries[slot].key_cols);
    }

    col_arr_entry_t *e = &cs->arr_entries[slot];
    memset(e, 0, sizeof(*e));

    e->rel_name = wl_strdup(rel_name);
    if (!e->rel_name) {
        if (slot == cs->arr_count - 1)
            cs->arr_count--; /* roll back append */
        return NULL;
    }

    e->key_cols = (uint32_t *)malloc(key_count * sizeof(uint32_t));
    if (!e->key_cols) {
        free(e->rel_name);
        memset(e, 0, sizeof(*e));
        if (slot == cs->arr_count - 1)
            cs->arr_count--;
        return NULL;
    }
    memcpy(e->key_cols, key_cols, key_count * sizeof(uint32_t));
    e->key_count = key_count;
    e->arr.key_cols = e->key_cols; /* shared view; key_cols owned by entry */
    e->arr.key_count = key_count;

    /* Initial build. */
    if (arr_build_full(&e->arr, rel) != 0) {
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        if (slot == cs->arr_count - 1)
            cs->arr_count--;
        return NULL;
    }
    if (!arr_memory_bytes(&e->arr, &e->mem_bytes)) {
        arr_free_contents(&e->arr);
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        if (slot == cs->arr_count - 1)
            cs->arr_count--;
        return NULL;
    }
    cs->arr_total_bytes += e->mem_bytes;
    e->lru_clock = ++cs->arr_clock;
    return &e->arr;
}

uint32_t
col_arrangement_find_first(const col_arrangement_t *arr,
    int64_t *const *columns, uint32_t rel_ncols,
    const int64_t *key_row)
{
    if (!arr || !columns || !key_row || arr->nbuckets == 0
        || arr->indexed_rows == 0)
        return UINT32_MAX;

    (void)rel_ncols; /* used for bounds checking in debug builds */

    uint32_t bucket
        = arr_hash_key(key_row, arr->key_cols, arr->key_count, arr->nbuckets);
    uint64_t head = arr->ht_head[bucket];
    uint32_t row = ARR_HEAD_GENERATION(head) == arr->generation
        ? ARR_HEAD_ROW(head) : UINT32_MAX;
    while (row != UINT32_MAX) {
        bool match = true;
        for (uint32_t k = 0; k < arr->key_count; k++) {
            if (columns[arr->key_cols[k]][row]
                != key_row[arr->key_cols[k]]) {
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
col_arrangement_find_first_typed(const col_arrangement_t *arr,
    const col_rel_t *rel, const int64_t *key_row)
{
    if (!arr || !rel || !rel->columns || !key_row || arr->nbuckets == 0
        || arr->indexed_rows == 0)
        return UINT32_MAX;

    uint32_t bucket = arr_hash_key_typed(rel, key_row, arr->key_cols,
            arr->key_count, arr->nbuckets);
    uint64_t head = arr->ht_head[bucket];
    uint32_t row = ARR_HEAD_GENERATION(head) == arr->generation
        ? ARR_HEAD_ROW(head) : UINT32_MAX;
    while (row != UINT32_MAX) {
        bool match = true;
        for (uint32_t k = 0; k < arr->key_count; k++) {
            uint32_t col = arr->key_cols[k];
            if (!wl_columnar_value_equal(rel, col, rel->columns[col][row],
                rel, col, key_row[col])) {
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

    /* Issue #433: Also invalidate filtered arrangement cache entries.
     * When a relation changes, its cached filtered arrangement is stale. */
    for (uint32_t i = 0; i < cs->filt_arr_count; i++) {
        if (strcmp(cs->filt_arr_entries[i].rel_name, rel_name) == 0)
            cs->filt_arr_entries[i].arr.indexed_rows = 0;
    }

    /* Issue #275: Also invalidate differential arrangement cache.
     * After consolidation permutes physical row order, row-index-based
     * hash tables in diff_arr become stale. Reset indexed_rows and
     * hash table state to force re-indexing on next access. */
    for (uint32_t i = 0; i < cs->diff_arr_count; i++) {
        if (strcmp(cs->diff_arr_entries[i].rel_name, rel_name) == 0) {
            col_diff_arrangement_t *darr = cs->diff_arr_entries[i].diff_arr;
            if (darr) {
                darr->indexed_rows = 0;
                darr->base_nrows = 0;
                darr->current_nrows = 0;
                if (darr->ht_head) {
                    memset(darr->ht_head, 0, darr->nbuckets * sizeof(uint32_t));
                }
            }
        }
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
    if (!arr_key_cols_valid(delta_rel, key_cols, key_count))
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

/* ======================================================================== */
/* Filtered Arrangement Cache (Issue #433)                                  */
/* ======================================================================== */

/*
 * col_session_get_filt_arrangement:
 *
 * Return (or lazily create) a persistent arrangement for `filtered_rel`
 * keyed on `key_cols[0..key_count)`.  The entry is keyed by
 * (rel_name, filter_hash, key_cols[]) and persists across semi-naive
 * sub-passes (unlike darr_entries which are cleared per sub-pass).
 *
 * Stale detection: if `e->arr.indexed_rows != filtered_rel->nrows`, the
 * arrangement is rebuilt.  This handles the case where filt_cache rebuilt
 * the filtered relation because the source EDB grew.
 *
 * Returns NULL on allocation failure or if key_count == 0.
 */
col_arrangement_t *
col_session_get_filt_arrangement(wl_col_session_t *cs, const char *rel_name,
    uint64_t filter_hash, const col_rel_t *filtered_rel,
    const uint32_t *key_cols, uint32_t key_count)
{
    if (!cs || !rel_name || !filtered_rel || !key_cols || key_count == 0)
        return NULL;
    if (!arr_key_cols_valid(filtered_rel, key_cols, key_count))
        return NULL;

    /* Search existing entries. */
    for (uint32_t i = 0; i < cs->filt_arr_count; i++) {
        col_filt_arr_entry_t *e = &cs->filt_arr_entries[i];
        if (e->filter_hash != filter_hash || e->key_count != key_count)
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
        /* Found: rebuild if stale (filtered_rel grew since last build). */
        if (e->arr.indexed_rows != filtered_rel->nrows) {
            arr_free_contents(&e->arr);
            if (filtered_rel->nrows > 0
                && arr_build_full(&e->arr, filtered_rel) != 0)
                return NULL;
        }
        return &e->arr;
    }

    /* Not found: grow cache and create new entry. */
    if (cs->filt_arr_count >= cs->filt_arr_cap) {
        uint32_t new_cap = cs->filt_arr_cap ? cs->filt_arr_cap * 2u : 4u;
        col_filt_arr_entry_t *ne = (col_filt_arr_entry_t *)realloc(
            cs->filt_arr_entries, new_cap * sizeof(col_filt_arr_entry_t));
        if (!ne)
            return NULL;
        cs->filt_arr_entries = ne;
        cs->filt_arr_cap = new_cap;
    }

    col_filt_arr_entry_t *e = &cs->filt_arr_entries[cs->filt_arr_count];
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
    e->filter_hash = filter_hash;
    e->arr.key_cols = e->key_cols; /* shared view; owned by entry */
    e->arr.key_count = key_count;
    cs->filt_arr_count++;

    if (filtered_rel->nrows > 0
        && arr_build_full(&e->arr, filtered_rel) != 0) {
        cs->filt_arr_count--;
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    return &e->arr;
}

/*
 * col_session_free_filt_arrangements:
 *
 * Free all entries in the filtered arrangement cache and reset the cache.
 * Called from col_session_destroy.
 */
void
col_session_free_filt_arrangements(wl_col_session_t *cs)
{
    if (!cs)
        return;
    for (uint32_t i = 0; i < cs->filt_arr_count; i++) {
        free(cs->filt_arr_entries[i].rel_name);
        free(cs->filt_arr_entries[i].key_cols);
        arr_free_contents(&cs->filt_arr_entries[i].arr);
    }
    free(cs->filt_arr_entries);
    cs->filt_arr_entries = NULL;
    cs->filt_arr_count = 0;
    cs->filt_arr_cap = 0;
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

    for (uint32_t r = 0; r < rel->nrows; r++)
        col_rel_row_copy_out(rel, r, sarr->sorted + (size_t)r * rel->ncols);
    /* #465: stable LSD radix sort by the single key column replaces the
     * platform-specific qsort_r path that did not exist on Android NDK
     * bionic libc.  Failure here is malloc-fail in the radix scratch
     * buffer; on that path sarr->sorted is unmodified and we propagate
     * ENOMEM so the caller can react. */
    if (col_radix_sort_rows_by_key(sarr->sorted, rel->nrows, rel->ncols,
        key_col) != 0) {
        free(sarr->sorted);
        sarr->sorted = NULL;
        return ENOMEM;
    }
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

/* ======================================================================== */
/* Differential Arrangement Registry (Issue #263)                           */
/* ======================================================================== */

/*
 * col_session_get_diff_arrangement:
 *
 * Return (or lazily create) a differential arrangement for `rel_name`
 * keyed on `key_cols[0..key_count)`. The arrangement persists across
 * iterations within an epoch, enabling incremental hash index reuse.
 *
 * Returns NULL on allocation failure.
 */
col_diff_arrangement_t *
col_session_get_diff_arrangement(wl_col_session_t *cs, const char *rel_name,
    const uint32_t *key_cols, uint32_t key_count)
{
    if (!cs || !rel_name || key_count == 0)
        return NULL;

    /* Search existing entries. */
    for (uint32_t i = 0; i < cs->diff_arr_count; i++) {
        col_diff_arr_entry_t *e = &cs->diff_arr_entries[i];
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
        if (match)
            return e->diff_arr;
    }

    /* Not found: grow registry and create new entry. */
    if (cs->diff_arr_count >= cs->diff_arr_cap) {
        uint32_t new_cap = cs->diff_arr_cap ? cs->diff_arr_cap * 2u : 4u;
        col_diff_arr_entry_t *ne = (col_diff_arr_entry_t *)realloc(
            cs->diff_arr_entries, new_cap * sizeof(col_diff_arr_entry_t));
        if (!ne)
            return NULL;
        cs->diff_arr_entries = ne;
        cs->diff_arr_cap = new_cap;
    }

    col_diff_arr_entry_t *e = &cs->diff_arr_entries[cs->diff_arr_count];
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

    e->diff_arr = col_diff_arrangement_create(key_cols, key_count, 0);
    if (!e->diff_arr) {
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    cs->diff_arr_count++;
    return e->diff_arr;
}

/*
 * col_session_free_diff_arrangements:
 *
 * Free all entries in the differential arrangement registry and reset it.
 * Called from col_session_destroy and when switching evaluation modes.
 */
void
col_session_free_diff_arrangements(wl_col_session_t *cs)
{
    if (!cs)
        return;
    for (uint32_t i = 0; i < cs->diff_arr_count; i++) {
        free(cs->diff_arr_entries[i].rel_name);
        free(cs->diff_arr_entries[i].key_cols);
        col_diff_arrangement_destroy(cs->diff_arr_entries[i].diff_arr);
    }
    free(cs->diff_arr_entries);
    cs->diff_arr_entries = NULL;
    cs->diff_arr_count = 0;
    cs->diff_arr_cap = 0;
}

/*
 * col_diff_arr_entry_clone - Deep-copy a single differential arrangement entry.
 *
 * Clones rel_name, key_cols, and the diff_arrangement for a K-fusion worker (#274).
 * On success, dst owns all allocations. On failure, dst is zeroed.
 *
 * Returns: 0 on success, ENOMEM on allocation failure.
 */
static int
col_diff_arr_entry_clone(const col_diff_arr_entry_t *src,
    col_diff_arr_entry_t *dst)
{
    memset(dst, 0, sizeof(*dst));

    if (!src || !src->rel_name)
        return ENOMEM;

    dst->rel_name = wl_strdup(src->rel_name);
    if (!dst->rel_name)
        return ENOMEM;

    if (src->key_count > 0) {
        dst->key_cols = (uint32_t *)malloc(src->key_count * sizeof(uint32_t));
        if (!dst->key_cols) {
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->key_cols, src->key_cols, src->key_count * sizeof(uint32_t));
    }
    dst->key_count = src->key_count;

    if (src->diff_arr) {
        dst->diff_arr = col_diff_arrangement_deep_copy(src->diff_arr);
        if (!dst->diff_arr) {
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
    }

    return 0;
}

/*
 * col_diff_arr_entries_clone - Deep-copy a differential arrangement registry (#274).
 *
 * Creates an independent copy of `count` entries for a K-fusion worker.
 * On success, *out_entries owns all allocations and *out_cap equals count.
 * On failure, *out_entries is NULL.
 *
 * Returns: 0 on success, ENOMEM on allocation failure.
 */
int
col_diff_arr_entries_clone(const col_diff_arr_entry_t *src, uint32_t count,
    col_diff_arr_entry_t **out_entries, uint32_t *out_cap)
{
    *out_entries = NULL;
    *out_cap = 0;

    if (count == 0)
        return 0;

    col_diff_arr_entry_t *cloned
        = (col_diff_arr_entry_t *)calloc(count, sizeof(col_diff_arr_entry_t));
    if (!cloned)
        return ENOMEM;

    for (uint32_t i = 0; i < count; i++) {
        int clone_rc = col_diff_arr_entry_clone(&src[i], &cloned[i]);
        if (clone_rc != 0) {
            for (uint32_t j = 0; j < i; j++) {
                free(cloned[j].rel_name);
                free(cloned[j].key_cols);
                col_diff_arrangement_destroy(cloned[j].diff_arr);
            }
            free(cloned);
            return clone_rc;
        }
    }

    *out_entries = cloned;
    *out_cap = count;
    return 0;
}
