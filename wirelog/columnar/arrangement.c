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

static void diff_entry_reset(col_diff_arr_entry_t *entry);

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
/*
 * arr_ledger_bytes: current ht_head + ht_next footprint for accounting;
 * 0 when the size is not representable (arr_memory_bytes already rejects
 * such tables at build time).
 */
static uint64_t
arr_ledger_bytes(const col_arrangement_t *arr)
{
    size_t bytes = 0;
    if (!arr || !arr_memory_bytes(arr, &bytes))
        return 0;
    return (uint64_t)bytes;
}

static bool
arr_reserve_bytes(col_arrangement_t *arr, uint64_t bytes,
    wl_columnar_memory_reservation_t *pending)
{
    wl_columnar_memory_governor_t *governor;

    if (!arr || !pending || bytes == 0)
        return bytes == 0;
    governor = wl_columnar_memory_governor_ref_get(arr->memory_governor);
    if (!governor)
        return true;
    wl_columnar_memory_reservation_init(pending);
    if (!wl_columnar_memory_reserve(governor, bytes, pending)
        || !wl_columnar_memory_commit(pending, arr)) {
        (void)wl_columnar_memory_release(pending);
        return false;
    }
    return true;
}

static bool
arr_publish_reservation(col_arrangement_t *arr,
    wl_columnar_memory_reservation_t *pending, uint64_t bytes)
{
    if (!arr || !arr->memory_governor)
        return true;
    if (bytes == arr->reserved_bytes) {
        return wl_columnar_memory_release(pending);
    }
    wl_columnar_memory_reservation_t previous;
    wl_columnar_memory_reservation_init(&previous);
    uint64_t old_state = atomic_load_explicit(&arr->reservation.state,
            memory_order_acquire);
    bool old_active = old_state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        || old_state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED;
    if (old_active
        && !wl_columnar_memory_reservation_move(&previous,
        &arr->reservation)) {
        (void)wl_columnar_memory_release(pending);
        return false;
    }
    if (!wl_columnar_memory_reservation_move(&arr->reservation, pending)) {
        (void)wl_columnar_memory_release(pending);
        if (old_active)
            (void)wl_columnar_memory_reservation_move(&arr->reservation,
                &previous);
        return false;
    }
    if (old_active && !wl_columnar_memory_release(&previous)) {
        wl_columnar_memory_reservation_t replacement;
        wl_columnar_memory_reservation_init(&replacement);
        (void)wl_columnar_memory_reservation_move(&replacement,
            &arr->reservation);
        (void)wl_columnar_memory_reservation_move(&arr->reservation,
            &previous);
        (void)wl_columnar_memory_release(&replacement);
        return false;
    }
    arr->reserved_bytes = bytes;
    return true;
}

/* Registry entries embed non-copyable reservation tokens.  A flat-array
 * relocation must move each token deliberately; realloc would leave identity
 * pointing into the old array and lose accounting. */
static bool
arr_entry_relocate(col_arr_entry_t *dst, col_arr_entry_t *src)
{
    uint64_t state = atomic_load_explicit(&src->arr.reservation.state,
            memory_order_acquire);
    if (src->arr.reservation.identity != &src->arr.reservation)
        return false;
    memcpy(dst, src, sizeof(*dst));
    wl_columnar_memory_reservation_init(&dst->arr.reservation);
    dst->arr.key_cols = dst->key_cols;
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
        return true;
    if (state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        return false;
    if (!wl_columnar_memory_reservation_move(&dst->arr.reservation,
        &src->arr.reservation))
        return false;
    state = atomic_load_explicit(&dst->arr.reservation.state,
            memory_order_acquire);
    if ((state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        || state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        && !wl_columnar_memory_transfer(&dst->arr.reservation, &dst->arr)) {
        (void)wl_columnar_memory_reservation_move(
            &src->arr.reservation, &dst->arr.reservation);
        return false;
    }
    return true;
}

static bool
filt_arr_entry_relocate(col_filt_arr_entry_t *dst, col_filt_arr_entry_t *src)
{
    uint64_t state = atomic_load_explicit(&src->arr.reservation.state,
            memory_order_acquire);
    if (src->arr.reservation.identity != &src->arr.reservation)
        return false;
    memcpy(dst, src, sizeof(*dst));
    wl_columnar_memory_reservation_init(&dst->arr.reservation);
    dst->arr.key_cols = dst->key_cols;
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
        return true;
    if (state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        return false;
    if (!wl_columnar_memory_reservation_move(&dst->arr.reservation,
        &src->arr.reservation))
        return false;
    state = atomic_load_explicit(&dst->arr.reservation.state,
            memory_order_acquire);
    if ((state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        || state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        && !wl_columnar_memory_transfer(&dst->arr.reservation, &dst->arr)) {
        (void)wl_columnar_memory_reservation_move(
            &src->arr.reservation, &dst->arr.reservation);
        return false;
    }
    return true;
}

static bool
arr_entries_grow(col_arr_entry_t **entries, uint32_t *capacity,
    uint32_t count, uint32_t new_capacity)
{
    col_arr_entry_t *old = *entries;
    col_arr_entry_t *grown = (col_arr_entry_t *)calloc(new_capacity,
            sizeof(*grown));
    if (!grown)
        return false;
    for (uint32_t i = 0; i < count; i++) {
        if (!arr_entry_relocate(&grown[i], &old[i])) {
            for (uint32_t j = 0; j < i; j++) {
                if (wl_columnar_memory_reservation_move(
                        &old[j].arr.reservation,
                        &grown[j].arr.reservation))
                    (void)wl_columnar_memory_transfer(
                        &old[j].arr.reservation, &old[j].arr);
            }
            free(grown);
            return false;
        }
    }
    free(old);
    *entries = grown;
    *capacity = new_capacity;
    return true;
}

static bool
filt_arr_entries_grow(col_filt_arr_entry_t **entries, uint32_t *capacity,
    uint32_t count, uint32_t new_capacity)
{
    col_filt_arr_entry_t *old = *entries;
    col_filt_arr_entry_t *grown = (col_filt_arr_entry_t *)calloc(new_capacity,
            sizeof(*grown));
    if (!grown)
        return false;
    for (uint32_t i = 0; i < count; i++) {
        if (!filt_arr_entry_relocate(&grown[i], &old[i])) {
            for (uint32_t j = 0; j < i; j++) {
                if (wl_columnar_memory_reservation_move(
                        &old[j].arr.reservation,
                        &grown[j].arr.reservation))
                    (void)wl_columnar_memory_transfer(
                        &old[j].arr.reservation, &old[j].arr);
            }
            free(grown);
            return false;
        }
    }
    free(old);
    *entries = grown;
    *capacity = new_capacity;
    return true;
}

/*
 * arr_ledger_sync: charge or credit the difference between @before and the
 * arrangement's current footprint under ARRANGEMENT (Issue #1380).
 */
static void
arr_ledger_sync(col_arrangement_t *arr, uint64_t before)
{
    if (!arr || !arr->ledger)
        return;
    uint64_t after = arr_ledger_bytes(arr);
    if (after > before)
        wl_mem_ledger_alloc(arr->ledger, WL_MEM_SUBSYS_ARRANGEMENT,
            after - before);
    else if (before > after)
        wl_mem_ledger_free(arr->ledger, WL_MEM_SUBSYS_ARRANGEMENT,
            before - after);
}

void
col_arr_attach_ledger(col_arrangement_t *arr, wl_mem_ledger_t *ledger)
{
    if (!arr || arr->ledger || !ledger)
        return;
    arr->ledger = ledger;
    arr_ledger_sync(arr, 0);
}

void
col_arr_attach_memory_governor(col_arrangement_t *arr,
    wl_columnar_memory_governor_ref_t *memory_governor)
{
    if (!arr || arr->memory_governor || !memory_governor)
        return;
    arr->memory_governor = memory_governor;
    wl_columnar_memory_governor_ref_retain(memory_governor);
}

void
col_arr_detach_memory_governor(col_arrangement_t *arr)
{
    if (!arr)
        return;
    (void)wl_columnar_memory_release(&arr->reservation);
    arr->reserved_bytes = 0;
    wl_columnar_memory_governor_ref_release(arr->memory_governor);
    arr->memory_governor = NULL;
}

void
arr_free_contents(col_arrangement_t *arr)
{
    if (!arr)
        return;
    if (arr->ledger) {
        uint64_t bytes = arr_ledger_bytes(arr);
        if (bytes > 0)
            wl_mem_ledger_free(arr->ledger, WL_MEM_SUBSYS_ARRANGEMENT,
                bytes);
    }
    free(arr->ht_head);
    free(arr->ht_next);
    (void)wl_columnar_memory_release(&arr->reservation);
    arr->reserved_bytes = 0;
    arr->ht_head = NULL;
    arr->ht_next = NULL;
    arr->nbuckets = 0;
    arr->ht_cap = 0;
    arr->indexed_rows = 0;
    arr->generation = 0;
}

/* A primary registry entry is unbuilt exactly when its buffers are freed
 * (arr_free_contents zeroes nbuckets) or its source token was cleared by
 * invalidation, deferred release or eviction (Issue #1500).  A built empty
 * index (16 buckets, no chain array, fresh token) is not unbuilt: it is
 * handed out as is, leased or not.  indexed_rows == 0 is therefore no
 * longer the primary registry's "unbuilt" marker; the delta, filtered,
 * sorted and differential arrangements keep it as theirs. */
static inline bool
arr_entry_unbuilt(const col_arr_entry_t *e)
{
    return e->arr.nbuckets == 0
           || !wl_columnar_relation_snapshot_valid(e->source_snapshot);
}

static inline void
arr_entry_mark_unbuilt(col_arr_entry_t *e)
{
    e->arr.indexed_rows = 0;
    e->source_snapshot = (col_relation_snapshot_t){ 0, 0, 0 };
}

/**
 * col_arr_entry_clone - Deep-copy one arrangement registry entry (#260).
 *
 * Only rel_name and key_cols are copied; hash storage is built lazily.
 * arr.key_cols is set to the new entry's key_cols (shared alias,
 * not a separate allocation — matches arrangement.c creation convention).
 * Returns 0 on success; dst is memset-zeroed before returning on failure.
 */
static int
col_arr_entry_clone(const col_arr_entry_t *src, col_arr_entry_t *dst,
    wl_columnar_memory_governor_ref_t *memory_governor)
{
    memset(dst, 0, sizeof(*dst));

    /* A registry slot may carry no name or keys (defence in depth, #1515);
     * it clones to an equally empty slot. */
    dst->rel_name = src->rel_name ? wl_strdup(src->rel_name) : NULL;
    if (src->rel_name && !dst->rel_name)
        return ENOMEM;

    if (src->key_count > 0 && src->key_cols) {
        dst->key_cols = (uint32_t *)malloc(src->key_count * sizeof(uint32_t));
        if (!dst->key_cols) {
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->key_cols, src->key_cols, src->key_count * sizeof(uint32_t));
    }
    dst->key_count = src->key_count;

    /* arr.key_cols is a shared alias of entry.key_cols (not separately owned).
    * Mirrors the convention in col_session_get_arrangement (arrangement.c). */
    dst->arr.key_cols = dst->key_cols;
    dst->arr.key_count = src->arr.key_count;
    /* Only schema metadata crosses into the worker. Storage, progress,
     * freshness and byte accounting stay zero until its first rebuild. */
    wl_columnar_memory_reservation_init(&dst->arr.reservation);
    col_arr_attach_memory_governor(&dst->arr, memory_governor);
    dst->lru_clock = src->lru_clock;
    return 0;
}

/**
 * col_arr_entries_clone - Deep-copy an arrangement registry array (#260).
 *
 * Creates an independent copy of `count` entries for a K-fusion worker.
 * On success, *out_entries owns all allocations and *out_cap equals count.
 * On failure, *out_entries is NULL.
 */
int
col_arr_entries_clone(const col_arr_entry_t *src, uint32_t count,
    col_arr_entry_t **out_entries, uint32_t *out_cap,
    wl_columnar_memory_governor_ref_t *memory_governor)
{
    *out_entries = NULL;
    *out_cap = 0;

    if (count == 0)
        return 0;

    col_arr_entry_t *cloned
        = (col_arr_entry_t *)calloc(count, sizeof(col_arr_entry_t));
    if (!cloned)
        return ENOMEM;

    for (uint32_t i = 0; i < count; i++) {
        int clone_rc = col_arr_entry_clone(&src[i], &cloned[i],
                memory_governor);
        if (clone_rc != 0) {
            for (uint32_t j = 0; j < i; j++) {
                free(cloned[j].rel_name);
                free(cloned[j].key_cols);
                arr_free_contents(&cloned[j].arr);
                col_arr_detach_memory_governor(&cloned[j].arr);
            }
            free(cloned);
            return clone_rc;
        }
    }

    *out_entries = cloned;
    *out_cap = count;
    return 0;
}

/* Full rebuild: index all nrows rows in rel into arr. */
static int
arr_build_full_impl(col_arrangement_t *arr, const col_rel_t *rel)
{
    uint32_t nrows = rel->nrows;
    uint32_t nbuckets;

    /* Keep the load-factor multiplication representable before sizing either
     * the bucket table or its row chains. */
    if (nrows > UINT32_MAX / 2u)
        return ENOMEM;
    nbuckets = arr_next_pow2(nrows > 0 ? nrows * 2u : 16u);

    /*
     * arr_next_pow2 returns 0 above 0x80000000, so nbuckets is 0 for nrows
     * in [0x40000001, 0x7FFFFFFF] -- 1,073,741,823 row counts. A zero value
     * would pass the "size changed" test below against a zeroed arrangement, skip
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
     * Counts above UINT32_MAX / 2 are refused before the load-factor
     * multiplication can wrap and undersize the bucket or row-chain arrays.
     * This full-build path is used whenever a cache index is stale or
     * incomplete; there is no separate partial-index sizing path.
     *
     * ENOMEM is reused deliberately: the relation is unrepresentable rather
     * than out of memory, and EOVERFLOW would say so.  It is not that
     * EOVERFLOW is unavailable -- join.c, which consumes these
     * arrangements, returns it as rc=84 in nine places. It is that all
     * call sites of this function discard the value and collapse to NULL,
     * so the distinction could never reach a caller.
     */
    if (nbuckets == 0)
        return ENOMEM;

    uint64_t *new_head = NULL;
    uint32_t *new_next = NULL;
    uint32_t new_cap = arr->ht_cap;
    size_t prospective_head_bytes;
    size_t prospective_next_bytes;
    uint64_t prospective_bytes;
    wl_columnar_memory_reservation_t pending;
    bool pending_valid = false;

    /* Prepare every potentially failing allocation before publishing any
     * part of the new arrangement.  This keeps a failed rebuild usable. */
    if (nbuckets != arr->nbuckets) {
        size_t head_bytes = (size_t)nbuckets * sizeof(uint64_t);
        if (nbuckets != 0 && head_bytes / sizeof(uint64_t) != nbuckets)
            return ENOMEM;
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
    }

    prospective_head_bytes = (size_t)nbuckets * sizeof(uint64_t);
    prospective_next_bytes = (size_t)new_cap * sizeof(uint32_t);
    if ((nbuckets != 0
        && prospective_head_bytes / sizeof(uint64_t) != nbuckets)
        || (new_cap != 0
        && prospective_next_bytes / sizeof(uint32_t) != new_cap)
        || prospective_head_bytes > SIZE_MAX - prospective_next_bytes) {
        free(new_head);
        return ENOMEM;
    }
    prospective_bytes = (uint64_t)(prospective_head_bytes
        + prospective_next_bytes);
    if (nbuckets != arr->nbuckets || new_cap != arr->ht_cap) {
        if (!arr_reserve_bytes(arr, prospective_bytes, &pending)) {
            free(new_head);
            return ENOMEM;
        }
        pending_valid = arr->memory_governor != NULL;
    }
    if (nbuckets != arr->nbuckets) {
        new_head = (uint64_t *)malloc(prospective_head_bytes);
        if (!new_head) {
            if (pending_valid)
                (void)wl_columnar_memory_release(&pending);
            return ENOMEM;
        }
        /* Zero has generation zero, which cannot match a live generation. */
        memset(new_head, 0, prospective_head_bytes);
    }
    if (new_cap != arr->ht_cap) {
        size_t next_bytes = prospective_next_bytes;
        new_next = (uint32_t *)malloc(next_bytes);
        if (!new_next) {
            free(new_head);
            if (pending_valid)
                (void)wl_columnar_memory_release(&pending);
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
    if (pending_valid
        && !arr_publish_reservation(arr, &pending, prospective_bytes)) {
        arr_free_contents(arr);
        return ENOMEM;
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

/* Ledger-aware wrapper: the build may free and reallocate ht_head/ht_next
 * on several paths, so the charge is reconciled once from the before/after
 * footprint instead of at each malloc (Issue #1380). */
static int
arr_build_full(col_arrangement_t *arr, const col_rel_t *rel)
{
    uint64_t before = arr->ledger ? arr_ledger_bytes(arr) : 0;
    int rc = arr_build_full_impl(arr, rel);
    arr_ledger_sync(arr, before);
    return rc;
}

/* The batch record owns every not-yet-installed buffer and credit.  This
 * mandatory checked drain performs no new forward admission after a stop. */
static int
wl_columnar_arrangement_cleanup_result(int primary, int cleanup)
{
    return primary > 0 || !cleanup ? primary : cleanup;
}
static int
wl_columnar_arrangement_credit_retire(wl_columnar_memory_reservation_t *token)
{
    if (token->identity != token) return EINVAL;
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED) {
        wl_columnar_memory_reservation_init(token); return 0;
    }
    bool accepted = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        ? wl_columnar_memory_release(token)
        : state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && wl_columnar_memory_rollback(token);
    if (!accepted) return EINVAL;
    wl_columnar_memory_reservation_init(token);
    return 0;
}
static int
wl_columnar_arrangement_registry_retire(wl_columnar_join_batch_record_t *record,
    int primary)
{
    if (!record->registry.new_entries) {
        record->phase = WL_COLUMNAR_JOIN_BATCH_INERT; return primary;
    }
    /* Only actual successful moves are reversed.  A failed reverse or owner
     * transfer keeps both original arrays and this exact subphase reachable. */
    for (;;) {
        if (record->registry.transport == 2u ||
            record->registry.transport == 3u){
            col_arr_entry_t *old = &record->registry.old_entries[
                record->registry.cursor];
            col_arr_entry_t *fresh = &record->registry.new_entries[
                record->registry.cursor];
            if (!wl_columnar_memory_reservation_move(&old->arr.reservation,
                &fresh->arr.reservation))
                return wl_columnar_arrangement_cleanup_result(primary, EINVAL);
            record->registry.transport = 1u;
            if (!wl_columnar_memory_transfer(&old->arr.reservation,
                &old->arr)){
                record->registry.transport = 4u;
                return wl_columnar_arrangement_cleanup_result(primary, EINVAL);
            }
        } else if (record->registry.transport == 4u) {
            col_arr_entry_t *old = &record->registry.old_entries[
                record->registry.cursor];
            if (!wl_columnar_memory_transfer(&old->arr.reservation, &old->arr))
                return wl_columnar_arrangement_cleanup_result(primary, EINVAL);
        }
        record->registry.transport = 0;
        if (!record->registry.cursor) break;
        record->registry.cursor--;
        col_arr_entry_t *fresh = &record->registry.new_entries[
            record->registry.cursor];
        uint64_t state = atomic_load_explicit(&fresh->arr.reservation.state,
                memory_order_acquire);
        if (state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            || state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
            record->registry.transport = 2u;
    }
    free(record->registry.new_entries); record->registry.new_entries = NULL;
    record->registry.old_entries = NULL;
    record->phase = WL_COLUMNAR_JOIN_BATCH_INERT;
    return primary;
}
static int
wl_columnar_arrangement_registry_copy(void *dst, const void *src, size_t bytes,
    wl_evaluation_control_t *control)
{
    for (size_t at = 0; at < bytes;) {
        size_t count = bytes - at; if (count > 128u) count = 128u;
        int rc = wl_evaluation_control_charge(control, count);
        if (rc) return rc;
        memcpy((char *)dst + at, (const char *)src + at, count); at += count;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    return 0;
}
int
wl_columnar_arrangement_registry_grow_controlled(
    wl_columnar_join_batch_record_t *record, uint32_t capacity)
{
    if (!record || !record->ready || !record->session || !record->control
        || record->phase != WL_COLUMNAR_JOIN_BATCH_INERT) return EINVAL;
    wl_col_session_t *session = record->session;
    wl_evaluation_control_t *control = record->control;
    if (capacity <= session->arr_cap || capacity < session->arr_count
        || (uint64_t)capacity * sizeof(col_arr_entry_t) >
        SIZE_MAX) return EINVAL;
    for (uint32_t i = 0; i < session->arr_count; i++) {
        int rc = wl_evaluation_control_charge(control, 1); if (rc) return rc;
        if (session->arr_entries[i].pin_count) return EBUSY;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    int rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    memset(&record->registry, 0, sizeof(record->registry));
    record->registry.capacity = capacity;
    record->registry.count = session->arr_count;
    record->registry.old_entries = session->arr_entries;
    record->phase = WL_COLUMNAR_JOIN_BATCH_REGISTRY;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 1); if (rc) goto done;
    record->registry.new_entries = malloc((size_t)capacity
            * sizeof(col_arr_entry_t));
    if (!record->registry.new_entries) {
        rc = ENOMEM; goto done;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    for (size_t at = 0; at < (size_t)capacity * sizeof(col_arr_entry_t);) {
        size_t count = (size_t)capacity * sizeof(col_arr_entry_t) - at;
        if (count > 128u) count = 128u;
        rc = wl_evaluation_control_charge(control, count); if (rc) goto done;
        memset((char *)record->registry.new_entries + at, 0, count);
        at += count;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    }
    const size_t offset = offsetof(col_arr_entry_t, arr)
        + offsetof(col_arrangement_t, reservation);
    const size_t end = offset + sizeof(wl_columnar_memory_reservation_t);
    for (; record->registry.cursor < session->arr_count;
        record->registry.cursor++){
        col_arr_entry_t *old = &record->registry.old_entries[
            record->registry.cursor];
        col_arr_entry_t *fresh = &record->registry.new_entries[
            record->registry.cursor];
        rc = wl_evaluation_control_charge(control, 96); if (rc) goto done;
        wl_columnar_memory_reservation_init(&fresh->arr.reservation);
        uint64_t state = atomic_load_explicit(&old->arr.reservation.state,
                memory_order_acquire);
        if (old->arr.reservation.identity != &old->arr.reservation
            || (state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
            && state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
            && state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            && state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)) {
            rc = EINVAL; goto done;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
        rc = wl_columnar_arrangement_registry_copy(fresh, old, offset, control);
        if (rc) goto done;
        rc = wl_columnar_arrangement_registry_copy((char *)fresh + end,
                (char *)old + end, sizeof(*old) - end, control);
        if (rc) goto done;
        record->registry.transport = 1u;
        if (state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            || state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED) {
            rc = wl_evaluation_control_charge(control, 96); if (rc) goto done;
            if (!wl_columnar_memory_reservation_move(&fresh->arr.reservation,
                &old->arr.reservation)) {
                rc = EINVAL; goto done;
            }
            record->registry.transport = 2u;
            wl_columnar_join_batch_checkpoint(record, 9,
                record->registry.cursor);
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
            rc = wl_evaluation_control_charge(control, 96); if (rc) goto done;
            if (!wl_columnar_memory_transfer(&fresh->arr.reservation,
                &fresh->arr)) {
                rc = EINVAL; goto done;
            }
            record->registry.transport = 3u;
            wl_columnar_join_batch_checkpoint(record, 10,
                record->registry.cursor);
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
        }
        record->registry.transport = 0;
    }
    rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    session->arr_entries = record->registry.new_entries;
    session->arr_cap = capacity;
    record->registry.new_entries = NULL;
    free(record->registry.old_entries); record->registry.old_entries = NULL;
    record->phase = WL_COLUMNAR_JOIN_BATCH_INERT;
    return 0;
done:
    return wl_columnar_arrangement_registry_retire(record, rc);
}
int
wl_columnar_arrangement_record_retire(wl_columnar_join_batch_record_t *record,
    int primary)
{
    if (!record || !record->ready) return primary > 0 ? primary : EINVAL;
    if (record->phase == WL_COLUMNAR_JOIN_BATCH_INERT) return primary;
    if (record->phase == WL_COLUMNAR_JOIN_BATCH_REGISTRY)
        return wl_columnar_arrangement_registry_retire(record, primary);
    if (record->phase != WL_COLUMNAR_JOIN_BATCH_BUILD)
        return wl_columnar_arrangement_cleanup_result(primary, EBUSY);
    col_arrangement_t *arr = record->entry ? &record->entry->arr : NULL;
    if (!arr) return wl_columnar_arrangement_cleanup_result(primary, EINVAL);
    if (!record->committed && record->build.credit_installed) {
        if (!wl_columnar_memory_reservation_move(&record->pending,
            &arr->reservation))
            return wl_columnar_arrangement_cleanup_result(primary, EINVAL);
        record->build.credit_installed = false;
    }
    if (!record->committed && record->retired.bytes) {
        if (!wl_columnar_memory_reservation_move(&arr->reservation,
            &record->retired))
            return wl_columnar_arrangement_cleanup_result(primary, EINVAL);
    }
    free(record->build.head); record->build.head = NULL;
    free(record->build.next); record->build.next = NULL;
    free(record->build.old_head); record->build.old_head = NULL;
    free(record->build.old_next); record->build.old_next = NULL;
    int rc = wl_columnar_arrangement_credit_retire(&record->pending);
    primary = wl_columnar_arrangement_cleanup_result(primary, rc);
    rc = wl_columnar_arrangement_credit_retire(&record->retired);
    primary = wl_columnar_arrangement_cleanup_result(primary, rc);
    if (!rc && !record->pending.bytes && !record->retired.bytes) {
        wl_columnar_memory_reservation_init(&record->pending);
        wl_columnar_memory_reservation_init(&record->retired);
        record->phase = WL_COLUMNAR_JOIN_BATCH_INERT;
        record->committed = false;
    }
    return primary;
}
static int
wl_columnar_arrangement_build_tail(wl_columnar_join_batch_record_t *record)
{
    return wl_evaluation_control_charge(record->control, 0);
}
int
wl_columnar_arrangement_build_controlled(
    wl_columnar_join_batch_record_t *record,
    col_arr_entry_t *entry, const col_rel_t *rel)
{
    if (!record || !record->ready || !record->control || !entry || !rel
        || record->phase != WL_COLUMNAR_JOIN_BATCH_INERT) return EINVAL;
    wl_evaluation_control_t *control = record->control;
    int rc = wl_evaluation_control_charge(control, 64);
    if (rc) return rc;
    col_arrangement_t *arr = &entry->arr;
    if (entry->pin_count) return EBUSY;
    if (rel->nrows > UINT32_MAX / 2u) return ENOMEM;
    uint32_t buckets = arr_next_pow2(rel->nrows ? rel->nrows * 2u : 16u);
    if (!buckets) return ENOMEM;
    uint32_t capacity = arr->ht_cap;
    if (rel->nrows > capacity) {
        uint32_t doubled = capacity > UINT32_MAX / 2u
            ? UINT32_MAX : capacity * 2u;
        capacity = rel->nrows > doubled ? rel->nrows : doubled;
        if (capacity < 16u) capacity = 16u;
    }
    uint64_t head_bytes, next_bytes, bytes;
    if (!wl_columnar_memory_size_mul(buckets, sizeof(uint64_t), &head_bytes)
        || !wl_columnar_memory_size_mul(capacity, sizeof(uint32_t), &next_bytes)
        || !wl_columnar_memory_size_add(head_bytes, next_bytes, &bytes)
        || head_bytes > SIZE_MAX || next_bytes > SIZE_MAX) return ENOMEM;
    rc = wl_columnar_arrangement_build_tail(record); if (rc) return rc;
    memset(&record->build, 0, sizeof(record->build));
    record->phase = WL_COLUMNAR_JOIN_BATCH_BUILD; record->entry = entry;
    record->build.buckets = buckets; record->build.capacity = capacity;
    record->build.bytes = bytes;
    record->build.ledger_before = arr_ledger_bytes(arr);
    bool changed = buckets != arr->nbuckets || capacity != arr->ht_cap;
    if (changed && arr->memory_governor) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(arr->memory_governor), bytes,
            &record->pending, &status, control);
        if (rc) goto done;
        if (status > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                record->session->memory_budget_denied = true;
            rc = ENOMEM; goto done;
        }
        bool accepted;
        rc = wl_columnar_memory_governor_commit_controlled(&record->pending,
                arr, &accepted, control);
        if (rc) goto done;
        if (!accepted) {
            rc = EINVAL; goto done;
        }
    }
    if (buckets != arr->nbuckets) {
        rc = wl_evaluation_control_charge(control, 1); if (rc) goto done;
        record->build.head = malloc((size_t)head_bytes);
        if (!record->build.head) {
            rc = ENOMEM; goto done;
        }
        rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
        for (uint64_t at = 0; at < head_bytes;) {
            uint64_t n = head_bytes - at; if (n > 128u) n = 128u;
            rc = wl_evaluation_control_charge(control, n); if (rc) goto done;
            memset((char *)record->build.head + at, 0, (size_t)n); at += n;
            wl_columnar_join_batch_checkpoint(record, 1, at);
            rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
        }
    }
    if (capacity != arr->ht_cap) {
        rc = wl_evaluation_control_charge(control, 1); if (rc) goto done;
        record->build.next = malloc((size_t)next_bytes);
        if (!record->build.next) {
            rc = ENOMEM; goto done;
        }
        rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 192); if (rc) goto done;
    rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
    if (changed && arr->memory_governor && bytes != arr->reserved_bytes) {
        if (arr->reserved_bytes && !wl_columnar_memory_reservation_move(
                &record->retired, &arr->reservation)) {
            rc = EINVAL; goto done;
        }
        if (!wl_columnar_memory_reservation_move(&arr->reservation,
            &record->pending)){
            rc = EINVAL; goto done;
        }
        record->build.credit_installed = true;
    }
    /* All fallible transport precedes this scalar physical installation. */
    arr_entry_mark_unbuilt(entry);
    if (record->build.head) {
        record->build.old_head = arr->ht_head;
        arr->ht_head = record->build.head; record->build.head = NULL;
        arr->nbuckets = buckets;
    }
    if (record->build.next) {
        record->build.old_next = arr->ht_next;
        arr->ht_next = record->build.next; record->build.next = NULL;
        arr->ht_cap = capacity;
    }
    if (changed && arr->memory_governor) arr->reserved_bytes = bytes;
    record->committed = true;
    arr_ledger_sync(arr, record->build.ledger_before);
    record->session->arr_total_bytes -= entry->mem_bytes;
    entry->mem_bytes = (size_t)bytes;
    record->session->arr_total_bytes += entry->mem_bytes;
    free(record->build.old_head); record->build.old_head = NULL;
    free(record->build.old_next); record->build.old_next = NULL;
    rc = wl_columnar_arrangement_credit_retire(&record->retired);
    if (rc) goto done;
    rc = wl_columnar_arrangement_credit_retire(&record->pending);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 8); if (rc) goto done;
    rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
    if (arr->generation == UINT32_MAX) {
        for (uint32_t b = 0; b < buckets; b++) {
            rc = wl_evaluation_control_charge(control, 1); if (rc) goto done;
            arr->ht_head[b] = 0;
            rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
        }
        arr->generation = 1;
    } else {
        arr->generation++; if (!arr->generation) arr->generation = 1;
    }
    for (uint32_t row = 0; row < rel->nrows; row++) {
        rc = wl_evaluation_control_charge(control, 1); if (rc) goto done;
        uint32_t hash = (uint32_t)UINT64_C(14695981039346656037);
        rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
        for (uint32_t k = 0; k < arr->key_count; k++) {
            rc = wl_evaluation_control_charge(control, 1); if (rc) goto done;
            uint32_t col = arr->key_cols[k];
            if (col >= rel->ncols || !rel->columns || !rel->columns[col]) {
                rc = EINVAL; goto done;
            }
            uint64_t value = wl_columnar_value_key(rel, col,
                    rel->columns[col][row]);
            rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
            for (uint32_t byte = 0; byte < 8u; byte++) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc) goto done;
                hash ^= (uint32_t)(value & 255u); hash *= 435u; value >>= 8;
                rc = wl_columnar_arrangement_build_tail(record);
                if (rc) goto done;
            }
        }
        uint32_t bucket = hash & (buckets - 1u);
        rc = wl_evaluation_control_charge(control, 3); if (rc) goto done;
        uint64_t head = arr->ht_head[bucket];
        arr->ht_next[row] = ARR_HEAD_GENERATION(head) == arr->generation
            ? ARR_HEAD_ROW(head) : UINT32_MAX;
        arr->ht_head[bucket] = ARR_MAKE_HEAD(arr->generation, row);
        record->build.row = row + 1u;
        wl_columnar_join_batch_checkpoint(record, 2, row + 1u);
        rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
    rc = wl_columnar_arrangement_build_tail(record); if (rc) goto done;
    arr->indexed_rows = rel->nrows;
    entry->source_snapshot = wl_columnar_relation_snapshot(rel);
    entry->rebuild_deferred = false;
    entry->lru_clock = ++record->session->arr_clock;
    rc = 0;
done:
    return wl_columnar_arrangement_record_retire(record, rc);
}

/* ======================================================================== */
/* Arrangement Cache LRU Eviction (Issue #216)                              */
/* ======================================================================== */

/*
 * col_arr_cache_evict_lru: evict arrangement cache entries until arr_total_bytes
 * drops below target_bytes, or until no more evictable entries remain.
 *
 * Priority:
 *   1. unbuilt entries (token cleared by invalidation) that still hold
 *      buffers: nothing readable is lost
 *   2. smallest lru_clock (least recently used)
 *
 * Eviction is a tombstone: arr_free_contents clears ht_head/ht_next and the
 * entry's source token is cleared, so the entry reads as unbuilt.  The slot
 * (rel_name, key_cols) is retained so the next access can rebuild without
 * re-allocating the entry.
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
            if (e->pin_count > 0) {
                e->evict_deferred = true;
                continue;
            }

            /* Prefer invalidated (unbuilt) entries first. */
            if (arr_entry_unbuilt(e)) {
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
        arr_entry_mark_unbuilt(e);
        e->mem_bytes = 0;
        e->evict_deferred = false;
    }
}

static int
wl_columnar_arrangement_evict_controlled(
    wl_columnar_join_batch_record_t *record,
    size_t target)
{
    wl_col_session_t *session = record->session;
    wl_evaluation_control_t *control = record->control;
    for (;;) {
        int rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        bool finished = session->arr_total_bytes <= target;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (finished) return 0;
        uint32_t victim = UINT32_MAX;
        uint64_t minimum = UINT64_MAX;
        for (uint32_t i = 0; i < session->arr_count; i++) {
            rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
            col_arr_entry_t *entry = &session->arr_entries[i];
            if (entry->mem_bytes) {
                if (entry->pin_count) entry->evict_deferred = true;
                else if (arr_entry_unbuilt(entry)) victim = i;
                else if (entry->lru_clock < minimum) {
                    minimum = entry->lru_clock; victim = i;
                }
            }
            rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
            if (victim == i && arr_entry_unbuilt(entry)) break;
        }
        if (victim == UINT32_MAX) return 0;
        rc = wl_evaluation_control_charge(control, 192); if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        col_arr_entry_t *entry = &session->arr_entries[victim];
        col_arrangement_t *arr = &entry->arr;
        memset(&record->build, 0, sizeof(record->build));
        record->phase = WL_COLUMNAR_JOIN_BATCH_BUILD; record->entry = entry;
        if (arr->reserved_bytes && !wl_columnar_memory_reservation_move(
                &record->retired, &arr->reservation))
            return wl_columnar_arrangement_record_retire(record, EINVAL);
        record->committed = true;
        record->build.old_head = arr->ht_head;
        record->build.old_next = arr->ht_next;
        uint64_t before = arr_ledger_bytes(arr);
        arr->ht_head = NULL; arr->ht_next = NULL;
        arr->nbuckets = 0; arr->ht_cap = 0; arr->generation = 0;
        arr->reserved_bytes = 0;
        arr_entry_mark_unbuilt(entry); arr_ledger_sync(arr, before);
        session->arr_total_bytes -= entry->mem_bytes;
        entry->mem_bytes = 0; entry->evict_deferred = false;
        rc = wl_columnar_arrangement_record_retire(record, 0);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
}
int
wl_columnar_arrangement_pin_retire_controlled(
    wl_columnar_join_batch_record_t *record,
    col_arrangement_pin_t *pin, int primary, bool mandatory)
{
    if (!record || !record->ready || !pin || !record->session)
        return wl_columnar_arrangement_cleanup_result(primary, EINVAL);
    if (!pin->active) return primary;
    wl_col_session_t *session = record->session;
    uintptr_t base = (uintptr_t)session->arr_entries;
    uintptr_t address = (uintptr_t)pin->entry;
    uint64_t bytes = (uint64_t)session->arr_count * sizeof(col_arr_entry_t);
    int rc = mandatory ? 0 : wl_evaluation_control_charge(record->control, 96);
    if (rc) return wl_columnar_arrangement_cleanup_result(primary, rc);
    if (!session->arr_entries || bytes > UINTPTR_MAX - base || address < base
        || address >= base + bytes || (address - base) % sizeof(col_arr_entry_t)
        || pin->session != session || pin->arr != &pin->entry->arr
        || !pin->entry->pin_count)
        return wl_columnar_arrangement_cleanup_result(primary, EINVAL);
    if (!mandatory) {
        rc = wl_evaluation_control_charge(record->control, 0);
        if (rc) return wl_columnar_arrangement_cleanup_result(primary, rc);
    }
    col_arr_entry_t *entry = pin->entry;
    entry->pin_count--;
    if (!entry->pin_count && entry->rebuild_deferred) {
        arr_entry_mark_unbuilt(entry); entry->rebuild_deferred = false;
    }
    bool evict = !entry->pin_count && entry->evict_deferred;
    memset(pin, 0, sizeof(*pin));
    if (!mandatory) {
        rc = wl_evaluation_control_charge(record->control, 0);
        if (rc) return wl_columnar_arrangement_cleanup_result(primary, rc);
        if (evict) {
            rc = wl_columnar_arrangement_evict_controlled(record,
                    session->arr_cache_limit_bytes / 4u * 3u);
            if (rc) return wl_columnar_arrangement_cleanup_result(primary, rc);
            entry->evict_deferred = false;
        }
    }
    return primary;
}
int
wl_columnar_arrangement_pending_evict_controlled(
    wl_columnar_join_batch_record_t *record)
{
    if (!record || !record->ready || !record->session) return EINVAL;
    bool pending = false;
    for (uint32_t i = 0; i < record->session->arr_count; i++) {
        int rc = wl_evaluation_control_charge(record->control, 1);
        if (rc) return rc;
        pending = pending || record->session->arr_entries[i].evict_deferred;
        rc = wl_evaluation_control_charge(record->control, 0);
        if (rc) return rc;
    }
    return pending ? wl_columnar_arrangement_evict_controlled(record,
               record->session->arr_cache_limit_bytes / 4u * 3u) : 0;
}
static int
wl_columnar_arrangement_name_equal_controlled(const char *a, const char *b,
    bool *equal, wl_evaluation_control_t *control)
{
    *equal = false;
    if (!a || !b) return 0;
    for (size_t i = 0;; i++) {
        int rc = wl_evaluation_control_charge(control, 1); if (rc) return rc;
        char av = a[i], bv = b[i];
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (av != bv) return 0;
        if (!av) {
            *equal = true; return 0;
        }
    }
}
int
wl_columnar_arrangement_primary_controlled(
    wl_columnar_join_batch_record_t *record,
    const col_rel_t *source, const char *name, const uint32_t *keys,
    uint32_t key_count, col_arr_entry_t **out)
{
    if (!record || !record->ready || !record->session || !record->control
        || record->phase != WL_COLUMNAR_JOIN_BATCH_INERT || !source || !name
        || !keys || !key_count || !out || *out) return EINVAL;
    wl_col_session_t *session = record->session;
    wl_evaluation_control_t *control = record->control;
    int rc;
    for (uint32_t k = 0; k < key_count; k++) {
        rc = wl_evaluation_control_charge(control, 1); if (rc) return rc;
        if (keys[k] >= source->ncols) return ENOENT;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    for (uint32_t i = 0; i < session->arr_count; i++) {
        rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
        col_arr_entry_t *entry = &session->arr_entries[i];
        bool match = entry->key_count == key_count;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (!match) continue;
        rc = wl_columnar_arrangement_name_equal_controlled(entry->rel_name,
                name, &match, control); if (rc) return rc;
        if (!match) continue;
        for (uint32_t k = 0; k < key_count; k++) {
            rc = wl_evaluation_control_charge(control, 1); if (rc) return rc;
            match = entry->key_cols && entry->key_cols[k] == keys[k];
            rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
            if (!match) break;
        }
        if (!match) continue;
        rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
        bool fresh = wl_columnar_relation_snapshot_equal(entry->source_snapshot,
                wl_columnar_relation_snapshot(source)) &&
            !arr_entry_unbuilt(entry)
            && entry->arr.indexed_rows == source->nrows &&
            !entry->rebuild_deferred;
        if (!fresh && entry->pin_count) {
            entry->rebuild_deferred = true; return EBUSY;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (!fresh) {
            rc = wl_columnar_arrangement_build_controlled(record, entry,
                    source);
            if (rc) return rc;
        }
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        entry->lru_clock = ++session->arr_clock; *out = entry; return 0;
    }
    if (session->arr_count >= COL_ARR_CACHE_MAX
        || session->arr_total_bytes >= session->arr_cache_limit_bytes) {
        rc = wl_columnar_arrangement_evict_controlled(record,
                session->arr_cache_limit_bytes / 4u * 3u);
        if (rc) return rc;
    }
    uint32_t slot = session->arr_count;
    for (uint32_t i = 0; i < session->arr_count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        bool reusable = !session->arr_entries[i].mem_bytes
            && !session->arr_entries[i].pin_count;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (reusable) {
            slot = i; break;
        }
    }
    if (slot == session->arr_count && slot >= session->arr_cap) {
        uint32_t capacity = session->arr_cap ? session->arr_cap * 2u : 8u;
        if (capacity < session->arr_cap) return ENOMEM;
        rc = wl_columnar_arrangement_registry_grow_controlled(record, capacity);
        if (rc) return rc;
    }
    /* Staged name/key ownership remains in the original registry overlay. */
    rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    memset(&record->registry, 0, sizeof(record->registry));
    record->registry.slot = slot;
    record->phase = WL_COLUMNAR_JOIN_BATCH_REGISTRY;
    size_t length = 0;
    for (;;) {
        rc = wl_evaluation_control_charge(control, 1); if (rc) goto create_done;
        char byte = name[length];
        if (length == SIZE_MAX) {
            rc = EOVERFLOW; goto create_done;
        }
        length++;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto create_done;
        if (!byte) break;
    }
    rc = wl_evaluation_control_charge(control, 1); if (rc) goto create_done;
    record->registry.name = malloc(length);
    if (!record->registry.name) {
        rc = ENOMEM; goto create_done;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto create_done;
    rc = wl_columnar_arrangement_registry_copy(record->registry.name, name,
            length, control); if (rc) goto create_done;
    if ((uint64_t)key_count * sizeof(uint32_t) > SIZE_MAX) {
        rc = ENOMEM; goto create_done;
    }
    rc = wl_evaluation_control_charge(control, 1); if (rc) goto create_done;
    record->registry.keys = malloc((size_t)key_count * sizeof(uint32_t));
    if (!record->registry.keys) {
        rc = ENOMEM; goto create_done;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto create_done;
    rc = wl_columnar_arrangement_registry_copy(record->registry.keys, keys,
            (size_t)key_count * sizeof(uint32_t), control);
    if (rc) goto create_done;
    rc = wl_evaluation_control_charge(control, 256); if (rc) goto create_done;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto create_done;
    col_arr_entry_t *entry = &session->arr_entries[slot];
    bool appended = slot == session->arr_count;
    if (!appended) {
        uint64_t state = atomic_load_explicit(&entry->arr.reservation.state,
                memory_order_acquire);
        if (entry->arr.ht_head || entry->arr.ht_next ||
            entry->arr.reserved_bytes
            || entry->arr.reservation.identity != &entry->arr.reservation
            || (state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
            && state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)) {
            rc = EINVAL; goto create_done;
        }
        free(entry->rel_name); free(entry->key_cols);
        wl_columnar_memory_governor_ref_release(entry->arr.memory_governor);
        entry->arr.memory_governor = NULL;
    }
    memset(entry, 0, sizeof(*entry));
    wl_columnar_memory_reservation_init(&entry->arr.reservation);
    entry->rel_name = record->registry.name; record->registry.name = NULL;
    entry->key_cols = record->registry.keys; record->registry.keys = NULL;
    entry->key_count = key_count; entry->arr.key_cols = entry->key_cols;
    entry->arr.key_count = key_count; entry->arr.ledger = &session->mem_ledger;
    col_arr_attach_memory_governor(&entry->arr, session->memory_governor);
    if (slot == session->arr_count) session->arr_count++;
    record->phase = WL_COLUMNAR_JOIN_BATCH_INERT;
    rc = wl_columnar_arrangement_build_controlled(record, entry, source);
    if (rc) {
        if (rc == ENOMEM && appended && !entry->mem_bytes
            && record->phase == WL_COLUMNAR_JOIN_BATCH_INERT) {
            free(entry->rel_name); entry->rel_name = NULL;
            free(entry->key_cols); entry->key_cols = NULL;
            wl_columnar_memory_governor_ref_release(entry->arr.memory_governor);
            entry->arr.memory_governor = NULL;
            session->arr_count--;
        }
        return rc;
    }
    *out = entry; return 0;
create_done:
    free(record->registry.name); record->registry.name = NULL;
    free(record->registry.keys); record->registry.keys = NULL;
    record->phase = WL_COLUMNAR_JOIN_BATCH_INERT;
    return rc;
}

static col_arr_entry_t *
col_arr_entry_for_arr(wl_col_session_t *cs, const col_arrangement_t *arr)
{
    if (!cs || !arr)
        return NULL;
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (&cs->arr_entries[i].arr == arr)
            return &cs->arr_entries[i];
    }
    return NULL;
}

/* ======================================================================== */
/* Arrangement Accessors (Phase 3C)                                         */
/* ======================================================================== */

static bool
col_arr_entry_key_match(const col_arr_entry_t *e, const char *rel_name,
    const uint32_t *key_cols, uint32_t key_count)
{
    if (!e || !rel_name || !key_cols || e->key_count != key_count
        || !e->rel_name || strcmp(e->rel_name, rel_name) != 0)
        return false;
    for (uint32_t k = 0; k < key_count; k++) {
        if (e->key_cols[k] != key_cols[k])
            return false;
    }
    return true;
}

static int
col_session_get_arrangement_for_source(wl_col_session_t *cs, col_rel_t *rel,
    const char *rel_name, const uint32_t *key_cols, uint32_t key_count,
    col_arrangement_t **out_arr)
{
    if (!cs || !rel || !rel_name || !key_cols || key_count == 0 || !out_arr)
        return EINVAL;
    *out_arr = NULL;
    wl_columnar_join_batch_record_t *pending =
        wl_columnar_eval_stack_cleanup_join_batch(cs->cleanup_active
            ? cs->cleanup_active : cs->cleanup_pending);
    if (pending && (pending->phase == WL_COLUMNAR_JOIN_BATCH_REGISTRY
        || pending->phase == WL_COLUMNAR_JOIN_BATCH_BUILD)) return EBUSY;
    if (!arr_key_cols_valid(rel, key_cols, key_count))
        return ENOENT;

    /* Search registry for matching (rel_name, key_cols) entry. */
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        col_arr_entry_t *e = &cs->arr_entries[i];
        if (!col_arr_entry_key_match(e, rel_name, key_cols, key_count))
            continue;

        /* A token mismatch invalidates the complete index. */
        bool snapshot_match = wl_columnar_relation_snapshot_equal(
            e->source_snapshot, wl_columnar_relation_snapshot(rel));
        /* Even a same-capacity rebuild rewrites bucket generations/chains.
         * Keep every index mutation behind the last reader's release.  A
         * built empty index (Issue #1500) is fresh and is returned below;
         * the unbuilt term mirrors the rebuild clause for symmetry, since
         * no path frees a leased entry's buffers. */
        if (e->pin_count > 0
            && (!snapshot_match || arr_entry_unbuilt(e)
            || e->arr.indexed_rows != rel->nrows
            || e->rebuild_deferred)) {
            e->rebuild_deferred = true;
            return EBUSY;
        }
        if (!snapshot_match || arr_entry_unbuilt(e)
            || e->arr.indexed_rows != rel->nrows) {
            /* Deduct stale bytes before rebuild; restore on failure. */
            cs->arr_total_bytes -= e->mem_bytes;
            if (arr_build_full(&e->arr, rel) != 0) {
                cs->arr_total_bytes += e->mem_bytes;
                return ENOMEM;
            }
            if (!arr_memory_bytes(&e->arr, &e->mem_bytes)) {
                arr_free_contents(&e->arr);
                arr_entry_mark_unbuilt(e);
                e->mem_bytes = 0;
                return ENOMEM;
            }
            cs->arr_total_bytes += e->mem_bytes;
            e->source_snapshot = wl_columnar_relation_snapshot(rel);
        }
        /* Bump LRU clock on every access. */
        e->lru_clock = ++cs->arr_clock;
        *out_arr = &e->arr;
        return 0;
    }

    /* Not found: evict if count or byte limits exceeded. */
    if (cs->arr_count >= COL_ARR_CACHE_MAX
        || cs->arr_total_bytes >= cs->arr_cache_limit_bytes) {
        /* Target: drop to 75% of limit to amortize eviction overhead. */
        size_t target = (cs->arr_cache_limit_bytes / 4u) * 3u;
        col_arr_cache_evict_lru(cs, target);
    }

    /* Prefer reusing a tombstone slot to keep arr_count bounded at
     * COL_ARR_CACHE_MAX.  A tombstone has mem_bytes == 0 (hash tables
     * freed; a build never yields zero bytes); rel_name/key_cols are still
     * valid and must be replaced below. */
    uint32_t slot = cs->arr_count; /* default: append */
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (cs->arr_entries[i].mem_bytes == 0
            && cs->arr_entries[i].pin_count == 0) {
            slot = i;
            break;
        }
    }

    const bool appended = slot == cs->arr_count;

    if (appended && cs->arr_count >= cs->arr_cap) {
        /* No tombstone available: grow the registry.  A lease contains
         * pointers into this flat registry.  Do not move it underneath an
         * active borrower; callers can fall back to an ephemeral
         * arrangement until the lease is released. */
        for (uint32_t i = 0; i < cs->arr_count; i++) {
            if (cs->arr_entries[i].pin_count > 0)
                return EBUSY;
        }
        uint32_t new_cap = cs->arr_cap ? cs->arr_cap * 2u : 8u;
        if (!arr_entries_grow(&cs->arr_entries, &cs->arr_cap,
            cs->arr_count, new_cap))
            return ENOMEM;
    }

    /* Issue #1515: take every allocation that can fail before disturbing
     * the registry, so a failure leaves an appended count untouched and a
     * reused tombstone exactly as it was. */
    char *new_name = wl_strdup(rel_name);
    if (!new_name)
        return ENOMEM;
    uint32_t *new_keys = (uint32_t *)malloc(key_count * sizeof(uint32_t));
    if (!new_keys) {
        free(new_name);
        return ENOMEM;
    }

    if (appended) {
        cs->arr_count++;
    } else {
        /* Reuse tombstone: free its old ownership before overwriting. */
        free(cs->arr_entries[slot].rel_name);
        free(cs->arr_entries[slot].key_cols);
        col_arr_detach_memory_governor(&cs->arr_entries[slot].arr);
    }

    col_arr_entry_t *e = &cs->arr_entries[slot];
    memset(e, 0, sizeof(*e));
    wl_columnar_memory_reservation_init(&e->arr.reservation);
    e->rel_name = new_name;
    e->key_cols = new_keys;
    memcpy(e->key_cols, key_cols, key_count * sizeof(uint32_t));
    e->key_count = key_count;
    e->arr.key_cols = e->key_cols; /* shared view; key_cols owned by entry */
    e->arr.key_count = key_count;
    e->arr.ledger = &cs->mem_ledger; /* Issue #1380: ARRANGEMENT accounting */
    col_arr_attach_memory_governor(&e->arr, cs->memory_governor);

    /* Initial build. */
    if (arr_build_full(&e->arr, rel) != 0
        || !arr_memory_bytes(&e->arr, &e->mem_bytes)) {
        arr_free_contents(&e->arr);
        e->mem_bytes = 0;
        if (appended) {
            col_arr_detach_memory_governor(&e->arr);
            free(e->rel_name);
            free(e->key_cols);
            memset(e, 0, sizeof(*e));
            cs->arr_count--; /* roll back append */
        } else {
            /* Issue #1515: a reused slot stays a well-formed tombstone
             * keyed on the new (rel_name, key_cols), shaped exactly like an
             * evicted entry: name, keys, ledger and the attached governor
             * remain, and an invalid token forces a rebuild on the next
             * hit.  Every registry scan keeps working and the next miss
             * can reuse the slot again. */
            e->source_snapshot = wl_columnar_relation_snapshot(NULL);
        }
        return ENOMEM;
    }
    e->source_snapshot = wl_columnar_relation_snapshot(rel);
    cs->arr_total_bytes += e->mem_bytes;
    e->lru_clock = ++cs->arr_clock;
    *out_arr = &e->arr;
    return 0;
}

col_arrangement_t *
col_session_get_arrangement(wl_session_t *sess, const char *rel_name,
    const uint32_t *key_cols, uint32_t key_count)
{
    wl_col_session_t *cs;
    col_rel_t *rel = NULL;
    col_arrangement_t *arr = NULL;

    if (!sess || !rel_name || !key_cols || key_count == 0)
        return NULL;

    cs = COL_SESSION(sess);
    for (uint32_t i = 0; i < cs->nrels; i++) {
        if (cs->rels[i] && cs->rels[i]->name
            && strcmp(cs->rels[i]->name, rel_name) == 0) {
            rel = cs->rels[i];
            break;
        }
    }
    if (!rel)
        return NULL;
    if (col_session_get_arrangement_for_source(cs, rel, rel_name, key_cols,
        key_count, &arr)
        != 0)
        return NULL;
    return arr;
}

static int
col_arrangement_pin_entry(wl_col_session_t *session, col_arr_entry_t *entry,
    col_arrangement_t *arr, col_arrangement_pin_t *pin)
{
    if (!session || !entry || !arr || !pin || &entry->arr != arr)
        return EINVAL;
    memset(pin, 0, sizeof(*pin));
    if (entry->pin_count == UINT32_MAX)
        return EOVERFLOW;
    entry->pin_count++;
    pin->entry = entry;
    pin->arr = arr;
    pin->session = session;
    pin->active = true;
    return 0;
}

int
col_session_pin_arrangement(wl_session_t *sess, const char *rel_name,
    const uint32_t *key_cols, uint32_t key_count, col_arrangement_pin_t *pin)
{
    col_arrangement_t *arr;
    col_arr_entry_t *entry;

    if (!pin)
        return EINVAL;
    memset(pin, 0, sizeof(*pin));
    arr = col_session_get_arrangement(sess, rel_name, key_cols, key_count);
    if (!arr)
        return ENOMEM;
    entry = col_arr_entry_for_arr(COL_SESSION(sess), arr);
    if (!entry)
        return EINVAL;
    return col_arrangement_pin_entry(COL_SESSION(sess), entry, arr, pin);
}

static bool
col_session_has_exact_relation(const wl_col_session_t *session,
    const col_rel_t *source, const char *name)
{
    if (!session || !source || !name)
        return false;
    for (uint32_t i = 0; i < session->nrels; i++) {
        const col_rel_t *candidate = session->rels[i];
        if (candidate && candidate->name
            && strcmp(candidate->name, name) == 0)
            return candidate == source;
    }
    return false;
}

static int
col_arrangement_probe_storage_owner_resolve(const col_rel_t *source,
    col_rel_t **out_owner, uint64_t *out_identity, uint64_t *out_generation)
{
    col_rel_t *owner = NULL;
    int rc;

    if (!source || !out_owner || !out_identity || !out_generation)
        return EINVAL;
    rc = col_rel_storage_owner_resolve(source, &owner);
    if (rc != 0)
        return rc;
    if (!owner || owner->storage_owner != owner
        || owner->storage_owner_identity != owner->relation_identity
        || owner->storage_owner_generation != owner->storage_generation
        || !wl_columnar_relation_generation_valid(
            owner->storage_owner_generation))
        return EINVAL;
    if (source->storage_owner != owner
        || source->storage_owner_identity != owner->relation_identity
        || source->storage_owner_generation != owner->storage_generation)
        return EBUSY;
    *out_owner = owner;
    *out_identity = owner->relation_identity;
    *out_generation = owner->storage_owner_generation;
    return 0;
}

int
col_session_acquire_arrangement_probe(wl_session_t *sess,
    col_arrangement_t *arr, const col_rel_t *source,
    col_arrangement_probe_t *probe)
{
    col_relation_snapshot_t snapshot;
    col_arr_entry_t *entry;
    col_rel_t *storage_owner = NULL;
    col_rel_t *post_storage_owner = NULL;
    wl_col_session_t *session;
    uint64_t storage_owner_identity = 0;
    uint64_t storage_owner_generation = 0;
    uint64_t post_storage_owner_identity = 0;
    uint64_t post_storage_owner_generation = 0;
    int rc;

    if (!sess || !arr || !source || !probe || probe->active
        || probe->identity != 0 || probe->arr || probe->source
        || probe->storage_owner || probe->storage_owner_identity != 0
        || probe->storage_owner_generation != 0
        || probe->arrangement_pin.active || probe->source_reader.owner)
        return EINVAL;
    session = COL_SESSION(sess);
    entry = col_arr_entry_for_arr(session, arr);
    snapshot = wl_columnar_relation_snapshot(source);
    if (!entry || !source->name || !entry->rel_name
        || strcmp(entry->rel_name, source->name) != 0)
        return EINVAL;
    if (!col_session_has_exact_relation(session, source, entry->rel_name)
        || !wl_columnar_relation_snapshot_valid(snapshot)
        || !wl_columnar_relation_snapshot_equal(entry->source_snapshot,
        snapshot) || arr_entry_unbuilt(entry) || entry->rebuild_deferred)
        return EBUSY;

    rc = col_arrangement_probe_storage_owner_resolve(source, &storage_owner,
            &storage_owner_identity, &storage_owner_generation);
    if (rc != 0)
        return rc;

    rc = col_rel_source_reader_acquire(source, &probe->source_reader);
    if (rc != 0)
        return rc;

    /* Admission freezes source storage, but identity and registry membership
     * are checked again so a future concurrent session registry cannot turn
     * this exact-source API back into a name-only lookup. */
    if (!col_session_has_exact_relation(session, source, entry->rel_name)
        || col_arr_entry_for_arr(session, arr) != entry
        || col_arrangement_probe_storage_owner_resolve(source,
        &post_storage_owner, &post_storage_owner_identity,
        &post_storage_owner_generation)
        != 0
        || post_storage_owner != storage_owner
        || post_storage_owner_identity != storage_owner_identity
        || post_storage_owner_generation != storage_owner_generation
        || !wl_columnar_relation_snapshot_equal(
            wl_columnar_relation_snapshot(source), snapshot)
        || !wl_columnar_relation_snapshot_equal(entry->source_snapshot,
        snapshot)) {
        rc = col_rel_source_reader_release(&probe->source_reader);
        return rc == 0 ? EBUSY : rc;
    }

    /* Pin the exact entry already validated under source admission.  Calling
     * the name-based getter here could rebuild, relocate, or otherwise mutate
     * registry state before a replacement race is rejected. */
    rc = col_arrangement_pin_entry(session, entry, arr,
            &probe->arrangement_pin);
    if (rc != 0) {
        int release_rc
            = col_rel_source_reader_release(&probe->source_reader);
        return release_rc == 0 ? rc : release_rc;
    }
    if (probe->arrangement_pin.arr != arr
        || probe->arrangement_pin.entry != entry
        || !col_session_has_exact_relation(session, source, entry->rel_name)
        || col_arrangement_probe_storage_owner_resolve(source,
        &post_storage_owner, &post_storage_owner_identity,
        &post_storage_owner_generation)
        != 0
        || post_storage_owner != storage_owner
        || post_storage_owner_identity != storage_owner_identity
        || post_storage_owner_generation != storage_owner_generation
        || !wl_columnar_relation_snapshot_equal(entry->source_snapshot,
        snapshot)) {
        col_arrangement_pin_release(&probe->arrangement_pin);
        rc = col_rel_source_reader_release(&probe->source_reader);
        return rc == 0 ? EBUSY : rc;
    }

    probe->arr = arr;
    probe->source = source;
    probe->source_snapshot = snapshot;
    probe->storage_owner = storage_owner;
    probe->storage_owner_identity = storage_owner_identity;
    probe->storage_owner_generation = storage_owner_generation;
    probe->identity = (uintptr_t)probe;
    probe->active = true;
    return 0;
}

int
col_session_acquire_primary_arrangement_probe(wl_session_t *sess,
    const col_rel_t *source, const uint32_t *key_cols, uint32_t key_count,
    col_arrangement_probe_t *probe)
{
    col_relation_snapshot_t snapshot;
    col_arrangement_t *arr = NULL;
    col_arr_entry_t *entry;
    col_rel_t *storage_owner = NULL;
    col_rel_t *post_storage_owner = NULL;
    wl_col_session_t *session;
    uint64_t storage_owner_identity = 0;
    uint64_t storage_owner_generation = 0;
    uint64_t post_storage_owner_identity = 0;
    uint64_t post_storage_owner_generation = 0;
    int rc;

    if (!sess || !source || !source->name || !key_cols || key_count == 0
        || !probe || probe->active || probe->identity != 0 || probe->arr
        || probe->source || probe->storage_owner
        || probe->storage_owner_identity != 0
        || probe->storage_owner_generation != 0
        || probe->arrangement_pin.active || probe->source_reader.owner)
        return EINVAL;
    session = COL_SESSION(sess);
    if (!col_session_has_exact_relation(session, source, source->name))
        return EBUSY;

    rc = col_arrangement_probe_storage_owner_resolve(source, &storage_owner,
            &storage_owner_identity, &storage_owner_generation);
    if (rc != 0)
        return rc;

    rc = col_rel_source_reader_acquire(source, &probe->source_reader);
    if (rc != 0)
        return rc;

    snapshot = wl_columnar_relation_snapshot(source);
    if (!wl_columnar_relation_snapshot_valid(snapshot)
        || !col_session_has_exact_relation(session, source, source->name)) {
        rc = col_rel_source_reader_release(&probe->source_reader);
        return rc == 0 ? EBUSY : rc;
    }

    rc = col_session_get_arrangement_for_source(session, (col_rel_t *)source,
            source->name, key_cols, key_count, &arr);
    if (rc != 0) {
        int release_rc = col_rel_source_reader_release(&probe->source_reader);
        return release_rc == 0 ? rc : release_rc;
    }
    entry = col_arr_entry_for_arr(session, arr);
    if (!entry) {
        rc = col_rel_source_reader_release(&probe->source_reader);
        return rc == 0 ? EINVAL : rc;
    }
    if (col_arrangement_probe_storage_owner_resolve(source,
        &post_storage_owner, &post_storage_owner_identity,
        &post_storage_owner_generation)
        != 0
        || post_storage_owner != storage_owner
        || post_storage_owner_identity != storage_owner_identity
        || post_storage_owner_generation != storage_owner_generation
        || !col_session_has_exact_relation(session, source, entry->rel_name)
        || !wl_columnar_relation_snapshot_equal(
            wl_columnar_relation_snapshot(source), snapshot)
        || !wl_columnar_relation_snapshot_equal(entry->source_snapshot,
        snapshot)) {
        rc = col_rel_source_reader_release(&probe->source_reader);
        return rc == 0 ? EBUSY : rc;
    }

    rc = col_arrangement_pin_entry(session, entry, arr,
            &probe->arrangement_pin);
    if (rc != 0) {
        int release_rc = col_rel_source_reader_release(&probe->source_reader);
        return release_rc == 0 ? rc : release_rc;
    }
    if (probe->arrangement_pin.arr != arr
        || probe->arrangement_pin.entry != entry
        || col_arrangement_probe_storage_owner_resolve(source,
        &post_storage_owner, &post_storage_owner_identity,
        &post_storage_owner_generation)
        != 0
        || post_storage_owner != storage_owner
        || post_storage_owner_identity != storage_owner_identity
        || post_storage_owner_generation != storage_owner_generation
        || !wl_columnar_relation_snapshot_equal(entry->source_snapshot,
        snapshot)) {
        col_arrangement_pin_release(&probe->arrangement_pin);
        rc = col_rel_source_reader_release(&probe->source_reader);
        return rc == 0 ? EBUSY : rc;
    }

    probe->arr = arr;
    probe->source = source;
    probe->source_snapshot = snapshot;
    probe->storage_owner = storage_owner;
    probe->storage_owner_identity = storage_owner_identity;
    probe->storage_owner_generation = storage_owner_generation;
    probe->identity = (uintptr_t)probe;
    probe->active = true;
    return 0;
}

void
col_arrangement_pin_release(col_arrangement_pin_t *pin)
{
    col_arr_entry_t *entry;
    wl_col_session_t *cs;
    bool evict_deferred;

    if (!pin || !pin->active || !pin->entry)
        return;
    entry = pin->entry;
    cs = pin->session;
    if (entry->pin_count > 0)
        entry->pin_count--;
    if (entry->pin_count == 0 && entry->rebuild_deferred) {
        /* Apply the deferred invalidation without rebuilding: the next
         * lookup sees an unbuilt entry and rebuilds lazily. */
        arr_entry_mark_unbuilt(entry);
        entry->rebuild_deferred = false;
    }
    evict_deferred = entry->pin_count == 0 && entry->evict_deferred;
    if (evict_deferred)
        entry->evict_deferred = false;
    memset(pin, 0, sizeof(*pin));
    if (evict_deferred && cs) {
        size_t target = (cs->arr_cache_limit_bytes / 4u) * 3u;
        col_arr_cache_evict_lru(cs, target);
    }
}

static int
col_arrangement_probe_release_ready(const col_arrangement_probe_t *probe);

int
col_arrangement_probe_release(col_arrangement_probe_t *probe)
{
    int rc;

    rc = col_arrangement_probe_release_ready(probe);
    if (rc != 0)
        return rc;
    col_arrangement_pin_release(&probe->arrangement_pin);
    rc = col_rel_source_reader_release(&probe->source_reader);
    if (rc != 0)
        return rc;
    memset(probe, 0, sizeof(*probe));
    return 0;
}

static int
col_arrangement_probe_release_ready(const col_arrangement_probe_t *probe)
{
    const wl_columnar_source_access_reader_t *reader;
    col_rel_t *storage_owner = NULL;
    uint64_t storage_owner_identity = 0;
    uint64_t storage_owner_generation = 0;

    if (!probe || !probe->active || probe->identity != (uintptr_t)probe
        || !probe->arr || !probe->source || !probe->storage_owner)
        return EINVAL;
    reader = &probe->source_reader;
    /* Validate every source-reader precondition before changing the index
     * pin.  The reader itself keeps the gate nonzero between this check and
     * release, so a successful precheck makes the two releases atomic with
     * respect to all supported token states. */
    if (col_arrangement_probe_storage_owner_resolve(probe->source,
        &storage_owner, &storage_owner_identity, &storage_owner_generation)
        != 0
        || storage_owner != probe->storage_owner
        || storage_owner_identity != probe->storage_owner_identity
        || storage_owner_generation != probe->storage_owner_generation
        || reader->owner != &storage_owner->source_access
        || reader->identity != (uintptr_t)reader || !reader->owner
        || (!reader->transferable
        && !wl_columnar_source_access_reader_thread_equal(reader))
        || (reader->transferable && reader->thread_valid)
        || !wl_columnar_source_access_gate_busy(reader->owner))
        return EINVAL;
    return 0;
}

void
col_arrangement_probe_bundle_init(col_arrangement_probe_bundle_t *bundle)
{
    if (!bundle)
        return;
    memset(bundle, 0, sizeof(*bundle));
    bundle->identity = (uintptr_t)bundle;
    bundle->active = true;
}

#ifdef WL_COLUMNAR_ARRANGEMENT_PROBE_TEST_HOOKS
static int wl_columnar_arrangement_probe_test_failure_rc;

void
wl_columnar_arrangement_probe_test_fail_next_acquire(int rc)
{
    wl_columnar_arrangement_probe_test_failure_rc = rc;
}

void
wl_columnar_arrangement_probe_test_clear(void)
{
    wl_columnar_arrangement_probe_test_failure_rc = 0;
}

void
wl_columnar_arrangement_test_force_stale(wl_col_session_t *sess,
    const char *rel_name)
{
    if (!sess || !rel_name)
        return;
    /* Change only the generation observed by an active producer.  The lease
     * and indexed storage remain intact so cleanup can be asserted. */
    for (uint32_t i = 0; i < sess->arr_count; i++) {
        col_arr_entry_t *entry = &sess->arr_entries[i];
        if (entry->rel_name && strcmp(entry->rel_name, rel_name) == 0) {
            entry->arr.generation++;
            if (entry->arr.generation == 0)
                entry->arr.generation = 1;
            return;
        }
    }
}

static int
wl_columnar_arrangement_probe_test_take_failure(void)
{
    int rc = wl_columnar_arrangement_probe_test_failure_rc;
    wl_columnar_arrangement_probe_test_failure_rc = 0;
    return rc;
}
#endif

int
col_arrangement_probe_bundle_acquire_primary(
    col_arrangement_probe_bundle_t *bundle, wl_session_t *sess,
    const col_rel_t *source, const uint32_t *key_cols, uint32_t key_count,
    col_arrangement_probe_t **out_probe)
{
    col_arrangement_probe_t acquired = { 0 };
    col_arrangement_probe_bundle_slot_t *slot;
    int rc;

    if (!bundle || !sess || !source || !key_cols || key_count == 0
        || !out_probe || bundle->identity != (uintptr_t)bundle
        || !bundle->active)
        return EINVAL;
    *out_probe = NULL;
#ifdef WL_COLUMNAR_ARRANGEMENT_PROBE_TEST_HOOKS
    rc = wl_columnar_arrangement_probe_test_take_failure();
    if (rc != 0)
        return rc;
#endif
    if (bundle->count >= COL_ARRANGEMENT_PROBE_BUNDLE_MAX) {
        rc = col_arrangement_probe_bundle_release(bundle);
        if (rc != 0)
            return rc;
        col_arrangement_probe_bundle_init(bundle);
        return EOVERFLOW;
    }
    rc = col_session_acquire_primary_arrangement_probe(sess, source,
            key_cols, key_count, &acquired);
    if (rc != 0)
        return rc;
    slot = &bundle->slots[bundle->count];
    slot->probe = acquired;
    /* The primary helper binds address-sensitive tokens to its caller's
     * stack object. Rebase both identities after moving the lease into the
     * operation-owned bundle slot. */
    slot->probe.identity = (uintptr_t)&slot->probe;
    slot->probe.source_reader.identity
        = (uintptr_t)&slot->probe.source_reader;
    slot->ref_count = 1;
    bundle->count++;
    *out_probe = &slot->probe;
    return 0;
}

int
col_arrangement_probe_bundle_acquire(col_arrangement_probe_bundle_t *bundle,
    wl_session_t *sess, col_arrangement_t *arr, const col_rel_t *source,
    col_arrangement_probe_t **out_probe)
{
    col_arrangement_probe_bundle_slot_t *slot;
    int rc;

    if (out_probe)
        *out_probe = NULL;
    if (!bundle || bundle->identity != (uintptr_t)bundle || !bundle->active
        || !sess || !arr || !source)
        return EINVAL;
    for (uint32_t i = 0; i < bundle->count; i++) {
        slot = &bundle->slots[i];
        if (slot->probe.active && slot->probe.arr == arr
            && slot->probe.source == source) {
            if (slot->ref_count == UINT32_MAX)
                return EOVERFLOW;
            slot->ref_count++;
            if (out_probe)
                *out_probe = &slot->probe;
            return 0;
        }
    }
    if (bundle->count >= COL_ARRANGEMENT_PROBE_BUNDLE_MAX) {
        int rollback_rc = col_arrangement_probe_bundle_release(bundle);
        if (rollback_rc != 0)
            return rollback_rc;
        col_arrangement_probe_bundle_init(bundle);
        return EOVERFLOW;
    }

    slot = &bundle->slots[bundle->count];
    memset(slot, 0, sizeof(*slot));
    rc = col_session_acquire_arrangement_probe(sess, arr, source,
            &slot->probe);
    if (rc != 0) {
        if (slot->probe.active)
            (void)col_arrangement_probe_release(&slot->probe);
        memset(slot, 0, sizeof(*slot));
        /* Bundle acquisition is transactional: a later dependency failure
         * must not leave earlier operation leases live. */
        if (bundle->count > 0) {
            int rollback_rc = col_arrangement_probe_bundle_release(bundle);
            if (rollback_rc != 0)
                return rollback_rc;
            col_arrangement_probe_bundle_init(bundle);
        }
        return rc;
    }
    slot->ref_count = 1;
    bundle->count++;
    if (out_probe)
        *out_probe = &slot->probe;
    return 0;
}

static int
col_arrangement_probe_dependency_release_ready(
    const col_arrangement_probe_dependency_t *dependency)
{
    col_rel_t *owner = NULL;
    if (!dependency || dependency->ref_count == 0 || !dependency->relation
        || !dependency->storage_owner
        || col_rel_storage_owner_resolve(dependency->relation, &owner) != 0
        || owner != dependency->storage_owner
        || owner->relation_identity != dependency->storage_owner_identity
        || owner->storage_generation
        != dependency->storage_owner_generation)
        return EINVAL;
    if (dependency->reader.identity != (uintptr_t)&dependency->reader
        || dependency->reader.owner != &owner->source_access
        || (!dependency->reader.transferable
        && !wl_columnar_source_access_reader_thread_equal(
            &dependency->reader))
        || (dependency->reader.transferable && dependency->reader.thread_valid)
        || !wl_columnar_source_access_gate_busy(dependency->reader.owner))
        return EINVAL;
    return 0;
}

int
col_arrangement_probe_bundle_acquire_dependency(
    col_arrangement_probe_bundle_t *bundle, const col_rel_t *relation)
{
    col_rel_t *owner = NULL;
    int rc;

    if (!bundle || bundle->identity != (uintptr_t)bundle || !bundle->active
        || !relation)
        return EINVAL;
#ifdef WL_COLUMNAR_ARRANGEMENT_PROBE_TEST_HOOKS
    rc = wl_columnar_arrangement_probe_test_take_failure();
    if (rc != 0)
        return rc;
#endif
    rc = col_rel_storage_owner_resolve(relation, &owner);
    if (rc != 0)
        return rc;
    for (uint32_t i = 0; i < bundle->dependency_count; i++) {
        col_arrangement_probe_dependency_t *dependency
            = &bundle->dependencies[i];
        if (dependency->ref_count > 0
            && dependency->storage_owner == owner) {
            if (dependency->ref_count == UINT32_MAX)
                return EOVERFLOW;
            dependency->ref_count++;
            return 0;
        }
    }
    if (bundle->dependency_count >= COL_ARRANGEMENT_PROBE_DEPENDENCY_MAX) {
        rc = col_arrangement_probe_bundle_release(bundle);
        if (rc != 0)
            return rc;
        col_arrangement_probe_bundle_init(bundle);
        return EOVERFLOW;
    }
    col_arrangement_probe_dependency_t *dependency
        = &bundle->dependencies[bundle->dependency_count];
    memset(dependency, 0, sizeof(*dependency));
    rc = col_rel_source_reader_acquire(relation, &dependency->reader);
    if (rc != 0)
        goto rollback;
    if (col_rel_storage_owner_resolve(relation, &owner) != 0
        || owner->relation_identity == 0
        || owner->storage_generation == 0) {
        rc = EBUSY;
        (void)col_rel_source_reader_release(&dependency->reader);
        goto rollback;
    }
    dependency->relation = relation;
    dependency->storage_owner = owner;
    dependency->storage_owner_identity = owner->relation_identity;
    dependency->storage_owner_generation = owner->storage_generation;
    dependency->ref_count = 1;
    bundle->dependency_count++;
    return 0;

rollback:
    memset(dependency, 0, sizeof(*dependency));
    if (bundle->count > 0 || bundle->dependency_count > 0) {
        int rollback_rc = col_arrangement_probe_bundle_release(bundle);
        if (rollback_rc != 0)
            return rollback_rc;
        col_arrangement_probe_bundle_init(bundle);
    }
    return rc;
}

int
col_arrangement_probe_bundle_release(col_arrangement_probe_bundle_t *bundle)
{
    int rc;

    if (!bundle || bundle->identity != (uintptr_t)bundle || !bundle->active)
        return EINVAL;
    for (uint32_t i = bundle->count; i > 0; i--) {
        col_arrangement_probe_bundle_slot_t *slot = &bundle->slots[i - 1u];
        if (slot->ref_count == 0 || !slot->probe.active)
            continue;
        rc = col_arrangement_probe_release_ready(&slot->probe);
        if (rc != 0)
            return rc;
    }
    for (uint32_t i = bundle->dependency_count; i > 0; i--) {
        if (bundle->dependencies[i - 1u].ref_count == 0)
            continue;
        rc = col_arrangement_probe_dependency_release_ready(
            &bundle->dependencies[i - 1u]);
        if (rc != 0)
            return rc;
    }
    for (uint32_t i = bundle->count; i > 0; i--) {
        col_arrangement_probe_bundle_slot_t *slot = &bundle->slots[i - 1u];
        if (slot->ref_count == 0 || !slot->probe.active)
            continue;
        rc = col_arrangement_probe_release(&slot->probe);
        if (rc != 0)
            return rc;
        memset(slot, 0, sizeof(*slot));
    }
    for (uint32_t i = bundle->dependency_count; i > 0; i--) {
        col_arrangement_probe_dependency_t *dependency
            = &bundle->dependencies[i - 1u];
        if (dependency->ref_count == 0)
            continue;
        rc = col_rel_source_reader_release(&dependency->reader);
        if (rc != 0)
            return rc;
        memset(dependency, 0, sizeof(*dependency));
    }
    memset(bundle, 0, sizeof(*bundle));
    return 0;
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

static void sarr_release(col_sorted_arr_t *sarr);
static void sarr_mark_deferred(col_sorted_arr_t *sarr);

void
col_session_invalidate_arrangements(wl_session_t *sess, const char *rel_name)
{
    if (!sess || !rel_name)
        return;
    wl_col_session_t *cs = COL_SESSION(sess);
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (cs->arr_entries[i].rel_name
            && strcmp(cs->arr_entries[i].rel_name, rel_name) == 0) {
            if (cs->arr_entries[i].pin_count > 0)
                cs->arr_entries[i].rebuild_deferred = true;
            else
                arr_entry_mark_unbuilt(&cs->arr_entries[i]);
        }
    }

    /* Sorted LFTJ copies have the same lifetime contract as primary
     * arrangements: preserve a pinned buffer, otherwise retire it now. */
    for (uint32_t i = 0; i < cs->sarr_count; i++) {
        col_sorted_arr_entry_t *entry = &cs->sarr_entries[i];
        if (!entry->rel_name || strcmp(entry->rel_name, rel_name) != 0)
            continue;
        if (entry->sarr.pin_count > 0) {
            sarr_mark_deferred(&entry->sarr);
        } else {
            sarr_release(&entry->sarr);
            entry->sarr.nrows = 0;
            entry->sarr.indexed_rows = 0;
            memset(&entry->sarr.source_snapshot, 0,
                sizeof(entry->sarr.source_snapshot));
        }
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
            col_diff_arr_entry_t *entry = &cs->diff_arr_entries[i];
            if (entry->pin_count > 0 || entry->transaction_pending
                || entry->generation == UINT64_MAX)
                entry->invalidation_deferred = true;
            else
                diff_entry_reset(entry);
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
        col_arr_detach_memory_governor(&e->arr);
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
        /* A changed source token requires a complete rebuild even when the
         * row count is unchanged. */
        bool snapshot_match = wl_columnar_relation_snapshot_equal(
            e->source_snapshot, wl_columnar_relation_snapshot(delta_rel));
        if (!snapshot_match || e->arr.indexed_rows != delta_rel->nrows) {
            if (delta_rel->nrows > 0) {
                if (arr_build_full(&e->arr, delta_rel) != 0)
                    return NULL;
            } else {
                arr_free_contents(&e->arr);
            }
            e->source_snapshot = wl_columnar_relation_snapshot(delta_rel);
        }
        return &e->arr;
    }

    /* Not found: grow cache and create new entry. */
    if (cs->darr_count >= cs->darr_cap) {
        uint32_t new_cap = cs->darr_cap ? cs->darr_cap * 2u : 4u;
        if (!arr_entries_grow(&cs->darr_entries, &cs->darr_cap,
            cs->darr_count, new_cap))
            return NULL;
    }

    col_arr_entry_t *e = &cs->darr_entries[cs->darr_count];
    memset(e, 0, sizeof(*e));
    wl_columnar_memory_reservation_init(&e->arr.reservation);

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
    e->arr.ledger = &cs->mem_ledger; /* Issue #1380 */
    col_arr_attach_memory_governor(&e->arr, cs->memory_governor);
    cs->darr_count++;

    /* Initial build. */
    if (delta_rel->nrows > 0 && arr_build_full(&e->arr, delta_rel) != 0) {
        arr_free_contents(&e->arr);
        col_arr_detach_memory_governor(&e->arr);
        cs->darr_count--;
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    e->source_snapshot = wl_columnar_relation_snapshot(delta_rel);
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
 * the filtered relation because its source token changed (Issue #1438;
 * the row-count check is the second guard, not the contract).
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
        bool snapshot_match = wl_columnar_relation_snapshot_equal(
            e->source_snapshot, wl_columnar_relation_snapshot(filtered_rel));
        if (!snapshot_match || e->arr.indexed_rows != filtered_rel->nrows) {
            if (filtered_rel->nrows > 0) {
                if (arr_build_full(&e->arr, filtered_rel) != 0)
                    return NULL;
            } else {
                arr_free_contents(&e->arr);
            }
            e->source_snapshot = wl_columnar_relation_snapshot(filtered_rel);
        }
        return &e->arr;
    }

    /* Not found: grow cache and create new entry. */
    if (cs->filt_arr_count >= cs->filt_arr_cap) {
        uint32_t new_cap = cs->filt_arr_cap ? cs->filt_arr_cap * 2u : 4u;
        if (!filt_arr_entries_grow(&cs->filt_arr_entries,
            &cs->filt_arr_cap, cs->filt_arr_count, new_cap))
            return NULL;
    }

    col_filt_arr_entry_t *e = &cs->filt_arr_entries[cs->filt_arr_count];
    memset(e, 0, sizeof(*e));
    wl_columnar_memory_reservation_init(&e->arr.reservation);

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
    e->arr.ledger = &cs->mem_ledger; /* Issue #1380 */
    col_arr_attach_memory_governor(&e->arr, cs->memory_governor);
    cs->filt_arr_count++;

    if (filtered_rel->nrows > 0
        && arr_build_full(&e->arr, filtered_rel) != 0) {
        arr_free_contents(&e->arr);
        col_arr_detach_memory_governor(&e->arr);
        cs->filt_arr_count--;
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    e->source_snapshot = wl_columnar_relation_snapshot(filtered_rel);
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
        col_arr_detach_memory_governor(&cs->filt_arr_entries[i].arr);
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
/*
 * sarr_release: free sorted[] and credit its ARRANGEMENT charge
 * (Issue #1380).
 */
static void
sarr_release(col_sorted_arr_t *sarr)
{
    if (!sarr || sarr->pin_count != 0)
        return;
    if (sarr->ledger && sarr->ledger_bytes > 0)
        wl_mem_ledger_free(sarr->ledger, WL_MEM_SUBSYS_ARRANGEMENT,
            sarr->ledger_bytes);
    sarr->ledger_bytes = 0;
    free(sarr->sorted);
    sarr->sorted = NULL;
}

#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
#if defined(_MSC_VER)
#define WL_COLUMNAR_ARRANGEMENT_LFTJ_TLS __declspec(thread)
#else
#define WL_COLUMNAR_ARRANGEMENT_LFTJ_TLS _Thread_local
#endif
static WL_COLUMNAR_ARRANGEMENT_LFTJ_TLS bool
wl_columnar_arrangement_sorted_fail_pending,
wl_columnar_arrangement_sorted_fail_consumed;
void
wl_columnar_arrangement_sorted_test_fail_next_allocation(void)
{
    wl_columnar_arrangement_sorted_fail_pending = true;
    wl_columnar_arrangement_sorted_fail_consumed = false;
}
bool
wl_columnar_arrangement_sorted_test_allocation_failure_consumed(void)
{
    return wl_columnar_arrangement_sorted_fail_consumed;
}
void
wl_columnar_arrangement_sorted_test_clear_allocation_failure(void)
{
    wl_columnar_arrangement_sorted_fail_pending = false;
    wl_columnar_arrangement_sorted_fail_consumed = false;
}
#endif
static int64_t *
wl_columnar_arrangement_sorted_allocate(size_t bytes)
{
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
    if (wl_columnar_arrangement_sorted_fail_pending) {
        wl_columnar_arrangement_sorted_fail_pending = false;
        wl_columnar_arrangement_sorted_fail_consumed = true;
        return NULL;
    }
#endif
    return malloc(bytes);
}

static int
sarr_build(col_sorted_arr_t *sarr, const col_rel_t *rel, uint32_t key_col)
{
    col_sorted_arr_t fresh;
    int64_t *old_sorted;
    uint64_t old_ledger_bytes;

    if (!sarr || !rel || sarr->pin_count != 0)
        return EBUSY;
    memset(&fresh, 0, sizeof(fresh));
    fresh.ncols = rel->ncols;
    fresh.key_col = key_col;
    fresh.ledger = sarr->ledger;

    if (rel->nrows == 0) {
        fresh.source_snapshot = wl_columnar_relation_snapshot(rel);
        old_sorted = sarr->sorted;
        old_ledger_bytes = sarr->ledger_bytes;
        sarr->sorted = NULL;
        sarr->ledger_bytes = 0;
        free(old_sorted);
        if (sarr->ledger && old_ledger_bytes > 0)
            wl_mem_ledger_free(sarr->ledger, WL_MEM_SUBSYS_ARRANGEMENT,
                old_ledger_bytes);
        *sarr = fresh;
        return 0;
    }

    size_t bytes = (size_t)rel->nrows * rel->ncols * sizeof(int64_t);
    fresh.sorted = wl_columnar_arrangement_sorted_allocate(bytes);
    if (!fresh.sorted)
        return ENOMEM;
    if (fresh.ledger) {
        wl_mem_ledger_alloc(fresh.ledger, WL_MEM_SUBSYS_ARRANGEMENT, bytes);
        fresh.ledger_bytes = bytes;
    }

    for (uint32_t r = 0; r < rel->nrows; r++)
        col_rel_row_copy_out(rel, r, fresh.sorted + (size_t)r * rel->ncols);
    /* #465: stable LSD radix sort by the single key column replaces the
     * platform-specific qsort_r path that did not exist on Android NDK
     * bionic libc.  Failure here is malloc-fail in the radix scratch
     * buffer; on that path sarr->sorted is unmodified and we propagate
     * ENOMEM so the caller can react. */
    if (col_radix_sort_rows_by_key(fresh.sorted, rel->nrows, rel->ncols,
        key_col) != 0) {
        sarr_release(&fresh);
        return ENOMEM;
    }
    fresh.nrows = rel->nrows;
    fresh.indexed_rows = rel->nrows;
    fresh.source_snapshot = wl_columnar_relation_snapshot(rel);
    old_sorted = sarr->sorted;
    old_ledger_bytes = sarr->ledger_bytes;
    sarr->sorted = NULL;
    sarr->ledger_bytes = 0;
    free(old_sorted);
    if (sarr->ledger && old_ledger_bytes > 0)
        wl_mem_ledger_free(sarr->ledger, WL_MEM_SUBSYS_ARRANGEMENT,
            old_ledger_bytes);
    *sarr = fresh;
    return 0;
}

static col_sorted_arr_entry_t *
sarr_find_entry(wl_col_session_t *cs, const char *rel_name, uint32_t key_col)
{
    if (!cs || !rel_name)
        return NULL;
    for (uint32_t i = 0; i < cs->sarr_count; i++) {
        col_sorted_arr_entry_t *entry = &cs->sarr_entries[i];
        if (entry->key_col == key_col
            && strcmp(entry->rel_name, rel_name) == 0)
            return entry;
    }
    return NULL;
}

static void
sarr_mark_deferred(col_sorted_arr_t *sarr)
{
    if (!sarr)
        return;
    sarr->rebuild_deferred = true;
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
    col_sorted_arr_entry_t *e = sarr_find_entry(cs, rel_name, key_col);
    if (e) {
        /* indexed_rows is only a progress marker; freshness is the source
         * snapshot. */
        if (!wl_columnar_relation_snapshot_equal(e->sarr.source_snapshot,
            wl_columnar_relation_snapshot(rel))
            || e->sarr.indexed_rows != rel->nrows
            || e->sarr.rebuild_deferred) {
            if (e->sarr.pin_count != 0) {
                sarr_mark_deferred(&e->sarr);
                return NULL;
            }
            if (sarr_build(&e->sarr, rel, key_col) != 0)
                return NULL;
        }
        return &e->sarr;
    }

    /* Not found: grow cache and create new entry. */
    if (cs->sarr_count >= cs->sarr_cap) {
        if (cs->sarr_active_pins != 0)
            return NULL;
        uint32_t new_cap = cs->sarr_cap ? cs->sarr_cap * 2u : 4u;
        col_sorted_arr_entry_t *ne = (col_sorted_arr_entry_t *)realloc(
            cs->sarr_entries, new_cap * sizeof(col_sorted_arr_entry_t));
        if (!ne)
            return NULL;
        cs->sarr_entries = ne;
        cs->sarr_cap = new_cap;
    }

    col_sorted_arr_entry_t *new_entry = &cs->sarr_entries[cs->sarr_count];
    memset(new_entry, 0, sizeof(*new_entry));

    new_entry->rel_name = wl_strdup(rel_name);
    if (!new_entry->rel_name)
        return NULL;

    new_entry->key_col = key_col;
    new_entry->sarr.key_col = key_col;
    new_entry->sarr.ledger = &cs->mem_ledger; /* Issue #1380 */
    cs->sarr_count++;

    if (sarr_build(&new_entry->sarr, rel, key_col) != 0) {
        cs->sarr_count--;
        free(new_entry->rel_name);
        memset(new_entry, 0, sizeof(*new_entry));
        return NULL;
    }
    return &new_entry->sarr;
}

/*
 * col_session_free_sorted_arrangements:
 *
 * Free all entries in the sorted arrangement cache and reset it.
 * Called from col_session_destroy.
 */
int
col_sorted_arrangement_probe_release(col_sorted_arrangement_probe_t *probe)
{
    col_sorted_arr_entry_t *entry;
    wl_col_session_t *session;
    int rc;

    if (!probe || !probe->active || probe->identity != (uintptr_t)probe
        || !probe->entry || !probe->arr || !probe->session
        || probe->entry->sarr.pin_count == 0
        || probe->entry->sarr.pin_count > probe->session->sarr_active_pins)
        return EINVAL;
    if (!probe->source_reader.owner
        || probe->source_reader.identity != (uintptr_t)&probe->source_reader)
        return EINVAL;
    entry = probe->entry;
    session = probe->session;
    /* Release the source gate first.  This is the only fallible part of
     * probe release; do not mutate pin/deferred-generation state until it has
     * succeeded, so a caller can retry a failed release transactionally. */
    rc = col_rel_source_reader_release(&probe->source_reader);
    if (rc != 0)
        return rc;
    if (entry->sarr.pin_count > 0)
        entry->sarr.pin_count--;
    if (session->sarr_active_pins > 0)
        session->sarr_active_pins--;
    if (entry->sarr.pin_count == 0 && entry->sarr.rebuild_deferred) {
        entry->sarr.rebuild_deferred = false;
        entry->sarr.indexed_rows = 0;
        entry->sarr.nrows = 0;
        memset(&entry->sarr.source_snapshot, 0,
            sizeof(entry->sarr.source_snapshot));
    }
    memset(probe, 0, sizeof(*probe));
    return 0;
}

static int
col_sorted_arrangement_probe_source_reader_ready(
    const col_rel_t *source, const wl_columnar_source_access_reader_t *reader)
{
    col_rel_t *storage_owner = NULL;
    wl_columnar_source_access_gate_t *descriptor_access;
    int rc;

    if (!source || !reader || !reader->owner)
        return EINVAL;
    rc = col_rel_storage_owner_resolve(source, &storage_owner);
    if (rc != 0)
        return rc;
    descriptor_access
        = (wl_columnar_source_access_gate_t *)&source->descriptor_access;
    if (!storage_owner || reader->owner != &storage_owner->source_access
        || reader->secondary_owner != descriptor_access
        || reader->identity != (uintptr_t)reader
        || (!reader->transferable
        && !wl_columnar_source_access_reader_thread_equal(reader))
        || (reader->transferable && reader->thread_valid)
        || !wl_columnar_source_access_gate_busy(reader->owner)
        || !wl_columnar_source_access_gate_busy(reader->secondary_owner))
        return EINVAL;
    return 0;
}

int
col_session_acquire_sorted_arrangement_probe_with_source_reader(
    wl_session_t *session, const col_rel_t *source, uint32_t key_col,
    col_sorted_arrangement_probe_t *probe,
    wl_columnar_source_access_reader_t *source_reader)
{
    wl_col_session_t *cs;
    col_sorted_arr_entry_t *entry;
    col_rel_t *resolved;
    col_relation_snapshot_t snapshot;
    wl_columnar_source_access_reader_t *reader = NULL;
    int rc;

    if (!session || !source || !source->name || !probe
        || probe->active || probe->identity != 0
        || probe->source_reader.owner
        || probe->source_reader.identity != 0
        || probe->source_reader.thread_valid
        || probe->source_reader.transferable)
        return EINVAL;
    cs = COL_SESSION(session);
    resolved = session_find_rel(cs, source->name);
    if (resolved != source)
        return EBUSY;
    if (key_col >= source->ncols)
        return EINVAL;
    if (source_reader) {
        rc = col_sorted_arrangement_probe_source_reader_ready(source,
                source_reader);
        if (rc != 0)
            return rc;
        reader = source_reader;
    } else {
        reader = &probe->source_reader;
    }
    memset(probe, 0, sizeof(*probe));
    if (!source_reader) {
        rc = col_rel_source_reader_acquire(source, &probe->source_reader);
        if (rc != 0)
            return rc;
    }
    snapshot = wl_columnar_relation_snapshot(source);
    entry = sarr_find_entry(cs, source->name, key_col);
    if (entry) {
        bool stale = !wl_columnar_relation_snapshot_equal(
            entry->sarr.source_snapshot, snapshot)
            || entry->sarr.indexed_rows != source->nrows
            || entry->sarr.rebuild_deferred;
        if (stale && entry->sarr.pin_count != 0) {
            sarr_mark_deferred(&entry->sarr);
            rc = EBUSY;
            goto fail_reader;
        }
        if (stale && sarr_build(&entry->sarr, source, key_col) != 0) {
            rc = ENOMEM;
            goto fail_reader;
        }
    } else {
        if (cs->sarr_count >= cs->sarr_cap && cs->sarr_active_pins != 0) {
            rc = EBUSY;
            goto fail_reader;
        }
        if (col_session_get_sorted_arrangement(cs, source->name, key_col)
            == NULL) {
            rc = ENOMEM;
            goto fail_reader;
        }
        entry = sarr_find_entry(cs, source->name, key_col);
        if (!entry) {
            rc = EINVAL;
            goto fail_reader;
        }
    }
    if (!wl_columnar_relation_snapshot_equal(
            entry->sarr.source_snapshot, snapshot)) {
        rc = EBUSY;
        goto fail_reader;
    }
    if (entry->sarr.pin_count == UINT32_MAX
        || cs->sarr_active_pins == UINT32_MAX) {
        rc = EOVERFLOW;
        goto fail_reader;
    }
    entry->sarr.pin_count++;
    cs->sarr_active_pins++;
    probe->entry = entry;
    probe->arr = &entry->sarr;
    probe->session = cs;
    probe->source = source;
    probe->source_snapshot = snapshot;
    if (source_reader) {
        probe->source_reader = *reader;
        probe->source_reader.identity = (uintptr_t)&probe->source_reader;
        memset(reader, 0, sizeof(*reader));
    }
    probe->identity = (uintptr_t)probe;
    probe->active = true;
    return 0;

fail_reader:
    if (!source_reader && probe->source_reader.owner) {
        int release_rc = col_rel_source_reader_release(&probe->source_reader);
        if (release_rc != 0)
            return release_rc;
    }
    memset(probe, 0, sizeof(*probe));
    return rc;
}

int
col_session_acquire_sorted_arrangement_probe(wl_session_t *session,
    const col_rel_t *source, uint32_t key_col,
    col_sorted_arrangement_probe_t *probe)
{
    return col_session_acquire_sorted_arrangement_probe_with_source_reader(
        session, source, key_col, probe, NULL);
}

void
col_session_free_sorted_arrangements(wl_col_session_t *cs)
{
    if (!cs)
        return;
    if (cs->sarr_active_pins != 0) {
        fprintf(stderr,
            "wirelog: fatal sorted-arrangement teardown with %u active "
            "probe(s)\n", cs->sarr_active_pins);
        abort();
    }
    for (uint32_t i = 0; i < cs->sarr_count; i++) {
        free(cs->sarr_entries[i].rel_name);
        sarr_release(&cs->sarr_entries[i].sarr);
    }
    free(cs->sarr_entries);
    cs->sarr_entries = NULL;
    cs->sarr_count = 0;
    cs->sarr_cap = 0;
}

/* ======================================================================== */
/* Differential Arrangement Registry (Issue #263)                           */
/* ======================================================================== */

static bool
diff_registry_has_pins(const wl_col_session_t *cs)
{
    if (!cs)
        return false;
    for (uint32_t i = 0; i < cs->diff_arr_count; i++)
        if (cs->diff_arr_entries[i].pin_count > 0)
            return true;
    return false;
}

static uint64_t
diff_entry_next_generation(uint64_t generation)
{
    return generation == UINT64_MAX ? UINT64_MAX : generation + 1u;
}

void
wl_columnar_arrangement_diff_reset_state(col_diff_arrangement_t *arr)
{
    if (!arr)
        return;
    if (arr->ht_head)
        memset(arr->ht_head, 0,
            (size_t)arr->nbuckets * sizeof(*arr->ht_head));
    if (arr->ht_next)
        memset(arr->ht_next, 0,
            (size_t)arr->ht_cap * sizeof(*arr->ht_next));
    arr->base_nrows = 0;
    arr->current_nrows = 0;
    arr->indexed_rows = 0;
    arr->source_snapshot = (col_relation_snapshot_t){ 0, 0, 0 };
}

static void
diff_entry_reset(col_diff_arr_entry_t *entry)
{
    if (!entry || !entry->diff_arr)
        return;
    wl_columnar_arrangement_diff_reset_state(entry->diff_arr);
    entry->generation = diff_entry_next_generation(entry->generation);
}

/*
 * col_session_get_diff_arrangement:
 *
 * Return (or lazily create) a differential arrangement for `rel_name`
 * keyed on `key_cols[0..key_count)`.  The arrangement persists across
 * iterations within an epoch and the caller indexes only rows beyond
 * `indexed_rows`, but only while its freshness token still matches
 * `source_rel`: on a relation identity or generation mismatch the hash
 * chains and progress counters are cleared and the token is poisoned, so
 * the caller re-indexes from row 0 (Issue #1438).
 *
 * Returns NULL on allocation failure.
 */
col_diff_arrangement_t *
col_session_get_diff_arrangement(wl_col_session_t *cs, const char *rel_name,
    const col_rel_t *source_rel,
    const uint32_t *key_cols, uint32_t key_count)
{
    if (!cs || !rel_name || !source_rel || key_count == 0)
        return NULL;
    if (cs->diff_txn_count > 0)
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
        if (match) {
            if (e->transaction_pending)
                return NULL;
            if (e->generation == UINT64_MAX)
                return NULL;
            col_diff_arrangement_t *arr = e->diff_arr;
            if (!wl_columnar_relation_snapshot_equal(
                    arr->source_snapshot,
                    wl_columnar_relation_snapshot(source_rel))) {
                if (e->pin_count > 0) {
                    e->invalidation_deferred = true;
                    return NULL;
                }
                diff_entry_reset(e);
            }
            return e->diff_arr;
        }
    }

    /* Not found: grow registry and create new entry.  A differential pin
     * stores an entry address, so never relocate the flat registry beneath
     * an active lease. */
    if (cs->diff_arr_count >= cs->diff_arr_cap) {
        if (diff_registry_has_pins(cs))
            return NULL;
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
    e->generation = 1u;

    wl_columnar_memory_admission_status_t admission_status =
        WL_COLUMNAR_MEMORY_ADMISSION_OK;
    e->diff_arr = col_diff_arrangement_create_with_memory_governor_status(
        key_cols, key_count, 0, cs->memory_governor, &admission_status);
    if (!e->diff_arr) {
        if (admission_status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
            cs->memory_budget_denied = true;
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    e->diff_arr->source_snapshot = (col_relation_snapshot_t){ 0, 0, 0 };
    col_diff_arrangement_attach_ledger(e->diff_arr, &cs->mem_ledger);
    cs->diff_arr_count++;
    return e->diff_arr;
}

int
col_session_pin_diff_arrangement(wl_col_session_t *cs, const char *rel_name,
    const col_rel_t *source_rel, const uint32_t *key_cols, uint32_t key_count,
    col_diff_arrangement_pin_t *pin)
{
    col_diff_arr_entry_t *entry = NULL;
    col_diff_arrangement_t *arr;
    col_relation_snapshot_t snapshot;

    if (!cs || !rel_name || !source_rel || !key_cols || key_count == 0
        || !pin)
        return EINVAL;
    if (!col_session_has_exact_relation(cs, source_rel, rel_name))
        return EINVAL;
    memset(pin, 0, sizeof(*pin));
    if (col_rel_source_reader_acquire(source_rel, &pin->source_reader) != 0)
        return EBUSY;
    arr = col_session_get_diff_arrangement(cs, rel_name, source_rel,
            key_cols, key_count);
    if (!arr) {
        (void)col_rel_source_reader_release(&pin->source_reader);
        return EBUSY;
    }
    for (uint32_t i = 0; i < cs->diff_arr_count; i++) {
        col_diff_arr_entry_t *candidate = &cs->diff_arr_entries[i];
        if (candidate->diff_arr == arr) {
            entry = candidate;
            break;
        }
    }
    if (!entry || entry->pin_count == UINT32_MAX) {
        (void)col_rel_source_reader_release(&pin->source_reader);
        return entry ? EOVERFLOW : EINVAL;
    }
    snapshot = wl_columnar_relation_snapshot(source_rel);
    if (arr->source_snapshot.relation_identity == 0)
        arr->source_snapshot = snapshot;
    if (!wl_columnar_relation_snapshot_equal(arr->source_snapshot, snapshot)
        || entry->generation == UINT64_MAX) {
        (void)col_rel_source_reader_release(&pin->source_reader);
        return EBUSY;
    }
    entry->pin_count++;
    pin->entry = entry;
    pin->arr = arr;
    pin->session = cs;
    pin->source = source_rel;
    pin->source_snapshot = snapshot;
    pin->generation = entry->generation;
    pin->active = true;
    return 0;
}

void
col_diff_arrangement_pin_release(col_diff_arrangement_pin_t *pin)
{
    col_diff_arr_entry_t *entry;
    int rc;

    if (!pin || !pin->active || !pin->entry)
        return;
    entry = pin->entry;
    if (entry->pin_count > 0)
        entry->pin_count--;
    if (entry->pin_count == 0 && entry->invalidation_deferred) {
        entry->invalidation_deferred = false;
        diff_entry_reset(entry);
    }
    rc = col_rel_source_reader_release(&pin->source_reader);
    (void)rc;
    memset(pin, 0, sizeof(*pin));
}

int
wl_columnar_arrangement_diff_txn_begin(wl_col_session_t *cs,
    const char *rel_name, const col_rel_t *source_rel,
    const uint32_t *key_cols, uint32_t key_count,
    wl_columnar_arrangement_diff_txn_t *txn)
{
    col_diff_arrangement_t **slot = NULL;
    uint32_t entry_index = UINT32_MAX;
    col_diff_arr_entry_t *entry = NULL;

    if (!cs || !rel_name || !source_rel || !key_cols || key_count == 0
        || !txn)
        return EINVAL;
    if (!col_session_has_exact_relation(cs, source_rel, rel_name))
        return EINVAL;
    if (cs->diff_txn_count > 0)
        return EBUSY;
    memset(txn, 0, sizeof(*txn));

    for (uint32_t i = 0; i < cs->diff_arr_count; i++) {
        entry = &cs->diff_arr_entries[i];
        if (entry->key_count != key_count
            || strcmp(entry->rel_name, rel_name) != 0)
            continue;
        bool match = true;
        for (uint32_t k = 0; k < key_count; k++) {
            if (entry->key_cols[k] != key_cols[k]) {
                match = false;
                break;
            }
        }
        if (match) {
            if (entry->pin_count > 0 || entry->transaction_pending)
                return EBUSY;
            entry->transaction_pending = true;
            entry_index = i;
            txn->entry = entry;
            slot = &entry->diff_arr;
            break;
        }
    }

    if (!slot) {
        if (cs->diff_arr_count >= cs->diff_arr_cap) {
            if (diff_registry_has_pins(cs))
                return EBUSY;
            uint32_t new_cap = cs->diff_arr_cap ? cs->diff_arr_cap * 2u : 4u;
            col_diff_arr_entry_t *entries = (col_diff_arr_entry_t *)realloc(
                cs->diff_arr_entries, new_cap * sizeof(*entries));
            if (!entries)
                return ENOMEM;
            cs->diff_arr_entries = entries;
            cs->diff_arr_cap = new_cap;
        }
        entry_index = cs->diff_arr_count;
        entry = &cs->diff_arr_entries[entry_index];
        memset(entry, 0, sizeof(*entry));
        entry->rel_name = wl_strdup(rel_name);
        entry->key_cols = (uint32_t *)malloc(
            (size_t)key_count * sizeof(*entry->key_cols));
        if (!entry->rel_name || !entry->key_cols) {
            free(entry->rel_name);
            free(entry->key_cols);
            memset(entry, 0, sizeof(*entry));
            return ENOMEM;
        }
        memcpy(entry->key_cols, key_cols,
            (size_t)key_count * sizeof(*entry->key_cols));
        entry->key_count = key_count;
        entry->generation = 1u;
        entry->transaction_pending = true;
        wl_columnar_memory_admission_status_t admission_status =
            WL_COLUMNAR_MEMORY_ADMISSION_OK;
        entry->diff_arr =
            col_diff_arrangement_create_with_memory_governor_status(
            key_cols, key_count, 0, cs->memory_governor,
            &admission_status);
        if (!entry->diff_arr) {
            if (admission_status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                cs->memory_budget_denied = true;
            free(entry->rel_name);
            free(entry->key_cols);
            memset(entry, 0, sizeof(*entry));
            return ENOMEM;
        }
        txn->entry = entry;
        txn->pending_entry = true;
        slot = &entry->diff_arr;
        cs->diff_arr_count++;
    }

    /* Both registry paths above establish the entry/slot pair.  Keep the
     * invariant explicit for static analysis before dereferencing either. */
    if (!entry || !slot)
        return EINVAL;

    txn->session = cs;
    txn->entry_index = entry_index;
    txn->entry_generation = txn->entry->generation;
    txn->persistent = *slot;
    wl_columnar_memory_admission_status_t admission_status =
        WL_COLUMNAR_MEMORY_ADMISSION_OK;
    txn->working = col_diff_arrangement_deep_copy_with_memory_governor_status(
        txn->persistent, cs->memory_governor, &admission_status);
    if (!txn->working) {
        if (admission_status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
            cs->memory_budget_denied = true;
        if (txn->pending_entry) {
            free(entry->rel_name);
            free(entry->key_cols);
            col_diff_arrangement_destroy(txn->persistent);
            memset(entry, 0, sizeof(*entry));
            cs->diff_arr_count--;
        } else {
            entry->transaction_pending = false;
        }
        memset(txn, 0, sizeof(*txn));
        return ENOMEM;
    }
    col_diff_arrangement_attach_ledger(txn->working,
        txn->pending_entry ? &cs->mem_ledger : txn->persistent->ledger);
    txn->slot = slot;
    cs->diff_txn_count++;

    if (!wl_columnar_relation_snapshot_equal(txn->working->source_snapshot,
        wl_columnar_relation_snapshot(source_rel))) {
        wl_columnar_arrangement_diff_reset_state(txn->working);
    }
    return 0;
}

int
wl_columnar_arrangement_diff_txn_commit(
    wl_columnar_arrangement_diff_txn_t *txn)
{
    col_diff_arr_entry_t *entry;

    if (!txn || !txn->session || !txn->persistent || !txn->working
        || txn->entry_index >= txn->session->diff_arr_cap)
        return EINVAL;
    entry = &txn->session->diff_arr_entries[txn->entry_index];
    if (entry->generation != txn->entry_generation
        || entry->pin_count > 0 || !entry->transaction_pending
        || entry->invalidation_deferred
        || entry->diff_arr != txn->persistent)
        return EBUSY;
    if (entry->generation == UINT64_MAX)
        return EOVERFLOW;
    entry->diff_arr = txn->working;
    col_diff_arrangement_destroy(txn->persistent);
    entry->transaction_pending = false;
    entry->generation++;
    if (txn->session->diff_txn_count > 0)
        txn->session->diff_txn_count--;
    memset(txn, 0, sizeof(*txn));
    return 0;
}

void
wl_columnar_arrangement_diff_txn_abort(
    wl_columnar_arrangement_diff_txn_t *txn)
{
    if (!txn)
        return;
    col_diff_arrangement_destroy(txn->working);
    if (txn->pending_entry && txn->session
        && txn->entry_index < txn->session->diff_arr_cap) {
        col_diff_arr_entry_t *entry
            = &txn->session->diff_arr_entries[txn->entry_index];
        free(entry->rel_name);
        free(entry->key_cols);
        col_diff_arrangement_destroy(txn->persistent);
        memset(entry, 0, sizeof(*entry));
        txn->session->diff_arr_count--;
    } else if (txn->session
        && txn->entry_index < txn->session->diff_arr_count) {
        col_diff_arr_entry_t *entry
            = &txn->session->diff_arr_entries[txn->entry_index];
        entry->transaction_pending = false;
        if (entry->invalidation_deferred) {
            entry->invalidation_deferred = false;
            diff_entry_reset(entry);
        }
    }
    if (txn->session && txn->session->diff_txn_count > 0)
        txn->session->diff_txn_count--;
    memset(txn, 0, sizeof(*txn));
}

/*
 * col_session_free_diff_arrangements:
 *
 * Free all entries in the differential arrangement registry and reset it.
 * Called from col_session_destroy and when switching evaluation modes.
 */
int
col_session_free_diff_arrangements(wl_col_session_t *cs)
{
    if (!cs)
        return EINVAL;
    if (diff_registry_has_pins(cs) || cs->diff_txn_count > 0)
        return EBUSY;
    for (uint32_t i = 0; i < cs->diff_arr_count; i++) {
        free(cs->diff_arr_entries[i].rel_name);
        free(cs->diff_arr_entries[i].key_cols);
        col_diff_arrangement_destroy(cs->diff_arr_entries[i].diff_arr);
    }
    free(cs->diff_arr_entries);
    cs->diff_arr_entries = NULL;
    cs->diff_arr_count = 0;
    cs->diff_arr_cap = 0;
    return 0;
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
        col_diff_arrangement_t cold = { 0 };
        cold.key_cols = src->diff_arr->key_cols;
        cold.key_count = src->diff_arr->key_count;
        cold.worker_id = src->diff_arr->worker_id;
        dst->diff_arr = col_diff_arrangement_deep_copy(&cold);
        if (!dst->diff_arr) {
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        dst->diff_arr->source_snapshot
            = (col_relation_snapshot_t){ 0, 0, 0 };
        dst->diff_arr->indexed_rows = 0;
        dst->diff_arr->base_nrows = 0;
        dst->diff_arr->current_nrows = 0;
    }
    dst->generation = src->generation ? src->generation : 1u;
    dst->pin_count = 0;
    dst->invalidation_deferred = false;

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

/* Distinct controlled probe contract: the embedded reader owns the descriptor
 * gate, while the separately allocated original reader owns the storage gate.
 * Neither live token is copied into another slot. */
static int
wl_columnar_arrangement_sorted_pair_ready(col_sorted_arrangement_probe_t *probe,
    wl_columnar_source_access_reader_t *reader, const col_rel_t *source)
{
    col_rel_t *owner = NULL;
    if (!probe || !reader || !source
        || col_rel_storage_owner_resolve(source, &owner) != 0
        || probe->source_reader.owner != &source->descriptor_access
        || probe->source_reader.secondary_owner
        || probe->source_reader.identity != (uintptr_t)&probe->source_reader
        || !wl_columnar_source_access_reader_thread_equal(&probe->source_reader)
        || reader->owner != &owner->source_access || reader->secondary_owner
        || reader->identity != (uintptr_t)reader
        || !wl_columnar_source_access_reader_thread_equal(
            reader)) return EINVAL;
    return 0;
}

int
wl_columnar_arrangement_sorted_probe_release_controlled(
    col_sorted_arrangement_probe_t *probe,
    wl_columnar_source_access_reader_t *reader, const col_rel_t *source,
    wl_evaluation_control_t *control)
{
    if (!probe || !reader) return EINVAL;
    if (probe->active && (probe->identity != (uintptr_t)probe
        || !probe->entry || !probe->session || probe->source != source
        || probe->arr != &probe->entry->sarr || !probe->arr->pin_count
        || probe->arr->pin_count > probe->session->sarr_active_pins))
        return EINVAL;
    /* Tokens themselves retain the actual release subphase after refusal. */
    int rc = 0;
    if (reader->owner) {
        rc = wl_columnar_relation_single_reader_release_controlled(reader,
                control);
        if (rc) return rc;
    }
    if (probe->source_reader.owner) {
        rc = wl_columnar_relation_single_reader_release_controlled(
            &probe->source_reader, control);
        if (rc) return rc;
    }
    if (control) {
        rc = wl_evaluation_control_charge(control, 32 + sizeof(*probe));
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    if (probe->active) {
        probe->arr->pin_count--;
        probe->session->sarr_active_pins--;
        if (!probe->arr->pin_count && probe->arr->rebuild_deferred) {
            probe->arr->rebuild_deferred = false;
            probe->arr->indexed_rows = probe->arr->nrows = 0;
            memset(&probe->arr->source_snapshot, 0,
                sizeof(probe->arr->source_snapshot));
        }
    }
    memset(probe, 0, sizeof(*probe));
    return control ? wl_evaluation_control_charge(control, 0) : 0;
}

static int
wl_columnar_arrangement_sorted_name_equal(wl_columnar_lftj_record_t *record,
    const char *a, const char *b, bool *equal)
{
    *equal = false;
    if (!a || !b) return 0;
    for (size_t i = 0;; i++) {
        int rc = wl_columnar_lftj_work(record, 2); if (rc) return rc;
        char av = a[i], bv = b[i];
        rc = wl_columnar_lftj_tail(record, 2, i + 1u); if (rc) return rc;
        if (av != bv) return 0;
        if (!av) {
            *equal = true; return 0;
        }
    }
}

static void
wl_columnar_arrangement_sorted_discard_fresh(wl_columnar_lftj_record_t *record)
{
    free(record->radix_temp); record->radix_temp = NULL;
    record->sort_bytes = 0;
    free(record->cache_fresh.sorted); record->cache_fresh.sorted = NULL;
    if (record->cache_ledger_installed) {
        wl_mem_ledger_free(record->cache_fresh.ledger,
            WL_MEM_SUBSYS_ARRANGEMENT, record->cache_fresh.ledger_bytes);
        record->cache_fresh.ledger_bytes = 0;
        record->cache_ledger_installed = false;
    }
    free(record->cache_name); record->cache_name = NULL;
}

int
wl_columnar_arrangement_sorted_acquire_controlled(
    wl_columnar_lftj_record_t *record, uint32_t input, uint32_t key_col)
{
    if (!record || !record->ready || input >= record->reader_count
        || !record->probes || !record->readers || !record->relations)
        return EINVAL;
    col_sorted_arrangement_probe_t *probe = &record->probes[input];
    wl_columnar_source_access_reader_t *reader = &record->readers[input];
    col_rel_t *source = record->relations[input];
    wl_col_session_t *session = record->session;
    int rc = wl_columnar_lftj_work(record, 32); if (rc) return rc;
    rc = wl_columnar_arrangement_sorted_pair_ready(probe, reader, source);
    if (rc) return rc;
    if (key_col >= source->ncols || probe->active || probe->identity)
        return EINVAL;
    rc = wl_columnar_lftj_tail(record, 2, input + 1u); if (rc) return rc;
    record->cache_entry = NULL;
    for (uint32_t i = 0; i < session->sarr_count; i++) {
        rc = wl_columnar_lftj_work(record, 8); if (rc) return rc;
        col_sorted_arr_entry_t *entry = &session->sarr_entries[i];
        bool key_equal = entry->key_col == key_col;
        rc = wl_columnar_lftj_tail(record, 2, i + 1u); if (rc) return rc;
        if (key_equal) {
            bool equal;
            rc = wl_columnar_arrangement_sorted_name_equal(record,
                    entry->rel_name, source->name, &equal);
            if (rc) return rc;
            if (equal) {
                record->cache_entry = entry; break;
            }
        }
    }
    col_relation_snapshot_t snapshot = wl_columnar_relation_snapshot(source);
    col_sorted_arr_entry_t *entry = record->cache_entry;
    bool fresh = entry && !entry->sarr.rebuild_deferred
        && entry->sarr.indexed_rows == source->nrows
        && wl_columnar_relation_snapshot_equal(entry->sarr.source_snapshot,
            snapshot);
    if (!fresh && entry && entry->sarr.pin_count) {
        rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
        entry->sarr.rebuild_deferred = true;
        return EBUSY;
    }
    if (!entry) {
        if (session->sarr_count == UINT32_MAX) return EOVERFLOW;
        if (session->sarr_count >= session->sarr_cap) {
            if (session->sarr_active_pins) return EBUSY;
            uint32_t capacity = session->sarr_cap ? session->sarr_cap * 2u : 4u;
            if (capacity < session->sarr_cap) return EOVERFLOW;
            uint64_t bytes;
            if (!wl_columnar_memory_size_mul(capacity,
                sizeof(col_sorted_arr_entry_t), &bytes) || bytes > SIZE_MAX)
                return EOVERFLOW;
            /* Actual libc relocation is an admitted allocator boundary,
             * not an unmetered engine copy disguised as one checkpoint. */
            for (uint64_t at = 0; at < bytes;) {
                uint64_t n = bytes - at; if (n > 128u) n = 128u;
                rc = wl_columnar_lftj_work(record, n); if (rc) return rc;
                at += n;
                rc = wl_columnar_lftj_tail(record, 2, at); if (rc) return rc;
            }
            rc = wl_columnar_lftj_work(record, 16); if (rc) return rc;
            record->registry_old = session->sarr_entries;
            record->registry_new = realloc(session->sarr_entries,
                    (size_t)bytes);
            if (!record->registry_new) {
                record->registry_old = NULL; return ENOMEM;
            }
            session->sarr_entries = record->registry_new;
            session->sarr_cap = capacity;
            record->registry_new = record->registry_old = NULL;
            rc = wl_columnar_lftj_tail(record, 2, capacity); if (rc) return rc;
        }
        entry = &session->sarr_entries[session->sarr_count];
        record->cache_entry = entry;
        uint64_t length = 0;
        do {
            rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
            char value = source->name[length++];
            rc = wl_columnar_lftj_tail(record, 2, length); if (rc) return rc;
            if (!value) break;
        } while (length < SIZE_MAX);
        if (!length || length == SIZE_MAX) return EOVERFLOW;
        rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
        record->cache_name = malloc((size_t)length);
        if (!record->cache_name) return ENOMEM;
        rc = wl_columnar_lftj_tail(record, 2, length); if (rc) goto discard;
        for (uint64_t i = 0; i < length; i++) {
            rc = wl_columnar_lftj_work(record, 1); if (rc) goto discard;
            record->cache_name[i] = source->name[i];
            rc = wl_columnar_lftj_tail(record, 2, i + 1u); if (rc) goto discard;
        }
    }
    if (!fresh) {
        rc = wl_columnar_lftj_work(record, sizeof(record->cache_fresh));
        if (rc) goto discard;
        memset(&record->cache_fresh, 0, sizeof(record->cache_fresh));
        record->cache_fresh.ncols = source->ncols;
        record->cache_fresh.key_col = key_col;
        record->cache_fresh.ledger = &session->mem_ledger;
        rc = wl_columnar_lftj_tail(record, 3, 0); if (rc) goto discard;
        if (source->nrows) {
            uint64_t cells, bytes;
            if (!wl_columnar_memory_size_mul(source->nrows, source->ncols,
                &cells) || !wl_columnar_memory_size_mul(cells,
                sizeof(int64_t), &bytes) || bytes > SIZE_MAX){
                rc = EOVERFLOW; goto discard;
            }
            rc = wl_columnar_lftj_work(record, 1); if (rc) goto discard;
            record->cache_fresh.sorted =
                wl_columnar_arrangement_sorted_allocate((size_t)bytes);
            if (!record->cache_fresh.sorted) {
                rc = ENOMEM; goto discard;
            }
            wl_mem_ledger_alloc(record->cache_fresh.ledger,
                WL_MEM_SUBSYS_ARRANGEMENT, bytes);
            record->cache_fresh.ledger_bytes = bytes;
            record->cache_ledger_installed = true;
            rc = wl_columnar_lftj_tail(record, 3, 0); if (rc) goto discard;
            for (uint32_t row = 0; row < source->nrows; row++)
                for (uint32_t col = 0; col < source->ncols; col++) {
                    rc = wl_columnar_lftj_work(record, 1); if (rc) goto discard;
                    record->cache_fresh.sorted[(size_t)row * source->ncols +
                        col]
                        = col_rel_get(source, row, col);
                    rc = wl_columnar_lftj_tail(record, 3,
                            (uint64_t)row * source->ncols + col + 1u);
                    if (rc) goto discard;
                }
            rc = wl_columnar_relation_radix_rows_controlled(
                record->cache_fresh.sorted, source->nrows, source->ncols,
                key_col, WIRELOG_TYPE_INT64, record);
            if (rc) goto discard;
        }
        record->cache_fresh.nrows = source->nrows;
        record->cache_fresh.indexed_rows = source->nrows;
        record->cache_fresh.source_snapshot = snapshot;
    }
    if (session->sarr_active_pins == UINT32_MAX
        || (fresh && entry->sarr.pin_count == UINT32_MAX)) {
        rc = EOVERFLOW; goto discard;
    }
    /* Last admitted cutoff: installs cache and exact probe atomically with
     * respect to the serialized caller. No fallible tail follows this block. */
    rc = wl_columnar_lftj_work(record, 96); if (rc) goto discard;
    rc = wl_columnar_lftj_tail(record, 2, input + 1u); if (rc) goto discard;
    if (!fresh) {
        bool new_entry = record->cache_name != NULL;
        record->cache_old = new_entry ? NULL : entry->sarr.sorted;
        record->old_ledger_bytes = new_entry ? 0 : entry->sarr.ledger_bytes;
        if (new_entry) {
            memset(entry, 0, sizeof(*entry));
            entry->rel_name = record->cache_name; record->cache_name = NULL;
            entry->key_col = key_col;
        }
        entry->sarr = record->cache_fresh;
        record->cache_fresh.sorted = NULL;
        record->cache_fresh.ledger_bytes = 0;
        record->cache_ledger_installed = false;
        free(record->cache_old); record->cache_old = NULL;
        if (record->old_ledger_bytes)
            wl_mem_ledger_free(&session->mem_ledger,
                WL_MEM_SUBSYS_ARRANGEMENT, record->old_ledger_bytes);
        record->old_ledger_bytes = 0;
        if (new_entry) session->sarr_count++;
    }
    entry->sarr.pin_count++; session->sarr_active_pins++;
    probe->entry = entry; probe->arr = &entry->sarr; probe->session = session;
    probe->source = source; probe->source_snapshot = snapshot;
    probe->identity = (uintptr_t)probe; probe->active = true;
    return 0;
discard:
    wl_columnar_arrangement_sorted_discard_fresh(record);
    return rc;
}
