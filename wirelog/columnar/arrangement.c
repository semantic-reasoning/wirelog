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

/* Incremental update: index only rows [old_nrows..rel->nrows). */
static int
arr_update_incremental_impl(col_arrangement_t *arr, const col_rel_t *rel,
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
        size_t next_bytes = (size_t)new_cap * sizeof(uint32_t);
        size_t head_bytes = (size_t)arr->nbuckets * sizeof(uint64_t);
        uint64_t target_bytes;
        wl_columnar_memory_reservation_t pending;
        bool pending_valid;
        if (new_cap != 0 && next_bytes / sizeof(uint32_t) != new_cap)
            return ENOMEM;
        if (head_bytes > SIZE_MAX - next_bytes)
            return ENOMEM;
        target_bytes = (uint64_t)(head_bytes + next_bytes);
        if (!arr_reserve_bytes(arr, target_bytes, &pending))
            return ENOMEM;
        pending_valid = arr->memory_governor != NULL;
        uint32_t *nxt = (uint32_t *)malloc(next_bytes);
        if (!nxt){
            if (pending_valid)
                (void)wl_columnar_memory_release(&pending);
            return ENOMEM;
        }
        if (arr->ht_next && arr->ht_cap > 0)
            memcpy(nxt, arr->ht_next,
                (size_t)arr->ht_cap * sizeof(uint32_t));
        free(arr->ht_next);
        arr->ht_next = nxt;
        arr->ht_cap = new_cap;
        if (pending_valid
            && !arr_publish_reservation(arr, &pending, target_bytes)) {
            arr_free_contents(arr);
            return ENOMEM;
        }
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

static int
arr_update_incremental(col_arrangement_t *arr, const col_rel_t *rel,
    uint32_t old_nrows)
{
    uint64_t before = arr->ledger ? arr_ledger_bytes(arr) : 0;
    int rc = arr_update_incremental_impl(arr, rel, old_nrows);
    arr_ledger_sync(arr, before);
    return rc;
}

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
            || e->arr.indexed_rows < rel->nrows || e->rebuild_deferred)) {
            e->rebuild_deferred = true;
            return EBUSY;
        }
        if (!snapshot_match || arr_entry_unbuilt(e)) {
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
        } else if (e->arr.indexed_rows < rel->nrows) {
            uint32_t old = e->arr.indexed_rows;
            cs->arr_total_bytes -= e->mem_bytes;
            if (arr_update_incremental(&e->arr, rel, old) != 0) {
                cs->arr_total_bytes += e->mem_bytes; /* restore on failure */
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
    if (sarr->ledger && sarr->ledger_bytes > 0)
        wl_mem_ledger_free(sarr->ledger, WL_MEM_SUBSYS_ARRANGEMENT,
            sarr->ledger_bytes);
    sarr->ledger_bytes = 0;
    free(sarr->sorted);
    sarr->sorted = NULL;
}

static int
sarr_build(col_sorted_arr_t *sarr, const col_rel_t *rel, uint32_t key_col)
{
    sarr_release(sarr);
    sarr->nrows = 0;
    sarr->ncols = rel->ncols;
    sarr->key_col = key_col;
    sarr->indexed_rows = 0;

    if (rel->nrows == 0) {
        sarr->source_snapshot = wl_columnar_relation_snapshot(rel);
        return 0;
    }

    size_t bytes = (size_t)rel->nrows * rel->ncols * sizeof(int64_t);
    sarr->sorted = (int64_t *)malloc(bytes);
    if (!sarr->sorted)
        return ENOMEM;
    if (sarr->ledger) {
        wl_mem_ledger_alloc(sarr->ledger, WL_MEM_SUBSYS_ARRANGEMENT, bytes);
        sarr->ledger_bytes = bytes;
    }

    for (uint32_t r = 0; r < rel->nrows; r++)
        col_rel_row_copy_out(rel, r, sarr->sorted + (size_t)r * rel->ncols);
    /* #465: stable LSD radix sort by the single key column replaces the
     * platform-specific qsort_r path that did not exist on Android NDK
     * bionic libc.  Failure here is malloc-fail in the radix scratch
     * buffer; on that path sarr->sorted is unmodified and we propagate
     * ENOMEM so the caller can react. */
    if (col_radix_sort_rows_by_key(sarr->sorted, rel->nrows, rel->ncols,
        key_col) != 0) {
        sarr_release(sarr);
        return ENOMEM;
    }
    sarr->nrows = rel->nrows;
    sarr->indexed_rows = rel->nrows;
    sarr->source_snapshot = wl_columnar_relation_snapshot(rel);
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
        /* indexed_rows is only a progress marker; freshness is the source
         * snapshot. */
        if (!wl_columnar_relation_snapshot_equal(e->sarr.source_snapshot,
            wl_columnar_relation_snapshot(rel))
            || e->sarr.indexed_rows != rel->nrows) {
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
    e->sarr.ledger = &cs->mem_ledger; /* Issue #1380 */
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
            col_diff_arrangement_t *arr = e->diff_arr;
            if (!wl_columnar_relation_snapshot_equal(
                    arr->source_snapshot,
                    wl_columnar_relation_snapshot(source_rel))) {
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
            return e->diff_arr;
        }
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
    e->diff_arr->source_snapshot = (col_relation_snapshot_t){ 0, 0, 0 };
    col_diff_arrangement_attach_ledger(e->diff_arr, &cs->mem_ledger);
    cs->diff_arr_count++;
    return e->diff_arr;
}

int
wl_columnar_arrangement_diff_txn_begin(wl_col_session_t *cs,
    const char *rel_name, const col_rel_t *source_rel,
    const uint32_t *key_cols, uint32_t key_count,
    wl_columnar_arrangement_diff_txn_t *txn)
{
    col_diff_arrangement_t **slot = NULL;

    if (!cs || !rel_name || !source_rel || !key_cols || key_count == 0
        || !txn)
        return EINVAL;
    memset(txn, 0, sizeof(*txn));

    for (uint32_t i = 0; i < cs->diff_arr_count; i++) {
        col_diff_arr_entry_t *entry = &cs->diff_arr_entries[i];
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
            slot = &entry->diff_arr;
            break;
        }
    }

    if (!slot) {
        if (cs->diff_arr_count >= cs->diff_arr_cap) {
            uint32_t new_cap = cs->diff_arr_cap ? cs->diff_arr_cap * 2u : 4u;
            col_diff_arr_entry_t *entries = (col_diff_arr_entry_t *)realloc(
                cs->diff_arr_entries, new_cap * sizeof(*entries));
            if (!entries)
                return ENOMEM;
            cs->diff_arr_entries = entries;
            cs->diff_arr_cap = new_cap;
        }
        col_diff_arr_entry_t *entry
            = &cs->diff_arr_entries[cs->diff_arr_count];
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
        entry->diff_arr = col_diff_arrangement_create(key_cols, key_count, 0);
        if (!entry->diff_arr) {
            free(entry->rel_name);
            free(entry->key_cols);
            memset(entry, 0, sizeof(*entry));
            return ENOMEM;
        }
        txn->entry = entry;
        txn->pending_entry = true;
        slot = &entry->diff_arr;
    }

    txn->session = cs;
    txn->persistent = *slot;
    txn->working = col_diff_arrangement_deep_copy(txn->persistent);
    if (!txn->working) {
        if (txn->pending_entry) {
            free(txn->entry->rel_name);
            free(txn->entry->key_cols);
            col_diff_arrangement_destroy(txn->persistent);
            memset(txn->entry, 0, sizeof(*txn->entry));
        }
        memset(txn, 0, sizeof(*txn));
        return ENOMEM;
    }
    col_diff_arrangement_attach_ledger(txn->working,
        txn->pending_entry ? &cs->mem_ledger : txn->persistent->ledger);
    txn->slot = slot;

    if (!wl_columnar_relation_snapshot_equal(txn->working->source_snapshot,
        wl_columnar_relation_snapshot(source_rel))) {
        if (txn->working->ht_head)
            memset(txn->working->ht_head, 0,
                (size_t)txn->working->nbuckets *
                sizeof(*txn->working->ht_head));
        if (txn->working->ht_next)
            memset(txn->working->ht_next, 0,
                (size_t)txn->working->ht_cap * sizeof(*txn->working->ht_next));
        txn->working->base_nrows = 0;
        txn->working->current_nrows = 0;
        txn->working->indexed_rows = 0;
        txn->working->source_snapshot = (col_relation_snapshot_t){ 0, 0, 0 };
    }
    return 0;
}

void
wl_columnar_arrangement_diff_txn_commit(
    wl_columnar_arrangement_diff_txn_t *txn)
{
    if (!txn || !txn->slot || !txn->persistent || !txn->working)
        return;
    *txn->slot = txn->working;
    col_diff_arrangement_destroy(txn->persistent);
    if (txn->pending_entry)
        txn->session->diff_arr_count++;
    memset(txn, 0, sizeof(*txn));
}

void
wl_columnar_arrangement_diff_txn_abort(
    wl_columnar_arrangement_diff_txn_t *txn)
{
    if (!txn)
        return;
    col_diff_arrangement_destroy(txn->working);
    if (txn->pending_entry && txn->entry) {
        free(txn->entry->rel_name);
        free(txn->entry->key_cols);
        col_diff_arrangement_destroy(txn->persistent);
        memset(txn->entry, 0, sizeof(*txn->entry));
    }
    memset(txn, 0, sizeof(*txn));
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
