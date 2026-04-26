/*
 * columnar/handle_remap.c - Compound-handle Remap Table (Issues #557 / #586 /
 *                          #589 — implementation of the public surface frozen
 *                          in handle_remap.h)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Open-addressing hash table with linear probing and SplitMix64 hashing,
 * struct-of-arrays layout, no per-entry deletion.  See
 * `handle_remap_design.md` for the rationale; this file is the
 * mechanical realisation of that design.
 */

#include "handle_remap.h"

#include <stdlib.h>
#include <string.h>

/* Smallest viable initial capacity (power of two).  Below this the
 * rounding/mod arithmetic costs dominate the table; above this is fine.
 * Matches handle_remap_design.md §2.7. */
#define WL_HANDLE_REMAP_MIN_CAP 16u

struct wl_handle_remap {
    int64_t *keys;       /* capacity entries; 0 = empty                  */
    int64_t *values;     /* capacity entries; valid iff keys[i] != 0     */
    size_t capacity;     /* power of two; index mask = capacity - 1      */
    size_t count;        /* live entries (excluding empties)             */
    size_t rehash_at;    /* count threshold that triggers grow           */
};

/* SplitMix64 finalizer (handle_remap_design.md §3.3).  Branchless,
 * well-mixed, identical to xxhash's internal finalizer and Java's
 * SplittableRandom.  Used unconditionally for all keys. */
static inline uint64_t
wl_handle_remap_hash_(int64_t key)
{
    uint64_t x = (uint64_t)key;
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebull;
    x ^= x >> 31;
    return x;
}

/* Round @x up to the next power of two.  @x == 0 yields the minimum
* capacity (caller decides what that is).  Returns 0 on overflow. */
static size_t
next_pow2_(size_t x)
{
    if (x == 0)
        return 0;
    /* If already a power of two, return as-is. */
    if ((x & (x - 1)) == 0)
        return x;
    size_t p = 1u;
    while (p && p < x)
        p <<= 1;
    return p; /* zero on overflow */
}

/* Compute the rehash threshold (75% of capacity) — same formula the
 * design doc fixes at §2.4 and §4.1. */
static inline size_t
rehash_threshold_(size_t capacity)
{
    /* capacity * 3 / 4 -- exact for power-of-two capacity, no FP. */
    return capacity - (capacity >> 2);
}

int
wl_handle_remap_create(size_t capacity, wl_handle_remap_t **out)
{
    if (!out)
        return WL_ERROR_INVALID_ARGS;
    *out = NULL;

    /* Pre-addition overflow gate (#589 meta-review): the load-factor
     * headroom calc below adds capacity/3 + 1 to capacity; reject any
     * input that would wrap size_t before the post-rounded check at
     * the next gate.  capacity > SIZE_MAX/2 is a comfortable bound
     * (the actual wrap is at ~3*SIZE_MAX/4) and keeps the constructor
     * memory-safe regardless of caller discipline. */
    if (capacity > SIZE_MAX / 2u)
        return WL_ERROR_OOM;

    /* Apply the load-factor headroom up front so the caller passes the
     * raw expected entry count (per design §4.1 / §2.8). */
    size_t target = capacity == 0
        ? WL_HANDLE_REMAP_MIN_CAP
        : capacity + (capacity / 3u) + 1u; /* ceil(capacity / 0.75) */
    if (target < WL_HANDLE_REMAP_MIN_CAP)
        target = WL_HANDLE_REMAP_MIN_CAP;

    size_t cap = next_pow2_(target);
    if (cap == 0)
        return WL_ERROR_OOM; /* size_t overflow on ceil */

    /* Belt-and-suspenders 32-bit overflow check (design §2.9): the SOA
     * layout costs `capacity * sizeof(int64_t) * 2` bytes total. */
    if (cap > SIZE_MAX / (sizeof(int64_t) * 2u))
        return WL_ERROR_OOM;

    wl_handle_remap_t *m
        = (wl_handle_remap_t *)calloc(1, sizeof(*m));
    if (!m)
        return WL_ERROR_OOM;
    m->keys = (int64_t *)calloc(cap, sizeof(int64_t));
    m->values = (int64_t *)calloc(cap, sizeof(int64_t));
    if (!m->keys || !m->values) {
        free(m->keys);
        free(m->values);
        free(m);
        return WL_ERROR_OOM;
    }
    m->capacity = cap;
    m->count = 0;
    m->rehash_at = rehash_threshold_(cap);
    *out = m;
    return 0;
}

void
wl_handle_remap_free(wl_handle_remap_t *remap)
{
    if (!remap)
        return;
    free(remap->keys);
    free(remap->values);
    free(remap);
}

/* Probe-then-insert.  Returns 0 on success, WL_ERROR_OOM never (the
 * caller is responsible for ensuring there is at least one empty slot
 * — see grow logic in wl_handle_remap_insert).  Always finds either
 * the matching key (overwrite) or an empty slot (insert) because the
 * caller maintains count < capacity.
 *
 * Defense-in-depth (#589 meta-review): the linear-probe loop is
 * unconditional `for(;;)`, so a bad invariant turns a future-bug into
 * an infinite loop.  Trip-wire `if` converts that to a debuggable
 * abort instead of a hang. */
static void
remap_put_unchecked_(wl_handle_remap_t *m,
    int64_t key, int64_t value)
{
    if (m->count >= m->capacity)
        abort(); /* invariant break: caller failed to grow */
    size_t mask = m->capacity - 1u;
    size_t i = (size_t)wl_handle_remap_hash_(key) & mask;
    for (;;) {
        int64_t k = m->keys[i];
        if (k == 0) {
            /* Empty slot: insert. */
            m->keys[i] = key;
            m->values[i] = value;
            m->count++;
            return;
        }
        if (k == key) {
            /* Existing key: overwrite (per design §4.2 contract). */
            m->values[i] = value;
            return;
        }
        i = (i + 1u) & mask;
    }
}

static int
remap_grow_(wl_handle_remap_t *m)
{
    size_t new_cap = m->capacity * 2u;
    if (new_cap < m->capacity) /* overflow */
        return WL_ERROR_OOM;
    if (new_cap > SIZE_MAX / (sizeof(int64_t) * 2u))
        return WL_ERROR_OOM;

    int64_t *nk = (int64_t *)calloc(new_cap, sizeof(int64_t));
    int64_t *nv = (int64_t *)calloc(new_cap, sizeof(int64_t));
    if (!nk || !nv) {
        free(nk);
        free(nv);
        return WL_ERROR_OOM;
    }

    /* Swap in the new arrays before reinserting so the unchecked-put
     * helper sees the new capacity/mask. */
    int64_t *old_k = m->keys;
    int64_t *old_v = m->values;
    size_t old_cap = m->capacity;
    m->keys = nk;
    m->values = nv;
    m->capacity = new_cap;
    m->count = 0;
    m->rehash_at = rehash_threshold_(new_cap);

    for (size_t i = 0; i < old_cap; i++) {
        if (old_k[i] != 0)
            remap_put_unchecked_(m, old_k[i], old_v[i]);
    }
    free(old_k);
    free(old_v);
    return 0;
}

int
wl_handle_remap_insert(wl_handle_remap_t *m,
    int64_t old_handle, int64_t new_handle)
{
    if (!m || old_handle == 0)
        return WL_ERROR_INVALID_ARGS;

    /* Grow before insertion if the resulting count would cross the
     * rehash threshold.  An overwrite of an existing key does not
     * change count, but we cannot tell that without first probing —
     * so check the worst case here.  This matches the design's §4.2
     * "rehash to double capacity" rule and keeps probe length under
     * the 0.75-load-factor bound. */
    if (m->count + 1u > m->rehash_at) {
        int rc = remap_grow_(m);
        if (rc != 0)
            return rc;
    }
    remap_put_unchecked_(m, old_handle, new_handle);
    return 0;
}

int64_t
wl_handle_remap_lookup(const wl_handle_remap_t *m, int64_t old_handle)
{
    if (!m || old_handle == 0)
        return 0;

    size_t mask = m->capacity - 1u;
    size_t i = (size_t)wl_handle_remap_hash_(old_handle) & mask;
    for (;;) {
        int64_t k = m->keys[i];
        if (k == 0)
            return 0; /* empty -> miss */
        if (k == old_handle)
            return m->values[i];
        i = (i + 1u) & mask;
    }
}
