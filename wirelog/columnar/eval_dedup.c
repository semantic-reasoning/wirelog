/*
 * columnar/eval_dedup.c - TDD worker relation deduplication
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#include "columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <xxhash.h>

/* Hash all columns of a single row using XXH3. */
uint64_t
wl_columnar_eval_dedup_row_hash(const col_rel_t *r, uint32_t row)
{
    int64_t buf[8];
    int64_t *p = r->ncols <= 8 ? buf
        : (int64_t *)malloc((size_t)r->ncols * sizeof(int64_t));
    if (!p)
        return 1; /* fallback: treat as unique */
    for (uint32_t c = 0; c < r->ncols; c++)
        p[c] = r->columns[c][row];
    uint64_t h = XXH3_64bits(p, (size_t)r->ncols * sizeof(int64_t));
    if (p != buf)
        free(p);
    return h ? h : 1; /* avoid 0 sentinel */
}

/* Grow the hash set to double capacity. */
static int
wl_columnar_eval_dedup_set_grow(col_rel_t *r)
{
    uint32_t old_cap = r->dedup_cap;
    uint32_t new_cap = old_cap ? old_cap * 2 : 1024;
    uint64_t *new_slots = (uint64_t *)calloc(new_cap, sizeof(uint64_t));
    if (!new_slots)
        return ENOMEM;
    uint32_t mask = new_cap - 1;
    /* Rehash existing entries. */
    for (uint32_t i = 0; i < old_cap; i++) {
        uint64_t h = r->dedup_slots[i];
        if (h == 0)
            continue;
        uint32_t pos = (uint32_t)(h & mask);
        while (new_slots[pos] != 0)
            pos = (pos + 1) & mask;
        new_slots[pos] = h;
    }
    free(r->dedup_slots);
    r->dedup_slots = new_slots;
    r->dedup_cap = new_cap;
    return 0;
}

/* Probabilistic dedup using 64-bit XXH3 hashes only (no full row comparison).
 * Birthday-bound collision probability: ~N^2 / 2^65.
 * For N=1M rows: P < 10^-7 (negligible).
 * Exact dedup is performed by tdd_dedup_rel after merge. */
bool
wl_columnar_eval_dedup_set_insert(col_rel_t *r, uint64_t h)
{
    if (r->dedup_count * 10 >= r->dedup_cap * 7) { /* >70% load */
        if (wl_columnar_eval_dedup_set_grow(r) != 0)
            return true; /* OOM: treat as unique (safe fallback) */
    }
    uint32_t mask = r->dedup_cap - 1;
    uint32_t pos = (uint32_t)(h & mask);
    while (r->dedup_slots[pos] != 0) {
        if (r->dedup_slots[pos] == h)
            return false; /* duplicate */
        pos = (pos + 1) & mask;
    }
    r->dedup_slots[pos] = h;
    r->dedup_count++;
    return true;
}

bool
wl_columnar_eval_dedup_set_contains(const col_rel_t *r, uint64_t h)
{
    if (!r || !r->dedup_slots || r->dedup_cap == 0)
        return false;
    uint32_t mask = r->dedup_cap - 1;
    uint32_t pos = (uint32_t)(h & mask);
    while (r->dedup_slots[pos] != 0) {
        if (r->dedup_slots[pos] == h)
            return true;
        pos = (pos + 1) & mask;
    }
    return false;
}

int
wl_columnar_eval_dedup_set_init_from_rel(col_rel_t *r)
{
    /* Size: next power-of-2 >= 2 * nrows, min 1024. */
    uint32_t target = r->nrows > 512 ? r->nrows * 2 : 1024;
    uint32_t cap = 1024;
    while (cap < target)
        cap *= 2;
    r->dedup_slots = (uint64_t *)calloc(cap, sizeof(uint64_t));
    if (!r->dedup_slots)
        return ENOMEM;
    r->dedup_cap = cap;
    r->dedup_count = 0;
    for (uint32_t i = 0; i < r->nrows; i++) {
        uint64_t h = wl_columnar_eval_dedup_row_hash(r, i);
        wl_columnar_eval_dedup_set_insert(r, h);
    }
    return 0;
}
