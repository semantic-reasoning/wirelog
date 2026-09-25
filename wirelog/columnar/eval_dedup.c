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

static bool
wl_dedup_token_valid(const col_rel_t *r)
{
    uint64_t bytes = (uint64_t)r->dedup_cap * sizeof(uint64_t);
    return (!r->memory_governor && r->dedup_reserved_bytes == 0)
           || (r->memory_governor && r->dedup_reserved_bytes == bytes
           && r->dedup_reservation.identity == &r->dedup_reservation
           && r->dedup_reservation.bytes == bytes
           && r->dedup_reservation.governor
           == wl_columnar_memory_governor_ref_get(r->memory_governor)
           && atomic_load_explicit(&r->dedup_reservation.state,
           memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
           && atomic_load_explicit(&r->dedup_reservation.owner_bits,
           memory_order_acquire) == (uintptr_t)r);
}

int
wl_columnar_eval_dedup_set_bytes(const col_rel_t *r, uint64_t *bytes)
{
    uint32_t occupied = 0;
    if (!r || !bytes)
        return EINVAL;
    if (!r->dedup_cap) {
        *bytes = 0;
        uint64_t state = atomic_load_explicit(&r->dedup_reservation.state,
                memory_order_acquire);
        return !r->dedup_slots && !r->dedup_count
               && !r->dedup_reserved_bytes
               && (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
               || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
            ? 0 : EINVAL;
    }
    if (!r->dedup_slots || (r->dedup_cap & (r->dedup_cap - 1u))
        || r->dedup_count > r->dedup_cap
        || !wl_columnar_memory_size_mul(r->dedup_cap,
        sizeof(*r->dedup_slots), bytes))
        return EINVAL;
    for (uint32_t i = 0; i < r->dedup_cap; i++)
        occupied += r->dedup_slots[i] != 0;
    if (occupied != r->dedup_count)
        return EINVAL;
    if (!r->memory_governor)
        return !r->dedup_reserved_bytes ? 0 : EINVAL;
    if (!wl_dedup_token_valid(r))
        return EBUSY;
    return 0;
}

void
wl_columnar_eval_dedup_set_clear(col_rel_t *r)
{
    if (!r)
        return;
    free(r->dedup_slots);
    r->dedup_slots = NULL;
    r->dedup_cap = 0;
    r->dedup_count = 0;
    if (r->dedup_reserved_bytes
        && !wl_columnar_memory_release(&r->dedup_reservation))
        abort();
    r->dedup_reserved_bytes = 0;
}

static int
wl_dedup_admission_rc(col_rel_t *r,
    wl_columnar_memory_admission_status_t status)
{
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
        r->memory_budget_denial_pending = true;
        return ENOSPC;
    }
    return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
        : EBUSY;
}

/* A full table is malformed but must never produce an unbounded probe. */
static bool
wl_dedup_probe(const uint64_t *slots, uint32_t cap, uint64_t h,
    uint32_t *empty)
{
    uint32_t pos = (uint32_t)(h & (cap - 1u));
    for (uint32_t i = 0; i < cap; i++, pos = (pos + 1u) & (cap - 1u)) {
        if (slots[pos] == h)
            return true;
        if (slots[pos] == 0) {
            if (empty)
                *empty = pos;
            return false;
        }
    }
    if (empty)
        *empty = UINT32_MAX;
    return false;
}

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
    uint32_t new_cap;
    uint64_t old_bytes, new_bytes;
    uint64_t *new_slots;
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_admission_status_t status;
    bool growth = old_cap != 0 && r->memory_governor;

    if (old_cap > UINT32_MAX / 2u)
        return EOVERFLOW;
    if (old_cap) {
        uint64_t actual;
        if (wl_columnar_eval_dedup_set_bytes(r, &actual) != 0)
            return EBUSY;
    }
    new_cap = old_cap ? old_cap * 2u : 1024u;
    if (!wl_columnar_memory_size_mul(old_cap, sizeof(uint64_t),
        &old_bytes)
        || !wl_columnar_memory_size_mul(new_cap, sizeof(uint64_t),
        &new_bytes))
        return EOVERFLOW;
#if SIZE_MAX < UINT64_MAX
    if (new_bytes > SIZE_MAX)
        return EOVERFLOW;
#endif
    wl_columnar_memory_reservation_init(&pending);
    if (r->memory_governor) {
        if (growth) {
            status = wl_columnar_memory_begin_growth(
                &r->dedup_reservation, new_bytes);
        } else {
            status = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(r->memory_governor),
                new_bytes, &pending);
        }
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return wl_dedup_admission_rc(r, status);
    }
    new_slots = (uint64_t *)calloc(new_cap, sizeof(uint64_t));
    if (!new_slots) {
        if (growth && !wl_columnar_memory_rollback_growth(
                &r->dedup_reservation))
            abort();
        if (pending.bytes && !wl_columnar_memory_rollback(&pending))
            abort();
        return ENOMEM;
    }
    /* Rehash existing entries. */
    for (uint32_t i = 0; i < old_cap; i++) {
        uint64_t h = r->dedup_slots[i];
        if (h == 0)
            continue;
        uint32_t pos = UINT32_MAX;
        if (!wl_dedup_probe(new_slots, new_cap, h, &pos)) {
            if (pos == UINT32_MAX)
                abort();
            new_slots[pos] = h;
        }
    }
    if (r->memory_governor) {
        if (growth) {
            if (!wl_columnar_memory_commit_growth(
                    &r->dedup_reservation))
                abort();
            r->dedup_reserved_bytes = old_bytes + new_bytes;
        } else {
            if (!wl_columnar_memory_commit(&pending, r)
                || !wl_columnar_memory_reservation_move(
                    &r->dedup_reservation, &pending))
                abort();
            r->dedup_reserved_bytes = new_bytes;
        }
    }
    free(r->dedup_slots);
    r->dedup_slots = new_slots;
    r->dedup_cap = new_cap;
    if (growth) {
        if (!wl_columnar_memory_reservation_downsize(
                &r->dedup_reservation, new_bytes))
            abort();
        r->dedup_reserved_bytes = new_bytes;
    }
    return 0;
}

/* Probabilistic dedup using 64-bit XXH3 hashes only (no full row comparison).
 * Birthday-bound collision probability: ~N^2 / 2^65.
 * For N=1M rows: P < 10^-7 (negligible).
 * Later merge dedup is an additional pass; a hash collision here can still
 * suppress a distinct row. */
bool
wl_columnar_eval_dedup_set_insert(col_rel_t *r, uint64_t h)
{
    uint32_t empty = UINT32_MAX;
    if (!r || !h)
        return true;
    if (r->dedup_slots && !wl_dedup_token_valid(r))
        return true;
    /* A duplicate never needs growth, even at the load threshold. */
    if (r->dedup_slots && r->dedup_cap
        && !(r->dedup_cap & (r->dedup_cap - 1u))
        && wl_dedup_probe(r->dedup_slots, r->dedup_cap, h, &empty))
        return false;
    if (!r->dedup_slots || !r->dedup_cap
        || (r->dedup_cap & (r->dedup_cap - 1u))
        || r->dedup_count >= r->dedup_cap
        || (uint64_t)r->dedup_count * 10u
        >= (uint64_t)r->dedup_cap * 7u) {
        bool pending_before = r->memory_budget_denial_pending;
        if (wl_columnar_eval_dedup_set_grow(r) != 0) {
            r->memory_budget_denial_pending = pending_before;
            return true; /* Preserve the unique fallback on denial/OOM. */
        }
        empty = UINT32_MAX;
        if (wl_dedup_probe(r->dedup_slots, r->dedup_cap, h, &empty))
            return false;
    }
    if (empty == UINT32_MAX)
        return true;
    r->dedup_slots[empty] = h;
    r->dedup_count++;
    return true;
}

bool
wl_columnar_eval_dedup_set_contains(const col_rel_t *r, uint64_t h)
{
    if (!r || !r->dedup_slots || !r->dedup_cap
        || (r->dedup_cap & (r->dedup_cap - 1u)) || !h)
        return false;
    if (!wl_dedup_token_valid(r))
        return false;
    return wl_dedup_probe(r->dedup_slots, r->dedup_cap, h, NULL);
}

int
wl_columnar_eval_dedup_set_init_from_rel(col_rel_t *r)
{
    uint64_t bytes;
    uint64_t *slots;
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_admission_status_t status;
    /* Size: next power-of-2 >= 2 * nrows, min 1024. */
    if (!r || r->dedup_slots || r->dedup_cap || r->dedup_count
        || r->dedup_reserved_bytes)
        return EBUSY;
    if (r->nrows > UINT32_MAX / 2u)
        return EOVERFLOW;
    if (r->nrows > r->capacity || (r->nrows && !r->columns))
        return EINVAL;
    if (r->nrows)
        for (uint32_t c = 0; c < r->ncols; c++)
            if (!r->columns[c])
                return EINVAL;
    uint32_t target = r->nrows > 512 ? r->nrows * 2u : 1024u;
    uint32_t cap = 1024;
    while (cap < target) {
        if (cap > UINT32_MAX / 2u)
            return EOVERFLOW;
        cap *= 2u;
    }
    if (!wl_columnar_memory_size_mul(cap, sizeof(uint64_t), &bytes))
        return EOVERFLOW;
#if SIZE_MAX < UINT64_MAX
    if (bytes > SIZE_MAX)
        return EOVERFLOW;
#endif
    wl_columnar_memory_reservation_init(&pending);
    if (r->memory_governor) {
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(r->memory_governor),
            bytes, &pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return wl_dedup_admission_rc(r, status);
    }
    slots = (uint64_t *)calloc(cap, sizeof(uint64_t));
    if (!slots) {
        if (pending.bytes && !wl_columnar_memory_rollback(&pending))
            abort();
        return ENOMEM;
    }
    uint32_t count = 0;
    for (uint32_t i = 0; i < r->nrows; i++) {
        uint64_t h = wl_columnar_eval_dedup_row_hash(r, i);
        uint32_t pos = UINT32_MAX;
        if (!wl_dedup_probe(slots, cap, h, &pos)) {
            if (pos == UINT32_MAX)
                abort();
            slots[pos] = h;
            count++;
        }
    }
    if (r->memory_governor) {
        if (!wl_columnar_memory_commit(&pending, r)
            || !wl_columnar_memory_reservation_move(
                &r->dedup_reservation, &pending))
            abort();
        r->dedup_reserved_bytes = bytes;
    }
    r->dedup_slots = slots;
    r->dedup_cap = cap;
    r->dedup_count = count;
    return 0;
}
