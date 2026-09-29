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
#define XXH_STATIC_LINKING_ONLY
#include <xxhash.h>

#ifdef WL_SESSION_TEST_HOOKS
static atomic_bool wl_dedup_fail_next_growth_alloc;
#ifdef _MSC_VER
#define WL_DEDUP_TEST_THREAD_LOCAL __declspec(thread)
#else
#define WL_DEDUP_TEST_THREAD_LOCAL _Thread_local
#endif
static WL_DEDUP_TEST_THREAD_LOCAL bool wl_dedup_fail_next_worker_growth_alloc;
#undef WL_DEDUP_TEST_THREAD_LOCAL

void
wl_columnar_eval_dedup_test_fail_next_growth_alloc(void)
{
    atomic_store_explicit(&wl_dedup_fail_next_growth_alloc, true,
        memory_order_release);
}

void
wl_columnar_eval_dedup_test_fail_next_worker_growth_alloc(void)
{
    wl_dedup_fail_next_worker_growth_alloc = true;
}

bool
wl_columnar_eval_dedup_test_worker_growth_alloc_pending(void)
{
    return wl_dedup_fail_next_worker_growth_alloc;
}
#endif

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
        || r->dedup_count > r->dedup_cap)
        return EINVAL;
    *bytes = (uint64_t)r->dedup_cap * sizeof(*r->dedup_slots);
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

/* Clone the current table without changing its load factor or probe layout.
* The image is private, but its slots must be admitted before allocation. */
int
wl_columnar_eval_dedup_set_clone_exact(col_rel_t *dst, const col_rel_t *src)
{
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_admission_status_t status;
    uint64_t bytes;
    int rc = wl_columnar_eval_dedup_set_bytes(src, &bytes);

    if (rc != 0 || !dst || dst->dedup_slots || dst->dedup_reserved_bytes)
        return rc != 0 ? rc : EINVAL;
    if (bytes == 0)
        return 0;
#if SIZE_MAX < UINT64_MAX
    if (bytes > SIZE_MAX)
        return EOVERFLOW;
#endif
    wl_columnar_memory_reservation_init(&pending);
    if (dst->memory_governor) {
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(dst->memory_governor),
            bytes, &pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return wl_dedup_admission_rc(dst, status);
    }
    uint64_t *slots = malloc((size_t)bytes);
    if (!slots) {
        if (pending.bytes && !wl_columnar_memory_rollback(&pending))
            abort();
        return ENOMEM;
    }
    memcpy(slots, src->dedup_slots, (size_t)bytes);
    if (pending.bytes && (!wl_columnar_memory_commit(&pending, dst)
        || !wl_columnar_memory_reservation_move(&dst->dedup_reservation,
        &pending)))
        abort();
    dst->dedup_slots = slots;
    dst->dedup_cap = src->dedup_cap;
    dst->dedup_count = src->dedup_count;
    dst->dedup_reserved_bytes = dst->memory_governor ? bytes : 0;
    return 0;
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
    uint64_t h;
    if (r->ncols <= 8) {
        int64_t buf[8];
        for (uint32_t c = 0; c < r->ncols; c++)
            buf[c] = r->columns[c][row];
        h = XXH3_64bits(buf, (size_t)r->ncols * sizeof(int64_t));
    } else {
        XXH3_state_t state;
        XXH3_INITSTATE(&state);
        (void)XXH3_64bits_reset(&state);
        for (uint32_t c = 0; c < r->ncols; c++) {
            int64_t value = r->columns[c][row];
            (void)XXH3_64bits_update(&state, &value, sizeof(value));
        }
        h = XXH3_64bits_digest(&state);
    }
    return h ? h : 1; /* avoid 0 sentinel */
}

/* Grow the hash set to double capacity. */
static int
wl_columnar_eval_dedup_set_grow(col_rel_t *r, uint64_t incoming_hash,
    bool *inserted)
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
    old_bytes = (uint64_t)old_cap * sizeof(uint64_t);
    new_bytes = (uint64_t)new_cap * sizeof(uint64_t);
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
#ifdef WL_SESSION_TEST_HOOKS
    bool tls_fail = wl_dedup_fail_next_worker_growth_alloc;
    wl_dedup_fail_next_worker_growth_alloc = false;
    bool global_fail = atomic_exchange_explicit(
        &wl_dedup_fail_next_growth_alloc, false, memory_order_acq_rel);
    bool fail_alloc = tls_fail || global_fail;
    new_slots = fail_alloc ? NULL
        : (uint64_t *)calloc(new_cap, sizeof(uint64_t));
#else
    new_slots = (uint64_t *)calloc(new_cap, sizeof(uint64_t));
#endif
    if (!new_slots) {
        if (growth && !wl_columnar_memory_rollback_growth(
                &r->dedup_reservation))
            abort();
        if (pending.bytes && !wl_columnar_memory_rollback(&pending))
            abort();
        return ENOMEM;
    }
    new_slots[incoming_hash & (new_cap - 1u)] = incoming_hash;
    uint32_t count_inc = 1;
    /* Rehash existing entries. */
    for (uint32_t i = 0; i < old_cap; i++) {
        uint64_t h = r->dedup_slots[i];
        if (h == 0)
            continue;
        if (h == incoming_hash) {
            count_inc = 0;
            continue;
        }
        uint32_t pos = (uint32_t)(h & (new_cap - 1u));
        while (new_slots[pos] != 0 && new_slots[pos] != h)
            pos = (pos + 1u) & (new_cap - 1u);
        if (new_slots[pos] == 0)
            new_slots[pos] = h;
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
    r->dedup_count += count_inc;
    *inserted = count_inc != 0;
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
    bool inserted = false;
    bool pending_before = r && r->memory_budget_denial_pending;
    if (wl_columnar_eval_dedup_set_insert_checked(r, h, &inserted) == 0)
        return inserted;
    if (r)
        r->memory_budget_denial_pending = pending_before;
    return true; /* Historical unique fallback on denial or malformed set. */
}

int
wl_columnar_eval_dedup_set_insert_checked(col_rel_t *r, uint64_t h,
    bool *inserted)
{
    uint32_t empty = UINT32_MAX;
    if (!r || !h || !inserted)
        return EINVAL;
    if (r->dedup_slots && !wl_dedup_token_valid(r))
        return EBUSY;
    /* A duplicate never needs growth, even at the load threshold. */
    if (r->dedup_slots && r->dedup_cap
        && !(r->dedup_cap & (r->dedup_cap - 1u))
        && wl_dedup_probe(r->dedup_slots, r->dedup_cap, h, &empty)) {
        *inserted = false;
        return 0;
    }
    if (!r->dedup_slots || !r->dedup_cap
        || (r->dedup_cap & (r->dedup_cap - 1u))
        || r->dedup_count >= r->dedup_cap
        || (uint64_t)r->dedup_count * 10u
        >= (uint64_t)r->dedup_cap * 7u) {
        int rc = wl_columnar_eval_dedup_set_grow(r, h, inserted);
        if (rc != 0)
            return rc;
        return 0;
    }
    if (empty == UINT32_MAX)
        return EBUSY;
    r->dedup_slots[empty] = h;
    r->dedup_count++;
    *inserted = true;
    return 0;
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
