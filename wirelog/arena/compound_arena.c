/*
 * arena/compound_arena.c - Compound-term Arena & Handle Allocator (Issue #533)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Skeleton implementation of the side-relation compound arena.  The
 * interface, handle format, and freeze/GC boundaries are designed to be
 * stable; the in-epoch reclamation strategy is intentionally minimal
 * (reset-on-boundary) so later refinements can plug in without changing
 * callers.  See compound_arena.h for the contract.
 */

#include "compound_arena.h"

#include "wirelog/util/log.h"

#include <inttypes.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

/* 8-byte alignment mirrors the main wl_arena_t contract; keeps payloads
 * suitable for int64_t element access. */
#define WL_COMPOUND_ALIGN 8u
#define WL_COMPOUND_ALIGN_UP(n) \
        (((n) + (WL_COMPOUND_ALIGN - 1)) & ~(uint32_t)(WL_COMPOUND_ALIGN - 1))

/* Initial entry-index capacity per generation.  Small (16) because we
 * lazy-grow; most epochs with a handful of compounds pay only this much. */
#define WL_COMPOUND_DEFAULT_ENTRY_CAP 16u
#define WL_COMPOUND_GATE_WRITER UINT64_MAX
#define WL_COMPOUND_GATE_READERS_MAX (WL_COMPOUND_GATE_WRITER - 1u)

#ifdef _MSC_VER
static uint64_t
compound_gate_load(const wl_atomic_u64 *gate)
{
    return (uint64_t)_InterlockedCompareExchange64(
        (volatile __int64 *)gate, 0, 0);
}

static void
compound_gate_store(wl_atomic_u64 *gate, uint64_t value)
{
    (void)_InterlockedExchange64((volatile __int64 *)gate, (__int64)value);
}
#else
static uint64_t
compound_gate_load(const wl_atomic_u64 *gate)
{
    return atomic_load_explicit(gate, memory_order_acquire);
}

static void
compound_gate_store(wl_atomic_u64 *gate, uint64_t value)
{
    atomic_store_explicit(gate, value, memory_order_release);
}
#endif

/* ======================================================================== */
/* Internal helpers                                                         */
/* ======================================================================== */

static int
compound_admission_prepare(wl_compound_arena_t *arena, uint64_t old_bytes,
    uint64_t new_bytes, wl_columnar_memory_reservation_t *reservation)
{
    if (!arena->admission_prepare)
        return 0;
    return arena->admission_prepare(arena->admission_context, old_bytes,
               new_bytes, reservation);
}

static bool
compound_failpoint(wl_compound_arena_t *arena,
    wl_compound_alloc_failpoint_t failpoint)
{
    if (!arena || arena->test_failpoint != (uint32_t)failpoint)
        return false;
    arena->test_failpoint = WL_COMPOUND_FAIL_NONE;
    return true;
}

static void
compound_admission_abort(wl_compound_arena_t *arena,
    wl_columnar_memory_reservation_t *reservation)
{
    if (arena->admission_abort)
        arena->admission_abort(arena->admission_context, reservation);
}

static int
compound_admission_publish(wl_compound_arena_t *arena,
    wl_columnar_memory_reservation_t *old_reservation,
    wl_columnar_memory_reservation_t *new_reservation, const void *owner)
{
    if (!arena->admission_publish)
        return 0;
    return arena->admission_publish(arena->admission_context, old_reservation,
               new_reservation, owner);
}

static int
compound_admission_release(wl_compound_arena_t *arena,
    wl_columnar_memory_reservation_t *reservation)
{
    if (arena->admission_release_reservation)
        return arena->admission_release_reservation(arena->admission_context,
                   reservation);
    return 0;
}

static int
compound_grow_capacity(uint32_t current, uint32_t used, uint32_t need,
    uint32_t default_cap, uint32_t *out)
{
    uint32_t capacity = current > 0 ? current : default_cap;

    if (capacity < used)
        return -1;
    while ((uint64_t)capacity - used < need) {
        if (capacity > UINT32_MAX / 2u)
            return -1;
        capacity *= 2u;
    }
    *out = capacity;
    return 0;
}

/* Stage payload and both entry arrays together.  The old buffers and
 * reservations remain live until every new allocation is ready, so a denial
 * or allocator failure leaves the generation unchanged. */
static int
gen_reserve(wl_compound_arena_t *arena, wl_compound_gen_t *gen,
    uint32_t need, uint32_t default_cap)
{
    uint32_t new_payload_cap = gen->capacity;
    uint32_t new_entry_cap = gen->entry_cap;
    bool grow_payload = need > gen->capacity - gen->used;
    bool grow_entries = gen->entry_count >= gen->entry_cap;
    uint8_t *new_base = NULL;
    uint32_t *new_offsets = NULL;
    int64_t *new_multiplicity = NULL;
    wl_columnar_memory_reservation_t *payload_reservation = NULL;
    wl_columnar_memory_reservation_t *entries_reservation = NULL;
    bool payload_published = false;
    bool entries_published = false;

    if (!grow_payload && !grow_entries)
        return 0;
    if (grow_payload
        && compound_grow_capacity(gen->capacity, gen->used, need,
        default_cap, &new_payload_cap) != 0)
        return -1;
    if (grow_entries) {
        new_entry_cap = gen->entry_cap > 0
            ? gen->entry_cap * 2u : WL_COMPOUND_DEFAULT_ENTRY_CAP;
        if (new_entry_cap <= gen->entry_cap
            || (uint64_t)new_entry_cap * sizeof(uint32_t) > SIZE_MAX
            || (uint64_t)new_entry_cap * sizeof(int64_t) > SIZE_MAX)
            return -1;
    }
    if (grow_payload && arena->admission_prepare) {
        payload_reservation = malloc(sizeof(*payload_reservation));
        if (!payload_reservation)
            return -1;
        if (compound_admission_prepare(arena, gen->capacity,
            new_payload_cap, payload_reservation) != 0) {
            free(payload_reservation);
            return -1;
        }
    }
    if (grow_entries && arena->admission_prepare) {
        entries_reservation = malloc(sizeof(*entries_reservation));
        if (!entries_reservation)
            goto fail;
    }
    if (grow_entries
        && compound_admission_prepare(arena,
        (uint64_t)gen->entry_cap * (sizeof(uint32_t) + sizeof(int64_t)),
        (uint64_t)new_entry_cap
        * (sizeof(uint32_t) + sizeof(int64_t)),
        entries_reservation) != 0) {
        if (payload_reservation)
            compound_admission_abort(arena, payload_reservation);
        free(payload_reservation);
        free(entries_reservation);
        return -1;
    }
    if (grow_payload) {
        if (compound_failpoint(arena, WL_COMPOUND_FAIL_PAYLOAD))
            goto fail;
        new_base = (uint8_t *)malloc(new_payload_cap);
        if (!new_base)
            goto fail;
        if (gen->used > 0)
            memcpy(new_base, gen->base, gen->used);
    }
    if (grow_entries) {
        if (compound_failpoint(arena, WL_COMPOUND_FAIL_ENTRY_OFFSETS))
            goto fail;
        new_offsets = (uint32_t *)malloc(
            (size_t)new_entry_cap * sizeof(uint32_t));
        if (compound_failpoint(arena, WL_COMPOUND_FAIL_MULTIPLICITY))
            goto fail;
        new_multiplicity = (int64_t *)malloc(
            (size_t)new_entry_cap * sizeof(int64_t));
        if (!new_offsets || !new_multiplicity)
            goto fail;
        if (gen->entry_count > 0) {
            memcpy(new_offsets, gen->entry_offsets,
                (size_t)gen->entry_count * sizeof(uint32_t));
            memcpy(new_multiplicity, gen->multiplicity,
                (size_t)gen->entry_count * sizeof(int64_t));
        }
    }
    /* A reservation returned by prepare is valid until this publish.  The
     * governor commit cannot fail for such a token; keep both publishes
     * adjacent so the ownership swap below is one logical transaction. */
    if (grow_payload) {
        if (compound_admission_publish(arena, gen->payload_admission,
            payload_reservation, gen) != 0)
            goto fail;
        payload_published = true;
    }
    if (grow_entries) {
        if (compound_admission_publish(arena, gen->entries_admission,
            entries_reservation, gen) != 0)
            goto fail;
        entries_published = true;
    }
    if (grow_payload && gen->payload_admission
        && compound_admission_release(arena, gen->payload_admission) != 0)
        goto fail;
    if (grow_entries && gen->entries_admission
        && compound_admission_release(arena, gen->entries_admission) != 0)
        goto fail;
    if (grow_payload) {
        free(gen->base);
        gen->base = new_base;
        gen->capacity = new_payload_cap;
        free(gen->payload_admission);
        gen->payload_admission = payload_reservation;
    }
    if (grow_entries) {
        free(gen->entry_offsets);
        free(gen->multiplicity);
        gen->entry_offsets = new_offsets;
        gen->multiplicity = new_multiplicity;
        gen->entry_cap = new_entry_cap;
        free(gen->entries_admission);
        gen->entries_admission = entries_reservation;
    }
    return 0;

fail:
    free(new_base);
    free(new_offsets);
    free(new_multiplicity);
    if (payload_reservation) {
        if (payload_published)
            (void)compound_admission_release(arena, payload_reservation);
        else
            compound_admission_abort(arena, payload_reservation);
        free(payload_reservation);
    }
    if (entries_reservation) {
        if (entries_published)
            (void)compound_admission_release(arena, entries_reservation);
        else
            compound_admission_abort(arena, entries_reservation);
        free(entries_reservation);
    }
    return -1;
}

static void
gen_free(wl_compound_gen_t *gen)
{
    free(gen->base);
    free(gen->entry_offsets);
    free(gen->multiplicity);
    memset(gen, 0, sizeof(*gen));
}

static void
gen_release_admission(wl_compound_arena_t *arena, wl_compound_gen_t *gen)
{
    if (gen->payload_admission) {
        (void)compound_admission_release(arena, gen->payload_admission);
        free(gen->payload_admission);
    }
    if (gen->entries_admission) {
        (void)compound_admission_release(arena, gen->entries_admission);
        free(gen->entries_admission);
    }
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

static wl_compound_arena_t *
compound_arena_create_impl(uint32_t session_seed, uint32_t default_gen_cap,
    uint32_t max_epochs, void *admission_context,
    void (*admission_release)(void *context),
    wl_compound_admission_prepare_fn admission_prepare_fn,
    wl_compound_admission_publish_fn admission_publish_fn,
    wl_compound_admission_abort_fn admission_abort_fn,
    wl_compound_admission_release_fn admission_release_reservation_fn)
{
    if (default_gen_cap == 0)
        return NULL;
    if (max_epochs == 0)
        max_epochs = WL_COMPOUND_DEFAULT_MAX_EPOCHS;
    if (max_epochs > WL_COMPOUND_DEFAULT_MAX_EPOCHS)
        max_epochs = WL_COMPOUND_DEFAULT_MAX_EPOCHS;

    wl_compound_arena_t *arena
        = (wl_compound_arena_t *)calloc(1, sizeof(wl_compound_arena_t));
    if (!arena)
        return NULL;

    arena->gens = (wl_compound_gen_t *)calloc(max_epochs,
            sizeof(wl_compound_gen_t));
    if (!arena->gens) {
        free(arena);
        return NULL;
    }

    arena->session_seed = session_seed & 0xFFFFFu;
    arena->current_epoch = 0;
    arena->max_epochs = max_epochs;
    arena->default_gen_cap = default_gen_cap;
    arena->frozen = false;
    arena->live_handles = 0;
    compound_gate_store(&arena->access_gate, 0);
    compound_gate_store(&arena->gc_hold_count, 0);
    arena->test_failpoint = WL_COMPOUND_FAIL_NONE;
    arena->admission_context = admission_context;
    arena->admission_release = admission_release;
    arena->admission_prepare = admission_prepare_fn;
    arena->admission_publish = admission_publish_fn;
    arena->admission_abort = admission_abort_fn;
    arena->admission_release_reservation
        = admission_release_reservation_fn;
    return arena;
}

wl_compound_arena_t *
wl_compound_arena_create(uint32_t session_seed, uint32_t default_gen_cap,
    uint32_t max_epochs)
{
    return compound_arena_create_impl(session_seed, default_gen_cap,
               max_epochs, NULL, NULL, NULL, NULL, NULL, NULL);
}

wl_compound_arena_t *
wl_compound_arena_create_with_admission(uint32_t session_seed,
    uint32_t default_gen_cap, uint32_t max_epochs, void *context,
    void (*release)(void *context),
    wl_compound_admission_prepare_fn prepare,
    wl_compound_admission_publish_fn publish,
    wl_compound_admission_abort_fn abort,
    wl_compound_admission_release_fn release_reservation)
{
    return compound_arena_create_impl(session_seed, default_gen_cap,
               max_epochs, context, release, prepare, publish, abort,
               release_reservation);
}

bool
wl_compound_arena_borrow(wl_compound_arena_t *arena,
    wl_compound_arena_borrow_t *borrow)
{
    uint64_t observed;

    if (!arena || !borrow || borrow->arena || borrow->identity != 0)
        return false;
    observed = compound_gate_load(&arena->access_gate);
    for (;;) {
        if (observed == WL_COMPOUND_GATE_WRITER
            || observed >= WL_COMPOUND_GATE_READERS_MAX)
            return false;
        if (atomic_compare_exchange_weak_explicit(&arena->access_gate,
            &observed, observed + 1u, memory_order_acquire,
            memory_order_relaxed))
            break;
    }
    borrow->arena = arena;
    borrow->identity = (uintptr_t)borrow;
    return true;
}

bool
wl_compound_arena_borrow_release(wl_compound_arena_borrow_t *borrow)
{
    wl_compound_arena_t *arena;
    uint64_t observed;

    if (!borrow || !borrow->arena
        || borrow->identity != (uintptr_t)borrow)
        return false;
    arena = borrow->arena;
    observed = compound_gate_load(&arena->access_gate);
    for (;;) {
        if (observed == 0 || observed == WL_COMPOUND_GATE_WRITER)
            return false;
        if (atomic_compare_exchange_weak_explicit(&arena->access_gate,
            &observed, observed - 1u, memory_order_release,
            memory_order_relaxed))
            break;
    }
    borrow->arena = NULL;
    borrow->identity = 0;
    return true;
}

bool
wl_compound_arena_mutation_begin(wl_compound_arena_t *arena,
    wl_compound_arena_mutation_t *mutation)
{
    uint64_t expected = 0;

    if (!arena || !mutation || mutation->arena || mutation->identity != 0)
        return false;
    for (;;) {
        expected = 0;
        if (atomic_compare_exchange_weak_explicit(&arena->access_gate,
            &expected, WL_COMPOUND_GATE_WRITER, memory_order_acquire,
            memory_order_relaxed))
            break;
        if (expected != 0)
            return false;
    }
    mutation->arena = arena;
    mutation->identity = (uintptr_t)mutation;
    return true;
}

bool
wl_compound_arena_mutation_end(wl_compound_arena_mutation_t *mutation)
{
    wl_compound_arena_t *arena;

    if (!mutation || !mutation->arena
        || mutation->identity != (uintptr_t)mutation)
        return false;
    arena = mutation->arena;
    if (compound_gate_load(&arena->access_gate)
        != WL_COMPOUND_GATE_WRITER)
        return false;
    compound_gate_store(&arena->access_gate, 0);
    mutation->arena = NULL;
    mutation->identity = 0;
    return true;
}

void
wl_compound_arena_test_fail_next(wl_compound_arena_t *arena,
    wl_compound_alloc_failpoint_t failpoint)
{
    if (arena)
        arena->test_failpoint = (uint32_t)failpoint;
}

int
wl_arena_compound_arena_gc_hold_acquire(wl_compound_arena_t *arena,
    wl_arena_compound_arena_gc_hold_t *hold)
{
    wl_compound_arena_borrow_t borrow = { 0 };
    uint64_t observed;

    if (!arena || !hold || hold->arena || hold->identity != 0)
        return EINVAL;
    if (!wl_compound_arena_borrow(arena, &borrow))
        return EBUSY;
    observed = compound_gate_load(&arena->gc_hold_count);
    for (;;) {
        if (observed == UINT64_MAX) {
            (void)wl_compound_arena_borrow_release(&borrow);
            return EOVERFLOW;
        }
        if (atomic_compare_exchange_weak_explicit(&arena->gc_hold_count,
            &observed, observed + 1u, memory_order_acquire,
            memory_order_relaxed))
            break;
    }
    hold->arena = arena;
    hold->identity = (uintptr_t)hold;
    (void)wl_compound_arena_borrow_release(&borrow);
    return 0;
}

int
wl_arena_compound_arena_gc_hold_release(wl_arena_compound_arena_gc_hold_t *hold)
{
    wl_compound_arena_t *arena;
    uint64_t observed;

    if (!hold || !hold->arena || hold->identity != (uintptr_t)hold)
        return EINVAL;
    arena = hold->arena;
    observed = compound_gate_load(&arena->gc_hold_count);
    if (observed == 0)
        return EINVAL;
    hold->arena = NULL;
    hold->identity = 0;
    for (;;) {
        /* A valid token contributes one count, keeping arena alive through
         * retries. After publishing its removal, never access arena again. */
        if (atomic_compare_exchange_weak_explicit(&arena->gc_hold_count,
            &observed, observed - 1u, memory_order_release,
            memory_order_relaxed))
            return 0;
        if (observed == 0) {
            hold->arena = arena;
            hold->identity = (uintptr_t)hold;
            return EINVAL;
        }
    }
}

bool
wl_compound_arena_free_checked(wl_compound_arena_t *arena)
{
    wl_compound_arena_mutation_t mutation = { 0 };

    if (!arena)
        return true;
    if (!wl_compound_arena_mutation_begin(arena, &mutation))
        return false;
    if (compound_gate_load(&arena->gc_hold_count) != 0) {
        (void)wl_compound_arena_mutation_end(&mutation);
        return false;
    }
    if (arena->gens) {
        for (uint32_t e = 0; e < arena->max_epochs; e++)
            gen_release_admission(arena, &arena->gens[e]);
        for (uint32_t e = 0; e < arena->max_epochs; e++)
            gen_free(&arena->gens[e]);
        free(arena->gens);
    }
    if (arena->admission_release)
        arena->admission_release(arena->admission_context);
    /* Keep the writer gate held until the final free.  No caller can observe
     * the local mutation token after this function returns, so publishing a
     * reader-available gate would only create a teardown race. */
    free(arena);
    return true;
}

void
wl_compound_arena_free(wl_compound_arena_t *arena)
{
    (void)wl_compound_arena_free_checked(arena);
}

static uint64_t
wl_compound_arena_alloc_mutating(wl_compound_arena_t *arena, uint32_t size)
{
    if (!arena || size == 0 || arena->frozen)
        return WL_COMPOUND_HANDLE_NULL;
    if (arena->current_epoch >= arena->max_epochs)
        return WL_COMPOUND_HANDLE_NULL;

    if (size > UINT32_MAX - (WL_COMPOUND_ALIGN - 1u))
        return WL_COMPOUND_HANDLE_NULL;
    uint32_t aligned = WL_COMPOUND_ALIGN_UP(size);

    wl_compound_gen_t *gen = &arena->gens[arena->current_epoch];
    uint32_t reserve_need = aligned;
    bool avoid_null_handle = arena->session_seed == 0
        && arena->current_epoch == 0 && gen->used == 0;
    if (avoid_null_handle) {
        if (aligned > UINT32_MAX - WL_COMPOUND_ALIGN)
            return WL_COMPOUND_HANDLE_NULL;
        reserve_need = aligned + WL_COMPOUND_ALIGN;
    }

    /* Stage payload and metadata growth together. */
    if (gen_reserve(arena, gen, reserve_need, arena->default_gen_cap) != 0)
        return WL_COMPOUND_HANDLE_NULL;

    uint32_t offset = gen->used;
    /* Guarantee non-null handles: bump the offset by WL_COMPOUND_ALIGN in
     * generation 0 so the very first allocation yields a non-zero handle.
     * (A zero session_seed + zero epoch + zero offset would collide with
     * WL_COMPOUND_HANDLE_NULL.) */
    if (avoid_null_handle) {
        offset = WL_COMPOUND_ALIGN;
        gen->used = WL_COMPOUND_ALIGN;
    }

    gen->entry_offsets[gen->entry_count] = offset;
    gen->multiplicity[gen->entry_count] = 1; /* caller holds one reference */
    gen->entry_count++;
    gen->used = offset + aligned;

    arena->live_handles++;
    uint64_t handle = wl_compound_handle_pack(arena->session_seed,
            arena->current_epoch, offset);
    WL_LOG(WL_LOG_SEC_ARENA, WL_LOG_TRACE,
        "handle_alloc(epoch=%u, offset=%u, packed=0x%" PRIx64 ")",
        arena->current_epoch, offset, handle);
    /* Issue #583: lifecycle audit trail under the COMPOUND section so a
     * single `WL_LOG=COMPOUND:5` captures end-to-end side-relation
     * activity without enabling the noisier generic-allocator ARENA
     * traces above. */
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=alloc handle=0x%" PRIx64 " size=%u epoch=%u",
        handle, size, arena->current_epoch);
    return handle;
}

uint64_t
wl_compound_arena_alloc(wl_compound_arena_t *arena, uint32_t size)
{
    wl_compound_arena_mutation_t mutation = { 0 };
    uint64_t handle;

    if (!wl_compound_arena_mutation_begin(arena, &mutation))
        return WL_COMPOUND_HANDLE_NULL;
    handle = wl_compound_arena_alloc_mutating(arena, size);
    (void)wl_compound_arena_mutation_end(&mutation);
    return handle;
}

/* Internal: locate the entry slot for a handle.  Returns the entry index or
 * (uint32_t)-1 if the handle cannot be resolved.  The offset field matches
 * entry_offsets[i] exactly because alloc returns them verbatim. */
static uint32_t
locate_entry(const wl_compound_arena_t *arena, uint64_t handle,
    const wl_compound_gen_t **out_gen)
{
    if (!arena || handle == WL_COMPOUND_HANDLE_NULL)
        return (uint32_t)-1;
    if (wl_compound_handle_session(handle) != arena->session_seed)
        return (uint32_t)-1;
    uint32_t epoch = wl_compound_handle_epoch(handle);
    if (epoch >= arena->max_epochs)
        return (uint32_t)-1;
    uint32_t offset = wl_compound_handle_offset(handle);
    const wl_compound_gen_t *gen = &arena->gens[epoch];
    if (offset >= gen->used)
        return (uint32_t)-1;
    /* Linear scan: entry_count is small (tens - hundreds per epoch for
     * typical workloads).  A sorted invariant lets us binary-search later
     * without changing the API contract. */
    for (uint32_t i = 0; i < gen->entry_count; i++) {
        if (gen->entry_offsets[i] == offset) {
            if (out_gen)
                *out_gen = gen;
            return i;
        }
    }
    return (uint32_t)-1;
}

const void *
wl_compound_arena_lookup(const wl_compound_arena_t *arena, uint64_t handle,
    uint32_t *out_size)
{
    const wl_compound_gen_t *gen = NULL;
    uint32_t idx = locate_entry(arena, handle, &gen);
    if (idx == (uint32_t)-1 || !gen)
        return NULL;
    uint32_t offset = gen->entry_offsets[idx];
    uint32_t next = (idx + 1 < gen->entry_count)
        ? gen->entry_offsets[idx + 1]
        : gen->used;
    if (out_size)
        *out_size = next - offset;
    return gen->base + offset;
}

const void *
wl_compound_arena_lookup_borrowed(
    const wl_compound_arena_borrow_t *borrow, uint64_t handle,
    uint32_t *out_size)
{
    if (!borrow || !borrow->arena
        || borrow->identity != (uintptr_t)borrow)
        return NULL;
    return wl_compound_arena_lookup(borrow->arena, handle, out_size);
}

int64_t
wl_compound_arena_multiplicity(const wl_compound_arena_t *arena,
    uint64_t handle)
{
    const wl_compound_gen_t *gen = NULL;
    uint32_t idx = locate_entry(arena, handle, &gen);
    if (idx == (uint32_t)-1 || !gen)
        return 0;
    return gen->multiplicity[idx];
}

static int
wl_compound_arena_retain_mutating(wl_compound_arena_t *arena, uint64_t handle,
    int64_t delta)
{
    if (!arena || delta == 0)
        return arena ? 0 : -1;
    /* Use const lookup to locate, then mutate by casting the gens pointer.
     * The generation belongs to `arena` which is non-const here. */
    const wl_compound_gen_t *cgen = NULL;
    uint32_t idx = locate_entry(arena, handle, &cgen);
    if (idx == (uint32_t)-1 || !cgen)
        return -1;
    uint32_t epoch = wl_compound_handle_epoch(handle);
    wl_compound_gen_t *gen = &arena->gens[epoch];
    int64_t before = gen->multiplicity[idx];
    int64_t after = before + delta;
    gen->multiplicity[idx] = after;
    if (before > 0 && after <= 0 && arena->live_handles > 0)
        arena->live_handles--;
    else if (before <= 0 && after > 0)
        arena->live_handles++;
    return 0;
}

int
wl_compound_arena_retain(wl_compound_arena_t *arena, uint64_t handle,
    int64_t delta)
{
    wl_compound_arena_mutation_t mutation = { 0 };
    int result;

    if (!wl_compound_arena_mutation_begin(arena, &mutation))
        return -1;
    result = wl_compound_arena_retain_mutating(arena, handle, delta);
    (void)wl_compound_arena_mutation_end(&mutation);
    return result;
}

void
wl_compound_arena_freeze(wl_compound_arena_t *arena)
{
    wl_compound_arena_mutation_t mutation = { 0 };

    if (!arena)
        return;
    if (!wl_compound_arena_mutation_begin(arena, &mutation))
        return;
    arena->frozen = true;
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=freeze epoch=%u live_handles=%" PRIu64,
        arena->current_epoch, arena->live_handles);
    (void)wl_compound_arena_mutation_end(&mutation);
}

void
wl_compound_arena_unfreeze(wl_compound_arena_t *arena)
{
    wl_compound_arena_mutation_t mutation = { 0 };

    if (!arena)
        return;
    if (!wl_compound_arena_mutation_begin(arena, &mutation))
        return;
    arena->frozen = false;
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=unfreeze epoch=%u live_handles=%" PRIu64,
        arena->current_epoch, arena->live_handles);
    (void)wl_compound_arena_mutation_end(&mutation);
}

static uint32_t
wl_compound_arena_gc_epoch_boundary_mutating(wl_compound_arena_t *arena)
{
    if (!arena)
        return (uint32_t)-1;
    if (arena->current_epoch >= arena->max_epochs) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "lifecycle event=gc_skip_saturated current_epoch=%u max_epochs=%u",
            arena->current_epoch, arena->max_epochs);
        return 0;
    }
    if (arena->frozen) {
        /* Issue #561 / #584: freeze guard is a no-op skip, not a reset.
         * Returning the unchanged current_epoch (rather than 0) lets
         * callers compare against a pre-call snapshot to detect "no
         * advance" without conflating it with "epoch zero advanced".
         *
         * Log under COMPOUND alongside the lifecycle traces in
         * arena_alloc / freeze / unfreeze (Issue #583) so a single
         * `WL_LOG=COMPOUND:5` captures the skip event in the same
         * audit stream. */
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "lifecycle event=gc_skip_frozen current_epoch=%u",
            arena->current_epoch);
        return arena->current_epoch;
    }
    /* Skeleton policy: sweep the current generation, count reclaimable
     * handles (multiplicity <= 0), and reset the generation's bump buffer
     * so it can be reused.  Subsequent allocations go into the same
     * generation when advancement would overflow max_epochs; otherwise we
     * advance so the 12-bit epoch field distinguishes generational scopes.
     *
     * Full generational reclaim (move-live-to-next-gen) is tracked in the
     * #533 close-out but is not required by the D1-D4 scaffolding tests.
     */
    wl_compound_gen_t *gen = &arena->gens[arena->current_epoch];
    uint32_t reclaimed = 0;
    for (uint32_t i = 0; i < gen->entry_count; i++) {
        if (gen->multiplicity[i] <= 0) {
            reclaimed++;
            /* Nothing else to do here: live_handles was already decremented
             * when the multiplicity transitioned through zero via
             * arena_retain. */
        }
    }
    /* Reset generation for reuse. */
    gen->used = 0;
    gen->entry_count = 0;
    uint32_t closed_epoch = arena->current_epoch;
    /* Advance epoch if room; otherwise the arena saturates and alloc will
     * refuse further allocations (callers rotate arenas). */
    if (arena->current_epoch + 1 < arena->max_epochs)
        arena->current_epoch++;
    else
        arena->current_epoch = arena->max_epochs; /* sentinel: saturated */
    WL_LOG(WL_LOG_SEC_ARENA, WL_LOG_INFO,
        "gc_epoch_boundary(epoch=%u, freed_handles=%u, remaining=%" PRIu64 ")",
        closed_epoch, reclaimed, arena->live_handles);
    if (arena->current_epoch + 5u >= arena->max_epochs) {
        WL_LOG(WL_LOG_SEC_ARENA, WL_LOG_WARN,
            "epoch nearing saturation (current=%u, max=%u)",
            arena->current_epoch, arena->max_epochs);
    }
    return reclaimed;
}

uint32_t
wl_compound_arena_gc_epoch_boundary(wl_compound_arena_t *arena)
{
    wl_compound_arena_mutation_t mutation = { 0 };
    uint32_t result;

    if (!wl_compound_arena_mutation_begin(arena, &mutation))
        return arena ? arena->current_epoch : (uint32_t)-1;
    if (compound_gate_load(&arena->gc_hold_count) != 0)
        result = arena->current_epoch;
    else
        result = wl_compound_arena_gc_epoch_boundary_mutating(arena);
    (void)wl_compound_arena_mutation_end(&mutation);
    return result;
}

int
wl_arena_compound_arena_gc_epoch_boundary_checked(wl_compound_arena_t *arena,
    uint32_t *reclaimed)
{
    wl_compound_arena_mutation_t mutation = { 0 };

    if (!arena || !reclaimed)
        return EINVAL;
    if (!wl_compound_arena_mutation_begin(arena, &mutation))
        return EBUSY;
    if (compound_gate_load(&arena->gc_hold_count) != 0 || arena->frozen) {
        (void)wl_compound_arena_mutation_end(&mutation);
        return EBUSY;
    }
    *reclaimed = wl_compound_arena_gc_epoch_boundary_mutating(arena);
    (void)wl_compound_arena_mutation_end(&mutation);
    return 0;
}
