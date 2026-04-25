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
#include <stdlib.h>
#include <string.h>

/* 8-byte alignment mirrors the main wl_arena_t contract; keeps payloads
 * suitable for int64_t element access. */
#define WL_COMPOUND_ALIGN 8u
#define WL_COMPOUND_ALIGN_UP(n) \
        (((n) + (WL_COMPOUND_ALIGN - 1)) & ~(uint32_t)(WL_COMPOUND_ALIGN - 1))

/* Default max_epochs when the caller passes 0. */
#define WL_COMPOUND_DEFAULT_MAX_EPOCHS (WL_COMPOUND_EPOCH_MAX + 1u)

/* Initial entry-index capacity per generation.  Small (16) because we
 * lazy-grow; most epochs with a handful of compounds pay only this much. */
#define WL_COMPOUND_DEFAULT_ENTRY_CAP 16u

/* ======================================================================== */
/* Internal helpers                                                         */
/* ======================================================================== */

static int
gen_reserve_entries(wl_compound_gen_t *gen)
{
    if (gen->entry_count < gen->entry_cap)
        return 0;
    uint32_t new_cap = gen->entry_cap > 0
        ? gen->entry_cap * 2u
        : WL_COMPOUND_DEFAULT_ENTRY_CAP;
    if (new_cap <= gen->entry_cap) /* overflow guard */
        return -1;
    uint32_t *no = (uint32_t *)realloc(gen->entry_offsets,
            (size_t)new_cap * sizeof(uint32_t));
    if (!no)
        return -1;
    gen->entry_offsets = no;
    int64_t *nm = (int64_t *)realloc(gen->multiplicity,
            (size_t)new_cap * sizeof(int64_t));
    if (!nm)
        return -1; /* entry_offsets was upgraded; left as-is (safe; only cap */
                   /* mismatch is a temporary state reverted on next retry). */
    gen->multiplicity = nm;
    gen->entry_cap = new_cap;
    return 0;
}

static int
gen_reserve_bytes(wl_compound_gen_t *gen, uint32_t need, uint32_t default_cap)
{
    uint32_t avail = gen->capacity - gen->used;
    if (need <= avail)
        return 0;
    uint32_t new_cap = gen->capacity > 0 ? gen->capacity : default_cap;
    while (new_cap - gen->used < need) {
        uint32_t doubled = new_cap * 2u;
        if (doubled <= new_cap) /* overflow */
            return -1;
        new_cap = doubled;
    }
    uint8_t *nb = (uint8_t *)realloc(gen->base, new_cap);
    if (!nb)
        return -1;
    gen->base = nb;
    gen->capacity = new_cap;
    return 0;
}

static void
gen_free(wl_compound_gen_t *gen)
{
    free(gen->base);
    free(gen->entry_offsets);
    free(gen->multiplicity);
    memset(gen, 0, sizeof(*gen));
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

wl_compound_arena_t *
wl_compound_arena_create(uint32_t session_seed, uint32_t default_gen_cap,
    uint32_t max_epochs)
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
    return arena;
}

void
wl_compound_arena_free(wl_compound_arena_t *arena)
{
    if (!arena)
        return;
    if (arena->gens) {
        for (uint32_t e = 0; e < arena->max_epochs; e++)
            gen_free(&arena->gens[e]);
        free(arena->gens);
    }
    free(arena);
}

uint64_t
wl_compound_arena_alloc(wl_compound_arena_t *arena, uint32_t size)
{
    if (!arena || size == 0 || arena->frozen)
        return WL_COMPOUND_HANDLE_NULL;
    if (arena->current_epoch >= arena->max_epochs)
        return WL_COMPOUND_HANDLE_NULL;

    uint32_t aligned = WL_COMPOUND_ALIGN_UP(size);
    if (aligned < size) /* overflow */
        return WL_COMPOUND_HANDLE_NULL;

    wl_compound_gen_t *gen = &arena->gens[arena->current_epoch];

    /* Grow payload buffer if needed. */
    if (gen_reserve_bytes(gen, aligned, arena->default_gen_cap) != 0)
        return WL_COMPOUND_HANDLE_NULL;
    if (gen_reserve_entries(gen) != 0)
        return WL_COMPOUND_HANDLE_NULL;

    uint32_t offset = gen->used;
    /* Guarantee non-null handles: bump the offset by WL_COMPOUND_ALIGN in
     * generation 0 so the very first allocation yields a non-zero handle.
     * (A zero session_seed + zero epoch + zero offset would collide with
     * WL_COMPOUND_HANDLE_NULL.) */
    if (arena->session_seed == 0 && arena->current_epoch == 0 && offset == 0) {
        if (gen_reserve_bytes(gen, aligned + WL_COMPOUND_ALIGN,
            arena->default_gen_cap) != 0)
            return WL_COMPOUND_HANDLE_NULL;
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

int
wl_compound_arena_retain(wl_compound_arena_t *arena, uint64_t handle,
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

void
wl_compound_arena_freeze(wl_compound_arena_t *arena)
{
    if (arena)
        arena->frozen = true;
}

void
wl_compound_arena_unfreeze(wl_compound_arena_t *arena)
{
    if (arena)
        arena->frozen = false;
}

uint32_t
wl_compound_arena_gc_epoch_boundary(wl_compound_arena_t *arena)
{
    if (!arena)
        return (uint32_t)-1;
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
