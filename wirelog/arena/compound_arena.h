/*
 * arena/compound_arena.h - Compound-term Arena & Handle Allocator (Issue #533)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * ========================================================================
 * Overview
 * ========================================================================
 *
 * The side-relation tier stores compound terms (e.g. `scope(metadata(t,
 * ts, loc, risk))`) outside the main row by encoding each compound as a
 * 64-bit handle.  The compound arena is a session-local, epoch-scoped
 * bump-pointer allocator that owns the raw payload bytes and the
 * per-handle multiplicity counter used by the epoch-frontier GC.
 *
 * Lifecycle (D1-D4 skeleton, Issue #533):
 *
 *   1.  wl_compound_arena_create() is called once per session (right after
 *       the session's main wl_arena_t is constructed).
 *   2.  At compound insertion, the caller reserves `size` bytes and receives
 *       a 64-bit handle (arena_alloc).  The handle is stored in the main
 *       col_rel_t row in place of the compound value.
 *   3.  On every frontier advance, arena_gc_epoch_boundary() walks the
 *       multiplicity table; handles whose Z-set count has reached zero
 *       are eligible for reclamation (skeleton: cleared in the generation
 *       table; full generational reclaim is finished in a follow-up issue).
 *   4.  K-Fusion freezes the arena (read-only) for the duration of a
 *       parallel plan execution; arena_freeze/unfreeze raise a sticky
 *       guard that causes arena_alloc to return 0 while frozen.
 *
 * Handle format (64 bits, little-endian bit packing):
 *
 *       63                 44 43                 32 31                   0
 *      +---------------------+---------------------+-----------------------+
 *      |   session_seed (20) |      epoch (12)     |       offset (32)     |
 *      +---------------------+---------------------+-----------------------+
 *
 *   - session_seed (20b): low 20 bits of the session's creation timestamp.
 *     Protects against cross-session handle reuse in multi-session harness
 *     tests (handles from session A must not be interpreted by session B).
 *   - epoch         (12b): generation counter within the arena.  Bumped by
 *     arena_gc_epoch_boundary().  Capped at 4095; the arena refuses new
 *     allocations after the cap (caller must create a fresh arena).
 *   - offset        (32b): byte offset into the current generation buffer.
 *     With 4-byte alignment this gives 16 GiB per generation — enough for
 *     any realistic compound-term workload.
 *
 * Thread safety: NOT thread-safe.  The arena is single-threaded by design
 * (single mutation window outside K-Fusion; frozen during K-Fusion).
 *
 * The handle value 0 (WL_COMPOUND_HANDLE_NULL) is reserved and is never
 * returned by arena_alloc.  Callers use 0 to indicate "no compound"
 * (e.g. a row that has no side-relation reference in this column).
 */

#ifndef WL_COMPOUND_ARENA_H
#define WL_COMPOUND_ARENA_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "columnar/memory_governor.h"

/* ======================================================================== */
/* Handle encoding                                                          */
/* ======================================================================== */

/** Null handle sentinel: guaranteed never returned by arena_alloc. */
#define WL_COMPOUND_HANDLE_NULL ((uint64_t)0)

/** Maximum epoch value (12 bits). Arena refuses new allocations when the
 *  current epoch exceeds this cap. */
#define WL_COMPOUND_EPOCH_MAX ((uint32_t)0xFFF)

/** Default generation table length when create() receives max_epochs == 0. */
#define WL_COMPOUND_DEFAULT_MAX_EPOCHS (WL_COMPOUND_EPOCH_MAX + 1u)

/** Maximum per-generation offset (32 bits). */
#define WL_COMPOUND_OFFSET_MAX ((uint32_t)0xFFFFFFFFu)

/**
 * Pack a handle from its three fields.  Fields are masked to the advertised
 * widths so callers may pass values derived from larger counters.
 */
static inline uint64_t
wl_compound_handle_pack(uint32_t session_seed, uint32_t epoch, uint32_t offset)
{
    return ((uint64_t)(session_seed & 0xFFFFFu) << 44)
           | ((uint64_t)(epoch & 0xFFFu) << 32)
           | ((uint64_t)offset);
}

/** Extract the 20-bit session seed from a handle. */
static inline uint32_t
wl_compound_handle_session(uint64_t handle)
{
    return (uint32_t)((handle >> 44) & 0xFFFFFu);
}

/** Extract the 12-bit epoch field from a handle. */
static inline uint32_t
wl_compound_handle_epoch(uint64_t handle)
{
    return (uint32_t)((handle >> 32) & 0xFFFu);
}

/** Extract the 32-bit offset field from a handle. */
static inline uint32_t
wl_compound_handle_offset(uint64_t handle)
{
    return (uint32_t)(handle & 0xFFFFFFFFu);
}

/* ======================================================================== */
/* Arena structure                                                          */
/* ======================================================================== */

/*
 * wl_compound_gen_t: one generation (epoch) of payload bytes + per-handle
 * multiplicity counters.
 *
 * Each generation is a contiguous bump buffer; entries[] stores the byte
 * offset at which each handle was allocated so the GC can walk handles in
 * allocation order without chasing pointers.
 */
typedef struct {
    uint8_t *base;           /* malloc-owned payload bytes (NULL until first alloc) */
    uint32_t capacity;       /* capacity in bytes */
    uint32_t used;           /* bytes allocated so far */
    uint32_t *entry_offsets; /* offset into base for each handle in this gen */
    int64_t *multiplicity;   /* Z-set multiplicity counter per handle (signed) */
    uint32_t entry_count;    /* number of handles allocated in this generation */
    uint32_t entry_cap;      /* capacity of entry_offsets[] / multiplicity[] */
    wl_columnar_memory_reservation_t *payload_admission;
    wl_columnar_memory_reservation_t *entries_admission;
} wl_compound_gen_t;

struct wl_compound_arena;
typedef struct wl_compound_arena wl_compound_arena_t;

typedef struct {
    wl_compound_arena_t *arena;
    uintptr_t identity;
} wl_compound_arena_borrow_t;

typedef struct {
    wl_compound_arena_t *arena;
    uintptr_t identity;
} wl_compound_arena_mutation_t;

typedef struct {
    wl_compound_arena_t *arena;
    uintptr_t identity;
} wl_arena_compound_arena_gc_hold_t;

typedef enum {
    WL_COMPOUND_FAIL_NONE = 0,
    WL_COMPOUND_FAIL_PAYLOAD = 1,
    WL_COMPOUND_FAIL_ENTRY_OFFSETS = 2,
    WL_COMPOUND_FAIL_MULTIPLICITY = 3,
} wl_compound_alloc_failpoint_t;

typedef int (*wl_compound_admission_prepare_fn)(void *context,
    uint64_t old_bytes, uint64_t new_bytes,
    wl_columnar_memory_reservation_t *reservation);
typedef int (*wl_compound_admission_publish_fn)(void *context,
    wl_columnar_memory_reservation_t *old_reservation,
    wl_columnar_memory_reservation_t *new_reservation, const void *owner);
typedef void (*wl_compound_admission_abort_fn)(void *context,
    wl_columnar_memory_reservation_t *reservation);
typedef int (*wl_compound_admission_release_fn)(void *context,
    wl_columnar_memory_reservation_t *reservation);

/**
 * wl_compound_arena_t:
 *
 * Session-local compound arena.  Owns an array of generations indexed by
 * epoch.  `frozen` is a runtime guard that causes arena_alloc to return 0
 * (NULL handle) while K-Fusion is executing.
 *
 * @session_seed: Low 20 bits of the session's creation counter; copied into
 *                every handle for cross-session integrity checks.
 * @current_epoch: Generation currently accepting new allocations.
 * @gens:         Array of generations, length == max_epochs.
 * @max_epochs:   Allocated length of gens[]; bounded by WL_COMPOUND_EPOCH_MAX + 1.
 * @default_gen_cap: Initial capacity per generation (bytes). Generations
 *                   grow transactionally on overflow when managed.
 * @frozen:       True while K-Fusion is executing; arena_alloc returns 0.
 * @live_handles: Total live handles (multiplicity > 0) across all epochs,
 *                maintained by arena_alloc/inc/dec/gc for test introspection.
 */
struct wl_compound_arena {
    uint32_t session_seed;
    uint32_t current_epoch;
    wl_compound_gen_t *gens;
    uint32_t max_epochs;
    uint32_t default_gen_cap;
    bool frozen;
    uint64_t live_handles;
    /* Optional owner for fixed arena/generation-table admission. */
    void *admission_context;
    void (*admission_release)(void *context);
    wl_compound_admission_prepare_fn admission_prepare;
    wl_compound_admission_publish_fn admission_publish;
    wl_compound_admission_abort_fn admission_abort;
    wl_compound_admission_release_fn admission_release_reservation;
    wl_atomic_u64 access_gate;
    wl_atomic_u64 gc_hold_count;
    uint32_t test_failpoint;
};

/* ======================================================================== */
/* API                                                                      */
/* ======================================================================== */

/**
 * wl_compound_arena_create:
 * @session_seed:    20-bit session identifier (masked internally).
 * @default_gen_cap: Initial capacity per generation, in bytes (must be > 0).
 * @max_epochs:      Maximum number of generations; clamped to the 12-bit cap.
 *                   Pass 0 for the default (WL_COMPOUND_EPOCH_MAX + 1).
 *
 * Allocate a new compound arena.  Generations are lazy-allocated on first
 * insertion, so the up-front memory cost is only the gens[] array.
 *
 * Returns: non-NULL pointer on success, NULL on allocation failure or
 *          invalid arguments.
 */
wl_compound_arena_t *
wl_compound_arena_create(uint32_t session_seed, uint32_t default_gen_cap,
    uint32_t max_epochs);

/** Create an arena with admission for fixed metadata and retained growth. */
wl_compound_arena_t *
wl_compound_arena_create_managed(uint32_t session_seed,
    uint32_t default_gen_cap, uint32_t max_epochs,
    wl_columnar_memory_governor_t *governor);

/* Internal callback form used by the governor glue translation unit. */
wl_compound_arena_t *
wl_compound_arena_create_with_admission(uint32_t session_seed,
    uint32_t default_gen_cap, uint32_t max_epochs, void *context,
    void (*release)(void *context),
    wl_compound_admission_prepare_fn prepare,
    wl_compound_admission_publish_fn publish,
    wl_compound_admission_abort_fn abort,
    wl_compound_admission_release_fn release_reservation);

bool
wl_compound_arena_borrow(wl_compound_arena_t *arena,
    wl_compound_arena_borrow_t *borrow);
bool
wl_compound_arena_borrow_release(wl_compound_arena_borrow_t *borrow);
bool
wl_compound_arena_mutation_begin(wl_compound_arena_t *arena,
    wl_compound_arena_mutation_t *mutation);
bool
wl_compound_arena_mutation_end(wl_compound_arena_mutation_t *mutation);
bool
wl_compound_arena_free_checked(wl_compound_arena_t *arena);

/** Protect handle IDs from GC and destruction without blocking allocation or
 * multiplicity updates. Tokens must be zero-initialized, remain at a stable
 * address, and must not be copied or manipulated concurrently. The caller must
 * already own a live arena when acquiring. A hold does not protect payload
 * pointers from relocation; use an ordinary borrow for pointer access.
 * Returns 0, EINVAL (invalid token), EBUSY (writer), or EOVERFLOW. */
int
wl_arena_compound_arena_gc_hold_acquire(wl_compound_arena_t *arena,
    wl_arena_compound_arena_gc_hold_t *hold);

/** Release without acquiring the mutation gate or implicitly running GC.
 * Returns 0 or EINVAL; invalid tokens are unchanged. */
int
wl_arena_compound_arena_gc_hold_release(
    wl_arena_compound_arena_gc_hold_t *hold);

/** Collect only when no hold, borrow, writer, or freeze prevents collection.
 * Returns EINVAL for invalid arguments, EBUSY without changing *reclaimed on
 * refusal, or 0 with the reclaimed count. Saturated arenas return 0 reclaimed.
 */
int
wl_arena_compound_arena_gc_epoch_boundary_checked(wl_compound_arena_t *arena,
    uint32_t *reclaimed);
void
wl_compound_arena_test_fail_next(wl_compound_arena_t *arena,
    wl_compound_alloc_failpoint_t failpoint);

/**
 * wl_compound_arena_free:
 * @arena: (transfer full): arena to free. NULL-safe.
 *
 * Free the arena and all per-generation buffers.  Any handle previously
 * returned by wl_compound_arena_alloc becomes invalid.  Callers which need
 * to observe an active worker lease must use free_checked().
 */
void
wl_compound_arena_free(wl_compound_arena_t *arena);

/**
 * wl_compound_arena_alloc:
 * @arena: arena to allocate from. Must not be NULL.
 * @size:  payload size in bytes. Must be > 0.
 *
 * Reserve @size bytes in the current generation, register a new handle, and
 * return the 64-bit handle.  Rounded up to 8-byte alignment internally.
 * Initial multiplicity is 1 (one reference inserted by the caller).
 *
 * Returns:
 *   non-zero handle on success.
 *   WL_COMPOUND_HANDLE_NULL (0) if the arena is frozen, exhausted, or
 *                              out of memory.
 */
uint64_t
wl_compound_arena_alloc(wl_compound_arena_t *arena, uint32_t size);

/**
 * wl_compound_arena_lookup:
 * @arena:    arena that owns the handle.
 * @handle:   64-bit handle returned by a previous arena_alloc.
 * @out_size: (out, optional) payload size for the handle.
 *
 * Return a read-only pointer to the payload bytes for @handle, or NULL
 * if the handle does not belong to this arena, the epoch has been GC'd,
 * or the offset is out of range.  The pointer is valid until the next
 * arena_alloc into the same generation.
 */
const void *
wl_compound_arena_lookup(const wl_compound_arena_t *arena, uint64_t handle,
    uint32_t *out_size);
const void *
wl_compound_arena_lookup_borrowed(
    const wl_compound_arena_borrow_t *borrow, uint64_t handle,
    uint32_t *out_size);

/**
 * wl_compound_arena_multiplicity:
 * @arena:  arena that owns the handle.
 * @handle: handle whose Z-set multiplicity is queried.
 *
 * Return the current Z-set multiplicity, or 0 if the handle is unknown.
 */
int64_t
wl_compound_arena_multiplicity(const wl_compound_arena_t *arena,
    uint64_t handle);

/**
 * wl_compound_arena_retain:
 * @arena:  arena that owns the handle.
 * @handle: handle to retain.
 * @delta:  signed delta to apply to the multiplicity (may be negative).
 *
 * Adjust the Z-set multiplicity for @handle by @delta.  Keeps the live
 * handle counter up-to-date: transitions through zero flip live_handles.
 *
 * Returns 0 on success, -1 if the handle is invalid.
 */
int
wl_compound_arena_retain(wl_compound_arena_t *arena, uint64_t handle,
    int64_t delta);

/**
 * wl_compound_arena_freeze:
 * @arena: arena to freeze.
 *
 * Mark the arena as read-only for the duration of K-Fusion execution.
 * arena_alloc returns 0 (NULL handle) while frozen; lookups are still
 * permitted.  Idempotent: calling freeze on a frozen arena is a no-op.
 */
void
wl_compound_arena_freeze(wl_compound_arena_t *arena);

/**
 * wl_compound_arena_unfreeze:
 * @arena: arena to unfreeze.
 *
 * Clear the read-only guard.  Idempotent.
 */
void
wl_compound_arena_unfreeze(wl_compound_arena_t *arena);

/**
 * wl_compound_arena_gc_epoch_boundary:
 * @arena: arena whose current epoch should be retired.
 *
 * Skeleton epoch-frontier GC (full implementation tracked by #533 close-out):
 *   1. Count entries with multiplicity <= 0 in the current generation and
 *      subtract them from live_handles (they are now reclaimable).
 *   2. Clear entries/multiplicity arrays of the current generation (payload
 *      buffer is retained for reuse on the next allocation cycle).
 *   3. Advance current_epoch by 1.  If the cap is exceeded, further alloc
 *      calls return 0 and the caller is expected to rotate arenas.
 *
 * Freeze-guard contract (Issue #561 / verified in #584):
 *   When arena->frozen is true, this function is a no-op: it neither
 *   sweeps multiplicity nor advances current_epoch.  The return value
 *   on the frozen path is the unchanged @current_epoch, which lets
 *   callers detect "no advance happened" by comparing against a
 *   pre-call snapshot.  It is NOT zero — that would conflict with
 *   epoch zero being a valid generational scope.
 *
 * Returns:
 *   - (uint32_t)-1 if @arena is NULL.
 *   - arena->current_epoch (unchanged) if frozen, held, or the gate is busy.
 *   - the number of handles reclaimed (>= 0) otherwise.
 */
uint32_t
wl_compound_arena_gc_epoch_boundary(wl_compound_arena_t *arena);

/**
 * wl_compound_arena_live_handles:
 * @arena: arena to query (NULL-safe).
 *
 * Return the current live-handle population of @arena (the value of
 * @arena->live_handles), or 0 if @arena is NULL.  Introspection-only
 * accessor used by internal remap sizing paths (Issue #586).
 *
 * Stability: NOT stable across rotations or during live K-Fusion
 * worker access.  Callers must invoke this either while the arena
 * is frozen (so coordinator-side mutation is paused) or before any
 * worker has spawned — see the file-level "Thread safety" note at
 * the top of this header.  `live_handles` is a plain (non-atomic)
 * `uint64_t`; this accessor is TSan-clean iff the caller honors
 * that contract.
 */
static inline uint64_t
wl_compound_arena_live_handles(const wl_compound_arena_t *arena)
{
    return arena ? arena->live_handles : (uint64_t)0;
}

#endif /* WL_COMPOUND_ARENA_H */
