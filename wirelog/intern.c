/*
 * intern.c - Symbol Intern Table
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Open-addressing hash table (FNV-1a + linear probing) for string
 * interning.  Maps strings to sequential integer IDs and supports
 * reverse lookup by ID.
 *
 * ========================================================================
 * Thread safety (Issue #958)
 * ========================================================================
 *
 * Parallel evaluation shares one table: the per-worker session copies
 * borrow the coordinator's `intern` pointer, so every worker family
 * (non-recursive, TDD, K-fusion) calls into this file concurrently.
 * String operations (contains/strlen/substr/to_number, and cat() in a
 * head position) reverse-map once or twice per row and intern their
 * results, so both directions are hot and concurrent.
 *
 * The split is therefore:
 *
 *   - wl_intern_put() and wl_intern_get() serialize on a writer mutex.
 *     They are the only functions that touch the slot table, so the
 *     table can still be grown and freed in place.
 *
 *   - wl_intern_reverse() and wl_intern_count() are lock-free.  A
 *     global lock here is not shippable: reverse is called per row, and
 *     locking it makes an 8-worker run several times slower than a
 *     1-worker run.
 *
 * Two properties make the lock-free readers safe:
 *
 *   1. The id -> string storage is segmented and never moves.  A
 *      segment is allocated once and is neither reallocated nor freed
 *      until the table is destroyed, so a pointer a reader has loaded
 *      stays valid.  (The previous realloc()-doubled array was the
 *      crash driver: a writer could relocate the array out from under
 *      a reader mid-dereference.)
 *
 *   2. `count` is published with a release store *after* the entry is
 *      fully written, and read with an acquire load.  A reader that
 *      sees id < count therefore also sees the segment pointer and the
 *      string bytes it names.
 *
 * wl_intern_create() and wl_intern_free() are not concurrent with
 * anything: the table is created before workers start and destroyed
 * after they join.
 */

#include "intern.h"

#include "thread.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* C11 atomics for the lock-free reader path.  Mirrors the MSVC split
 * already carried by columnar/mem_ledger.h. */
/* *INDENT-OFF* */
#ifdef _MSC_VER
#include <intrin.h>

/* MSVC does not support C11 _Atomic as a type qualifier or keyword.
 * Use volatile uint32_t as the atomic field type: under MSVC's
 * /volatile:ms model a volatile load/store carries acquire/release
 * semantics on x86 and x64.  MSVC defaults to /volatile:iso on ARM64,
 * where it does not; that path is unexercised here -- the Windows CI job
 * is MinGW GCC and takes the <stdatomic.h> branch.  This
 * is the same assumption columnar/mem_ledger.h makes.  MinGW GCC is not
 * _MSC_VER and takes the <stdatomic.h> branch below. */
typedef volatile uint32_t wl_intern_atomic_u32;
#define WL_INTERN_LOAD_ACQUIRE(p) (*(p))
#define WL_INTERN_LOAD_RELAXED(p) (*(p))
#define WL_INTERN_STORE_RELEASE(p, v) (*(p) = (uint32_t)(v))
#else
#include <stdatomic.h>
typedef _Atomic uint32_t wl_intern_atomic_u32;
#define WL_INTERN_LOAD_ACQUIRE(p) \
        atomic_load_explicit((p), memory_order_acquire)
#define WL_INTERN_LOAD_RELAXED(p) \
        atomic_load_explicit((p), memory_order_relaxed)
#define WL_INTERN_STORE_RELEASE(p, v) \
        atomic_store_explicit((p), (uint32_t)(v), memory_order_release)
#endif
/* *INDENT-ON* */

/* ======================================================================== */
/* Constants                                                                */
/* ======================================================================== */

#define INTERN_INITIAL_CAP 64
#define INTERN_LOAD_FACTOR_NUM 3 /* 75% load factor = 3/4 */
#define INTERN_LOAD_FACTOR_DEN 4

/* Sentinel: empty hash table slot */
#define SLOT_EMPTY UINT32_MAX

/*
 * Segmented id -> string storage.
 *
 * Segment s holds the ids [64 * (2^s - 1), 64 * (2^(s+1) - 1)), that is
 * 64 << s entries, and is allocated on first use.  Growth adds a new
 * segment instead of reallocating, so existing entries never move.  The
 * segment directory itself is a fixed-size array inside the table, so
 * it never moves either.
 *
 * 26 segments span the whole uint32 id space up to INTERN_MAX_STRINGS.
 * The doubling schedule keeps the directory tiny and the per-lookup
 * arithmetic to a shift, an add and a bit scan.
 */
#define INTERN_SEG0_SHIFT 6
#define INTERN_SEG0 (1u << INTERN_SEG0_SHIFT) /* first segment: 64 entries */
#define INTERN_MAX_SEGMENTS 26
#define INTERN_MAX_STRINGS (UINT32_MAX - INTERN_SEG0 + 1u)

/* ======================================================================== */
/* Hash Table Entry                                                         */
/* ======================================================================== */

typedef struct {
    uint32_t string_id; /* index into the segments, or SLOT_EMPTY */
} wl_intern_slot_t;

/* ======================================================================== */
/* Intern Table Structure                                                   */
/* ======================================================================== */

struct wl_intern {
    /* Reverse mapping: id -> string (owned copies), never relocated.
     * Read without the lock; see the header comment. */
    char **segments[INTERN_MAX_SEGMENTS];

    /* Number of published entries.  Release-stored by the writer once
     * the entry is complete; acquire-loaded by lock-free readers. */
    wl_intern_atomic_u32 count;

    /* Hash table: string -> id (open addressing).  Only ever touched
     * with @lock held, which is what makes growing it (and freeing the
     * old allocation) safe. */
    wl_intern_slot_t *slots;
    uint32_t slot_capacity;

    /* Writers only.  Readers (reverse/count) never take it. */
    mutex_t lock;
};

/* ======================================================================== */
/* FNV-1a Hash                                                              */
/* ======================================================================== */

static uint32_t
fnv1a(const char *str)
{
    uint32_t hash = 2166136261u;
    for (const unsigned char *p = (const unsigned char *)str; *p; p++) {
        hash ^= *p;
        hash *= 16777619u;
    }
    return hash;
}

/* ======================================================================== */
/* Segment Addressing                                                       */
/* ======================================================================== */

/* floor(log2(v)) for v >= 1. */
static inline uint32_t
intern_ilog2(uint32_t v)
{
    /* *INDENT-OFF* */
#if defined(__GNUC__) || defined(__clang__)
    return 31u - (uint32_t)__builtin_clz(v);
#elif defined(_MSC_VER)
    unsigned long idx;
    _BitScanReverse(&idx, (unsigned long)v);
    return (uint32_t)idx;
#else
    uint32_t r = 0;
    while (v > 1u) {
        v >>= 1;
        r++;
    }
    return r;
#endif
    /* *INDENT-ON* */
}

/* Segment holding @id. */
static inline uint32_t
intern_seg_of(uint32_t id)
{
    return intern_ilog2((id >> INTERN_SEG0_SHIFT) + 1u);
}

/* First id stored in segment @seg. */
static inline uint32_t
intern_seg_base(uint32_t seg)
{
    return (INTERN_SEG0 << seg) - INTERN_SEG0;
}

/* Number of entries in segment @seg. */
static inline uint32_t
intern_seg_size(uint32_t seg)
{
    return INTERN_SEG0 << seg;
}

/* Entry for @id.  @id must be below the published count (readers) or
 * below the writer's own count (writers). */
static inline char *
intern_string_at(const wl_intern_t *intern, uint32_t id)
{
    uint32_t seg = intern_seg_of(id);

    return intern->segments[seg][id - intern_seg_base(seg)];
}

/* Make sure the segment holding @id exists.  Caller holds @lock. */
static int
intern_seg_ensure(wl_intern_t *intern, uint32_t id)
{
    uint32_t seg = intern_seg_of(id);

    if (seg >= INTERN_MAX_SEGMENTS)
        return -1;
    if (intern->segments[seg])
        return 0;

    char **block = (char **)calloc((size_t)intern_seg_size(seg),
            sizeof(char *));
    if (!block)
        return -1;

    intern->segments[seg] = block;
    return 0;
}

/* ======================================================================== */
/* Internal Helpers                                                         */
/* ======================================================================== */

/*
 * Double the slot table and re-insert every published entry.
 *
 * Caller holds @lock.  wl_intern_put() and wl_intern_get() are the only
 * readers of intern->slots and both hold the lock, so the old table can
 * be freed here.
 */
static int
intern_resize(wl_intern_t *intern)
{
    uint32_t count = WL_INTERN_LOAD_RELAXED(&intern->count);

    if (intern->slot_capacity > UINT32_MAX / 2U)
        return -1;
    uint32_t new_cap = intern->slot_capacity * 2U;
    wl_intern_slot_t *new_slots
        = (wl_intern_slot_t *)malloc((size_t)new_cap
            * sizeof(wl_intern_slot_t));
    if (!new_slots)
        return -1;

    memset(new_slots, 0xff, (size_t)new_cap * sizeof(wl_intern_slot_t));

    /* Re-insert all existing entries */
    for (uint32_t i = 0; i < count; i++) {
        uint32_t h = fnv1a(intern_string_at(intern, i)) & (new_cap - 1);
        while (new_slots[h].string_id != SLOT_EMPTY)
            h = (h + 1) & (new_cap - 1);
        new_slots[h].string_id = i;
    }

    free(intern->slots);
    intern->slots = new_slots;
    intern->slot_capacity = new_cap;
    return 0;
}

/*
 * Probe the slot table for @str.  Caller holds @lock.
 *
 * Returns the id if present.  Otherwise returns -1 and, when @out_slot
 * is non-NULL, stores the index of the empty slot the probe stopped on.
 */
static int64_t
intern_lookup_locked(const wl_intern_t *intern, const char *str,
    uint32_t *out_slot)
{
    uint32_t mask = intern->slot_capacity - 1;
    uint32_t h = fnv1a(str) & mask;

    while (intern->slots[h].string_id != SLOT_EMPTY) {
        uint32_t sid = intern->slots[h].string_id;
        if (strcmp(intern_string_at(intern, sid), str) == 0)
            return (int64_t)sid;
        h = (h + 1) & mask;
    }

    if (out_slot)
        *out_slot = h;
    return -1;
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

wl_intern_t *
wl_intern_create(void)
{
    wl_intern_t *intern = (wl_intern_t *)calloc(1, sizeof(wl_intern_t));
    if (!intern)
        return NULL;

    if (mutex_init(&intern->lock) != 0) {
        free(intern);
        return NULL;
    }

    WL_INTERN_STORE_RELEASE(&intern->count, 0u);

    intern->slot_capacity = INTERN_INITIAL_CAP;
    intern->slots = (wl_intern_slot_t *)malloc(intern->slot_capacity
            * sizeof(wl_intern_slot_t));
    if (!intern->slots) {
        mutex_destroy(&intern->lock);
        free(intern);
        return NULL;
    }

    for (uint32_t i = 0; i < intern->slot_capacity; i++)
        intern->slots[i].string_id = SLOT_EMPTY;

    return intern;
}

void
wl_intern_free(wl_intern_t *intern)
{
    if (!intern)
        return;

    uint32_t count = WL_INTERN_LOAD_RELAXED(&intern->count);
    for (uint32_t i = 0; i < count; i++)
        free(intern_string_at(intern, i));

    for (uint32_t seg = 0; seg < INTERN_MAX_SEGMENTS; seg++)
        free((void *)intern->segments[seg]);

    free(intern->slots);
    mutex_destroy(&intern->lock);
    free(intern);
}

int64_t
wl_intern_put(wl_intern_t *intern, const char *str)
{
    if (!intern || !str)
        return -1;

    mutex_lock(&intern->lock);

    /* Check if already interned */
    uint32_t h = 0;
    int64_t existing = intern_lookup_locked(intern, str, &h);
    if (existing >= 0) {
        mutex_unlock(&intern->lock);
        return existing;
    }

    /* New string: make room for it in the segmented storage. */
    uint32_t new_id = WL_INTERN_LOAD_RELAXED(&intern->count);
    if (new_id >= INTERN_MAX_STRINGS
        || intern_seg_ensure(intern, new_id) != 0) {
        mutex_unlock(&intern->lock);
        return -1;
    }

    size_t slen = strlen(str);
    char *copy = (char *)malloc(slen + 1);
    if (!copy) {
        mutex_unlock(&intern->lock);
        return -1;
    }
    memcpy(copy, str, slen + 1);

    /* Resize hash table before insertion if the new entry would exceed the load factor. */
    if ((uint64_t)(new_id + 1U) * INTERN_LOAD_FACTOR_DEN
        > (uint64_t)intern->slot_capacity * INTERN_LOAD_FACTOR_NUM) {
        if (intern_resize(intern) != 0) {
            free(copy);
            mutex_unlock(&intern->lock);
            return -1;
        }
        uint32_t mask = intern->slot_capacity - 1U;
        h = fnv1a(str) & mask;
        while (intern->slots[h].string_id != SLOT_EMPTY)
            h = (h + 1U) & mask;
    }

    uint32_t seg = intern_seg_of(new_id);
    intern->segments[seg][new_id - intern_seg_base(seg)] = copy;

    /* Release store: any reader that acquire-loads a count greater than
     * new_id also sees the segment pointer and the string above.  The
     * entry must be complete before the count is published, never the
     * other way round. */
    /* Release is sufficient only because EVERY store to count happens
     * under intern->lock: another writer's segment and slot writes are
     * ordered before its unlock, which is ordered before our lock and
     * therefore before this store.  An unlocked count store would break
     * the transitivity a lock-free reverse() depends on. */
    WL_INTERN_STORE_RELEASE(&intern->count, new_id + 1U);

    intern->slots[h].string_id = new_id;

    mutex_unlock(&intern->lock);
    return (int64_t)new_id;
}

int64_t
wl_intern_get(const wl_intern_t *intern, const char *str)
{
    if (!intern || !str)
        return -1;

    /* Reading the slot table requires the lock, because a concurrent
     * put() may be growing it.  The table is heap-allocated, so casting
     * away const to take the lock is well defined. */
    wl_intern_t *mutable_intern = (wl_intern_t *)intern;

    mutex_lock(&mutable_intern->lock);
    int64_t id = intern_lookup_locked(intern, str, NULL);
    mutex_unlock(&mutable_intern->lock);

    return id;
}

const char *
wl_intern_reverse(const wl_intern_t *intern, int64_t id)
{
    if (!intern || id < 0)
        return NULL;

    /* Acquire load: pairs with the release store in wl_intern_put(), so
     * an id below the observed count names a fully written entry. */
    uint32_t count = WL_INTERN_LOAD_ACQUIRE(&intern->count);
    if (id > (int64_t)UINT32_MAX || (uint32_t)id >= count)
        return NULL;

    return intern_string_at(intern, (uint32_t)id);
}

uint32_t
wl_intern_count(const wl_intern_t *intern)
{
    if (!intern)
        return 0;

    return WL_INTERN_LOAD_ACQUIRE(&intern->count);
}
