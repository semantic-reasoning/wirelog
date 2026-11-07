/* Private shared intern representation; never installed.
 * Copyright (C) CleverPlant; licensed under LGPL-3.0.
 * Raw-only consumers do not acquire expression-engine dependencies. */
#ifndef WL_INTERN_PRIVATE_H
#define WL_INTERN_PRIVATE_H
#include "intern.h"
#include "thread.h"
#include "columnar/memory_governor.h"

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
    wl_mutex_t lock;

    /* Interned strings are program-owned and may outlive any session. */
    wl_columnar_memory_governor_ref_t *memory_governor;
    wl_columnar_memory_reservation_t reservation;
    uint64_t reserved_bytes;
    uint64_t pins;
    bool owner_released;
};

static inline uint32_t intern_seg_size(uint32_t seg);
static inline char *intern_string_at(const wl_intern_t *intern,
    uint32_t id);

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

#endif
