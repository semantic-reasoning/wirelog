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
 *
 * The per-put cost of the writer lock and of the governor admission
 * (Issue #1431) is measured by bench/bench_intern.c; the baseline numbers
 * live in docs/INTERN_PERF.md (Issue #1472).  Re-run the bench before
 * moving work into or out of the locked region.
 */

#include "intern.h"

#include "columnar/memory_governor.h"

#include "thread.h"

#include <stdint.h>
#include <errno.h>
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
    wl_mutex_t lock;

    /* Interned strings are program-owned and may outlive any session. */
    wl_columnar_memory_governor_ref_t *memory_governor;
    wl_columnar_memory_reservation_t reservation;
    uint64_t reserved_bytes;
};

static inline uint32_t intern_seg_size(uint32_t seg);
static inline char *intern_string_at(const wl_intern_t *intern,
    uint32_t id);

static uint64_t
intern_retained_bytes_locked(const wl_intern_t *intern)
{
    uint64_t total = (uint64_t)intern->slot_capacity
        * sizeof(wl_intern_slot_t);
    uint32_t count = WL_INTERN_LOAD_RELAXED(&intern->count);

    for (uint32_t seg = 0; seg < INTERN_MAX_SEGMENTS; seg++) {
        if (intern->segments[seg]) {
            uint64_t bytes = (uint64_t)intern_seg_size(seg)
                * sizeof(char *);
            if (UINT64_MAX - total < bytes)
                return UINT64_MAX;
            total += bytes;
        }
    }
    for (uint32_t i = 0; i < count; i++) {
        const char *str = intern_string_at(intern, i);
        size_t len = strlen(str);
        if (len == SIZE_MAX)
            return UINT64_MAX;
        len++;
        if ((uint64_t)len > UINT64_MAX - total)
            return UINT64_MAX;
        total += (uint64_t)len;
    }
    return total;
}

static int
intern_publish_reservation(wl_intern_t *intern,
    wl_columnar_memory_reservation_t *pending, uint64_t bytes)
{
    wl_columnar_memory_reservation_t previous;
    if (!intern || !pending || !intern->memory_governor)
        return EINVAL;
    if (!wl_columnar_memory_commit(pending, intern))
        return ENOMEM;
    wl_columnar_memory_reservation_init(&previous);
    if (intern->reserved_bytes > 0
        && !wl_columnar_memory_reservation_move(
            &previous, &intern->reservation)) {
        (void)wl_columnar_memory_release(pending);
        return ENOMEM;
    }
    if (!wl_columnar_memory_reservation_move(
            &intern->reservation, pending)) {
        if (intern->reserved_bytes > 0) {
            wl_columnar_memory_reservation_init(&intern->reservation);
            (void)wl_columnar_memory_reservation_move(
                &intern->reservation, &previous);
        }
        (void)wl_columnar_memory_release(pending);
        return ENOMEM;
    }
    if (intern->reserved_bytes > 0)
        (void)wl_columnar_memory_release(&previous);
    intern->reserved_bytes = bytes;
    return 0;
}

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
intern_resize_prepare(const wl_intern_t *intern,
    wl_intern_slot_t **out_slots, uint32_t *out_capacity)
{
    uint32_t count;
    wl_intern_slot_t *new_slots;
    uint32_t new_cap;

    if (!intern || !out_slots || !out_capacity
        || intern->slot_capacity > UINT32_MAX / 2U)
        return -1;
    count = WL_INTERN_LOAD_RELAXED(&intern->count);
    new_cap = intern->slot_capacity * 2U;
    new_slots
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

    *out_slots = new_slots;
    *out_capacity = new_cap;
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

    if (wl_mutex_init(&intern->lock) != 0) {
        free(intern);
        return NULL;
    }

    wl_columnar_memory_reservation_init(&intern->reservation);

    WL_INTERN_STORE_RELEASE(&intern->count, 0u);

    intern->slot_capacity = INTERN_INITIAL_CAP;
    intern->slots = (wl_intern_slot_t *)malloc(intern->slot_capacity
            * sizeof(wl_intern_slot_t));
    if (!intern->slots) {
        wl_mutex_destroy(&intern->lock);
        free(intern);
        return NULL;
    }

    for (uint32_t i = 0; i < intern->slot_capacity; i++)
        intern->slots[i].string_id = SLOT_EMPTY;

    return intern;
}

int
wl_intern_attach_memory_governor(
    wl_intern_t *intern,
    wl_columnar_memory_governor_ref_t *governor_ref)
{
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_admission_status_t status;
    wl_columnar_memory_governor_t *governor;
    uint64_t bytes;

    if (!intern || !governor_ref)
        return EINVAL;

    wl_mutex_lock(&intern->lock);
    if (intern->memory_governor == governor_ref) {
        wl_mutex_unlock(&intern->lock);
        return EALREADY;
    }
    /* Issue #1469: an owner that no live session, worker or result holds
     * any more (the table is its only reference) is orphaned; rebind the
     * table to the caller's governor.  A held owner stays in charge. */
    bool rebind = intern->memory_governor != NULL;
    if (rebind
        && !wl_columnar_memory_governor_ref_is_sole(intern->memory_governor)) {
        wl_mutex_unlock(&intern->lock);
        return EBUSY;
    }

    governor = wl_columnar_memory_governor_ref_get(governor_ref);
    if (rebind) {
        /* The retained footprint is exact by the #1431 invariant; an
         * attached table never holds zero reserved bytes. */
        if (intern->reserved_bytes == 0) {
            wl_mutex_unlock(&intern->lock);
            return EINVAL;
        }
        bytes = intern->reserved_bytes;
    } else {
        bytes = intern_retained_bytes_locked(intern);
    }
    if (!governor || bytes == UINT64_MAX) {
        wl_mutex_unlock(&intern->lock);
        return ENOMEM;
    }
    wl_columnar_memory_reservation_init(&pending);
    status = wl_columnar_memory_reserve_checked(governor, bytes, &pending);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        wl_mutex_unlock(&intern->lock);
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
            ? WL_INTERN_ERR_MEMORY_BUDGET : EOVERFLOW;
    }
    if (rebind) {
        /* Admit under the new governor first: publish commits @pending,
         * installs it, and credits the orphaned owner's reservation (the
         * owner is still alive because the table holds its reference).
         * Any failure leaves the old token installed and releases the
         * new one; the rollback below is a no-op when publish has
         * already released the token. */
        int rc = intern_publish_reservation(intern, &pending, bytes);
        if (rc != 0) {
            (void)wl_columnar_memory_rollback(&pending);
            wl_mutex_unlock(&intern->lock);
            return rc;
        }
        wl_columnar_memory_governor_ref_t *old = intern->memory_governor;
        wl_columnar_memory_governor_ref_retain(governor_ref);
        intern->memory_governor = governor_ref;
        /* Last touch of the orphaned owner: this drops its final
         * reference and frees it. */
        wl_columnar_memory_governor_ref_release(old);
        wl_mutex_unlock(&intern->lock);
        return 0;
    }
    if (!wl_columnar_memory_commit(&pending, intern)) {
        (void)wl_columnar_memory_release(&pending);
        wl_mutex_unlock(&intern->lock);
        return ENOMEM;
    }
    wl_columnar_memory_governor_ref_retain(governor_ref);
    intern->memory_governor = governor_ref;
    (void)wl_columnar_memory_reservation_move(
        &intern->reservation, &pending);
    intern->reserved_bytes = bytes;
    wl_mutex_unlock(&intern->lock);
    return 0;
}

int
wl_intern_detach_memory_governor(
    wl_intern_t *intern,
    wl_columnar_memory_governor_ref_t *governor_ref)
{
    if (!intern || !governor_ref)
        return EINVAL;
    wl_mutex_lock(&intern->lock);
    if (!intern->memory_governor) {
        wl_mutex_unlock(&intern->lock);
        return 0;
    }
    if (intern->memory_governor != governor_ref) {
        wl_mutex_unlock(&intern->lock);
        return EBUSY;
    }
    if (intern->reserved_bytes > 0)
        (void)wl_columnar_memory_release(&intern->reservation);
    intern->reserved_bytes = 0;
    intern->memory_governor = NULL;
    wl_columnar_memory_governor_ref_release(governor_ref);
    wl_mutex_unlock(&intern->lock);
    return 0;
}

void
wl_intern_free(wl_intern_t *intern)
{
    if (!intern)
        return;

    uint32_t count;

    wl_mutex_lock(&intern->lock);
    count = WL_INTERN_LOAD_RELAXED(&intern->count);
    if (intern->reserved_bytes > 0)
        (void)wl_columnar_memory_release(&intern->reservation);
    intern->reserved_bytes = 0;
    wl_columnar_memory_governor_ref_release(intern->memory_governor);
    intern->memory_governor = NULL;
    wl_mutex_unlock(&intern->lock);
    for (uint32_t i = 0; i < count; i++)
        free(intern_string_at(intern, i));

    for (uint32_t seg = 0; seg < INTERN_MAX_SEGMENTS; seg++)
        free((void *)intern->segments[seg]);

    free(intern->slots);
    wl_mutex_destroy(&intern->lock);
    free(intern);
}

int64_t
wl_intern_put(wl_intern_t *intern, const char *str)
{
    int64_t id = -1;
    return wl_intern_put_checked(intern, str, &id) == 0 ? id : -1;
}

int
wl_intern_put_checked(wl_intern_t *intern, const char *str, int64_t *out_id)
{
    wl_columnar_memory_reservation_t pending;
    bool pending_valid = false;
    char *copy;
    char **new_segment = NULL;
    wl_intern_slot_t *new_slots = NULL;
    uint32_t h = 0;
    uint32_t new_id;
    uint32_t seg;
    uint32_t new_cap;
    bool needs_segment;
    bool needs_resize;
    uint64_t retained_after = 0;
    size_t slen;
    size_t copy_bytes;
    int64_t existing;
    int failure = ENOMEM;

    if (out_id)
        *out_id = -1;
    if (!intern || !str || !out_id)
        return EINVAL;

    /* Prepare the owned copy before taking the writer lock.  strlen/malloc/
     * memcpy are independent of the table and used to make every worker wait
     * behind the lock while doing allocation and copying (#961).  A duplicate
     * discovered below simply releases this speculative copy; its bytes are
     * never charged to the governor (Issue #1431).  Segment and slot arrays
     * are still allocated under the lock: they grow geometrically, so their
     * cost is amortized over 64 * 2^k and 2x puts respectively. */
    slen = strlen(str);
    if (slen == SIZE_MAX)
        return EOVERFLOW;
    copy_bytes = slen + 1;
    copy = (char *)malloc(copy_bytes);
    if (!copy)
        return ENOMEM;
    memcpy(copy, str, copy_bytes);

    wl_mutex_lock(&intern->lock);
    existing = intern_lookup_locked(intern, str, &h);
    if (existing >= 0) {
        wl_mutex_unlock(&intern->lock);
        free(copy);
        *out_id = existing;
        return 0;
    }
    new_id = WL_INTERN_LOAD_RELAXED(&intern->count);
    if (new_id >= INTERN_MAX_STRINGS) {
        failure = EOVERFLOW;
        goto fail;
    }
    seg = intern_seg_of(new_id);
    needs_segment = intern->segments[seg] == NULL;
    needs_resize = (uint64_t)(new_id + 1U) * INTERN_LOAD_FACTOR_DEN
        > (uint64_t)intern->slot_capacity * INTERN_LOAD_FACTOR_NUM;
    if (needs_resize && intern->slot_capacity > UINT32_MAX / 2U) {
        failure = EOVERFLOW;
        goto fail;
    }
    new_cap = needs_resize ? intern->slot_capacity * 2U
                           : intern->slot_capacity;

    if (intern->memory_governor) {
        /* Retained footprint after this put: every byte the table holds now
         * except the slot array a resize frees, plus the copy, a fresh
         * segment when @new_id opens one, and the doubled slot array.
         * intern->reserved_bytes is exact (attach scans, every publish
         * stores the new total), so no rescan is needed here.  Growth is
         * reserved while the old footprint is still held, so exact fit
         * means old + new <= budget, the same convention as relation.c. */
        uint64_t old_bytes = intern->reserved_bytes;
        uint64_t freed_slot_bytes = 0;
        uint64_t segment_bytes = 0;
        uint64_t slot_bytes = 0;
        wl_columnar_memory_admission_status_t status;

        if (needs_resize
            && (!wl_columnar_memory_size_mul(intern->slot_capacity,
            sizeof(wl_intern_slot_t), &freed_slot_bytes)
            || !wl_columnar_memory_size_mul(new_cap,
            sizeof(wl_intern_slot_t), &slot_bytes))) {
            failure = EOVERFLOW;
            goto fail;
        }
        if (needs_segment
            && !wl_columnar_memory_size_mul(intern_seg_size(seg),
            sizeof(char *), &segment_bytes)) {
            failure = EOVERFLOW;
            goto fail;
        }
        if (freed_slot_bytes > old_bytes) {
            failure = EINVAL;
            goto fail;
        }
        retained_after = old_bytes - freed_slot_bytes;
        if (!wl_columnar_memory_size_add(retained_after, copy_bytes,
            &retained_after)
            || !wl_columnar_memory_size_add(retained_after, segment_bytes,
            &retained_after)
            || !wl_columnar_memory_size_add(retained_after, slot_bytes,
            &retained_after)) {
            failure = EOVERFLOW;
            goto fail;
        }
        wl_columnar_memory_reservation_init(&pending);
        status = wl_columnar_memory_reserve_growth(
            wl_columnar_memory_governor_ref_get(intern->memory_governor),
            old_bytes, retained_after, &pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            failure = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? WL_INTERN_ERR_MEMORY_BUDGET
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
            goto fail;
        }
        pending_valid = true;
    }

    if (needs_segment) {
        new_segment = (char **)calloc((size_t)intern_seg_size(seg),
                sizeof(char *));
        if (!new_segment)
            goto fail;
    }
    if (needs_resize
        && intern_resize_prepare(intern, &new_slots, &new_cap) != 0)
        goto fail;
    if (pending_valid
        && intern_publish_reservation(intern, &pending, retained_after) != 0)
        goto fail;

    /* Nothing below can fail: the reservation now belongs to the table. */
    if (new_slots) {
        free(intern->slots);
        intern->slots = new_slots;
        intern->slot_capacity = new_cap;
    }
    if (needs_resize) {
        h = fnv1a(str) & (intern->slot_capacity - 1U);
        while (intern->slots[h].string_id != SLOT_EMPTY)
            h = (h + 1U) & (intern->slot_capacity - 1U);
    }
    if (new_segment)
        intern->segments[seg] = new_segment;
    intern->segments[seg][new_id - intern_seg_base(seg)] = copy;
    intern->slots[h].string_id = new_id;

    /* Release store: any reader that acquire-loads a count greater than
     * new_id also sees the segment pointer and the string above.  The
     * entry must be complete before the count is published, never the
     * other way round.  Release is sufficient only because EVERY store to
     * count happens under intern->lock. */
    WL_INTERN_STORE_RELEASE(&intern->count, new_id + 1U);
    wl_mutex_unlock(&intern->lock);
    *out_id = (int64_t)new_id;
    return 0;

fail:
    free(copy);
    free((void *)new_segment);
    free(new_slots);
    if (pending_valid && !wl_columnar_memory_rollback(&pending))
        abort(); /* A live local token has no valid owner after this point. */
    wl_mutex_unlock(&intern->lock);
    return failure;
}

/* Controlled publication keeps the existing table governor under its actual
 * writer lock. All engine loops poll; mutex waiting is a platform primitive. */
#ifdef WL_SESSION_TEST_HOOKS
void (*wl_intern_test_controlled_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t);
int wl_intern_test_fail_allocation = -1;
bool wl_intern_test_fail_commit;
#endif
static int
wl_intern_controlled_step(wl_evaluation_control_t *control, unsigned stage,
    uint64_t progress)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_intern_test_controlled_checkpoint)
        wl_intern_test_controlled_checkpoint(control, stage, progress);
#else
    (void)stage; (void)progress;
#endif
    return wl_evaluation_control_charge(control, 0);
}
static void *
wl_intern_controlled_malloc(size_t bytes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_intern_test_fail_allocation >= 0
        && wl_intern_test_fail_allocation-- == 0) {
        wl_intern_test_fail_allocation = -1;
        return NULL;
    }
#endif
    return malloc(bytes);
}
static int
wl_intern_controlled_hash(const char *str, uint32_t *out,
    wl_evaluation_control_t *control, unsigned stage)
{
    uint32_t hash = 2166136261u;
    for (size_t i = 0;; i++) {
        int rc = wl_evaluation_control_charge(control, 4);
        if (rc) return rc;
        unsigned char byte = (unsigned char)str[i];
        if (byte) {
            hash ^= byte; hash *= 16777619u;
        }
        rc = wl_intern_controlled_step(control, stage, i + 1);
        if (rc) return rc;
        if (!byte) {
            *out = hash; return 0;
        }
    }
}
static int
wl_intern_controlled_equal(const char *a, const char *b, bool *equal,
    wl_evaluation_control_t *control)
{
    for (size_t i = 0;; i++) {
        int rc = wl_evaluation_control_charge(control, 3);
        if (rc) return rc;
        unsigned char av = (unsigned char)a[i], bv = (unsigned char)b[i];
        rc = wl_intern_controlled_step(control, 4, i + 1);
        if (rc) return rc;
        if (av != bv || !av) {
            *equal = av == bv; return 0;
        }
    }
}
static int
wl_intern_controlled_admit(wl_columnar_memory_governor_t *governor,
    uint64_t old, uint64_t bytes, bool growth,
    wl_columnar_memory_reservation_t *token, int *status, bool *denied,
    wl_evaluation_control_t *control)
{
    wl_columnar_memory_admission_status_t admission;
    int rc = growth ? wl_columnar_memory_governor_reserve_growth_controlled(
        governor, old, bytes, token, &admission, control)
        : wl_columnar_memory_governor_reserve_checked_controlled(governor,
            bytes, token, &admission, control);
    if (rc) return rc;
    if (admission == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || admission == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) return 0;
    *denied = admission == WL_COLUMNAR_MEMORY_ADMISSION_DENIED;
    *status = *denied ? WL_INTERN_ERR_MEMORY_BUDGET :
        admission == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
    return 0;
}
int
wl_intern_put_controlled(wl_intern_t *intern, const char *str, int64_t *out_id,
    int *status, bool *budget_denied, wl_evaluation_control_t *control)
{
    if (!intern || !str || !out_id || !status || !budget_denied) return EINVAL;
    if (!control) {
        *status = wl_intern_put_checked(intern, str, out_id);
        *budget_denied = *status == WL_INTERN_ERR_MEMORY_BUDGET;
        return 0;
    }
    int rc = wl_evaluation_control_charge(control, 48), failure = 0;
    if (rc) return rc;
    bool denied = false, scratch_live = false, pending_live = false;
    bool pending_committed = false;
    char *copy = NULL; char **segment = NULL;
    wl_intern_slot_t *slots = NULL;
    wl_columnar_memory_reservation_t scratch, pending, retired;
    wl_columnar_memory_reservation_init(&scratch);
    wl_columnar_memory_reservation_init(&pending);
    wl_columnar_memory_reservation_init(&retired);
    wl_mutex_lock(&intern->lock);
    wl_columnar_memory_governor_t *governor = intern->memory_governor
        ? wl_columnar_memory_governor_ref_get(intern->memory_governor) : NULL;
    size_t length = 0;
    for (;;) {
        rc = wl_evaluation_control_charge(control, 2);
        if (rc) goto fail;
        unsigned char byte = (unsigned char)str[length];
        rc = wl_intern_controlled_step(control, 1, length + 1);
        if (rc) goto fail;
        if (!byte) break;
        if (length == SIZE_MAX - 1) {
            rc = EOVERFLOW; goto fail;
        }
        length++;
    }
    size_t bytes = length + 1;
    if (governor) {
        rc = wl_intern_controlled_admit(governor, 0, bytes, false, &scratch,
                &failure, &denied, control);
        if (rc || failure) goto fail;
        scratch_live = true;
    }
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) goto fail;
    copy = wl_intern_controlled_malloc(bytes);
    if (!copy) {
        failure = ENOMEM; goto fail;
    }
    rc = wl_intern_controlled_step(control, 2, 0);
    if (rc) goto fail;
    for (size_t i = 0; i < bytes; i++) {
        rc = wl_evaluation_control_charge(control, 2);
        if (rc) goto fail;
        copy[i] = str[i];
        rc = wl_intern_controlled_step(control, 2, i + 1);
        if (rc) goto fail;
    }
    uint32_t hash;
    rc = wl_intern_controlled_hash(copy, &hash, control, 3);
    if (rc) goto fail;
    uint32_t h = hash & (intern->slot_capacity - 1);
    int64_t result = -1;
    for (;;) {
        rc = wl_evaluation_control_charge(control, 20);
        if (rc) goto fail;
        uint32_t id = intern->slots[h].string_id;
        rc = wl_intern_controlled_step(control, 5, h);
        if (rc) goto fail;
        if (id == SLOT_EMPTY) break;
        bool equal;
        rc = wl_intern_controlled_equal(intern_string_at(intern, id), copy,
                &equal, control);
        if (rc) goto fail;
        if (equal) {
            result = id; break;
        }
        h = (h + 1) & (intern->slot_capacity - 1);
    }
    if (result >= 0) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) goto fail;
        rc = wl_intern_controlled_step(control, 9, 0);
        if (rc) goto fail;
        free(copy); copy = NULL;
        if (scratch_live && !wl_columnar_memory_rollback(&scratch)) abort();
        wl_mutex_unlock(&intern->lock);
        *out_id = result; *status = 0; *budget_denied = false;
        return 0;
    }
    rc = wl_evaluation_control_charge(control, 48);
    if (rc) goto fail;
    uint32_t id = WL_INTERN_LOAD_RELAXED(&intern->count);
    if (id >= INTERN_MAX_STRINGS) {
        failure = EOVERFLOW; goto fail;
    }
    uint32_t seg = intern_seg_of(id);
    bool need_segment = !intern->segments[seg];
    bool resize = (uint64_t)(id + 1) * INTERN_LOAD_FACTOR_DEN
        > (uint64_t)intern->slot_capacity * INTERN_LOAD_FACTOR_NUM;
    if (resize && intern->slot_capacity > UINT32_MAX / 2) {
        rc = EOVERFLOW; goto fail;
    }
    uint32_t capacity = resize ? intern->slot_capacity *
        2 : intern->slot_capacity;
    uint64_t segment_bytes = need_segment
        ? (uint64_t)intern_seg_size(seg) * sizeof(char *) : 0;
    uint64_t slot_bytes = resize ? (uint64_t)capacity * sizeof(*slots) : 0;
    uint64_t old_slots = resize ? (uint64_t)intern->slot_capacity *
        sizeof(*slots) : 0;
    uint64_t retained = intern->reserved_bytes;
    if (segment_bytes > SIZE_MAX || slot_bytes > SIZE_MAX) {
        rc = EOVERFLOW; goto fail;
    }
    if (governor) {
        if (old_slots > retained
            || !wl_columnar_memory_size_add(retained - old_slots, bytes,
            &retained)
            || !wl_columnar_memory_size_add(retained, segment_bytes, &retained)
            || !wl_columnar_memory_size_add(retained, slot_bytes, &retained)) {
            rc = EOVERFLOW; goto fail;
        }
    }
    rc = wl_intern_controlled_step(control, 6, 0);
    if (rc) goto fail;
    if (governor) {
        rc = wl_intern_controlled_admit(governor, intern->reserved_bytes,
                retained, true, &pending, &failure, &denied, control);
        if (rc || failure) goto fail;
        pending_live = true;
    }
    if (need_segment) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc) goto fail;
        segment = wl_intern_controlled_malloc((size_t)segment_bytes);
        if (!segment) {
            failure = ENOMEM; goto fail;
        }
        for (uint32_t i = 0; i < intern_seg_size(seg); i++) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc) goto fail;
            segment[i] = NULL;
            rc = wl_intern_controlled_step(control, 7, i + 1);
            if (rc) goto fail;
        }
    }
    if (resize) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc) goto fail;
        slots = wl_intern_controlled_malloc((size_t)slot_bytes);
        if (!slots) {
            failure = ENOMEM; goto fail;
        }
        for (uint32_t i = 0; i < capacity; i++) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc) goto fail;
            slots[i].string_id = SLOT_EMPTY;
            rc = wl_intern_controlled_step(control, 8, i + 1);
            if (rc) goto fail;
        }
        for (uint32_t i = 0; i < id; i++) {
            rc = wl_evaluation_control_charge(control, 20);
            if (rc) goto fail;
            const char *old = intern_string_at(intern, i);
            rc = wl_intern_controlled_step(control, 10, i + 1);
            if (rc) goto fail;
            uint32_t key;
            rc = wl_intern_controlled_hash(old, &key, control, 11);
            if (rc) goto fail;
            uint32_t at = key & (capacity - 1);
            for (;;) {
                rc = wl_evaluation_control_charge(control, 2);
                if (rc) goto fail;
                bool empty = slots[at].string_id == SLOT_EMPTY;
                rc = wl_intern_controlled_step(control, 12, at);
                if (rc) goto fail;
                if (empty) break;
                at = (at + 1) & (capacity - 1);
            }
            rc = wl_evaluation_control_charge(control, 1);
            if (rc) goto fail;
            slots[at].string_id = i;
            rc = wl_intern_controlled_step(control, 10, i + 1);
            if (rc) goto fail;
        }
        h = hash & (capacity - 1);
        for (;;) {
            rc = wl_evaluation_control_charge(control, 2);
            if (rc) goto fail;
            bool empty = slots[h].string_id == SLOT_EMPTY;
            rc = wl_intern_controlled_step(control, 12, h);
            if (rc) goto fail;
            if (empty) break;
            h = (h + 1) & (capacity - 1);
        }
    }
    if (governor) {
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_intern_test_fail_commit) {
            wl_intern_test_fail_commit = false; failure = ENOMEM; goto fail;
        }
#endif
        bool accepted;
        rc = wl_columnar_memory_governor_commit_controlled(&pending, intern,
                &accepted, control);
        if (rc) goto fail;
        if (!accepted) {
            failure = ENOMEM; goto fail;
        }
        pending_committed = true;
    }
    rc = wl_evaluation_control_charge(control, 48);
    if (rc) goto fail;
    if (governor && (pending.identity != &pending
        || pending.bytes != retained || pending.governor != governor
        || atomic_load_explicit(&pending.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || intern->reservation.identity != &intern->reservation
        || intern->reservation.bytes != intern->reserved_bytes
        || intern->reservation.governor != governor
        || atomic_load_explicit(&intern->reservation.state,
        memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)) {
        failure = EINVAL; goto fail;
    }
    rc = wl_intern_controlled_step(control, 13, id);
    if (rc) goto fail;
    /* Two real token moves32 EACH (64), canonical/address fields at most16,
     * result fields3 and unlock1 total84;96 covers publication. Credit
     * retirement below is mandatory owner cleanup, not forward engine work. */
    rc = wl_evaluation_control_charge(control, 96);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) goto fail;
    wl_intern_slot_t *old = intern->slots;
    if (governor) {
        if (!wl_columnar_memory_reservation_move(&retired, &intern->reservation)
            || !wl_columnar_memory_reservation_move(&intern->reservation,
            &pending))
            abort();
        pending_live = false;
    }
    if (resize) {
        intern->slots = slots; intern->slot_capacity = capacity; slots = NULL;
    }
    if (segment) {
        intern->segments[seg] = segment; segment = NULL;
    }
    intern->segments[seg][id - intern_seg_base(seg)] = copy; copy = NULL;
    intern->slots[h].string_id = id;
    if (governor) intern->reserved_bytes = retained;
    WL_INTERN_STORE_RELEASE(&intern->count, id + 1);
    if (resize) free(old);
    if (governor && !wl_columnar_memory_release(&retired)) abort();
    if (scratch_live && !wl_columnar_memory_rollback(&scratch)) abort();
    wl_mutex_unlock(&intern->lock);
    *out_id = id; *status = 0; *budget_denied = false;
    return 0;
fail:
    free(copy); free(segment); free(slots);
    if (pending_live) {
        bool released = pending_committed ? wl_columnar_memory_release(&pending)
            : wl_columnar_memory_rollback(&pending);
        if (!released) abort();
    }
    if (scratch_live && !wl_columnar_memory_rollback(&scratch)) abort();
    wl_mutex_unlock(&intern->lock);
    if (rc) return rc;
    *out_id = -1; *status = failure; *budget_denied = denied;
    return 0;
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

    wl_mutex_lock(&mutable_intern->lock);
    int64_t id = intern_lookup_locked(intern, str, NULL);
    wl_mutex_unlock(&mutable_intern->lock);

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
