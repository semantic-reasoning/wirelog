/*
 * columnar/mem_ledger.h - wirelog Memory Ledger
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * Thread-safe memory accounting ledger with per-subsystem tracking,
 * budget accounting, pressure hints, and human-readable reporting.  Admission
 * enforcement is owned by the memory governor, not this post-allocation
 * ledger.
 *
 * Issue #224: Memory Observability and Graceful Degradation for DOOP OOM
 */

#ifndef WL_COLUMNAR_MEM_LEDGER_H
#define WL_COLUMNAR_MEM_LEDGER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* C11 atomics for lock-free thread safety */
#ifdef _MSC_VER
/* MSVC: Use intrinsic functions instead of <stdatomic.h> */
#include <windows.h>
#include <intrin.h>
#include <string.h>

/* MSVC does not support C11 _Atomic as a type qualifier or keyword.
 * Use volatile uint64_t as the atomic field type; all mutations go through
 * _Interlocked* intrinsics in mem_ledger.c which provide the required
 * acquire/release semantics. */
typedef volatile uint64_t wl_atomic_u64;

/* C11 atomic types/initializer used by the parallel keyed-join path.
 * Field accesses are paired with the atomic_*_explicit shims below; for
 * relaxed-order callers a volatile-qualified plain type is sufficient on
 * the architectures MSVC targets (x86/x64/ARM64), and the shimmed
 * fetch_add routes through InterlockedExchangeAdd64 for the wide
 * read-modify-write ops. */
typedef volatile bool atomic_bool;
typedef volatile int atomic_int;
typedef volatile uint_fast64_t atomic_uint_fast64_t;
#define ATOMIC_VAR_INIT(x) (x)
#define atomic_init(ptr, val) (*(ptr) = (val))

typedef char wl_columnar_mem_ledger_long64_width[(sizeof(LONG64) ==
        8) ? 1 : -1];
typedef char wl_columnar_mem_ledger_u64_width[(sizeof(wl_atomic_u64) ==
        8) ? 1 : -1];
typedef char wl_columnar_mem_ledger_fast64_width[(sizeof(atomic_uint_fast64_t)
        == 8) ? 1 : -1];
typedef char wl_columnar_mem_ledger_u64_alignment[(__alignof(wl_atomic_u64) >=
        8) ? 1 : -1];
typedef char wl_columnar_mem_ledger_fast64_alignment[(__alignof(
            atomic_uint_fast64_t) >= 8) ? 1 : -1];

/* MSVC atomic macros using intrinsics that are guaranteed to exist */
#define atomic_load_explicit(ptr, order) (*(ptr))
#define atomic_store_explicit(ptr, val, order) (*(ptr) = (val))

/* Every read-modify-write below dispatches on sizeof(*ptr) so that an
 * atomic_bool (1 byte) or atomic_int (4 bytes) uses the intrinsic of its
 * own width.  Routing every operand through the 64-bit intrinsics read and
 * overwrote the bytes next to a narrow atomic: a failure-injection flag that
 * was never armed read as set whenever its neighbour in .bss was non-zero,
 * and the exchange then zeroed that neighbour. Fetch arithmetic passes the
 * addend's representation to the platform operation without signed C sums
 * or a user retry loop. The signed prior return preserves narrow comparisons;
 * unsigned wide consumers must convert it before further arithmetic. */
static inline __int64
wl_atomic_fetch_add_internal(volatile void *ptr, uint64_t inc, size_t width)
{
    switch (width) {
    case 1: {
        uint8_t bits = (uint8_t)inc;
        char addend;
        memcpy(&addend, &bits, sizeof(addend));
        return _InterlockedExchangeAdd8((volatile char *)ptr, addend);
    }
    case 2: {
        uint16_t bits = (uint16_t)inc;
        short addend;
        memcpy(&addend, &bits, sizeof(addend));
        return _InterlockedExchangeAdd16((volatile short *)ptr, addend);
    }
    case 4: {
        uint32_t bits = (uint32_t)inc;
        long addend;
        memcpy(&addend, &bits, sizeof(addend));
        return _InterlockedExchangeAdd((volatile long *)ptr, addend);
    }
    default: {
        LONG64 addend;
        memcpy(&addend, &inc, sizeof(addend));
        /* Requires eight-byte alignment, as did the previous wide CAS.
         * The platform supplies the atomic operation and full barrier. */
        return InterlockedExchangeAdd64((volatile LONG64 *)ptr, addend);
    }
    }
}

#define atomic_fetch_add_explicit(ptr, inc, order) \
        wl_atomic_fetch_add_internal((ptr), (uint64_t)(inc), sizeof(*(ptr)))
/* The subtrahend is negated in unsigned arithmetic so that a decrement of
 * any magnitude wraps rather than overflowing the signed negation. */
#define atomic_fetch_sub_explicit(ptr, dec, order)                        \
        wl_atomic_fetch_add_internal((ptr),                               \
            UINT64_C(0) - (uint64_t)(dec), sizeof(*(ptr)))

/* On failure the observed value is written back through `expected` at the
 * operand's width, so a `bool *` or `int *` expected is never widened. */
static inline bool
wl_atomic_compare_exchange_weak_internal(volatile void *ptr, void *expected,
    __int64 desired, size_t width)
{
    switch (width) {
    case 1: {
        char want = *(char *)expected;
        char seen = _InterlockedCompareExchange8((volatile char *)ptr,
                (char)desired, want);
        if (seen != want) {
            *(char *)expected = seen;
            return false;
        }
        return true;
    }
    case 2: {
        short want = *(short *)expected;
        short seen = _InterlockedCompareExchange16((volatile short *)ptr,
                (short)desired, want);
        if (seen != want) {
            *(short *)expected = seen;
            return false;
        }
        return true;
    }
    case 4: {
        long want = *(long *)expected;
        long seen = _InterlockedCompareExchange((volatile long *)ptr,
                (long)desired, want);
        if (seen != want) {
            *(long *)expected = seen;
            return false;
        }
        return true;
    }
    default: {
        __int64 want = *(__int64 *)expected;
        __int64 seen = _InterlockedCompareExchange64((volatile __int64 *)ptr,
                desired, want);
        if (seen != want) {
            *(__int64 *)expected = seen;
            return false;
        }
        return true;
    }
    }
}

#define atomic_compare_exchange_weak_explicit(ptr, expected, desired,        \
            succ_order, fail_order)        \
        wl_atomic_compare_exchange_weak_internal((ptr), (expected),           \
            (__int64)(desired), sizeof(*(ptr)))

static inline __int64
wl_atomic_exchange_internal(volatile void *ptr, __int64 val, size_t width)
{
    switch (width) {
    case 1:
        return _InterlockedExchange8((volatile char *)ptr, (char)val);
    case 2:
        return _InterlockedExchange16((volatile short *)ptr, (short)val);
    case 4:
        return _InterlockedExchange((volatile long *)ptr, (long)val);
    default:
        return _InterlockedExchange64((volatile __int64 *)ptr, val);
    }
}

#define atomic_exchange_explicit(ptr, val, order) \
        wl_atomic_exchange_internal((ptr), (__int64)(val), sizeof(*(ptr)))

/* Memory orders (ignored on MSVC - intrinsics always use acquire/release semantics) */
#define memory_order_relaxed 0
#define memory_order_release 1
#define memory_order_acquire 2
#else
#include <stdatomic.h>
/* Portable atomic uint64 type for struct fields */
typedef _Atomic uint64_t wl_atomic_u64;
#endif

/* Reclaimers are invoked only at an owner-defined quiescent point.  The
 * registry deliberately has fixed storage: registering or unregistering a
 * callback must not allocate while the process is under memory pressure. */
#define WL_MEM_LEDGER_MAX_RECLAIMERS 8u

typedef uint64_t wl_mem_reclaimer_handle_t;

typedef struct {
    uint64_t bytes_released; /* measured bytes actually returned to allocator */
    uint32_t candidates;     /* candidates examined by the callback(s) */
} wl_mem_reclaim_result_t;

typedef wl_mem_reclaim_result_t (*wl_mem_reclaimer_fn)(void *owner);

typedef struct {
    wl_mem_reclaimer_fn fn;
    void *owner;
    wl_mem_reclaimer_handle_t handle;
    bool active;
} wl_mem_reclaimer_slot_t;

/* ======================================================================== */
/* Subsystem IDs                                                            */
/* ======================================================================== */

/*
 * Memory subsystem identifiers.
 * Each subsystem gets a fraction of the total budget (the per-subsystem
 * caps only drive wl_mem_ledger_subsys_over_budget() and
 * wl_mem_ledger_should_backpressure(); the RELATION share is the one the
 * join operator polls, so it is kept at 50%):
 *   RELATION:    50% - operator output relations (join outputs) whose
 *                      col_rel_t.mem_ledger is attached
 *   ARENA:       10% - eval arenas and delta pools (fixed capacity,
 *                      charged at create, credited at destroy)
 *   CACHE:       10% - materialization cache entries (re-parented from
 *                      RELATION on insert)
 *   ARRANGEMENT: 10% - hash, delta, filtered, sorted and differential
 *                      arrangements
 *   TIMESTAMP:    5% - timestamp arrays of ledger-attached relations
 *   CHANNEL:      5% - TDD delta transport: MPSC ring storage plus
 *                      payloads in flight between publish and drain
 *   STORED:       5% - session-owned relations (EDB/IDB and worker
 *                      partitions); a gauge sampled at iteration
 *                      boundaries, see wl_mem_ledger_set_gauge()
 *   TEMPORARY:    5% - heap-backed delta_pool temporaries; a gauge
 *                      sampled just before each delta_pool_reset()
 *
 * Issue #1380: CHANNEL, STORED and TEMPORARY were added so the ledger
 * covers every allocation class that a bounded-memory TDD run has to
 * reason about.  Byte units are exact allocation sizes (binary, 1 KB =
 * 1024 B in the report), see docs/MEMORY.md.
 */
#define WL_MEM_SUBSYS_RELATION 0
#define WL_MEM_SUBSYS_ARENA 1
#define WL_MEM_SUBSYS_CACHE 2
#define WL_MEM_SUBSYS_ARRANGEMENT 3
#define WL_MEM_SUBSYS_TIMESTAMP 4
#define WL_MEM_SUBSYS_CHANNEL 5
#define WL_MEM_SUBSYS_STORED 6
#define WL_MEM_SUBSYS_TEMPORARY 7
#define WL_MEM_SUBSYS_COUNT 8

/* Human-readable subsystem names (parallel array, indexed by subsystem ID) */
extern const char *wl_mem_subsys_names[WL_MEM_SUBSYS_COUNT];

/* Subsystem budget fractions (numerator / 100, must sum to 100) */
extern const uint32_t wl_mem_subsys_pct[WL_MEM_SUBSYS_COUNT];

/* ======================================================================== */
/* Ledger Type                                                              */
/* ======================================================================== */

/*
 * wl_mem_ledger_t: session-level memory accounting ledger.
 *
 * All fields updated via atomic operations; safe to read/write from
 * multiple threads (K-fusion worker threads and session thread).
 *
 * @total_budget:  Maximum allowed bytes (0 = unlimited).
 * @current_bytes: Total currently allocated bytes across all subsystems.
 * @peak_bytes:    High-water mark of current_bytes over the session lifetime.
 * @subsys_bytes:  Per-subsystem current allocation in bytes.
 * @subsys_peak:   Per-subsystem high-water mark.
 */
typedef struct wl_mem_ledger {
    wl_atomic_u64 total_budget;
    wl_atomic_u64 current_bytes;
    wl_atomic_u64 peak_bytes;
    wl_atomic_u64 subsys_bytes[WL_MEM_SUBSYS_COUNT];
    wl_atomic_u64 subsys_peak[WL_MEM_SUBSYS_COUNT];
    wl_mem_reclaimer_slot_t reclaimers[WL_MEM_LEDGER_MAX_RECLAIMERS];
    wl_mem_reclaimer_handle_t next_reclaimer_handle;
} wl_mem_ledger_t;

/*
 * wl_mem_ledger_snapshot_t: plain (non-atomic) copy of a ledger, taken by
 * wl_mem_ledger_snapshot().  This is the type reporters and stats
 * accessors consume so that they never touch the atomics directly.
 */
typedef struct {
    uint64_t total_budget;
    uint64_t current_bytes;
    uint64_t peak_bytes;
    uint64_t subsys_bytes[WL_MEM_SUBSYS_COUNT];
    uint64_t subsys_peak[WL_MEM_SUBSYS_COUNT];
} wl_mem_ledger_snapshot_t;

/* ======================================================================== */
/* API                                                                      */
/* ======================================================================== */

/*
 * wl_mem_ledger_init:
 * @ledger:        Ledger to initialise (caller-allocated or embedded).
 * @budget_bytes:  Total memory budget in bytes.  0 = unlimited.
 *
 * Zeroes all counters and sets the budget.
 * Safe to call from a single thread during session creation.
 */
void
wl_mem_ledger_init(wl_mem_ledger_t *ledger, uint64_t budget_bytes);

/*
 * wl_mem_ledger_alloc:
 * @ledger:    Ledger to update.
 * @subsys:    WL_MEM_SUBSYS_* identifier.
 * @bytes:     Number of bytes allocated.  Accounting saturates at
 *             UINT64_MAX; this function does not reserve memory or reject an
 *             allocation.
 *
 * Records an allocation.  Updates current_bytes, peak_bytes,
 * subsys_bytes[subsys], and subsys_peak[subsys] atomically.
 * Does NOT enforce the budget; call wl_mem_ledger_over_budget() after.
 *
 * Thread-safe.
 */
void
wl_mem_ledger_alloc(wl_mem_ledger_t *ledger, int subsys, uint64_t bytes);

/*
 * wl_mem_ledger_free:
 * @ledger:    Ledger to update.
 * @subsys:    WL_MEM_SUBSYS_* identifier.
 * @bytes:     Number of bytes freed.
 *
 * Records a deallocation.  Clamps to zero to tolerate accounting skew.
 * Thread-safe.
 */
void
wl_mem_ledger_free(wl_mem_ledger_t *ledger, int subsys, uint64_t bytes);

/* Register/unregister a callback used by a quiescent owner to release
 * reclaimable capacity.  Unregister before destroying @owner.  A stale
 * handle cannot unregister a later callback that reused the slot. */
int
wl_mem_ledger_register_reclaimer(wl_mem_ledger_t *ledger,
    wl_mem_reclaimer_fn fn, void *owner, wl_mem_reclaimer_handle_t *out);
void
wl_mem_ledger_unregister_reclaimer(wl_mem_ledger_t *ledger,
    wl_mem_reclaimer_handle_t handle);

/* Invoke currently registered reclaimers.  Callers must provide a quiescent
 * point for every owner and keep owners alive until this function returns. */
wl_mem_reclaim_result_t
wl_mem_ledger_reclaim(wl_mem_ledger_t *ledger);

/*
 * wl_mem_ledger_over_budget:
 * @ledger:  Ledger to check.
 *
 * Returns true when total_budget > 0 AND current_bytes > total_budget.
 * Thread-safe (single atomic load each).
 */
bool
wl_mem_ledger_over_budget(const wl_mem_ledger_t *ledger);

/*
 * wl_mem_ledger_subsys_over_budget:
 * @ledger:  Ledger to check.
 * @subsys:  WL_MEM_SUBSYS_* identifier.
 *
 * Returns true when the subsystem has exceeded its proportional share
 * of the total budget.  Returns false when budget is 0 (unlimited).
 * Thread-safe.
 */
bool
wl_mem_ledger_subsys_over_budget(const wl_mem_ledger_t *ledger, int subsys);

/*
 * wl_mem_ledger_should_backpressure:
 * @ledger:    Ledger to check.
 * @subsys:    WL_MEM_SUBSYS_* identifier.
 * @threshold: Fraction (0-100) of subsystem cap at which to signal pressure.
 *
 * Returns true when the subsystem has consumed >= threshold% of its cap.
 * Values above 100 are invalid and return false.  This accounting hint is
 * not an admission decision.
 * Callers use this to trigger cache eviction, worker scaling, etc.
 * Returns false when budget is 0 (unlimited).
 * Thread-safe.
 */
bool
wl_mem_ledger_should_backpressure(const wl_mem_ledger_t *ledger, int subsys,
    uint32_t threshold_pct);

/*
 * wl_mem_ledger_bytes_remaining:
 * @ledger:  Ledger to query.
 *
 * Returns budget - current_bytes, or UINT64_MAX when budget is 0 (unlimited).
 * Returns 0 when over budget.
 * Thread-safe.
 */
uint64_t
wl_mem_ledger_bytes_remaining(const wl_mem_ledger_t *ledger);

/*
 * wl_mem_ledger_set_gauge:
 * @ledger:  Ledger to update.
 * @subsys:  WL_MEM_SUBSYS_* identifier.
 * @bytes:   New absolute value for the subsystem.
 *
 * Replaces the subsystem's current byte count with @bytes and moves the
 * total by the difference, updating both peaks.  Used for classes that
 * are enumerated and re-measured at well-defined points (STORED and
 * TEMPORARY) instead of being charged per allocation.  Mixing alloc/free
 * and set_gauge on the same subsystem is not supported.
 * Thread-safe.
 */
void
wl_mem_ledger_set_gauge(wl_mem_ledger_t *ledger, int subsys, uint64_t bytes);

/*
 * wl_mem_ledger_snapshot:
 * @ledger:  Ledger to read.
 * @out:     Receives a plain copy of every counter.
 *
 * Relaxed loads; counters may be mutually skewed by concurrent updates.
 * Thread-safe.
 */
void
wl_mem_ledger_snapshot(const wl_mem_ledger_t *ledger,
    wl_mem_ledger_snapshot_t *out);

/*
 * wl_mem_ledger_report:
 * @ledger:  Ledger to report.
 *
 * Prints a human-readable per-subsystem memory breakdown to stderr.
 * Format (one line per subsystem plus totals):
 *
 *   [wirelog mem] budget=48.0GB current=12.3GB peak=15.6GB budget_bytes=...
 *     RELATION     current=8.2GB  peak=10.1GB  cap=24.0GB current_bytes=...
 *     ARENA        current=1.1GB  peak=2.0GB   cap=9.6GB
 *     CACHE        current=0.3GB  peak=0.5GB   cap=4.8GB
 *     ARRANGEMENT  current=0.4GB  peak=0.6GB   cap=4.8GB
 *     TIMESTAMP    current=0.1GB  peak=0.2GB   cap=2.4GB
 *     CHANNEL      current=0B     peak=0.3GB   cap=2.4GB
 *     STORED       current=2.0GB  peak=2.0GB   cap=2.4GB
 *     TEMPORARY    current=0B     peak=1.2GB   cap=2.4GB
 *
 * The exact *_bytes fields use IEC byte counts and are stable for parsers;
 * the human-readable fields are retained for operator diagnostics.
 * Thread-safe (reads atomics with relaxed ordering; for reporting only).
 */
void
wl_mem_ledger_report(const wl_mem_ledger_t *ledger);

#endif /* WL_COLUMNAR_MEM_LEDGER_H */
