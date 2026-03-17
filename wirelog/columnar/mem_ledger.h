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
 * budget enforcement, and human-readable reporting.
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
#include <intrin.h>
#pragma intrinsic(_InterlockedCompareExchange64)
#pragma intrinsic(_InterlockedAdd64)
#pragma intrinsic(_InterlockedExchange64)

/* MSVC does not support C11 _Atomic as a type qualifier or keyword.
 * Use volatile uint64_t as the atomic field type; all mutations go through
 * _Interlocked* intrinsics in mem_ledger.c which provide the required
 * acquire/release semantics. */
typedef volatile uint64_t wl_atomic_u64;

/* MSVC atomic macros using intrinsics */
#define atomic_load_explicit(ptr, order) (*(ptr))
#define atomic_store_explicit(ptr, val, order) (*(ptr) = (val))
#define atomic_fetch_add_explicit(ptr, inc, order) \
    _InterlockedAdd64((volatile LONG64 *)(ptr), (LONG64)(inc)) - (LONG64)(inc)
#define atomic_fetch_sub_explicit(ptr, dec, order) \
    _InterlockedAdd64((volatile LONG64 *)(ptr), -(LONG64)(dec)) + (LONG64)(dec)
#define atomic_compare_exchange_weak_explicit(ptr, expected, desired,        \
                                              succ_order, fail_order)        \
    (_InterlockedCompareExchange64((volatile LONG64 *)(ptr),                 \
                                   (LONG64)(desired), (LONG64) * (expected)) \
     == (LONG64) * (expected))

/* Memory orders (ignored on MSVC - intrinsics always use acquire/release semantics) */
#define memory_order_relaxed 0
#define memory_order_release 1
#define memory_order_acquire 2
#else
#include <stdatomic.h>
/* Portable atomic uint64 type for struct fields */
typedef _Atomic uint64_t wl_atomic_u64;
#endif

/* ======================================================================== */
/* Subsystem IDs                                                            */
/* ======================================================================== */

/*
 * Memory subsystem identifiers.
 * Each subsystem gets a fraction of the total budget:
 *   RELATION:    50% - IDB relation data buffers
 *   ARENA:       20% - Worker arenas and delta pools
 *   CACHE:       10% - Materialization cache
 *   ARRANGEMENT: 10% - Hash and sorted arrangements
 *   TIMESTAMP:   10% - Timestamp arrays and delta relations
 */
#define WL_MEM_SUBSYS_RELATION 0
#define WL_MEM_SUBSYS_ARENA 1
#define WL_MEM_SUBSYS_CACHE 2
#define WL_MEM_SUBSYS_ARRANGEMENT 3
#define WL_MEM_SUBSYS_TIMESTAMP 4
#define WL_MEM_SUBSYS_COUNT 5

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
typedef struct {
    wl_atomic_u64 total_budget;
    wl_atomic_u64 current_bytes;
    wl_atomic_u64 peak_bytes;
    wl_atomic_u64 subsys_bytes[WL_MEM_SUBSYS_COUNT];
    wl_atomic_u64 subsys_peak[WL_MEM_SUBSYS_COUNT];
} wl_mem_ledger_t;

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
 * @bytes:     Number of bytes allocated.
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
 * wl_mem_ledger_report:
 * @ledger:  Ledger to report.
 *
 * Prints a human-readable per-subsystem memory breakdown to stderr.
 * Format (one line per subsystem plus totals):
 *
 *   [wirelog mem] budget=48.0GB current=12.3GB peak=15.6GB
 *     RELATION     current=8.2GB  peak=10.1GB  cap=24.0GB
 *     ARENA        current=1.1GB  peak=2.0GB   cap=9.6GB
 *     CACHE        current=0.3GB  peak=0.5GB   cap=4.8GB
 *     ARRANGEMENT  current=0.4GB  peak=0.6GB   cap=4.8GB
 *     TIMESTAMP    current=0.1GB  peak=0.2GB   cap=4.8GB
 *
 * Thread-safe (reads atomics with relaxed ordering; for reporting only).
 */
void
wl_mem_ledger_report(const wl_mem_ledger_t *ledger);

#endif /* WL_COLUMNAR_MEM_LEDGER_H */
