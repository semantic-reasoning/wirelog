/*
 * columnar/mem_ledger.c - wirelog Memory Ledger Implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Thread-safe memory accounting using C11 _Atomic operations.
 *
 * Issue #224: Memory Observability and Graceful Degradation for DOOP OOM
 */

#include "columnar/mem_ledger.h"

#include <errno.h>
#ifndef _MSC_VER
#include <stdatomic.h>
#endif
#include <stdio.h>
#include <string.h>

/* ======================================================================== */
/* Subsystem Metadata                                                       */
/* ======================================================================== */

const char *wl_mem_subsys_names[WL_MEM_SUBSYS_COUNT] = {
    "RELATION",    /* 0 */
    "ARENA",       /* 1 */
    "CACHE",       /* 2 */
    "ARRANGEMENT", /* 3 */
    "TIMESTAMP",   /* 4 */
    "CHANNEL",     /* 5 */
    "STORED",      /* 6 */
    "TEMPORARY",   /* 7 */
};

/* Budget fraction for each subsystem (must sum to 100) */
const uint32_t wl_mem_subsys_pct[WL_MEM_SUBSYS_COUNT] = {
    50, /* RELATION    */
    10, /* ARENA       */
    10, /* CACHE       */
    10, /* ARRANGEMENT */
    5,  /* TIMESTAMP   */
    5,  /* CHANNEL     */
    5,  /* STORED      */
    5,  /* TEMPORARY   */
};

/* ======================================================================== */
/* Internal Helpers                                                         */
/* ======================================================================== */

/*
 * fmt_bytes: format @bytes as a human-readable string (B/KB/MB/GB).
 * Writes into buf[len].  Returns buf.
 */
static char *
fmt_bytes(uint64_t bytes, char *buf, size_t len)
{
    if (bytes >= (uint64_t)1024 * 1024 * 1024) {
        snprintf(buf, len, "%.1fGB",
            (double)bytes / ((double)1024 * 1024 * 1024));
    } else if (bytes >= (uint64_t)1024 * 1024) {
        snprintf(buf, len, "%.1fMB", (double)bytes / ((double)1024 * 1024));
    } else if (bytes >= 1024) {
        snprintf(buf, len, "%.1fKB", (double)bytes / 1024.0);
    } else {
        snprintf(buf, len, "%lluB", (unsigned long long)bytes);
    }
    return buf;
}

/* Return floor(value * percentage / 100) without overflowing value *
 * percentage.  The percentage tables and public threshold inputs are
 * uint32_t, so the quotient/remainder form keeps the multiplication bounded
 * even for UINT64_MAX-scale budgets. */
static uint64_t
percent_of(uint64_t value, uint32_t percentage)
{
    uint64_t quotient = value / 100;
    uint64_t remainder = value % 100;
    return quotient * (uint64_t)percentage
           + (remainder * (uint64_t)percentage) / 100;
}

/* All accounting writers serialize on this ledger-local gate. Raw callers
 * wait without cancellation; controlled publication will admit each attempt
 * before acquiring the same gate. No callback, allocation, reclaimer or other
 * lock acquisition is permitted inside the finite accounting section. */
static void
wl_columnar_mem_ledger_writer_acquire(wl_mem_ledger_t *ledger)
{
    for (;;) {
        uint64_t expected = 0;
        if (atomic_compare_exchange_weak_explicit(&ledger->writer_gate,
            &expected, 1, memory_order_acquire, memory_order_relaxed))
            return;
        /* Read-only waiting avoids repeated write ownership transfers while
         * a peer is performing the finite protected operation. */
        while (atomic_load_explicit(&ledger->writer_gate,
            memory_order_relaxed) != 0) {
        }
    }
}

static void
wl_columnar_mem_ledger_writer_release(wl_mem_ledger_t *ledger)
{
#ifdef _MSC_VER
    (void)atomic_exchange_explicit(&ledger->writer_gate, 0,
        memory_order_release);
#else
    atomic_store_explicit(&ledger->writer_gate, 0, memory_order_release);
#endif
}

/* Protected arithmetic preserves saturating/clamped raw accounting. Counter
 * loads/stores remain atomic for lock-free readers; these helpers never retry.
 * Saturation can lose magnitude, so subsystem sums need not equal total. */
static uint64_t
wl_columnar_mem_ledger_saturating_add(wl_atomic_u64 *counter, uint64_t bytes)
{
    uint64_t old = atomic_load_explicit(counter, memory_order_relaxed);
    uint64_t next = UINT64_MAX - old < bytes ? UINT64_MAX : old + bytes;
    atomic_store_explicit(counter, next, memory_order_relaxed);
    return next;
}

static void
wl_columnar_mem_ledger_update_peak(wl_atomic_u64 *counter, uint64_t value)
{
    uint64_t old = atomic_load_explicit(counter, memory_order_relaxed);
    if (old < value)
        atomic_store_explicit(counter, value, memory_order_relaxed);
}

static void
wl_columnar_mem_ledger_counter_sub_clamped(wl_atomic_u64 *counter,
    uint64_t bytes)
{
    uint64_t old = atomic_load_explicit(counter, memory_order_relaxed);
    atomic_store_explicit(counter, bytes > old ? 0 : old - bytes,
        memory_order_relaxed);
}

static void
wl_columnar_mem_ledger_total_add(wl_mem_ledger_t *ledger, uint64_t bytes)
{
    uint64_t next = wl_columnar_mem_ledger_saturating_add(
        &ledger->current_bytes, bytes);
    wl_columnar_mem_ledger_update_peak(&ledger->peak_bytes, next);
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

void
wl_mem_ledger_init(wl_mem_ledger_t *ledger, uint64_t budget_bytes)
{
    if (!ledger)
        return;
    memset(ledger, 0, sizeof(*ledger));
    atomic_store_explicit(&ledger->writer_gate, 0, memory_order_relaxed);
    atomic_store_explicit(&ledger->total_budget, budget_bytes,
        memory_order_relaxed);
}

void
wl_columnar_mem_ledger_event_locked(wl_mem_ledger_t *ledger, int subsys,
    uint64_t bytes, bool allocate)
{
    if (bytes == 0)
        return;
    if (allocate) {
        uint64_t subsys_new = wl_columnar_mem_ledger_saturating_add(
            &ledger->subsys_bytes[subsys], bytes);
        wl_columnar_mem_ledger_update_peak(&ledger->subsys_peak[subsys],
            subsys_new);
        uint64_t total_new = wl_columnar_mem_ledger_saturating_add(
            &ledger->current_bytes, bytes);
        wl_columnar_mem_ledger_update_peak(&ledger->peak_bytes, total_new);
    } else {
        wl_columnar_mem_ledger_counter_sub_clamped(
            &ledger->subsys_bytes[subsys],
            bytes);
        wl_columnar_mem_ledger_counter_sub_clamped(&ledger->current_bytes,
            bytes);
    }
}

void
wl_mem_ledger_alloc(wl_mem_ledger_t *ledger, int subsys, uint64_t bytes)
{
    if (!ledger || bytes == 0)
        return;
    if (subsys < 0 || subsys >= WL_MEM_SUBSYS_COUNT)
        return;
    wl_columnar_mem_ledger_writer_acquire(ledger);
    wl_columnar_mem_ledger_event_locked(ledger, subsys, bytes, true);
    wl_columnar_mem_ledger_writer_release(ledger);
}

void
wl_mem_ledger_free(wl_mem_ledger_t *ledger, int subsys, uint64_t bytes)
{
    if (!ledger || bytes == 0)
        return;
    if (subsys < 0 || subsys >= WL_MEM_SUBSYS_COUNT)
        return;

    wl_columnar_mem_ledger_writer_acquire(ledger);
    wl_columnar_mem_ledger_event_locked(ledger, subsys, bytes, false);
    wl_columnar_mem_ledger_writer_release(ledger);
}

int
wl_columnar_mem_ledger_clone_unpublished(wl_mem_ledger_t *destination,
    const wl_mem_ledger_t *source)
{
    if (!destination || !source || destination == source)
        return EINVAL;
    wl_mem_ledger_snapshot_t snapshot;
    wl_mem_ledger_snapshot(source, &snapshot);
    wl_mem_ledger_init(destination, snapshot.total_budget);
    atomic_store_explicit(&destination->current_bytes, snapshot.current_bytes,
        memory_order_relaxed);
    atomic_store_explicit(&destination->peak_bytes, snapshot.peak_bytes,
        memory_order_relaxed);
    for (unsigned i = 0; i < WL_MEM_SUBSYS_COUNT; i++) {
        atomic_store_explicit(&destination->subsys_bytes[i],
            snapshot.subsys_bytes[i], memory_order_relaxed);
        atomic_store_explicit(&destination->subsys_peak[i],
            snapshot.subsys_peak[i], memory_order_relaxed);
    }
    return 0;
}

int
wl_mem_ledger_register_reclaimer(wl_mem_ledger_t *ledger,
    wl_mem_reclaimer_fn fn, void *owner, wl_mem_reclaimer_handle_t *out)
{
    if (!ledger || !fn || !out)
        return EINVAL;

    for (uint32_t i = 0; i < WL_MEM_LEDGER_MAX_RECLAIMERS; i++) {
        wl_mem_reclaimer_slot_t *slot = &ledger->reclaimers[i];
        if (slot->active)
            continue;
        wl_mem_reclaimer_handle_t handle = ++ledger->next_reclaimer_handle;
        if (handle == 0)
            handle = ++ledger->next_reclaimer_handle;
        slot->fn = fn;
        slot->owner = owner;
        slot->handle = handle;
        slot->active = true;
        *out = handle;
        return 0;
    }
    *out = 0;
    return ENOSPC;
}

void
wl_mem_ledger_unregister_reclaimer(wl_mem_ledger_t *ledger,
    wl_mem_reclaimer_handle_t handle)
{
    if (!ledger || handle == 0)
        return;
    for (uint32_t i = 0; i < WL_MEM_LEDGER_MAX_RECLAIMERS; i++) {
        wl_mem_reclaimer_slot_t *slot = &ledger->reclaimers[i];
        if (!slot->active || slot->handle != handle)
            continue;
        /* Clear the owner before making the slot reusable.  A quiescent
         * caller therefore cannot observe a live callback with a dead owner. */
        slot->fn = NULL;
        slot->owner = NULL;
        slot->handle = 0;
        slot->active = false;
        return;
    }
}

wl_mem_reclaim_result_t
wl_mem_ledger_reclaim(wl_mem_ledger_t *ledger)
{
    wl_mem_reclaim_result_t total = { 0, 0 };
    if (!ledger)
        return total;

    /* The owner supplies the quiescent point.  Copying the callback and
     * context before invocation lets a callback unregister itself safely;
     * its owner remains responsible for surviving until this call returns. */
    for (uint32_t i = 0; i < WL_MEM_LEDGER_MAX_RECLAIMERS; i++) {
        wl_mem_reclaimer_fn fn = ledger->reclaimers[i].active
            ? ledger->reclaimers[i].fn : NULL;
        void *owner = ledger->reclaimers[i].active
            ? ledger->reclaimers[i].owner : NULL;
        if (!fn)
            continue;
        wl_mem_reclaim_result_t result = fn(owner);
        if (UINT64_MAX - total.bytes_released < result.bytes_released)
            total.bytes_released = UINT64_MAX;
        else
            total.bytes_released += result.bytes_released;
        if (UINT32_MAX - total.candidates < result.candidates)
            total.candidates = UINT32_MAX;
        else
            total.candidates += result.candidates;
    }
    return total;
}

void
wl_mem_ledger_set_gauge(wl_mem_ledger_t *ledger, int subsys, uint64_t bytes)
{
    if (!ledger)
        return;
    if (subsys < 0 || subsys >= WL_MEM_SUBSYS_COUNT)
        return;

    wl_columnar_mem_ledger_writer_acquire(ledger);
    uint64_t old = atomic_load_explicit(&ledger->subsys_bytes[subsys],
            memory_order_relaxed);
    atomic_store_explicit(&ledger->subsys_bytes[subsys], bytes,
        memory_order_relaxed);
    wl_columnar_mem_ledger_update_peak(&ledger->subsys_peak[subsys], bytes);

    /* Move the total by the difference so current_bytes stays the sum of
     * the subsystem counters (modulo the documented relaxed skew). */
    if (bytes > old)
        wl_columnar_mem_ledger_total_add(ledger, bytes - old);
    else if (old > bytes)
        wl_columnar_mem_ledger_counter_sub_clamped(&ledger->current_bytes,
            old - bytes);
    wl_columnar_mem_ledger_writer_release(ledger);
}

bool
wl_mem_ledger_over_budget(const wl_mem_ledger_t *ledger)
{
    if (!ledger)
        return false;
    uint64_t budget
        = atomic_load_explicit(&ledger->total_budget, memory_order_relaxed);
    if (budget == 0)
        return false;
    uint64_t current
        = atomic_load_explicit(&ledger->current_bytes, memory_order_relaxed);
    return current > budget;
}

bool
wl_mem_ledger_subsys_over_budget(const wl_mem_ledger_t *ledger, int subsys)
{
    if (!ledger || subsys < 0 || subsys >= WL_MEM_SUBSYS_COUNT)
        return false;
    uint64_t budget
        = atomic_load_explicit(&ledger->total_budget, memory_order_relaxed);
    if (budget == 0)
        return false;
    uint64_t cap = percent_of(budget, wl_mem_subsys_pct[subsys]);
    uint64_t current = atomic_load_explicit(&ledger->subsys_bytes[subsys],
            memory_order_relaxed);
    return current > cap;
}

bool
wl_mem_ledger_should_backpressure(const wl_mem_ledger_t *ledger, int subsys,
    uint32_t threshold_pct)
{
    if (!ledger || subsys < 0 || subsys >= WL_MEM_SUBSYS_COUNT)
        return false;
    uint64_t budget
        = atomic_load_explicit(&ledger->total_budget, memory_order_relaxed);
    if (budget == 0)
        return false;
    uint64_t cap = percent_of(budget, wl_mem_subsys_pct[subsys]);
    if (cap == 0)
        return false;
    if (threshold_pct > 100)
        return false;
    uint64_t current = atomic_load_explicit(&ledger->subsys_bytes[subsys],
            memory_order_relaxed);
    /* current >= cap * threshold_pct / 100 */
    return current >= percent_of(cap, threshold_pct);
}

uint64_t
wl_mem_ledger_bytes_remaining(const wl_mem_ledger_t *ledger)
{
    if (!ledger)
        return UINT64_MAX;
    uint64_t budget
        = atomic_load_explicit(&ledger->total_budget, memory_order_relaxed);
    if (budget == 0)
        return UINT64_MAX;
    uint64_t current
        = atomic_load_explicit(&ledger->current_bytes, memory_order_relaxed);
    if (current >= budget)
        return 0;
    return budget - current;
}

void
wl_mem_ledger_snapshot(const wl_mem_ledger_t *ledger,
    wl_mem_ledger_snapshot_t *out)
{
    if (!out)
        return;
    memset(out, 0, sizeof(*out));
    if (!ledger)
        return;

    out->total_budget
        = atomic_load_explicit(&ledger->total_budget, memory_order_relaxed);
    out->current_bytes
        = atomic_load_explicit(&ledger->current_bytes, memory_order_relaxed);
    out->peak_bytes
        = atomic_load_explicit(&ledger->peak_bytes, memory_order_relaxed);
    for (int i = 0; i < WL_MEM_SUBSYS_COUNT; i++) {
        out->subsys_bytes[i] = atomic_load_explicit(&ledger->subsys_bytes[i],
                memory_order_relaxed);
        out->subsys_peak[i] = atomic_load_explicit(&ledger->subsys_peak[i],
                memory_order_relaxed);
    }
}

void
wl_mem_ledger_report(const wl_mem_ledger_t *ledger)
{
    if (!ledger)
        return;

    wl_mem_ledger_snapshot_t snap;
    wl_mem_ledger_snapshot(ledger, &snap);
    uint64_t budget = snap.total_budget;

    char b1[32], b2[32], b3[32], b4[32];
    fprintf(stderr,
        "[wirelog mem] budget=%s current=%s peak=%s budget_bytes=%llu "
        "current_bytes=%llu peak_bytes=%llu\n",
        budget == 0 ? "unlimited" : fmt_bytes(budget, b1, sizeof(b1)),
        fmt_bytes(snap.current_bytes, b2, sizeof(b2)),
        fmt_bytes(snap.peak_bytes, b3, sizeof(b3)), (unsigned long long)budget,
        (unsigned long long)snap.current_bytes,
        (unsigned long long)snap.peak_bytes);

    for (int i = 0; i < WL_MEM_SUBSYS_COUNT; i++) {
        uint64_t sc = snap.subsys_bytes[i];
        uint64_t sp = snap.subsys_peak[i];
        uint64_t cap = (budget > 0) ? percent_of(budget,
                wl_mem_subsys_pct[i]) : 0;
        fprintf(stderr,
            "  %-12s current=%-10s peak=%-10s cap=%s current_bytes=%llu "
            "peak_bytes=%llu cap_bytes=%llu\n",
            wl_mem_subsys_names[i], fmt_bytes(sc, b1, sizeof(b1)),
            fmt_bytes(sp, b2, sizeof(b2)),
            cap > 0 ? fmt_bytes(cap, b3, sizeof(b3))
                        : fmt_bytes(0, b4, sizeof(b4)),
            (unsigned long long)sc, (unsigned long long)sp,
            (unsigned long long)cap);
    }
}
