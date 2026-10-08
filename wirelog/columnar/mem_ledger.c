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

/* The accounting API has no failure return.  Saturating increments preserve
 * the invariant that a counter never wraps into a deceptively small value;
 * admission and allocation failure remain responsibilities of the governor
 * and allocator layers described in docs/MEMORY.md. */
static uint64_t
saturating_add(wl_atomic_u64 *counter, uint64_t bytes)
{
    uint64_t old = atomic_load_explicit(counter, memory_order_relaxed);
    for (;;) {
        uint64_t next = (UINT64_MAX - old < bytes) ? UINT64_MAX : old + bytes;
        if (atomic_compare_exchange_weak_explicit(counter, &old, next,
            memory_order_relaxed,
            memory_order_relaxed))
            return next;
    }
}

/*
 * update_peak: atomically update @peak_atom to max(*peak_atom, new_val).
 * Uses compare-exchange loop.
 */
static void
update_peak(wl_atomic_u64 *peak_atom, uint64_t new_val)
{
    uint64_t old = atomic_load_explicit(peak_atom, memory_order_relaxed);
    while (old < new_val) {
        if (atomic_compare_exchange_weak_explicit(peak_atom, &old, new_val,
            memory_order_relaxed,
            memory_order_relaxed)) {
            break;
        }
        /* old updated by CAS on failure; retry */
    }
}

/*
 * total_add: add @bytes to current_bytes and bump peak_bytes.
 */
static void
total_add(wl_mem_ledger_t *ledger, uint64_t bytes)
{
    uint64_t total_new = saturating_add(&ledger->current_bytes, bytes);
    update_peak(&ledger->peak_bytes, total_new);
}

/*
 * counter_sub_clamped: subtract @bytes from @counter, clamping at zero.
 *
 * CAS loop rather than fetch_sub: avoids the TOCTOU race between load and
 * subtract under concurrent worker teardown.  Without CAS, two concurrent
 * frees could both read the same old value, both decide to subtract the
 * full amount, and underflow the counter.
 */
static void
counter_sub_clamped(wl_atomic_u64 *counter, uint64_t bytes)
{
    uint64_t old = atomic_load_explicit(counter, memory_order_relaxed);
    uint64_t updated;
    do {
        updated = (bytes > old) ? 0 : old - bytes;
    } while (!atomic_compare_exchange_weak_explicit(
            counter, &old, updated, memory_order_relaxed,
            memory_order_relaxed));
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
    atomic_store_explicit(&ledger->total_budget, budget_bytes,
        memory_order_relaxed);
}

void
wl_mem_ledger_alloc(wl_mem_ledger_t *ledger, int subsys, uint64_t bytes)
{
    if (!ledger || bytes == 0)
        return;
    if (subsys < 0 || subsys >= WL_MEM_SUBSYS_COUNT)
        return;

    /* Update subsystem counter */
    uint64_t subsys_new = saturating_add(&ledger->subsys_bytes[subsys], bytes);
    update_peak(&ledger->subsys_peak[subsys], subsys_new);

    /* Update total counter */
    uint64_t total_new = saturating_add(&ledger->current_bytes, bytes);
    update_peak(&ledger->peak_bytes, total_new);
}

void
wl_mem_ledger_free(wl_mem_ledger_t *ledger, int subsys, uint64_t bytes)
{
    if (!ledger || bytes == 0)
        return;
    if (subsys < 0 || subsys >= WL_MEM_SUBSYS_COUNT)
        return;

    counter_sub_clamped(&ledger->subsys_bytes[subsys], bytes);
    counter_sub_clamped(&ledger->current_bytes, bytes);
}

void
wl_columnar_mem_ledger_apply_exclusive(
    const wl_columnar_mem_ledger_exclusive_prepared_t *prepared)
{
    if (!prepared || !prepared->ledger)
        return;
    wl_mem_ledger_t *ledger = prepared->ledger;
    for (uint32_t i = 0; i < prepared->count; i++) {
        const wl_columnar_mem_ledger_exclusive_request_t *request =
            &prepared->requests[i];
        uint64_t bytes = request->bytes;
        if (!bytes)
            continue;
        int subsystem = request->subsystem;
        uint64_t sub = atomic_load_explicit(&ledger->subsys_bytes[subsystem],
                memory_order_relaxed);
        uint64_t total;
        if (request->allocate) {
            sub = UINT64_MAX - sub < bytes ? UINT64_MAX : sub + bytes;
            atomic_store_explicit(&ledger->subsys_bytes[subsystem], sub,
                memory_order_relaxed);
            uint64_t peak =
                atomic_load_explicit(&ledger->subsys_peak[subsystem],
                    memory_order_relaxed);
            atomic_store_explicit(&ledger->subsys_peak[subsystem],
                peak < sub ? sub : peak, memory_order_relaxed);
            total = atomic_load_explicit(&ledger->current_bytes,
                    memory_order_relaxed);
            total = UINT64_MAX - total < bytes ? UINT64_MAX : total + bytes;
            atomic_store_explicit(&ledger->current_bytes, total,
                memory_order_relaxed);
            peak = atomic_load_explicit(&ledger->peak_bytes,
                    memory_order_relaxed);
            atomic_store_explicit(&ledger->peak_bytes,
                peak < total ? total : peak, memory_order_relaxed);
        } else {
            atomic_store_explicit(&ledger->subsys_bytes[subsystem],
                bytes > sub ? 0 : sub - bytes, memory_order_relaxed);
            total = atomic_load_explicit(&ledger->current_bytes,
                    memory_order_relaxed);
            atomic_store_explicit(&ledger->current_bytes,
                bytes > total ? 0 : total - bytes, memory_order_relaxed);
        }
    }
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

    uint64_t old = atomic_exchange_explicit(&ledger->subsys_bytes[subsys],
            bytes, memory_order_relaxed);
    update_peak(&ledger->subsys_peak[subsys], bytes);

    /* Move the total by the difference so current_bytes stays the sum of
     * the subsystem counters (modulo the documented relaxed skew). */
    if (bytes > old)
        total_add(ledger, bytes - old);
    else if (old > bytes)
        counter_sub_clamped(&ledger->current_bytes, old - bytes);
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
