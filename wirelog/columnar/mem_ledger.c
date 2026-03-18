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
};

/* Budget fraction for each subsystem (must sum to 100) */
const uint32_t wl_mem_subsys_pct[WL_MEM_SUBSYS_COUNT] = {
    50, /* RELATION    */
    20, /* ARENA       */
    10, /* CACHE       */
    10, /* ARRANGEMENT */
    10, /* TIMESTAMP   */
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
    uint64_t subsys_new
        = atomic_fetch_add_explicit(&ledger->subsys_bytes[subsys], bytes,
                                    memory_order_relaxed)
          + bytes;
    update_peak(&ledger->subsys_peak[subsys], subsys_new);

    /* Update total counter */
    uint64_t total_new = atomic_fetch_add_explicit(&ledger->current_bytes,
                                                   bytes, memory_order_relaxed)
                         + bytes;
    update_peak(&ledger->peak_bytes, total_new);
}

void
wl_mem_ledger_free(wl_mem_ledger_t *ledger, int subsys, uint64_t bytes)
{
    if (!ledger || bytes == 0)
        return;
    if (subsys < 0 || subsys >= WL_MEM_SUBSYS_COUNT)
        return;

    /* Clamp-subtract subsystem with CAS loop: avoids TOCTOU race between
     * load and subtract under concurrent K-fusion worker teardown.
     * Without CAS, two concurrent frees could both read the same old_s,
     * both decide sub_s = bytes, and both subtract, causing underflow. */
    {
        uint64_t old_s = atomic_load_explicit(&ledger->subsys_bytes[subsys],
                                              memory_order_relaxed);
        uint64_t new_s;
        do {
            new_s = (bytes > old_s) ? 0 : old_s - bytes;
        } while (!atomic_compare_exchange_weak_explicit(
            &ledger->subsys_bytes[subsys], &old_s, new_s, memory_order_relaxed,
            memory_order_relaxed));
    }

    /* Clamp-subtract total with CAS loop (same race fix) */
    {
        uint64_t old_t = atomic_load_explicit(&ledger->current_bytes,
                                              memory_order_relaxed);
        uint64_t new_t;
        do {
            new_t = (bytes > old_t) ? 0 : old_t - bytes;
        } while (!atomic_compare_exchange_weak_explicit(
            &ledger->current_bytes, &old_t, new_t, memory_order_relaxed,
            memory_order_relaxed));
    }
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
    uint64_t cap = (budget * wl_mem_subsys_pct[subsys]) / 100;
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
    uint64_t cap = (budget * wl_mem_subsys_pct[subsys]) / 100;
    if (cap == 0)
        return false;
    uint64_t current = atomic_load_explicit(&ledger->subsys_bytes[subsys],
                                            memory_order_relaxed);
    /* current >= cap * threshold_pct / 100 */
    return current >= (cap * threshold_pct) / 100;
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
wl_mem_ledger_report(const wl_mem_ledger_t *ledger)
{
    if (!ledger)
        return;

    uint64_t budget
        = atomic_load_explicit(&ledger->total_budget, memory_order_relaxed);
    uint64_t current
        = atomic_load_explicit(&ledger->current_bytes, memory_order_relaxed);
    uint64_t peak
        = atomic_load_explicit(&ledger->peak_bytes, memory_order_relaxed);

    char b1[32], b2[32], b3[32], b4[32];
    fprintf(stderr, "[wirelog mem] budget=%s current=%s peak=%s\n",
            budget == 0 ? "unlimited" : fmt_bytes(budget, b1, sizeof(b1)),
            fmt_bytes(current, b2, sizeof(b2)),
            fmt_bytes(peak, b3, sizeof(b3)));

    for (int i = 0; i < WL_MEM_SUBSYS_COUNT; i++) {
        uint64_t sc = atomic_load_explicit(&ledger->subsys_bytes[i],
                                           memory_order_relaxed);
        uint64_t sp = atomic_load_explicit(&ledger->subsys_peak[i],
                                           memory_order_relaxed);
        uint64_t cap = (budget > 0) ? (budget * wl_mem_subsys_pct[i]) / 100 : 0;
        fprintf(stderr, "  %-12s current=%-10s peak=%-10s cap=%s\n",
                wl_mem_subsys_names[i], fmt_bytes(sc, b1, sizeof(b1)),
                fmt_bytes(sp, b2, sizeof(b2)),
                cap > 0 ? fmt_bytes(cap, b3, sizeof(b3))
                        : fmt_bytes(0, b4, sizeof(b4)));
    }
}
