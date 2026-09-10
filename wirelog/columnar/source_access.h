/*
 * columnar/source_access.h - allocation-free source reader/writer gate
 *
 * Internal relation source-reader/writer gate.  Broader operation-scope and
 * public teardown integration is completed by the subsequent lifecycle units.
 */

#ifndef WL_COLUMNAR_SOURCE_ACCESS_H
#define WL_COLUMNAR_SOURCE_ACCESS_H

#include "columnar/mem_ledger.h"
#include "wirelog/thread.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>

#define WL_COLUMNAR_SOURCE_ACCESS_WRITER UINT64_MAX

typedef struct wl_columnar_source_access_gate {
    wl_atomic_u64 state;
} wl_columnar_source_access_gate_t;

static inline bool
wl_columnar_source_access_gate_busy(
    const wl_columnar_source_access_gate_t *gate)
{
    return gate
           && atomic_load_explicit(&gate->state, memory_order_acquire) != 0;
}

/* Claim the writer state for terminal destruction.  The claim is intentionally
 * not released: the owning relation is destroyed while it is held. */
static inline int
wl_columnar_source_access_writer_claim(
    wl_columnar_source_access_gate_t *gate)
{
    uint64_t expected;
    if (!gate)
        return EINVAL;
    for (;;) {
        expected = 0;
        if (atomic_compare_exchange_weak_explicit(&gate->state, &expected,
            WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_acquire,
            memory_order_relaxed))
            return 0;
        if (expected != 0)
            return EBUSY;
    }
}

typedef struct wl_columnar_source_access_reader {
    wl_columnar_source_access_gate_t *owner;
    uintptr_t identity;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid;
} wl_columnar_source_access_reader_t;

typedef struct wl_columnar_source_access_writer {
    wl_columnar_source_access_gate_t *owner;
    uintptr_t identity;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid;
} wl_columnar_source_access_writer_t;

static inline void
wl_columnar_source_access_gate_init(wl_columnar_source_access_gate_t *gate)
{
    if (gate)
        atomic_store_explicit(&gate->state, 0, memory_order_relaxed);
}

static inline bool
wl_columnar_source_access_reader_thread_equal(
    const wl_columnar_source_access_reader_t *token)
{
#if defined(WL_HAVE_C11_THREADS)
    return token->thread_valid
           && thrd_equal(token->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return token->thread_valid
           && token->owner_thread == GetCurrentThreadId();
#else
    return token->thread_valid
           && pthread_equal(token->owner_thread, pthread_self()) != 0;
#endif
}

static inline bool
wl_columnar_source_access_writer_thread_equal(
    const wl_columnar_source_access_writer_t *token)
{
#if defined(WL_HAVE_C11_THREADS)
    return token->thread_valid
           && thrd_equal(token->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return token->thread_valid
           && token->owner_thread == GetCurrentThreadId();
#else
    return token->thread_valid
           && pthread_equal(token->owner_thread, pthread_self()) != 0;
#endif
}

static inline int
wl_columnar_source_access_reader_acquire(
    wl_columnar_source_access_gate_t *gate,
    wl_columnar_source_access_reader_t *token)
{
    uint64_t observed;
    if (!gate || !token || token->owner || token->identity != 0
        || token->thread_valid)
        return EINVAL;
    observed = atomic_load_explicit(&gate->state, memory_order_acquire);
    for (;;) {
        if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
            return EBUSY;
        if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u)
            return EOVERFLOW;
        if (atomic_compare_exchange_weak_explicit(&gate->state, &observed,
            observed + 1u, memory_order_acquire, memory_order_relaxed))
            break;
    }
    token->owner = gate;
    token->identity = (uintptr_t)token;
#if defined(WL_HAVE_C11_THREADS)
    token->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    token->owner_thread = GetCurrentThreadId();
#else
    token->owner_thread = pthread_self();
#endif
    token->thread_valid = true;
    return 0;
}

static inline int
wl_columnar_source_access_reader_release(
    wl_columnar_source_access_reader_t *token)
{
    wl_columnar_source_access_gate_t *gate;
    uint64_t observed;
    if (!token || token->identity != (uintptr_t)token || !token->owner
        || !wl_columnar_source_access_reader_thread_equal(token))
        return EINVAL;
    gate = token->owner;
    observed = atomic_load_explicit(&gate->state, memory_order_acquire);
    for (;;) {
        if (observed == 0 || observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
            return EINVAL;
        if (atomic_compare_exchange_weak_explicit(&gate->state, &observed,
            observed - 1u, memory_order_release, memory_order_relaxed))
            break;
    }
    token->owner = NULL;
    token->identity = 0;
    token->thread_valid = false;
    return 0;
}

static inline int
wl_columnar_source_access_writer_acquire(
    wl_columnar_source_access_gate_t *gate,
    wl_columnar_source_access_writer_t *token)
{
    uint64_t expected;
    if (!gate || !token || token->owner || token->identity != 0
        || token->thread_valid)
        return EINVAL;
    for (;;) {
        expected = 0;
        if (atomic_compare_exchange_weak_explicit(&gate->state, &expected,
            WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_acquire,
            memory_order_relaxed))
            break;
        if (expected != 0)
            return EBUSY;
    }
    token->owner = gate;
    token->identity = (uintptr_t)token;
#if defined(WL_HAVE_C11_THREADS)
    token->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    token->owner_thread = GetCurrentThreadId();
#else
    token->owner_thread = pthread_self();
#endif
    token->thread_valid = true;
    return 0;
}

static inline int
wl_columnar_source_access_writer_release(
    wl_columnar_source_access_writer_t *token)
{
    wl_columnar_source_access_gate_t *gate;
    uint64_t observed;
    if (!token || token->identity != (uintptr_t)token || !token->owner
        || !wl_columnar_source_access_writer_thread_equal(token))
        return EINVAL;
    gate = token->owner;
    observed = atomic_load_explicit(&gate->state, memory_order_acquire);
    if (observed != WL_COLUMNAR_SOURCE_ACCESS_WRITER)
        return EINVAL;
    for (;;) {
        if (atomic_compare_exchange_weak_explicit(&gate->state, &observed, 0,
            memory_order_release, memory_order_relaxed))
            break;
        if (observed != WL_COLUMNAR_SOURCE_ACCESS_WRITER)
            return EBUSY;
    }
    token->owner = NULL;
    token->identity = 0;
    token->thread_valid = false;
    return 0;
}

#endif
