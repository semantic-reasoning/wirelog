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
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

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
    /* Optional descriptor gate acquired before owner. Relation readers keep
     * both gates so alias metadata cannot be replaced while in use. */
    wl_columnar_source_access_gate_t *secondary_owner;
    uintptr_t identity;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid;
    bool transferable;
    /* 0: ordinary/full token, 1: descriptor pinned while storage admission
     * is pending. */
    uint8_t acquire_stage;
    uint8_t release_stage;
} wl_columnar_source_access_reader_t;

typedef struct wl_columnar_source_access_writer {
    wl_columnar_source_access_gate_t *owner;
    /* Optional relation-descriptor reader held before owner admission. */
    wl_columnar_source_access_gate_t *secondary_owner;
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

/* A resumable two-gate lease for private relation growth. Unlike the general
 * reader/writer helpers below, each advance performs at most one strong CAS.
 * The record is address- and thread-bound and must stay alive until DONE. */
typedef enum wl_columnar_source_access_growth_gate_phase {
    WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_INERT = 0,
    WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_ACQUIRE_SOURCE,
    WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_ACQUIRE_DESTINATION,
    WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_HELD,
    WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_DESTINATION,
    WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_SOURCE,
    WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_DONE
} wl_columnar_source_access_growth_gate_phase_t;

typedef struct wl_columnar_source_access_growth_gate_attempt {
    struct wl_columnar_source_access_growth_gate_attempt *identity;
    wl_columnar_source_access_gate_t *source;
    wl_columnar_source_access_gate_t *destination;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    wl_columnar_source_access_growth_gate_phase_t phase;
    bool thread_valid;
    bool source_held;
    bool destination_held;
} wl_columnar_source_access_growth_gate_attempt_t;

/* A bounded, address-bound relation lease. The receipt must be zero-initialized
 * and remain alive at its original address on its creating thread until
 * RELEASE reaches DONE. The first descriptor_count gates are descriptor
 * gates sorted by address; the remaining gates are storage gates sorted by
 * address. This matches relation mutation order: all descriptors before any
 * storage owner. Each advance performs at most one strong CAS. Partial
 * acquisitions remain owned across retries; callers enter RELEASE exactly
 * once on stop/abort and drain one gate per advance before discarding it. */
#define WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_MAX 4u
typedef enum wl_columnar_source_access_gate_mode {
    WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER = 1,
    WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER = 2
} wl_columnar_source_access_gate_mode_t;

typedef enum wl_columnar_source_access_gate_set_phase {
    WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_INERT = 0,
    WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_ACQUIRE,
    WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_HELD,
    WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_RELEASE,
    WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_DONE
} wl_columnar_source_access_gate_set_phase_t;

typedef struct wl_columnar_source_access_gate_set_attempt {
    struct wl_columnar_source_access_gate_set_attempt *identity;
    wl_columnar_source_access_gate_t *gates[
        WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_MAX];
    wl_columnar_source_access_gate_mode_t modes[
        WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_MAX];
    size_t count;
    size_t descriptor_count;
    size_t cursor;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    wl_columnar_source_access_gate_set_phase_t phase;
    bool thread_valid;
} wl_columnar_source_access_gate_set_attempt_t;

#define WL_COLUMNAR_SOURCE_ACCESS_GATE_READER \
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER
#define WL_COLUMNAR_SOURCE_ACCESS_GATE_WRITER \
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER

typedef struct wl_columnar_source_access_gate_request {
    wl_columnar_source_access_gate_t *gate;
    wl_columnar_source_access_gate_mode_t mode;
} wl_columnar_source_access_gate_request_t;

#define WL_COLUMNAR_SOURCE_ACCESS_GATE_ROUND_MAX 4u

/* Address-bound, thread-affine receipt for one all-or-release round.  The
 * caller must initialize it as {0} before its first acquire and keep this
 * record and every gate at a stable address while held. */
typedef struct wl_columnar_source_access_gate_round {
    uintptr_t identity;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid;
    size_t request_count;
    size_t acquired_count;
    wl_columnar_source_access_gate_request_t requests
    [WL_COLUMNAR_SOURCE_ACCESS_GATE_ROUND_MAX];
} wl_columnar_source_access_gate_round_t;

static inline bool
wl_columnar_source_access_gate_set_thread_equal(
    const wl_columnar_source_access_gate_set_attempt_t *attempt)
{
#if defined(WL_HAVE_C11_THREADS)
    return attempt->thread_valid
           && thrd_equal(attempt->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return attempt->thread_valid
           && attempt->owner_thread == GetCurrentThreadId();
#else
    return attempt->thread_valid
           && pthread_equal(attempt->owner_thread, pthread_self()) != 0;
#endif
}

static inline bool
wl_columnar_source_access_gate_set_shape_valid(
    const wl_columnar_source_access_gate_set_attempt_t *attempt)
{
    if (!attempt || attempt->identity != attempt || !attempt->count
        || attempt->count > WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_MAX
        || !attempt->descriptor_count
        || attempt->descriptor_count >= attempt->count
        || attempt->cursor > attempt->count)
        return false;
    for (size_t i = 0; i < attempt->count; i++) {
        if (!attempt->gates[i]
            || (attempt->modes[i]
            != WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER
            && attempt->modes[i]
            != WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER)
            || ((i > 0 && i < attempt->descriptor_count)
            && (uintptr_t)attempt->gates[i - 1]
            >= (uintptr_t)attempt->gates[i])
            || (i > attempt->descriptor_count
            && (uintptr_t)attempt->gates[i - 1]
            >= (uintptr_t)attempt->gates[i]))
            return false;
        for (size_t j = 0; j < i; j++)
            if (attempt->gates[j] == attempt->gates[i])
                return false;
    }
    switch (attempt->phase) {
    case WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_ACQUIRE:
        return attempt->cursor < attempt->count;
    case WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_HELD:
        return attempt->cursor == attempt->count;
    case WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_RELEASE:
        return true;
    case WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_DONE:
        return attempt->cursor == 0;
    default:
        return false;
    }
}

static inline int
wl_columnar_source_access_gate_set_attempt_begin(
    wl_columnar_source_access_gate_set_attempt_t *attempt,
    wl_columnar_source_access_gate_t *const *gates,
    const wl_columnar_source_access_gate_mode_t *modes, size_t count,
    size_t descriptor_count)
{
    if (!attempt || !gates || !modes || count == 0
        || count > WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_MAX
        || descriptor_count == 0 || descriptor_count >= count
        || attempt->identity || attempt->count || attempt->cursor
        || attempt->descriptor_count || attempt->phase
        || attempt->thread_valid)
        return EINVAL;
    for (size_t i = 0; i < count; i++) {
        if (!gates[i]
            || (modes[i] != WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER
            && modes[i] != WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER)
            || ((i > 0 && i < descriptor_count)
            && (uintptr_t)gates[i - 1] >= (uintptr_t)gates[i])
            || (i > descriptor_count
            && (uintptr_t)gates[i - 1] >= (uintptr_t)gates[i]))
            return EINVAL;
        for (size_t j = 0; j < i; j++)
            if (gates[j] == gates[i])
                return EINVAL;
        attempt->gates[i] = gates[i];
        attempt->modes[i] = modes[i];
    }
    attempt->identity = attempt;
    attempt->count = count;
    attempt->descriptor_count = descriptor_count;
#if defined(WL_HAVE_C11_THREADS)
    attempt->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    attempt->owner_thread = GetCurrentThreadId();
#else
    attempt->owner_thread = pthread_self();
#endif
    attempt->thread_valid = true;
    attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_ACQUIRE;
    return 0;
}

/* Returns EAGAIN for one contended gate or after a successful intermediate
 * acquisition/release. Returns 0 when all gates are held or fully released. */
static inline int
wl_columnar_source_access_gate_set_attempt_advance(
    wl_columnar_source_access_gate_set_attempt_t *attempt)
{
    if (!wl_columnar_source_access_gate_set_shape_valid(attempt)
        || !wl_columnar_source_access_gate_set_thread_equal(attempt))
        return EINVAL;
    if (attempt->phase == WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_HELD
        || attempt->phase == WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_DONE)
        return 0;
    if (attempt->phase == WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_ACQUIRE) {
        if (attempt->cursor >= attempt->count)
            return EINVAL;
        wl_columnar_source_access_gate_t *gate =
            attempt->gates[attempt->cursor];
        uint64_t observed = atomic_load_explicit(&gate->state,
                memory_order_acquire);
        uint64_t desired;
        if (attempt->modes[attempt->cursor]
            == WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER) {
            if (observed != 0)
                return EAGAIN;
            desired = WL_COLUMNAR_SOURCE_ACCESS_WRITER;
        } else {
            if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
                return EAGAIN;
            if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u)
                return EOVERFLOW;
            desired = observed + 1u;
        }
        if (!atomic_compare_exchange_strong_explicit(&gate->state, &observed,
            desired, memory_order_acquire, memory_order_relaxed))
            return EAGAIN;
        attempt->cursor++;
        if (attempt->cursor == attempt->count) {
            attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_HELD;
            return 0;
        }
        return EAGAIN;
    }
    if (attempt->phase == WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_RELEASE) {
        if (attempt->cursor == 0) {
            attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_DONE;
            return 0;
        }
        size_t i = attempt->cursor - 1u;
        wl_columnar_source_access_gate_t *gate = attempt->gates[i];
        uint64_t expected = attempt->modes[i]
            == WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER
            ? WL_COLUMNAR_SOURCE_ACCESS_WRITER
            : atomic_load_explicit(&gate->state, memory_order_acquire);
        if (attempt->modes[i]
            == WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER
            && (expected == 0 || expected == WL_COLUMNAR_SOURCE_ACCESS_WRITER))
            return EBUSY;
        if (!atomic_compare_exchange_strong_explicit(&gate->state, &expected,
            attempt->modes[i]
            == WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER
                    ? 0 : expected - 1u,
            memory_order_release, memory_order_relaxed))
            return EAGAIN;
        attempt->cursor--;
        if (attempt->cursor == 0) {
            attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_DONE;
            return 0;
        }
        return EAGAIN;
    }
    return EINVAL;
}

static inline int
wl_columnar_source_access_gate_set_attempt_release(
    wl_columnar_source_access_gate_set_attempt_t *attempt)
{
    if (!wl_columnar_source_access_gate_set_shape_valid(attempt)
        || !wl_columnar_source_access_gate_set_thread_equal(attempt)
        || (attempt->phase != WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_ACQUIRE
        && attempt->phase != WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_HELD))
        return EINVAL;
    attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_RELEASE;
    return 0;
}

static inline bool
wl_columnar_source_access_growth_gate_thread_equal(
    const wl_columnar_source_access_growth_gate_attempt_t *attempt)
{
#if defined(WL_HAVE_C11_THREADS)
    return attempt->thread_valid
           && thrd_equal(attempt->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return attempt->thread_valid
           && attempt->owner_thread == GetCurrentThreadId();
#else
    return attempt->thread_valid
           && pthread_equal(attempt->owner_thread, pthread_self()) != 0;
#endif
}

static inline bool
wl_columnar_source_access_growth_gate_shape_valid(
    const wl_columnar_source_access_growth_gate_attempt_t *attempt)
{
    if (!attempt || !attempt->source || !attempt->destination
        || attempt->source == attempt->destination)
        return false;
    switch (attempt->phase) {
    case WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_ACQUIRE_SOURCE:
        return !attempt->source_held && !attempt->destination_held;
    case WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_ACQUIRE_DESTINATION:
        return attempt->source_held && !attempt->destination_held;
    case WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_HELD:
    case WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_DESTINATION:
        return attempt->source_held && attempt->destination_held;
    case WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_SOURCE:
        return attempt->source_held && !attempt->destination_held;
    case WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_DONE:
        return !attempt->source_held && !attempt->destination_held;
    default:
        return false;
    }
}

static inline int
wl_columnar_source_access_growth_gate_attempt_begin(
    wl_columnar_source_access_growth_gate_attempt_t *attempt,
    wl_columnar_source_access_gate_t *source,
    wl_columnar_source_access_gate_t *destination)
{
    if (!attempt || !source || !destination || source == destination
        || attempt->identity || attempt->source || attempt->destination
        || attempt->phase || attempt->thread_valid || attempt->source_held
        || attempt->destination_held)
        return EINVAL;
    attempt->identity = attempt;
    attempt->source = source;
    attempt->destination = destination;
#if defined(WL_HAVE_C11_THREADS)
    attempt->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    attempt->owner_thread = GetCurrentThreadId();
#else
    attempt->owner_thread = pthread_self();
#endif
    attempt->thread_valid = true;
    attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_ACQUIRE_SOURCE;
    return 0;
}

/* EAGAIN means the single gate transition raced or is temporarily blocked;
 * retry this same address-bound attempt. No call spins or decrements a gate
 * more than once. */
static inline int
wl_columnar_source_access_growth_gate_attempt_advance(
    wl_columnar_source_access_growth_gate_attempt_t *attempt)
{
    if (!attempt || attempt->identity != attempt
        || !wl_columnar_source_access_growth_gate_shape_valid(attempt)
        || !wl_columnar_source_access_growth_gate_thread_equal(attempt))
        return EINVAL;
    if (attempt->phase == WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_DONE
        || attempt->phase == WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_HELD)
        return 0;
    if (attempt->phase
        == WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_ACQUIRE_SOURCE) {
        uint64_t observed = atomic_load_explicit(&attempt->source->state,
                memory_order_acquire);
        if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
            return EAGAIN;
        if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u)
            return EOVERFLOW;
        uint64_t expected = observed;
        if (!atomic_compare_exchange_strong_explicit(&attempt->source->state,
            &expected, observed + 1u, memory_order_acquire,
            memory_order_relaxed))
            return EAGAIN;
        attempt->source_held = true;
        attempt->phase =
            WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_ACQUIRE_DESTINATION;
        return EAGAIN;
    }
    if (attempt->phase
        == WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_ACQUIRE_DESTINATION) {
        uint64_t expected = 0;
        if (!atomic_compare_exchange_strong_explicit(
                &attempt->destination->state, &expected,
                WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_acquire,
                memory_order_relaxed))
            return EAGAIN;
        attempt->destination_held = true;
        attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_HELD;
        return 0;
    }
    if (attempt->phase
        == WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_DESTINATION) {
        uint64_t expected = WL_COLUMNAR_SOURCE_ACCESS_WRITER;
        if (!attempt->destination_held
            || !atomic_compare_exchange_strong_explicit(
                &attempt->destination->state, &expected, 0,
                memory_order_release,
                memory_order_relaxed))
            return EAGAIN;
        attempt->destination_held = false;
        attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_SOURCE;
        return EAGAIN;
    }
    if (attempt->phase
        == WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_SOURCE) {
        uint64_t observed;
        if (!attempt->source_held)
            return EINVAL;
        observed = atomic_load_explicit(&attempt->source->state,
                memory_order_acquire);
        if (observed == 0 || observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
            return EAGAIN;
        uint64_t expected = observed;
        if (!atomic_compare_exchange_strong_explicit(&attempt->source->state,
            &expected, observed - 1u, memory_order_release,
            memory_order_relaxed))
            return EAGAIN;
        attempt->source_held = false;
        attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_DONE;
        return 0;
    }
    return EINVAL;
}

/* Begin reverse retirement. It is safe after partial acquisition or release;
 * each successful gate decrement is recorded before the next call. */
static inline int
wl_columnar_source_access_growth_gate_attempt_release(
    wl_columnar_source_access_growth_gate_attempt_t *attempt)
{
    if (!attempt || attempt->identity != attempt
        || !wl_columnar_source_access_growth_gate_shape_valid(attempt)
        || !wl_columnar_source_access_growth_gate_thread_equal(attempt))
        return EINVAL;
    if (attempt->phase == WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_DONE)
        return 0;
    if (attempt->destination_held)
        attempt->phase =
            WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_DESTINATION;
    else if (attempt->source_held)
        attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_SOURCE;
    else
        attempt->phase = WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_DONE;
    return 0;
}
typedef struct wl_columnar_source_access_cohort_entry {
    wl_columnar_source_access_gate_t *gate;
    uint64_t readers;
} wl_columnar_source_access_cohort_entry_t;

/* The caller keeps reader and entry arrays stable and externally serializes
 * their lifecycle. Suspended readers retain both gate references. */
typedef struct wl_columnar_source_access_cohort {
    uintptr_t identity;
    wl_columnar_source_access_gate_t *source;
    wl_columnar_source_access_reader_t *const *reader_refs;
    wl_columnar_source_access_cohort_entry_t *entries;
    size_t reader_count;
    size_t entry_count;
} wl_columnar_source_access_cohort_t;

static inline void
wl_columnar_source_access_gate_init(wl_columnar_source_access_gate_t *gate)
{
    if (gate)
        atomic_store_explicit(&gate->state, 0, memory_order_relaxed);
}

static inline int
wl_columnar_source_access_gate_reader_acquire(
    wl_columnar_source_access_gate_t *gate)
{
    uint64_t observed;
    if (!gate)
        return EINVAL;
    observed = atomic_load_explicit(&gate->state, memory_order_acquire);
    for (;;) {
        if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
            return EBUSY;
        if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u)
            return EOVERFLOW;
        if (atomic_compare_exchange_weak_explicit(&gate->state, &observed,
            observed + 1u, memory_order_acquire, memory_order_relaxed))
            return 0;
    }
}

/* Attempt exactly one reader-gate CAS. Contention is resumable EAGAIN. */
static inline int
wl_columnar_source_access_gate_reader_acquire_step(
    wl_columnar_source_access_gate_t *gate, bool *acquired)
{
    uint64_t observed;
    if (!acquired)
        return EINVAL;
    *acquired = false;
    if (!gate)
        return EINVAL;
    observed = atomic_load_explicit(&gate->state, memory_order_acquire);
    if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
        return EAGAIN;
    if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u)
        return EOVERFLOW;
    if (!atomic_compare_exchange_strong_explicit(&gate->state, &observed,
        observed + 1u, memory_order_acquire, memory_order_relaxed))
        return EAGAIN;
    *acquired = true;
    return 0;
}

static inline int
wl_columnar_source_access_gate_reader_release(
    wl_columnar_source_access_gate_t *gate)
{
    uint64_t observed;
    if (!gate)
        return EINVAL;
    observed = atomic_load_explicit(&gate->state, memory_order_acquire);
    for (;;) {
        if (observed == 0 || observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
            return EINVAL;
        if (atomic_compare_exchange_weak_explicit(&gate->state, &observed,
            observed - 1u, memory_order_release, memory_order_relaxed))
            return 0;
    }
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

static inline bool
wl_columnar_source_access_gate_round_thread_equal(
    const wl_columnar_source_access_gate_round_t *round)
{
#if defined(WL_HAVE_C11_THREADS)
    return round->thread_valid
           && thrd_equal(round->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return round->thread_valid
           && round->owner_thread == GetCurrentThreadId();
#else
    return round->thread_valid
           && pthread_equal(round->owner_thread, pthread_self()) != 0;
#endif
}

static inline void
wl_columnar_source_access_gate_round_clear(
    wl_columnar_source_access_gate_round_t *round)
{
    round->identity = 0;
    round->thread_valid = false;
    round->request_count = 0;
    round->acquired_count = 0;
    for (size_t i = 0; i < WL_COLUMNAR_SOURCE_ACCESS_GATE_ROUND_MAX; i++) {
        round->requests[i].gate = NULL;
        round->requests[i].mode = 0;
    }
}

static inline int
wl_columnar_source_access_gate_round_validate(
    const wl_columnar_source_access_gate_round_t *round)
{
    if (!round || round->identity != (uintptr_t)round
        || !wl_columnar_source_access_gate_round_thread_equal(round)
        || !round->request_count
        || round->request_count > WL_COLUMNAR_SOURCE_ACCESS_GATE_ROUND_MAX
        || round->acquired_count > round->request_count)
        return EINVAL;
    return 0;
}

/* Drop exactly the holds recorded in the acquired prefix.  A reader hold
 * prevents the count from reaching zero or WRITER before its atomic decrement;
 * an exclusive writer hold prevents any peer from changing the sentinel. */
static inline void
wl_columnar_source_access_gate_round_unwind(
    wl_columnar_source_access_gate_round_t *round)
{
    if (wl_columnar_source_access_gate_round_validate(round) != 0)
        abort();
    while (round->acquired_count) {
        size_t index = --round->acquired_count;
        wl_columnar_source_access_gate_request_t *request
            = &round->requests[index];
        uint64_t state = atomic_load_explicit(&request->gate->state,
                memory_order_acquire);
        if (request->mode == WL_COLUMNAR_SOURCE_ACCESS_GATE_READER) {
            if (state == 0 || state == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
                abort();
            uint64_t prior = (uint64_t)atomic_fetch_sub_explicit(
                &request->gate->state, 1, memory_order_release);
            if (prior == 0 || prior == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
                abort();
        } else {
            if (request->mode != WL_COLUMNAR_SOURCE_ACCESS_GATE_WRITER
                || state != WL_COLUMNAR_SOURCE_ACCESS_WRITER)
                abort();
            atomic_store_explicit(&request->gate->state, 0,
                memory_order_release);
        }
    }
}

/* Attempt a fixed-size set exactly once.  `round` must be zero-initialized or
 * have been cleared by a previous failed acquire or successful release.
 * Requests before descriptor_count
 * are descriptor gates; remaining requests are storage gates.  The function
 * sorts each group by uintptr_t gate address and pairs every mode with its gate.
 * EAGAIN/EOVERFLOW returns only after every acquired hold has been reversed. */
static inline int
wl_columnar_source_access_gate_round_acquire(
    wl_columnar_source_access_gate_round_t *round,
    const wl_columnar_source_access_gate_request_t *requests,
    size_t descriptor_count, size_t storage_count)
{
    size_t count;
    if (!round || !requests
        || descriptor_count > WL_COLUMNAR_SOURCE_ACCESS_GATE_ROUND_MAX
        || storage_count > WL_COLUMNAR_SOURCE_ACCESS_GATE_ROUND_MAX
        || descriptor_count > WL_COLUMNAR_SOURCE_ACCESS_GATE_ROUND_MAX
        - storage_count
        || descriptor_count + storage_count == 0
        || round->identity || round->thread_valid || round->request_count
        || round->acquired_count)
        return EINVAL;
    count = descriptor_count + storage_count;
    for (size_t i = 0; i < count; i++) {
        if (!requests[i].gate
            || (requests[i].mode != WL_COLUMNAR_SOURCE_ACCESS_GATE_READER
            && requests[i].mode != WL_COLUMNAR_SOURCE_ACCESS_GATE_WRITER))
            return EINVAL;
        for (size_t j = 0; j < i; j++)
            if (requests[i].gate == requests[j].gate)
                return EINVAL;
    }
    round->identity = (uintptr_t)round;
    round->owner_thread =
#if defined(WL_HAVE_C11_THREADS)
        thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
        GetCurrentThreadId();
#else
        pthread_self();
#endif
    round->thread_valid = true;
    round->request_count = count;
    for (size_t i = 0; i < count; i++)
        round->requests[i] = requests[i];
    /* Four-entry insertion sort, separately preserving descriptor priority. */
    for (size_t start = 0; start < count; ) {
        size_t end = start < descriptor_count ? descriptor_count : count;
        for (size_t i = start + 1; i < end; i++) {
            wl_columnar_source_access_gate_request_t item
                = round->requests[i];
            size_t j = i;
            while (j > start
                && (uintptr_t)round->requests[j - 1].gate
                > (uintptr_t)item.gate) {
                round->requests[j] = round->requests[j - 1];
                j--;
            }
            round->requests[j] = item;
        }
        start = end;
    }
    for (size_t i = 0; i < count; i++) {
        wl_columnar_source_access_gate_request_t *request
            = &round->requests[i];
        uint64_t observed;
        if (request->mode == WL_COLUMNAR_SOURCE_ACCESS_GATE_READER) {
            observed = atomic_load_explicit(&request->gate->state,
                    memory_order_acquire);
            if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER) {
                wl_columnar_source_access_gate_round_unwind(round);
                wl_columnar_source_access_gate_round_clear(round);
                return EAGAIN;
            }
            if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u) {
                wl_columnar_source_access_gate_round_unwind(round);
                wl_columnar_source_access_gate_round_clear(round);
                return EOVERFLOW;
            }
            if (!atomic_compare_exchange_strong_explicit(&request->gate->state,
                &observed, observed + 1u, memory_order_acquire,
                memory_order_relaxed)) {
                wl_columnar_source_access_gate_round_unwind(round);
                wl_columnar_source_access_gate_round_clear(round);
                return observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u
                    ? EOVERFLOW : EAGAIN;
            }
        } else {
            observed = 0;
            if (!atomic_compare_exchange_strong_explicit(&request->gate->state,
                &observed, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
                memory_order_acquire, memory_order_relaxed)) {
                wl_columnar_source_access_gate_round_unwind(round);
                wl_columnar_source_access_gate_round_clear(round);
                return EAGAIN;
            }
        }
        round->acquired_count++;
    }
    return 0;
}

static inline int
wl_columnar_source_access_gate_round_release(
    wl_columnar_source_access_gate_round_t *round)
{
    if (wl_columnar_source_access_gate_round_validate(round) != 0
        || round->acquired_count != round->request_count)
        return EINVAL;
    wl_columnar_source_access_gate_round_unwind(round);
    wl_columnar_source_access_gate_round_clear(round);
    return 0;
}

static inline int
wl_columnar_source_access_cohort_exchange(
    wl_columnar_source_access_gate_t *source,
    wl_columnar_source_access_reader_t *const *readers, size_t count,
    wl_columnar_source_access_cohort_entry_t *entries, size_t capacity,
    wl_columnar_source_access_cohort_t *cohort)
{
    uint64_t expected;
    size_t unique = 0;
    if (!source || !readers || !entries || !count || !cohort
        || cohort->identity || cohort->source || cohort->reader_refs
        || cohort->entries || cohort->reader_count || cohort->entry_count
        || count >= WL_COLUMNAR_SOURCE_ACCESS_WRITER)
        return EINVAL;
    for (size_t i = 0; i < count; i++) {
        const wl_columnar_source_access_reader_t *reader = readers[i];
        if (!reader || reader->identity != (uintptr_t)reader
            || reader->owner != source || !reader->transferable
            || reader->thread_valid || !reader->secondary_owner
            || reader->secondary_owner == source)
            return EINVAL;
        for (size_t j = 0; j < i; j++)
            if (readers[j] == reader)
                return EINVAL;
    }
    for (size_t i = 0; i < count; i++) {
        size_t j = 0;
        while (j < i
            && readers[j]->secondary_owner != readers[i]->secondary_owner)
            j++;
        if (j == i)
            unique++;
    }
    if (capacity < unique)
        return EINVAL;
    unique = 0;
    /* Source admission is first. Until it is restored, an ordinary release
     * sees WRITER and cannot decrement either gate, even if a descriptor
     * exchange fails partway through this loop. */
    do {
        expected = (uint64_t)count;
        if (atomic_compare_exchange_weak_explicit(&source->state, &expected,
            WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_acquire,
            memory_order_relaxed))
            break;
        if (expected != count)
            return EBUSY;
    } while (true);
    for (size_t i = 0; i < count; i++) {
        wl_columnar_source_access_gate_t *gate
            = readers[i]->secondary_owner;
        size_t j = 0;
        while (j < unique && entries[j].gate != gate)
            j++;
        if (j == unique) {
            entries[unique].gate = gate;
            entries[unique].readers = 1;
            unique++;
        } else {
            entries[j].readers++;
        }
    }
    for (size_t i = 0; i < unique; i++) {
        do {
            expected = entries[i].readers;
            if (atomic_compare_exchange_weak_explicit(&entries[i].gate->state,
                &expected, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
                memory_order_acquire, memory_order_relaxed))
                break;
            if (expected != entries[i].readers) {
                while (i > 0) {
                    i--;
                    atomic_store_explicit(&entries[i].gate->state,
                        entries[i].readers, memory_order_release);
                }
                atomic_store_explicit(&source->state, (uint64_t)count,
                    memory_order_release);
                return EBUSY;
            }
        } while (true);
    }
    cohort->identity = (uintptr_t)cohort;
    cohort->source = source;
    cohort->reader_refs = readers;
    cohort->entries = entries;
    cohort->reader_count = count;
    cohort->entry_count = unique;
    return 0;
}

/* Validation is a snapshot only. The caller must externally serialize the
 * cohort, reader/entry arrays, and gate lifecycle through restoration; this
 * call does not reserve their state or prepare an atomic commit. */
static inline int
wl_columnar_source_access_cohort_validate(
    const wl_columnar_source_access_cohort_t *cohort)
{
    if (!cohort || cohort->identity != (uintptr_t)cohort
        || !cohort->source || !cohort->reader_refs || !cohort->entries
        || !cohort->reader_count || !cohort->entry_count
        || cohort->entry_count > cohort->reader_count)
        return EINVAL;
    for (size_t i = 0; i < cohort->reader_count; i++) {
        const wl_columnar_source_access_reader_t *reader
            = cohort->reader_refs[i];
        bool found = false;
        if (!reader || reader->identity != (uintptr_t)reader
            || reader->owner != cohort->source || !reader->transferable
            || reader->thread_valid || !reader->secondary_owner
            || reader->secondary_owner == cohort->source
            || reader->release_stage != 0)
            return EINVAL;
        for (size_t j = 0; j < i; j++)
            if (cohort->reader_refs[j] == reader)
                return EINVAL;
        for (size_t j = 0; j < cohort->entry_count; j++)
            if (cohort->entries[j].gate == reader->secondary_owner)
                found = true;
        if (!found)
            return EINVAL;
    }
    for (size_t i = 0; i < cohort->entry_count; i++) {
        size_t count = 0;
        if (!cohort->entries[i].gate
            || cohort->entries[i].gate == cohort->source)
            return EINVAL;
        for (size_t j = 0; j < i; j++)
            if (cohort->entries[j].gate == cohort->entries[i].gate)
                return EINVAL;
        for (size_t j = 0; j < cohort->reader_count; j++)
            if (cohort->reader_refs[j]->secondary_owner
                == cohort->entries[i].gate)
                count++;
        if (count != cohort->entries[i].readers)
            return EINVAL;
    }
    if (atomic_load_explicit(&cohort->source->state, memory_order_acquire)
        != WL_COLUMNAR_SOURCE_ACCESS_WRITER)
        return EBUSY;
    for (size_t i = 0; i < cohort->entry_count; i++)
        if (!cohort->entries[i].gate || !cohort->entries[i].readers
            || atomic_load_explicit(&cohort->entries[i].gate->state,
            memory_order_acquire) != WL_COLUMNAR_SOURCE_ACCESS_WRITER)
            return EBUSY;
    return 0;
}

static inline int
wl_columnar_source_access_cohort_restore(
    wl_columnar_source_access_cohort_t *cohort)
{
    uint64_t expected;
    int rc = wl_columnar_source_access_cohort_validate(cohort);
    if (rc != 0)
        return rc;
    for (size_t i = 0; i < cohort->entry_count; i++) {
        do {
            expected = WL_COLUMNAR_SOURCE_ACCESS_WRITER;
        } while (!atomic_compare_exchange_weak_explicit(
                &cohort->entries[i].gate->state, &expected,
                cohort->entries[i].readers, memory_order_release,
                memory_order_relaxed));
    }
    do {
        expected = WL_COLUMNAR_SOURCE_ACCESS_WRITER;
    } while (!atomic_compare_exchange_weak_explicit(&cohort->source->state,
        &expected, (uint64_t)cohort->reader_count, memory_order_release,
        memory_order_relaxed));
    cohort->identity = 0;
    cohort->source = NULL;
    cohort->reader_refs = NULL;
    cohort->entries = NULL;
    cohort->reader_count = 0;
    cohort->entry_count = 0;
    return 0;
}

static inline void
wl_columnar_source_access_reader_initialize(
    wl_columnar_source_access_gate_t *owner_gate,
    wl_columnar_source_access_reader_t *token, bool transferable)
{
    token->owner = owner_gate;
    token->secondary_owner = NULL;
    token->identity = (uintptr_t)token;
    token->acquire_stage = 0;
    token->release_stage = 0;
    if (transferable) {
        token->thread_valid = false;
    } else {
#if defined(WL_HAVE_C11_THREADS)
        token->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
        token->owner_thread = GetCurrentThreadId();
#else
        token->owner_thread = pthread_self();
#endif
        token->thread_valid = true;
    }
    token->transferable = transferable;
}

static inline int
wl_columnar_source_access_reader_acquire_common(
    wl_columnar_source_access_gate_t *owner_gate,
    wl_columnar_source_access_reader_t *token, bool transferable)
{
    int rc;

    if (!owner_gate || !token || token->owner || token->secondary_owner
        || token->identity != 0 || token->thread_valid || token->transferable
        || token->acquire_stage != 0 || token->release_stage != 0)
        return EINVAL;
    rc = wl_columnar_source_access_gate_reader_acquire(owner_gate);
    if (rc != 0)
        return rc;
    wl_columnar_source_access_reader_initialize(owner_gate, token,
        transferable);
    return 0;
}

static inline int
wl_columnar_source_access_reader_acquire(
    wl_columnar_source_access_gate_t *gate,
    wl_columnar_source_access_reader_t *token)
{
    return wl_columnar_source_access_reader_acquire_common(gate, token,
               false);
}

/* Session-owned readers have stable token addresses but may be retired by a
 * later, externally serialized teardown thread.  Identity validation still
 * rejects copied tokens; the atomic gate makes sequential cross-thread
 * transfer safe without weakening ordinary operation-token affinity. */
static inline int
wl_columnar_source_access_reader_acquire_transferable(
    wl_columnar_source_access_gate_t *gate,
    wl_columnar_source_access_reader_t *token)
{
    return wl_columnar_source_access_reader_acquire_common(gate, token, true);
}

static inline void
wl_columnar_source_access_reader_clear(
    wl_columnar_source_access_reader_t *token)
{
    token->owner = NULL;
    token->secondary_owner = NULL;
    token->identity = 0;
    token->thread_valid = false;
    token->transferable = false;
    token->acquire_stage = 0;
    token->release_stage = 0;
}

/* Perform at most one source-gate reference decrement. Contention returns
 * EAGAIN with the token retaining its remaining release stage. */
static inline int
wl_columnar_source_access_reader_release_step(
    wl_columnar_source_access_reader_t *token, bool *done)
{
    wl_columnar_source_access_gate_t *gate;
    uint64_t observed;
    if (!done)
        return EINVAL;
    *done = false;
    if (!token || token->identity != (uintptr_t)token || !token->owner
        || token->release_stage > 1 || token->acquire_stage > 1
        || (token->acquire_stage == 1
        && (token->secondary_owner || token->release_stage != 0))
        || (!token->transferable
        && !wl_columnar_source_access_reader_thread_equal(token))
        || (token->transferable && token->thread_valid))
        return EINVAL;
    gate = token->release_stage == 0 ? token->owner
        : token->secondary_owner;
    if (!gate)
        return EINVAL;
    observed = atomic_load_explicit(&gate->state, memory_order_acquire);
    if (observed == 0 || observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
        return EINVAL;
    if (token->release_stage == 0 && token->secondary_owner
        && !wl_columnar_source_access_gate_busy(token->secondary_owner))
        return EINVAL;
    if (!atomic_compare_exchange_strong_explicit(&gate->state, &observed,
        observed - 1u, memory_order_release, memory_order_relaxed))
        return EAGAIN;
    if (token->release_stage == 0 && token->secondary_owner) {
        token->release_stage = 1;
        return 0;
    }
    wl_columnar_source_access_reader_clear(token);
    *done = true;
    return 0;
}

static inline int
wl_columnar_source_access_reader_release(
    wl_columnar_source_access_reader_t *token)
{
    bool done = false;
    int rc;
    do {
        rc = wl_columnar_source_access_reader_release_step(token, &done);
    } while (rc == EAGAIN || (!rc && !done));
    return rc;
}

static inline void
wl_columnar_source_access_writer_initialize(
    wl_columnar_source_access_gate_t *gate,
    wl_columnar_source_access_writer_t *token)
{
    token->owner = gate;
    token->secondary_owner = NULL;
    token->identity = (uintptr_t)token;
#if defined(WL_HAVE_C11_THREADS)
    token->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    token->owner_thread = GetCurrentThreadId();
#else
    token->owner_thread = pthread_self();
#endif
    token->thread_valid = true;
}

static inline int
wl_columnar_source_access_writer_acquire(
    wl_columnar_source_access_gate_t *gate,
    wl_columnar_source_access_writer_t *token)
{
    uint64_t expected;
    if (!gate || !token || token->owner || token->secondary_owner
        || token->identity != 0 || token->thread_valid)
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
    wl_columnar_source_access_writer_initialize(gate, token);
    return 0;
}

/* Validate exclusive authority without consuming it. */
static inline int
wl_columnar_source_access_writer_validate(
    const wl_columnar_source_access_writer_t *token,
    const wl_columnar_source_access_gate_t *gate)
{
    if (!token || !gate || token->identity != (uintptr_t)token
        || token->owner != gate || token->secondary_owner
        || !wl_columnar_source_access_writer_thread_equal(token)
        || atomic_load_explicit(&gate->state, memory_order_acquire)
        != WL_COLUMNAR_SOURCE_ACCESS_WRITER)
        return EINVAL;
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
    if (token->secondary_owner
        && wl_columnar_source_access_gate_reader_release(
            token->secondary_owner) != 0)
        return EINVAL;
    token->owner = NULL;
    token->secondary_owner = NULL;
    token->identity = 0;
    token->thread_valid = false;
    return 0;
}

/* Relocate an exclusive writer without reopening its gate. Tokens are bound
 * to their address, so a plain struct copy cannot transfer ownership. */
static inline int
wl_columnar_source_access_writer_move(
    wl_columnar_source_access_writer_t *dst,
    wl_columnar_source_access_writer_t *src)
{
    if (!dst || !src || dst == src || dst->owner || dst->secondary_owner
        || dst->identity || dst->thread_valid
        || src->identity != (uintptr_t)src || !src->owner
        || !wl_columnar_source_access_writer_thread_equal(src))
        return EINVAL;
    if (atomic_load_explicit(&src->owner->state, memory_order_acquire)
        != WL_COLUMNAR_SOURCE_ACCESS_WRITER)
        return EBUSY;
    *dst = *src;
    dst->identity = (uintptr_t)dst;
    src->owner = NULL;
    src->secondary_owner = NULL;
    src->identity = 0;
    src->thread_valid = false;
    return 0;
}

#endif
