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

static inline int
wl_columnar_source_access_cohort_restore(
    wl_columnar_source_access_cohort_t *cohort)
{
    uint64_t expected;
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
            || reader->secondary_owner == cohort->source)
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

static inline int
wl_columnar_source_access_reader_acquire_common(
    wl_columnar_source_access_gate_t *owner_gate,
    wl_columnar_source_access_reader_t *token, bool transferable)
{
    int rc;

    if (!owner_gate || !token || token->owner || token->secondary_owner
        || token->identity != 0 || token->thread_valid || token->transferable)
        return EINVAL;
    rc = wl_columnar_source_access_gate_reader_acquire(owner_gate);
    if (rc != 0)
        return rc;
    token->owner = owner_gate;
    token->secondary_owner = NULL;
    token->identity = (uintptr_t)token;
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

static inline int
wl_columnar_source_access_reader_release(
    wl_columnar_source_access_reader_t *token)
{
    wl_columnar_source_access_gate_t *gate;
    uint64_t observed;
    if (!token || token->identity != (uintptr_t)token || !token->owner
        || (!token->transferable
        && !wl_columnar_source_access_reader_thread_equal(token))
        || (token->transferable && token->thread_valid))
        return EINVAL;
    gate = token->owner;
    observed = atomic_load_explicit(&gate->state, memory_order_acquire);
    if (observed == 0 || observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER)
        return EINVAL;
    if (token->secondary_owner
        && !wl_columnar_source_access_gate_busy(token->secondary_owner))
        return EINVAL;
    if (wl_columnar_source_access_gate_reader_release(gate) != 0)
        return EINVAL;
    if (token->secondary_owner
        && wl_columnar_source_access_gate_reader_release(
            token->secondary_owner) != 0)
        return EINVAL;
    token->owner = NULL;
    token->secondary_owner = NULL;
    token->identity = 0;
    token->thread_valid = false;
    token->transferable = false;
    return 0;
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

#endif
