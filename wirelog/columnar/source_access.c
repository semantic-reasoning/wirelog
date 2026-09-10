/*
 * columnar/source_access.c - allocation-free source reader/writer gate
 */

#include "columnar/source_access.h"

#include <errno.h>
#include <limits.h>

#define WL_SOURCE_ACCESS_WRITER UINT64_MAX
#define WL_SOURCE_ACCESS_MAX_READERS (UINT64_MAX - UINT64_C(1))

static int
token_prepare(wl_columnar_source_access_t *gate,
    wl_columnar_source_access_token_t *token)
{
    if (!gate || !token)
        return EINVAL;
    if (token->gate || token->identity || token->mode != 0)
        return EINVAL;
    token->gate = gate;
    token->identity = token;
    return 0;
}

void
wl_columnar_source_access_init(wl_columnar_source_access_t *gate)
{
    if (gate)
        atomic_init(&gate->state, 0);
}

int
wl_columnar_source_access_read_acquire(wl_columnar_source_access_t *gate,
    wl_columnar_source_access_token_t *token)
{
    uint64_t observed;

    if (token_prepare(gate, token) != 0)
        return EINVAL;
    observed = atomic_load_explicit(&gate->state, memory_order_acquire);
    for (;;) {
        if (observed == WL_SOURCE_ACCESS_WRITER)
            goto busy;
        if (observed == WL_SOURCE_ACCESS_MAX_READERS)
            goto overflow;
        if (atomic_compare_exchange_weak_explicit(&gate->state, &observed,
            observed + UINT64_C(1), memory_order_acquire,
            memory_order_relaxed)) {
            token->mode = WL_COLUMNAR_SOURCE_ACCESS_READER;
            return 0;
        }
    }

busy:
    token->gate = NULL;
    token->identity = NULL;
    return EBUSY;
overflow:
    token->gate = NULL;
    token->identity = NULL;
    return EOVERFLOW;
}

int
wl_columnar_source_access_write_acquire(wl_columnar_source_access_t *gate,
    wl_columnar_source_access_token_t *token)
{
    uint64_t expected = 0;

    if (token_prepare(gate, token) != 0)
        return EINVAL;
    if (!atomic_compare_exchange_strong_explicit(&gate->state, &expected,
        WL_SOURCE_ACCESS_WRITER, memory_order_acquire,
        memory_order_relaxed)) {
        token->gate = NULL;
        token->identity = NULL;
        return EBUSY;
    }
    token->mode = WL_COLUMNAR_SOURCE_ACCESS_WRITER;
    return 0;
}

int
wl_columnar_source_access_release(wl_columnar_source_access_token_t *token)
{
    wl_columnar_source_access_t *gate;
    uint64_t expected;

    if (!token || !token->gate || token->identity != token
        || token->mode == 0)
        return EINVAL;
    gate = token->gate;
    if (token->mode == WL_COLUMNAR_SOURCE_ACCESS_WRITER) {
        expected = WL_SOURCE_ACCESS_WRITER;
        if (!atomic_compare_exchange_strong_explicit(&gate->state, &expected,
            0, memory_order_release, memory_order_relaxed))
            return EINVAL;
    } else if (token->mode == WL_COLUMNAR_SOURCE_ACCESS_READER) {
        expected = atomic_load_explicit(&gate->state, memory_order_acquire);
        for (;;) {
            if (expected == 0 || expected == WL_SOURCE_ACCESS_WRITER)
                return EINVAL;
            if (atomic_compare_exchange_weak_explicit(&gate->state, &expected,
                expected - UINT64_C(1), memory_order_release,
                memory_order_relaxed))
                break;
        }
    } else {
        return EINVAL;
    }
    token->gate = NULL;
    token->identity = NULL;
    token->mode = 0;
    return 0;
}
