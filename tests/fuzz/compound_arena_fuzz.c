/*
 * compound_arena_fuzz.c - libFuzzer entrypoint for compound arena APIs.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This target exercises bounded single-threaded compound arena operations for
 * Issue #743.  Allocation failures and invalid handles are expected outcomes;
 * the harness only checks that public arena operations remain memory-safe.
 */

#include <stddef.h>
#include <stdint.h>

#include "../wirelog/arena/compound_arena.h"

#define COMPOUND_FUZZ_MAX_BYTES 4096
#define COMPOUND_FUZZ_MAX_OPS 128
#define COMPOUND_FUZZ_MAX_ALLOC 256
#define COMPOUND_FUZZ_MAX_HANDLES 32
#define COMPOUND_FUZZ_TOUCH_LIMIT 64

typedef struct {
    const uint8_t *data;
    size_t size;
    size_t pos;
} compound_fuzz_reader_t;

static uint8_t
read_u8(compound_fuzz_reader_t *r)
{
    if (r->pos >= r->size)
        return 0;
    return r->data[r->pos++];
}

static uint32_t
read_u32(compound_fuzz_reader_t *r)
{
    uint32_t v = 0;
    for (uint32_t i = 0; i < 4; i++)
        v |= (uint32_t)read_u8(r) << (i * 8);
    return v;
}

static uint64_t
read_u64(compound_fuzz_reader_t *r)
{
    uint64_t v = 0;
    for (uint32_t i = 0; i < 8; i++)
        v |= (uint64_t)read_u8(r) << (i * 8);
    return v;
}

static uint64_t
choose_handle(compound_fuzz_reader_t *r, const uint64_t *handles,
    uint32_t handle_count)
{
    uint8_t selector = read_u8(r);
    if (handle_count > 0 && (selector & 1u) != 0)
        return handles[selector % handle_count];
    return read_u64(r);
}

static void
touch_lookup(const wl_compound_arena_t *arena, uint64_t handle)
{
    uint32_t payload_size = 0;
    const uint8_t *payload = (const uint8_t *)wl_compound_arena_lookup(arena,
            handle, &payload_size);
    if (!payload || payload_size == 0)
        return;

    uint32_t limit = payload_size < COMPOUND_FUZZ_TOUCH_LIMIT
        ? payload_size
        : COMPOUND_FUZZ_TOUCH_LIMIT;
    volatile uint8_t sink = 0;
    for (uint32_t i = 0; i < limit; i++)
        sink ^= payload[i];
    (void)sink;
}

static wl_compound_arena_t *
create_bounded_arena(compound_fuzz_reader_t *r)
{
    uint32_t session_seed = read_u32(r);
    uint32_t default_gen_cap = 8u
        + (read_u32(r) % (COMPOUND_FUZZ_MAX_ALLOC * 2u));
    uint32_t max_epochs = 1u + (read_u8(r) % 8u);
    return wl_compound_arena_create(session_seed, default_gen_cap, max_epochs);
}

int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    compound_fuzz_reader_t r = {
        data,
        size < COMPOUND_FUZZ_MAX_BYTES ? size : COMPOUND_FUZZ_MAX_BYTES,
        0,
    };
    uint64_t handles[COMPOUND_FUZZ_MAX_HANDLES];
    uint32_t handle_count = 0;
    uint32_t handle_next = 0;

    wl_compound_arena_free(NULL);
    (void)wl_compound_arena_lookup(NULL, WL_COMPOUND_HANDLE_NULL, NULL);
    (void)wl_compound_arena_multiplicity(NULL, WL_COMPOUND_HANDLE_NULL);
    (void)wl_compound_arena_retain(NULL, WL_COMPOUND_HANDLE_NULL, 1);
    wl_compound_arena_freeze(NULL);
    wl_compound_arena_unfreeze(NULL);
    (void)wl_compound_arena_gc_epoch_boundary(NULL);
    (void)wl_compound_arena_live_handles(NULL);

    wl_compound_arena_t *arena = create_bounded_arena(&r);
    if (!arena)
        return 0;

    uint32_t ops = 1u + (read_u8(&r) % COMPOUND_FUZZ_MAX_OPS);
    for (uint32_t i = 0; i < ops; i++) {
        uint8_t op = read_u8(&r) % 9u;
        switch (op) {
        case 0: {
            uint32_t alloc_size = read_u32(&r) % (COMPOUND_FUZZ_MAX_ALLOC + 1u);
            uint64_t handle = wl_compound_arena_alloc(arena, alloc_size);
            if (handle != WL_COMPOUND_HANDLE_NULL) {
                handles[handle_next] = handle;
                handle_next = (handle_next + 1u) % COMPOUND_FUZZ_MAX_HANDLES;
                if (handle_count < COMPOUND_FUZZ_MAX_HANDLES)
                    handle_count++;
                touch_lookup(arena, handle);
            }
            break;
        }
        case 1: {
            uint64_t handle = choose_handle(&r, handles, handle_count);
            touch_lookup(arena, handle);
            break;
        }
        case 2: {
            uint64_t handle = choose_handle(&r, handles, handle_count);
            volatile int64_t mult = wl_compound_arena_multiplicity(arena,
                    handle);
            (void)mult;
            break;
        }
        case 3: {
            uint64_t handle = choose_handle(&r, handles, handle_count);
            int64_t delta = (int64_t)(read_u8(&r) % 9u) - 4;
            (void)wl_compound_arena_retain(arena, handle, delta);
            break;
        }
        case 4:
            wl_compound_arena_freeze(arena);
            break;
        case 5:
            wl_compound_arena_unfreeze(arena);
            break;
        case 6:
            (void)wl_compound_arena_gc_epoch_boundary(arena);
            break;
        case 7: {
            volatile uint64_t live = wl_compound_arena_live_handles(arena);
            (void)live;
            break;
        }
        case 8:
            wl_compound_arena_free(arena);
            handle_count = 0;
            handle_next = 0;
            arena = create_bounded_arena(&r);
            if (!arena)
                return 0;
            break;
        default:
            break;
        }
    }

    wl_compound_arena_free(arena);
    return 0;
}
