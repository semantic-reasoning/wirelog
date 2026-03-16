/*
 * backend/delta_pool.c - wirelog Delta Pool Allocator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "delta_pool.h"

#include <stdlib.h>
#include <string.h>

/* Alignment for all arena allocations (8 bytes = sizeof(int64_t)). */
#define POOL_ALIGN 8
#define POOL_ALIGN_UP(n) (((n) + (POOL_ALIGN - 1)) & ~(size_t)(POOL_ALIGN - 1))

delta_pool_t *
delta_pool_create(uint32_t max_slots, size_t slot_size, size_t arena_bytes)
{
    if (max_slots == 0 || slot_size == 0 || arena_bytes == 0)
        return NULL;

    delta_pool_t *pool = (delta_pool_t *)malloc(sizeof(delta_pool_t));
    if (!pool)
        return NULL;

    size_t aligned_slot = POOL_ALIGN_UP(slot_size);
    pool->slab = (char *)calloc(max_slots, aligned_slot);
    if (!pool->slab) {
        free(pool);
        return NULL;
    }

    pool->arena = (char *)malloc(arena_bytes);
    if (!pool->arena) {
        free(pool->slab);
        free(pool);
        return NULL;
    }

    pool->slot_size = aligned_slot;
    pool->slot_cap = max_slots;
    pool->slot_used = 0;
    pool->arena_cap = arena_bytes;
    pool->arena_used = 0;
    return pool;
}

void *
delta_pool_alloc_slot(delta_pool_t *pool)
{
    if (!pool || pool->slot_used >= pool->slot_cap)
        return NULL;
    void *ptr = pool->slab + (size_t)pool->slot_used * pool->slot_size;
    memset(ptr, 0, pool->slot_size);
    pool->slot_used++;
    return ptr;
}

void *
delta_pool_alloc_data(delta_pool_t *pool, size_t bytes)
{
    if (!pool || bytes == 0)
        return NULL;

    size_t aligned = POOL_ALIGN_UP(bytes);
    if (pool->arena_used + aligned > pool->arena_cap)
        return NULL;

    void *ptr = pool->arena + pool->arena_used;
    pool->arena_used += aligned;
    return ptr;
}

void
delta_pool_reset(delta_pool_t *pool)
{
    if (!pool)
        return;
    pool->slot_used = 0;
    pool->arena_used = 0;
}

void
delta_pool_destroy(delta_pool_t *pool)
{
    if (!pool)
        return;
    free(pool->slab);
    free(pool->arena);
    free(pool);
}

bool
delta_pool_owns_slot(const delta_pool_t *pool, const void *ptr)
{
    if (!pool || !ptr)
        return false;
    const char *p = (const char *)ptr;
    const char *base = pool->slab;
    const char *end = base + (size_t)pool->slot_cap * pool->slot_size;
    return p >= base && p < end;
}

bool
delta_pool_owns_data(const delta_pool_t *pool, const void *ptr)
{
    if (!pool || !ptr)
        return false;
    const char *p = (const char *)ptr;
    return p >= pool->arena && p < pool->arena + pool->arena_cap;
}
