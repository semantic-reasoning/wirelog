/*
 * arena/arena.c - wirelog Columnar Backend Arena Allocator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "arena.h"

#include <stdlib.h>
#include <string.h>

/* Alignment for all arena allocations (8 bytes = sizeof(int64_t)). */
#define WL_ARENA_ALIGN 8

/* Round @n up to the next multiple of WL_ARENA_ALIGN. */
#define WL_ARENA_ALIGN_UP(n) \
    (((n) + (WL_ARENA_ALIGN - 1)) & ~(size_t)(WL_ARENA_ALIGN - 1))

wl_arena_t *
wl_arena_create(size_t capacity)
{
    if (capacity == 0)
        return NULL;

    wl_arena_t *arena = (wl_arena_t *)malloc(sizeof(wl_arena_t));
    if (!arena)
        return NULL;

    arena->base = malloc(capacity);
    if (!arena->base) {
        free(arena);
        return NULL;
    }

    arena->capacity = capacity;
    arena->used = 0;
    return arena;
}

void *
wl_arena_alloc(wl_arena_t *arena, size_t size)
{
    if (!arena || size == 0)
        return NULL;

    size_t aligned = WL_ARENA_ALIGN_UP(size);
    if (arena->used + aligned > arena->capacity)
        return NULL;

    void *ptr = (char *)arena->base + arena->used;
    arena->used += aligned;
    return ptr;
}

void
wl_arena_reset(wl_arena_t *arena)
{
    if (!arena)
        return;
    arena->used = 0;
}

void
wl_arena_free(wl_arena_t *arena)
{
    if (!arena)
        return;
    free(arena->base);
    free(arena);
}
