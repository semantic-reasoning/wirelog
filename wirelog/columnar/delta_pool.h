/*
 * backend/delta_pool.h - wirelog Delta Pool Allocator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * ========================================================================
 * Overview
 * ========================================================================
 *
 * The delta pool provides a bulk allocator for temporary fixed-size structs
 * and their associated data buffers used during operator evaluation.
 *
 * The pool pre-allocates a slab of fixed-size slots and a large arena for
 * variable-size data.  On reset, all allocations are released at once (O(1)).
 *
 * ========================================================================
 * Thread Safety
 * ========================================================================
 *
 * NOT thread-safe.  Each K-fusion worker must create its own pool.
 */

#ifndef WL_BACKEND_DELTA_POOL_H
#define WL_BACKEND_DELTA_POOL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
 * delta_pool_t:
 *
 * Slab + arena allocator for temporary fixed-size objects.
 *
 * @slab:         Pre-allocated byte array for fixed-size slots.
 * @slot_size:    Size of each slot in bytes.
 * @slot_cap:     Total number of slots.
 * @slot_used:    Number of slots currently in use.
 * @arena:        Backing buffer for variable-size data allocations.
 * @arena_cap:    Total arena capacity in bytes.
 * @arena_used:   Bytes consumed so far.
 */
typedef struct {
    char *slab;
    size_t slot_size;
    uint32_t slot_cap;
    uint32_t slot_used;
    char *arena;
    size_t arena_cap;
    size_t arena_used;
} delta_pool_t;

/**
 * delta_pool_create:
 * @max_slots:   Maximum number of fixed-size slots in the slab.
 * @slot_size:   Size of each slot in bytes.
 * @arena_bytes: Size of the data arena in bytes.
 *
 * Returns NULL on allocation failure.
 */
delta_pool_t *
delta_pool_create(uint32_t max_slots, size_t slot_size, size_t arena_bytes);

/**
 * delta_pool_alloc_slot:
 * @pool: Pool to allocate from.
 *
 * Return a zeroed pointer to a slot from the slab.
 * Returns NULL if the slab is exhausted.
 */
void *
delta_pool_alloc_slot(delta_pool_t *pool);

/**
 * delta_pool_alloc_data:
 * @pool:  Pool to allocate from.
 * @bytes: Number of bytes to allocate (8-byte aligned).
 *
 * Bump-allocate from the data arena.
 * Returns NULL if the arena is exhausted.
 */
void *
delta_pool_alloc_data(delta_pool_t *pool, size_t bytes);

/**
 * delta_pool_reset:
 * @pool: Pool to reset.  NULL-safe.
 *
 * Reset all slab and arena counters to zero.  O(1) operation.
 */
void
delta_pool_reset(delta_pool_t *pool);

/**
 * delta_pool_destroy:
 * @pool: (transfer full): Pool to destroy.  NULL-safe.
 */
void
delta_pool_destroy(delta_pool_t *pool);

/**
 * delta_pool_owns_slot:
 * @pool: Pool to check against.
 * @ptr:  Pointer to check.
 *
 * Return true if @ptr points into the pool's slab.
 */
bool
delta_pool_owns_slot(const delta_pool_t *pool, const void *ptr);

/**
 * delta_pool_owns_data:
 * @pool: Pool to check against.
 * @ptr:  Pointer to check.
 *
 * Return true if @ptr points into the pool's data arena.
 */
bool
delta_pool_owns_data(const delta_pool_t *pool, const void *ptr);

#endif /* WL_BACKEND_DELTA_POOL_H */
