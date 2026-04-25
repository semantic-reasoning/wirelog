/*
 * columnar/handle_remap.h - Compound-handle Remap Table (Issue #557)
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
 * Open-addressing hash table that maps old compound handles to new
 * compound handles across an epoch-boundary remap pass. Used by the
 * EDB remap (#563) to rewrite handle columns after compound-arena
 * compaction so persisted handles remain valid past the 12-bit epoch
 * saturation point (#550).
 *
 * Probing:       linear
 * Load factor:   rehash above 0.75
 * Empty marker:  key == 0 (== WL_COMPOUND_HANDLE_NULL, reserved)
 * Deletion:      not supported (build-then-query usage)
 *
 * See `handle_remap_design.md` for the full rationale, including why
 * open addressing was chosen over chaining and why linear probing was
 * chosen over quadratic.
 *
 * ========================================================================
 * Thread Safety
 * ========================================================================
 *
 * NOT thread-safe. Intended for single-threaded use on the main thread
 * between epochs while K-Fusion workers are paused (compound arena
 * frozen). Concurrent readers after the build phase ends require an
 * external rwlock; the table itself has no internal synchronization.
 *
 * ========================================================================
 * Error Codes
 * ========================================================================
 *
 * Operations return 0 on success and a positive errno on failure, to
 * match the existing convention in wirelog/columnar/ (e.g. session.c,
 * eval.c return ENOMEM / EINVAL / ENOENT directly):
 *
 *   WL_ERROR_INVALID_ARGS   (EINVAL)   bad argument (NULL out, key == 0)
 *   WL_ERROR_OOM            (ENOMEM)   allocation failed
 *   WL_ERROR_NOT_FOUND      (ENOENT)   reserved for future find_or_fail
 *
 * `wl_handle_remap_lookup` does not return an error on miss; it
 * returns 0 (WL_COMPOUND_HANDLE_NULL). Since NULL is never a valid
 * remapped value, callers can use 0 as the unambiguous miss sentinel.
 */

#ifndef WL_COLUMNAR_HANDLE_REMAP_H
#define WL_COLUMNAR_HANDLE_REMAP_H

#include <errno.h>
#include <stddef.h>
#include <stdint.h>

#ifndef WL_ERROR_INVALID_ARGS
#define WL_ERROR_INVALID_ARGS EINVAL
#endif

#ifndef WL_ERROR_OOM
#define WL_ERROR_OOM ENOMEM
#endif

#ifndef WL_ERROR_NOT_FOUND
#define WL_ERROR_NOT_FOUND ENOENT
#endif

/**
 * wl_handle_remap_t:
 *
 * Opaque open-addressing hash table mapping old (pre-compaction)
 * compound handles to new (post-compaction) compound handles.
 * Layout is implementation-defined; see `handle_remap.c`.
 */
typedef struct wl_handle_remap wl_handle_remap_t;

/**
 * wl_handle_remap_create:
 * @capacity: hint for the expected number of live entries (e.g. the
 *            live-handle count returned by the compound arena before
 *            compaction). The implementation rounds the actual table
 *            size up so the load factor stays at or below 0.75 after
 *            inserting @capacity entries, then rounds further to the
 *            next power of two. Pass 0 to accept the implementation
 *            minimum (16 slots).
 * @out:      (out, transfer full): receives the new table on success;
 *            set to NULL on failure. Must not be NULL.
 *
 * Allocate an empty remap table sized for at least @capacity entries
 * before the first rehash.
 *
 * Returns: 0 on success, WL_ERROR_INVALID_ARGS if @out is NULL,
 *          WL_ERROR_OOM on allocation failure.
 */
int
wl_handle_remap_create(size_t capacity, wl_handle_remap_t **out);

/**
 * wl_handle_remap_insert:
 * @remap:      table to insert into. Must not be NULL.
 * @old_handle: 64-bit handle from the pre-compaction arena. The value
 *              0 (WL_COMPOUND_HANDLE_NULL) is rejected because it is
 *              reserved as the empty-slot marker.
 * @new_handle: 64-bit handle that @old_handle should remap to. May be
 *              any value, including a freshly allocated post-
 *              compaction handle.
 *
 * Insert (or overwrite, if @old_handle is already present) the
 * mapping (@old_handle -> @new_handle).
 *
 * If the table's live entry count would exceed 75% of capacity, the
 * implementation rehashes into a fresh array of double capacity
 * before inserting. If that allocation fails, the table is left
 * unchanged at its current capacity and WL_ERROR_OOM is returned;
 * callers may continue using the table at the previous size (lookups
 * still work; future inserts may also fail).
 *
 * Returns: 0 on success,
 *          WL_ERROR_INVALID_ARGS if @remap is NULL or @old_handle is 0,
 *          WL_ERROR_OOM if rehash allocation fails.
 */
int
wl_handle_remap_insert(wl_handle_remap_t *remap,
    int64_t old_handle,
    int64_t new_handle);

/**
 * wl_handle_remap_lookup:
 * @remap:      table to query. NULL-safe (returns 0).
 * @old_handle: handle to look up. Pass-through 0 returns 0.
 *
 * Look up the new handle that @old_handle was remapped to.
 *
 * Returns: the stored new handle on hit;
 *          0 (WL_COMPOUND_HANDLE_NULL) on miss, or if @remap is NULL,
 *          or if @old_handle is 0. Because no live mapping ever has
 *          new_handle == 0 (NULL is reserved by the compound arena),
 *          0 is unambiguous: it means "not in the table".
 */
int64_t
wl_handle_remap_lookup(const wl_handle_remap_t *remap,
    int64_t old_handle);

/**
 * wl_handle_remap_free:
 * @remap: (transfer full): table to free. NULL-safe.
 *
 * Release the table and all its backing storage. After this call
 * @remap must not be used.
 */
void
wl_handle_remap_free(wl_handle_remap_t *remap);

#endif /* WL_COLUMNAR_HANDLE_REMAP_H */
