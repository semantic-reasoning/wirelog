/*
 * wirelog-internal.h - wirelog Internal Utilities
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 */

#ifndef WIRELOG_INTERNAL_H
#define WIRELOG_INTERNAL_H

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* Internal bridge used when evaluator failures are reported after worker
 * threads have joined.  The implementation stores the message in the
 * caller's thread-local error slot. */
void
wl_extension_error_set(const char *message);

void
wl_extension_error_set_expr_status(int status);

/* Internal callback lifetime lease. The lease token is valid until released,
 * even if the originating snapshot is released or the entry is unregistered
 * while the callback is running. */
void *
wl_extension_callback_lease_acquire(
    const wirelog_extension_snapshot_t *snapshot,
    const wirelog_extension_descriptor_t *descriptor);
void
wl_extension_callback_lease_release(void *lease);

/* ======================================================================== */
/* String Utilities (Pure C11)                                             */
/* ======================================================================== */

/**
 * wl_strdup:
 * @s: String to duplicate.  May be NULL.
 *
 * Allocate a copy of the string @s using malloc.  Returns NULL if @s is NULL
 * or if memory allocation fails.  The returned pointer must be freed by the
 * caller.
 *
 * This is a pure C11 implementation (no POSIX dependency).
 */
static inline char *
wl_strdup(const char *s)
{
    if (!s)
        return NULL;
    size_t len = strlen(s) + 1;
    char *dup = (char *)malloc(len);
    if (!dup)
        return NULL;
    memcpy(dup, s, len);
    return dup;
}

#endif /* WIRELOG_INTERNAL_H */
