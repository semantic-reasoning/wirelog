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

#include "wirelog-extension.h"
#include "evaluation_control.h"
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/* Internal status for governor admission denial.  Keep it distinct from
 * ENOMEM and ENOSPC so compound saturation and allocator failures retain
 * their existing meanings until the public facade boundary. */
#define WL_ERR_MEMORY_BUDGET 0x10001

/* Internal bridge used when evaluator failures are reported after worker
 * threads have joined.  The implementation stores the message in the
 * caller's thread-local error slot. */
void
wl_extension_error_set(const char *message);

void
wl_extension_error_set_expr_status(int status);

/* Expression allocation failures are memory verdicts, not extension
 * diagnostics.  Keep this mapping at the facade boundary so all public
 * session entry points agree on the result of a governed evaluation. */
int
wl_facade_session_error_code(int rc, int expr_status);

int
wl_facade_session_error_code_with_budget(int rc, int expr_status,
    bool budget_denied);

/* A fact mutation begins by clearing the transient denial flag.  Some
 * relation growth paths still return ENOMEM for a denied reservation; scope
 * that translation to this operation so later snapshot errors stay distinct. */
int
wl_facade_fact_mutation_error_code(int rc, bool budget_denied);

/* Internal callback lifetime lease. The lease token is valid until released,
 * even if the originating snapshot is released or the entry is unregistered
 * while the callback is running. */
void *
wl_extension_callback_lease_acquire(
    const wirelog_extension_snapshot_t *snapshot,
    const wirelog_extension_descriptor_t *descriptor);
void
wl_extension_callback_lease_release(void *lease);

/* Caller keeps the immutable authentic snapshot alive through each call.
 * name[0..name_bytes] is readable, name_bytes<=255, with a terminating NUL
 * at the bound; an earlier NUL ignores the remaining encoded bytes.
 * Nonzero preserves every output and TLS diagnostic. A successful miss sets
 * descriptor=NULL,index=SIZE_MAX,found=false. The index is snapshot-specific. */
int wl_extension_control_lookup_attempt(
    const wirelog_extension_snapshot_t *snapshot,
    const char *name, size_t name_bytes,
    const wirelog_extension_descriptor_t **descriptor,
    size_t *index, bool *found, wl_evaluation_control_t *control, uint64_t id);
/* Fixed indexed membership, not a second scan. A successful lease transfers
 * exactly one raw callback lease release to caller. Misses set NULL/false.
 * Counter saturation is EOVERFLOW, after the final saved-identity guard. */
int wl_extension_control_lease_index_attempt(
    const wirelog_extension_snapshot_t *snapshot,
    size_t index, const wirelog_extension_descriptor_t *expected, void **lease,
    bool *found, wl_evaluation_control_t *control, uint64_t id);

/* Issue #2122: true when every extension in @snapshot declares a
 * thread-safe callback, and for a NULL or empty snapshot. */
bool
wl_extension_snapshot_all_thread_safe(
    const wirelog_extension_snapshot_t *snapshot);

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
