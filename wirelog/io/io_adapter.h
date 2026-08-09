/*
 * io_adapter.h - wirelog I/O Adapter Interface
 *
 * Copyright (C) CleverPlant
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Licensed under LGPL-3.0-or-later
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */

/**
 * @file io_adapter.h
 * @brief Pluggable I/O adapter ABI for custom source and sink backends.
 */

#ifndef WIRELOG_IO_IO_ADAPTER_H
#define WIRELOG_IO_IO_ADAPTER_H

#include "wirelog/wirelog-export.h"
#include "wirelog/wirelog-types.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ======================================================================== */
/* Constants                                                                */
/* ======================================================================== */

/* WIRELOG_IO_ABI_VERSION:
 *   1u (v0.30.0): symbols carried internal `wl_io_*` / `WL_IO_*`
 *      prefixes on this public installed header, contradicting the
 *      AGENTS.md naming-convention rule that public symbols must use
 *      `wirelog_*` / `WIRELOG_*`.
 *   2u (v0.40+): symbols renamed; the registration-time
 *      `adapter->abi_version != WIRELOG_IO_ABI_VERSION` check rejects
 *      v0.30.0 plugins (abi_version == 1u) loud, with a diagnostic
 *      retrievable via `wirelog_io_last_error()`.  Plugin authors
 *      built against v0.30.0 must rebuild against v0.40+ to
 *      ABI-version 2u.
 */
#define WIRELOG_IO_ABI_VERSION   2u
#define WIRELOG_IO_MAX_ADAPTERS  32

/* ======================================================================== */
/* Opaque Context                                                           */
/* ======================================================================== */

typedef struct wirelog_io_ctx wirelog_io_ctx_t;

/* ======================================================================== */
/* Context Accessors (implemented in #453)                                  */
/* ======================================================================== */

/*
 * wirelog_io_ctx_num_cols() is the PHYSICAL row stride of the relation --
 * the number of int64_t slots one tuple occupies, which is the number of
 * fields read() must produce per row and the width wirelog inserts the
 * returned buffer at.  It is NOT the relation's declared column count.
 *
 * A column declared as an `inline` compound occupies its full arity of
 * consecutive slots: `.decl inp(id: int64, p: pair/2 inline, s: symbol)` is
 * three declared columns but num_cols 4, and its `.input` file carries four
 * fields.  A `side` compound is one handle slot, and so is every scalar, so
 * the two numbers agree for every relation that declares no inline compound
 * column.
 *
 * wirelog_io_ctx_col_type(ctx, i) is indexed by the same physical position,
 * so `i` runs over slots and not over declared columns.  Each slot of an
 * inline compound reports WIRELOG_TYPE_INT64 -- the slots carry raw int64
 * payload, and the declared type of a compound column does not describe
 * them.  In the example above the types are INT64, INT64, INT64, STRING.
 *
 * Before #985 both accessors reported the declared column count and the
 * declared per-column types, which named a stride the storage does not use;
 * an adapter for such a relation was handed the wrong width and the wrong
 * types.  The signatures are unchanged and WIRELOG_IO_ABI_VERSION is
 * unchanged, but an adapter that reconstructed the stride from a schema of
 * its own -- rather than from num_cols, as the contract in
 * docs/io-adapters.md requires -- will now disagree with wirelog for a
 * relation with an inline compound column.  Index by num_cols.
 */
WIRELOG_API const char *
wirelog_io_ctx_relation_name(const wirelog_io_ctx_t *ctx);
WIRELOG_API uint32_t
wirelog_io_ctx_num_cols(const wirelog_io_ctx_t *ctx);
WIRELOG_API wirelog_column_type_t
wirelog_io_ctx_col_type(const wirelog_io_ctx_t *ctx, uint32_t col);
WIRELOG_API const char *
wirelog_io_ctx_param(const wirelog_io_ctx_t *ctx, const char *key);
WIRELOG_API int64_t
wirelog_io_ctx_intern_string(wirelog_io_ctx_t *ctx, const char *utf8);
WIRELOG_API void *
wirelog_io_ctx_platform(const wirelog_io_ctx_t *ctx);
WIRELOG_API int
wirelog_io_ctx_set_platform(wirelog_io_ctx_t *ctx, void *ptr);

/* ======================================================================== */
/* Adapter VTable                                                           */
/* ======================================================================== */

typedef struct wirelog_io_adapter {
    uint32_t abi_version;
    const char *scheme;
    const char *description;
    int (*read)(wirelog_io_ctx_t *ctx, int64_t **out_data, uint32_t *out_nrows,
        void *user_data);
    int (*validate)(wirelog_io_ctx_t *ctx, char *errbuf, size_t errbuf_len,
        void *user_data);
    void *user_data;
} wirelog_io_adapter_t;

/* ======================================================================== */
/* Registration API                                                         */
/* ======================================================================== */

/* __attribute__((used)) prevents iOS static-library dead-stripping.
 * MSVC does not support it; use #pragma comment(linker, /include:) if needed. */
#if defined(__GNUC__) || defined(__clang__)
#define WIRELOG_IO_USED __attribute__((used))
#else
#define WIRELOG_IO_USED
#endif

/*
 * Adapter Lifetime Contract
 * -------------------------
 * The wirelog_io_adapter_t pointer passed to wirelog_io_register_adapter() MUST remain
 * valid (i.e. the pointed-to struct must not be freed or modified) until the
 * corresponding wirelog_io_unregister_adapter() call or process exit.  The registry
 * stores the raw pointer and returns it verbatim from wirelog_io_find_adapter().
 *
 * The scheme string within the adapter struct is copied into an internal
 * fixed-size buffer (SCHEME_MAX_LEN-1 characters) at registration time, so
 * the caller's scheme pointer may be freed or reused after register returns.
 *
 * Thread Safety
 * -------------
 * All three public API functions (register, unregister, find) are thread-safe.
 * They acquire a process-global mutex for the duration of each operation.
 * TSan CI gate (Issue #459) validates this invariant on every PR.
 *
 * Error Reporting
 * ---------------
 * On failure, functions return -1 and record a human-readable reason in a
 * thread-local buffer retrievable via wirelog_io_last_error().  On success the
 * error buffer is cleared.  The returned pointer is valid until the next call
 * on the same thread.
 *
 * Return values:
 *   wirelog_io_register_adapter   0 on success, -1 on error (NULL input, ABI
 *                             mismatch, duplicate scheme, or registry full)
 *   wirelog_io_unregister_adapter 0 on success, -1 if scheme is not registered
 *   wirelog_io_find_adapter        pointer to adapter, or NULL if not found
 */

WIRELOG_API int wirelog_io_register_adapter(
    const wirelog_io_adapter_t *adapter) WIRELOG_IO_USED;

WIRELOG_API int wirelog_io_unregister_adapter(
    const char *scheme) WIRELOG_IO_USED;

WIRELOG_API const wirelog_io_adapter_t *wirelog_io_find_adapter(
    const char *scheme) WIRELOG_IO_USED;

WIRELOG_API const char *wirelog_io_last_error(void);

/* ======================================================================== */
/* Plugin Entry Point (Path B, Issue #461)                                  */
/* ======================================================================== */

/*
 * Macro for plugin shared libraries to export their entry symbol.
 * Users define their entry function with this attribute so the linker
 * does not strip it even under -fvisibility=hidden or LTO.
 */
#if defined(_WIN32) || defined(__CYGWIN__)
#define WIRELOG_IO_PLUGIN_EXPORT __declspec(dllexport)
#elif defined(__GNUC__) || defined(__clang__)
#define WIRELOG_IO_PLUGIN_EXPORT __attribute__((visibility("default")))
#else
#define WIRELOG_IO_PLUGIN_EXPORT
#endif

/*
 * Plugin entry point signature.
 *
 * A plugin shared library must export exactly one symbol named
 * "wirelog_io_plugin_entry" with this signature.  The CLI plugin loader
 * calls dlopen() on the library, resolves this symbol via dlsym(),
 * validates the ABI version, and bulk-registers all returned adapters.
 *
 * Parameters:
 *   n_out    [out] Number of adapters in the returned array.
 *   abi_ver  [in]  Host's WIRELOG_IO_ABI_VERSION.  The plugin should check
 *                  this against its own compiled version and return NULL
 *                  on mismatch.
 *
 * Returns:
 *   Array of adapter pointers (must remain valid for process lifetime),
 *   or NULL on ABI mismatch / error.
 */
typedef const wirelog_io_adapter_t *const *(*wirelog_io_plugin_entry_fn)(
    uint32_t *n_out, uint32_t abi_ver);

#define WIRELOG_IO_PLUGIN_ENTRY_SYMBOL "wirelog_io_plugin_entry"

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_IO_IO_ADAPTER_H */
