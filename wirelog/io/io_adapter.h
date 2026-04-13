/*
 * io_adapter.h - wirelog I/O Adapter Interface
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
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

#define WL_IO_ABI_VERSION   1u
#define WL_IO_MAX_ADAPTERS  32

/* ======================================================================== */
/* Opaque Context                                                           */
/* ======================================================================== */

typedef struct wl_io_ctx wl_io_ctx_t;

/* ======================================================================== */
/* Context Accessors (implemented in #453)                                  */
/* ======================================================================== */

const char *wl_io_ctx_relation_name(const wl_io_ctx_t *ctx);
uint32_t    wl_io_ctx_num_cols(const wl_io_ctx_t *ctx);
wirelog_column_type_t wl_io_ctx_col_type(const wl_io_ctx_t *ctx, uint32_t col);
const char *wl_io_ctx_param(const wl_io_ctx_t *ctx, const char *key);
int64_t     wl_io_ctx_intern_string(wl_io_ctx_t *ctx, const char *utf8);
void       *wl_io_ctx_platform(const wl_io_ctx_t *ctx);
int         wl_io_ctx_set_platform(wl_io_ctx_t *ctx, void *ptr);

/* ======================================================================== */
/* Adapter VTable                                                           */
/* ======================================================================== */

typedef struct wl_io_adapter {
    uint32_t abi_version;
    const char *scheme;
    const char *description;
    int (*read)(wl_io_ctx_t *ctx, int64_t **out_data, uint32_t *out_nrows,
        void *user_data);
    int (*validate)(wl_io_ctx_t *ctx, char *errbuf, size_t errbuf_len,
        void *user_data);
    void *user_data;
} wl_io_adapter_t;

/* ======================================================================== */
/* Registration API                                                         */
/* ======================================================================== */

/* __attribute__((used)) prevents iOS static-library dead-stripping.
 * MSVC does not support it; use #pragma comment(linker, /include:) if needed. */
#if defined(__GNUC__) || defined(__clang__)
#define WL_IO_USED __attribute__((used))
#else
#define WL_IO_USED
#endif

/*
 * Adapter Lifetime Contract
 * -------------------------
 * The wl_io_adapter_t pointer passed to wl_io_register_adapter() MUST remain
 * valid (i.e. the pointed-to struct must not be freed or modified) until the
 * corresponding wl_io_unregister_adapter() call or process exit.  The registry
 * stores the raw pointer and returns it verbatim from wl_io_find_adapter().
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
 * thread-local buffer retrievable via wl_io_last_error().  On success the
 * error buffer is cleared.  The returned pointer is valid until the next call
 * on the same thread.
 *
 * Return values:
 *   wl_io_register_adapter   0 on success, -1 on error (NULL input, ABI
 *                             mismatch, duplicate scheme, or registry full)
 *   wl_io_unregister_adapter 0 on success, -1 if scheme is not registered
 *   wl_io_find_adapter        pointer to adapter, or NULL if not found
 */

WL_PUBLIC int wl_io_register_adapter(const wl_io_adapter_t *adapter) WL_IO_USED;

WL_PUBLIC int wl_io_unregister_adapter(const char *scheme) WL_IO_USED;

WL_PUBLIC const wl_io_adapter_t *wl_io_find_adapter(
    const char *scheme) WL_IO_USED;

WL_PUBLIC const char *wl_io_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_IO_IO_ADAPTER_H */
