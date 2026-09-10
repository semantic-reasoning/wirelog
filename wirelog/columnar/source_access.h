/*
 * columnar/source_access.h - allocation-free source reader/writer gate
 *
 * INTERNAL HEADER - not installed, not part of the public API.
 */

#ifndef WL_COLUMNAR_SOURCE_ACCESS_H
#define WL_COLUMNAR_SOURCE_ACCESS_H

#include "columnar/mem_ledger.h"

#include <stdbool.h>
#include <stdint.h>

typedef struct {
    atomic_uint_fast64_t state;
} wl_columnar_source_access_t;

typedef enum {
    WL_COLUMNAR_SOURCE_ACCESS_READER = 1,
    WL_COLUMNAR_SOURCE_ACCESS_WRITER = 2,
} wl_columnar_source_access_mode_t;

/* A token is caller-owned and must remain at a stable address until release. */
typedef struct {
    wl_columnar_source_access_t *gate;
    const void *identity;
    wl_columnar_source_access_mode_t mode;
} wl_columnar_source_access_token_t;

void
wl_columnar_source_access_init(wl_columnar_source_access_t *gate);

int
wl_columnar_source_access_read_acquire(
    wl_columnar_source_access_t *gate,
    wl_columnar_source_access_token_t *token);

int
wl_columnar_source_access_write_acquire(
    wl_columnar_source_access_t *gate,
    wl_columnar_source_access_token_t *token);

int
wl_columnar_source_access_release(
    wl_columnar_source_access_token_t *token);

#endif /* WL_COLUMNAR_SOURCE_ACCESS_H */
