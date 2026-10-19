/* Internal immutable numeric spill segments; not a durable interchange format. */
#ifndef WL_COLUMNAR_SPILL_H
#define WL_COLUMNAR_SPILL_H

#include "columnar/memory_governor.h"
#include "wirelog/wirelog-types.h"
#include <stddef.h>
#include <stdint.h>

typedef struct wl_columnar_spill_store wl_columnar_spill_store_t;
typedef struct wl_columnar_spill_segment wl_columnar_spill_segment_t;
typedef struct wl_columnar_spill_reader wl_columnar_spill_reader_t;

typedef enum {
    WL_COLUMNAR_SPILL_OK = 0,
    WL_COLUMNAR_SPILL_INVALID,
    WL_COLUMNAR_SPILL_UNSUPPORTED,
    WL_COLUMNAR_SPILL_MEMORY,
    WL_COLUMNAR_SPILL_ALLOCATION,
    WL_COLUMNAR_SPILL_QUOTA,
    WL_COLUMNAR_SPILL_CAPACITY,
    WL_COLUMNAR_SPILL_IO,
    WL_COLUMNAR_SPILL_CANCELLED,
    WL_COLUMNAR_SPILL_BUSY,
    WL_COLUMNAR_SPILL_OVERFLOW,
} wl_columnar_spill_status_t;

typedef struct {
    uint32_t iteration, stratum, worker;
    int64_t multiplicity;
} wl_columnar_spill_metadata_t;

/* Source fills exactly ncols self-contained lanes and metadata for ordinal.
 * It must keep its own input alive/accounted, and must not reenter this store.
 * STRING, compounds, intern IDs and pointers are NOT supported payloads. */
typedef wl_columnar_spill_status_t (*wl_columnar_spill_source_fn)(void *,
    uint64_t ordinal, uint64_t *lanes, wl_columnar_spill_metadata_t *metadata);
typedef bool (*wl_columnar_spill_cancel_fn)(void *);

typedef struct {
    uint64_t store_bytes, segment_bytes, writer_bytes, reader_bytes;
} wl_columnar_spill_floor_t;

/* Includes owned descriptors, path, schema and one-row scratch, not caller
 * input/output, governor object, OS descriptor/kernel cache or allocator slack.
 * Creation: store + segment + writer. Reading: store + segment + N*reader. */
wl_columnar_spill_status_t wl_columnar_spill_floor(const char *directory,
    uint32_t ncols, wl_columnar_spill_floor_t *out);

/* Explicit opt-in only. Directory must already exist and remain under caller
 * control. max_row_bytes bounds each reader/writer scratch, quota includes
 * header/schema. Governor must be non-NULL and is retained until close.
 * All lifecycle calls (open/create/acquire/release/close) are externally
 * serialized. Independent acquired readers may read concurrently; each reader
 * permits one active read. No acquisition from an already released pointer. */
wl_columnar_spill_status_t wl_columnar_spill_store_open(const char *directory,
    wl_columnar_memory_governor_ref_t *governor, uint64_t quota,
    uint64_t max_row_bytes, wl_columnar_spill_store_t **out);
wl_columnar_spill_status_t wl_columnar_spill_store_close(
    wl_columnar_spill_store_t *store);
uint64_t wl_columnar_spill_store_bytes(const wl_columnar_spill_store_t *store);

/* Scalar schemas only: logical and physical column types are identical.
 * Immutable publication occurs only on success; *out is NULL on failure. */
wl_columnar_spill_status_t wl_columnar_spill_create(
    wl_columnar_spill_store_t *store, const wirelog_column_type_t *types,
    uint32_t ncols, uint64_t nrows, wl_columnar_spill_source_fn source,
    void *source_context, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_spill_segment_t **out);
/* BUSY leaves the owner unchanged while any acquired reader exists. */
wl_columnar_spill_status_t wl_columnar_spill_release(
    wl_columnar_spill_segment_t *segment);
wl_columnar_spill_status_t wl_columnar_spill_acquire(
    wl_columnar_spill_segment_t *segment, wl_columnar_spill_reader_t **out);
void wl_columnar_spill_reader_release(wl_columnar_spill_reader_t *reader);

/* Ordinal is stable for this segment's lifetime. Iterate ranges with repeated
 * calls. On error *lanes is NULL and metadata is zero; no partial row succeeds.
 * Returned lanes live until this reader's next read/release. Schema lives
 * through the segment owner or an acquired reader. No whole-file loading. */
wl_columnar_spill_status_t wl_columnar_spill_read(
    wl_columnar_spill_reader_t *reader, uint64_t ordinal,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    const uint64_t **lanes, wl_columnar_spill_metadata_t *metadata);
const wirelog_column_type_t *wl_columnar_spill_schema(
    const wl_columnar_spill_segment_t *segment, uint32_t *ncols,
    uint64_t *nrows);

#endif
