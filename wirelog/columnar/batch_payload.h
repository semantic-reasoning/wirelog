/* Internal owned numeric batches for bounded, synchronous publication. */
#ifndef WL_COLUMNAR_BATCH_PAYLOAD_H
#define WL_COLUMNAR_BATCH_PAYLOAD_H

#include "columnar/spill.h"

struct col_rel;
typedef struct wl_columnar_batch_payload wl_columnar_batch_payload_t;
typedef struct wl_columnar_batch_payload_reader
    wl_columnar_batch_payload_reader_t;
typedef enum {
    WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT,
    WL_COLUMNAR_BATCH_PAYLOAD_SPILLED,
} wl_columnar_batch_payload_storage_t;

/* Payload descriptor/schema/resident rows and reader descriptor, respectively.
 * Spilled mode additionally needs spill_floor's segment/writer/reader and a
 * live store. Caller source, context, stack and governor are additional costs.
 * Each retained payload has its own metadata floor, even when its rows spill. */
wl_columnar_spill_status_t wl_columnar_batch_payload_floor(uint32_t ncols,
    uint32_t nrows, wl_columnar_batch_payload_storage_t storage,
    uint64_t *payload_bytes, uint64_t *reader_bytes);

/* Copy the first rows under a source-reader lease. Explicit scalar schemas only;
 * no STRING/compound IDs, pointers, graph layouts or nullary rows. Missing types
 * mean INT64; missing timestamps mean {0,0,0,+1}. Source/callback state must remain
 * live during capture; none is retained. Callbacks must not reenter or tear down
 * the source, store or session. Spilled store must use this governor.
 * Publication is all-or-nothing; *out is NULL on error. Cancellation may run
 * after a private prefix has been copied, but never exposes it. */
wl_columnar_spill_status_t wl_columnar_batch_payload_capture(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_spill_store_t *store, const struct col_rel *source,
    uint32_t rows, wl_columnar_batch_payload_storage_t storage,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_batch_payload_t **out);

/* Demote an exclusively owned resident payload. On success replaces *owner,
 * invalidating the old address and borrowed schema pointers; ordinals, types,
 * lane bits and metadata remain identical. Active readers return BUSY; an
 * already spilled payload or mismatched governor returns INVALID.
 * Every error preserves *owner and its contents. Callbacks must not reenter
 * payload/store lifecycle operations. The store must remain live through this
 * call. Peak admission includes the old payload plus a spilled payload floor,
 * spill segment and writer floor (including path scratch); reserve that slack
 * before pressure exhausts it. Small/empty payloads may not save memory. */
wl_columnar_spill_status_t wl_columnar_batch_payload_spill(
    wl_columnar_batch_payload_t **owner, wl_columnar_spill_store_t *store,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context);

/* Lifecycle operations are externally serialized. Readers pin the payload;
 * release returns BUSY unchanged while readers exist. Independently acquired
 * readers may read concurrently (one active read per reader). Never release a
 * reader during its read. Numeric payloads retain no session/source pointers.
 * Already acquired spilled readers survive store retirement; new acquisition
 * on retired spilled storage returns CLOSED. Resident readers stay acquirable. */
wl_columnar_spill_status_t wl_columnar_batch_payload_release(
    wl_columnar_batch_payload_t *payload);
wl_columnar_spill_status_t wl_columnar_batch_payload_acquire(
    wl_columnar_batch_payload_t *payload,
    wl_columnar_batch_payload_reader_t **out);
void wl_columnar_batch_payload_reader_release(
    wl_columnar_batch_payload_reader_t *reader);
wl_columnar_spill_status_t wl_columnar_batch_payload_read(
    wl_columnar_batch_payload_reader_t *reader, uint64_t ordinal,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    const uint64_t **lanes, wl_columnar_spill_metadata_t *metadata);
/* Schema is borrowed through owner/reader lifetime; rows are stable ordinals.
 * Returned lanes live through the reader's next read/release. Error reads clear
 * both outputs, including cancellation and out-of-range ordinals. */
const wirelog_column_type_t *wl_columnar_batch_payload_schema(
    const wl_columnar_batch_payload_t *payload, uint32_t *ncols,
    uint32_t *nrows);

#endif
