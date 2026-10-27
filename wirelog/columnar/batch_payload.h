/* Internal owned immutable batches for bounded, synchronous publication. */
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

/* Eligibility concerns backing/credit only, not caller ownership authority.
 * Callers serialize the source lifecycle. Readers and alias borrows are dynamic
 * contention: they do not change this predicate and take returns BUSY for them. */
bool wl_columnar_batch_payload_can_take(const struct col_rel *source);

/* Transfer a complete governed numeric heap relation to an immutable spilled
 * owner. Caller exclusively owns *source (never a registry/cache borrow) and
 * serializes mutation and lifecycle; callbacks must not reenter any involved
 * source/store/governor lifecycle. Same-governor store/source required.
 * Canonical heap backing only, without pool/arena/shared/retraction ownership.
 * All ordinals, lane bits, types and timestamp/signed metadata are preserved;
 * original relation identity ends, and no relation-index cache is transferable.
 *
 * Success frees resident storage and clears *source before publishing *out.
 * Every failure leaves *source unchanged and *out NULL, refunding private files
 * and charges. Existing readers/aliases can refuse terminal retirement after
 * capture. Cancellation is checked before retirement, never after it commits.
 * Transition needs old source plus bounded capture scratch and payload metadata;
 * insufficient transition credit fails explicitly. Small inputs may save none.
 * Source and output pointer slots must be distinct and remain live throughout. */
wl_columnar_spill_status_t wl_columnar_batch_payload_take(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_spill_store_t *store, struct col_rel **source,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_batch_payload_t **out);

/* Stream a known number of numeric scalar rows directly into a spilled payload,
 * without a resident relation or rowset. Same-governor store required. Types
 * are copied; source/types/contexts are borrowed only until return. The source
 * fills all ncols lanes and complete metadata in the supplied one-row scratch
 * for ascending ordinals [0, rows). It must supply self-contained numeric bits,
 * never IDs/pointers/compound handles disguised as integers. STRING is rejected.
 * An empty payload permits NULL source; nonempty input requires a callback.
 * Callback state must remain live; callbacks must not reenter/retire any store,
 * governor or payload lifecycle involved in this call.
 *
 * Only the descriptor/schema and existing spill segment/writer scratch are
 * admitted; source state/stack and governor/OS costs remain additional. Source
 * errors propagate unchanged. All errors, including late cancellation, leave
 * *out NULL and refund private storage. Consumed input/callback side effects
 * cannot be rolled back: retry with a fresh or rewound source. Publication,
 * pins, retirement and read semantics match spilled relation capture. */
wl_columnar_spill_status_t wl_columnar_batch_payload_create_spilled(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_spill_store_t *store, const wirelog_column_type_t *types,
    uint32_t ncols, uint32_t rows, wl_columnar_spill_source_fn source,
    void *source_context, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_batch_payload_t **out);

/* Explicit STRING provenance: caller guarantees all STRING IDs originate in
 * intern, already attached to governor. Pins retain strings beyond owner
 * teardown; invalid IDs fail atomically. Numeric capture above still rejects
 * STRING. Compound/graph layouts remain unsupported. Callbacks must not free
 * the intern owner. Retained table storage is an additional resident floor. */
wl_columnar_spill_status_t wl_columnar_batch_payload_capture_with_intern(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_spill_store_t *store, const struct col_rel *source,
    uint32_t rows, wl_columnar_batch_payload_storage_t storage,
    wl_intern_t *intern, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context,
    wl_columnar_batch_payload_t **out);
/* Explicit flat INLINE capture. Declarations and intern must originate from
 * source; caller guarantees every expanded argument is self-contained (no
 * nested handles). Source metadata, offsets, arities and physical types must
 * agree. SIDE/graph layouts are rejected. Names are not retained. Functor IDs
 * pin intern even with numeric-only arguments. Declarations are borrowed only
 * during this call; callbacks must not mutate them or retire intern. */
wl_columnar_spill_status_t wl_columnar_batch_payload_capture_with_inline(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_spill_store_t *store, const struct col_rel *source,
    uint32_t rows, wl_columnar_batch_payload_storage_t storage,
    const wirelog_column_t *columns, uint32_t logical_count,
    wl_intern_t *intern,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_batch_payload_t **out);
wl_columnar_spill_status_t wl_columnar_batch_payload_floor_with_inline(
    uint32_t ncols, uint32_t nrows, uint32_t logical_count,
    wl_columnar_batch_payload_storage_t storage,
    uint64_t *payload_bytes, uint64_t *reader_bytes);
/* Borrowed semantic fields, name == NULL; scalar capture returns NULL/count0. */
const wirelog_column_t *wl_columnar_batch_payload_inline_schema(
    const wl_columnar_batch_payload_t *payload, uint32_t *logical_count);

/* Resolve an ID read from a STRING lane or INLINE functor descriptor. Borrowed result lives through reader
 * release, even after intern owner retirement. Invalid IDs clear *out. */
wl_columnar_spill_status_t wl_columnar_batch_payload_resolve_string(
    wl_columnar_batch_payload_reader_t *reader, uint64_t id, const char **out);

/* Demote an exclusively owned resident payload. On success replaces *owner,
 * invalidating the old address and borrowed schema pointers; ordinals, types,
 * lane bits and metadata remain identical. Active readers return BUSY; an
 * already spilled payload or mismatched governor returns INVALID.
 * Every error preserves *owner and its contents. Callbacks must not reenter
 * payload/store lifecycle operations. The store must remain live through this
 * call. Peak admission includes the old payload plus a spilled payload floor,
 * spill segment and writer floor (including path scratch); reserve that slack
 * before pressure exhausts it. Small/empty payloads may not save memory.
 * STRING/INLINE demotion needs a new pin; intern owner retirement returns CLOSED. */
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

/* Exact admission owner identity; no ownership transfer. */
bool wl_columnar_batch_payload_uses_governor(
    const wl_columnar_batch_payload_t *payload,
    const wl_columnar_memory_governor_ref_t *governor);

#endif
