/* Internal append-only numeric Z-set traces over immutable batch payloads. */
#ifndef WL_COLUMNAR_TRACE_STORE_H
#define WL_COLUMNAR_TRACE_STORE_H

#include "columnar/batch_payload.h"

typedef struct wl_columnar_trace_store wl_columnar_trace_store_t;
typedef struct wl_columnar_trace_store_snapshot
    wl_columnar_trace_store_snapshot_t;
typedef struct wl_columnar_trace_store_cursor wl_columnar_trace_store_cursor_t;

typedef struct {
    uint64_t generation;
    uint32_t rows, segments;
} wl_columnar_trace_store_state_t;

/* Governed owner/schema, all segment nodes, one snapshot/directory and one
 * cursor respectively. Payload/reader/spill/input memory remains additional.
 * Directory/node metadata is O(segments), never O(rows). */
wl_columnar_spill_status_t wl_columnar_trace_store_floor(uint32_t ncols,
    uint32_t segments, uint64_t *owner, uint64_t *nodes,
    uint64_t *snapshot, uint64_t *cursor);
wl_columnar_spill_status_t wl_columnar_trace_store_create(
    wl_columnar_memory_governor_ref_t *governor,
    const wirelog_column_type_t *types, uint32_t ncols,
    wl_columnar_trace_store_t **out);
wl_columnar_trace_store_state_t wl_columnar_trace_store_state(
    const wl_columnar_trace_store_t *trace);

/* Every lifecycle call (including cursor acquisition/read/release) is externally
 * serialized against lifecycle mutation. Independently acquired cursors of the
 * SAME trace may read concurrently, but never concurrently with explicit
 * acquisition/release/append/demotion/retirement. A governed per-trace mutex
 * serializes lazy reader transitions, not actual private-reader I/O. Different
 * trace owners sharing a spill store must externally serialize their cursor
 * operations, as must raw payload/store lifecycle operations on that store.
 * One cursor permits only one active caller. Callbacks must not mutate or
 * reenter involved lifecycles.
 * Numeric scalar sources must use this governor. All rows must have zero
 * iteration/stratum/worker/reserved coordinates; arbitrary signed multiplicity
 * is preserved, including zero and negative contributions. No consolidation.
 * Schema/layout validation rejects STRING/compound/graph sources, even empty.
 * Caller guarantees numeric lanes are self-contained, not disguised handles.
 *
 * Capture copies all rows; source ownership stays with caller. Take requires an
 * exclusively owned governed heap source (never a registry/cache borrow), and
 * retires its storage/clears *source on success. All prospective allocations
 * and validation precede retirement; no failure/cancel point follows it.
 * Errors leave source, trace generation/rows/segments and prior snapshots intact.
 * Empty segments are retained and count against the UINT32_MAX segment limit;
 * total rows also cannot exceed UINT32_MAX. Generation is a storage commit
 * counter, NOT logical time; overflow is refused before mutation.
 * Transition credit includes source + private payload + node/write scratch. */
wl_columnar_spill_status_t wl_columnar_trace_store_append_capture(
    wl_columnar_trace_store_t *trace, const struct col_rel *source,
    wl_columnar_batch_payload_storage_t storage,
    wl_columnar_spill_store_t *store,
    wl_columnar_spill_cancel_fn cancel, void *context);
wl_columnar_spill_status_t wl_columnar_trace_store_append_take(
    wl_columnar_trace_store_t *trace, struct col_rel **source,
    wl_columnar_spill_store_t *store,
    wl_columnar_spill_cancel_fn cancel, void *context);
/* Demote one zero-based segment ordinal; snapshot-pinned nodes return BUSY
 * even without active readers. Already spilled nodes return INVALID.
 * Success changes storage only, preserving ordinals and commit generation. */
wl_columnar_spill_status_t wl_columnar_trace_store_demote(
    wl_columnar_trace_store_t *trace, uint32_t segment,
    wl_columnar_spill_store_t *store,
    wl_columnar_spill_cancel_fn cancel, void *context);

/* Retirement relinquishes the owner and closes append/demote/new snapshots.
 * Existing snapshots/cursors keep the owner and all nodes alive until drained.
 * After retirement the caller must not use the owner pointer. NULL is a no-op.
 * This does NOT retire the externally owned spill store. If that store retires,
 * an already acquired segment reader survives, but later lazy acquisitions
 * return CLOSED. A trace pin cannot promise reads across store retirement. */
void wl_columnar_trace_store_retire(wl_columnar_trace_store_t *trace);
wl_columnar_spill_status_t wl_columnar_trace_store_snapshot_acquire(
    wl_columnar_trace_store_t *trace, wl_columnar_trace_store_snapshot_t **out);
void wl_columnar_trace_store_snapshot_release(
    wl_columnar_trace_store_snapshot_t *snapshot);
const wirelog_column_type_t *wl_columnar_trace_store_snapshot_schema(
    const wl_columnar_trace_store_snapshot_t *snapshot, uint32_t *ncols,
    uint32_t *rows);
wl_columnar_trace_store_state_t wl_columnar_trace_store_snapshot_state(
    const wl_columnar_trace_store_snapshot_t *snapshot);
bool wl_columnar_trace_store_snapshot_uses_governor(
    const wl_columnar_trace_store_snapshot_t *snapshot,
    const wl_columnar_memory_governor_ref_t *governor);

/* Each cursor independently retains its snapshot, so caller may release its
 * snapshot immediately. At most one payload reader is active per cursor.
 * Immutable directory binary search uses at most 32 comparisons (UINT32
 * segments), including arbitrarily many empty segments. No row offset table.
 * Returned lanes last until this cursor's next read/release. Errors clear lanes
 * and metadata; they never mean absent rows. All cursor memory is governed. */
wl_columnar_spill_status_t wl_columnar_trace_store_cursor_acquire(
    wl_columnar_trace_store_snapshot_t *snapshot,
    wl_columnar_trace_store_cursor_t **out);
void wl_columnar_trace_store_cursor_release(
    wl_columnar_trace_store_cursor_t *cursor);
wl_columnar_spill_status_t wl_columnar_trace_store_cursor_read(
    wl_columnar_trace_store_cursor_t *cursor, uint64_t ordinal,
    wl_columnar_spill_cancel_fn cancel, void *context,
    const uint64_t **lanes, wl_columnar_spill_metadata_t *metadata);

#endif
