/* Explicit indexed JOIN over pinned immutable payloads; internal API. */
#ifndef WL_COLUMNAR_PAYLOAD_JOIN_H
#define WL_COLUMNAR_PAYLOAD_JOIN_H

#include "columnar/batch_payload.h"
#include "columnar/continuation.h"

/* Both owners remain caller-owned. Separate readers pin them (also for self
 * JOIN), so release/demotion is BUSY until active cancellation or destruction.
 * The same governor is required. Inputs must have scalar numeric schemas,
 * INT64 keys and default
 * metadata {0,0,0,+1}; constructor checks ALL rows before publication.
 * Output concatenates left then right lanes with their original physical types.
 * STRING/INLINE, FLOAT keys and differential metadata are UNSUPPORTED.
 *
 * batch_bytes bounds output lane storage, not total memory. Producer, index,
 * types, readers and output relation are governed. The resident ordinal hash
 * index is O(right rows); it must fit in addition to input payloads and bounded
 * reader/output scratch. The generic continuation's fixed descriptor remains
 * an additional ungoverned allocation, as in other continuation producers.
 * Construction streams both inputs; cancel is checked per row. Publication
 * scans at most 256 row/chain actions per call, returning PROGRESS if no output.
 * Pending batches remain frozen on sink failure. Read/cancel errors are sticky
 * and available via error(); they never become DONE or absent matches.
 *
 * Serialized lifecycle/publication; independently created producers can run
 * concurrently over the same immutable inputs. Cancel frees readers, index and
 * output scratch while active. After DONE, cancel is a no-op: destroy is
 * required to release all retained resources. Destroy also frees the producer
 * and generic continuation.
 * Callbacks must not reenter or tear down input/store/producer lifecycle.
 * No automatic evaluator routing, controlled-attempt or scheduler integration.
 */
wl_columnar_spill_status_t wl_columnar_payload_join_create(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_continuation_t **out);
/* Explicit zero-coordinate Z-set JOIN. Same ownership, schema/key and
 * publication contract as create(), but arbitrary int64 multiplicities are
 * accepted only with iteration/stratum/worker all zero on BOTH complete inputs.
 * Each matching pair emits checked lhs_weight * rhs_weight, including zero,
 * in zero-initialized timestamp metadata. No consolidation is performed.
 * Nonzero logical times remain UNSUPPORTED; this is not timed differential JOIN.
 * Overflow is sticky OVERFLOW: the current private batch is discarded without
 * publication, while earlier committed batches remain visible (no whole-JOIN
 * rollback). Products are evaluated only for matching keys.
 * batch_bytes still bounds lane storage only. Governed timestamp capacity is
 * additional resident memory, as are the index, readers and producer state.
 * The ordinary constructor retains its default-metadata restriction. */
wl_columnar_spill_status_t wl_columnar_payload_join_create_weighted(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_continuation_t **out);
/* Explicit capped-index variants of create()/create_weighted(). index_bytes
 * includes the index reservation descriptor and uint32 bucket/link arrays.
 * For N rows, B is the next power of two >= N, clamped to [1, 2^31]; storage
 * is sizeof(index descriptor) + 4*(B+N). The descriptor contains one
 * wl_columnar_memory_reservation_t. Select the largest N <= right_rows fitting
 * the cap. A nonempty right input requires N >= 1; an empty one still requires
 * the descriptor and one bucket. An insufficient cap (including zero) is MEMORY.
 * All other resident floors remain additional, as described above.
 *
 * Reuse this index for disjoint right-row windows, rescanning the left input
 * for each window. Exact result MULTISETS are preserved; order may differ from
 * full-index constructors. Skew cannot grow the index. Extra read work is
 * O(left_rows * windows + right_rows + candidate probes), in addition to the
 * constructor's complete-input metadata validation. Clearing, building, probing
 * and phase transitions share the 256-action publication quantum; allocation
 * and constructor validation are synchronous. No new partition files or hidden
 * full-index fallback. Pending output is frozen through retries. The same
 * weighted overflow and current-private-batch atomicity contract applies even
 * if that batch spans windows. Existing full-index constructors are unchanged. */
wl_columnar_spill_status_t wl_columnar_payload_join_create_bounded(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    uint64_t index_bytes, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out);
wl_columnar_spill_status_t wl_columnar_payload_join_create_weighted_bounded(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    uint64_t index_bytes, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out);
/* Ordinary capped JOIN with direct output projection. Indices address the
 * flattened left-then-right schema; duplicates/reordering/key omission are
 * valid. A positive count requires a non-NULL map and all indices in range;
 * invalid maps return INVALID with *out NULL. Count zero ignores the pointer
 * and concatenates, as create_bounded(). The producer copies the map into a
 * separately governed allocation (reservation descriptor + count uint32s).
 * Output scratch and batch_bytes use the projected width; input readers retain
 * their full row scratch. No full-width intermediate output is materialized.
 * Existing ownership, metadata validation, windowing and retry contracts apply.
 * The caller may change/free the map after construction, not during callbacks. */
wl_columnar_spill_status_t wl_columnar_payload_join_create_projected_bounded(
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, const uint32_t *projection,
    uint32_t project_count, uint64_t batch_bytes, uint64_t index_bytes,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_continuation_t **out);
/* Only for a continuation returned by these constructors; NULL is INVALID.
 * Reports producer errors, not errors owned by the publication sink. */
wl_columnar_spill_status_t wl_columnar_payload_join_error(
    const wl_columnar_continuation_t *continuation);

#endif
