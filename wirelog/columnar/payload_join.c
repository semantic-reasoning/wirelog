#include "columnar/payload_join.h"
#include "columnar/internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define WL_COLUMNAR_PAYLOAD_JOIN_END UINT32_MAX
#define WL_COLUMNAR_PAYLOAD_JOIN_QUANTUM 256u

/* A single payload keeps its eager reader/capability for the producer lifetime.
 * A trace cursor independently pins its snapshot and opens one segment lazily. */
typedef struct {
    wl_columnar_batch_payload_t *payload;
    wl_columnar_trace_store_snapshot_t *snapshot;
} wl_columnar_payload_join_source_t;
typedef struct {
    wl_columnar_batch_payload_reader_t *payload;
    wl_columnar_trace_store_cursor_t *trace;
} wl_columnar_payload_join_cursor_t;

static const wirelog_column_type_t *
wl_columnar_payload_join_source_schema(wl_columnar_payload_join_source_t source,
    uint32_t *cols, uint32_t *rows)
{
    return source.snapshot
        ? wl_columnar_trace_store_snapshot_schema(source.snapshot, cols, rows)
        : wl_columnar_batch_payload_schema(source.payload, cols, rows);
}
static bool
wl_columnar_payload_join_source_governor(
    wl_columnar_payload_join_source_t source,
    wl_columnar_memory_governor_ref_t *g)
{
    return source.snapshot
        ? wl_columnar_trace_store_snapshot_uses_governor(source.snapshot, g)
        : wl_columnar_batch_payload_uses_governor(source.payload, g);
}
static wl_columnar_spill_status_t
wl_columnar_payload_join_source_acquire(
    wl_columnar_payload_join_source_t source,
    wl_columnar_payload_join_cursor_t *cursor)
{
    return source.snapshot
        ? wl_columnar_trace_store_cursor_acquire(source.snapshot,
               &cursor->trace)
        : wl_columnar_batch_payload_acquire(source.payload, &cursor->payload);
}
static void
wl_columnar_payload_join_source_release(
    wl_columnar_payload_join_cursor_t *cursor)
{
    wl_columnar_batch_payload_reader_release(cursor->payload);
    wl_columnar_trace_store_cursor_release(cursor->trace);
    memset(cursor, 0, sizeof(*cursor));
}
static wl_columnar_spill_status_t
wl_columnar_payload_join_source_read(wl_columnar_payload_join_cursor_t *cursor,
    uint32_t row, wl_columnar_spill_cancel_fn cancel, void *context,
    const uint64_t **lanes, wl_columnar_spill_metadata_t *meta)
{
    return cursor->trace
        ? wl_columnar_trace_store_cursor_read(cursor->trace, row, cancel,
               context, lanes, meta)
        : wl_columnar_batch_payload_read(cursor->payload, row, cancel,
               context, lanes, meta);
}

typedef struct {
    wl_columnar_memory_reservation_t reservation;
    uint32_t slots[];
} wl_columnar_payload_join_index_t;

typedef struct {
    wl_columnar_memory_reservation_t reservation;
    uint32_t columns[];
} wl_columnar_payload_join_projection_t;

typedef enum {
    WL_COLUMNAR_PAYLOAD_JOIN_CLEAR,
    WL_COLUMNAR_PAYLOAD_JOIN_BUILD,
    WL_COLUMNAR_PAYLOAD_JOIN_PROBE,
    WL_COLUMNAR_PAYLOAD_JOIN_DONE
} wl_columnar_payload_join_phase_t;

typedef struct {
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor;
    wl_columnar_payload_join_cursor_t left, right;
    wl_columnar_payload_join_index_t *index;
    wl_columnar_payload_join_projection_t *projection;
    uint32_t output_cols;
    col_rel_t *batch;
    wl_columnar_spill_cancel_fn cancel;
    void *cancel_context;
    wl_columnar_spill_status_t error;
    const uint64_t *left_lanes;
    uint64_t identity;
    uint32_t left_rows, right_rows, left_cols, right_cols, left_key, right_key;
    uint32_t buckets, capacity, row, candidate;
    uint32_t window_start, window_rows, window_capacity, build_row,
        clear_bucket;
    wl_columnar_payload_join_phase_t phase;
    bool started, stopped, weighted;
    int64_t left_multiplicity;
    wirelog_column_type_t types[];
} wl_columnar_payload_join_t;

static uint32_t
wl_columnar_payload_join_buckets(uint32_t rows)
{
    uint32_t buckets = 1;
    while (buckets < rows && buckets < (UINT32_C(1) << 31)) buckets <<= 1;
    return buckets;
}

static uint64_t
wl_columnar_payload_join_index_bytes(uint32_t rows)
{
    return sizeof(wl_columnar_payload_join_index_t)
           + ((uint64_t)wl_columnar_payload_join_buckets(rows) + rows)
           * sizeof(uint32_t);
}

#ifdef WL_TEST_PAYLOAD_JOIN
extern bool wl_columnar_payload_join_test_allocation(void);
#endif

static wl_columnar_spill_status_t
wl_columnar_payload_join_allocate(wl_columnar_memory_governor_ref_t *g,
    uint64_t bytes, void **out)
{
    *out = NULL;
    if (bytes > SIZE_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_reservation_init(&pending);
    wl_columnar_memory_admission_status_t admission =
        wl_columnar_memory_reserve_checked(wl_columnar_memory_governor_ref_get(
                g),
            bytes, &pending);
    if (admission > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return admission == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
            ? WL_COLUMNAR_SPILL_MEMORY : WL_COLUMNAR_SPILL_OVERFLOW;
    void *ptr = NULL;
    bool fail = false;
#ifdef WL_TEST_PAYLOAD_JOIN
    fail = wl_columnar_payload_join_test_allocation();
#endif
    if (!fail) ptr = calloc(1, (size_t)bytes);
    if (!ptr) {
        if (!wl_columnar_memory_rollback(&pending)) abort();
        return WL_COLUMNAR_SPILL_ALLOCATION;
    }
    wl_columnar_memory_reservation_t *token = ptr;
    wl_columnar_memory_reservation_init(token);
    if (!wl_columnar_memory_reservation_move(token, &pending)
        || !wl_columnar_memory_commit(token, ptr)) abort();
    *out = ptr;
    return WL_COLUMNAR_SPILL_OK;
}

static void
wl_columnar_payload_join_free(void *ptr)
{
    if (!ptr) return;
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reservation_move(&token, ptr)) abort();
    free(ptr);
    if (!wl_columnar_memory_release(&token)) abort();
}

/* Unsigned FNV-1a, matching build and probe; exact equality handles collisions. */
static uint32_t
wl_columnar_payload_join_hash(uint64_t key, uint32_t buckets)
{
    uint32_t hash = UINT32_C(2166136261);
    for (unsigned i = 0; i < 8; i++) {
        hash = (hash ^ (uint32_t)(key & 255)) * UINT32_C(16777619);
        key >>= 8;
    }
    return hash & (buckets - 1);
}

static void
wl_columnar_payload_join_cancel(void *context)
{
    wl_columnar_payload_join_t *p = context;
    p->stopped = true;
    if (p->error ==
        WL_COLUMNAR_SPILL_OK) p->error = WL_COLUMNAR_SPILL_CANCELLED;
    wl_columnar_payload_join_source_release(&p->left);
    wl_columnar_payload_join_source_release(&p->right);
    wl_columnar_payload_join_free(p->index); p->index = NULL;
    wl_columnar_payload_join_free(p->projection); p->projection = NULL;
    col_rel_destroy(p->batch); p->batch = NULL;
}

static void
wl_columnar_payload_join_destroy(void *context)
{
    wl_columnar_payload_join_t *p = context;
    wl_columnar_memory_governor_ref_t *g = p->governor;
    wl_columnar_payload_join_cancel(p);
    wl_columnar_payload_join_free(p);
    wl_columnar_memory_governor_ref_release(g);
}

static bool
wl_columnar_payload_join_validate(void *context,
    const wl_columnar_continuation_cursor_t *cursor)
{
    wl_columnar_payload_join_t *p = context;
    return !p->stopped && cursor->input_identity == p->identity
           && cursor->arrangement_identity == p->identity &&
           cursor->generation == 1;
}

static void
wl_columnar_payload_join_disposition(void *context,
    const wl_columnar_continuation_batch_t *batch, bool committed)
{
    (void)batch; (void)committed;
    wl_columnar_payload_join_t *p = context;
    if (p->batch) p->batch->nrows = 0;
}

static wl_columnar_continuation_status_t
wl_columnar_payload_join_produce(void *context,
    const wl_columnar_continuation_cursor_t *cursor,
    wl_columnar_continuation_batch_t *batch)
{
    wl_columnar_payload_join_t *p = context;
    if (p->error !=
        WL_COLUMNAR_SPILL_OK) return WL_COLUMNAR_CONTINUATION_INVALID;
    uint32_t work = 0;
    while (p->phase != WL_COLUMNAR_PAYLOAD_JOIN_DONE
        && work < WL_COLUMNAR_PAYLOAD_JOIN_QUANTUM
        && p->batch->nrows < p->capacity) {
        if (p->cancel && p->cancel(p->cancel_context)) {
            p->error = WL_COLUMNAR_SPILL_CANCELLED; break;
        }
        work++;
        if (p->phase == WL_COLUMNAR_PAYLOAD_JOIN_CLEAR) {
            if (p->clear_bucket < p->buckets)
                p->index->slots[p->clear_bucket++] =
                    WL_COLUMNAR_PAYLOAD_JOIN_END;
            else p->phase = WL_COLUMNAR_PAYLOAD_JOIN_BUILD;
            continue;
        }
        if (p->phase == WL_COLUMNAR_PAYLOAD_JOIN_BUILD) {
            if (p->build_row == p->window_rows) {
                p->phase = WL_COLUMNAR_PAYLOAD_JOIN_PROBE;
                continue;
            }
            const uint64_t *lanes;
            wl_columnar_spill_metadata_t meta;
            p->error = wl_columnar_payload_join_source_read(&p->right,
                    p->window_start + p->build_row, p->cancel,
                    p->cancel_context, &lanes, &meta);
            if (p->error != WL_COLUMNAR_SPILL_OK) break;
            uint32_t hash = wl_columnar_payload_join_hash(
                lanes[p->right_key], p->buckets);
            p->index->slots[(uint64_t)p->buckets + p->build_row] =
                p->index->slots[hash];
            p->index->slots[hash] = p->build_row++;
            continue;
        }
        if (p->row == p->left_rows) {
            /* Every window is within [0, right_rows], so this sum fits uint32.
             * Links are local ordinals; payload reads retain global identity. */
            p->window_start += p->window_rows;
            if (p->window_start == p->right_rows) {
                p->phase = WL_COLUMNAR_PAYLOAD_JOIN_DONE;
                continue;
            }
            p->window_rows = p->right_rows - p->window_start;
            if (p->window_rows > p->window_capacity)
                p->window_rows = p->window_capacity;
            p->row = 0; p->started = false; p->left_lanes = NULL;
            p->candidate = WL_COLUMNAR_PAYLOAD_JOIN_END;
            p->clear_bucket = 0; p->build_row = 0;
            p->phase = WL_COLUMNAR_PAYLOAD_JOIN_CLEAR;
            continue;
        }
        if (!p->started) {
            wl_columnar_spill_metadata_t meta;
            p->error = wl_columnar_payload_join_source_read(&p->left, p->row,
                    p->cancel, p->cancel_context, &p->left_lanes, &meta);
            if (p->error != WL_COLUMNAR_SPILL_OK) break;
            p->left_multiplicity = meta.multiplicity;
            p->candidate = p->index->slots[wl_columnar_payload_join_hash(
                        p->left_lanes[p->left_key], p->buckets)];
            p->started = true;
            continue;
        }
        if (p->candidate == WL_COLUMNAR_PAYLOAD_JOIN_END) {
            p->row++; p->started = false; continue;
        }
        const uint64_t *right;
        wl_columnar_spill_metadata_t meta;
        p->error = wl_columnar_payload_join_source_read(&p->right,
                p->window_start + p->candidate,
                p->cancel, p->cancel_context, &right, &meta);
        if (p->error != WL_COLUMNAR_SPILL_OK) break;
        p->candidate = p->index->slots[(uint64_t)p->buckets + p->candidate];
        if (p->left_lanes[p->left_key] != right[p->right_key]) continue;
        int64_t multiplicity = 1;
        if (p->weighted && wl_columnar_arithmetic_checked_mul_int64(
                p->left_multiplicity, meta.multiplicity, &multiplicity) != 0) {
            p->error = WL_COLUMNAR_SPILL_OVERFLOW;
            break;
        }
        uint32_t row = p->batch->nrows++;
        if (p->weighted)
            p->batch->timestamps[row] = (col_delta_timestamp_t){
                .multiplicity = multiplicity
            };
        for (uint32_t c = 0; c < p->output_cols; c++) {
            uint32_t source = p->projection ? p->projection->columns[c] : c;
            const uint64_t *lane = source < p->left_cols
                ? &p->left_lanes[source] : &right[source - p->left_cols];
            memcpy(&p->batch->columns[c][row], lane, 8);
        }
    }
    if (p->error != WL_COLUMNAR_SPILL_OK) {
        p->batch->nrows = 0;
        return WL_COLUMNAR_CONTINUATION_INVALID;
    }
    if (!p->batch->nrows && p->phase == WL_COLUMNAR_PAYLOAD_JOIN_DONE)
        return WL_COLUMNAR_CONTINUATION_DONE;
    if (cursor->position > UINT64_MAX - work ||
        cursor->sequence == UINT64_MAX){
        p->error = WL_COLUMNAR_SPILL_OVERFLOW;
        p->batch->nrows = 0;
        return WL_COLUMNAR_CONTINUATION_INVALID;
    }
    batch->next_cursor = *cursor;
    batch->next_cursor.position += work;
    batch->next_cursor.sequence++;
    if (!p->batch->nrows) return WL_COLUMNAR_CONTINUATION_PROGRESS;
    batch->payload = p->batch; batch->rows = p->batch->nrows;
    batch->bytes = (uint64_t)p->batch->ncols * 8 * batch->rows;
    batch->complete = p->phase == WL_COLUMNAR_PAYLOAD_JOIN_DONE;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_spill_status_t
wl_columnar_payload_join_source_validate(
    wl_columnar_payload_join_source_t source,
    uint32_t key, uint32_t *cols, uint32_t *rows, const wl_intern_t *intern)
{
    const wirelog_column_type_t *types =
        wl_columnar_payload_join_source_schema(source, cols, rows);
    if (!types || key >= *cols) return WL_COLUMNAR_SPILL_INVALID;
    uint32_t logical = 0;
    const wirelog_column_t *columns = source.snapshot
        ? wl_columnar_trace_store_snapshot_inline_schema(source.snapshot,
            &logical)
        : wl_columnar_batch_payload_inline_schema(source.payload, &logical);
    bool owns_intern = intern && (source.snapshot
        ? wl_columnar_trace_store_snapshot_uses_intern(source.snapshot, intern)
        : wl_columnar_batch_payload_uses_intern(source.payload, intern));
    if (columns && (!owns_intern ||
        wl_columnar_spill_validate_inline_schema(types, *cols, columns,
        logical) != WL_COLUMNAR_SPILL_OK))
        return WL_COLUMNAR_SPILL_UNSUPPORTED;
    if (types[key] != WIRELOG_TYPE_INT64 &&
        !(intern && types[key] == WIRELOG_TYPE_STRING))
        return WL_COLUMNAR_SPILL_UNSUPPORTED;
    /* Audit every lane, including non-keys and projected-away references.
     * Old factories pass NULL and retain their complete numeric restriction. */
    for (uint32_t c = 0; c < *cols; c++)
        if (types[c] == WIRELOG_TYPE_STRING && !owns_intern)
            return WL_COLUMNAR_SPILL_UNSUPPORTED;
    return WL_COLUMNAR_SPILL_OK;
}

static wl_columnar_spill_status_t
wl_columnar_payload_join_create_impl(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_payload_join_source_t left,
    wl_columnar_payload_join_source_t right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_continuation_t **out, bool weighted, bool bounded,
    uint64_t index_bytes, const uint32_t *projection, uint32_t project_count,
    const wl_intern_t *intern)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!g || !wl_columnar_payload_join_source_governor(left, g)
        || !wl_columnar_payload_join_source_governor(right,
        g)) return WL_COLUMNAR_SPILL_INVALID;
    uint32_t lc, lr, rc, rr;
    wl_columnar_spill_status_t status =
        wl_columnar_payload_join_source_validate(left,
            left_key, &lc, &lr, intern);
    if (status != WL_COLUMNAR_SPILL_OK) return status;
    status = wl_columnar_payload_join_source_validate(right, right_key, &rc,
            &rr,
            intern);
    if (status != WL_COLUMNAR_SPILL_OK) return status;
    if (wl_columnar_payload_join_source_schema(left, NULL, NULL)[left_key] !=
        wl_columnar_payload_join_source_schema(right, NULL, NULL)[right_key])
        return WL_COLUMNAR_SPILL_UNSUPPORTED;
    if (lc > UINT32_MAX - rc) return WL_COLUMNAR_SPILL_OVERFLOW;
    uint32_t input_cols = lc + rc;
    if (project_count && !projection) return WL_COLUMNAR_SPILL_INVALID;
    uint32_t cols = project_count ? project_count : input_cols;
    uint64_t producer_bytes = sizeof(wl_columnar_payload_join_t)
        + (uint64_t)cols * sizeof(wirelog_column_type_t);
    uint64_t projection_bytes = sizeof(wl_columnar_payload_join_projection_t)
        + (uint64_t)project_count * sizeof(uint32_t);
    if (producer_bytes > SIZE_MAX || projection_bytes > SIZE_MAX)
        return WL_COLUMNAR_SPILL_OVERFLOW;
    for (uint32_t c = 0; c < project_count; c++)
        if (projection[c] >= input_cols) return WL_COLUMNAR_SPILL_INVALID;
    uint64_t capacity = batch_bytes / ((uint64_t)cols * 8);
    if (!capacity) return WL_COLUMNAR_SPILL_MEMORY;
    if (capacity > UINT32_MAX) capacity = UINT32_MAX;
    uint32_t window_rows = rr;
    if (bounded) {
        if (index_bytes < wl_columnar_payload_join_index_bytes(rr ? 1 : 0))
            return WL_COLUMNAR_SPILL_MEMORY;
        uint32_t lo = 0, hi = rr;
        while (lo < hi) {
            uint32_t mid = (uint32_t)(((uint64_t)lo + hi + 1) / 2);
            if (wl_columnar_payload_join_index_bytes(mid) <= index_bytes)
                lo = mid;
            else hi = mid - 1;
        }
        window_rows = lo;
    }
    uint32_t buckets = wl_columnar_payload_join_buckets(window_rows);
    void *allocation;
    status = wl_columnar_payload_join_allocate(g, producer_bytes, &allocation);
    if (status != WL_COLUMNAR_SPILL_OK) return status;
    wl_columnar_payload_join_t *p = allocation;
    p->governor = g; wl_columnar_memory_governor_ref_retain(g);
    p->left_cols = lc; p->right_cols = rc; p->left_rows = lr;
    p->right_rows = rr;
    p->window_rows = window_rows; p->window_capacity = window_rows;
    p->phase = !lr || !rr ? WL_COLUMNAR_PAYLOAD_JOIN_DONE : bounded
        ? WL_COLUMNAR_PAYLOAD_JOIN_CLEAR : WL_COLUMNAR_PAYLOAD_JOIN_PROBE;
    p->left_key = left_key; p->right_key = right_key; p->buckets = buckets;
    p->weighted = weighted;
    p->output_cols = cols;
    p->capacity = (uint32_t)capacity; p->cancel = cancel;
    p->cancel_context = cancel_context;
    status = wl_columnar_payload_join_source_acquire(left, &p->left);
    if (status != WL_COLUMNAR_SPILL_OK) goto fail;
    status = wl_columnar_payload_join_source_acquire(right, &p->right);
    if (status != WL_COLUMNAR_SPILL_OK) goto fail;
    if (project_count) {
        status = wl_columnar_payload_join_allocate(g, projection_bytes,
                &allocation);
        if (status != WL_COLUMNAR_SPILL_OK) goto fail;
        p->projection = allocation;
        memcpy(p->projection->columns, projection,
            (size_t)project_count * sizeof(uint32_t));
    }
    status = wl_columnar_payload_join_allocate(g,
            wl_columnar_payload_join_index_bytes(window_rows), &allocation);
    if (status != WL_COLUMNAR_SPILL_OK) goto fail;
    p->index = allocation;
    for (uint32_t b = 0; !bounded && b < buckets;
        b++) p->index->slots[b] = WL_COLUMNAR_PAYLOAD_JOIN_END;
    for (unsigned side = 0; side < 2; side++) {
        uint32_t rows = side ? rr : lr;
        for (uint32_t row = 0; row < rows; row++) {
            const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
            status =
                wl_columnar_payload_join_source_read(side ? &p->right :
                    &p->left,
                    row, cancel, cancel_context, &lanes, &meta);
            if (status != WL_COLUMNAR_SPILL_OK) goto fail;
            if (meta.iteration || meta.stratum || meta.worker ||
                (!weighted && meta.multiplicity != 1)) {
                status = WL_COLUMNAR_SPILL_UNSUPPORTED; goto fail;
            }
            if (side && !bounded) {
                uint32_t hash = wl_columnar_payload_join_hash(lanes[right_key],
                        buckets);
                p->index->slots[(uint64_t)buckets +
                    row] = p->index->slots[hash];
                p->index->slots[hash] = row;
            }
        }
    }
    const wirelog_column_type_t *left_types =
        wl_columnar_payload_join_source_schema(left, NULL, NULL);
    const wirelog_column_type_t *right_types =
        wl_columnar_payload_join_source_schema(right, NULL, NULL);
    for (uint32_t c = 0; c < cols; c++) {
        uint32_t source = p->projection ? p->projection->columns[c] : c;
        p->types[c] = source <
            lc ? left_types[source] : right_types[source - lc];
    }
    int error = wl_columnar_relation_new_auto_governed("$payload_join", cols,
            p->capacity, weighted, g, &p->batch);
    if (!error) error = col_rel_set_column_types(p->batch, p->types, cols);
    if (error) {
        status = error == ENOSPC ||
            (error == ENOMEM && p->batch &&
            p->batch->memory_budget_denial_pending)
            ? WL_COLUMNAR_SPILL_MEMORY : error ==
            ENOMEM ? WL_COLUMNAR_SPILL_ALLOCATION
            : error ==
            EOVERFLOW ? WL_COLUMNAR_SPILL_OVERFLOW : WL_COLUMNAR_SPILL_INVALID;
        goto fail;
    }
    p->identity = p->batch->relation_identity;
    wl_columnar_continuation_producer_t producer = {
        .context = p, .produce = wl_columnar_payload_join_produce,
        .validate = wl_columnar_payload_join_validate,
        .disposition = wl_columnar_payload_join_disposition,
        .cancel = wl_columnar_payload_join_cancel,
        .destroy = wl_columnar_payload_join_destroy
    };
    wl_columnar_continuation_cursor_t cursor = {.input_identity = p->identity,
                                                .arrangement_identity =
                                                    p->identity,
                                                .generation = 1};
    *out = wl_columnar_continuation_create(&producer, &cursor);
    if (*out) return WL_COLUMNAR_SPILL_OK;
    status = WL_COLUMNAR_SPILL_ALLOCATION;
fail:
    wl_columnar_payload_join_destroy(p);
    return status;
}

wl_columnar_spill_status_t
wl_columnar_payload_join_error(const wl_columnar_continuation_t *cont)
{
    wl_columnar_payload_join_t *p =
        wl_columnar_continuation_producer_context(cont);
    return p ? p->error : WL_COLUMNAR_SPILL_INVALID;
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.payload = left},
               (wl_columnar_payload_join_source_t){.payload = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, false,
               false, 0, NULL, 0, NULL);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_weighted(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.payload = left},
               (wl_columnar_payload_join_source_t){.payload = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, true,
               false, 0, NULL, 0, NULL);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_bounded(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    uint64_t index_bytes, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.payload = left},
               (wl_columnar_payload_join_source_t){.payload = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, false,
               true, index_bytes, NULL, 0, NULL);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_weighted_bounded(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    uint64_t index_bytes, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.payload = left},
               (wl_columnar_payload_join_source_t){.payload = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, true,
               true, index_bytes, NULL, 0, NULL);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_projected_bounded(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, const uint32_t *projection,
    uint32_t project_count, uint64_t batch_bytes, uint64_t index_bytes,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.payload = left},
               (wl_columnar_payload_join_source_t){.payload = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, false,
               true, index_bytes, projection, project_count, NULL);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_reference_projected_bounded(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, const uint32_t *projection,
    uint32_t project_count, uint64_t batch_bytes, uint64_t index_bytes,
    const wl_intern_t *intern, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.payload = left},
               (wl_columnar_payload_join_source_t){.payload = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, false,
               true, index_bytes, projection, project_count, intern);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_trace_bounded(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_trace_store_snapshot_t *left,
    wl_columnar_trace_store_snapshot_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    uint64_t index_bytes, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.snapshot = left},
               (wl_columnar_payload_join_source_t){.snapshot = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, false,
               true, index_bytes, NULL, 0, NULL);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_reference_trace_projected_bounded(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_trace_store_snapshot_t *left,
    wl_columnar_trace_store_snapshot_t *right,
    uint32_t left_key, uint32_t right_key, const uint32_t *projection,
    uint32_t project_count, uint64_t batch_bytes, uint64_t index_bytes,
    const wl_intern_t *intern, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.snapshot = left},
               (wl_columnar_payload_join_source_t){.snapshot = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, false,
               true, index_bytes, projection, project_count, intern);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_weighted_reference_trace_projected_bounded(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_trace_store_snapshot_t *left,
    wl_columnar_trace_store_snapshot_t *right,
    uint32_t left_key, uint32_t right_key, const uint32_t *projection,
    uint32_t project_count, uint64_t batch_bytes, uint64_t index_bytes,
    const wl_intern_t *intern, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.snapshot = left},
               (wl_columnar_payload_join_source_t){.snapshot = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, true,
               true, index_bytes, projection, project_count, intern);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_reference_delta_trace_projected_bounded(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *left,
    wl_columnar_trace_store_snapshot_t *right,
    uint32_t left_key, uint32_t right_key, const uint32_t *projection,
    uint32_t project_count, uint64_t batch_bytes, uint64_t index_bytes,
    const wl_intern_t *intern, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.payload = left},
               (wl_columnar_payload_join_source_t){.snapshot = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, true,
               true, index_bytes, projection, project_count, intern);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_weighted_trace_bounded(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_trace_store_snapshot_t *left,
    wl_columnar_trace_store_snapshot_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    uint64_t index_bytes, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.snapshot = left},
               (wl_columnar_payload_join_source_t){.snapshot = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, true,
               true, index_bytes, NULL, 0, NULL);
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create_delta_trace_bounded(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *left,
    wl_columnar_trace_store_snapshot_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    uint64_t index_bytes, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_continuation_t **out)
{
    return wl_columnar_payload_join_create_impl(g,
               (wl_columnar_payload_join_source_t){.payload = left},
               (wl_columnar_payload_join_source_t){.snapshot = right}, left_key,
               right_key, batch_bytes, cancel, cancel_context, out, true,
               true, index_bytes, NULL, 0, NULL);
}
