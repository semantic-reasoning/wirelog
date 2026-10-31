#include "columnar/batch_payload.h"
#include "columnar/internal.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#if defined(_MSC_VER)
typedef char wl_columnar_batch_payload_inline_arity_check_
    [(WL_COMPOUND_INLINE_MAX_ARITY ==
        WL_COLUMNAR_SPILL_INLINE_MAX_ARITY) ? 1 : -1];
#else
_Static_assert(WL_COMPOUND_INLINE_MAX_ARITY ==
    WL_COLUMNAR_SPILL_INLINE_MAX_ARITY,
    "spill and relation INLINE arity bounds must agree");
#endif

typedef struct {
    wl_columnar_memory_reservation_t reservation;
} wl_columnar_batch_payload_allocation_t;

struct wl_columnar_batch_payload {
    wl_columnar_batch_payload_allocation_t allocation;
    wl_columnar_memory_governor_ref_t *governor;
    wl_columnar_spill_segment_t *segment;
    wl_intern_t *intern;
    uint64_t readers, data_offset, row_bytes;
    uint32_t ncols, nrows, logical_count;
    wirelog_column_type_t types[];
};

struct wl_columnar_batch_payload_reader {
    wl_columnar_batch_payload_allocation_t allocation;
    wl_columnar_batch_payload_t *payload;
    wl_columnar_spill_reader_t *spill_reader;
};

#ifdef WL_TEST_BATCH_PAYLOAD
static unsigned wl_columnar_batch_payload_allocation_fail;
void wl_columnar_batch_payload_test_fail_allocation(unsigned count)
{
    wl_columnar_batch_payload_allocation_fail = count;
}
#endif

static wl_columnar_spill_status_t
wl_columnar_batch_payload_allocate(wl_columnar_memory_governor_ref_t *g,
    uint64_t bytes, void **out)
{
    *out = NULL;
    if (bytes > SIZE_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_reservation_init(&pending);
    wl_columnar_memory_admission_status_t rc =
        wl_columnar_memory_reserve_checked(
        wl_columnar_memory_governor_ref_get(g), bytes, &pending);
    if (rc > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return rc == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
            ? WL_COLUMNAR_SPILL_MEMORY : WL_COLUMNAR_SPILL_OVERFLOW;
    bool fail = false;
#ifdef WL_TEST_BATCH_PAYLOAD
    if (wl_columnar_batch_payload_allocation_fail)
        fail = --wl_columnar_batch_payload_allocation_fail == 0;
#endif
    wl_columnar_batch_payload_allocation_t *p = fail ? NULL : calloc(1,
            (size_t)bytes);
    if (!p) {
        if (!wl_columnar_memory_rollback(&pending)) abort();
        return WL_COLUMNAR_SPILL_ALLOCATION;
    }
    wl_columnar_memory_reservation_init(&p->reservation);
    if (!wl_columnar_memory_reservation_move(&p->reservation, &pending)
        || !wl_columnar_memory_commit(&p->reservation, p)) abort();
    *out = p;
    return WL_COLUMNAR_SPILL_OK;
}

static void
wl_columnar_batch_payload_free(void *ptr)
{
    wl_columnar_batch_payload_allocation_t *p = ptr;
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reservation_move(&token, &p->reservation)) abort();
    free(p);
    if (!wl_columnar_memory_release(&token)) abort();
}

static uint64_t
wl_columnar_batch_payload_logical_offset(uint32_t ncols)
{
    return (sizeof(wl_columnar_batch_payload_t)
           + (uint64_t)ncols * sizeof(wirelog_column_type_t) + 7)
           & ~UINT64_C(7);
}

static wirelog_column_t *
wl_columnar_batch_payload_logical(wl_columnar_batch_payload_t *p)
{
    return (wirelog_column_t *)((char *)p
           + wl_columnar_batch_payload_logical_offset(p->ncols));
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_floor(uint32_t ncols, uint32_t nrows,
    wl_columnar_batch_payload_storage_t storage, uint64_t *payload_bytes,
    uint64_t *reader_bytes)
{
    return wl_columnar_batch_payload_floor_with_inline(ncols, nrows, 0,
               storage, payload_bytes, reader_bytes);
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_floor_with_inline(uint32_t ncols, uint32_t nrows,
    uint32_t logical_count, wl_columnar_batch_payload_storage_t storage,
    uint64_t *payload_bytes, uint64_t *reader_bytes)
{
    if (!payload_bytes || !reader_bytes) return WL_COLUMNAR_SPILL_INVALID;
    *payload_bytes = 0; *reader_bytes = 0;
    if (!ncols || logical_count > ncols ||
        (storage != WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT
        && storage != WL_COLUMNAR_BATCH_PAYLOAD_SPILLED))
        return WL_COLUMNAR_SPILL_INVALID;
    uint64_t header = wl_columnar_batch_payload_logical_offset(ncols)
        + (uint64_t)logical_count * sizeof(wirelog_column_t);
    uint64_t row = (uint64_t)ncols * 8 + sizeof(wl_columnar_spill_metadata_t);
    uint64_t bytes = 0;
    if (storage == WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT
        && (!wl_columnar_memory_size_mul(row, nrows, &bytes)
        || !wl_columnar_memory_size_add(header, bytes, &header)))
        return WL_COLUMNAR_SPILL_OVERFLOW;
    if (header > SIZE_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    *payload_bytes = header;
    *reader_bytes = sizeof(wl_columnar_batch_payload_reader_t);
    return WL_COLUMNAR_SPILL_OK;
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_create_spilled(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_spill_store_t *store, const wirelog_column_type_t *types,
    uint32_t ncols, uint32_t rows, wl_columnar_spill_source_fn source,
    void *source_context, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context, wl_columnar_batch_payload_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!g || !types || (rows && !source)
        || !wl_columnar_spill_store_uses_governor(store, g))
        return WL_COLUMNAR_SPILL_INVALID;
    uint64_t bytes, reader_bytes;
    wl_columnar_spill_status_t rc = wl_columnar_batch_payload_floor(ncols,
            rows, WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, &bytes, &reader_bytes);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    void *allocation = NULL;
    rc = wl_columnar_batch_payload_allocate(g, bytes, &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    wl_columnar_batch_payload_t *p = allocation;
    p->governor = g; wl_columnar_memory_governor_ref_retain(g);
    p->ncols = ncols; p->nrows = rows;
    memcpy(p->types, types, (size_t)ncols * sizeof(*types));
    rc = wl_columnar_spill_create(store, p->types, ncols, rows, source,
            source_context, cancel, cancel_context, &p->segment);
    if (rc == WL_COLUMNAR_SPILL_OK && cancel && cancel(cancel_context))
        rc = WL_COLUMNAR_SPILL_CANCELLED;
    if (rc == WL_COLUMNAR_SPILL_OK) *out = p;
    else if (wl_columnar_batch_payload_release(p) != WL_COLUMNAR_SPILL_OK)
        abort();
    return rc;
}

static wl_columnar_spill_status_t
wl_columnar_batch_payload_source(void *context, uint64_t ordinal,
    uint64_t *lanes, wl_columnar_spill_metadata_t *metadata)
{
    const col_rel_t *source = context;
    for (uint32_t c = 0; c < source->ncols; c++)
        memcpy(&lanes[c], &source->columns[c][ordinal], sizeof(*lanes));
    *metadata = (wl_columnar_spill_metadata_t){ .multiplicity = 1 };
    if (source->timestamps) {
        const col_delta_timestamp_t *ts = &source->timestamps[ordinal];
        metadata->iteration = ts->iteration; metadata->stratum = ts->stratum;
        metadata->worker = ts->worker;
        metadata->multiplicity = ts->multiplicity;
    }
    return WL_COLUMNAR_SPILL_OK;
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_capture(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_spill_store_t *store, const col_rel_t *source, uint32_t rows,
    wl_columnar_batch_payload_storage_t storage,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_batch_payload_t **out)
{
    return wl_columnar_batch_payload_capture_with_intern(g, store, source,
               rows, storage, NULL, cancel, cancel_context, out);
}

static bool
wl_columnar_batch_payload_token_owned(const col_rel_t *r,
    const wl_columnar_memory_reservation_t *token, uint64_t bytes)
{
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (!bytes)
        return state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY ||
               state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED;
    return token->identity == token && token->bytes == bytes &&
           token->governor ==
           wl_columnar_memory_governor_ref_get(r->memory_governor) &&
           state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED &&
           token->replacement_bytes == 0 &&
           atomic_load_explicit(&token->owner_bits, memory_order_acquire) ==
           (uintptr_t)r;
}

bool
wl_columnar_batch_payload_can_take(const col_rel_t *r)
{
    uint64_t bytes;
    return r && r->memory_governor && !r->pool_owned && !r->arena_owned &&
           !r->col_shared && r->storage_owner == r &&
           r->storage_owner_identity == r->relation_identity &&
           r->storage_owner_generation == r->storage_generation &&
           wl_columnar_relation_snapshot_valid(wl_columnar_relation_snapshot(
                   r)) &&
           !r->deferred_relation_session && !r->deferred_relation_next &&
           !r->retract_backup_columns && !r->retract_backup_timestamps &&
           !r->retract_backup_nrows && !r->retract_backup_base_nrows &&
           !r->retract_backup_capacity &&
           !r->retract_backup_timestamp_capacity &&
           !r->retract_backup_sorted_nrows && !r->retract_backup_run_count &&
           r->descriptor_reserved_bytes != 0 &&
           col_rel_retained_live_bytes(r, &bytes) &&
           bytes == r->retained_reserved_bytes &&
           wl_columnar_batch_payload_token_owned(r, &r->retained_reservation,
               r->retained_reserved_bytes) &&
           wl_columnar_batch_payload_token_owned(r, &r->descriptor_reservation,
               r->descriptor_reserved_bytes) &&
           wl_columnar_batch_payload_token_owned(r, &r->metadata_reservation,
               r->metadata_reserved_bytes) &&
           wl_columnar_batch_payload_token_owned(r, &r->dedup_reservation,
               r->dedup_reserved_bytes) &&
           !r->pool_name_reserved_bytes &&
           wl_columnar_batch_payload_token_owned(r, &r->pool_name_reservation,
               0);
}

static wl_columnar_spill_status_t
wl_columnar_batch_payload_take_impl(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_spill_store_t *store, col_rel_t **source,
    wl_intern_t *intern, const wirelog_column_t *columns,
    uint32_t logical_count,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_batch_payload_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!g || !source || !*source || (*source)->memory_governor != g ||
        !wl_columnar_spill_store_uses_governor(store, g))
        return WL_COLUMNAR_SPILL_INVALID;
    if (!wl_columnar_batch_payload_can_take(*source))
        return WL_COLUMNAR_SPILL_UNSUPPORTED;
    wl_columnar_batch_payload_t *p = NULL;
    wl_columnar_spill_status_t rc = logical_count
        ? wl_columnar_batch_payload_capture_with_inline(g, store, *source,
            (*source)->nrows, WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, columns,
            logical_count, intern, cancel, cancel_context, &p)
        : wl_columnar_batch_payload_capture_with_intern(g, store, *source,
            (*source)->nrows, WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, intern,
            cancel, cancel_context, &p);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    if (cancel && cancel(cancel_context)) {
        rc = WL_COLUMNAR_SPILL_CANCELLED;
    } else {
        int retired = col_rel_destroy_checked(*source);
        if (retired == 0) {
            *source = NULL;
            *out = p;
            return WL_COLUMNAR_SPILL_OK;
        }
        rc = retired ==
            EBUSY ? WL_COLUMNAR_SPILL_BUSY : WL_COLUMNAR_SPILL_INVALID;
    }
    if (wl_columnar_batch_payload_release(p) != WL_COLUMNAR_SPILL_OK) abort();
    return rc;
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_take(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_spill_store_t *store, col_rel_t **source,
    wl_columnar_spill_cancel_fn cancel, void *context,
    wl_columnar_batch_payload_t **out)
{
    return wl_columnar_batch_payload_take_impl(g, store, source, NULL, NULL, 0,
               cancel, context, out);
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_take_with_intern(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_spill_store_t *store, col_rel_t **source, wl_intern_t *intern,
    wl_columnar_spill_cancel_fn cancel, void *context,
    wl_columnar_batch_payload_t **out)
{
    return wl_columnar_batch_payload_take_impl(g, store, source, intern, NULL,
               0,
               cancel, context, out);
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_take_with_inline(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_spill_store_t *store, col_rel_t **source,
    const wirelog_column_t *columns, uint32_t logical_count,
    wl_intern_t *intern,
    wl_columnar_spill_cancel_fn cancel, void *context,
    wl_columnar_batch_payload_t **out)
{
    if (!columns || !logical_count || !intern) {
        if (out) *out = NULL;
        return WL_COLUMNAR_SPILL_INVALID;
    }
    return wl_columnar_batch_payload_take_impl(g, store, source, intern,
               columns,
               logical_count, cancel, context, out);
}

static wl_columnar_spill_status_t
wl_columnar_batch_payload_capture_impl(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_spill_store_t *store, const col_rel_t *source, uint32_t rows,
    wl_columnar_batch_payload_storage_t storage,
    wl_intern_t *intern, const wirelog_column_t *columns,
    uint32_t logical_count, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context,
    wl_columnar_batch_payload_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!g || !source || (storage == WL_COLUMNAR_BATCH_PAYLOAD_SPILLED
        && !wl_columnar_spill_store_uses_governor(store, g)))
        return WL_COLUMNAR_SPILL_INVALID;
    wl_columnar_source_access_reader_t lease = {0};
    int access = col_rel_source_reader_acquire(source, &lease);
    if (access) return access == EBUSY ? WL_COLUMNAR_SPILL_BUSY
        : WL_COLUMNAR_SPILL_INVALID;
    wl_columnar_spill_status_t rc = WL_COLUMNAR_SPILL_UNSUPPORTED;
    wl_columnar_batch_payload_t *p = NULL;
    uint64_t bytes, reader_bytes, header;
    bool strings = logical_count != 0;
    if (source->has_graph_column || !source->ncols) goto done;
    if (logical_count) {
        rc = WL_COLUMNAR_SPILL_INVALID;
        if (!intern || !source->column_types || !source->compound_arity_map
            || source->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
            || source->compound_arity_len != logical_count) goto done;
        rc = wl_columnar_spill_validate_inline_schema(source->column_types,
                source->ncols, columns, logical_count);
        if (rc != WL_COLUMNAR_SPILL_OK) goto done;
        uint32_t compounds = 0, first = 0;
        for (uint32_t i = 0; i < logical_count; i++) {
            bool inlined = columns[i].compound_kind ==
                WIRELOG_COMPOUND_KIND_INLINE;
            uint32_t width = inlined ? columns[i].compound_arity : 1;
            if (width != source->compound_arity_map[i]) {
                rc = WL_COLUMNAR_SPILL_INVALID; goto done;
            }
            if (inlined && compounds++ == 0)
                first = columns[i].compound_inline_col_offset;
        }
        if (compounds != source->compound_count ||
            first != source->inline_physical_offset){
            rc = WL_COLUMNAR_SPILL_INVALID; goto done;
        }
    } else if (source->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || source->compound_count || source->compound_arity_map
        || source->compound_arity_len ||
        source->inline_physical_offset) goto done;
    rc = WL_COLUMNAR_SPILL_INVALID;
    if (rows > source->nrows || source->nrows > source->capacity
        || !source->columns || (source->timestamps
        && rows > source->timestamp_capacity)) goto done;
    for (uint32_t c = 0; c < source->ncols; c++) {
        if (!source->columns[c]) goto done;
        wirelog_column_type_t type = source->column_types
            ? source->column_types[c] : WIRELOG_TYPE_INT64;
        if (type == WIRELOG_TYPE_STRING && intern) {
            strings = true; continue;
        }
        if (type != WIRELOG_TYPE_INT32 && type != WIRELOG_TYPE_INT64
            && type != WIRELOG_TYPE_UINT32 && type != WIRELOG_TYPE_UINT64
            && type != WIRELOG_TYPE_FLOAT && type != WIRELOG_TYPE_BOOL) {
            rc = WL_COLUMNAR_SPILL_UNSUPPORTED; goto done;
        }
    }
    rc = wl_columnar_batch_payload_floor_with_inline(source->ncols, rows,
            logical_count, storage,
            &bytes, &reader_bytes);
    if (rc != WL_COLUMNAR_SPILL_OK) goto done;
    rc = wl_columnar_batch_payload_floor_with_inline(source->ncols, 0,
            logical_count, storage,
            &header, &reader_bytes);
    if (rc != WL_COLUMNAR_SPILL_OK) goto done;
    if (cancel && cancel(cancel_context)) {
        rc = WL_COLUMNAR_SPILL_CANCELLED; goto done;
    }
    void *allocation = NULL;
    rc = wl_columnar_batch_payload_allocate(g, bytes, &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) goto done;
    p = allocation; p->governor = g;
    wl_columnar_memory_governor_ref_retain(g);
    if (strings) {
        int pin = wl_intern_pin_acquire(intern, g);
        if (pin) {
            rc = pin == EPIPE ? WL_COLUMNAR_SPILL_CLOSED
                : pin == EOVERFLOW ? WL_COLUMNAR_SPILL_OVERFLOW
                : WL_COLUMNAR_SPILL_INVALID;
            goto done;
        }
        p->intern = intern;
    }
    p->ncols = source->ncols; p->nrows = rows; p->data_offset = header;
    p->logical_count = logical_count;
    for (uint32_t i = 0; i < logical_count; i++) {
        if (columns[i].compound_kind == WIRELOG_COMPOUND_KIND_INLINE
            && !wl_intern_reverse(intern, columns[i].compound_functor_id)) {
            rc = WL_COLUMNAR_SPILL_INVALID; goto done;
        }
        wl_columnar_batch_payload_logical(p)[i] = columns[i];
        wl_columnar_batch_payload_logical(p)[i].name = NULL;
    }
    p->row_bytes = (uint64_t)p->ncols * 8 +
        sizeof(wl_columnar_spill_metadata_t);
    for (uint32_t c = 0; c < p->ncols; c++)
        p->types[c] =
            source->column_types ? source->column_types[c] : WIRELOG_TYPE_INT64;
    if (storage == WL_COLUMNAR_BATCH_PAYLOAD_SPILLED) {
        if (logical_count)
            rc = wl_columnar_spill_create_with_inline(store, p->types, p->ncols,
                    rows, wl_columnar_batch_payload_source, (void *)source,
                    wl_columnar_batch_payload_logical(p), logical_count,
                    p->intern,
                    cancel, cancel_context, &p->segment);
        else
            rc = wl_columnar_spill_create_with_intern(store, p->types, p->ncols,
                    rows, wl_columnar_batch_payload_source, (void *)source,
                    p->intern,
                    cancel, cancel_context, &p->segment);
    } else {
        for (uint32_t row = 0; row < rows; row++) {
            if (cancel && cancel(cancel_context)) {
                rc = WL_COLUMNAR_SPILL_CANCELLED; break;
            }
            uint64_t *lanes = (uint64_t *)((char *)p + header + row *
                p->row_bytes);
            wl_columnar_spill_metadata_t metadata;
            (void)wl_columnar_batch_payload_source((void *)source, row, lanes,
                &metadata);
            for (uint32_t c = 0; c < p->ncols; c++) {
                if (p->types[c] == WIRELOG_TYPE_STRING &&
                    (lanes[c] > INT64_MAX ||
                    !wl_intern_reverse(p->intern, (int64_t)lanes[c]))) {
                    rc = WL_COLUMNAR_SPILL_INVALID; break;
                }
            }
            if (rc != WL_COLUMNAR_SPILL_OK) break;
            memcpy(lanes + p->ncols, &metadata, sizeof(metadata));
        }
    }
    if (rc == WL_COLUMNAR_SPILL_OK && cancel && cancel(cancel_context))
        rc = WL_COLUMNAR_SPILL_CANCELLED;
done:
    if (col_rel_source_reader_release(&lease) != 0) abort();
    if (rc == WL_COLUMNAR_SPILL_OK) {
        if (p->segment && p->intern) {
            wl_intern_pin_release(p->intern);
            p->intern = NULL; /* Segment now owns the pin. */
        }
        *out = p;
    }else if (p &&
        wl_columnar_batch_payload_release(p) != WL_COLUMNAR_SPILL_OK) abort();
    return rc;
}

static wl_columnar_spill_status_t
wl_columnar_batch_payload_resident_row(void *context, uint64_t ordinal,
    uint64_t *lanes, wl_columnar_spill_metadata_t *metadata)
{
    const wl_columnar_batch_payload_t *p = context;
    const char *row = (const char *)p + p->data_offset + ordinal * p->row_bytes;
    memcpy(lanes, row, (size_t)p->ncols * sizeof(*lanes));
    memcpy(metadata, row + (size_t)p->ncols * sizeof(*lanes),
        sizeof(*metadata));
    return WL_COLUMNAR_SPILL_OK;
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_spill(wl_columnar_batch_payload_t **owner,
    wl_columnar_spill_store_t *store, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context)
{
    if (!owner || !*owner) return WL_COLUMNAR_SPILL_INVALID;
    wl_columnar_batch_payload_t *old = *owner;
    if (old->segment || !wl_columnar_spill_store_uses_governor(store,
        old->governor)) return WL_COLUMNAR_SPILL_INVALID;
    if (old->readers) return WL_COLUMNAR_SPILL_BUSY;
    if (cancel && cancel(cancel_context)) return WL_COLUMNAR_SPILL_CANCELLED;
    uint64_t bytes, reader_bytes;
    wl_columnar_spill_status_t rc =
        wl_columnar_batch_payload_floor_with_inline(old->ncols,
            old->nrows, old->logical_count, WL_COLUMNAR_BATCH_PAYLOAD_SPILLED,
            &bytes,
            &reader_bytes);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    void *allocation = NULL;
    rc = wl_columnar_batch_payload_allocate(old->governor, bytes, &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    wl_columnar_batch_payload_t *replacement = allocation;
    replacement->governor = old->governor;
    wl_columnar_memory_governor_ref_retain(replacement->governor);
    replacement->ncols = old->ncols;
    replacement->nrows = old->nrows;
    memcpy(replacement->types, old->types,
        (size_t)old->ncols * sizeof(*old->types));
    replacement->logical_count = old->logical_count;
    if (old->logical_count) {
        memcpy(wl_columnar_batch_payload_logical(replacement),
            wl_columnar_batch_payload_logical(old),
            (size_t)old->logical_count * sizeof(wirelog_column_t));
        rc = wl_columnar_spill_create_with_inline(store, old->types, old->ncols,
                old->nrows, wl_columnar_batch_payload_resident_row, old,
                wl_columnar_batch_payload_logical(old), old->logical_count,
                old->intern,
                cancel, cancel_context, &replacement->segment);
    } else {
        rc = wl_columnar_spill_create_with_intern(store, old->types, old->ncols,
                old->nrows, wl_columnar_batch_payload_resident_row, old,
                old->intern,
                cancel, cancel_context, &replacement->segment);
    }
    if (rc == WL_COLUMNAR_SPILL_OK && cancel && cancel(cancel_context))
        rc = WL_COLUMNAR_SPILL_CANCELLED;
    if (rc != WL_COLUMNAR_SPILL_OK) {
        if (wl_columnar_batch_payload_release(replacement) !=
            WL_COLUMNAR_SPILL_OK) abort();
        return rc;
    }
    *owner = replacement;
    if (wl_columnar_batch_payload_release(old) != WL_COLUMNAR_SPILL_OK) abort();
    return WL_COLUMNAR_SPILL_OK;
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_release(wl_columnar_batch_payload_t *p)
{
    if (!p) return WL_COLUMNAR_SPILL_INVALID;
    if (p->readers) return WL_COLUMNAR_SPILL_BUSY;
    if (p->segment) {
        wl_columnar_spill_status_t rc = wl_columnar_spill_release(p->segment);
        if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    }
    wl_columnar_memory_governor_ref_t *g = p->governor;
    wl_intern_t *intern = p->intern;
    wl_columnar_batch_payload_free(p);
    if (intern) wl_intern_pin_release(intern);
    wl_columnar_memory_governor_ref_release(g);
    return WL_COLUMNAR_SPILL_OK;
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_acquire(wl_columnar_batch_payload_t *p,
    wl_columnar_batch_payload_reader_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!p || p->readers == UINT64_MAX) return WL_COLUMNAR_SPILL_INVALID;
    wl_columnar_spill_reader_t *spill_reader = NULL;
    wl_columnar_spill_status_t rc = WL_COLUMNAR_SPILL_OK;
    if (p->segment) {
        rc = wl_columnar_spill_acquire(p->segment, &spill_reader);
        if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    }
    void *allocation;
    rc = wl_columnar_batch_payload_allocate(p->governor,
            sizeof(wl_columnar_batch_payload_reader_t), &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) {
        wl_columnar_spill_reader_release(spill_reader);
        return rc;
    }
    wl_columnar_batch_payload_reader_t *r = allocation;
    r->spill_reader = spill_reader;
    r->payload = p; p->readers++; *out = r;
    return WL_COLUMNAR_SPILL_OK;
}

void
wl_columnar_batch_payload_reader_release(wl_columnar_batch_payload_reader_t *r)
{
    if (!r) return;
    if (r->spill_reader) wl_columnar_spill_reader_release(r->spill_reader);
    r->payload->readers--;
    wl_columnar_batch_payload_free(r);
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_read(wl_columnar_batch_payload_reader_t *r,
    uint64_t ordinal, wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    const uint64_t **lanes, wl_columnar_spill_metadata_t *metadata)
{
    if (lanes) *lanes = NULL;
    if (metadata) memset(metadata, 0, sizeof(*metadata));
    if (!r || !lanes || !metadata || ordinal >= r->payload->nrows)
        return WL_COLUMNAR_SPILL_INVALID;
    if (r->spill_reader) return wl_columnar_spill_read(r->spill_reader, ordinal,
                   cancel, cancel_context, lanes, metadata);
    if (cancel && cancel(cancel_context)) return WL_COLUMNAR_SPILL_CANCELLED;
    const wl_columnar_batch_payload_t *p = r->payload;
    *lanes = (const uint64_t *)((const char *)p + p->data_offset + ordinal *
        p->row_bytes);
    memcpy(metadata, *lanes + p->ncols, sizeof(*metadata));
    return WL_COLUMNAR_SPILL_OK;
}

bool
wl_columnar_batch_payload_uses_intern(const wl_columnar_batch_payload_t *p,
    const wl_intern_t *intern)
{
    return p && intern && (p->segment ?
           wl_columnar_spill_segment_uses_intern(p->segment, intern) :
           p->intern == intern);
}

const wirelog_column_type_t *
wl_columnar_batch_payload_schema(const wl_columnar_batch_payload_t *p,
    uint32_t *ncols, uint32_t *nrows)
{
    if (ncols) *ncols = p ? p->ncols : 0;
    if (nrows) *nrows = p ? p->nrows : 0;
    return p ? p->types : NULL;
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_resolve_string(wl_columnar_batch_payload_reader_t *r,
    uint64_t id, const char **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!r) return WL_COLUMNAR_SPILL_INVALID;
    if (r->spill_reader) return wl_columnar_spill_resolve_string(
            r->spill_reader,
            id, out);
    if (!r->payload->intern || id > INT64_MAX) return WL_COLUMNAR_SPILL_INVALID;
    *out = wl_intern_reverse(r->payload->intern, (int64_t)id);
    return *out ? WL_COLUMNAR_SPILL_OK : WL_COLUMNAR_SPILL_INVALID;
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_capture_with_intern(
    wl_columnar_memory_governor_ref_t *g, wl_columnar_spill_store_t *store,
    const col_rel_t *source, uint32_t rows,
    wl_columnar_batch_payload_storage_t storage,
    wl_intern_t *intern, wl_columnar_spill_cancel_fn cancel,
    void *cancel_context,
    wl_columnar_batch_payload_t **out)
{
    return wl_columnar_batch_payload_capture_impl(g, store, source, rows,
               storage, intern, NULL, 0, cancel, cancel_context, out);
}

wl_columnar_spill_status_t
wl_columnar_batch_payload_capture_with_inline(
    wl_columnar_memory_governor_ref_t *g, wl_columnar_spill_store_t *store,
    const col_rel_t *source, uint32_t rows,
    wl_columnar_batch_payload_storage_t storage,
    const wirelog_column_t *columns, uint32_t logical_count,
    wl_intern_t *intern,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_batch_payload_t **out)
{
    if (!columns || !logical_count || !intern) {
        if (out) *out = NULL;
        return WL_COLUMNAR_SPILL_INVALID;
    }
    return wl_columnar_batch_payload_capture_impl(g, store, source, rows,
               storage, intern, columns, logical_count, cancel, cancel_context,
               out);
}

const wirelog_column_t *
wl_columnar_batch_payload_inline_schema(const wl_columnar_batch_payload_t *p,
    uint32_t *logical_count)
{
    if (logical_count) *logical_count = p ? p->logical_count : 0;
    if (!p || !logical_count || !p->logical_count) return NULL;
    return (const wirelog_column_t *)((const char *)p
           + wl_columnar_batch_payload_logical_offset(p->ncols));
}

bool
wl_columnar_batch_payload_uses_governor(const wl_columnar_batch_payload_t *p,
    const wl_columnar_memory_governor_ref_t *g)
{
    return p && g && p->governor == g;
}
