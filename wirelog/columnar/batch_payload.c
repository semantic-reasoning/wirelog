#include "columnar/batch_payload.h"
#include "columnar/internal.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    wl_columnar_memory_reservation_t reservation;
} wl_columnar_batch_payload_allocation_t;

struct wl_columnar_batch_payload {
    wl_columnar_batch_payload_allocation_t allocation;
    wl_columnar_memory_governor_ref_t *governor;
    wl_columnar_spill_segment_t *segment;
    uint64_t readers, data_offset, row_bytes;
    uint32_t ncols, nrows;
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

wl_columnar_spill_status_t
wl_columnar_batch_payload_floor(uint32_t ncols, uint32_t nrows,
    wl_columnar_batch_payload_storage_t storage, uint64_t *payload_bytes,
    uint64_t *reader_bytes)
{
    if (!payload_bytes || !reader_bytes) return WL_COLUMNAR_SPILL_INVALID;
    *payload_bytes = 0; *reader_bytes = 0;
    if (!ncols || (storage != WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT
        && storage != WL_COLUMNAR_BATCH_PAYLOAD_SPILLED))
        return WL_COLUMNAR_SPILL_INVALID;
    uint64_t header = (sizeof(wl_columnar_batch_payload_t)
        + (uint64_t)ncols * sizeof(wirelog_column_type_t) + 7) & ~UINT64_C(7);
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
    if (source->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || source->compound_count || source->compound_arity_map
        || source->compound_arity_len || source->inline_physical_offset
        || source->has_graph_column || !source->ncols) goto done;
    rc = WL_COLUMNAR_SPILL_INVALID;
    if (rows > source->nrows || source->nrows > source->capacity
        || !source->columns || (source->timestamps
        && rows > source->timestamp_capacity)) goto done;
    for (uint32_t c = 0; c < source->ncols; c++) {
        if (!source->columns[c]) goto done;
        wirelog_column_type_t type = source->column_types
            ? source->column_types[c] : WIRELOG_TYPE_INT64;
        if (type != WIRELOG_TYPE_INT32 && type != WIRELOG_TYPE_INT64
            && type != WIRELOG_TYPE_UINT32 && type != WIRELOG_TYPE_UINT64
            && type != WIRELOG_TYPE_FLOAT && type != WIRELOG_TYPE_BOOL) {
            rc = WL_COLUMNAR_SPILL_UNSUPPORTED; goto done;
        }
    }
    rc = wl_columnar_batch_payload_floor(source->ncols, rows, storage,
            &bytes, &reader_bytes);
    if (rc != WL_COLUMNAR_SPILL_OK) goto done;
    rc = wl_columnar_batch_payload_floor(source->ncols, 0, storage,
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
    p->ncols = source->ncols; p->nrows = rows; p->data_offset = header;
    p->row_bytes = (uint64_t)p->ncols * 8 +
        sizeof(wl_columnar_spill_metadata_t);
    for (uint32_t c = 0; c < p->ncols; c++)
        p->types[c] =
            source->column_types ? source->column_types[c] : WIRELOG_TYPE_INT64;
    if (storage == WL_COLUMNAR_BATCH_PAYLOAD_SPILLED) {
        rc = wl_columnar_spill_create(store, p->types, p->ncols, rows,
                wl_columnar_batch_payload_source, (void *)source, cancel,
                cancel_context,
                &p->segment);
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
            memcpy(lanes + p->ncols, &metadata, sizeof(metadata));
        }
    }
    if (rc == WL_COLUMNAR_SPILL_OK && cancel && cancel(cancel_context))
        rc = WL_COLUMNAR_SPILL_CANCELLED;
done:
    if (col_rel_source_reader_release(&lease) != 0) abort();
    if (rc == WL_COLUMNAR_SPILL_OK) *out = p;
    else if (p &&
        wl_columnar_batch_payload_release(p) != WL_COLUMNAR_SPILL_OK) abort();
    return rc;
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
    wl_columnar_batch_payload_free(p);
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

const wirelog_column_type_t *
wl_columnar_batch_payload_schema(const wl_columnar_batch_payload_t *p,
    uint32_t *ncols, uint32_t *nrows)
{
    if (ncols) *ncols = p ? p->ncols : 0;
    if (nrows) *nrows = p ? p->nrows : 0;
    return p ? p->types : NULL;
}
