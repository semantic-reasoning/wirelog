#include "columnar/payload_join.h"
#include "columnar/internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define WL_COLUMNAR_PAYLOAD_JOIN_END UINT32_MAX
#define WL_COLUMNAR_PAYLOAD_JOIN_QUANTUM 256u

typedef struct {
    wl_columnar_memory_reservation_t reservation;
    uint32_t slots[];
} wl_columnar_payload_join_index_t;

typedef struct {
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor;
    wl_columnar_batch_payload_reader_t *left, *right;
    wl_columnar_payload_join_index_t *index;
    col_rel_t *batch;
    wl_columnar_spill_cancel_fn cancel;
    void *cancel_context;
    wl_columnar_spill_status_t error;
    const uint64_t *left_lanes;
    uint64_t identity;
    uint32_t left_rows, right_rows, left_cols, right_cols, left_key, right_key;
    uint32_t buckets, capacity, row, candidate;
    bool started, stopped;
    wirelog_column_type_t types[];
} wl_columnar_payload_join_t;

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
    wl_columnar_batch_payload_reader_release(p->left); p->left = NULL;
    wl_columnar_batch_payload_reader_release(p->right); p->right = NULL;
    wl_columnar_payload_join_free(p->index); p->index = NULL;
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
    while (p->row < p->left_rows && work < WL_COLUMNAR_PAYLOAD_JOIN_QUANTUM
        && p->batch->nrows < p->capacity) {
        if (p->cancel && p->cancel(p->cancel_context)) {
            p->error = WL_COLUMNAR_SPILL_CANCELLED; break;
        }
        work++;
        if (!p->started) {
            wl_columnar_spill_metadata_t meta;
            p->error = wl_columnar_batch_payload_read(p->left, p->row,
                    p->cancel, p->cancel_context, &p->left_lanes, &meta);
            if (p->error != WL_COLUMNAR_SPILL_OK) break;
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
        p->error = wl_columnar_batch_payload_read(p->right, p->candidate,
                p->cancel, p->cancel_context, &right, &meta);
        if (p->error != WL_COLUMNAR_SPILL_OK) break;
        p->candidate = p->index->slots[(uint64_t)p->buckets + p->candidate];
        if (p->left_lanes[p->left_key] != right[p->right_key]) continue;
        uint32_t row = p->batch->nrows++;
        for (uint32_t c = 0; c < p->left_cols; c++)
            memcpy(&p->batch->columns[c][row], &p->left_lanes[c], 8);
        for (uint32_t c = 0; c < p->right_cols; c++)
            memcpy(&p->batch->columns[p->left_cols + c][row], &right[c], 8);
    }
    if (p->error != WL_COLUMNAR_SPILL_OK) {
        p->batch->nrows = 0;
        return WL_COLUMNAR_CONTINUATION_INVALID;
    }
    if (!p->batch->nrows && p->row == p->left_rows)
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
    batch->complete = p->row == p->left_rows;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_spill_status_t
wl_columnar_payload_join_schema(wl_columnar_batch_payload_t *payload,
    uint32_t key, uint32_t *cols, uint32_t *rows)
{
    const wirelog_column_type_t *types =
        wl_columnar_batch_payload_schema(payload, cols, rows);
    uint32_t logical;
    if (!types || key >= *cols) return WL_COLUMNAR_SPILL_INVALID;
    if (wl_columnar_batch_payload_inline_schema(payload, &logical) ||
        types[key] != WIRELOG_TYPE_INT64) return WL_COLUMNAR_SPILL_UNSUPPORTED;
    for (uint32_t c = 0; c < *cols; c++)
        if (types[c] ==
            WIRELOG_TYPE_STRING) return WL_COLUMNAR_SPILL_UNSUPPORTED;
    return WL_COLUMNAR_SPILL_OK;
}

wl_columnar_spill_status_t
wl_columnar_payload_join_create(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *left, wl_columnar_batch_payload_t *right,
    uint32_t left_key, uint32_t right_key, uint64_t batch_bytes,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_continuation_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!g || !wl_columnar_batch_payload_uses_governor(left, g)
        || !wl_columnar_batch_payload_uses_governor(right,
        g)) return WL_COLUMNAR_SPILL_INVALID;
    uint32_t lc, lr, rc, rr;
    wl_columnar_spill_status_t status = wl_columnar_payload_join_schema(left,
            left_key, &lc, &lr);
    if (status != WL_COLUMNAR_SPILL_OK) return status;
    status = wl_columnar_payload_join_schema(right, right_key, &rc, &rr);
    if (status != WL_COLUMNAR_SPILL_OK) return status;
    if (lc > UINT32_MAX - rc) return WL_COLUMNAR_SPILL_OVERFLOW;
    uint32_t cols = lc + rc;
    uint64_t capacity = batch_bytes / ((uint64_t)cols * 8);
    if (!capacity) return WL_COLUMNAR_SPILL_MEMORY;
    if (capacity > UINT32_MAX) capacity = UINT32_MAX;
    uint32_t buckets = 1;
    while (buckets < rr && buckets < (UINT32_C(1) << 31)) buckets <<= 1;
    void *allocation;
    status = wl_columnar_payload_join_allocate(g,
            sizeof(wl_columnar_payload_join_t)
            + (uint64_t)cols * sizeof(wirelog_column_type_t), &allocation);
    if (status != WL_COLUMNAR_SPILL_OK) return status;
    wl_columnar_payload_join_t *p = allocation;
    p->governor = g; wl_columnar_memory_governor_ref_retain(g);
    p->left_cols = lc; p->right_cols = rc; p->left_rows = lr;
    p->right_rows = rr;
    p->left_key = left_key; p->right_key = right_key; p->buckets = buckets;
    p->capacity = (uint32_t)capacity; p->cancel = cancel;
    p->cancel_context = cancel_context;
    status = wl_columnar_batch_payload_acquire(left, &p->left);
    if (status != WL_COLUMNAR_SPILL_OK) goto fail;
    status = wl_columnar_batch_payload_acquire(right, &p->right);
    if (status != WL_COLUMNAR_SPILL_OK) goto fail;
    status = wl_columnar_payload_join_allocate(g, sizeof(*p->index)
            + ((uint64_t)buckets + rr) * sizeof(uint32_t), &allocation);
    if (status != WL_COLUMNAR_SPILL_OK) goto fail;
    p->index = allocation;
    for (uint32_t b = 0; b < buckets;
        b++) p->index->slots[b] = WL_COLUMNAR_PAYLOAD_JOIN_END;
    for (unsigned side = 0; side < 2; side++) {
        uint32_t rows = side ? rr : lr;
        for (uint32_t row = 0; row < rows; row++) {
            const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
            status = wl_columnar_batch_payload_read(side ? p->right : p->left,
                    row, cancel, cancel_context, &lanes, &meta);
            if (status != WL_COLUMNAR_SPILL_OK) goto fail;
            if (meta.iteration || meta.stratum || meta.worker ||
                meta.multiplicity != 1) {
                status = WL_COLUMNAR_SPILL_UNSUPPORTED; goto fail;
            }
            if (side) {
                uint32_t hash = wl_columnar_payload_join_hash(lanes[right_key],
                        buckets);
                p->index->slots[(uint64_t)buckets +
                    row] = p->index->slots[hash];
                p->index->slots[hash] = row;
            }
        }
    }
    memcpy(p->types, wl_columnar_batch_payload_schema(left, NULL, NULL),
        (size_t)lc * sizeof(*p->types));
    memcpy(p->types + lc, wl_columnar_batch_payload_schema(right, NULL, NULL),
        (size_t)rc * sizeof(*p->types));
    int error = wl_columnar_relation_new_auto_governed("$payload_join", cols,
            p->capacity, false, g, &p->batch);
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
