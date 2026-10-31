#include "columnar/trace_store.h"
#include "columnar/internal.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    wl_columnar_memory_reservation_t reservation;
} wl_columnar_trace_store_allocation_t;

typedef struct wl_columnar_trace_store_node {
    wl_columnar_trace_store_allocation_t allocation;
    struct wl_columnar_trace_store_node *next;
    wl_columnar_batch_payload_t *payload;
    uint64_t pins;
    uint32_t begin, end;
    bool spilled;
} wl_columnar_trace_store_node_t;

struct wl_columnar_trace_store {
    wl_columnar_trace_store_allocation_t allocation;
    wl_columnar_memory_governor_ref_t *governor;
    wl_columnar_trace_store_node_t *head, *tail;
    wl_mutex_t reader_lock;
    uint64_t snapshots;
    wl_columnar_trace_store_state_t state;
    uint32_t ncols;
    bool retired;
    wirelog_column_type_t types[];
};
struct wl_columnar_trace_store_snapshot {
    wl_columnar_trace_store_allocation_t allocation;
    wl_columnar_trace_store_t *trace;
    uint64_t refs;
    wl_columnar_trace_store_state_t state;
    wl_columnar_trace_store_node_t *nodes[];
};
struct wl_columnar_trace_store_cursor {
    wl_columnar_trace_store_allocation_t allocation;
    wl_columnar_trace_store_snapshot_t *snapshot;
    wl_columnar_trace_store_node_t *node;
    wl_columnar_batch_payload_reader_t *reader;
};

#ifdef WL_TEST_TRACE_STORE
static unsigned wl_columnar_trace_store_fail_allocation;
void wl_columnar_trace_store_test_fail_allocation(unsigned n)
{
    wl_columnar_trace_store_fail_allocation = n;
}
/* Overflow injection changes only counters; tests restore them before other
 * operations. This symbol is never compiled into the production library. */
void wl_columnar_trace_store_test_state(wl_columnar_trace_store_t *t,
    wl_columnar_trace_store_state_t state)
{
    t->state = state;
}
#endif

static wl_columnar_spill_status_t
wl_columnar_trace_store_allocate(wl_columnar_memory_governor_ref_t *g,
    uint64_t bytes, void **out)
{
    *out = NULL;
    if (bytes > SIZE_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_init(&token);
    wl_columnar_memory_admission_status_t status =
        wl_columnar_memory_reserve_checked(wl_columnar_memory_governor_ref_get(
                g),
            bytes, &token);
    if (status > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
            ? WL_COLUMNAR_SPILL_MEMORY : WL_COLUMNAR_SPILL_OVERFLOW;
    bool fail = false;
#ifdef WL_TEST_TRACE_STORE
    if (wl_columnar_trace_store_fail_allocation)
        fail = --wl_columnar_trace_store_fail_allocation == 0;
#endif
    wl_columnar_trace_store_allocation_t *a = fail ? NULL : calloc(1,
            (size_t)bytes);
    if (!a) {
        if (!wl_columnar_memory_rollback(&token)) abort();
        return WL_COLUMNAR_SPILL_ALLOCATION;
    }
    wl_columnar_memory_reservation_init(&a->reservation);
    if (!wl_columnar_memory_reservation_move(&a->reservation, &token) ||
        !wl_columnar_memory_commit(&a->reservation, a)) abort();
    *out = a;
    return WL_COLUMNAR_SPILL_OK;
}
static void wl_columnar_trace_store_free(void *ptr)
{
    wl_columnar_trace_store_allocation_t *a = ptr;
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reservation_move(&token, &a->reservation)) abort();
    free(a);
    if (!wl_columnar_memory_release(&token)) abort();
}

wl_columnar_spill_status_t
wl_columnar_trace_store_floor(uint32_t ncols, uint32_t segments,
    uint64_t *owner, uint64_t *nodes, uint64_t *snapshot, uint64_t *cursor)
{
    if (!owner || !nodes || !snapshot ||
        !cursor) return WL_COLUMNAR_SPILL_INVALID;
    *owner = *nodes = *snapshot = *cursor = 0;
    if (!ncols) return WL_COLUMNAR_SPILL_INVALID;
    uint64_t ob = sizeof(wl_columnar_trace_store_t) + (uint64_t)ncols *
        sizeof(wirelog_column_type_t);
    uint64_t nb = (uint64_t)segments * sizeof(wl_columnar_trace_store_node_t);
    uint64_t sb = sizeof(wl_columnar_trace_store_snapshot_t) +
        (uint64_t)segments *
        sizeof(wl_columnar_trace_store_node_t *);
    if (ob > SIZE_MAX || sb > SIZE_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    *owner = ob; *nodes = nb; *snapshot = sb;
    *cursor = sizeof(wl_columnar_trace_store_cursor_t);
    return WL_COLUMNAR_SPILL_OK;
}
static bool wl_columnar_trace_store_numeric(wirelog_column_type_t type)
{
    return type == WIRELOG_TYPE_INT32 || type == WIRELOG_TYPE_INT64 ||
           type == WIRELOG_TYPE_UINT32 || type == WIRELOG_TYPE_UINT64 ||
           type == WIRELOG_TYPE_FLOAT || type == WIRELOG_TYPE_BOOL;
}
wl_columnar_spill_status_t
wl_columnar_trace_store_create(wl_columnar_memory_governor_ref_t *g,
    const wirelog_column_type_t *types, uint32_t ncols,
    wl_columnar_trace_store_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!g || !types || !ncols) return WL_COLUMNAR_SPILL_INVALID;
    for (uint32_t i = 0; i < ncols; i++)
        if (!wl_columnar_trace_store_numeric(
                types[i])) return WL_COLUMNAR_SPILL_UNSUPPORTED;
    uint64_t owner, nodes, snapshot, cursor;
    wl_columnar_spill_status_t rc = wl_columnar_trace_store_floor(ncols, 0,
            &owner, &nodes, &snapshot, &cursor);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    void *allocation;
    rc = wl_columnar_trace_store_allocate(g, owner, &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    wl_columnar_trace_store_t *t = allocation;
    if (wl_mutex_init(&t->reader_lock)) {
        wl_columnar_trace_store_free(t);
        return WL_COLUMNAR_SPILL_ALLOCATION;
    }
    t->governor = g; wl_columnar_memory_governor_ref_retain(g);
    t->ncols = ncols;
    memcpy(t->types, types, (size_t)ncols * sizeof(*types));
    *out = t;
    return WL_COLUMNAR_SPILL_OK;
}
wl_columnar_trace_store_state_t
wl_columnar_trace_store_state(const wl_columnar_trace_store_t *t)
{
    return t ? t->state : (wl_columnar_trace_store_state_t){0};
}

static wl_columnar_spill_status_t
wl_columnar_trace_store_validate(wl_columnar_trace_store_t *t,
    const col_rel_t *source, wl_columnar_spill_cancel_fn cancel, void *context)
{
    if (!t || !source) return WL_COLUMNAR_SPILL_INVALID;
    if (t->retired) return WL_COLUMNAR_SPILL_CLOSED;
    if (source->memory_governor != t->governor || source->ncols != t->ncols)
        return WL_COLUMNAR_SPILL_INVALID;
    if (t->state.generation == UINT64_MAX || t->state.segments == UINT32_MAX ||
        source->nrows >
        UINT32_MAX - t->state.rows) return WL_COLUMNAR_SPILL_OVERFLOW;
    wl_columnar_source_access_reader_t lease = {0};
    int access = col_rel_source_reader_acquire(source, &lease);
    if (access) return access ==
               EBUSY ? WL_COLUMNAR_SPILL_BUSY : WL_COLUMNAR_SPILL_INVALID;
    wl_columnar_spill_status_t rc = WL_COLUMNAR_SPILL_UNSUPPORTED;
    if (source->has_graph_column ||
        source->compound_kind != WIRELOG_COMPOUND_KIND_NONE ||
        source->compound_count || source->compound_arity_map ||
        source->compound_arity_len || source->inline_physical_offset) goto done;
    rc = WL_COLUMNAR_SPILL_INVALID;
    if (source->nrows > source->capacity || !source->columns ||
        (source->timestamps &&
        source->nrows > source->timestamp_capacity)) goto done;
    for (uint32_t i = 0; i < t->ncols; i++) {
        wirelog_column_type_t type = source->column_types
            ? source->column_types[i] : WIRELOG_TYPE_INT64;
        if (type != t->types[i]) {
            rc = WL_COLUMNAR_SPILL_UNSUPPORTED; goto done;
        }
        if (!source->columns[i]) goto done;
    }
    for (uint32_t i = 0; i < source->nrows; i++) {
        if (cancel && cancel(context)) {
            rc = WL_COLUMNAR_SPILL_CANCELLED; goto done;
        }
        const col_delta_timestamp_t *stamp =
            source->timestamps ? &source->timestamps[i] : NULL;
        if (stamp &&
            (stamp->iteration || stamp->stratum || stamp->worker ||
            stamp->_reserved)){
            rc = WL_COLUMNAR_SPILL_UNSUPPORTED; goto done;
        }
    }
    rc = cancel &&
        cancel(context) ? WL_COLUMNAR_SPILL_CANCELLED : WL_COLUMNAR_SPILL_OK;
done:
    if (col_rel_source_reader_release(&lease)) abort();
    return rc;
}
static void wl_columnar_trace_store_publish(wl_columnar_trace_store_t *t,
    wl_columnar_trace_store_node_t *node, uint32_t rows)
{
    node->begin = t->state.rows; node->end = node->begin + rows;
    if (t->tail) t->tail->next = node;
    else t->head = node;
    t->tail = node;
    t->state.rows = node->end; t->state.segments++; t->state.generation++;
}
static wl_columnar_spill_status_t
wl_columnar_trace_store_append(wl_columnar_trace_store_t *t,
    const col_rel_t *source, col_rel_t **take,
    wl_columnar_batch_payload_storage_t storage,
    wl_columnar_spill_store_t *store,
    wl_columnar_spill_cancel_fn cancel, void *context)
{
    wl_columnar_spill_status_t rc = wl_columnar_trace_store_validate(t, source,
            cancel, context);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    uint32_t rows = source->nrows;
    void *allocation;
    rc = wl_columnar_trace_store_allocate(t->governor,
            sizeof(wl_columnar_trace_store_node_t), &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    wl_columnar_trace_store_node_t *node = allocation;
    node->spilled = storage == WL_COLUMNAR_BATCH_PAYLOAD_SPILLED;
    if (take) rc = wl_columnar_batch_payload_take(t->governor, store, take,
                cancel, context, &node->payload);
    else rc = wl_columnar_batch_payload_capture(t->governor, store, source,
                rows, storage, cancel, context, &node->payload);
    if (rc != WL_COLUMNAR_SPILL_OK) {
        wl_columnar_trace_store_free(node); return rc;
    }
    /* No fallible operation follows source retirement. */
    wl_columnar_trace_store_publish(t, node, rows);
    return WL_COLUMNAR_SPILL_OK;
}
wl_columnar_spill_status_t
wl_columnar_trace_store_append_capture(wl_columnar_trace_store_t *t,
    const col_rel_t *source, wl_columnar_batch_payload_storage_t storage,
    wl_columnar_spill_store_t *store, wl_columnar_spill_cancel_fn cancel,
    void *context)
{
    return wl_columnar_trace_store_append(t, source, NULL, storage, store,
               cancel, context);
}
wl_columnar_spill_status_t
wl_columnar_trace_store_append_take(wl_columnar_trace_store_t *t,
    col_rel_t **source,
    wl_columnar_spill_store_t *store, wl_columnar_spill_cancel_fn cancel,
    void *context)
{
    return wl_columnar_trace_store_append(t, source ? *source : NULL, source,
               WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, store, cancel, context);
}
wl_columnar_spill_status_t
wl_columnar_trace_store_demote(wl_columnar_trace_store_t *t, uint32_t segment,
    wl_columnar_spill_store_t *store, wl_columnar_spill_cancel_fn cancel,
    void *context)
{
    if (!t || segment >= t->state.segments) return WL_COLUMNAR_SPILL_INVALID;
    if (t->retired) return WL_COLUMNAR_SPILL_CLOSED;
    wl_columnar_trace_store_node_t *node = t->head;
    for (uint32_t i = 0; i < segment; i++) {
        if (cancel && cancel(context)) return WL_COLUMNAR_SPILL_CANCELLED;
        node = node->next;
    }
    if (node->pins) return WL_COLUMNAR_SPILL_BUSY;
    if (node->spilled) return WL_COLUMNAR_SPILL_INVALID;
    wl_columnar_spill_status_t rc =
        wl_columnar_batch_payload_spill(&node->payload, store, cancel, context);
    if (rc == WL_COLUMNAR_SPILL_OK) node->spilled = true;
    return rc;
}
static void wl_columnar_trace_store_destroy(wl_columnar_trace_store_t *t)
{
    wl_columnar_memory_governor_ref_t *g = t->governor;
    wl_columnar_trace_store_node_t *node = t->head;
    while (node) {
        wl_columnar_trace_store_node_t *next = node->next;
        if (node->pins ||
            wl_columnar_batch_payload_release(node->payload) !=
            WL_COLUMNAR_SPILL_OK) abort();
        wl_columnar_trace_store_free(node); node = next;
    }
    wl_mutex_destroy(&t->reader_lock);
    wl_columnar_trace_store_free(t);
    wl_columnar_memory_governor_ref_release(g);
}
void wl_columnar_trace_store_retire(wl_columnar_trace_store_t *t)
{
    if (!t) return;
    t->retired = true;
    if (!t->snapshots) wl_columnar_trace_store_destroy(t);
}
wl_columnar_spill_status_t
wl_columnar_trace_store_snapshot_acquire(wl_columnar_trace_store_t *t,
    wl_columnar_trace_store_snapshot_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!t) return WL_COLUMNAR_SPILL_INVALID;
    if (t->retired) return WL_COLUMNAR_SPILL_CLOSED;
    if (t->snapshots == UINT64_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    uint64_t owner, nodes, snapshot, cursor;
    wl_columnar_spill_status_t rc = wl_columnar_trace_store_floor(t->ncols,
            t->state.segments,
            &owner, &nodes, &snapshot, &cursor);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    for (wl_columnar_trace_store_node_t *n = t->head; n; n = n->next)
        if (n->pins == UINT64_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    void *allocation;
    rc = wl_columnar_trace_store_allocate(t->governor, snapshot, &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    wl_columnar_trace_store_snapshot_t *s = allocation;
    s->trace = t; s->refs = 1; s->state = t->state;
    uint32_t i = 0;
    for (wl_columnar_trace_store_node_t *n = t->head; n; n = n->next) {
        s->nodes[i++] = n; n->pins++;
    }
    t->snapshots++; *out = s;
    return WL_COLUMNAR_SPILL_OK;
}
void wl_columnar_trace_store_snapshot_release(
    wl_columnar_trace_store_snapshot_t *s)
{
    if (!s || --s->refs) return;
    wl_columnar_trace_store_t *t = s->trace;
    for (uint32_t i = 0; i < s->state.segments; i++) s->nodes[i]->pins--;
    t->snapshots--;
    wl_columnar_trace_store_free(s);
    if (t->retired && !t->snapshots) wl_columnar_trace_store_destroy(t);
}
const wirelog_column_type_t *
wl_columnar_trace_store_snapshot_schema(
    const wl_columnar_trace_store_snapshot_t *s,
    uint32_t *ncols, uint32_t *rows)
{
    if (ncols) *ncols = s ? s->trace->ncols : 0;
    if (rows) *rows = s ? s->state.rows : 0;
    return s ? s->trace->types : NULL;
}
wl_columnar_trace_store_state_t
wl_columnar_trace_store_snapshot_state(
    const wl_columnar_trace_store_snapshot_t *s)
{
    return s ? s->state : (wl_columnar_trace_store_state_t){0};
}
bool wl_columnar_trace_store_snapshot_uses_governor(
    const wl_columnar_trace_store_snapshot_t *s,
    const wl_columnar_memory_governor_ref_t *g)
{
    return s && s->trace->governor == g;
}
wl_columnar_spill_status_t
wl_columnar_trace_store_cursor_acquire(wl_columnar_trace_store_snapshot_t *s,
    wl_columnar_trace_store_cursor_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!s) return WL_COLUMNAR_SPILL_INVALID;
    if (s->refs == UINT64_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    void *allocation;
    wl_columnar_spill_status_t rc =
        wl_columnar_trace_store_allocate(s->trace->governor,
            sizeof(wl_columnar_trace_store_cursor_t), &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    wl_columnar_trace_store_cursor_t *c = allocation;
    c->snapshot = s; s->refs++; *out = c;
    return WL_COLUMNAR_SPILL_OK;
}
void wl_columnar_trace_store_cursor_release(wl_columnar_trace_store_cursor_t *c)
{
    if (!c) return;
    wl_columnar_trace_store_snapshot_t *s = c->snapshot;
    wl_mutex_lock(&s->trace->reader_lock);
    wl_columnar_batch_payload_reader_release(c->reader);
    wl_mutex_unlock(&s->trace->reader_lock);
    wl_columnar_trace_store_free(c);
    wl_columnar_trace_store_snapshot_release(s);
}
wl_columnar_spill_status_t
wl_columnar_trace_store_cursor_read(wl_columnar_trace_store_cursor_t *c,
    uint64_t ordinal, wl_columnar_spill_cancel_fn cancel, void *context,
    const uint64_t **lanes, wl_columnar_spill_metadata_t *metadata)
{
    if (lanes) *lanes = NULL;
    if (metadata) memset(metadata, 0, sizeof(*metadata));
    if (!c || !lanes || !metadata || ordinal >= c->snapshot->state.rows)
        return WL_COLUMNAR_SPILL_INVALID;
    if (cancel && cancel(context)) return WL_COLUMNAR_SPILL_CANCELLED;
    wl_columnar_trace_store_snapshot_t *s = c->snapshot;
    uint32_t lo = 0, hi = s->state.segments;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo)/2;
        if (s->nodes[mid]->end <= ordinal) lo = mid + 1;
        else hi = mid;
    }
    wl_columnar_trace_store_node_t *node = s->nodes[lo];
    if (node != c->node) {
        wl_mutex_lock(&s->trace->reader_lock);
        wl_columnar_batch_payload_reader_release(c->reader);
        c->reader = NULL; c->node = NULL;
        wl_columnar_spill_status_t rc =
            wl_columnar_batch_payload_acquire(node->payload, &c->reader);
        if (rc == WL_COLUMNAR_SPILL_OK) c->node = node;
        wl_mutex_unlock(&s->trace->reader_lock);
        if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    }
    return wl_columnar_batch_payload_read(c->reader, ordinal - node->begin,
               cancel, context, lanes, metadata);
}
