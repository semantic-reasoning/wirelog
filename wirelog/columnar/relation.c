/*
 * columnar/relation.c - wirelog Columnar Relation Storage
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Relation lifecycle, schema, and row-append operations.
 * Extracted from backend/columnar_nanoarrow.c.
 */

#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* wl_atomic_u64 and the atomic_*_explicit shims come from mem_ledger.h
 * (via internal.h): C11 <stdatomic.h> elsewhere, _Interlocked* on MSVC,
 * which has no C11 atomics in its default C mode.  Relaxed ordering is
 * sufficient: the counter only has to hand out distinct values. */
static wl_atomic_u64 wl_next_relation_identity = 1u;

#ifdef WL_TEST_APPEND_HOOK
wl_columnar_append_transition_hook_t wl_columnar_append_transition_hook;
#endif

#ifdef WL_TEST_SET_HOOK
wl_columnar_set_transition_hook_t wl_columnar_set_transition_hook;
#endif

static int
col_rel_new_identity(uint64_t *out)
{
    uint64_t current = atomic_load_explicit(&wl_next_relation_identity,
            memory_order_relaxed);
    for (;;) {
        if (current == 0u || current == UINT64_MAX)
            return EOVERFLOW;
        uint64_t next = current + 1u;
        if (atomic_compare_exchange_weak_explicit(&wl_next_relation_identity,
            &current, next, memory_order_relaxed, memory_order_relaxed)) {
            *out = current;
            return 0;
        }
    }
}

/* Test-only seam used to exercise the terminal allocator state without
 * manufacturing UINT64_MAX relations in a production process. */
int
col_rel_test_set_next_identity(uint64_t next)
{
    atomic_store_explicit(&wl_next_relation_identity, next,
        memory_order_relaxed);
    return 0;
}

#if defined(_MSC_VER)
#define WL_COLUMNAR_RELATION_NOINLINE __declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
#define WL_COLUMNAR_RELATION_NOINLINE __attribute__((noinline))
#else
#define WL_COLUMNAR_RELATION_NOINLINE
#endif

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __ARM_NEON__
#include <arm_neon.h>
#endif

/*
 * Portable software-prefetch macros (Issue #363).
 *
 * WL_PREFETCH_R(addr) — read prefetch, L2 temporal (gather source).
 * WL_PREFETCH_W(addr) — write prefetch, L2 temporal (scatter destination).
 *
 * MSVC does not support __builtin_prefetch; map to _mm_prefetch with
 * _MM_HINT_T1 (L2 locality) for both read and write variants since MSVC
 * does not expose a write-intent hint via _mm_prefetch.
 */
#if defined(_MSC_VER)
#include <xmmintrin.h>
#define WL_PREFETCH_R(addr) _mm_prefetch((const char *)(addr), _MM_HINT_T1)
#define WL_PREFETCH_W(addr) _mm_prefetch((const char *)(addr), _MM_HINT_T1)
#elif defined(__GNUC__) || defined(__clang__)
#define WL_PREFETCH_R(addr) __builtin_prefetch((addr), 0, 1)
#define WL_PREFETCH_W(addr) __builtin_prefetch((addr), 1, 1)
#else
#define WL_PREFETCH_R(addr) ((void)(addr))
#define WL_PREFETCH_W(addr) ((void)(addr))
#endif

#ifdef WL_RADIX_BENCH
static bool
wl_columnar_relation_radix_bench_enabled(void)
{
    const char *env = getenv("WIRELOG_RADIX_BENCH_LOG");

    return env && env[0] != '\0' && strcmp(env, "0") != 0;
}
#endif

/* ---- COW helpers --------------------------------------------------------- */

static int col_rel_grow_owned_transition_impl(col_rel_t *r,
    uint32_t new_cap, bool defer_alias_release);
static int col_rel_cow_unshare_impl(col_rel_t *r, uint32_t new_cap,
    bool defer_alias_release);

static int
col_rel_grow_owned_transition(col_rel_t *r, uint32_t new_cap)
{
    return col_rel_grow_owned_transition_impl(r, new_cap, false);
}

static void
col_rel_storage_owner_init(col_rel_t *r)
{
    if (!r)
        return;
    r->storage_owner = r;
    r->storage_owner_identity = r->relation_identity;
    r->storage_owner_generation = r->storage_generation;
    r->storage_alias_borrows = 0;
    wl_columnar_source_access_gate_init(&r->source_access);
}

int
col_rel_storage_owner_resolve(const col_rel_t *src, col_rel_t **out_owner)
{
    col_rel_t *owner;
    if (!src || !out_owner)
        return EINVAL;
    owner = src->storage_owner;
    if (!owner) {
        owner = (col_rel_t *)src; /* legacy zero-initialized relation */
        owner->storage_owner = owner;
        owner->storage_owner_identity = owner->relation_identity;
        owner->storage_owner_generation = owner->storage_generation;
        wl_columnar_source_access_gate_init(&owner->source_access);
    }
    if (owner->storage_owner != owner
        || owner->storage_owner_identity != owner->relation_identity
        || owner->storage_owner_generation == 0
        || !wl_columnar_relation_generation_valid(
            owner->storage_owner_generation))
        return EINVAL;
    if (owner != src && src->storage_owner != owner)
        return EINVAL;
    *out_owner = owner;
    return 0;
}

int
col_rel_storage_alias_release(col_rel_t *alias)
{
    col_rel_t *owner;
    if (!alias || !alias->storage_owner)
        return 0;
    if (alias->storage_owner == alias)
        return 0;
    owner = alias->storage_owner;
    if (owner->storage_owner != owner
        || owner->storage_owner_identity != owner->relation_identity
        || owner->storage_alias_borrows == 0
        || alias->storage_owner_identity != owner->relation_identity)
        return EINVAL;
    owner->storage_alias_borrows--;
    col_rel_storage_owner_init(alias);
    return 0;
}

int
col_rel_storage_owner_destroy_status(const col_rel_t *owner)
{
    if (!owner || owner->storage_owner != owner
        || owner->storage_owner_identity != owner->relation_identity)
        return EINVAL;
    if (owner->storage_alias_borrows != 0
        || wl_columnar_source_access_gate_busy(&owner->source_access))
        return EBUSY;
    return 0;
}

int
col_rel_source_reader_acquire(const col_rel_t *rel,
    wl_columnar_source_access_reader_t *token)
{
    col_rel_t *owner = NULL;
    int rc;
    if (!rel || !token)
        return EINVAL;
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc != 0)
        return rc;
    return wl_columnar_source_access_reader_acquire(
        &owner->source_access, token);
}

int
col_rel_source_reader_release(wl_columnar_source_access_reader_t *token)
{
    return wl_columnar_source_access_reader_release(token);
}

static int
col_rel_source_writer_acquire(const col_rel_t *rel,
    wl_columnar_source_access_writer_t *token)
{
    col_rel_t *owner = NULL;
    int rc;

    if (!rel || !token)
        return EINVAL;
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc != 0)
        return rc;
    return wl_columnar_source_access_writer_acquire(
        &owner->source_access, token);
}

/* Metadata/type publication rewrites the canonical relation descriptor and,
 * for types, may reinterpret existing row lanes.  A live storage alias must
 * therefore be denied rather than allowed to observe a mixed descriptor. */
static int
col_rel_published_writer_acquire(const col_rel_t *rel,
    wl_columnar_source_access_writer_t *token)
{
    col_rel_t *owner = NULL;
    int rc;

    if (!rel || !token)
        return EINVAL;
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc != 0)
        return rc;
    if (owner->storage_alias_borrows > 0)
        return EBUSY;
    return wl_columnar_source_access_writer_acquire(
        &owner->source_access, token);
}

int
col_rel_set(col_rel_t *r, uint32_t row, uint32_t col, int64_t val)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    bool alias_release_pending = false;
    col_rel_t *owner = NULL;
    int rc;

    if (!r || !r->columns || row >= r->capacity || col >= r->ncols
        || !r->columns[col])
        return EINVAL;
    if (r->column_types && r->column_types[col] == WIRELOG_TYPE_FLOAT) {
        if (!wl_columnar_float_bits_valid(val))
            return EINVAL;
        if (wl_columnar_float_bits_zero(val))
            val = 0;
    }

    rc = col_rel_storage_owner_resolve(r, &owner);
    if (rc != 0)
        return rc;
    rc = col_rel_source_writer_acquire(r, &writer);
    if (rc != 0)
        return rc;
    if (r == owner && owner->storage_alias_borrows > 0) {
        rc = EBUSY;
        goto release_writer;
    }

    /* Keep the canonical owner writer admission held through the detach,
     * cell write, and logical generation publication.  The deferred alias
     * release prevents a new reader from entering through the detached view
     * before this mutation is complete. */
    if (r->col_shared) {
        rc = col_rel_cow_unshare_impl(r, 0, true);
        if (rc != 0)
            goto release_writer;
        alias_release_pending = true;
    }
#ifdef WL_TEST_SET_HOOK
    if (alias_release_pending && wl_columnar_set_transition_hook)
        wl_columnar_set_transition_hook(r);
#endif
    r->columns[col][row] = val;
    wl_columnar_relation_touch_view(r);
    rc = 0;

release_writer:
    if (alias_release_pending) {
        int alias_rc = col_rel_storage_alias_release(r);
        if (alias_rc != 0 && rc == 0)
            rc = alias_rc;
    }
    if (wl_columnar_source_access_writer_release(&writer) != 0 && rc == 0)
        rc = EINVAL;
    return rc;
}

/* Prepare a complete private replacement for a relation resize.  This is
 * deliberately independent of the current ownership mode: a shared or arena
 * relation is copied into heap storage and is not changed until the caller
 * reaches its publication point. */
static int
col_rel_prepare_resize(const col_rel_t *r, uint32_t new_cap,
    int64_t ***out_columns, col_delta_timestamp_t **out_timestamps)
{
    int64_t **columns = NULL;
    col_delta_timestamp_t *timestamps = NULL;

    if (!r || !out_columns || !out_timestamps)
        return EINVAL;
    if (r->ncols > 0) {
        columns = col_columns_alloc(r->ncols, new_cap);
        if (!columns)
            return ENOMEM;
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->columns && r->columns[c] && r->nrows > 0)
                memcpy(columns[c], r->columns[c],
                    (size_t)r->nrows * sizeof(int64_t));
        }
    }
    if (r->timestamps) {
        timestamps = (col_delta_timestamp_t *)malloc(
            (size_t)new_cap * sizeof(*timestamps));
        if (!timestamps) {
            col_columns_free(columns, r->ncols);
            return ENOMEM;
        }
        if (r->nrows > 0)
            memcpy(timestamps, r->timestamps,
                (size_t)r->nrows * sizeof(*timestamps));
        if (new_cap > r->nrows)
            memset(timestamps + r->nrows, 0,
                (size_t)(new_cap - r->nrows) * sizeof(*timestamps));
    }
    *out_columns = columns;
    *out_timestamps = timestamps;
    return 0;
}

static void
col_rel_release_column_storage(col_rel_t *r)
{
    if (!r || !r->columns)
        return;
    if (r->col_shared) {
        for (uint32_t c = 0; c < r->ncols; c++)
            if (!r->col_shared[c])
                free(r->columns[c]);
        free((void *)r->columns);
        free(r->col_shared);
    } else if (r->arena_owned) {
        free((void *)r->columns);
    } else {
        col_columns_free(r->columns, r->ncols);
    }
    r->columns = NULL;
    r->col_shared = NULL;
}

static void
col_rel_publish_resize(col_rel_t *r, int64_t **columns,
    col_delta_timestamp_t *timestamps, uint32_t new_cap)
{
    col_rel_release_column_storage(r);
    free(r->timestamps);
    r->columns = columns;
    r->timestamps = timestamps;
    r->capacity = new_cap;
    r->arena_owned = false;
}

uint64_t
col_rel_owned_ledger_bytes(const col_rel_t *r)
{
    if (!r || r->arena_owned || !r->columns || r->capacity == 0)
        return 0;
    uint64_t owned_cols = 0;
    int64_t **columns = r->columns;
    bool *shared = r->col_shared;
    for (uint32_t c = 0; c < r->ncols; c++, columns++) {
        /* columns/shared are allocated together for exactly r->ncols slots. */
        if ((!shared || !shared[c]) && *columns) /* NOLINT(clang-analyzer-security.ArrayBound) */
            owned_cols++;
    }
    if (owned_cols == 0)
        return 0;
    if (owned_cols > UINT64_MAX / r->capacity / sizeof(int64_t))
        return UINT64_MAX;
    return owned_cols * r->capacity * sizeof(int64_t);
}

uint64_t
col_rel_timestamp_ledger_bytes(const col_rel_t *r)
{
    if (!r || !r->timestamps || r->capacity == 0)
        return 0;
    /* capacity is uint32_t, so the product cannot overflow uint64_t. */
    return (uint64_t)r->capacity * sizeof(col_delta_timestamp_t);
}

uint64_t
col_rel_transport_bytes(const col_rel_t *r)
{
    uint64_t cols = col_rel_owned_ledger_bytes(r);
    uint64_t ts = col_rel_timestamp_ledger_bytes(r);
    if (cols > UINT64_MAX - ts)
        return UINT64_MAX;
    return cols + ts;
}

static bool
col_rel_retained_bytes(uint32_t ncols, uint32_t capacity, bool timestamps,
    uint64_t *out)
{
    uint64_t columns;
    uint64_t timestamp_bytes = 0;

    if (!out || !wl_columnar_memory_size_mul(ncols, capacity, &columns)
        || !wl_columnar_memory_size_mul(columns, sizeof(int64_t), &columns))
        return false;
    if (timestamps
        && (!wl_columnar_memory_size_mul(capacity,
        sizeof(col_delta_timestamp_t), &timestamp_bytes)
        || !wl_columnar_memory_size_add(columns, timestamp_bytes, out)))
        return false;
    if (!timestamps)
        *out = columns;
    return true;
}

static void
col_rel_reservation_rollback(wl_columnar_memory_reservation_t *reservation)
{
    if (!reservation)
        return;
    (void)wl_columnar_memory_release(reservation);
}

static int
col_rel_reserve_retained_shape(const col_rel_t *r, uint32_t capacity,
    bool timestamps, wl_columnar_memory_reservation_t *pending)
{
    uint64_t bytes;
    wl_columnar_memory_admission_status_t status;

    if (!r || !pending)
        return -1;
    wl_columnar_memory_reservation_init(pending);
    if (!r->memory_governor)
        return 0;
    /* Shared and arena-backed relations are deliberately not attached by
     * the retained-EDB path yet; their ownership transitions are separate
     * admission units. */
    if (r->arena_owned || r->col_shared)
        return -1;
    if (!col_rel_retained_bytes(r->ncols, capacity, timestamps, &bytes))
        return -1;
    /* A consolidation or bulk append may have reduced the physical
     * capacity while the committed token still covers the larger shape.
     * Keep that conservative token: no new admission is needed until the
     * relation grows beyond it. */
    if (bytes <= r->retained_reserved_bytes)
        return 0;
    if (bytes == 0)
        return 0;
    status = wl_columnar_memory_reserve_growth(
        wl_columnar_memory_governor_ref_get(r->memory_governor),
        r->retained_reserved_bytes, bytes, pending);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return -1;
    return 1;
}

static int
col_rel_reserve_retained(const col_rel_t *r, uint32_t capacity,
    wl_columnar_memory_reservation_t *pending)
{
    return col_rel_reserve_retained_shape(r, capacity,
               r->timestamps != NULL, pending);
}

static int
col_rel_publish_retained_reservation(col_rel_t *r,
    wl_columnar_memory_reservation_t *pending, uint64_t bytes)
{
    uint64_t old_bytes;
    wl_columnar_memory_reservation_t previous;

    if (!r || !pending)
        return EINVAL;
    if (!r->memory_governor || bytes == 0)
        return 0;
    if (!wl_columnar_memory_commit(pending, r))
        return ENOMEM;
    old_bytes = r->retained_reserved_bytes;
    wl_columnar_memory_reservation_init(&previous);
    if (old_bytes > 0
        && !wl_columnar_memory_reservation_move(
            &previous, &r->retained_reservation)) {
        (void)wl_columnar_memory_release(pending);
        return ENOMEM;
    }
    if (!wl_columnar_memory_reservation_move(&r->retained_reservation,
        pending)) {
        if (old_bytes > 0) {
            wl_columnar_memory_reservation_init(&r->retained_reservation);
            (void)wl_columnar_memory_reservation_move(
                &r->retained_reservation, &previous);
        }
        (void)wl_columnar_memory_release(pending);
        return ENOMEM;
    }
    if (old_bytes > 0)
        (void)wl_columnar_memory_release(&previous);
    r->retained_reserved_bytes = bytes;
    return 0;
}

/* Reserve the complete private shape of an ownership transition.  The
 * ordinary retained path deliberately rejects shared and arena-backed
 * relations because those buffers are not yet heap-owned; transition paths
 * use this helper instead and charge the replacement only once. */
static int
col_rel_reserve_transition(const col_rel_t *r, uint32_t capacity,
    wl_columnar_memory_reservation_t *pending, uint64_t *bytes_out)
{
    uint64_t bytes;
    wl_columnar_memory_admission_status_t status;

    if (!r || !pending || !bytes_out)
        return -1;
    wl_columnar_memory_reservation_init(pending);
    if (!col_rel_retained_bytes(r->ncols, capacity,
        r->timestamps != NULL, &bytes))
        return -1;
    *bytes_out = bytes;
    if (!r->memory_governor || bytes == 0 ||
        bytes <= r->retained_reserved_bytes)
        return 0;
    status = wl_columnar_memory_reserve_growth(
        wl_columnar_memory_governor_ref_get(r->memory_governor),
        r->retained_reserved_bytes, bytes, pending);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return -1;
    return 1;
}

/* Migrate an arena relation, or grow a relation while it still contains
 * borrowed/arena storage.  Every replacement buffer and timestamp array is
 * prepared before the relation is changed, so admission or allocation
 * failure leaves the old ownership and reservation untouched. */
static int
col_rel_grow_owned_transition_impl(col_rel_t *r, uint32_t new_cap,
    bool defer_alias_release)
{
    wl_columnar_memory_reservation_t pending;
    int pending_rc;
    uint64_t new_bytes;
    int64_t **new_columns = NULL;
    col_delta_timestamp_t *new_timestamps = NULL;
    int64_t **old_columns;
    col_delta_timestamp_t *old_timestamps;
    bool *old_shared_flags;
    bool old_arena;
    uint64_t ledger_before;

    if (!r || !r->columns || r->ncols == 0 || new_cap < r->nrows)
        return EINVAL;
    pending_rc = col_rel_reserve_transition(r, new_cap, &pending,
            &new_bytes);
    if (pending_rc < 0)
        return ENOMEM;
    new_columns = col_columns_alloc(r->ncols, new_cap);
    if (!new_columns)
        goto fail;
    for (uint32_t c = 0; c < r->ncols; c++)
        memcpy(new_columns[c], r->columns[c],
            (size_t)r->nrows * sizeof(int64_t));
    if (r->timestamps) {
        new_timestamps = (col_delta_timestamp_t *)malloc(
            (size_t)new_cap * sizeof(*new_timestamps));
        if (!new_timestamps)
            goto fail;
        memcpy(new_timestamps, r->timestamps,
            (size_t)r->nrows * sizeof(*new_timestamps));
    }
    if (pending_rc > 0
        && col_rel_publish_retained_reservation(r, &pending, new_bytes) != 0)
        goto fail;

    old_columns = r->columns;
    old_timestamps = r->timestamps;
    old_shared_flags = r->col_shared;
    old_arena = r->arena_owned;
    ledger_before = col_rel_owned_ledger_bytes(r);
    r->columns = new_columns;
    r->timestamps = new_timestamps;
    r->capacity = new_cap;
    r->arena_owned = false;
    r->col_shared = NULL;
    if (old_arena) {
        free((void *)old_columns);
    } else {
        for (uint32_t c = 0; c < r->ncols; c++)
            if (!old_shared_flags || !old_shared_flags[c])
                free(old_columns[c]);
        free((void *)old_columns);
    }
    free(old_shared_flags);
    free(old_timestamps);
    col_rel_ledger_reconcile(r, ledger_before);
    if (!defer_alias_release)
        (void)col_rel_storage_alias_release(r);
    wl_columnar_relation_touch_storage(r);
    return 0;

fail:
    col_rel_reservation_rollback(&pending);
    col_columns_free(new_columns, r->ncols);
    free(new_timestamps);
    return ENOMEM;
}

int
col_rel_promote_arena_admitted(col_rel_t *r)
{
    if (!r || !r->arena_owned)
        return 0;
    return col_rel_grow_owned_transition(r, r->capacity);
}

/* Copy-on-write at the current capacity.  Shared buffers are never freed;
 * owned columns remain in place, and the reservation is published only once
 * every private replacement has been copied successfully. */
int
col_rel_cow_unshare(col_rel_t *r, uint32_t new_cap)
{
    return col_rel_cow_unshare_impl(r, new_cap, false);
}

static int
col_rel_cow_unshare_impl(col_rel_t *r, uint32_t new_cap,
    bool defer_alias_release)
{
    wl_columnar_memory_reservation_t pending;
    int pending_rc;
    uint64_t new_bytes;
    int64_t **private_cols = NULL;
    uint32_t capacity;
    uint64_t ledger_before;

    if (!r || !r->col_shared)
        return 0;
    capacity = new_cap ? new_cap : (r->capacity ? r->capacity
                                                    : COL_REL_INIT_CAP);
    if (capacity > r->capacity)
        return col_rel_grow_owned_transition_impl(r, capacity,
                   defer_alias_release);
    pending_rc = col_rel_reserve_transition(r, capacity, &pending,
            &new_bytes);
    if (pending_rc < 0)
        return ENOMEM;
    private_cols = (int64_t **)calloc(r->ncols, sizeof(*private_cols));
    if (!private_cols)
        goto fail;
    for (uint32_t c = 0; c < r->ncols; c++) {
        if (!r->col_shared[c])
            continue;
        private_cols[c] = (int64_t *)malloc((size_t)capacity
                * sizeof(int64_t));
        if (!private_cols[c])
            goto fail;
        memcpy(private_cols[c], r->columns[c],
            (size_t)r->nrows * sizeof(int64_t));
    }
    if (pending_rc > 0
        && col_rel_publish_retained_reservation(r, &pending, new_bytes) != 0)
        goto fail;
    ledger_before = col_rel_owned_ledger_bytes(r);
    int64_t **private_cursor = private_cols;
    int64_t **relation_cursor = r->columns;
    bool *shared_cursor = r->col_shared;
    for (uint32_t c = 0; c < r->ncols;
        c++, private_cursor++, relation_cursor++, shared_cursor++) {
        if (!*private_cursor) /* NOLINT(clang-analyzer-security.ArrayBound) */
            continue;
        *relation_cursor = *private_cursor;
        *shared_cursor = false;
        *private_cursor = NULL;
    }
    free((void *)private_cols);
    private_cols = NULL;
    {
        bool any_shared = false;
        for (uint32_t c = 0; c < r->ncols; c++)
            any_shared = any_shared || r->col_shared[c];
        if (!any_shared) {
            free(r->col_shared);
            r->col_shared = NULL;
        }
    }
    col_rel_ledger_reconcile(r, ledger_before);
    /* Private columns replaced the borrowed view: one storage epoch. */
    if (!defer_alias_release)
        (void)col_rel_storage_alias_release(r);
    wl_columnar_relation_touch_storage(r);
    return 0;

fail:
    col_rel_reservation_rollback(&pending);
    if (private_cols) {
        int64_t **cursor = private_cols;
        for (uint32_t c = 0; c < r->ncols; c++, cursor++)
            free(*cursor); /* NOLINT(clang-analyzer-security.ArrayBound) */
        free((void *)private_cols);
    }
    return ENOMEM;
}

int
col_rel_attach_memory_governor(col_rel_t *r,
    wl_columnar_memory_governor_ref_t *memory_governor)
{
    if (!r || !memory_governor)
        return EINVAL;
    if (r->memory_governor == memory_governor)
        return 0;
    if (r->memory_governor || r->retained_reserved_bytes != 0)
        return EBUSY;
    r->memory_governor = memory_governor;
    wl_columnar_memory_governor_ref_retain(memory_governor);
    wl_columnar_memory_reservation_init(&r->retained_reservation);
    return 0;
}

int
col_rel_enable_timestamps(col_rel_t *r)
{
    wl_columnar_memory_reservation_t pending;
    int reserve_rc;
    uint64_t new_bytes;
    col_delta_timestamp_t *timestamps;

    if (!r)
        return EINVAL;
    if (r->timestamps || r->capacity == 0)
        return 0;
    if (!r->memory_governor) {
        r->timestamps = (col_delta_timestamp_t *)calloc(
            r->capacity, sizeof(*r->timestamps));
        if (!r->timestamps)
            return ENOMEM;
        wl_columnar_relation_touch_storage(r);
        return 0;
    }
    reserve_rc = col_rel_reserve_retained_shape(
        r, r->capacity, true, &pending);
    if (reserve_rc < 0)
        return ENOMEM;
    if (!col_rel_retained_bytes(r->ncols, r->capacity, true, &new_bytes))
        goto fail;
    timestamps = (col_delta_timestamp_t *)calloc(
        r->capacity, sizeof(*timestamps));
    if (!timestamps)
        goto fail;
    r->timestamps = timestamps;
    if (reserve_rc > 0
        && col_rel_publish_retained_reservation(r, &pending, new_bytes) != 0) {
        r->timestamps = NULL;
        free(timestamps);
        goto fail;
    }
    col_rel_ledger_reconcile(r, col_rel_owned_ledger_bytes(r));
    wl_columnar_relation_touch_storage(r);
    return 0;

fail:
    col_rel_reservation_rollback(&pending);
    return ENOMEM;
}

/*
 * ledger_sync_timestamps: bring the TIMESTAMP charge for r->timestamps in
 * line with its current size (Issue #1380).  Every relation.c growth and
 * shrink path already brackets the column change with
 * col_rel_ledger_reconcile(), so piggybacking here covers the realloc of
 * timestamps in the same paths, plus any eval-layer calloc of timestamps
 * on an attached relation at its next reconcile or release.
 */
static void
ledger_sync_timestamps(col_rel_t *r)
{
    uint64_t now = col_rel_timestamp_ledger_bytes(r);
    if (now > r->ledger_ts_bytes) {
        wl_mem_ledger_alloc(r->mem_ledger, WL_MEM_SUBSYS_TIMESTAMP,
            now - r->ledger_ts_bytes);
    } else if (r->ledger_ts_bytes > now) {
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_TIMESTAMP,
            r->ledger_ts_bytes - now);
    }
    r->ledger_ts_bytes = now;
}

void
col_rel_ledger_reconcile(col_rel_t *r, uint64_t before_bytes)
{
    if (!r || !r->mem_ledger)
        return;
    uint64_t after_bytes = col_rel_owned_ledger_bytes(r);
    if (after_bytes > before_bytes) {
        wl_mem_ledger_alloc(r->mem_ledger, WL_MEM_SUBSYS_RELATION,
            after_bytes - before_bytes);
    } else if (before_bytes > after_bytes) {
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_RELATION,
            before_bytes - after_bytes);
    }
    ledger_sync_timestamps(r);
}

void
col_rel_ledger_release(col_rel_t *r)
{
    if (!r || !r->mem_ledger)
        return;
    uint64_t bytes = col_rel_owned_ledger_bytes(r);
    if (bytes > 0)
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_RELATION, bytes);
    if (r->ledger_ts_bytes > 0) {
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_TIMESTAMP,
            r->ledger_ts_bytes);
        r->ledger_ts_bytes = 0;
    }
}

/* ---- lifecycle ---------------------------------------------------------- */

void
col_rel_free_contents(col_rel_t *r)
{
    if (!r)
        return;
    if (r->storage_owner == r && r->storage_alias_borrows > 0)
        return;
    /* Alias bookkeeping is released before the relation is zeroed.  The
     * owner itself is not denied destruction here; #1494 wires this status
     * into the synchronous teardown transaction. */
    (void)col_rel_storage_alias_release(r);
    if (r->memory_governor) {
        if (r->retained_reserved_bytes > 0)
            (void)wl_columnar_memory_release(&r->retained_reservation);
        r->retained_reserved_bytes = 0;
        wl_columnar_memory_governor_ref_release(r->memory_governor);
        r->memory_governor = NULL;
    }
    /* Release only currently heap-owned, non-borrowed data buffers. */
    col_rel_ledger_release(r);
    free(r->name);
    if (!r->arena_owned) {
        if (r->col_shared && r->columns) {
            /* Free only non-shared columns (6B zero-copy sharing) */
            for (uint32_t c = 0; c < r->ncols; c++) {
                if (!r->col_shared[c])
                    free(r->columns[c]);
            }
            free((void *)r->columns);
        } else {
            col_columns_free(r->columns, r->ncols);
        }
    } else {
        /* Arena owns column buffers; only free the columns array itself */
        free((void *)r->columns);
    }
    free(r->col_shared);
    col_columns_free(r->retract_backup_columns, r->ncols);
    col_columns_free(r->merge_columns, r->ncols);
    free(r->row_scratch);
    free(r->timestamps);
    if (r->col_names) {
        for (uint32_t i = 0; i < r->ncols; i++)
            free(r->col_names[i]);
        free((void *)r->col_names);
    }
    free(r->column_types);
    free(r->dedup_slots);
    free(r->compound_arity_map);
    if (r->schema_ok)
        ArrowSchemaRelease(&r->schema);
    memset(r, 0, sizeof(*r));
}

/*
 * col_rel_destroy:
 * Free contents and, if heap-allocated (pool_owned == false), the struct
 * itself.  Pool-owned structs have their memory reclaimed on pool_reset();
 * calling free() on them would corrupt the pool allocator.
 */
int
col_rel_destroy_checked(col_rel_t *r)
{
    int owner_status;
    if (!r)
        return 0;
    owner_status = col_rel_storage_owner_destroy_status(r);
    if (owner_status == EBUSY)
        return EBUSY;
    if (owner_status == 0
        && wl_columnar_source_access_writer_claim(&r->source_access) != 0)
        return EBUSY;
    bool from_pool = r->pool_owned;
    col_rel_free_contents(r); /* memset zeroes pool_owned */
    if (!from_pool)
        free(r);
    /* If pool_owned: struct memory freed on pool_reset(), skip free(). */
    return 0;
}

void
col_rel_destroy(col_rel_t *r)
{
    (void)col_rel_destroy_checked(r);
}

/*
 * col_rel_set_schema:
 * Initialise ncols, col_names[], data buffer, and ArrowSchema.
 * Called lazily on first insert (EDB) or when relation is first produced.
 * Returns 0 on success, ENOMEM/EINVAL on failure.
 */
static int
col_rel_set_schema_impl(col_rel_t *r, uint32_t ncols,
    const char *const *col_names)
{
    wl_columnar_memory_reservation_t pending;
    int pending_rc;
    int failure_rc = EINVAL;
    uint64_t retained_bytes = 0;

    if (!r)
        return EINVAL;
    if (r->ncols != 0)
        return 0; /* already initialised */

    r->ncols = ncols;
    pending_rc = col_rel_reserve_retained(r, COL_REL_INIT_CAP, &pending);
    if (pending_rc < 0) {
        r->ncols = 0;
        return ENOMEM;
    }

    if (ncols > 0) {
        r->capacity = COL_REL_INIT_CAP;
        r->columns = col_columns_alloc(ncols, r->capacity);
        if (!r->columns) {
            failure_rc = ENOMEM;
            goto fail;
        }

        r->col_names = (char **)calloc(ncols, sizeof(char *));
        if (!r->col_names) {
            failure_rc = ENOMEM;
            goto fail;
        }
        for (uint32_t i = 0; i < ncols; i++) {
            if (col_names && col_names[i]) {
                r->col_names[i] = wl_strdup(col_names[i]);
            } else {
                char buf[32];
                snprintf(buf, sizeof(buf), "col%u", i);
                r->col_names[i] = wl_strdup(buf);
            }
            if (!r->col_names[i]) {
                failure_rc = ENOMEM;
                goto fail;
            }
        }
    }

    /* Arrow schema: struct<col0:i64, col1:i64, ...> */
    /* Release any prior schema (handles ncols==0 → ncols>0 upgrade;
     * col_rel_new_auto with ncols=0 sets schema_ok=true with an empty
     * struct schema, and a later set_schema with ncols>0 must release
     * the old schema before reinitializing). */
    if (r->schema_ok) {
        ArrowSchemaRelease(&r->schema);
        r->schema_ok = false;
    }
    ArrowSchemaInit(&r->schema);
    if (ArrowSchemaSetTypeStruct(&r->schema, (int64_t)ncols) != NANOARROW_OK) {
        goto fail;
    }
    for (uint32_t i = 0; i < ncols; i++) {
        enum ArrowType arrow_type = r->column_types
            && r->column_types[i] == WIRELOG_TYPE_FLOAT
            ? NANOARROW_TYPE_DOUBLE : NANOARROW_TYPE_INT64;
        ArrowSchemaRelease(r->schema.children[i]);
        if (ArrowSchemaInitFromType(r->schema.children[i], arrow_type)
            != NANOARROW_OK) {
            goto fail;
        }
        const char *cname
            = (r->col_names && r->col_names[i]) ? r->col_names[i] : "";
        ArrowSchemaSetName(r->schema.children[i], cname);
    }
    r->schema_ok = true;
    if (pending_rc > 0) {
        if (!col_rel_retained_bytes(r->ncols, r->capacity,
            r->timestamps != NULL, &retained_bytes)
            || col_rel_publish_retained_reservation(r, &pending,
            retained_bytes) != 0) {
            failure_rc = ENOMEM;
            goto fail;
        }
    }
    wl_columnar_relation_touch_view(r);
    return 0;

fail:
    col_rel_reservation_rollback(&pending);
    /* ArrowSchemaSetTypeStruct/InitFromType can fail after partially
     * initializing the freshly-created schema while schema_ok is still
     * false.  Release unconditionally: ArrowSchemaRelease is the matching
     * cleanup for every initialized schema state. */
    ArrowSchemaRelease(&r->schema);
    r->schema_ok = false;
    col_columns_free(r->columns, ncols);
    r->columns = NULL;
    if (r->col_names) {
        for (uint32_t i = 0; i < ncols; i++)
            free(r->col_names[i]);
        free((void *)r->col_names);
        r->col_names = NULL;
    }
    r->capacity = 0;
    r->ncols = 0;
    return failure_rc;
}

int
col_rel_set_schema(col_rel_t *r, uint32_t ncols, const char *const *col_names)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc;
    int release_rc;

    if (!r)
        return EINVAL;
    rc = col_rel_published_writer_acquire(r, &writer);
    if (rc != 0)
        return rc;
    rc = col_rel_set_schema_impl(r, ncols, col_names);
    release_rc = wl_columnar_source_access_writer_release(&writer);
    if (rc == 0 && release_rc != 0)
        rc = release_rc;
    return rc;
}

static int
col_rel_prepare_column_types(const col_rel_t *r,
    const wirelog_column_type_t *types, uint32_t ncols,
    wirelog_column_type_t **out_types, struct ArrowSchema *out_schema)
{
    wirelog_column_type_t *copy = NULL;
    if (!r || !out_types || !out_schema || ncols != r->ncols)
        return EINVAL;
    if (ncols > 0) {
        copy = (wirelog_column_type_t *)malloc(
            (size_t)ncols * sizeof(*copy));
        if (!copy)
            return ENOMEM;
        for (uint32_t i = 0; i < ncols; i++)
            copy[i] = types ? types[i] : WIRELOG_TYPE_INT64;
    }
    if (r->nrows > 0 && !r->column_types) {
        for (uint32_t i = 0; i < ncols; i++) {
            if (copy && copy[i] == WIRELOG_TYPE_FLOAT) {
                free(copy);
                return EINVAL;
            }
        }
    }
    for (uint32_t c = 0; c < ncols; c++) {
        if (!copy || copy[c] != WIRELOG_TYPE_FLOAT)
            continue;
        for (uint32_t row = 0; row < r->nrows; row++) {
            if (!wl_columnar_float_bits_valid(r->columns[c][row])) {
                free(copy);
                return EINVAL;
            }
        }
    }

    ArrowSchemaInit(out_schema);
    if (ArrowSchemaSetTypeStruct(out_schema, (int64_t)ncols)
        != NANOARROW_OK) {
        free(copy);
        ArrowSchemaRelease(out_schema);
        return EINVAL;
    }
    for (uint32_t i = 0; i < ncols; i++) {
        enum ArrowType arrow_type = copy && copy[i] == WIRELOG_TYPE_FLOAT
            ? NANOARROW_TYPE_DOUBLE : NANOARROW_TYPE_INT64;
        if (ArrowSchemaInitFromType(out_schema->children[i], arrow_type)
            != NANOARROW_OK) {
            free(copy);
            ArrowSchemaRelease(out_schema);
            return EINVAL;
        }
        ArrowSchemaSetName(out_schema->children[i],
            r->col_names && r->col_names[i] ? r->col_names[i] : "");
    }
    *out_types = copy;
    return 0;
}

static int
col_rel_set_column_types_impl(col_rel_t *r,
    const wirelog_column_type_t *types, uint32_t ncols, bool publish)
{
    if (!r || ncols != r->ncols)
        return EINVAL;
    wirelog_column_type_t *copy = NULL;
    if (ncols > 0) {
        copy = (wirelog_column_type_t *)malloc(
            (size_t)ncols * sizeof(*copy));
        if (!copy)
            return ENOMEM;
        for (uint32_t i = 0; i < ncols; i++)
            copy[i] = types ? types[i] : WIRELOG_TYPE_INT64;
    }
    if (r->nrows > 0 && !r->column_types) {
        for (uint32_t i = 0; i < ncols; i++) {
            if (copy && copy[i] == WIRELOG_TYPE_FLOAT) {
                free(copy);
                return EINVAL;
            }
        }
    }
    /* Validate existing rows before changing their physical interpretation.
     * This keeps a schema update transactional when a relation was already
     * populated through a legacy path. */
    for (uint32_t c = 0; c < ncols; c++) {
        if (copy && copy[c] == WIRELOG_TYPE_FLOAT) {
            for (uint32_t row = 0; row < r->nrows; row++) {
                if (!wl_columnar_float_bits_valid(r->columns[c][row])) {
                    free(copy);
                    return EINVAL;
                }
            }
        }
    }
    if (r->schema_ok) {
        for (uint32_t i = 0; i < ncols; i++) {
            enum ArrowType arrow_type = copy[i]
                == WIRELOG_TYPE_FLOAT ? NANOARROW_TYPE_DOUBLE
                                       : NANOARROW_TYPE_INT64;
            ArrowSchemaRelease(r->schema.children[i]);
            if (ArrowSchemaInitFromType(r->schema.children[i], arrow_type)
                != NANOARROW_OK) {
                for (uint32_t j = 0; j <= i; j++) {
                    enum ArrowType old_type = r->column_types
                        && r->column_types[j] == WIRELOG_TYPE_FLOAT
                        ? NANOARROW_TYPE_DOUBLE : NANOARROW_TYPE_INT64;
                    ArrowSchemaRelease(r->schema.children[j]);
                    (void)ArrowSchemaInitFromType(r->schema.children[j],
                        old_type);
                    ArrowSchemaSetName(r->schema.children[j],
                        r->col_names && r->col_names[j]
                            ? r->col_names[j] : "");
                }
                free(copy);
                return EINVAL;
            }
            const char *name = r->col_names && r->col_names[i]
                ? r->col_names[i] : "";
            ArrowSchemaSetName(r->schema.children[i], name);
        }
    } else if (ncols > 0) {
        ArrowSchemaInit(&r->schema);
        if (ArrowSchemaSetTypeStruct(&r->schema, (int64_t)ncols)
            != NANOARROW_OK) {
            ArrowSchemaRelease(&r->schema);
            free(copy);
            return EINVAL;
        }
        for (uint32_t i = 0; i < ncols; i++) {
            enum ArrowType arrow_type = copy[i] == WIRELOG_TYPE_FLOAT
                ? NANOARROW_TYPE_DOUBLE : NANOARROW_TYPE_INT64;
            if (ArrowSchemaInitFromType(r->schema.children[i], arrow_type)
                != NANOARROW_OK) {
                ArrowSchemaRelease(&r->schema);
                free(copy);
                return EINVAL;
            }
            ArrowSchemaSetName(r->schema.children[i],
                r->col_names && r->col_names[i] ? r->col_names[i] : "");
        }
        r->schema_ok = true;
    }
    free(r->column_types);
    r->column_types = copy;
    if (r->column_types) {
        for (uint32_t c = 0; c < ncols; c++) {
            if (r->column_types[c] != WIRELOG_TYPE_FLOAT)
                continue;
            for (uint32_t row = 0; row < r->nrows; row++) {
                if (wl_columnar_float_bits_zero(r->columns[c][row]))
                    r->columns[c][row] = 0;
            }
        }
    }
    if (publish)
        wl_columnar_relation_touch_view(r);
    return 0;
}

int
col_rel_set_column_types(col_rel_t *r, const wirelog_column_type_t *types,
    uint32_t ncols)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc;
    int release_rc;

    if (!r)
        return EINVAL;
    rc = col_rel_published_writer_acquire(r, &writer);
    if (rc != 0)
        return rc;
    rc = col_rel_set_column_types_impl(r, types, ncols, true);
    release_rc = wl_columnar_source_access_writer_release(&writer);
    if (rc == 0 && release_rc != 0)
        rc = release_rc;
    return rc;
}

int
col_rel_alloc(col_rel_t **out, const char *name)
{
    col_rel_t *r = (col_rel_t *)calloc(1, sizeof(col_rel_t));
    if (!r)
        return ENOMEM;
    r->name = wl_strdup(name);
    if (!r->name) {
        free(r);
        return ENOMEM;
    }
    if (col_rel_new_identity(&r->relation_identity) != 0) {
        free(r->name);
        free(r);
        return EOVERFLOW;
    }
    r->view_generation = 1u;
    r->storage_generation = 1u;
    r->pool_owned = false;
    col_rel_storage_owner_init(r);
    *out = r;
    return 0;
}

/* ------------------------------------------------------------------------ */
/* Compound-column layout (Issue #532 Task 2).                              */
/* ------------------------------------------------------------------------ */

/* Reject an INLINE column that violates the inline-tier invariants. Returns
 * EINVAL on violation, 0 when the column is acceptable. Non-INLINE columns
 * are always acceptable at this layer (SIDE widths are handled below and
 * NONE columns are scalars).
 *
 * K-Fusion C1 (§5): INLINE columns must fit within arity<=MAX_ARITY and
 * depth<=MAX_DEPTH; wider/deeper functors must lower to the SIDE tier.
 * Violations are structured logs (error=arity_overflow|depth_overflow) so
 * operators can diagnose schema-mismatch without abort. */
static int
col_rel_validate_inline_col(const col_rel_logical_col_t *col)
{
    if (col->kind != WIRELOG_COMPOUND_KIND_INLINE)
        return 0;
    if (col->arity == 0u || col->arity > WL_COMPOUND_INLINE_MAX_ARITY) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=validate path=inline error=arity_overflow "
            "expected=1..%u got=%u",
            (unsigned)WL_COMPOUND_INLINE_MAX_ARITY, col->arity);
        return EINVAL;
    }
    if (col->depth > WL_COMPOUND_INLINE_MAX_DEPTH) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=validate path=inline error=depth_overflow "
            "expected=0..%u got=%u",
            (unsigned)WL_COMPOUND_INLINE_MAX_DEPTH, col->depth);
        return EINVAL;
    }
    return 0;
}

/* Width contribution of one logical column in the physical schema.
 * NONE/SIDE contribute a single slot (SIDE stores only the opaque handle
 * into the side-relation). INLINE contributes `arity` slots; callers must
 * have already accepted the column via col_rel_validate_inline_col. */
static uint32_t
col_rel_slot_width(const col_rel_logical_col_t *col)
{
    if (col->kind == WIRELOG_COMPOUND_KIND_INLINE)
        return col->arity;
    return 1u;
}

int
col_rel_compute_physical_layout(const col_rel_logical_col_t *logical_cols,
    uint32_t logical_ncols,
    uint32_t *out_physical_ncols,
    uint32_t *out_offset_map,
    uint32_t *out_inline_physical_offset,
    uint32_t *out_compound_count)
{
    if (!logical_cols || logical_ncols == 0u) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=layout error=bad_input logical_cols=%p logical_ncols=%u",
            (const void *)logical_cols, logical_ncols);
        return EINVAL;
    }

    /* Validate before touching outputs so EINVAL leaves everything intact.
    * Per-column failures are already logged inside validate_inline_col. */
    for (uint32_t i = 0; i < logical_ncols; i++) {
        int rc = col_rel_validate_inline_col(&logical_cols[i]);
        if (rc != 0) {
            WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
                "event=layout error=validation logical_col=%u", i);
            return rc;
        }
    }

    uint32_t physical = 0u;
    uint32_t compound_count = 0u;
    uint32_t first_inline_offset = 0u;
    bool saw_inline = false;

    for (uint32_t i = 0; i < logical_ncols; i++) {
        if (out_offset_map)
            out_offset_map[i] = physical;
        if (logical_cols[i].kind == WIRELOG_COMPOUND_KIND_INLINE) {
            if (!saw_inline) {
                first_inline_offset = physical;
                saw_inline = true;
            }
            compound_count++;
        }
        physical += col_rel_slot_width(&logical_cols[i]);
    }

    if (out_physical_ncols)
        *out_physical_ncols = physical;
    if (out_inline_physical_offset)
        *out_inline_physical_offset = first_inline_offset;
    if (out_compound_count)
        *out_compound_count = compound_count;
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_DEBUG,
        "event=layout path=%s logical=%u physical=%u compound=%u "
        "first_inline_offset=%u",
        compound_count > 0u ? "inline" : "scalar",
        logical_ncols, physical, compound_count, first_inline_offset);
    return 0;
}

int
col_rel_apply_compound_schema(col_rel_t *r,
    const col_rel_logical_col_t *logical_cols,
    uint32_t logical_ncols)
{
    if (!r) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=apply_schema error=null_relation");
        return EINVAL;
    }

    /* Reuse the pure layout routine for validation + offset math so the
     * two entry points cannot drift. We don't need the per-column offsets
     * here, but the prefix sum walks every column exactly once. */
    uint32_t compound_count = 0u;
    uint32_t inline_offset = 0u;
    int rc = col_rel_compute_physical_layout(logical_cols, logical_ncols,
            NULL, NULL, &inline_offset, &compound_count);
    if (rc != 0)
        return rc;

    uint32_t *arity_map
        = (uint32_t *)malloc((size_t)logical_ncols * sizeof(uint32_t));
    if (!arity_map) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_ERROR,
            "event=apply_schema error=oom bytes=%zu",
            (size_t)logical_ncols * sizeof(uint32_t));
        return ENOMEM;
    }

    bool has_inline = false;
    bool has_side = false;
    for (uint32_t i = 0; i < logical_ncols; i++) {
        arity_map[i] = col_rel_slot_width(&logical_cols[i]);
        if (logical_cols[i].kind == WIRELOG_COMPOUND_KIND_INLINE)
            has_inline = true;
        else if (logical_cols[i].kind == WIRELOG_COMPOUND_KIND_SIDE)
            has_side = true;
    }

    /* Commit: free any prior map before taking ownership of the new one. */
    free(r->compound_arity_map);
    r->compound_arity_map = arity_map;
    r->compound_count = compound_count;
    r->inline_physical_offset = inline_offset;
    const char *path_tag;
    if (has_inline) {
        r->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
        path_tag = "inline";
    } else if (has_side) {
        r->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        path_tag = "side";
    } else {
        r->compound_kind = WIRELOG_COMPOUND_KIND_NONE;
        path_tag = "none";
    }
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_DEBUG,
        "event=apply_schema path=%s rel=%s logical=%u compound_count=%u "
        "inline_offset=%u",
        path_tag, r->name ? r->name : "(anon)", logical_ncols,
        compound_count, inline_offset);
    wl_columnar_relation_touch_view(r);
    return 0;
}

static int
col_rel_append_row_impl(col_rel_t *r, const int64_t *row,
    wl_columnar_source_access_writer_t *writer, bool writer_held)
{
    bool alias_release_pending = false;
    int rc;

    if (!r || !row || !writer)
        return EINVAL;
    /* Do all structural validation before any resize, COW, or timestamp
     * publication.  In particular, a zero-column or partially initialized
     * relation must fail without consuming capacity or a generation epoch. */
    /* Zero-arity relations are valid empty-tuple relations.  A relation with
     * physical columns, however, must have every column buffer present. */
    if (r->ncols == 0
        && (r->relation_identity == 0 || r->view_generation == 0
        || r->storage_generation == 0))
        return EINVAL;
    bool empty_unallocated = r->nrows == 0 && r->capacity == 0;
    if (r->ncols > 0 && !r->columns && !empty_unallocated)
        return EINVAL;
    for (uint32_t c = 0; c < r->ncols; c++)
        if ((!r->columns || !r->columns[c]) && !empty_unallocated)
            return EINVAL;
    if (r->column_types) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->column_types[c] == WIRELOG_TYPE_FLOAT
                && !wl_columnar_float_bits_valid(row[c]))
                return EINVAL;
        }
    }

    /* Resolve and admit the canonical storage owner only after validation,
     * but before any resize, COW, timestamp, value, or generation mutation.
     * A reader on an alias therefore excludes append on every view of the
     * same backing storage. */
    if (!writer_held) {
        rc = col_rel_source_writer_acquire(r, writer);
        if (rc != 0)
            return rc;
    }

    bool needs_resize = r->nrows >= r->capacity;
    if (needs_resize) {
        /* Canonical-owner storage replacement frees the old buffers.  Live
         * shared views still point at those buffers, so refuse growth until
         * the aliases are released.  A shared-view destination is allowed
         * to COW/grow because it replaces its own alias safely. */
        if (r->storage_owner == r && r->storage_alias_borrows > 0) {
            rc = EBUSY;
            goto release_writer;
        }
        uint64_t ledger_before = col_rel_owned_ledger_bytes(r);
        uint32_t new_cap = r->capacity ? r->capacity * 2 : COL_REL_INIT_CAP;
        if (new_cap <= r->capacity) /* overflow guard */
            goto enomem;
        if (r->col_shared || r->arena_owned) {
            /* Ownership transitions stage columns and timestamps together;
             * admission happens before any source buffer is copied and the
             * helper publishes the storage generation on success. */
            if (col_rel_grow_owned_transition_impl(r, new_cap, true) != 0)
                goto enomem;
            alias_release_pending = true;
        } else {
            /* Heap-owned growth is one transaction: admit the new footprint
             * (retained relations only), prepare private buffers, commit the
             * token, then publish.  Nothing observable changes on failure. */
            wl_columnar_memory_reservation_t pending;
            int pending_rc = 0;
            uint64_t new_bytes = 0;
            bool admitted = r->memory_governor != NULL;
            if (admitted) {
                pending_rc = col_rel_reserve_retained(r, new_cap, &pending);
                if (pending_rc < 0)
                    goto enomem;
                if (!col_rel_retained_bytes(r->ncols, new_cap,
                    r->timestamps != NULL, &new_bytes)) {
                    col_rel_reservation_rollback(&pending);
                    goto enomem;
                }
            }
            int64_t **new_cols = NULL;
            col_delta_timestamp_t *new_ts = NULL;
            int prepare_rc = col_rel_prepare_resize(r, new_cap, &new_cols,
                    &new_ts);
            if (prepare_rc != 0) {
                if (admitted)
                    col_rel_reservation_rollback(&pending);
                rc = prepare_rc;
                goto release_writer;
            }
            if (admitted && pending_rc > 0
                && col_rel_publish_retained_reservation(r, &pending,
                new_bytes) != 0) {
                col_rel_reservation_rollback(&pending);
                col_columns_free(new_cols, r->ncols);
                free(new_ts);
                goto enomem;
            }
            col_rel_publish_resize(r, new_cols, new_ts, new_cap);
            col_rel_ledger_reconcile(r, ledger_before);
            wl_columnar_relation_touch_storage(r);
        }
    }
    /* A shared view can still have spare capacity.  Privatize it before the
     * in-place row write even when no capacity growth is needed. */
    if (r->col_shared) {
        if (col_rel_cow_unshare_impl(r, 0, true) != 0)
            goto enomem;
        alias_release_pending = true;
    }
#ifdef WL_TEST_APPEND_HOOK
    if (alias_release_pending && wl_columnar_append_transition_hook)
        wl_columnar_append_transition_hook(r);
#endif
    if (r->timestamps)
        memset(&r->timestamps[r->nrows], 0, sizeof(col_delta_timestamp_t));
    /* Structural and value validation above makes this raw copy
    * non-failing; retain the check as a defensive invariant. */
    if (col_rel_row_copy_in_raw(r, r->nrows, row) != 0)
        goto einval;
    if (r->column_types) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->column_types[c] == WIRELOG_TYPE_FLOAT
                && wl_columnar_float_bits_zero(r->columns[c][r->nrows]))
                r->columns[c][r->nrows] = 0;
        }
    }
    r->nrows++;
    wl_columnar_relation_touch_view(r);
    rc = 0;
    goto release_writer;

enomem:
    rc = ENOMEM;
    goto release_writer;
einval:
    rc = EINVAL;
release_writer:
    if (alias_release_pending) {
        int alias_rc = col_rel_storage_alias_release(r);
        if (alias_rc != 0 && rc == 0)
            rc = alias_rc;
    }
    if (!writer_held
        && wl_columnar_source_access_writer_release(writer) != 0 && rc == 0)
        rc = EINVAL;
    return rc;
}

int
col_rel_append_row(col_rel_t *r, const int64_t *row)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    return col_rel_append_row_impl(r, row, &writer, false);
}

int
col_rel_append_row_locked(col_rel_t *r, const int64_t *row,
    wl_columnar_source_access_writer_t *writer)
{
    return col_rel_append_row_impl(r, row, writer, true);
}

/* Reserve the complete destination shape before a multi-row operation starts.
 * The caller holds the destination writer.  This keeps subsequent locked
 * appends allocation-free, so a destination admission failure cannot leave
 * a partially emitted delta after the source has been transformed. */
int
col_rel_reserve_rows_locked(col_rel_t *r, uint32_t additional,
    wl_columnar_source_access_writer_t *writer)
{
    if (!r || !writer || additional > UINT32_MAX - r->nrows)
        return EINVAL;
    uint32_t required = r->nrows + additional;
    /* A shared view can have spare capacity but still needs COW before the
     * first locked append.  Perform that fallible transition up front. */
    if (r->col_shared && required <= r->capacity)
        return col_rel_cow_unshare_impl(r, 0, false);
    if (required <= r->capacity)
        return 0;
    if (r->storage_owner == r && r->storage_alias_borrows > 0)
        return EBUSY;

    uint32_t new_cap = r->capacity ? r->capacity : COL_REL_INIT_CAP;
    while (new_cap < required) {
        if (new_cap > UINT32_MAX / 2u)
            return ENOMEM;
        new_cap *= 2u;
    }
    if (r->col_shared || r->arena_owned)
        return col_rel_grow_owned_transition_impl(r, new_cap, false);

    uint64_t ledger_before = col_rel_owned_ledger_bytes(r);
    wl_columnar_memory_reservation_t pending;
    int pending_rc = 0;
    uint64_t new_bytes = 0;
    bool admitted = r->memory_governor != NULL;
    if (admitted) {
        pending_rc = col_rel_reserve_retained(r, new_cap, &pending);
        if (pending_rc < 0
            || !col_rel_retained_bytes(r->ncols, new_cap,
            r->timestamps != NULL, &new_bytes)) {
            if (pending_rc > 0)
                col_rel_reservation_rollback(&pending);
            return ENOMEM;
        }
    }
    int64_t **new_cols = NULL;
    col_delta_timestamp_t *new_ts = NULL;
    int rc = col_rel_prepare_resize(r, new_cap, &new_cols, &new_ts);
    if (rc != 0) {
        if (admitted)
            col_rel_reservation_rollback(&pending);
        return rc;
    }
    if (admitted && pending_rc > 0
        && col_rel_publish_retained_reservation(r, &pending, new_bytes) != 0) {
        col_rel_reservation_rollback(&pending);
        col_columns_free(new_cols, r->ncols);
        free(new_ts);
        return ENOMEM;
    }
    col_rel_publish_resize(r, new_cols, new_ts, new_cap);
    col_rel_ledger_reconcile(r, ledger_before);
    wl_columnar_relation_touch_storage(r);
    return 0;
}

int
col_rel_reset_rows_locked(col_rel_t *r,
    wl_columnar_source_access_writer_t *writer)
{
    col_rel_t *owner = NULL;
    if (!r || !writer || !writer->owner
        || writer->identity != (uintptr_t)writer
        || !writer->thread_valid
        || !wl_columnar_source_access_writer_thread_equal(writer)
        || col_rel_storage_owner_resolve(r, &owner) != 0
        || writer->owner != &owner->source_access)
        return EINVAL;
    if (owner->storage_alias_borrows > 0)
        return EBUSY;
    r->nrows = 0;
    r->sorted_nrows = 0;
    r->base_nrows = 0;
    r->run_count = 0;
    memset(r->run_ends, 0, sizeof(r->run_ends));
    uint64_t ledger_before = col_rel_owned_ledger_bytes(r);
    free(r->timestamps);
    r->timestamps = NULL;
    free(r->dedup_slots);
    r->dedup_slots = NULL;
    r->dedup_cap = 0;
    r->dedup_count = 0;
    col_rel_ledger_reconcile(r, ledger_before);
    wl_columnar_relation_touch_replacement(r);
    return 0;
}

/* Copy all rows from src into dst (must have same ncols).
 * If src has timestamps and dst has timestamp tracking enabled, the source
 * timestamps are propagated to the newly appended rows.
 * Optimized (Issue #300): bulk memcpy instead of per-row append. */
int
col_rel_append_all(col_rel_t *dst, const col_rel_t *src, wl_arena_t *arena)
{
    col_rel_t *src_owner = NULL;
    col_rel_t *dst_owner = NULL;
    wl_columnar_source_access_reader_t source_reader = { 0 };
    wl_columnar_source_access_writer_t destination_writer = { 0 };
    bool source_reader_acquired = false;
    bool destination_writer_acquired = false;
    bool alias_release_pending = false;
    wirelog_column_type_t *prepared_types = NULL;
    struct ArrowSchema prepared_schema;
    bool prepared_schema_ok = false;
    int rc;
    (void)arena;
    if (!dst || !src || dst->ncols != src->ncols)
        return EINVAL;
    if (src->nrows == 0) {
        /* An empty source still completes a destination ownership
         * transition.  In particular, a reused TDD destination may be a
         * shared view whose old source lease must not survive a successful
         * fallback merely because there are no rows to copy. */
        if (dst->col_shared) {
            wl_columnar_source_access_writer_t writer = { 0 };
            rc = col_rel_source_writer_acquire(dst, &writer);
            if (rc != 0)
                return rc;
            rc = col_rel_cow_unshare(dst, 0);
            if (wl_columnar_source_access_writer_release(&writer) != 0
                && rc == 0)
                rc = EINVAL;
            return rc;
        }
        return 0;
    }

    /* Preserve the historical zero-initialized, zero-column arithmetic path.
     * These stack relations have no generation or owner metadata, so owner
     * resolution would turn a formerly valid row-count operation into EINVAL.
     * Initialized zero-column relations have an owner and use the normal
     * source-access gate below. */
    if (dst->ncols == 0 && src->ncols == 0
        && !dst->storage_owner && !src->storage_owner) {
        if (src->nrows > UINT32_MAX - dst->nrows)
            return EOVERFLOW;
        dst->nrows += src->nrows;
        wl_columnar_relation_touch_view(dst);
        return 0;
    }

    /* Resolve canonical owners before reading mutable source or destination
     * state.  A same-owner append uses one writer token: the source reader
     * and destination writer cannot coexist on one gate. */
    rc = col_rel_storage_owner_resolve(src, &src_owner);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(dst, &dst_owner);
    if (rc != 0)
        return rc;
    if (src_owner == dst_owner) {
        rc = wl_columnar_source_access_writer_acquire(
            &dst_owner->source_access, &destination_writer);
        if (rc != 0)
            return rc;
        destination_writer_acquired = true;
    } else {
        rc = wl_columnar_source_access_reader_acquire(
            &src_owner->source_access, &source_reader);
        if (rc != 0)
            return rc;
        source_reader_acquired = true;
        rc = wl_columnar_source_access_writer_acquire(
            &dst_owner->source_access, &destination_writer);
        if (rc != 0)
            goto cleanup;
        destination_writer_acquired = true;
    }

    /* A typed relation must never receive lanes whose physical meaning is
     * unknown or differs from its own.  Legacy untyped relations are treated
     * as int64, which preserves the pre-typed append contract. */
    if (dst->column_types || src->column_types) {
        for (uint32_t c = 0; c < dst->ncols; c++) {
            wirelog_column_type_t dst_type = dst->column_types
                ? dst->column_types[c] : WIRELOG_TYPE_INT64;
            wirelog_column_type_t src_type = src->column_types
                ? src->column_types[c] : WIRELOG_TYPE_INT64;
            /* Existing integer-family relations all use the same int64
             * storage lane, and legacy producers do not carry metadata.
             * Float lanes are different: accepting an untyped or differently
             * typed float source would reinterpret its bits. */
            if ((dst_type == WIRELOG_TYPE_FLOAT
                || src_type == WIRELOG_TYPE_FLOAT)
                && dst_type != src_type)
                goto invalid;
            if (!dst->column_types && src->column_types
                && src_type == WIRELOG_TYPE_FLOAT && dst->nrows > 0)
                goto invalid;
        }
    }

    if (src->column_types) {
        for (uint32_t row = 0; row < src->nrows; row++) {
            for (uint32_t c = 0; c < src->ncols; c++) {
                if (src->column_types[c] == WIRELOG_TYPE_FLOAT
                    && !wl_columnar_float_bits_valid(src->columns[c][row]))
                    goto invalid;
            }
        }
        if (!dst->column_types) {
            for (uint32_t row = 0; row < dst->nrows; row++) {
                for (uint32_t c = 0; c < dst->ncols; c++) {
                    if (src->column_types[c] == WIRELOG_TYPE_FLOAT
                        && !wl_columnar_float_bits_valid(
                            dst->columns[c][row]))
                        goto invalid;
                }
            }
        }
    }

    if (src->nrows > UINT32_MAX - dst->nrows)
        goto overflow;

    uint32_t dst_base = dst->nrows;
    uint32_t new_nrows = dst->nrows + src->nrows;

    /* Replacing canonical-owner storage frees buffers still referenced by
     * live aliases.  Keep the raw-pointer alias model safe by refusing only
     * owner growth; alias destinations may still detach and grow privately. */
    if (dst == dst_owner && dst_owner->storage_alias_borrows > 0
        && new_nrows > dst->capacity) {
        rc = EBUSY;
        goto cleanup;
    }

    /* Type metadata is part of the append transaction.  Prepare its owned
     * copy and complete Arrow schema before resizing or changing dst. */
    if (!dst->column_types && src->column_types) {
        int type_rc = col_rel_prepare_column_types(dst, src->column_types,
                dst->ncols, &prepared_types, &prepared_schema);
        if (type_rc != 0) {
            rc = type_rc;
            goto cleanup;
        }
        prepared_schema_ok = true;
    }

    /* Ensure dst has sufficient capacity. */
    if (new_nrows > dst->capacity) {
        uint64_t ledger_before = col_rel_owned_ledger_bytes(dst);
        bool transitioned = false;
        uint32_t new_cap = dst->capacity ? dst->capacity * 2 : COL_REL_INIT_CAP;
        while (new_cap < new_nrows) {
            uint32_t next_cap = new_cap * 2;
            if (next_cap <= new_cap) /* overflow guard */
                goto overflow;
            new_cap = next_cap;
        }

        if (dst->arena_owned && !dst->col_shared && !dst->memory_governor
            && arena) {
            /* An unmanaged arena relation still belongs to its caller's
             * arena.  Grow its columns in that arena and retain the ownership
             * flag; admission-based heap promotion is only for retained
             * relations (or for the legacy no-arena fallback below). */
            int64_t **new_columns = (int64_t **)calloc(dst->ncols,
                    sizeof(*new_columns));
            col_delta_timestamp_t *new_timestamps = NULL;
            if (!new_columns) {
                rc = ENOMEM;
                goto cleanup;
            }
            for (uint32_t c = 0; c < dst->ncols; c++) {
                new_columns[c] = (int64_t *)wl_arena_alloc(arena,
                        (size_t)new_cap * sizeof(int64_t));
                if (!new_columns[c]) {
                    free((void *)new_columns);
                    rc = ENOMEM;
                    goto cleanup;
                }
                memcpy(new_columns[c], dst->columns[c],
                    (size_t)dst->nrows * sizeof(int64_t));
            }
            if (dst->timestamps) {
                new_timestamps = (col_delta_timestamp_t *)malloc(
                    (size_t)new_cap * sizeof(*new_timestamps));
                if (!new_timestamps) {
                    free((void *)new_columns);
                    rc = ENOMEM;
                    goto cleanup;
                }
                memcpy(new_timestamps, dst->timestamps,
                    (size_t)dst->nrows * sizeof(*new_timestamps));
            }
            free((void *)dst->columns);
            free(dst->timestamps);
            dst->columns = new_columns;
            dst->timestamps = new_timestamps;
            dst->capacity = new_cap;
            /* Arena columns are not charged to RELATION, but timestamps are
             * heap-backed and may have grown.  Keep the attached ledger in
             * sync even though this ownership-preserving path is marked as a
             * transition below. */
            col_rel_ledger_reconcile(dst, ledger_before);
            wl_columnar_relation_touch_storage(dst);
            transitioned = true;
        } else if (dst->col_shared || dst->arena_owned) {
            rc = col_rel_grow_owned_transition_impl(dst, new_cap, true);
            if (rc != 0)
                goto cleanup;
            transitioned = true;
            alias_release_pending = true;
        } else {
            /* Heap-owned growth: admit (retained relations), prepare, commit,
             * publish -- see col_rel_append_row. */
            wl_columnar_memory_reservation_t pending;
            int pending_rc = 0;
            uint64_t new_bytes = 0;
            bool admitted = dst->memory_governor != NULL;
            if (admitted) {
                pending_rc = col_rel_reserve_retained(dst, new_cap, &pending);
                if (pending_rc < 0)
                    goto allocation_failure;
                if (!col_rel_retained_bytes(dst->ncols, new_cap,
                    dst->timestamps != NULL, &new_bytes)) {
                    col_rel_reservation_rollback(&pending);
                    goto allocation_failure;
                }
            }
            int64_t **new_cols = NULL;
            col_delta_timestamp_t *new_ts = NULL;
            int prepare_rc = col_rel_prepare_resize(dst, new_cap,
                    &new_cols, &new_ts);
            if (prepare_rc != 0) {
                if (admitted)
                    col_rel_reservation_rollback(&pending);
                rc = prepare_rc;
                goto cleanup;
            }
            if (admitted && pending_rc > 0
                && col_rel_publish_retained_reservation(dst, &pending,
                new_bytes) != 0) {
                col_rel_reservation_rollback(&pending);
                col_columns_free(new_cols, dst->ncols);
                free(new_ts);
                goto allocation_failure;
            }
            col_rel_publish_resize(dst, new_cols, new_ts, new_cap);
            transitioned = false;
        }
        if (!transitioned) {
            col_rel_ledger_reconcile(dst, ledger_before);
            wl_columnar_relation_touch_storage(dst);
        }
    }

    /* Bulk append also mutates spare capacity, so a shared view must be
     * privatized even when the destination does not grow. */
    if (dst->col_shared) {
        rc = col_rel_cow_unshare_impl(dst, 0, true);
        if (rc != 0)
            goto cleanup;
        alias_release_pending = true;
    }

#ifdef WL_TEST_APPEND_HOOK
    if (alias_release_pending && wl_columnar_append_transition_hook)
        wl_columnar_append_transition_hook(dst);
#endif

    /* Install the prepared type metadata only after all potentially failing
     * destination growth has completed.  A rejected append therefore cannot
     * change an untyped destination's schema. */
    if (prepared_schema_ok) {
        if (dst->schema_ok)
            ArrowSchemaRelease(&dst->schema);
        free(dst->column_types);
        dst->column_types = prepared_types;
        dst->schema = prepared_schema;
        dst->schema_ok = true;
        prepared_types = NULL;
        prepared_schema_ok = false;
    }

    /* Bulk copy all rows per-column.  All fallible preparation is complete. */
    for (uint32_t c = 0; c < dst->ncols; c++)
        memcpy(dst->columns[c] + dst_base, src->columns[c],
            (size_t)src->nrows * sizeof(int64_t));
    if (dst->column_types) {
        for (uint32_t c = 0; c < dst->ncols; c++) {
            if (dst->column_types[c] != WIRELOG_TYPE_FLOAT)
                continue;
            for (uint32_t row = dst_base; row < new_nrows; row++) {
                if (wl_columnar_float_bits_zero(dst->columns[c][row]))
                    dst->columns[c][row] = 0;
            }
        }
    }
    dst->nrows = new_nrows;

    /* Copy timestamps if both have tracking enabled */
    if (src->timestamps && dst->timestamps)
        memcpy(&dst->timestamps[dst_base], src->timestamps,
            src->nrows * sizeof(col_delta_timestamp_t));

    wl_columnar_relation_touch_view(dst);

    rc = 0;
    goto cleanup;

invalid:
    rc = EINVAL;
    goto cleanup;
overflow:
    rc = EOVERFLOW;
    goto cleanup;
allocation_failure:
    rc = ENOMEM;
cleanup:
    free(prepared_types);
    if (prepared_schema_ok)
        ArrowSchemaRelease(&prepared_schema);
    if (alias_release_pending) {
        int alias_rc = col_rel_storage_alias_release(dst);
        if (alias_rc != 0 && rc == 0)
            rc = alias_rc;
    }
    if (destination_writer_acquired
        && wl_columnar_source_access_writer_release(&destination_writer) != 0
        && rc == 0)
        rc = EINVAL;
    if (source_reader_acquired
        && wl_columnar_source_access_reader_release(&source_reader) != 0
        && rc == 0)
        rc = EINVAL;
    return rc;
}

/* ---- compaction ---------------------------------------------------------- */

/*
 * col_rel_compact:
 * Shrink oversized data and timestamps buffers after bulk retraction.
 *
 * Guards:
 *   - NULL or ncols==0: no-op
 *   - nrows==0: release data, merge_buf, timestamps; zero capacities
 *   - capacity <= nrows*4: already tight enough; skip
 *
 * On success the capacity is reduced to max(nrows*2, COL_REL_INIT_CAP).
 * merge_buf is always freed (it will be re-allocated on next consolidation).
 * Allocation failures are non-fatal: the relation remains valid.
 * sorted_nrows is clamped to nrows if it drifted above.
 */
void
col_rel_compact(col_rel_t *r)
{
    if (!r || r->ncols == 0)
        return;
    uint64_t ledger_before = col_rel_owned_ledger_bytes(r);

    if (r->nrows == 0) {
        bool storage_changed = r->columns != NULL || r->timestamps != NULL;
        col_rel_ledger_release(r);
        if (r->memory_governor) {
            /* Empty compaction drops every retained buffer.  Release the
             * matching admission before allowing a later append to start a
             * new capacity; otherwise reserve_growth() would compare the
             * new shape with a stale, larger token. */
            if (r->retained_reserved_bytes > 0)
                (void)wl_columnar_memory_release(
                    &r->retained_reservation);
            r->retained_reserved_bytes = 0;
            wl_columnar_memory_reservation_init(
                &r->retained_reservation);
        }
        if (!r->arena_owned) {
            if (r->col_shared && r->columns) {
                /* COW: free only non-shared columns (Issue #396) */
                for (uint32_t c = 0; c < r->ncols; c++) {
                    if (!r->col_shared[c])
                        free(r->columns[c]);
                }
                free((void *)r->columns);
                free(r->col_shared);
                r->col_shared = NULL;
            } else {
                col_columns_free(r->columns, r->ncols);
            }
        } else {
            free((void *)r->columns);
        }
        r->columns = NULL;
        r->capacity = 0;
        r->arena_owned = false;
        col_columns_free(r->merge_columns, r->ncols);
        r->merge_columns = NULL;
        r->merge_buf_cap = 0;
        free(r->timestamps);
        r->timestamps = NULL;
        r->sorted_nrows = 0;
        r->base_nrows = 0;
        if (storage_changed)
            wl_columnar_relation_touch_storage(r);
        return;
    }

    /* Only compact when buffer is more than 4x oversized.
     * Cast to uint64_t to prevent overflow when nrows > UINT32_MAX/4. */
    if (r->capacity <= (uint64_t)r->nrows * 4)
        goto free_merge_buf;

    /* A retained relation's admission token covers the complete live heap
     * shape.  The governor has no atomic shrink operation: reserving the
     * replacement while the old token is committed would incorrectly charge
     * the temporary overlap, while releasing first would make compaction
     * failure observable.  Keep the existing buffers in this case; this is
     * conservative and preserves an exact token for subsequent growth. */
    if (r->memory_governor)
        goto free_merge_buf;

    {
        uint32_t tight = r->nrows * 2;
        if (tight < r->nrows) /* overflow guard */
            tight = UINT32_MAX;
        if (tight < COL_REL_INIT_CAP)
            tight = COL_REL_INIT_CAP;

        /* Shrinking (and, for a shared view, privatizing) is one transaction:
         * the tight private buffers are prepared first and published only
         * once every allocation succeeded, so a failure leaves the
         * relation, its ownership flags and its generations untouched. */
        int64_t **new_cols = NULL;
        col_delta_timestamp_t *new_ts = NULL;
        if (col_rel_prepare_resize(r, tight, &new_cols, &new_ts) != 0)
            goto free_merge_buf;
        col_rel_publish_resize(r, new_cols, new_ts, tight);
        col_rel_ledger_reconcile(r, ledger_before);
        wl_columnar_relation_touch_storage(r);
    }

free_merge_buf:
    col_columns_free(r->merge_columns, r->ncols);
    r->merge_columns = NULL;
    r->merge_buf_cap = 0;

    if (r->sorted_nrows > r->nrows)
        r->sorted_nrows = r->nrows;
    if (r->base_nrows > r->nrows)
        r->base_nrows = r->nrows;
}

/*
 * col_rel_install_shared_view:
 * Install src's column buffers into dst as a zero-copy shared view.
 * dst's existing columns are freed (respecting any existing col_shared flags).
 * After this call, dst->columns[c] points to src->columns[c] and
 * dst->col_shared[c] is true for all c, so col_rel_free_contents/COW
 * will not free the borrowed buffers.
 *
 * This function does not establish a lifetime reference on src.  The caller
 * must keep src (and its storage) alive, or use the pin/deferred-destruction
 * contract from Issues #1384/#1435 before installing a view.  Generation
 * publication here intentionally does not invent a second ownership model.
 *
 * dst and src must have the same ncols.
 * Returns 0 on success, ENOMEM if the col_shared flags array cannot be
 * allocated (dst remains valid with its original columns in that case).
 *
 * Issue #396: used by tdd_broadcast_deltas to eliminate O(|delta|) deep
 * copies when broadcasting the union delta to worker sessions.
 */
static int
col_rel_install_shared_view_unprotected(col_rel_t *dst, const col_rel_t *src)
{
    col_rel_t *source_owner = NULL;
    col_rel_t *old_owner = NULL;
    wirelog_column_type_t *shared_types = NULL;
    int64_t **shared_columns = NULL;
    bool *shared_flags = NULL;
    col_delta_timestamp_t *shared_timestamps = NULL;
    char **shared_names = NULL;
    uint32_t *shared_arity_map = NULL;
    uint32_t shared_arity_len = 0;
    bool shared_has_graph_column = false;
    uint32_t shared_graph_col_idx = 0;
    uint32_t shared_declared_ncols = 0;
    wirelog_compound_kind_t shared_compound_kind
        = WIRELOG_COMPOUND_KIND_NONE;
    uint32_t shared_compound_count = 0;
    uint32_t shared_inline_physical_offset = 0;
    struct ArrowSchema prepared_schema;
    bool prepared_schema_ok = false;
    int prepare_rc = ENOMEM;
    if (!dst || !src || dst == src || dst->ncols != src->ncols)
        return EINVAL;
    if (!dst->storage_owner)
        col_rel_storage_owner_init(dst);
    if (col_rel_storage_owner_resolve(src, &source_owner) != 0
        || col_rel_storage_owner_resolve(dst, &old_owner) != 0)
        return EINVAL;
    /* Pool and arena owners can outlive their relation descriptors through
     * allocator reset, so they cannot be represented by a raw owner pointer
     * until the later allocator-lifetime unit adds a stable control block. */
    if (source_owner->pool_owned || source_owner->arena_owned)
        return EINVAL;
    /* A relation with live flattened aliases cannot itself become an alias:
     * doing so would invalidate the children's canonical owner. */
    if (dst->storage_alias_borrows > 0 || source_owner == dst
        || source_owner->storage_alias_borrows == UINT32_MAX)
        return source_owner == dst ? EINVAL : EBUSY;
    if (old_owner != dst && old_owner->storage_alias_borrows == 0)
        return EINVAL;
    /* A shared-view publication is a new epoch on the destination.  Reserve
     * the next values before replacing any buffers so exhaustion cannot leave
     * a successfully installed view with an invalid generation. */
    if (!wl_columnar_relation_generation_valid(dst->view_generation)
        || !wl_columnar_relation_generation_valid(dst->storage_generation)
        || dst->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || dst->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EOVERFLOW;
    if (src->has_graph_column && src->graph_col_idx >= src->ncols)
        return EINVAL;
    if (src->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        && src->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        && src->compound_kind != WIRELOG_COMPOUND_KIND_SIDE)
        return EINVAL;
    /* The map has no separate length field.  Bound the inferred logical
     * length by the physical width because every valid entry contributes at
     * least one physical slot. */
    if (src->compound_kind != WIRELOG_COMPOUND_KIND_NONE) {
        if (!src->compound_arity_map || src->ncols == 0u)
            return EINVAL;
        uint64_t physical_width = 0;
        while (shared_arity_len < src->ncols
            && physical_width < src->ncols) {
            uint32_t arity = src->compound_arity_map[shared_arity_len];
            if (arity == 0u
                || physical_width + (uint64_t)arity > src->ncols)
                return EINVAL;
            physical_width += arity;
            shared_arity_len++;
        }
        if (physical_width != src->ncols || shared_arity_len == 0u)
            return EINVAL;
        if (src->compound_kind == WIRELOG_COMPOUND_KIND_INLINE) {
            if (src->compound_count == 0u
                || src->compound_count > shared_arity_len
                || src->inline_physical_offset >= src->ncols)
                return EINVAL;
        } else if (src->compound_count != 0u
            || src->inline_physical_offset != 0u) {
            return EINVAL;
        }
        shared_arity_map = (uint32_t *)malloc(
            (size_t)shared_arity_len * sizeof(*shared_arity_map));
        if (!shared_arity_map)
            goto prepare_fail;
        memcpy(shared_arity_map, src->compound_arity_map,
            (size_t)shared_arity_len * sizeof(*shared_arity_map));
        shared_compound_kind = src->compound_kind;
        shared_compound_count = src->compound_count;
        shared_inline_physical_offset = src->inline_physical_offset;
    }
    shared_has_graph_column = src->has_graph_column;
    shared_graph_col_idx = src->graph_col_idx;
    shared_declared_ncols = src->declared_ncols;
    if (dst->ncols > 0) {
        shared_columns = (int64_t **)calloc(dst->ncols,
                sizeof(*shared_columns));
        shared_flags = (bool *)calloc(dst->ncols, sizeof(*shared_flags));
        if (!shared_columns || !shared_flags)
            goto prepare_fail;
        for (uint32_t c = 0; c < dst->ncols; c++) {
            shared_columns[c] = src->columns ? src->columns[c] : NULL;
            shared_flags[c] = true;
        }
    }
    if (src->column_types && src->ncols > 0) {
        shared_types = (wirelog_column_type_t *)malloc(
            (size_t)src->ncols * sizeof(*shared_types));
        if (!shared_types)
            goto prepare_fail;
        memcpy(shared_types, src->column_types,
            (size_t)src->ncols * sizeof(*shared_types));
    }
    if (src->ncols > 0) {
        shared_names = (char **)calloc(src->ncols, sizeof(*shared_names));
        if (!shared_names)
            goto prepare_fail;
        for (uint32_t c = 0; c < src->ncols; c++) {
            const char *name = src->col_names && src->col_names[c]
                ? src->col_names[c] : "";
            shared_names[c] = wl_strdup(name);
            if (!shared_names[c])
                goto prepare_fail;
        }
    }
    if (src->timestamps && src->capacity > 0) {
        shared_timestamps = (col_delta_timestamp_t *)malloc(
            (size_t)src->capacity * sizeof(*shared_timestamps));
        if (!shared_timestamps)
            goto prepare_fail;
        memcpy(shared_timestamps, src->timestamps,
            (size_t)src->capacity * sizeof(*shared_timestamps));
    } else if (dst->timestamps && src->capacity > 0) {
        /* The destination enabled timestamp tracking before it became a
        * view (Issue #1434: the retained-EDB admission tests rely on that
        * tracking surviving installation and the later growth transition).
        * Keep it enabled with a zeroed array sized like the borrowed
        * columns instead of dropping it because the source has none. */
        shared_timestamps = (col_delta_timestamp_t *)calloc(
            (size_t)src->capacity, sizeof(*shared_timestamps));
        if (!shared_timestamps)
            goto prepare_fail;
    }
    /* Prepare a complete schema for every destination, including a relation
     * whose schema was never initialized.  The installed buffers and their
     * Arrow interpretation must be published as one transaction. */
    ArrowSchemaInit(&prepared_schema);
    prepared_schema_ok = true;
    if (ArrowSchemaSetTypeStruct(&prepared_schema,
        (int64_t)dst->ncols) != NANOARROW_OK) {
        prepare_rc = EINVAL;
        goto prepare_fail;
    }
    for (uint32_t c = 0; c < dst->ncols; c++) {
        enum ArrowType arrow_type = shared_types
            && shared_types[c] == WIRELOG_TYPE_FLOAT
            ? NANOARROW_TYPE_DOUBLE : NANOARROW_TYPE_INT64;
        if (ArrowSchemaInitFromType(prepared_schema.children[c],
            arrow_type) != NANOARROW_OK) {
            prepare_rc = EINVAL;
            goto prepare_fail;
        }
        ArrowSchemaSetName(prepared_schema.children[c],
            shared_names ? shared_names[c] : "");
    }
    {
        int64_t **old_columns = dst->columns;
        bool *old_shared = dst->col_shared;
        bool old_arena_owned = dst->arena_owned;
        uint64_t ledger_before = col_rel_owned_ledger_bytes(dst);

        /* Ownership metadata is committed with the borrowed columns. Both
        * counters were validated before any destination state changed. */
        if (old_owner != dst)
            old_owner->storage_alias_borrows--;
        source_owner->storage_alias_borrows++;

        /* Commit point: all allocations and schema preparation succeeded. */
        if (!old_arena_owned && old_columns) {
            for (uint32_t c = 0; c < dst->ncols; c++)
                if ((!old_shared || !old_shared[c]) && old_columns[c])
                    free(old_columns[c]);
        }
        free((void *)old_columns);
        free(old_shared);
        free(dst->column_types);
        free(dst->timestamps);
        free(dst->compound_arity_map);
        if (dst->col_names) {
            for (uint32_t c = 0; c < dst->ncols; c++)
                free(dst->col_names[c]);
            free((void *)dst->col_names);
        }
        col_columns_free(dst->merge_columns, dst->ncols);
        dst->merge_columns = NULL;
        dst->merge_buf_cap = 0;
        col_columns_free(dst->retract_backup_columns, dst->ncols);
        dst->retract_backup_columns = NULL;
        dst->retract_backup_nrows = 0;
        dst->retract_backup_capacity = 0;
        dst->retract_backup_sorted_nrows = 0;
        dst->retract_backup_run_count = 0;
        memset(dst->retract_backup_run_ends, 0,
            sizeof(dst->retract_backup_run_ends));
        free(dst->dedup_slots);
        dst->dedup_slots = NULL;
        dst->dedup_cap = 0;
        dst->dedup_count = 0;
        free(dst->row_scratch);
        dst->row_scratch = NULL;
        if (dst->schema_ok)
            ArrowSchemaRelease(&dst->schema);
        dst->columns = shared_columns;
        dst->col_shared = shared_flags;
        dst->column_types = shared_types;
        dst->timestamps = shared_timestamps;
        dst->has_graph_column = shared_has_graph_column;
        dst->graph_col_idx = shared_graph_col_idx;
        dst->declared_ncols = shared_declared_ncols;
        dst->compound_kind = shared_compound_kind;
        dst->compound_count = shared_compound_count;
        dst->compound_arity_map = shared_arity_map;
        dst->inline_physical_offset = shared_inline_physical_offset;
        dst->col_names = shared_names;
        dst->arena_owned = false;
        dst->nrows = src->nrows;
        dst->capacity = src->capacity;
        dst->sorted_nrows = src->sorted_nrows;
        dst->base_nrows = src->base_nrows;
        dst->run_count = src->run_count;
        memcpy(dst->run_ends, src->run_ends, sizeof(dst->run_ends));
        dst->schema = prepared_schema;
        dst->schema_ok = prepared_schema_ok;
        dst->storage_owner = source_owner;
        dst->storage_owner_identity = source_owner->relation_identity;
        dst->storage_owner_generation = source_owner->storage_generation;
        dst->storage_alias_borrows = 0;
        dst->view_generation++;
        dst->storage_generation++;
        col_rel_ledger_reconcile(dst, ledger_before);
    }
    shared_columns = NULL;
    shared_flags = NULL;
    shared_types = NULL;
    shared_timestamps = NULL;
    shared_names = NULL;
    shared_arity_map = NULL;
    return 0;

prepare_fail:
    if (prepared_schema_ok)
        ArrowSchemaRelease(&prepared_schema);
    free((void *)shared_columns);
    free(shared_flags);
    free(shared_types);
    free(shared_timestamps);
    free(shared_arity_map);
    if (shared_names) {
        for (uint32_t c = 0; c < src->ncols; c++)
            free(shared_names[c]);
        free((void *)shared_names);
    }
    return prepare_rc;
}

int
col_rel_install_shared_view(col_rel_t *dst, const col_rel_t *src)
{
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_t *owner = NULL;
    int rc;

    if (!dst || !src)
        return EINVAL;
    rc = col_rel_storage_owner_resolve(src, &owner);
    if (rc != 0)
        return rc;
    rc = col_rel_source_reader_acquire(owner, &reader);
    if (rc != 0)
        return rc;
    rc = col_rel_install_shared_view_unprotected(dst, src);
    if (col_rel_source_reader_release(&reader) != 0 && rc == 0)
        rc = EINVAL;
    return rc;
}

/* ---- column name lookup ------------------------------------------------- */

int
col_rel_col_idx(const col_rel_t *r, const char *name)
{
    if (!r->col_names || !name)
        return -1;
    for (uint32_t i = 0; i < r->ncols; i++) {
        if (r->col_names[i] && strcmp(r->col_names[i], name) == 0)
            return (int)i;
    }
    /* fallback: "col<N>" convention */
    if (name[0] == 'c' && name[1] == 'o' && name[2] == 'l') {
        char *end;
        long v = strtol(name + 3, &end, 10);
        if (*end == '\0' && v >= 0 && (uint32_t)v < r->ncols)
            return (int)v;
    }
    return -1;
}

/* ---- convenience constructors ------------------------------------------- */

/*
 * col_rel_clone_compound_meta:
 * Replicate src's compound metadata onto dst. Mirrors the logical-column
 * walk used by col_rel_new_like / col_rel_pool_new_like (Issue #534): walk
 * src->compound_arity_map summing widths until physical ncols are covered,
 * then deep-copy that many entries so dst and src destroy paths stay
 * independent (K-Fusion isolation, Issue #553).
 *
 * Src must satisfy: src->compound_kind != WIRELOG_COMPOUND_KIND_NONE,
 * src->compound_arity_map != NULL, src->ncols > 0. Callers gate on these.
 *
 * Returns 0 on success (compound metadata installed on dst), -1 on
 * width-inconsistency or allocation failure (dst's compound metadata is
 * left untouched, matching the existing graceful-degrade behaviour).
 */
static int
col_rel_clone_compound_meta(col_rel_t *dst, const col_rel_t *src)
{
    /* compound_arity_map is keyed by LOGICAL column index, but we record
     * at least as many entries as the physical ncols to match the
     * source's allocation footprint. Walk the source map summing widths
     * until we cover ncols physical slots, then copy that many entries.
     * Bail gracefully on any inconsistency. */
    uint32_t logical_count = 0u;
    uint32_t acc = 0u;
    while (acc < src->ncols) {
        if (src->compound_arity_map[logical_count] == 0u) {
            /* Corrupt width -- leave compound metadata cleared rather
             * than propagating the inconsistency. */
            return -1;
        }
        acc += src->compound_arity_map[logical_count];
        logical_count++;
    }
    if (logical_count == 0u || acc != src->ncols)
        return -1;

    uint32_t *copy = (uint32_t *)malloc(
        (size_t)logical_count * sizeof(uint32_t));
    if (!copy)
        return -1;
    memcpy(copy, src->compound_arity_map,
        (size_t)logical_count * sizeof(uint32_t));
    dst->compound_arity_map = copy;
    dst->compound_kind = src->compound_kind;
    dst->compound_count = src->compound_count;
    dst->inline_physical_offset = src->inline_physical_offset;
    return 0;
}

/* Helper: create a new owned relation with given ncols and auto-named cols. */
col_rel_t *
col_rel_new_auto(const char *name, uint32_t ncols)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    if (col_rel_set_schema(r, ncols, NULL) != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    return r;
}

/* Helper: create owned relation copying col_names from src.
 *
 * Issue #1140: a NULL src is rejected rather than dereferenced.  Several
 * callers obtain their template from a lookup that can miss and check only
 * the returned relation, so failing here is what makes that check
 * sufficient. */
col_rel_t *
col_rel_new_like(const char *name, const col_rel_t *src)
{
    if (!src)
        return NULL;
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    if (src->column_types && src->ncols > 0) {
        r->column_types = (wirelog_column_type_t *)malloc(
            (size_t)src->ncols * sizeof(*r->column_types));
        if (!r->column_types) {
            col_rel_destroy(r);
            return NULL;
        }
        memcpy(r->column_types, src->column_types,
            (size_t)src->ncols * sizeof(*r->column_types));
    }
    if (col_rel_set_schema(r, src->ncols, (const char *const *)src->col_names)
        != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    /* Issue #535: inherit graph-column metadata so deltas/clones route by
     * __graph_id consistently with their source relation. */
    r->has_graph_column = src->has_graph_column;
    r->graph_col_idx = src->graph_col_idx;
    /* Issue #534 Task #1: inherit compound metadata so FILTER/PROJECT/LFTJ
     * outputs remain INLINE-kind and the logical<->physical translation
     * stays valid for downstream ops. compound_arity_map is a separately
     * owned heap array; we deep-copy it so dst and src destroy paths stay
     * independent (K-Fusion isolation). Failure is non-fatal: the helper
     * leaves dst's compound metadata cleared (NONE-kind), matching the
     * pre-#553 graceful-degrade contract. */
    if (src->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        && src->compound_arity_map && src->ncols > 0u) {
        (void)col_rel_clone_compound_meta(r, src);
    }
    return r;
}

/* Pool-aware col_rel constructor wrappers.
 *
 * The struct slot is allocated from the pool slab (O(1), no free needed).
 * Data buffers and col_names are still heap-allocated so that realloc in
 * col_rel_append_row remains safe and col_names are available for column
 * lookup (col_rel_col_idx).  The pool_owned flag tells col_rel_destroy to
 * skip free() on the struct itself while still freeing heap-allocated
 * internals. */
static col_rel_t *
col_rel_pool_fallback_like(delta_pool_t *pool, col_rel_t *r,
    const char *name, const col_rel_t *like)
{
    col_rel_free_contents(r);
    if (pool->slot_used > 0)
        pool->slot_used--;
    return col_rel_new_like(name, like);
}

static col_rel_t *
col_rel_pool_fallback_auto(delta_pool_t *pool, col_rel_t *r,
    const char *name, uint32_t ncols)
{
    col_rel_free_contents(r);
    if (pool->slot_used > 0)
        pool->slot_used--;
    return col_rel_new_auto(name, ncols);
}

col_rel_t *
col_rel_pool_new_like(delta_pool_t *pool, const char *name,
    const col_rel_t *like)
{
    /* Issue #1140: reject before delta_pool_alloc_slot() so a rejected call
     * does not burn a pool slot. */
    if (!like)
        return NULL;
    if (!pool)
        return col_rel_new_like(name, like); /* Fallback to malloc */
    /* Reserve the identity before consuming a slab slot.  The allocator is
     * deliberately non-wrapping; exhaustion is a hard rejection and must
     * not make a subsequent valid pool allocation appear exhausted. */
    if (pool->slot_used < pool->slot_cap) {
        uint64_t identity = 0;
        if (col_rel_new_identity(&identity) != 0)
            return NULL;
        col_rel_t *r = (col_rel_t *)delta_pool_alloc_slot(pool);
        if (!r)
            return col_rel_new_like(name, like);
        r->pool_owned = true;
        r->relation_identity = identity;
        r->view_generation = 1u;
        r->storage_generation = 1u;
        col_rel_storage_owner_init(r);
        r->name = wl_strdup(name);
        if (!r->name) {
            return col_rel_pool_fallback_like(pool, r, name, like);
        }
        r->ncols = like->ncols;
        r->capacity = COL_REL_INIT_CAP;
        if (like->ncols > 0) {
            r->columns = col_columns_alloc(like->ncols, r->capacity);
            if (!r->columns) {
                return col_rel_pool_fallback_like(pool, r, name, like);
            }
        }
        /* Copy col_names so col_rel_col_idx works for downstream operators */
        if (like->col_names && like->ncols > 0) {
            r->col_names = (char **)calloc(like->ncols, sizeof(char *));
            if (!r->col_names)
                return col_rel_pool_fallback_like(pool, r, name, like);
            for (uint32_t i = 0; i < like->ncols; i++) {
                if (like->col_names[i]) {
                    r->col_names[i] = wl_strdup(like->col_names[i]);
                    if (!r->col_names[i])
                        return col_rel_pool_fallback_like(pool, r, name,
                                   like);
                }
            }
        }
        r->has_graph_column = like->has_graph_column;
        r->graph_col_idx = like->graph_col_idx;
        if (like->column_types
            && col_rel_set_column_types(r, like->column_types,
            like->ncols) != 0) {
            return col_rel_pool_fallback_like(pool, r, name, like);
        }
        if (like->compound_kind != WIRELOG_COMPOUND_KIND_NONE
            && like->compound_arity_map && like->ncols > 0u)
            (void)col_rel_clone_compound_meta(r, like);
        r->nrows = 0;
        return r;
    }
    /* Pool exhausted: retain the historical heap fallback. */
    return col_rel_new_like(name, like);
}

col_rel_t *
col_rel_pool_new_auto(delta_pool_t *pool, wl_arena_t *arena,
    const char *name, uint32_t ncols)
{
    if (!pool)
        return col_rel_new_auto(name, ncols); /* Fallback */
    if (pool->slot_used < pool->slot_cap) {
        uint64_t identity = 0;
        if (col_rel_new_identity(&identity) != 0)
            return NULL;
        col_rel_t *r = (col_rel_t *)delta_pool_alloc_slot(pool);
        if (!r)
            return col_rel_new_auto(name, ncols);
        r->pool_owned = true;
        r->relation_identity = identity;
        r->view_generation = 1u;
        r->storage_generation = 1u;
        col_rel_storage_owner_init(r);
        r->name = wl_strdup(name);
        if (!r->name) {
            return col_rel_pool_fallback_auto(pool, r, name, ncols);
        }
        r->ncols = ncols;
        r->capacity = COL_REL_INIT_CAP;
        if (ncols > 0) {
            bool use_arena = false;
            if (arena) {
                r->columns = (int64_t **)calloc(ncols,
                        sizeof(int64_t *));
                if (r->columns) {
                    bool ok = true;
                    for (uint32_t c = 0; c < ncols; c++) {
                        r->columns[c] = (int64_t *)wl_arena_alloc(arena,
                                (size_t)r->capacity * sizeof(int64_t));
                        if (!r->columns[c]) {
                            ok = false;
                            break;
                        }
                    }
                    if (ok) {
                        use_arena = true;
                        r->arena_owned = true;
                    } else {
                        free((void *)r->columns);
                        r->columns = NULL;
                    }
                }
            }
            if (!use_arena) {
                r->columns = col_columns_alloc(ncols, r->capacity);
                r->arena_owned = false;
            }
            if (!r->columns) {
                return col_rel_pool_fallback_auto(pool, r, name, ncols);
            }
            r->col_names = (char **)calloc(ncols, sizeof(char *));
            if (!r->col_names)
                return col_rel_pool_fallback_auto(pool, r, name, ncols);
            for (uint32_t i = 0; i < ncols; i++) {
                char buf[32];
                snprintf(buf, sizeof(buf), "col%u", i);
                r->col_names[i] = wl_strdup(buf);
                if (!r->col_names[i])
                    return col_rel_pool_fallback_auto(pool, r, name, ncols);
            }
        }
        r->nrows = 0;
        return r;
    }
    return col_rel_new_auto(name, ncols);
}

/* ---- deep copy ----------------------------------------------------------- */

/**
 * @brief Produce a fully-owned deep copy of a col_rel_t (Issue #553).
 *
 * Allocates a new heap-resident relation and clones every owned buffer
 * and metadata field so the result is byte-equivalent to @p src yet
 * shares no storage with it.  Mutating either side after the call (data
 * appends, schema upgrades, retraction, consolidation) leaves the other
 * untouched.  This is the K-Fusion isolation primitive: callers that
 * need to fork a relation across worker boundaries should use this in
 * preference to col_rel_new_like + replay.
 *
 * Field-group taxonomy (per #553 body):
 *   - Groups A/B: columns, col_names, ArrowSchema -- deep copied.  The
 *     ArrowSchema is rebuilt via the same manual reinit used by
 *     col_rel_set_schema (ArrowSchemaInit + ArrowSchemaSetTypeStruct +
 *     per-child ArrowSchemaInitFromType + ArrowSchemaSetName).  The
 *     schema struct is NEVER bit-copied because the release callback /
 *     private_data would alias and trigger a double-release on destroy.
 *   - Group C: timestamps array sized to capacity (not nrows).
 *   - Group D: persistent merge buffer sized to merge_buf_cap.
 *   - Group E: retraction backup defensively deep-copied if populated.
 *   - Group F: run_count + run_ends fixed array memcpy'd.
 *   - Group G: compound_arity_map cloned via col_rel_clone_compound_meta;
 *     corrupt arity maps degrade dst to NONE-kind (matches new_like).
 *   - Group H: has_graph_column, graph_col_idx direct copy.
 *   - Group I: pool_owned, arena_owned, mem_ledger always reset.
 *   - Group J: dedup_*, col_shared, row_scratch always reset; any cache
 *     state will be rebuilt lazily on demand.
 *
 * Memory-ledger policy (Issue #554, design rule R-1):
 *   The deep copy is treated as a TRANSIENT relation.  dst->mem_ledger
 *   is unconditionally set to NULL regardless of whether src is wired
 *   up to a ledger.  Rationale: a deep copy is a short-lived workspace
 *   relation (K-Fusion fork point, retraction backup, debug snapshot)
 *   whose buffer growth and free events must NOT charge against the
 *   originating session's RELATION subsystem budget.  If a deep copy
 *   is later promoted into a long-lived role, the caller may wire up
 *   dst->mem_ledger explicitly post-copy and the ledger will pick up
 *   subsequent realloc/free events from there on.  Source ledger
 *   counters are likewise never touched by this function.
 *
 * @param src    Relation to clone.  Must be non-NULL.  May carry inline
 *               compound metadata, sharing flags, retraction backup,
 *               etc.; the function reads through src->columns even when
 *               col_shared[c] is true (borrowed-readable contract).
 * @param out    Out-parameter for the heap-allocated copy.  Must be
 *               non-NULL.  On any failure *out is left as NULL.
 * @param arena  Reserved.  Currently ignored -- the deep copy always
 *               heap-owns its buffers so subsequent col_rel_destroy on
 *               the result is correct without an arena reference.
 *               Accepted to match the FROZEN signature in the issue
 *               body and reserve room for an arena-aware variant.
 *
 * @retval 0       Success; @p *out points at the newly allocated copy.
 * @retval EINVAL  src or out was NULL.
 * @retval ENOMEM  Allocation of dst, name, columns, col_names, schema,
 *                 timestamps, merge buffer, or retraction backup
 *                 failed.  Partial state is unwound via
 *                 col_rel_free_contents + free(dst); calloc-zero-init
 *                 makes that safe at any unwind point.
 *
 * @note The result inherits no pool/arena/ledger affiliation from
 *       @p src.  Callers that need ledger accounting on the copy must
 *       wire up dst->mem_ledger themselves.
 */
int
col_rel_deep_copy(const col_rel_t *src, col_rel_t **out, wl_arena_t *arena)
{
    (void)arena; /* reserved; see contract block in internal.h */
    if (!src || !out)
        return EINVAL;
    *out = NULL;

    col_rel_t *dst = (col_rel_t *)calloc(1, sizeof(*dst));
    if (!dst)
        return ENOMEM;

    /* Name: deep-copy the optional null-terminated owned string. */
    if (src->name) {
        dst->name = wl_strdup(src->name);
        if (!dst->name) {
            col_rel_free_contents(dst);
            free(dst);
            return ENOMEM;
        }
    }

    /* Scalar bookkeeping (Groups A scalars + D scalars). */
    if (col_rel_new_identity(&dst->relation_identity) != 0) {
        col_rel_free_contents(dst);
        free(dst);
        return EOVERFLOW;
    }
    dst->view_generation = src->view_generation;
    dst->storage_generation = src->storage_generation;
    col_rel_storage_owner_init(dst);
    dst->ncols = src->ncols;
    dst->nrows = src->nrows;
    dst->capacity = src->capacity;
    dst->sorted_nrows = src->sorted_nrows;
    dst->base_nrows = src->base_nrows;

    /* Group H: graph-column metadata (Issue #535). */
    dst->has_graph_column = src->has_graph_column;
    dst->graph_col_idx = src->graph_col_idx;

    /*
     * Group I (pool_owned, arena_owned, mem_ledger): always reset.  The
     * copy is heap-allocated and never participates in pool/arena/ledger
     * accounting -- callers that need those must wire them up explicitly.
     *
     * mem_ledger policy (Issue #554, R-1):
     *   The deep copy is a TRANSIENT relation: dst->mem_ledger stays NULL
     *   so its buffer realloc/free events never charge the source
     *   session's RELATION subsystem budget.  Source ledger counters are
     *   never touched by this function (we only read src's columns and
     *   metadata; no allocations are reported to src->mem_ledger).
     *
     * Group J (dedup_slots/cap/count, col_shared, row_scratch): always
     * reset.  The dedup hash table and row scratch are caches that will
     * be rebuilt lazily; col_shared is unconditionally cleared because
     * deep copies own all of their columns.
     *
     * calloc above already zeroed every byte, so no explicit assignment
     * is required here -- the comment is the contract.
     */

    /*
     * Group A: column buffers.  Allocate a private ncols x capacity grid
     * via col_columns_alloc, then memcpy the live nrows of each column.
     * Reading through src->columns[c] is correct even when col_shared[c]
     * is true (the buffer is borrowed-readable); the copy unconditionally
     * owns its storage so col_shared stays NULL on dst (Group J).
     */
    if (src->ncols > 0u && src->capacity > 0u) {
        dst->columns = col_columns_alloc(src->ncols, src->capacity);
        if (!dst->columns) {
            col_rel_free_contents(dst);
            free(dst);
            return ENOMEM;
        }
        if (src->columns) {
            for (uint32_t c = 0; c < src->ncols; c++) {
                if (src->columns[c] && src->nrows > 0u) {
                    memcpy(dst->columns[c], src->columns[c],
                        (size_t)src->nrows * sizeof(int64_t));
                }
            }
        }
    }

    /*
     * Group B: col_names array + ArrowSchema (manual reinit).
     *
     * Schema is rebuilt the same way col_rel_set_schema does it
     * (ArrowSchemaInit + ArrowSchemaSetTypeStruct + per-child
     * ArrowSchemaInitFromType + ArrowSchemaSetName) rather than
     * memcpy'd; the ArrowSchema release callback would otherwise
     * alias between src and dst, leading to a double-release.
     */
    if (src->ncols > 0u && src->col_names) {
        dst->col_names = (char **)calloc(src->ncols, sizeof(char *));
        if (!dst->col_names) {
            col_rel_free_contents(dst);
            free(dst);
            return ENOMEM;
        }
        for (uint32_t i = 0; i < src->ncols; i++) {
            if (src->col_names[i]) {
                dst->col_names[i] = wl_strdup(src->col_names[i]);
                if (!dst->col_names[i]) {
                    col_rel_free_contents(dst);
                    free(dst);
                    return ENOMEM;
                }
            }
        }
    }
    if (src->column_types && src->ncols > 0u) {
        dst->column_types = (wirelog_column_type_t *)malloc(
            (size_t)src->ncols * sizeof(*dst->column_types));
        if (!dst->column_types) {
            col_rel_free_contents(dst);
            free(dst);
            return ENOMEM;
        }
        memcpy(dst->column_types, src->column_types,
            (size_t)src->ncols * sizeof(*dst->column_types));
    }

    /*
     * Group D: persistent merge buffer.  When src has a merge buffer
     * sized to merge_buf_cap rows, allocate the same shape for dst and
     * memcpy merge_buf_cap entries per column.  The buffer is reused
     * across consolidation iterations, so any in-flight content is
     * preserved.  merge_buf_cap is copied unconditionally; sorted_nrows
     * and base_nrows were already taken in the scalar bookkeeping pass.
     */
    dst->merge_buf_cap = src->merge_buf_cap;
    if (src->merge_columns && src->ncols > 0u && src->merge_buf_cap > 0u) {
        dst->merge_columns = col_columns_alloc(src->ncols, src->merge_buf_cap);
        if (!dst->merge_columns) {
            col_rel_free_contents(dst);
            free(dst);
            return ENOMEM;
        }
        for (uint32_t c = 0; c < src->ncols; c++) {
            if (src->merge_columns[c]) {
                memcpy(dst->merge_columns[c], src->merge_columns[c],
                    (size_t)src->merge_buf_cap * sizeof(int64_t));
            }
        }
    }

    /*
     * Group E: zero-copy retraction backup (Issue #300, #369).  Outside
     * a retraction step every retract_backup_* field is NULL/0, so this
     * branch is normally inert -- but if a deep copy ever lands inside
     * a retraction window, the copy must own its own backup or any
     * subsequent restore would corrupt src's columns.  We therefore
     * defensively allocate retract_backup_capacity rows and memcpy
     * retract_backup_nrows live entries per column, plus mirror the run
     * tracking scalars + fixed array.
     */
    dst->retract_backup_nrows = src->retract_backup_nrows;
    dst->retract_backup_capacity = src->retract_backup_capacity;
    dst->retract_backup_sorted_nrows = src->retract_backup_sorted_nrows;
    dst->retract_backup_run_count = src->retract_backup_run_count;
    memcpy(dst->retract_backup_run_ends, src->retract_backup_run_ends,
        sizeof(src->retract_backup_run_ends));
    if (src->retract_backup_columns && src->ncols > 0u
        && src->retract_backup_capacity > 0u) {
        dst->retract_backup_columns = col_columns_alloc(src->ncols,
                src->retract_backup_capacity);
        if (!dst->retract_backup_columns) {
            col_rel_free_contents(dst);
            free(dst);
            return ENOMEM;
        }
        for (uint32_t c = 0; c < src->ncols; c++) {
            if (src->retract_backup_columns[c]
                && src->retract_backup_nrows > 0u) {
                memcpy(dst->retract_backup_columns[c],
                    src->retract_backup_columns[c],
                    (size_t)src->retract_backup_nrows * sizeof(int64_t));
            }
        }
    }

    /*
     * Group F: tiered run-tracking metadata (Issue #369).  Both fields
     * are fixed-size POD: a scalar count plus a COL_MAX_RUNS-element
     * array of run end offsets.  Direct copy (the retract-backup mirror
     * lands in Group E).
     */
    dst->run_count = src->run_count;
    memcpy(dst->run_ends, src->run_ends, sizeof(src->run_ends));

    /*
     * Group C: timestamps array.  Sized to capacity (not nrows) so that
     * append_row paths after the copy do not need to grow the timestamps
     * buffer in lockstep with the columns buffer.  Skipped when src has
     * timestamp tracking disabled (timestamps == NULL).
     */
    if (src->timestamps && src->capacity > 0u) {
        dst->timestamps = (col_delta_timestamp_t *)malloc(
            (size_t)src->capacity * sizeof(col_delta_timestamp_t));
        if (!dst->timestamps) {
            col_rel_free_contents(dst);
            free(dst);
            return ENOMEM;
        }
        memcpy(dst->timestamps, src->timestamps,
            (size_t)src->capacity * sizeof(col_delta_timestamp_t));
    }

    /*
     * Group G: compound-column metadata (Issue #534, #553).  Mirror the
     * col_rel_new_like / col_rel_pool_new_like clone path: when the
     * source carries an INLINE-tier arity map, deep-copy it through the
     * shared col_rel_clone_compound_meta helper.  Helper failure is
     * handled with the existing graceful-degrade contract (compound
     * metadata cleared to NONE-kind on dst), keeping behavior aligned
     * with col_rel_new_like.  Sources with WIRELOG_COMPOUND_KIND_NONE
     * leave dst at the zero default already established by calloc.
     */
    if (src->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        && src->compound_arity_map && src->ncols > 0u) {
        (void)col_rel_clone_compound_meta(dst, src);
    }

    if (src->schema_ok) {
        ArrowSchemaInit(&dst->schema);
        if (ArrowSchemaSetTypeStruct(&dst->schema, (int64_t)src->ncols)
            != NANOARROW_OK) {
            ArrowSchemaRelease(&dst->schema);
            col_rel_free_contents(dst);
            free(dst);
            return ENOMEM;
        }
        for (uint32_t i = 0; i < src->ncols; i++) {
            enum ArrowType arrow_type = src->column_types
                && src->column_types[i] == WIRELOG_TYPE_FLOAT
                ? NANOARROW_TYPE_DOUBLE : NANOARROW_TYPE_INT64;
            if (ArrowSchemaInitFromType(dst->schema.children[i],
                arrow_type)
                != NANOARROW_OK) {
                ArrowSchemaRelease(&dst->schema);
                col_rel_free_contents(dst);
                free(dst);
                return ENOMEM;
            }
            const char *cname = (dst->col_names && dst->col_names[i])
                ? dst->col_names[i] : "";
            ArrowSchemaSetName(dst->schema.children[i], cname);
        }
        dst->schema_ok = true;
    }

    *out = dst;
    return 0;
}

/* ---- radix sort by single key column ----------------------------------- */

/*
 * col_radix_sort_rows_by_key: stable LSD radix sort of a row-major
 * int64_t buffer by a single key column.  Used by arrangement.c
 * (sarr_build) and lftj.c (lftj_iter_init) to replace the
 * platform-specific qsort_r path -- those call sites only need a sort
 * by one column, which radix can do in eight passes over the key's
 * bytes without any comparator callback.  See #465 for the Android
 * portability driver that motivated removing qsort_r.
 *
 * Allocates one tmp row-major scratch buffer.  Returns 0 on success,
 * -1 if scratch allocation fails (the input is then unmodified).
 */
WL_COLUMNAR_RELATION_NOINLINE int
wl_columnar_relation_radix_sort_rows_by_key_typed(int64_t *data,
    uint32_t nrows,
    uint32_t ncols, uint32_t key_col, wirelog_column_type_t key_type)
{
    if (!data || ncols == 0 || nrows <= 1 || key_col >= ncols)
        return 0;

    size_t row_bytes = (size_t)ncols * sizeof(int64_t);
    int64_t *tmp = (int64_t *)malloc((size_t)nrows * row_bytes);
    if (!tmp)
        return -1;

    int64_t *src = data;
    int64_t *dst = tmp;
    uint32_t count[256];
    uint32_t prefix[256];

    /*
     * LSD radix over the 8 bytes of the int64_t key column.  Byte 0
     * (LSB) is processed first; byte 7 (MSB) last.  Integer keys flip
     * the sign bit so unsigned bucket order matches signed int64 order;
     * float keys use the monotonic IEEE-754 transform below.  Stable:
     * rows with equal key keep their relative order.
     */
    for (int b = 0; b < 8; b++) {
        int shift = b * 8;
        int is_sign_byte = (b == 7);

        memset(count, 0, sizeof(count));
        for (uint32_t row = 0; row < nrows; row++) {
            uint64_t raw = (uint64_t)src[(size_t)row * ncols + key_col];
            uint64_t ordered = key_type == WIRELOG_TYPE_FLOAT
                ? ((raw & UINT64_C(0x8000000000000000))
                        ? ~raw : raw ^ UINT64_C(0x8000000000000000))
                : raw;
            uint8_t bv = (uint8_t)(ordered >> shift);
            if (is_sign_byte && key_type != WIRELOG_TYPE_FLOAT)
                bv ^= 0x80u;
            count[bv]++;
        }

        prefix[0] = 0;
        for (int i = 1; i < 256; i++)
            prefix[i] = prefix[i - 1] + count[i - 1];

        for (uint32_t row = 0; row < nrows; row++) {
            uint64_t raw = (uint64_t)src[(size_t)row * ncols + key_col];
            uint64_t ordered = key_type == WIRELOG_TYPE_FLOAT
                ? ((raw & UINT64_C(0x8000000000000000))
                        ? ~raw : raw ^ UINT64_C(0x8000000000000000))
                : raw;
            uint8_t bv = (uint8_t)(ordered >> shift);
            if (is_sign_byte && key_type != WIRELOG_TYPE_FLOAT)
                bv ^= 0x80u;
            uint32_t out_pos = prefix[bv]++;
            memcpy(dst + (size_t)out_pos * ncols,
                src + (size_t)row * ncols,
                row_bytes);
        }

        int64_t *t = src;
        src = dst;
        dst = t;
    }

    /* After 8 passes the sorted output is in `src`.  Copy back if the
     * final pass ended in tmp. */
    if (src != data)
        memcpy(data, src, (size_t)nrows * row_bytes);

    free(tmp);
    return 0;
}

int
col_radix_sort_rows_by_key(int64_t *data, uint32_t nrows, uint32_t ncols,
    uint32_t key_col)
{
    return wl_columnar_relation_radix_sort_rows_by_key_typed(data, nrows,
               ncols, key_col, WIRELOG_TYPE_INT64);
}

/*
 * col_rel_insertion_sort: insertion sort for small segments (Issue #343).
 *
 * Sort sub-range [start_row, start_row + nrows) of r in-place.
 * O(n^2) but low constant overhead — faster than radix sort for small N.
 */
static int
col_rel_insertion_sort(col_rel_t *r, uint32_t start_row, uint32_t nrows)
{
    uint32_t nc = r->ncols;
    size_t row_count = (size_t)nrows + 1u;
    if (nc != 0 && row_count > SIZE_MAX
        / ((size_t)nc * sizeof(int64_t)))
        return EOVERFLOW;
    int64_t *work = (int64_t *)malloc(
        row_count * nc * sizeof(*work));
    if (!work)
        return ENOMEM;
    for (uint32_t i = 0; i < nrows; i++)
        col_rel_row_copy_out(r, start_row + i, work + (size_t)i * nc);

    for (uint32_t i = 1; i < nrows; i++) {
        int64_t *tbuf = work + (size_t)nrows * nc;
        memcpy(tbuf, work + (size_t)i * nc,
            (size_t)nc * sizeof(*work));
        uint32_t j = i;
        while (j > 0) {
            /* Compare r[start_row + j - 1] against saved key
             * in tbuf (not the relation -- row i is overwritten
             * after the first shift). */
            int cmp = 0;
            for (uint32_t c = 0; c < nc; c++) {
                int64_t va = work[(size_t)(j - 1) * nc + c];
                int64_t vb = tbuf[c];
                if (r->column_types
                    && r->column_types[c] == WIRELOG_TYPE_FLOAT) {
                    int fcmp = wl_columnar_float_compare_bits(va, vb);
                    if (fcmp != 0) {
                        cmp = fcmp;
                        break;
                    }
                    continue;
                }
                if (va > vb) {
                    cmp = 1;
                    break;
                }
                if (va < vb) {
                    cmp = -1;
                    break;
                }
            }
            if (cmp <= 0)
                break;
            memcpy(work + (size_t)j * nc,
                work + (size_t)(j - 1) * nc,
                (size_t)nc * sizeof(*work));
            j--;
        }
        memcpy(work + (size_t)j * nc, tbuf,
            (size_t)nc * sizeof(*work));
    }

    for (uint32_t c = 0; c < nc; c++)
        for (uint32_t i = 0; i < nrows; i++)
            r->columns[c][start_row + i] = work[(size_t)i * nc + c];
    free(work);
    return 0;
}

/* ======================================================================== */
/* Fused Uniform Check + Count Pass (Issue #363 Phase 1)                    */
/* ======================================================================== */

/*
 * radix_uniform_count_fused: single gather pass that combines the uniform
 * check and the count pass into one traversal over col_data.
 *
 * Returns true  if all nrows bytes are identical — caller skips the pass.
 * Returns false if not uniform; bv_cache[] and count[] are populated and
 *               ready for the prefix-sum / scatter steps.
 *
 * Benefit over calling radix_uniform_check_fast then radix_count_pass_fast:
 * non-skipped passes pay for only one set of gather loads instead of two.
 */

#ifdef __AVX2__
static bool
radix_uniform_count_fused_avx2(const int64_t *col_data, uint32_t start_row,
    const uint32_t *src, uint32_t nrows, int shift, int is_sign_byte,
    uint8_t first_bv, uint8_t *bv_cache, uint32_t *count)
{
    uint8_t xmask = is_sign_byte ? 0x80u : 0u;
    __m256i vshift = _mm256_set1_epi64x(shift);
    uint8_t uniform = 1; /* bitwise-AND accumulator; 0 once any mismatch seen */
    uint32_t i = 0;

    /* SIMD gather + byte extract + bv_cache write + uniformity tracking.
     * Prefetch col_data 16 elements (2 iterations) ahead to hide L3 gather
     * latency (Issue #363 Phase 2). */
    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);
        __m128i vidx0 = _mm_loadu_si128((const __m128i *)(src + i));
        __m128i vidx1 = _mm_loadu_si128((const __m128i *)(src + i + 4));
        __m256i vals0 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx0, 8);
        __m256i vals1 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx1, 8);
        __m256i sh0 = _mm256_srlv_epi64(vals0, vshift);
        __m256i sh1 = _mm256_srlv_epi64(vals1, vshift);

        bv_cache[i + 0] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                0) ^ xmask;
        bv_cache[i + 1] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                1) ^ xmask;
        bv_cache[i + 2] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                2) ^ xmask;
        bv_cache[i + 3] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                3) ^ xmask;
        bv_cache[i + 4] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                0) ^ xmask;
        bv_cache[i + 5] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                1) ^ xmask;
        bv_cache[i + 6] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                2) ^ xmask;
        bv_cache[i + 7] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                3) ^ xmask;

        /* Branchless bitwise accumulation — avoids misprediction in hot loop */
        uniform &= (bv_cache[i + 0] == first_bv) & (bv_cache[i + 1] ==
            first_bv) &
            (bv_cache[i + 2] == first_bv) & (bv_cache[i + 3] == first_bv) &
            (bv_cache[i + 4] == first_bv) & (bv_cache[i + 5] == first_bv) &
            (bv_cache[i + 6] == first_bv) & (bv_cache[i + 7] == first_bv);
    }

    /* Scalar tail */
    for (; i < nrows; i++) {
        uint8_t bv = (uint8_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_byte)
            bv ^= 0x80u;
        bv_cache[i] = bv;
        uniform &= (uint8_t)(bv == first_bv);
    }

    if (uniform)
        return true;

    /* Not uniform: build histogram from bv_cache (sequential read) */
    memset(count, 0, 256 * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
static bool
radix_uniform_count_fused_neon(const int64_t *col_data, uint32_t start_row,
    const uint32_t *src, uint32_t nrows, int shift, int is_sign_byte,
    uint8_t first_bv, uint8_t *bv_cache, uint32_t *count)
{
    uint8_t xmask = is_sign_byte ? 0x80u : 0u;
    int64x2_t vneg_shift = vdupq_n_s64(-(int64_t)shift);
    uint8x8_t vfirst = vdup_n_u8(first_bv);
    /* Bit-AND mask: stays UINT64_MAX while all compared bytes equal first_bv */
    uint64_t uniform_mask = UINT64_MAX;
    uint32_t i = 0;

    /* NEON gather + byte extract + bv_cache write + uniformity tracking.
     * Prefetch col_data 16 elements (2 iterations) ahead to hide L3 gather
     * latency (Issue #363 Phase 2). */
    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);
        int64_t v0 = col_data[start_row + src[i + 0]];
        int64_t v1 = col_data[start_row + src[i + 1]];
        int64_t v2 = col_data[start_row + src[i + 2]];
        int64_t v3 = col_data[start_row + src[i + 3]];
        int64_t v4 = col_data[start_row + src[i + 4]];
        int64_t v5 = col_data[start_row + src[i + 5]];
        int64_t v6 = col_data[start_row + src[i + 6]];
        int64_t v7 = col_data[start_row + src[i + 7]];

        int64x2_t vec01 = vcombine_s64(
            vcreate_s64((uint64_t)v0), vcreate_s64((uint64_t)v1));
        int64x2_t vec23 = vcombine_s64(
            vcreate_s64((uint64_t)v2), vcreate_s64((uint64_t)v3));
        int64x2_t vec45 = vcombine_s64(
            vcreate_s64((uint64_t)v4), vcreate_s64((uint64_t)v5));
        int64x2_t vec67 = vcombine_s64(
            vcreate_s64((uint64_t)v6), vcreate_s64((uint64_t)v7));

        uint64x2_t sv01 = vshlq_u64(vreinterpretq_u64_s64(vec01), vneg_shift);
        uint64x2_t sv23 = vshlq_u64(vreinterpretq_u64_s64(vec23), vneg_shift);
        uint64x2_t sv45 = vshlq_u64(vreinterpretq_u64_s64(vec45), vneg_shift);
        uint64x2_t sv67 = vshlq_u64(vreinterpretq_u64_s64(vec67), vneg_shift);

        uint8_t bvals[8] = {
            (uint8_t)vgetq_lane_u64(sv01, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv01, 1) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv23, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv23, 1) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv45, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv45, 1) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv67, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv67, 1) ^ xmask,
        };
        uint8x8_t bvec = vld1_u8(bvals);
        vst1_u8(bv_cache + i, bvec);

        /* SIMD compare: all-0xFF if every byte equals first_bv; AND into mask */
        uint8x8_t cmp = vceq_u8(bvec, vfirst);
        uniform_mask &= vget_lane_u64(vreinterpret_u64_u8(cmp), 0);
    }

    /* Scalar tail */
    bool uniform_tail = true;
    for (; i < nrows; i++) {
        uint8_t bv = (uint8_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_byte)
            bv ^= 0x80u;
        bv_cache[i] = bv;
        if (bv != first_bv)
            uniform_tail = false;
    }

    if (uniform_mask == UINT64_MAX && uniform_tail)
        return true;

    /* Not uniform: build histogram from bv_cache (sequential read) */
    memset(count, 0, 256 * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __ARM_NEON__ */

#if !defined(__AVX2__) && !defined(__ARM_NEON__)
/*
 * Scalar fallback: single loop over all elements — builds bv_cache[] and
 * count[] in one pass and returns true if all bytes are equal to first_bv.
 * Only compiled on non-SIMD targets to avoid unused-function warnings.
 */
static bool
radix_uniform_count_fused_scalar(const int64_t *col_data, uint32_t start_row,
    const uint32_t *src, uint32_t nrows, int shift, int is_sign_byte,
    uint8_t first_bv, uint8_t *bv_cache, uint32_t *count)
{
    bool uniform = true;

    memset(count, 0, 256 * sizeof(uint32_t));
    for (uint32_t i = 0; i < nrows; i++) {
        uint8_t bv = (uint8_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_byte)
            bv ^= 0x80u;
        bv_cache[i] = bv;
        count[bv]++;
        if (bv != first_bv)
            uniform = false;
    }
    return uniform;
}
#endif /* !__AVX2__ && !__ARM_NEON__ */

/* Compile-time dispatch: AVX2 > NEON > scalar */
#ifdef __AVX2__
#define radix_uniform_count_fused_fast radix_uniform_count_fused_avx2
#elif defined(__ARM_NEON__)
#define radix_uniform_count_fused_fast radix_uniform_count_fused_neon
#else
#define radix_uniform_count_fused_fast radix_uniform_count_fused_scalar
#endif

/* ======================================================================== */
/* Fused Uniform Check + Count Pass for k=16 (Issue #363 Phase 5c ext.)    */
/* ======================================================================== */

/*
 * k=16 variants: same two-pass strategy as the k=8 functions above, but
 * operating on uint16_t bucket values and a 65536-entry count[] histogram.
 *
 * Pass 1 (SIMD): gather int64_t, extract 16-bit bucket, write bv_cache[],
 *   accumulate uniformity check.  Return true immediately if uniform.
 * Pass 2 (scalar, only when not uniform): memset(count, 0, 256KB) + histogram.
 */

#ifdef __AVX2__
static bool
radix_uniform_count_fused_k16_avx2(const int64_t *col_data,
    uint32_t start_row, const uint32_t *src, uint32_t nrows, int shift,
    int is_sign_pass, uint16_t first_bv, uint16_t *bv_cache, uint32_t *count)
{
    uint16_t xmask = is_sign_pass ? 0x8000u : 0u;
    __m256i vshift = _mm256_set1_epi64x(shift);
    __m256i vmask16 = _mm256_set1_epi64x(0xFFFF);
    uint8_t uniform = 1;
    uint32_t i = 0;

    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);
        __m128i vidx0 = _mm_loadu_si128((const __m128i *)(src + i));
        __m128i vidx1 = _mm_loadu_si128((const __m128i *)(src + i + 4));
        __m256i vals0 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx0, 8);
        __m256i vals1 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx1, 8);
        __m256i sh0 = _mm256_and_si256(
            _mm256_srlv_epi64(vals0, vshift), vmask16);
        __m256i sh1 = _mm256_and_si256(
            _mm256_srlv_epi64(vals1, vshift), vmask16);

        uint16_t b0 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                0) ^ xmask;
        uint16_t b1 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                1) ^ xmask;
        uint16_t b2 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                2) ^ xmask;
        uint16_t b3 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                3) ^ xmask;
        uint16_t b4 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                0) ^ xmask;
        uint16_t b5 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                1) ^ xmask;
        uint16_t b6 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                2) ^ xmask;
        uint16_t b7 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                3) ^ xmask;

        bv_cache[i + 0] = b0;
        bv_cache[i + 1] = b1;
        bv_cache[i + 2] = b2;
        bv_cache[i + 3] = b3;
        bv_cache[i + 4] = b4;
        bv_cache[i + 5] = b5;
        bv_cache[i + 6] = b6;
        bv_cache[i + 7] = b7;

        uniform &= (b0 == first_bv) & (b1 == first_bv) &
            (b2 == first_bv) & (b3 == first_bv) &
            (b4 == first_bv) & (b5 == first_bv) &
            (b6 == first_bv) & (b7 == first_bv);
    }

    for (; i < nrows; i++) {
        uint16_t bv =
            (uint16_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_pass)
            bv ^= 0x8000u;
        bv_cache[i] = bv;
        uniform &= (uint8_t)(bv == first_bv);
    }

    if (uniform)
        return true;

    memset(count, 0, 65536u * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
static bool
radix_uniform_count_fused_k16_neon(const int64_t *col_data,
    uint32_t start_row, const uint32_t *src, uint32_t nrows, int shift,
    int is_sign_pass, uint16_t first_bv, uint16_t *bv_cache, uint32_t *count)
{
    uint16_t xmask = is_sign_pass ? 0x8000u : 0u;
    int64x2_t vneg_shift = vdupq_n_s64(-(int64_t)shift);
    uint16x8_t vfirst = vdupq_n_u16(first_bv);
    uint64_t uniform_mask = UINT64_MAX;
    uint32_t i = 0;

    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);

        int64_t v0 = col_data[start_row + src[i + 0]];
        int64_t v1 = col_data[start_row + src[i + 1]];
        int64_t v2 = col_data[start_row + src[i + 2]];
        int64_t v3 = col_data[start_row + src[i + 3]];
        int64_t v4 = col_data[start_row + src[i + 4]];
        int64_t v5 = col_data[start_row + src[i + 5]];
        int64_t v6 = col_data[start_row + src[i + 6]];
        int64_t v7 = col_data[start_row + src[i + 7]];

        int64x2_t vec01 = vcombine_s64(
            vcreate_s64((uint64_t)v0), vcreate_s64((uint64_t)v1));
        int64x2_t vec23 = vcombine_s64(
            vcreate_s64((uint64_t)v2), vcreate_s64((uint64_t)v3));
        int64x2_t vec45 = vcombine_s64(
            vcreate_s64((uint64_t)v4), vcreate_s64((uint64_t)v5));
        int64x2_t vec67 = vcombine_s64(
            vcreate_s64((uint64_t)v6), vcreate_s64((uint64_t)v7));

        uint64x2_t sv01 = vshlq_u64(
            vreinterpretq_u64_s64(vec01), vneg_shift);
        uint64x2_t sv23 = vshlq_u64(
            vreinterpretq_u64_s64(vec23), vneg_shift);
        uint64x2_t sv45 = vshlq_u64(
            vreinterpretq_u64_s64(vec45), vneg_shift);
        uint64x2_t sv67 = vshlq_u64(
            vreinterpretq_u64_s64(vec67), vneg_shift);

        uint16_t bvals[8] = {
            (uint16_t)vgetq_lane_u64(sv01, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv01, 1) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv23, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv23, 1) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv45, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv45, 1) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv67, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv67, 1) ^ xmask,
        };
        uint16x8_t bvec = vld1q_u16(bvals);
        vst1q_u16(bv_cache + i, bvec);

        uint16x8_t cmp = vceqq_u16(bvec, vfirst);
        uint64x2_t c64 = vreinterpretq_u64_u16(cmp);
        uniform_mask &= vgetq_lane_u64(c64, 0) & vgetq_lane_u64(c64, 1);
    }

    bool uniform_tail = true;
    for (; i < nrows; i++) {
        uint16_t bv =
            (uint16_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_pass)
            bv ^= 0x8000u;
        bv_cache[i] = bv;
        if (bv != first_bv)
            uniform_tail = false;
    }

    if (uniform_mask == UINT64_MAX && uniform_tail)
        return true;

    memset(count, 0, 65536u * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __ARM_NEON__ */

#if !defined(__AVX2__) && !defined(__ARM_NEON__)
static bool
radix_uniform_count_fused_k16_scalar(const int64_t *col_data,
    uint32_t start_row, const uint32_t *src, uint32_t nrows, int shift,
    int is_sign_pass, uint16_t first_bv, uint16_t *bv_cache, uint32_t *count)
{
    uint16_t uacc = 0;

    memset(count, 0, 65536u * sizeof(uint32_t));
    for (uint32_t i = 0; i < nrows; i++) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16u]);
        int64_t v = col_data[start_row + src[i]];
        uint16_t bv = (uint16_t)((uint64_t)v >> shift);
        if (is_sign_pass)
            bv ^= 0x8000u;
        bv_cache[i] = bv;
        uacc |= (uint16_t)(bv ^ first_bv);
        count[bv]++;
    }
    return uacc == 0;
}
#endif /* !__AVX2__ && !__ARM_NEON__ */

/* Compile-time dispatch for k=16: AVX2 > NEON > scalar */
#ifdef __AVX2__
#define radix_uniform_count_fused_k16_fast radix_uniform_count_fused_k16_avx2
#elif defined(__ARM_NEON__)
#define radix_uniform_count_fused_k16_fast radix_uniform_count_fused_k16_neon
#else
#define radix_uniform_count_fused_k16_fast radix_uniform_count_fused_k16_scalar
#endif

/*
 * radix_sort_k16: LSD radix sort using 16-bit radix (Issue #363 Phase 5b/5c).
 *
 * 4 passes × 65536 buckets.  Scalar fused uniform+count loop with
 * WL_PREFETCH_R; in-place prefix sum avoids a separate 256KB prefix[].
 * Called for nrows >= 50000 where fewer passes justify the larger histogram.
 */
static int
radix_sort_k16(col_rel_t *r, uint32_t start_row, uint32_t nrows)
{
    uint32_t nc = r->ncols;

    const uint32_t radix_bits = 16u;
    const uint32_t num_passes = 64u / radix_bits;  /* 4 for k=16 */
    const uint32_t hist_size = 1u << radix_bits;   /* 65536 for k=16 */

    uint32_t *perm_a = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint32_t *perm_b = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint16_t *bv_cache = (uint16_t *)malloc(nrows * sizeof(uint16_t));
    uint32_t *count = (uint32_t *)malloc(hist_size * sizeof(uint32_t));
    if (!perm_a || !perm_b || !bv_cache || !count) {
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        free(count);
        /* The transactional insertion fallback is intentionally limited to
         * small inputs.  Running O(n^2) insertion sort for a failed 50K-row
         * k16 allocation would turn an allocation failure into a timeout. */
        return nrows <= 32 ? col_rel_insertion_sort(r, start_row, nrows)
                           : ENOMEM;
    }

    for (uint32_t i = 0; i < nrows; i++)
        perm_a[i] = i;

    uint32_t *src = perm_a;
    uint32_t *dst = perm_b;

#ifdef WL_RADIX_BENCH
    uint64_t _tU = 0, _tS = 0, _tA = 0, _t0 = 0;
    uint32_t _nSk = 0, _nPs = 0;
#endif

    for (int c = (int)nc - 1; c >= 0; c--) {
        const int64_t *col_data = r->columns[c];
        for (uint32_t pass = 0; pass < num_passes; pass++) {
            int shift = (int)(pass * radix_bits);
            int is_sign_pass = (pass == num_passes - 1);

            uint16_t first_bv;
            {
                int64_t val = col_data[start_row + src[0]];
                first_bv = (uint16_t)((uint64_t)val >> shift);
                if (is_sign_pass)
                    first_bv ^= 0x8000u;
            }
#ifdef WL_RADIX_BENCH
            _t0 = now_ns();
#endif
            /* SIMD-dispatched fused uniform check + count pass for k=16
             * (Issue #363 Phase 5c extension). */
            if (radix_uniform_count_fused_k16_fast(col_data, start_row,
                src, nrows, shift, is_sign_pass, first_bv,
                bv_cache, count)) {
#ifdef WL_RADIX_BENCH
                _tU += now_ns() - _t0;
                _nSk++;
#endif
                continue;
            }
#ifdef WL_RADIX_BENCH
            _tU += now_ns() - _t0;
            _nPs++;
            _t0 = now_ns();
#endif
            {
                uint32_t running = 0;
                for (uint32_t i = 0; i < hist_size; i++) {
                    uint32_t cnt = count[i];
                    count[i] = running;
                    running += cnt;
                }
            }
            for (uint32_t i = 0; i < nrows; i++) {
                if (i + 16u < nrows)
                    WL_PREFETCH_W(dst + count[bv_cache[i + 16u]]);
                dst[count[bv_cache[i]]++] = src[i];
            }
            uint32_t *t = src;
            src = dst;
            dst = t;
#ifdef WL_RADIX_BENCH
            _tS += now_ns() - _t0;
#endif
        }
    }

#ifdef WL_RADIX_BENCH
    _t0 = now_ns();
#endif
    int64_t *temp_col = (int64_t *)malloc(nrows * sizeof(int64_t));
    if (!temp_col) {
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        free(count);
        return ENOMEM;
    }
    for (uint32_t c = 0; c < nc; c++) {
        int64_t *col = r->columns[c];
        for (uint32_t i = 0; i < nrows; i++) {
            if (i + 8u < nrows)
                WL_PREFETCH_R(col + start_row + src[i + 8u]);
            temp_col[i] = col[start_row + src[i]];
        }
        memcpy(col + start_row, temp_col, nrows * sizeof(int64_t));
    }
    free(temp_col);
    free(perm_a);
    free(perm_b);
    free(bv_cache);
    free(count);
#ifdef WL_RADIX_BENCH
    _tA = now_ns() - _t0;
    if (wl_columnar_relation_radix_bench_enabled()) {
        uint64_t _tot = _tU + _tS + _tA;
        fprintf(stderr,
            "[radix-bench k=16] nrows=%u nc=%u pass=%u skip=%u "
            "uniform_+_count=%.0f%% scatter=%.0f%% apply=%.0f%% "
            "total_ms=%.3f\n",
            nrows, nc, _nPs, _nSk,
            100.0 * (double)_tU / (double)(_tot + 1),
            100.0 * (double)_tS / (double)(_tot + 1),
            100.0 * (double)_tA / (double)(_tot + 1),
            (double)_tot * 1e-6);
    }
#endif
    return 0;
}

/*
 * col_rel_radix_sort: index-permutation LSD radix sort (Phase B, Issue #330).
 *
 * Sort sub-range [start_row, start_row + nrows) of r in-place.
 * Uses col_rel_get() for key extraction (layout-independent).
 * Sorts a permutation array instead of scattering full rows.
 * Permutation is applied once at the end via col_rel_row_copy_out/in.
 *
 * Optimizations (Issue #343):
 *   - Hybrid threshold: insertion sort for nrows <= 32
 *   - Skip-pass: skip byte positions where all values have the same byte
 *   - Byte-value cache: read column data once per pass, reuse for scatter
 *
 * Adaptive radix width (Issue #363 Phase 5c):
 *   - nrows >= 50000: k=16 via radix_sort_k16() — 4 passes × 65536 buckets
 *   - nrows <  50000: k=8  with SIMD fused uniform+count — 8 passes × 256 buckets
 *
 * Falls back to insertion sort on allocation failure.
 */
static int
col_rel_radix_sort_raw(col_rel_t *r, uint32_t start_row, uint32_t nrows)
{
    if (nrows <= 1)
        return 0;

    /* IEEE-754 keys need a different sign transform from signed integers.
     * Keep the established radix fast path for integer-only relations and
     * use the typed comparator until the float radix path is introduced. */
    if (r->column_types) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->column_types[c] == WIRELOG_TYPE_FLOAT)
                goto insertion;
        }
    }

    /* Hybrid threshold: insertion sort for small segments (Issue #343) */
    if (nrows <= 32)
        goto insertion;

    /* Adaptive radix width (Issue #363 Phase 5c): dispatch to k=16 for large
     * arrays where fewer passes outweigh the larger histogram cost.
     *
     * Empirical threshold (Apple M-series, 1-col, 64-bit uniform-random keys):
     *   nrows=10K: k8=0.66ms  k16=0.81ms  k8 faster by 1.23x
     *   nrows=20K: k8=0.81ms  k16=0.91ms  k8 faster by 1.12x
     *   nrows=30K: k8=0.83ms  k16=0.89ms  k8 faster by 1.08x
     *   nrows=40K: k8=0.81ms  k16=0.82ms  near parity
     *   nrows=50K: k8=0.79ms  k16=0.78ms  k16 faster by 1.01x
     *   nrows=60K: k8=0.82ms  k16=0.80ms  k16 faster by 1.02x
     *   nrows=100K: k8=1.41ms k16=1.32ms  k16 faster by 1.07x
     * Crossover at ~40-50K rows; 50000 is a conservative round boundary.
     *
     * k=16 uses a scalar fused loop (not SIMD): the 4-pass reduction
     * already yields fewer total iterations than 8-pass k=8+SIMD, and a
     * 256KB histogram makes SIMD gather impractical (cache pressure). */
    if (nrows >= 50000u) {
        int rc = radix_sort_k16(r, start_row, nrows);
        if (rc == 0)
            wl_columnar_relation_touch_view(r);
        return rc;
    }

    /* k=8 SIMD path: 8 passes × 256 buckets (1KB histogram, stack-allocated).
     * SIMD-dispatched fused uniform+count avoids a separate gather loop. */
    uint32_t nc = r->ncols;

    const uint32_t radix_bits = 8u;
    const uint32_t num_passes = 64u / radix_bits;  /* 8 for k=8 */

    uint32_t *perm_a = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint32_t *perm_b = (uint32_t *)malloc(nrows * sizeof(uint32_t));
    uint8_t  *bv_cache = (uint8_t *)malloc(nrows);
    if (!perm_a || !perm_b || !bv_cache) {
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        return nrows <= 32 ? col_rel_insertion_sort(r, start_row, nrows)
                           : ENOMEM;
    }

    for (uint32_t i = 0; i < nrows; i++)
        perm_a[i] = i;

    uint32_t *src = perm_a;
    uint32_t *dst = perm_b;
    uint32_t count[256];
    uint32_t prefix[256];

#ifdef WL_RADIX_BENCH
    /* _tU = fused uniform+count (radix_uniform_count_fused_fast)
     * _tS = scatter (prefix scan + scatter loop)
     * _tA = apply permutation per-column */
    uint64_t _tU = 0, _tS = 0, _tA = 0, _t0 = 0;
    uint32_t _nSk = 0, _nPs = 0;
#endif

    /* LSD radix sort: column nc-1 (LSB) to column 0 (MSB),
     * pass 0 (LSB) to pass num_passes-1 (MSB) within each column. */
    for (int c = (int)nc - 1; c >= 0; c--) {
        const int64_t *col_data = r->columns[c];
        for (uint32_t pass = 0; pass < num_passes; pass++) {
            int shift = (int)(pass * radix_bits);
            int is_sign_pass = (pass == num_passes - 1);

            /* Skip-pass: if all values identical at this byte position, skip. */
            uint8_t first_bv;
            {
                int64_t val = col_data[start_row + src[0]];
                first_bv = (uint8_t)((uint64_t)val >> shift);
                if (is_sign_pass)
                    first_bv ^= 0x80u;
            }
            /* Fused uniform check + count pass: single gather traversal
             * (Issue #363 Phase 1).  Returns true if all nrows bytes at
             * this position are identical (skip); returns false with
             * bv_cache[] and count[] populated when not uniform. */
#ifdef WL_RADIX_BENCH
            _t0 = now_ns();
#endif
            if (radix_uniform_count_fused_fast(col_data, start_row, src,
                nrows, shift, is_sign_pass, first_bv, bv_cache, count)) {
#ifdef WL_RADIX_BENCH
                _tU += now_ns() - _t0;
                _nSk++;
#endif
                continue;
            }
#ifdef WL_RADIX_BENCH
            _tU += now_ns() - _t0;
            _nPs++;
            _t0 = now_ns();
#endif
            prefix[0] = 0;
            for (int i = 1; i < 256; i++)
                prefix[i] = prefix[i - 1] + count[i - 1];

            /* Scatter pass with software prefetch (Issue #363 Phase 2). */
            for (uint32_t i = 0; i < nrows; i++) {
                if (i + 16u < nrows)
                    WL_PREFETCH_W(dst + prefix[bv_cache[i + 16u]]);
                dst[prefix[bv_cache[i]]++] = src[i];
            }
            uint32_t *t = src;
            src = dst;
            dst = t;
#ifdef WL_RADIX_BENCH
            _tS += now_ns() - _t0;
#endif
        }
    }

#ifdef WL_RADIX_BENCH
    _t0 = now_ns();
#endif
    /* Apply permutation per-column (Issue #334): contiguous access pattern. */
    int64_t *temp_col = (int64_t *)malloc(nrows * sizeof(int64_t));
    if (!temp_col) {
        free(perm_a);
        free(perm_b);
        free(bv_cache);
        return ENOMEM;
    }
    for (uint32_t c = 0; c < nc; c++) {
        int64_t *col = r->columns[c];
        /* Gather: prefetch 8 elements ahead (Issue #363 Phase 3). */
        for (uint32_t i = 0; i < nrows; i++) {
            if (i + 8u < nrows)
                WL_PREFETCH_R(col + start_row + src[i + 8u]);
            temp_col[i] = col[start_row + src[i]];
        }
        memcpy(col + start_row, temp_col, nrows * sizeof(int64_t));
    }
    free(temp_col);
    free(perm_a);
    free(perm_b);
    free(bv_cache);
#ifdef WL_RADIX_BENCH
    _tA = now_ns() - _t0;
    if (wl_columnar_relation_radix_bench_enabled()) {
        uint64_t _tot = _tU + _tS + _tA;
        fprintf(stderr,
            "[radix-bench k=8] nrows=%u nc=%u pass=%u skip=%u "
            "uniform_+_count=%.0f%% scatter=%.0f%% apply=%.0f%% "
            "total_ms=%.3f\n",
            nrows, nc, _nPs, _nSk,
            100.0 * (double)_tU / (double)(_tot + 1),
            100.0 * (double)_tS / (double)(_tot + 1),
            100.0 * (double)_tA / (double)(_tot + 1),
            (double)_tot * 1e-6);
    }
#endif
    wl_columnar_relation_touch_view(r);
    return 0;

insertion:
    {
        int rc = col_rel_insertion_sort(r, start_row, nrows);
        if (rc == 0)
            wl_columnar_relation_touch_view(r);
        return rc;
    }
}

/* Public sort entry point.  COW is performed outside the algorithm so the
 * direct range API and the all-row wrapper share the same borrowed-storage
 * contract.  A failed sort discards the private copy and restores the
 * original borrowed view. */
int
col_rel_radix_sort(col_rel_t *r, uint32_t start_row, uint32_t nrows)
{
    if (!r || start_row > r->nrows || nrows > r->nrows - start_row)
        return EINVAL;
    if (nrows <= 1)
        return 0;

    int64_t **old_columns = NULL;
    bool *old_shared = NULL;
    uint64_t ledger_before = 0;
    uint64_t old_view = 0;
    uint64_t old_storage = 0;
    bool borrowed = r->col_shared != NULL;
    if (borrowed) {
        old_columns = (int64_t **)malloc((size_t)r->ncols
                * sizeof(*old_columns));
        old_shared = (bool *)malloc((size_t)r->ncols * sizeof(*old_shared));
        if (!old_columns || !old_shared) {
            free((void *)old_columns);
            free(old_shared);
            return ENOMEM;
        }
        memcpy((void *)old_columns, (const void *)r->columns,
            (size_t)r->ncols * sizeof(*old_columns));
        memcpy(old_shared, r->col_shared,
            (size_t)r->ncols * sizeof(*old_shared));
        ledger_before = col_rel_owned_ledger_bytes(r);
        old_view = r->view_generation;
        old_storage = r->storage_generation;
        if (col_rel_cow_unshare(r, 0) != 0) {
            free((void *)old_columns);
            free(old_shared);
            return ENOMEM;
        }
    }

    int rc = col_rel_radix_sort_raw(r, start_row, nrows);
    if (rc != 0 && borrowed) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            int64_t *current = NULL;
            int64_t *saved = NULL;
            memcpy((void *)&current, (const unsigned char *)r->columns
                + (size_t)c * sizeof(current), sizeof(current));
            memcpy((void *)&saved, (const unsigned char *)old_columns
                + (size_t)c * sizeof(saved), sizeof(saved));
            if (current != saved)
                free((void *)current);
            memcpy((unsigned char *)r->columns
                + (size_t)c * sizeof(current), (const void *)&saved,
                sizeof(saved));
        }
        free(r->col_shared);
        r->col_shared = old_shared;
        old_shared = NULL;
        col_rel_ledger_reconcile(r, ledger_before);
        /* The detach was rolled back; its storage epoch goes with it. */
        r->view_generation = old_view;
        r->storage_generation = old_storage;
    }
    free((void *)old_columns);
    free(old_shared);
    return rc;
}

/*
 * col_rel_radix_sort_int64: sort all rows of r in-place using LSD radix sort.
 *
 * Sorts lexicographically by all ncols columns (column 0 is most significant).
 * Handles signed int64_t by flipping the sign bit on the MSB of each column
 * so that unsigned byte comparison yields the correct signed ordering.
 *
 * Complexity: O(ncols * 8 * nrows) time, O(nrows * ncols) extra space.
 * Sets r->sorted_nrows = r->nrows on completion.
 * Falls back to insertion sort on allocation failure.
 */
void
col_rel_radix_sort_int64(col_rel_t *r)
{
    if (!r || r->ncols == 0) {
        if (r)
            r->sorted_nrows = r->nrows;
        return;
    }
    if (r->nrows <= 1) {
        r->sorted_nrows = r->nrows;
        return;
    }
    if (r->ncols == 0)
        return;

    /* col_rel_radix_sort() detaches a shared view itself (through the
     * admitted col_rel_cow_unshare) and rolls the detach back if the sort
     * cannot allocate, so no COW happens here. */
    int rc = col_rel_radix_sort(r, 0, r->nrows);
    if (rc == 0)
        r->sorted_nrows = r->nrows;
}
