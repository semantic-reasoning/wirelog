/*
 * test_memory_admission_relation.c - retained relation ownership transitions
 *
 * Issue #1434: COW and arena-to-heap promotion must admit the replacement
 * before publishing ownership, and must leave the source relation untouched
 * when the exact budget is denied.
 */

#include "../wirelog/columnar/internal.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#ifndef _WIN32
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

static int failures;
static uint64_t overwrite_expected_peak;
static uint64_t overwrite_expected_live;
static unsigned overwrite_retirement_witnesses;
static uint64_t charged_fixed_bytes(const col_rel_t *relation);

#define CHECK(condition, message) do { \
            if (!(condition)) { \
                fprintf(stderr, "FAIL: %s\n", message); \
                failures++; \
            } \
} while (0)

static void
observe_overwrite_retirement(const col_rel_t *rel)
{
    overwrite_retirement_witnesses++;
    CHECK(rel && rel->memory_governor
        && rel->retained_reserved_bytes == overwrite_expected_live
        && col_rel_owned_ledger_bytes(rel) == overwrite_expected_live
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(rel->memory_governor))
        == overwrite_expected_peak + charged_fixed_bytes(rel),
        "overwrite frees old physical storage before releasing its token");
}

static void
make_resolution(wl_columnar_memory_resolution_t *resolution,
    uint64_t usable_bytes)
{
    memset(resolution, 0, sizeof(*resolution));
    resolution->budget_bytes = usable_bytes;
    resolution->usable_bytes = usable_bytes;
    resolution->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution->source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution->status = WL_COLUMNAR_MEMORY_OK;
}

static uint64_t
heap_descriptor_bytes(const char *name)
{
    return sizeof(col_rel_t) + strlen(name) + 1u
           + sizeof(wl_columnar_memory_reservation_t);
}

static uint64_t
charged_fixed_bytes(const col_rel_t *relation)
{
    if (!relation)
        return 0;
    return (relation->descriptor_reservation
        ? relation->descriptor_reservation->bytes : 0)
           + (relation->metadata_reservation
        ? relation->metadata_reservation->bytes : 0)
           + (relation->shared_table_reservation
        ? relation->shared_table_reservation->bytes : 0);
}

static uint64_t
shared_view_table_bytes(uint32_t ncols)
{
    return sizeof(wl_columnar_memory_reservation_t)
           + (uint64_t)ncols * (sizeof(int64_t *) + sizeof(bool));
}

static uint64_t
replacement_table_bytes(uint32_t ncols)
{
    return sizeof(wl_columnar_memory_reservation_t)
           + (uint64_t)ncols * sizeof(int64_t *);
}

static uint64_t
auto_metadata_bytes(uint32_t ncols)
{
    uint64_t bytes = sizeof(wl_columnar_memory_reservation_t) + 3u;
    for (uint32_t i = 0; i < ncols; i++) {
        char name[32];
        snprintf(name, sizeof(name), "col%u", i);
        bytes += sizeof(char *) + sizeof(struct ArrowSchema *)
            + sizeof(struct ArrowSchema) + 2u
            + 2u * (strlen(name) + 1u);
    }
    return bytes;
}

static uint64_t
heap_auto_bytes(const char *name, uint32_t ncols)
{
    return heap_descriptor_bytes(name) + auto_metadata_bytes(ncols);
}

static uint64_t
pool_auto_bytes(const char *name, uint32_t ncols)
{
    return strlen(name) + 1u + auto_metadata_bytes(ncols);
}

static col_rel_t *
make_shared_view(wl_columnar_memory_governor_ref_t *ref,
    col_rel_t **source_out)
{
    col_rel_t *source = col_rel_new_auto("source", 1);
    col_rel_t *view = col_rel_new_auto("view", 1);
    int64_t rows[] = { 9, 1 };

    if (!source || !view || col_rel_append_row(source, &rows[0]) != 0
        || col_rel_append_row(source, &rows[1]) != 0
        || col_rel_attach_memory_governor(view, ref) != 0
        || col_rel_install_shared_view(view, source) != 0) {
        col_rel_destroy(view);
        col_rel_destroy(source);
        return NULL;
    }
    *source_out = source;
    return view;
}

static col_rel_t *
make_full_shared_view(wl_columnar_memory_governor_ref_t *ref,
    col_rel_t **source_out, bool timestamps)
{
    col_rel_t *source = col_rel_new_auto("full-source", 1);
    col_rel_t *view = col_rel_new_auto("full-view", 1);
    int64_t value = 3;

    if (!source || !view)
        goto fail;
    for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++)
        if (col_rel_append_row(source, &value) != 0)
            goto fail;
    if (timestamps && col_rel_enable_timestamps(view) != 0)
        goto fail;
    if (col_rel_attach_memory_governor(view, ref) != 0
        || col_rel_install_shared_view(view, source) != 0)
        goto fail;
    *source_out = source;
    return view;

fail:
    col_rel_destroy(view);
    col_rel_destroy(source);
    return NULL;
}

static void
test_cow_exact_fit_and_denial(void)
{
    const uint64_t private_bytes = 64u * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = NULL;
    col_rel_t *view;

    make_resolution(&resolution, private_bytes + heap_auto_bytes("view", 1)
        + shared_view_table_bytes(1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    view = ref ? make_shared_view(ref, &source) : NULL;
    CHECK(view != NULL, "COW exact-fit setup");
    if (view) {
        int64_t *borrowed = view->columns[0];
        int64_t value = 5;
        CHECK(col_rel_append_row(view, &value) == 0,
            "spare-capacity append COW");
        CHECK(view->col_shared == NULL, "exact-fit COW was published");
        CHECK(view->columns[0] != borrowed, "COW retained borrowed buffer");
        WL_IGNORE_RESULT(col_rel_radix_sort_int64(view));
        CHECK(view->columns[0][0] == 1 && view->columns[0][1] == 5
            && view->columns[0][2] == 9,
            "COW append/sort did not preserve copied rows");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == private_bytes + charged_fixed_bytes(view),
            "exact-fit COW reservation");
        col_rel_destroy(view);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "COW reservation release");
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution,
        private_bytes - 1u + heap_auto_bytes("view", 1)
        + shared_view_table_bytes(1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    source = NULL;
    view = ref ? make_shared_view(ref, &source) : NULL;
    CHECK(view != NULL, "COW denial setup");
    if (view) {
        int64_t *borrowed = view->columns[0];
        WL_IGNORE_RESULT(col_rel_radix_sort_int64(view));
        CHECK(view->col_shared != NULL && view->columns[0] == borrowed,
            "denied COW changed ownership");
        CHECK(view->columns[0][0] == 9 && view->columns[0][1] == 1,
            "denied COW changed rows");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == charged_fixed_bytes(view),
            "denied COW left reservation");
        col_rel_destroy(view);
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_governed_logical_copy(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *source = col_rel_new_auto("logical-copy", 1);
    col_rel_t *copy = NULL;
    uint64_t expected = 0;
    int64_t value = 42;

    CHECK(source && col_rel_append_row(source, &value) == 0
        && col_rel_enable_timestamps(source) == 0,
        "governed logical-copy source");
    if (!source)
        return;
    CHECK(source->timestamps != NULL,
        "logical-copy timestamp allocation");
    if (!source->timestamps)
        goto cleanup;
    source->timestamps[0].multiplicity = -3;
    uint32_t timestamp_capacity = source->capacity + 8u;
    col_delta_timestamp_t *expanded = realloc(source->timestamps,
            (size_t)timestamp_capacity * sizeof(*expanded));
    CHECK(expanded != NULL, "logical-copy timestamp expansion");
    if (!expanded)
        goto cleanup;
    memset(expanded + source->timestamp_capacity, 0,
        (size_t)(timestamp_capacity - source->timestamp_capacity)
        * sizeof(*expanded));
    source->timestamps = expanded;
    source->timestamp_capacity = timestamp_capacity;
    source->sorted_nrows = source->nrows;
    source->base_nrows = 1;
    source->run_count = 1;
    source->run_ends[0] = source->nrows;
    expected = (uint64_t)source->ncols * source->capacity * sizeof(int64_t)
        + (uint64_t)timestamp_capacity * sizeof(col_delta_timestamp_t);
    make_resolution(&resolution,
        expected + heap_auto_bytes("logical-copy", 1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref != NULL, "logical-copy governor");
    if (!ref)
        goto cleanup;

    int copy_rc = wl_columnar_relation_deep_copy_governed(source, &copy, ref);
    if (copy_rc != 0)
        fprintf(stderr, "logical copy rc=%d cap=%u ts=%u expected=%llu\n",
            copy_rc, source->capacity, source->timestamp_capacity,
            (unsigned long long)expected);
    CHECK(copy_rc == 0 && copy,
        "exact-fit governed logical copy");
    if (copy) {
        CHECK(copy->relation_identity != source->relation_identity
            && copy->nrows == source->nrows &&
            copy->capacity == source->capacity
            && copy->base_nrows == source->base_nrows
            && copy->columns[0] != source->columns[0]
            && copy->columns[0][0] == value,
            "logical copy has independent rows and identity");
        CHECK(copy->sorted_nrows == source->sorted_nrows
            && copy->run_count == 1
            && copy->run_ends[0] == source->run_ends[0],
            "logical copy preserves valid sort metadata");
        CHECK(copy->timestamps && copy->timestamp_capacity
            == timestamp_capacity
            && copy->timestamps[0].multiplicity == -3,
            "logical copy preserves timestamps");
        CHECK(copy->memory_governor == ref
            && copy->retained_reserved_bytes == expected
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == expected + charged_fixed_bytes(copy),
            "logical copy carries exact payload reservation");
        CHECK(wl_columnar_relation_accounting_complete(copy, ref),
            "complete accounting uses physical timestamp capacity");
        uint64_t retained_token_bytes = copy->retained_reservation.bytes;
        copy->retained_reservation.bytes--;
        CHECK(!wl_columnar_relation_accounting_complete(copy, ref),
            "complete accounting rejects a short timestamp reservation");
        copy->retained_reservation.bytes = retained_token_bytes;
        col_rel_destroy(copy);
        copy = NULL;
    }
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "logical-copy reservation released");

    wl_columnar_source_access_writer_t writer = { 0 };
    CHECK(col_rel_source_writer_acquire(source, &writer) == 0,
        "logical-copy source writer setup");
    CHECK(wl_columnar_relation_deep_copy_governed(source, &copy,
        ref) == EBUSY && !copy
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "logical copy refuses a concurrently published source");
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0,
        "logical-copy source writer release");

    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        expected - 1u + heap_auto_bytes("logical-copy", 1),
        memory_order_release);
    CHECK(wl_columnar_relation_deep_copy_governed(source, &copy,
        ref) == ENOMEM && !copy
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0
        && source->nrows == 1 && source->columns[0][0] == value,
        "denied logical copy leaves source and governor unchanged");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        expected + heap_auto_bytes("logical-copy", 1),
        memory_order_release);
    wl_columnar_relation_test_fail_next_governed_copy_payload_alloc();
    CHECK(wl_columnar_relation_deep_copy_governed(source, &copy, ref) == ENOMEM
        && !copy && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0
        && source->nrows == 1 && source->columns[0][0] == value,
        "post-admission allocation failure rolls back reservation");
    CHECK(wl_columnar_relation_deep_copy_governed(source, &copy,
        ref) == 0 && copy,
        "denied logical copy can retry");
cleanup:
    col_rel_destroy(copy);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "logical-copy retry reservation released");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_governed_empty_compound_copy(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *source = col_rel_new_auto("empty-compound-copy", 2);
    col_rel_t *copy = NULL;
    uint32_t arity = 2;

    CHECK(source != NULL, "empty compound-copy source");
    if (!source)
        return;
    source->compound_arity_map = malloc(sizeof(arity));
    source->compound_arity_len = 1;
    CHECK(source->compound_arity_map != NULL,
        "empty compound-copy arity allocation");
    if (!source->compound_arity_map)
        goto cleanup;
    source->compound_arity_map[0] = arity;
    source->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    source->compound_count = 1;
    if (!source->column_types)
        source->column_types = calloc(source->ncols,
                sizeof(*source->column_types));
    CHECK(source->column_types != NULL,
        "empty compound-copy type allocation");
    if (!source->column_types)
        goto cleanup;
    source->column_types[0] = WIRELOG_TYPE_INT64;
    source->column_types[1] = WIRELOG_TYPE_INT64;
    uint64_t expected = (uint64_t)source->ncols * source->capacity
        * sizeof(int64_t);
    make_resolution(&resolution,
        expected + heap_auto_bytes("empty-compound-copy", 2)
        + 2u * sizeof(wirelog_column_type_t) + sizeof(uint32_t));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref != NULL, "empty compound-copy governor");
    if (!ref)
        goto cleanup;

    CHECK(wl_columnar_relation_deep_copy_governed(source, &copy, ref) == 0
        && copy, "governed empty compound copy");
    if (copy) {
        CHECK(copy->nrows == 0 && copy->ncols == 2
            && copy->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
            && copy->compound_count == 1 && copy->compound_arity_map
            && copy->compound_arity_map[0] == arity,
            "empty compound copy preserves logical schema metadata");
        CHECK(copy->column_types && source->column_types
            && copy->column_types[0] == source->column_types[0]
            && copy->column_types[1] == source->column_types[1],
            "empty compound copy preserves column types");
    }
cleanup:
    col_rel_destroy(copy);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "empty compound-copy reservation released");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_cow_multi_column_cleanup(void)
{
    const uint64_t private_bytes = 2u * 64u * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = col_rel_new_auto("multi-source", 2);
    col_rel_t *view = col_rel_new_auto("multi-view", 2);
    int64_t first[] = {9, 90};
    int64_t second[] = {1, 10};
    int64_t appended[] = {5, 50};

    make_resolution(&resolution,
        private_bytes + heap_auto_bytes("multi-view", 2)
        + shared_view_table_bytes(2));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && source && view, "multi-column COW setup");
    if (ref && source && view) {
        CHECK(col_rel_append_row(source, first) == 0
            && col_rel_append_row(source, second) == 0
            && col_rel_attach_memory_governor(view, ref) == 0
            && col_rel_install_shared_view(view, source) == 0,
            "multi-column shared view setup");
        CHECK(col_rel_append_row(view, appended) == 0,
            "multi-column COW append");
        CHECK(view->col_shared == NULL
            && view->columns[0][2] == appended[0]
            && view->columns[1][2] == appended[1],
            "multi-column COW did not privatize every column");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == private_bytes + charged_fixed_bytes(view),
            "multi-column COW reservation");
    }
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_arena_promotion_with_timestamps(void)
{
    const uint32_t capacity = COL_REL_INIT_CAP;
    const uint64_t bytes = (uint64_t)capacity * sizeof(int64_t)
        + (uint64_t)capacity * sizeof(col_delta_timestamp_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    delta_pool_t *pool;
    wl_arena_t *arena;
    col_rel_t *relation;
    int64_t value = 42;

    make_resolution(&resolution, bytes + pool_auto_bytes("arena", 1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    arena = wl_arena_create(4096);
    relation = pool && arena ? col_rel_pool_new_auto(pool, arena, "arena", 1)
                             : NULL;
    CHECK(ref && relation && col_rel_append_row(relation, &value) == 0,
        "arena promotion setup");
    if (relation) {
        CHECK(col_rel_enable_timestamps(relation) == 0,
            "timestamps before arena promotion");
        CHECK(col_rel_attach_memory_governor(relation, ref) == 0,
            "arena relation governor attach");
        int64_t *old_columns = relation->columns[0];
        CHECK(col_rel_promote_arena_admitted(relation) == 0,
            "exact-fit arena promotion");
        CHECK(!relation->arena_owned && relation->columns[0] != old_columns,
            "arena promotion did not publish heap ownership");
        CHECK(relation->columns[0][0] == value
            && relation->timestamps != NULL,
            "arena promotion lost data or timestamps");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == bytes + charged_fixed_bytes(relation),
            "arena promotion reservation");
        col_rel_free_contents(relation);
    }
    if (pool)
        delta_pool_destroy(pool);
    if (arena)
        wl_arena_free(arena);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "arena promotion reservation release");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_append_transitions(void)
{
    const uint64_t grown_bytes = (uint64_t)(COL_REL_INIT_CAP * 2u)
        * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = NULL;
    col_rel_t *view;
    int64_t value = 11;

    make_resolution(&resolution,
        grown_bytes + heap_auto_bytes("full-view", 1)
        + shared_view_table_bytes(1) + replacement_table_bytes(1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    view = ref ? make_full_shared_view(ref, &source, false) : NULL;
    CHECK(view != NULL, "append_row transition setup");
    if (view) {
        CHECK(col_rel_append_row(view, &value) == 0
            && view->capacity == COL_REL_INIT_CAP * 2u
            && view->nrows == COL_REL_INIT_CAP + 1u
            && view->col_shared == NULL,
            "append_row COW growth");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == grown_bytes + charged_fixed_bytes(view),
            "append_row COW reservation");
        col_rel_destroy(view);
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution,
        grown_bytes + heap_auto_bytes("full-view", 1)
        + shared_view_table_bytes(1) + replacement_table_bytes(1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    source = NULL;
    view = ref ? make_full_shared_view(ref, &source, false) : NULL;
    CHECK(view != NULL, "append_all transition setup");
    if (view) {
        col_rel_t *suffix = col_rel_new_auto("suffix", 1);
        CHECK(suffix && col_rel_append_row(suffix, &value) == 0
            && col_rel_append_all(view, suffix, NULL) == 0
            && view->capacity == COL_REL_INIT_CAP * 2u
            && view->nrows == COL_REL_INIT_CAP + 1u
            && view->col_shared == NULL,
            "append_all COW growth");
        if (suffix)
            col_rel_destroy(suffix);
        col_rel_destroy(view);
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution, (uint64_t)(COL_REL_INIT_CAP * 2u)
        * (sizeof(int64_t) + sizeof(col_delta_timestamp_t))
        + heap_auto_bytes("full-view", 1)
        + shared_view_table_bytes(1) + replacement_table_bytes(1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    source = NULL;
    view = ref ? make_full_shared_view(ref, &source, true) : NULL;
    CHECK(view != NULL, "timestamp transition setup");
    if (view) {
        CHECK(col_rel_append_row(view, &value) == 0
            && view->timestamps != NULL
            && view->capacity == COL_REL_INIT_CAP * 2u,
            "timestamp COW growth");
        col_rel_destroy(view);
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_arena_promotion_denial_preserves_state(void)
{
    const uint64_t bytes = (uint64_t)COL_REL_INIT_CAP * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    delta_pool_t *pool = NULL;
    wl_arena_t *arena = NULL;
    col_rel_t *relation = NULL;
    int64_t value = 7;

    make_resolution(&resolution,
        bytes - 1u + pool_auto_bytes("denied", 1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    arena = wl_arena_create(4096);
    relation = pool && arena ? col_rel_pool_new_auto(pool, arena, "denied", 1)
                             : NULL;
    CHECK(ref && relation && col_rel_append_row(relation, &value) == 0,
        "denied arena setup");
    if (relation) {
        int64_t *old_columns = relation->columns[0];
        CHECK(col_rel_attach_memory_governor(relation, ref) == 0,
            "denied arena governor attach");
        CHECK(col_rel_promote_arena_admitted(relation) == ENOMEM,
            "arena denial result");
        CHECK(relation->arena_owned && relation->columns[0] == old_columns
            && relation->columns[0][0] == value,
            "arena denial changed relation");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == charged_fixed_bytes(relation),
            "arena denial left reservation");
        col_rel_free_contents(relation);
    }
    if (pool)
        delta_pool_destroy(pool);
    if (arena)
        wl_arena_free(arena);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_unmanaged_arena_append_all_preserves_ownership(void)
{
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    wl_arena_t *arena = wl_arena_create(4096);
    col_rel_t *dst = pool && arena
        ? col_rel_pool_new_auto(pool, arena, "unmanaged-dst", 1) : NULL;
    col_rel_t *src = col_rel_new_auto("unmanaged-src", 1);
    int64_t value = 19;

    CHECK(dst && src, "unmanaged arena append_all setup");
    if (dst && src) {
        CHECK(col_rel_append_row(dst, &value) == 0,
            "unmanaged arena destination seed");
        for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++)
            CHECK(col_rel_append_row(src, &value) == 0,
                "unmanaged arena source fill");
        int64_t *old_column = dst->columns[0];
        CHECK(col_rel_append_all(dst, src, arena) == 0,
            "unmanaged arena append_all growth");
        CHECK(dst->arena_owned && dst->columns[0] != old_column
            && dst->capacity == COL_REL_INIT_CAP * 2u
            && dst->nrows == COL_REL_INIT_CAP + 1u,
            "unmanaged arena growth lost arena ownership");
        col_rel_free_contents(dst);
    }
    col_rel_destroy(src);
    if (pool)
        delta_pool_destroy(pool);
    if (arena)
        wl_arena_free(arena);
}

static col_rel_t *
make_full_arena_relation(delta_pool_t **pool_out, wl_arena_t **arena_out,
    const char *name, int64_t value)
{
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    wl_arena_t *arena = wl_arena_create(4096);
    col_rel_t *relation = pool && arena
        ? col_rel_pool_new_auto(pool, arena, name, 1) : NULL;

    if (relation) {
        for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++) {
            if (col_rel_append_row(relation, &value) != 0) {
                col_rel_free_contents(relation);
                relation = NULL;
                break;
            }
        }
    }
    if (!relation) {
        if (pool)
            delta_pool_destroy(pool);
        if (arena)
            wl_arena_free(arena);
        pool = NULL;
        arena = NULL;
    }
    *pool_out = pool;
    *arena_out = arena;
    return relation;
}

static void
test_unmanaged_arena_append_all_reconciles_timestamps(void)
{
    const uint32_t grown_capacity = COL_REL_INIT_CAP * 2u;
    delta_pool_t *pool = NULL;
    wl_arena_t *arena = NULL;
    col_rel_t *dst = make_full_arena_relation(&pool, &arena,
            "ledger-arena-dst", 31);
    col_rel_t *src = col_rel_new_auto("ledger-arena-src", 1);
    wl_mem_ledger_t ledger;
    wl_mem_ledger_snapshot_t snapshot;
    int64_t value = 47;

    wl_mem_ledger_init(&ledger, 0);
    CHECK(dst && src, "arena timestamp ledger setup");
    if (dst && src) {
        CHECK(col_rel_enable_timestamps(dst) == 0,
            "arena timestamp enable");
        dst->mem_ledger = &ledger;
        col_rel_ledger_reconcile(dst, 0);
        for (uint32_t i = 0; i < 1; i++)
            CHECK(col_rel_append_row(src, &value) == 0,
                "arena timestamp source seed");
        /* Fill the source enough to force exactly one destination growth. */
        for (uint32_t i = 1; i < COL_REL_INIT_CAP; i++)
            CHECK(col_rel_append_row(src, &value) == 0,
                "arena timestamp source fill");
        CHECK(col_rel_append_all(dst, src, arena) == 0,
            "arena append_all timestamp growth");
        wl_mem_ledger_snapshot(&ledger, &snapshot);
        CHECK(dst->arena_owned && dst->capacity == grown_capacity,
            "arena timestamp growth changed ownership");
        CHECK(snapshot.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP]
            == (uint64_t)grown_capacity * sizeof(col_delta_timestamp_t),
            "arena append_all timestamp ledger accounting");
        CHECK(dst->ledger_ts_bytes
            == (uint64_t)grown_capacity * sizeof(col_delta_timestamp_t),
            "arena append_all timestamp ledger token");
        col_rel_free_contents(dst);
        wl_mem_ledger_snapshot(&ledger, &snapshot);
        CHECK(snapshot.current_bytes == 0,
            "arena timestamp ledger release");
    }
    col_rel_destroy(src);
    if (pool)
        delta_pool_destroy(pool);
    if (arena)
        wl_arena_free(arena);
}

static void
test_arena_append_all_admission_boundary(void)
{
    const uint64_t exact_bytes = (uint64_t)(COL_REL_INIT_CAP * 2u)
        * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    delta_pool_t *pool = NULL;
    wl_arena_t *arena = NULL;
    col_rel_t *dst;
    col_rel_t *src;
    int64_t value = 53;

    make_resolution(&resolution,
        exact_bytes + pool_auto_bytes("arena-append-exact", 1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    dst = ref ? make_full_arena_relation(&pool, &arena,
            "arena-append-exact", value) : NULL;
    src = col_rel_new_auto("arena-append-source", 1);
    CHECK(dst && src, "arena append_all exact-fit setup");
    if (dst && src) {
        CHECK(col_rel_attach_memory_governor(dst, ref) == 0,
            "arena append_all exact-fit governor");
        CHECK(col_rel_append_row(src, &value) == 0
            && col_rel_append_all(dst, src, arena) == 0,
            "arena append_all exact-fit admission");
        CHECK(!dst->arena_owned && dst->capacity == COL_REL_INIT_CAP * 2u
            && dst->nrows == COL_REL_INIT_CAP + 1u,
            "arena append_all exact-fit state");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == exact_bytes + charged_fixed_bytes(dst),
            "arena append_all exact-fit reservation");
        col_rel_destroy(src);
        col_rel_free_contents(dst);
    } else {
        col_rel_destroy(src);
        if (dst)
            col_rel_free_contents(dst);
    }
    if (pool)
        delta_pool_destroy(pool);
    if (arena)
        wl_arena_free(arena);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution,
        exact_bytes - 1u + pool_auto_bytes("arena-append-denied", 1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    pool = NULL;
    arena = NULL;
    dst = ref ? make_full_arena_relation(&pool, &arena,
            "arena-append-denied", value) : NULL;
    src = col_rel_new_auto("arena-append-denied-source", 1);
    CHECK(dst && src, "arena append_all denial setup");
    if (dst && src) {
        int64_t *old_column = dst->columns[0];
        uint32_t old_capacity = dst->capacity;
        uint32_t old_rows = dst->nrows;
        CHECK(col_rel_attach_memory_governor(dst, ref) == 0,
            "arena append_all denial governor");
        CHECK(col_rel_append_row(src, &value) == 0
            && col_rel_append_all(dst, src, arena) == ENOMEM,
            "arena append_all one-byte denial");
        CHECK(dst->arena_owned && dst->columns[0] == old_column
            && dst->capacity == old_capacity && dst->nrows == old_rows,
            "arena append_all denial changed state");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == charged_fixed_bytes(dst),
            "arena append_all denial left reservation");
        col_rel_destroy(src);
        col_rel_free_contents(dst);
    } else {
        col_rel_destroy(src);
        if (dst)
            col_rel_free_contents(dst);
    }
    if (pool)
        delta_pool_destroy(pool);
    if (arena)
        wl_arena_free(arena);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static col_rel_t *
make_full_heap_relation(const char *name, int64_t value, bool timestamps)
{
    col_rel_t *relation = col_rel_new_auto(name, 1);
    if (!relation)
        return NULL;
    for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++) {
        if (col_rel_append_row(relation, &value) != 0)
            goto fail;
    }
    if (timestamps && col_rel_enable_timestamps(relation) != 0)
        goto fail;
    return relation;

fail:
    col_rel_destroy(relation);
    return NULL;
}

static void
test_heap_append_all_admission_boundary(void)
{
    const uint64_t exact_bytes = (uint64_t)(COL_REL_INIT_CAP * 2u)
        * (sizeof(int64_t) + sizeof(col_delta_timestamp_t));
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *dst;
    col_rel_t *src;
    int64_t value = 61;

    make_resolution(&resolution,
        exact_bytes + heap_auto_bytes("heap-append-exact", 1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    dst = ref ? make_full_heap_relation("heap-append-exact", value, true)
              : NULL;
    src = col_rel_new_auto("heap-append-source", 1);
    CHECK(dst && src, "heap append_all exact-fit setup");
    if (dst && src) {
        uint64_t old_storage = dst->storage_generation;
        CHECK(col_rel_enable_timestamps(src) == 0
            && col_rel_append_row(src, &value) == 0,
            "heap append_all timestamp source");
        src->timestamps[0].iteration = 77;
        CHECK(col_rel_attach_memory_governor(dst, ref) == 0,
            "heap append_all exact-fit governor");
        CHECK(col_rel_append_all(dst, src, NULL) == 0,
            "heap append_all exact-fit admission");
        CHECK(dst->capacity == COL_REL_INIT_CAP * 2u
            && dst->nrows == COL_REL_INIT_CAP + 1u
            && dst->timestamps[COL_REL_INIT_CAP].iteration == 77,
            "heap append_all exact-fit state");
        CHECK(dst->storage_generation == old_storage + 1u,
            "heap append_all advanced storage generation once");
        CHECK(dst->retained_reserved_bytes == exact_bytes
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == exact_bytes + charged_fixed_bytes(dst),
            "heap append_all exact-fit reservation");
    }
    col_rel_destroy(src);
    col_rel_destroy(dst);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution,
        exact_bytes - 1u + heap_auto_bytes("heap-append-denied", 1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    dst = ref ? make_full_heap_relation("heap-append-denied", value, true)
              : NULL;
    src = col_rel_new_auto("heap-append-denied-source", 1);
    CHECK(dst && src, "heap append_all denial setup");
    if (dst && src) {
        int64_t *old_columns = dst->columns[0];
        col_delta_timestamp_t *old_timestamps = dst->timestamps;
        uint32_t old_capacity = dst->capacity;
        uint32_t old_rows = dst->nrows;
        uint64_t old_storage = dst->storage_generation;
        CHECK(col_rel_enable_timestamps(src) == 0
            && col_rel_append_row(src, &value) == 0,
            "heap append_all denial timestamp source");
        CHECK(col_rel_attach_memory_governor(dst, ref) == 0,
            "heap append_all denial governor");
        CHECK(col_rel_append_all(dst, src, NULL) == ENOMEM,
            "heap append_all one-byte denial");
        CHECK(dst->columns[0] == old_columns
            && dst->timestamps == old_timestamps
            && dst->capacity == old_capacity && dst->nrows == old_rows
            && dst->storage_generation == old_storage,
            "heap append_all denial changed state");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == charged_fixed_bytes(dst),
            "heap append_all denial left reservation");
    }
    col_rel_destroy(src);
    col_rel_destroy(dst);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_cow_capacity_denial_preserves_state(void)
{
    const uint64_t grown_bytes = (uint64_t)(COL_REL_INIT_CAP * 2u)
        * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = NULL;
    col_rel_t *view;
    int64_t value = 23;

    make_resolution(&resolution,
        grown_bytes - 1u + heap_auto_bytes("full-view", 1)
        + shared_view_table_bytes(1) + replacement_table_bytes(1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    view = ref ? make_full_shared_view(ref, &source, false) : NULL;
    CHECK(view != NULL, "COW capacity denial setup");
    if (view) {
        uint32_t old_capacity = view->capacity;
        uint32_t old_rows = view->nrows;
        int64_t *old_column = view->columns[0];
        bool *old_shared = view->col_shared;
        CHECK(col_rel_append_row(view, &value) == ENOMEM,
            "COW capacity denial result");
        CHECK(view->capacity == old_capacity && view->nrows == old_rows
            && view->columns[0] == old_column
            && view->col_shared == old_shared
            && view->col_shared != NULL
            && view->columns[0][0] == 3,
            "COW capacity denial changed relation");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == charged_fixed_bytes(view),
            "COW capacity denial left reservation");
        col_rel_destroy(view);
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_cow_ledger_reconcile_is_exact_once(void)
{
    wl_mem_ledger_t ledger;
    wl_mem_ledger_snapshot_t snapshot;
    col_rel_t *source = col_rel_new_auto("ledger-source", 1);
    col_rel_t *view = col_rel_new_auto("ledger-view", 1);
    int64_t rows[] = { 9, 1 };
    bool view_destroyed = false;

    wl_mem_ledger_init(&ledger, 0);
    CHECK(source && view, "COW ledger setup");
    if (source && view) {
        CHECK(col_rel_append_row(source, &rows[0]) == 0
            && col_rel_append_row(source, &rows[1]) == 0
            && col_rel_install_shared_view(view, source) == 0,
            "COW ledger shared view");
        view->mem_ledger = &ledger;
        col_rel_ledger_reconcile(view, 0);
        WL_IGNORE_RESULT(col_rel_radix_sort_int64(view));
        wl_mem_ledger_snapshot(&ledger, &snapshot);
        CHECK(snapshot.subsys_bytes[WL_MEM_SUBSYS_RELATION]
            == (uint64_t)view->capacity * sizeof(int64_t),
            "COW radix sort double-charged private columns");
        col_rel_destroy(view);
        wl_mem_ledger_snapshot(&ledger, &snapshot);
        CHECK(snapshot.current_bytes == 0,
            "COW radix sort left stale ledger bytes");
        view_destroyed = true;
    }
    if (!view_destroyed)
        col_rel_destroy(view);
    if (source)
        col_rel_destroy(source);
}

static void
test_physical_timestamp_capacity(void)
{
    col_rel_t *r = col_rel_new_auto("physical-ts", 1);
    col_rel_t *copy = NULL;
    col_rel_t *view = col_rel_new_auto("physical-view", 1);
    col_rel_t *replacement_target = col_rel_new_auto("physical-replacement", 1);
    col_rel_replacement_t replacement = { 0 };
    wl_mem_ledger_t ledger;
    wl_mem_ledger_snapshot_t snapshot;
    wl_mem_ledger_init(&ledger, 0);
    CHECK(r && view, "physical timestamp fixture");
    if (!r || !view) goto cleanup;
    int64_t value = 42;
    CHECK(col_rel_append_row(r, &value) == 0
        && col_rel_enable_timestamps(r) == 0, "physical timestamp seed");
    r->timestamps[0].multiplicity = -7;
    uint32_t physical = r->timestamp_capacity;
    uint64_t bytes = (uint64_t)physical * sizeof(col_delta_timestamp_t);
    CHECK(physical == r->capacity && bytes > 0, "initial physical capacity");
    r->mem_ledger = &ledger;
    col_rel_ledger_reconcile(r, 0);
    /* Mirror the column-only rollback detachment: the timestamp allocation
     * remains live while the logical relation has no rows/column capacity. */
    int64_t **columns = r->columns;
    uint32_t capacity = r->capacity;
    uint64_t before = col_rel_owned_ledger_bytes(r);
    r->columns = NULL;
    r->capacity = 0;
    r->nrows = 0;
    col_rel_ledger_reconcile(r, before);
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    CHECK(r->timestamp_capacity == physical
        && col_rel_timestamp_ledger_bytes(r) == bytes
        && col_rel_transport_bytes(r) == bytes
        && snapshot.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == bytes,
        "detached columns must not credit retained timestamps");
    CHECK(col_rel_deep_copy(r, &copy, NULL) == 0 && copy
        && copy->timestamp_capacity == physical
        && col_rel_timestamp_ledger_bytes(copy) == bytes
        && copy->timestamps[0].multiplicity == -7,
        "detached deep copy preserves physical timestamps");
    CHECK(col_rel_install_shared_view(view, r) == 0
        && view->timestamp_capacity == physical
        && view->timestamps != r->timestamps
        && col_rel_transport_bytes(view) == bytes,
        "alias owns independent retained timestamp allocation");
    col_rel_destroy(view); view = NULL;
    if (copy && replacement_target) {
        int rc = col_rel_prepare_replacement(replacement_target, copy,
                &replacement);
        CHECK(rc == 0, "replacement plans retained physical timestamp bytes");
        if (rc == 0) col_rel_commit_replacement_locked(replacement_target,
                &replacement);
        col_rel_discard_replacement(&replacement);
        CHECK(replacement_target->timestamp_capacity == physical
            && col_rel_transport_bytes(replacement_target) == bytes,
            "replacement transfers physical capacity");
    }
    /* Restore smaller logical storage without reallocating timestamp memory. */
    r->columns = columns;
    r->capacity = capacity / 2;
    r->nrows = 1;
    col_rel_ledger_reconcile(r, 0);
    CHECK(col_rel_timestamp_ledger_bytes(r) == bytes,
        "smaller restored columns must not shrink timestamp accounting");
    before = col_rel_owned_ledger_bytes(r);
    r->capacity = capacity;
    col_rel_ledger_reconcile(r, before);
    CHECK(col_rel_compact(r) == 0
        && r->timestamp_capacity == r->capacity
        && col_rel_timestamp_ledger_bytes(r) ==
        (uint64_t)r->timestamp_capacity * sizeof(col_delta_timestamp_t),
        "physical compaction updates timestamp capacity");
cleanup:
    col_rel_discard_replacement(&replacement);
    col_rel_destroy(replacement_target);
    col_rel_destroy(view);
    col_rel_destroy(copy);
    col_rel_destroy(r);
    wl_mem_ledger_snapshot(&ledger, &snapshot);
    CHECK(snapshot.current_bytes == 0,
        "physical timestamp ledger release once");
}

static void
test_cow_retained_timestamp_capacity_admission(void)
{
    const uint32_t physical = COL_REL_INIT_CAP * 2u;
    const uint64_t bytes = (uint64_t)COL_REL_INIT_CAP * sizeof(int64_t)
        + (uint64_t)physical * sizeof(col_delta_timestamp_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *source = NULL;
    col_rel_t *view = NULL;
    int64_t value = 9;

    make_resolution(&resolution,
        bytes - 1u + heap_auto_bytes("skew-view", 1)
        + shared_view_table_bytes(1));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    source = col_rel_new_auto("skew-source", 1);
    view = col_rel_new_auto("skew-view", 1);
    CHECK(ref && source && view, "skewed timestamp COW setup");
    if (!ref || !source || !view) goto cleanup;
    CHECK(col_rel_append_row(source, &value) == 0
        && col_rel_enable_timestamps(source) == 0,
        "skewed timestamp seed");
    if (!source->timestamps) goto cleanup;
    col_delta_timestamp_t *expanded = realloc(source->timestamps,
            (size_t)physical * sizeof(*expanded));
    CHECK(expanded != NULL, "expand physical timestamp capacity");
    if (!expanded) goto cleanup;
    source->timestamps = expanded;
    source->timestamp_capacity = physical;
    CHECK(col_rel_attach_memory_governor(view, ref) == 0
        && col_rel_install_shared_view(view, source) == 0,
        "install skewed timestamp view");
    int64_t *old_column = view->columns[0];
    bool *old_shared = view->col_shared;
    CHECK(col_rel_cow_unshare(view, 0) == ENOMEM
        && view->columns[0] == old_column && view->col_shared == old_shared
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == charged_fixed_bytes(view),
        "retained timestamp capacity denial preserves state");
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref) wl_columnar_memory_governor_ref_release(ref);
}

static void
test_governed_pool_clone_fallback(void)
{
    for (unsigned route = 0; route < 3; route++) {
        wl_columnar_memory_resolution_t resolution;
        make_resolution(&resolution, 1024 * 1024);
        wl_columnar_memory_governor_ref_t *ref =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(ref != NULL, "clone governor setup");
        if (!ref) continue;
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(ref);
        delta_pool_t *pool = route ?
            delta_pool_create_managed(1, sizeof(col_rel_t), 64, g) : NULL;
        col_rel_t *src = col_rel_new_auto("source", 1);
        col_rel_t *occupied = NULL, *clone = NULL;
        CHECK(src && (!route || pool), "clone fixture setup");
        if (!src || (route && !pool)) goto cleanup;
        int64_t value = 42;
        CHECK(col_rel_append_row(src, &value) == 0, "source seed");
        CHECK(col_rel_enable_timestamps(src) == 0, "source timestamp mode");
        if (route == 2) {
            occupied = col_rel_pool_new_like(pool, "occupied", src);
            CHECK(occupied && occupied->pool_owned, "actual pool exhaustion");
            if (!occupied) goto cleanup;
        }
        uint64_t baseline = wl_columnar_memory_reserved(g);
        uint64_t payload = (uint64_t)COL_REL_INIT_CAP * sizeof(int64_t);
        uint64_t descriptor = route == 1
            ? pool_auto_bytes("retry", 1) + sizeof("retry")
            + sizeof(wl_columnar_memory_reservation_t)
            : heap_auto_bytes("retry", 1);
        uint32_t used = pool ? pool->slot_used : 0;
        atomic_store_explicit(&g->usable_bytes,
            baseline + payload + descriptor - 1,
            memory_order_release);
        clone = wl_columnar_relation_pool_new_like_governed(pool, "denied", src,
                ref);
        CHECK(!clone, "constructor cannot escape admission via fallback");
        CHECK(!pool || pool->slot_used == used, "failed slot restored");
        CHECK(wl_columnar_memory_reserved(g) == baseline,
            "failed reservation restored");
        if (clone) goto cleanup;
        atomic_store_explicit(&g->usable_bytes,
            baseline + payload + descriptor,
            memory_order_release);
        clone = wl_columnar_relation_pool_new_like_governed(pool, "retry", src,
                ref);
        CHECK(clone && clone->memory_governor == ref,
            "effective governor retained");
        if (!clone) goto cleanup;
        CHECK(clone->pool_owned == (route == 1), "expected pool or heap route");
        CHECK(!clone->timestamps, "legacy timestamp mode unchanged");
        CHECK(col_rel_enable_timestamps(clone) != 0 && !clone->timestamps,
            "timestamp admission refusal");
        for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++)
            CHECK(col_rel_append_row(clone, &value) == 0,
                "fill admitted capacity");
        CHECK(col_rel_append_row(clone, &value) != 0
            && clone->nrows == COL_REL_INIT_CAP,
            "growth denial preserves rows");
        atomic_store_explicit(&g->usable_bytes, 1024 * 1024,
            memory_order_release);
        CHECK(col_rel_enable_timestamps(clone) == 0
            && col_rel_append_row(clone, &value) == 0,
            "timestamp and growth retry");
        CHECK(src->nrows == 1 && src->columns[0][0] == value,
            "source unchanged");
        col_rel_destroy(clone); clone = NULL;
        CHECK(wl_columnar_memory_reserved(g) == baseline,
            "clone reservation balanced");
cleanup:
        col_rel_destroy(clone);
        col_rel_destroy(occupied);
        col_rel_destroy(src);
        delta_pool_destroy(pool);
        CHECK(wl_columnar_memory_reserved(g) == 0,
            "slab and payload charged once and released");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_relation_retirement_token(void)
{
    wl_columnar_relation_retirement_token_t token;
    wl_columnar_relation_retirement_token_t copied_token;
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_source_access_reader_t descriptor_reader = { 0 };
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *relation = col_rel_new_auto("retirement", 1);
    uint64_t baseline;
    int64_t value = 19;

    make_resolution(&resolution, 1024 * 1024);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(relation && ref, "retirement fixture setup");
    if (!relation || !ref)
        goto cleanup;
    CHECK(col_rel_attach_memory_governor(relation, ref) == 0,
        "retirement fixture admission");
    for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++)
        CHECK(col_rel_append_row(relation, &value) == 0,
            "retirement fixture growth");
    CHECK(col_rel_enable_timestamps(relation) == 0,
        "retirement fixture timestamp admission");
    baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    CHECK(baseline > 0, "retirement has a live reservation");

    /* Descriptor readers prevent even the first writer from being admitted. */
    CHECK(wl_columnar_source_access_reader_acquire(
            &relation->descriptor_access, &descriptor_reader) == 0,
        "retirement descriptor reader acquire");
    wl_columnar_relation_retirement_init(&token);
    CHECK(wl_columnar_relation_retirement_prepare(relation, &token) == EBUSY
        && atomic_load_explicit(&relation->source_access.state,
        memory_order_acquire) == 0,
        "retirement refuses an active descriptor reader");
    CHECK(wl_columnar_source_access_reader_release(&descriptor_reader) == 0,
        "retirement descriptor reader release");

    /* A source reader lets descriptor admission succeed and source admission
     * fail. The partial writer must be released so the same token can retry. */
    CHECK(wl_columnar_source_access_reader_acquire(&relation->source_access,
        &reader) == 0, "retirement reader acquire");
    CHECK(wl_columnar_relation_retirement_prepare(relation, &token) == EBUSY,
        "retirement refuses an active source reader");
    CHECK(atomic_load_explicit(&relation->descriptor_access.state,
        memory_order_acquire) == 0
        && atomic_load_explicit(&relation->source_access.state,
        memory_order_acquire) == 1,
        "partial retirement admission released descriptor writer");
    CHECK(relation->nrows == COL_REL_INIT_CAP
        && relation->columns[0][0] == value
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == baseline,
        "refused retirement preserves data and accounting");
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0,
        "retirement reader release");

    /* A damaged/non-releasable credit must be refused before payload teardown. */
    const void *saved_identity = relation->retained_reservation.identity;
    wl_columnar_memory_governor_t *saved_governor =
        relation->retained_reservation.governor;
    relation->retained_reservation.identity = NULL;
    CHECK(wl_columnar_relation_retirement_prepare(relation, &token) == EBUSY
        && relation->nrows == COL_REL_INIT_CAP
        && relation->retained_reserved_bytes > 0
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == baseline
        && atomic_load_explicit(&relation->descriptor_access.state,
        memory_order_acquire) == 0,
        "unreleasable reservation refuses retirement unchanged");
    relation->retained_reservation.identity = saved_identity;
    relation->retained_reservation.governor = NULL;
    CHECK(wl_columnar_relation_retirement_prepare(relation, &token) == EBUSY
        && relation->nrows == COL_REL_INIT_CAP
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == baseline,
        "reservation with mismatched governor refuses retirement unchanged");
    relation->retained_reservation.governor = saved_governor;

    CHECK(wl_columnar_relation_retirement_prepare(relation, &token) == 0,
        "retirement retry prepares");
    CHECK(atomic_load_explicit(&relation->descriptor_access.state,
        memory_order_acquire) == WL_COLUMNAR_SOURCE_ACCESS_WRITER
        && atomic_load_explicit(&relation->source_access.state,
        memory_order_acquire) == WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        "prepared retirement holds both writers");
    CHECK(wl_columnar_source_access_reader_acquire(&relation->source_access,
        &reader) == EBUSY
        && col_rel_append_row(relation, &value) != 0
        && relation->nrows == COL_REL_INIT_CAP,
        "prepared retirement blocks readers and relation mutation");
    copied_token = token;
    CHECK(wl_columnar_relation_retirement_cancel(&copied_token) == EINVAL
        && atomic_load_explicit(&relation->descriptor_access.state,
        memory_order_acquire) == WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        "copied retirement token cannot release original writers");
#ifndef _WIN32
    pid_t child = fork();
    CHECK(child >= 0, "fork copied-token commit witness");
    if (child == 0) {
        struct rlimit core_limit = { 0, 0 };
        (void)setrlimit(RLIMIT_CORE, &core_limit);
        fclose(stderr);
        copied_token = token;
        wl_columnar_relation_retirement_commit(&copied_token);
        _exit(0);
    }
    if (child > 0) {
        int status = 0;
        CHECK(waitpid(child, &status, 0) == child
            && WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT,
            "copied retirement token commit is rejected");
    }
#endif
    CHECK(wl_columnar_relation_retirement_cancel(&token) == 0,
        "prepared retirement cancels");
    CHECK(relation->nrows == COL_REL_INIT_CAP
        && relation->columns[0][0] == value
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == baseline,
        "cancel preserves relation contents and accounting");
    CHECK(wl_columnar_relation_retirement_cancel(&token) == 0,
        "retirement cancel is idempotent");

    CHECK(wl_columnar_relation_retirement_prepare(relation, &token) == 0,
        "retirement prepares for commit");
    wl_columnar_relation_retirement_commit(&token);
    relation = NULL;
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "retirement commit releases reservation after contents");

cleanup:
    col_rel_destroy(relation);
    if (reader.owner)
        (void)wl_columnar_source_access_reader_release(&reader);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_relation_retirement_rejects_borrowed_ownership(void)
{
    wl_columnar_relation_retirement_token_t token;
    col_rel_t *source = NULL;
    col_rel_t *view = NULL;
    delta_pool_t *pool = NULL;
    wl_arena_t *arena = NULL;
    col_rel_t *arena_relation = NULL;
    int64_t value = 7;

    wl_columnar_relation_retirement_init(&token);
    source = col_rel_new_auto("retirement-source", 1);
    view = col_rel_new_auto("retirement-view", 1);
    CHECK(source && view && col_rel_append_row(source, &value) == 0
        && col_rel_install_shared_view(view, source) == 0,
        "retirement alias setup");
    if (source && view) {
        uint64_t borrows = col_rel_storage_alias_borrow_count(source);
        CHECK(wl_columnar_relation_retirement_prepare(source, &token) == EBUSY
            && col_rel_storage_alias_borrow_count(source) == borrows
            && source->nrows == 1 && source->columns[0][0] == value
            && atomic_load_explicit(&source->source_access.state,
            memory_order_acquire) == 0,
            "retirement rejects a root with a live alias without mutation");
        CHECK(wl_columnar_relation_retirement_prepare(view, &token) == EBUSY
            && view->storage_owner == source && view->nrows == 1,
            "retirement rejects alias descriptors without mutation");
    }
    col_rel_destroy(view);
    view = NULL;
    col_rel_destroy(source);
    source = NULL;

    pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    col_rel_t *pool_relation = pool
        ? col_rel_pool_new_auto(pool, NULL, "retirement-pool", 1) : NULL;
    CHECK(pool_relation && pool_relation->pool_owned,
        "retirement pool fixture setup");
    if (pool_relation) {
        CHECK(wl_columnar_relation_retirement_prepare(pool_relation, &token)
            == EBUSY && pool_relation->pool_owned
            && atomic_load_explicit(&pool_relation->descriptor_access.state,
            memory_order_acquire) == 0,
            "retirement rejects pool ownership without gate mutation");
    }
    col_rel_destroy(pool_relation);
    delta_pool_destroy(pool);

    arena = wl_arena_create(4096);
    arena_relation = col_rel_new_auto("retirement-arena", 1);
    CHECK(arena && arena_relation, "retirement arena fixture setup");
    if (arena && arena_relation) {
        int64_t *arena_column = wl_arena_alloc(arena,
                COL_REL_INIT_CAP * sizeof(*arena_column));
        CHECK(arena_column != NULL, "retirement arena column allocation");
        if (arena_column) {
            memcpy(arena_column, arena_relation->columns[0],
                arena_relation->nrows * sizeof(*arena_column));
            free(arena_relation->columns[0]);
            arena_relation->columns[0] = arena_column;
            arena_relation->arena_owned = true;
            CHECK(wl_columnar_relation_retirement_prepare(arena_relation,
                &token) == EBUSY && arena_relation->arena_owned
                && atomic_load_explicit(
                    &arena_relation->descriptor_access.state,
                    memory_order_acquire) == 0,
                "retirement rejects arena ownership without gate mutation");
        }
    }
    col_rel_destroy(arena_relation);
    wl_arena_free(arena);
}

static void
test_governed_delta_restore_overwrite_order(void)
{
    wl_columnar_memory_resolution_t resolution;
    make_resolution(&resolution, 1024 * 1024);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    col_rel_t *rel = col_rel_new_auto("restore-overwrite", 1);
    int64_t old_value = 7;
    int64_t restored[100];
    CHECK(ref && rel, "overwrite fixture setup");
    if (!ref || !rel) goto cleanup;
    CHECK(col_rel_append_row(rel, &old_value) == 0,
        "overwrite seed row");
    CHECK(col_rel_attach_memory_governor(rel, ref) == 0
        && col_rel_reserve_capacity_admitted(rel, rel->capacity, NULL) == 0,
        "overwrite old storage admission");
    for (uint32_t i = 0; i < 100; i++) restored[i] = (int64_t)i + 100;
    overwrite_expected_peak = rel->retained_reserved_bytes
        + sizeof(restored);
    overwrite_expected_live = sizeof(restored);
    overwrite_retirement_witnesses = 0;
    wl_columnar_relation_test_after_retired_storage_free =
        observe_overwrite_retirement;
    CHECK(wl_columnar_relation_delta_restore_flat_overwrite(rel,
        rel->relation_identity, restored, 100, 1) == 0,
        "governed delta overwrite succeeds");
    wl_columnar_relation_test_after_retired_storage_free = NULL;
    CHECK(overwrite_retirement_witnesses == 1
        && rel->capacity == 100 && rel->nrows == 100
        && rel->columns[0][0] == 100 && rel->columns[0][99] == 199
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == sizeof(restored) + charged_fixed_bytes(rel),
        "overwrite installs complete rows and releases retired charge once");
cleanup:
    wl_columnar_relation_test_after_retired_storage_free = NULL;
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "overwrite reservation teardown balanced");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_resize_allocation_cleanup_precedes_rollback(void)
{
    wl_columnar_memory_resolution_t resolution;
    make_resolution(&resolution, 1024 * 1024);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    col_rel_t *rel = col_rel_new_auto("resize-rollback-order", 1);
    int64_t value = 11;
    CHECK(ref && rel, "resize rollback fixture setup");
    if (!ref || !rel) goto cleanup;
    CHECK(col_rel_append_row(rel, &value) == 0
        && col_rel_attach_memory_governor(rel, ref) == 0
        && col_rel_reserve_capacity_admitted(rel, rel->capacity, NULL) == 0,
        "resize rollback fixture admitted");
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    wl_columnar_relation_test_watch_next_rollback_cleanup();
    wl_columnar_relation_test_fail_next_reservation_commit();
    CHECK(col_rel_reserve_capacity_admitted(rel, rel->capacity + 1, NULL)
        == ENOMEM,
        "injected reservation publication refusal after resize allocation");
    CHECK(wl_columnar_relation_test_rollback_cleanup_was_ordered()
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == baseline
        && rel->capacity == COL_REL_INIT_CAP && rel->nrows == 1
        && rel->columns[0][0] == value,
        "private columns retire before pending rollback restores credit");
cleanup:
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "resize rollback fixture reservation balanced");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_cow_private_cleanup_precedes_rollback(void)
{
    wl_columnar_memory_resolution_t resolution;
    make_resolution(&resolution, 1024 * 1024);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    col_rel_t *source = NULL;
    col_rel_t *view = ref ? make_shared_view(ref, &source) : NULL;
    CHECK(ref && source && view, "COW rollback fixture setup");
    if (!ref || !source || !view) goto cleanup;
    wl_columnar_relation_test_watch_next_rollback_cleanup();
    wl_columnar_relation_test_fail_next_reservation_commit();
    CHECK(col_rel_cow_unshare(view, 0) == ENOMEM,
        "injected COW publication refusal");
    CHECK(wl_columnar_relation_test_rollback_cleanup_was_ordered()
        && view->col_shared && view->col_shared[0]
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == charged_fixed_bytes(view),
        "private COW column retires before pending rollback");
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "COW rollback fixture reservation balanced");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

#ifndef _WIN32
static void
test_terminal_release_policy_child(bool retired_release)
{
    pid_t child = fork();
    CHECK(child >= 0, "terminal release test fork");
    if (child < 0) return;
    if (child == 0) {
        wl_columnar_memory_resolution_t resolution;
        make_resolution(&resolution, 1024 * 1024);
        wl_columnar_memory_governor_ref_t *ref =
            wl_columnar_memory_governor_ref_create(&resolution);
        col_rel_t *rel = col_rel_new_auto("terminal-release", 1);
        int64_t value = 3;
        if (!ref || !rel || col_rel_append_row(rel, &value) != 0
            || col_rel_attach_memory_governor(rel, ref) != 0
            || col_rel_reserve_capacity_admitted(rel, rel->capacity,
            NULL) != 0)
            _exit(10);
        if (retired_release) {
            int64_t rows[100];
            for (uint32_t i = 0; i < 100; i++) rows[i] = i;
            wl_columnar_memory_governor_test_refuse_next_release();
            (void)wl_columnar_relation_delta_restore_flat_overwrite(rel,
                rel->relation_identity, rows, 100, 1);
        } else {
            wl_columnar_relation_test_fail_next_prepare_resize();
            wl_columnar_memory_governor_test_refuse_next_release();
            (void)col_rel_reserve_capacity_admitted(rel,
                rel->capacity * 2, NULL);
        }
        _exit(0);
    }
    int status = 0;
    CHECK(waitpid(child, &status, 0) == child
        && WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT,
        retired_release
            ? "retired release refusal follows terminal policy"
            : "pending rollback refusal follows terminal policy");
}

static void
test_terminal_release_policy(void)
{
    test_terminal_release_policy_child(false);
    test_terminal_release_policy_child(true);
}
#endif

static void
test_heap_descriptor_admission(void)
{
    const char *name = "descriptor-admission";
    uint64_t exact = heap_descriptor_bytes(name);
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *relation = NULL;

    make_resolution(&resolution, exact - 1u);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref != NULL, "descriptor governor setup");
    if (!ref)
        return;
    CHECK(wl_columnar_relation_alloc_governed(&relation, name, ref)
        == ENOSPC && !relation
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "denied descriptor constructor leaves no owned bytes");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, exact, memory_order_release);
    CHECK(wl_columnar_relation_alloc_governed(&relation, name, ref) == 0
        && relation && relation->descriptor_reservation
        && relation->descriptor_reservation->bytes == exact
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == exact,
        "exact descriptor constructor retains charge");
    col_rel_destroy(relation);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "descriptor constructor releases after destruction");

    relation = NULL;
    CHECK(col_rel_alloc(&relation, name) == 0 && relation,
        "descriptor adoption fixture");
    if (relation) {
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                ref)->usable_bytes, exact - 1u, memory_order_release);
        CHECK(col_rel_attach_memory_governor(relation, ref) == ENOSPC
            && relation->memory_governor == NULL
            && relation->descriptor_reservation == NULL
            && relation->memory_budget_denial_pending,
            "denied descriptor adoption preserves detached owner");
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                ref)->usable_bytes, exact, memory_order_release);
        CHECK(col_rel_attach_memory_governor(relation, ref) == 0
            && relation->descriptor_reservation
            && relation->descriptor_reservation->bytes == exact,
            "descriptor adoption can retry");
        col_rel_destroy(relation);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "descriptor adoption releases after destruction");
    }
    relation = NULL;
    CHECK(col_rel_alloc(&relation, "unnamed") == 0 && relation,
        "unnamed adoption fixture");
    if (relation) {
        free(relation->name);
        relation->name = NULL;
        uint64_t unnamed_bytes = sizeof(col_rel_t)
            + sizeof(wl_columnar_memory_reservation_t);
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                ref)->usable_bytes, unnamed_bytes, memory_order_release);
        CHECK(col_rel_attach_memory_governor(relation, ref) == 0
            && relation->descriptor_reservation
            && relation->descriptor_reservation->bytes == unnamed_bytes,
            "unnamed descriptor adoption charges only owned bytes");
        col_rel_destroy(relation);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "unnamed descriptor release");
    }
    wl_columnar_memory_governor_ref_release(ref);
}

static void
test_governed_clear_preserves_live_descriptor_charge(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *relation;
    int64_t value = 37;

    make_resolution(&resolution, UINT64_C(1) << 20);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    relation = ref ? col_rel_new_auto("clear-reuse", 1) : NULL;
    CHECK(ref && relation, "governed clear fixture");
    if (!ref || !relation) {
        if (relation)
            col_rel_destroy(relation);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        return;
    }
    CHECK(col_rel_attach_memory_governor(relation, ref) == 0
        && col_rel_append_row(relation, &value) == 0
        && col_rel_reserve_capacity_admitted(relation,
        relation->capacity + 64, NULL) == 0,
        "governed clear fixture admission");
    uint64_t descriptor_bytes = relation->descriptor_reservation
        ? relation->descriptor_reservation->bytes : 0;
    uint64_t reserved_with_contents = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    int64_t *contents = relation->columns[0];
    CHECK(relation->retained_reserved_bytes > 0
        && reserved_with_contents > descriptor_bytes,
        "governed clear fixture has retained storage charge");
    CHECK(col_rel_storage_alias_borrow_acquire(relation) == 0,
        "governed clear fixture alias borrow");
    col_rel_free_contents(relation);
    CHECK(relation->columns[0] == contents && relation->nrows == 1
        && col_rel_storage_alias_borrow_count(relation) == 1
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == reserved_with_contents,
        "clear with live alias leaves contents and accounting untouched");
    CHECK(col_rel_storage_alias_borrow_release(relation) == 0,
        "governed clear fixture alias retirement");
    col_rel_free_contents(relation);
    CHECK(relation->memory_governor == ref
        && relation->descriptor_reservation != NULL
        && relation->metadata_reservation == NULL
        && relation->retained_reserved_bytes == 0
        && relation->nrows == 0 && relation->storage_owner == relation
        && wl_columnar_relation_accounting_complete(relation, ref)
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == descriptor_bytes,
        "clear keeps live descriptor charge and resets owned contents");
    CHECK(col_rel_destroy_checked(relation) == 0
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "checked destroy releases cleared descriptor after free");
    wl_columnar_memory_governor_ref_release(ref);
}

static void
test_relation_accounting_aux_reservation(void)
{
    col_rel_t *relation;
    const uint64_t aux_bytes = 64;
    relation = col_rel_new_auto("unaccounted-aux", 1);
    CHECK(relation, "ungoverned aux accounting fixture");
    if (relation) {
        CHECK(wl_columnar_relation_accounting_complete(relation, NULL),
            "ungoverned accounting accepts empty aux state");
        atomic_store_explicit(&relation->aux_reservation.state,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, memory_order_release);
        CHECK(!wl_columnar_relation_accounting_complete(relation, NULL),
            "ungoverned accounting rejects stale committed aux token");
        atomic_store_explicit(&relation->aux_reservation.state,
            WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_release);
        relation->aux_reserved_bytes = aux_bytes;
        CHECK(!wl_columnar_relation_accounting_complete(relation, NULL),
            "ungoverned accounting rejects aux charge");
    }
    col_rel_destroy(relation);
}

static void
test_schema_metadata_adoption_and_replacement(void)
{
    const char *name = "metadata-adopt";
    const uint64_t descriptor = heap_descriptor_bytes(name);
    const uint64_t metadata = auto_metadata_bytes(2);
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *relation = col_rel_new_auto(name, 2);
    make_resolution(&resolution, descriptor + metadata - 1u);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(relation && ref, "metadata adoption fixture");
    if (!relation || !ref)
        goto cleanup;
    CHECK(col_rel_attach_memory_governor(relation, ref) == ENOSPC
        && !relation->memory_governor
        && !relation->descriptor_reservation
        && !relation->metadata_reservation
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "metadata denial leaves whole relation unattached");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, descriptor + metadata,
        memory_order_release);
    CHECK(col_rel_attach_memory_governor(relation, ref) == 0
        && relation->metadata_reservation
        && relation->metadata_reservation->bytes == metadata
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor + metadata,
        "metadata adoption exact fit");

    const uint64_t typed = metadata
        + 2u * sizeof(wirelog_column_type_t);
    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT
    };
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, descriptor + metadata + typed - 1u,
        memory_order_release);
    CHECK(col_rel_set_column_types(relation, types, 2) == ENOSPC
        && !relation->column_types
        && relation->metadata_reservation->bytes == metadata
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor + metadata,
        "one-byte type replacement denial rolls back schema and token");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, descriptor + metadata + typed,
        memory_order_release);
    CHECK(col_rel_set_column_types(relation, types, 2) == 0
        && relation->column_types
        && relation->column_types[1] == WIRELOG_TYPE_FLOAT
        && relation->metadata_reservation->bytes == typed
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor + typed,
        "type replacement admits staged old and new metadata exactly");
    wirelog_column_type_t next_types[] = {
        WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_INT64
    };
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, descriptor + 2u * typed - 1u,
        memory_order_release);
    CHECK(col_rel_set_column_types(relation, next_types, 2) == ENOSPC
        && relation->column_types[0] == WIRELOG_TYPE_INT64
        && relation->column_types[1] == WIRELOG_TYPE_FLOAT
        && relation->metadata_reservation->bytes == typed
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor + typed,
        "same-shape type update keeps old image on overlap denial");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, descriptor + 2u * typed,
        memory_order_release);
    CHECK(col_rel_set_column_types(relation, next_types, 2) == 0
        && relation->column_types[0] == WIRELOG_TYPE_FLOAT
        && relation->metadata_reservation->bytes == typed
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor + typed,
        "same-shape type update succeeds at staged peak and retires old");
cleanup:
    col_rel_destroy(relation);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "metadata adoption releases after destruction");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_schema_metadata_first_allocation(void)
{
    const char *name = "metadata-first";
    const uint64_t descriptor = heap_descriptor_bytes(name);
    const uint64_t metadata = auto_metadata_bytes(2);
    const uint64_t columns = 2u * COL_REL_INIT_CAP * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *relation = NULL;
    make_resolution(&resolution, descriptor + metadata - 1u);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && wl_columnar_relation_alloc_governed(&relation,
        name, ref) == 0, "governed schema allocation fixture");
    if (!ref || !relation)
        goto cleanup;
    CHECK(col_rel_set_schema(relation, 2, NULL) == ENOSPC
        && relation->ncols == 0 && !relation->col_names
        && !relation->schema_ok && !relation->metadata_reservation
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == descriptor,
        "schema name and Arrow denial precedes allocation");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, descriptor + metadata + columns,
        memory_order_release);
    CHECK(col_rel_set_schema(relation, 2, NULL) == 0
        && relation->metadata_reservation
        && relation->metadata_reservation->bytes == metadata
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor + metadata + columns,
        "schema metadata and columns admit exact fit on retry");
cleanup:
    col_rel_destroy(relation);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_nested_arrow_metadata_adoption(void)
{
    const char *name = "nested-arrow";
    const uint64_t arrow_bytes = 3u
        + 2u * (sizeof(struct ArrowSchema *)
        + sizeof(struct ArrowSchema))
        + 3u + sizeof("nested") + 2u + sizeof("value");
    const uint64_t metadata = arrow_bytes
        + sizeof(wl_columnar_memory_reservation_t);
    const uint64_t descriptor = heap_descriptor_bytes(name);
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *relation = NULL;
    make_resolution(&resolution, descriptor + metadata);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_alloc(&relation, name) == 0,
        "nested Arrow metadata fixture");
    if (!ref || !relation)
        goto cleanup;
    ArrowSchemaInit(&relation->schema);
    CHECK(ArrowSchemaSetTypeStruct(&relation->schema, 1) == NANOARROW_OK
        && ArrowSchemaSetTypeStruct(relation->schema.children[0], 1)
        == NANOARROW_OK
        && ArrowSchemaSetName(relation->schema.children[0], "nested")
        == NANOARROW_OK
        && ArrowSchemaInitFromType(
            relation->schema.children[0]->children[0],
            NANOARROW_TYPE_INT64) == NANOARROW_OK
        && ArrowSchemaSetName(relation->schema.children[0]->children[0],
        "value") == NANOARROW_OK,
        "nested Arrow schema construction");
    relation->schema_ok = true;
    CHECK(col_rel_attach_memory_governor(relation, ref) == 0
        && relation->metadata_reservation
        && relation->metadata_reservation->bytes == metadata
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor + metadata,
        "nested Arrow child arrays and names are fully adopted");
cleanup:
    col_rel_destroy(relation);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_pool_metadata_name_reuse(void)
{
    const char *name = "pool-metadata";
    const uint64_t exact = pool_auto_bytes(name, 1);
    wl_columnar_memory_resolution_t resolution;
    make_resolution(&resolution, exact);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    CHECK(ref && pool, "pool metadata fixture");
    if (!ref || !pool)
        goto cleanup;
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        col_rel_t *relation = col_rel_pool_new_auto(pool, NULL, name, 1);
        CHECK(relation && relation->pool_owned, "pool metadata slot");
        if (!relation)
            break;
        CHECK(col_rel_attach_memory_governor(relation, ref) == 0
            && !relation->descriptor_reservation
            && relation->metadata_reservation
            && relation->metadata_reservation->bytes == exact
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == exact,
            "pool name and schema charged without slab double count");
        col_rel_free_contents(relation);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "pool metadata released before slot reuse");
        delta_pool_reset(pool);
    }
    col_rel_t *source = col_rel_new_auto("pool-source", 1);
    uint64_t name_only = strlen(name) + 1u
        + sizeof(wl_columnar_memory_reservation_t);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, name_only - 1u, memory_order_release);
    col_rel_t *denied = source
        ? wl_columnar_relation_pool_new_like_governed(pool, name,
            source, ref) : NULL;
    CHECK(source && !denied && pool->slot_used == 0u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "pool name denial restores slot and releases fallback charge");
    col_rel_destroy(denied);
    col_rel_destroy(source);
cleanup:
    delta_pool_destroy(pool);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_compound_map_admission_and_bounds(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *relation = col_rel_new_auto("map-admission", 2);
    col_rel_t *copy = NULL;
    const col_rel_logical_col_t wide = {
        WIRELOG_COMPOUND_KIND_INLINE, 2u, 0u
    };
    uint64_t before;
    uint64_t new_metadata;
    uint64_t clone_peak;
    uint32_t *old_map;
    uint64_t old_view;

    CHECK(relation != NULL, "compound map fixture");
    if (!relation)
        return;
    make_resolution(&resolution, UINT64_MAX);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref != NULL, "compound map governor");
    if (!ref) {
        col_rel_destroy(relation);
        return;
    }
    CHECK(col_rel_attach_memory_governor(relation, ref) == 0,
        "compound map attach");
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    new_metadata = relation->metadata_reservation->bytes
        + sizeof(uint32_t);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + new_metadata - 1u,
        memory_order_release);
    old_view = relation->view_generation;
    CHECK(col_rel_apply_compound_schema(relation, &wide, 1u) == ENOSPC
        && !relation->compound_arity_map
        && relation->view_generation == old_view
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "one-byte-short map admission leaves relation unchanged");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + new_metadata,
        memory_order_release);
    CHECK(col_rel_apply_compound_schema(relation, &wide, 1u) == 0
        && relation->compound_arity_len == 1u
        && relation->metadata_reservation->bytes == new_metadata,
        "exact-fit map admission charges logical allocation");
    old_map = relation->compound_arity_map;
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    old_view = relation->view_generation;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + new_metadata - 1u,
        memory_order_release);
    CHECK(col_rel_apply_compound_schema(relation, &wide, 1u) == ENOSPC
        && relation->compound_arity_map == old_map
        && relation->view_generation == old_view
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "map replacement denial retains old token and buffer");

    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, UINT64_MAX, memory_order_release);
    relation->compound_arity_len = 0;
    CHECK(wl_columnar_relation_new_like_governed_checked("bad-map",
        relation, ref, &copy) == EINVAL && !copy,
        "zero map length is rejected before clone read");
    relation->compound_arity_len = 1;
    relation->compound_arity_map[0] = 0;
    CHECK(wl_columnar_relation_new_like_governed_checked("bad-map",
        relation, ref, &copy) == EINVAL && !copy,
        "zero arity is rejected before clone read");
    relation->compound_arity_map[0] = 3;
    CHECK(wl_columnar_relation_new_like_governed_checked("bad-map",
        relation, ref, &copy) == EINVAL && !copy,
        "oversized arity is rejected before clone read");
    relation->compound_arity_map[0] = 1;
    CHECK(wl_columnar_relation_new_like_governed_checked("bad-map",
        relation, ref, &copy) == EINVAL && !copy,
        "arity sum mismatch is rejected before clone read");
    relation->compound_arity_map[0] = 2;
    clone_peak = before + heap_descriptor_bytes("clone-map")
        + (uint64_t)relation->ncols * relation->capacity * sizeof(int64_t)
        + auto_metadata_bytes(2) + new_metadata;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, clone_peak - 1u, memory_order_release);
    CHECK(wl_columnar_relation_new_like_governed_checked("clone-map",
        relation, ref, &copy) == ENOSPC && !copy,
        "checked heap clone preserves one-byte map admission denial");
    CHECK(wl_columnar_relation_pool_new_like_governed_checked(NULL,
        "clone-map", relation, ref, &copy) == ENOSPC && !copy,
        "checked pool fallback preserves one-byte map admission denial");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, clone_peak, memory_order_release);
    CHECK(wl_columnar_relation_new_like_governed_checked("clone-map",
        relation, ref, &copy) == 0 && copy
        && copy->compound_arity_len == 1u
        && copy->compound_arity_map[0] == 2u
        && copy->metadata_reservation
        && copy->metadata_reservation->bytes == new_metadata,
        "checked heap clone charges exact compound map metadata");
    col_rel_destroy(copy);
    copy = NULL;
    col_rel_destroy(relation);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "compound map tokens released");
    wl_columnar_memory_governor_ref_release(ref);
}

static void
test_shared_view_metadata_admission(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = col_rel_new_auto("shared-meta-source", 2);
    col_rel_t *view = col_rel_new_auto("shared-meta-view", 2);
    const col_rel_logical_col_t wide = {
        WIRELOG_COMPOUND_KIND_INLINE, 2u, 0u
    };
    uint64_t before;
    uint64_t old_metadata;
    uint64_t next_metadata = auto_metadata_bytes(2) + sizeof(uint32_t);
    uint64_t next_table = shared_view_table_bytes(2);
    uint64_t view_generation;
    int64_t **old_columns;
    wl_columnar_memory_reservation_t *old_token;

    CHECK(source && view, "shared metadata fixtures");
    if (!source || !view)
        goto cleanup;
    CHECK(col_rel_apply_compound_schema(source, &wide, 1u) == 0,
        "shared source compound schema");
    make_resolution(&resolution, UINT64_MAX);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(view, ref) == 0,
        "shared destination governor");
    if (!ref || !view->memory_governor)
        goto cleanup_ref;
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    view_generation = view->view_generation;
    old_columns = view->columns;
    old_token = view->metadata_reservation;
    old_metadata = old_token->bytes;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + next_metadata + next_table - 1u,
        memory_order_release);
    CHECK(col_rel_install_shared_view(view, source) == ENOSPC
        && view->columns == old_columns
        && view->metadata_reservation == old_token
        && view->view_generation == view_generation
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "shared view one-byte denial preserves destination");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + next_metadata + next_table,
        memory_order_release);
    CHECK(col_rel_install_shared_view(view, source) == 0
        && view->compound_arity_len == 1u
        && view->compound_arity_map[0] == 2u
        && view->metadata_reservation->bytes == next_metadata
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before
        - old_metadata + next_metadata + next_table,
        "shared view exact-fit retry publishes copied map metadata");
cleanup_ref:
    col_rel_destroy(view);
    view = NULL;
    col_rel_destroy(source);
    source = NULL;
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "shared metadata token cleanup");
        wl_columnar_memory_governor_ref_release(ref);
    }
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(source);
}

static void
test_shared_view_table_lifetime(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *source = col_rel_new_auto("table-source", 1);
    col_rel_t *view = col_rel_new_auto("table-view", 1);
    wl_columnar_memory_governor_ref_t *ref = NULL;
    uint64_t before;
    int64_t **old_columns;
    bool *old_flags;

    make_resolution(&resolution, UINT64_MAX);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(source && view && ref
        && col_rel_attach_memory_governor(view, ref) == 0,
        "shared table fixture");
    if (!source || !view || !ref || !view->memory_governor)
        goto cleanup;
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        before + auto_metadata_bytes(1)
        + shared_view_table_bytes(1) - 1u, memory_order_release);
    old_columns = view->columns;
    CHECK(col_rel_install_shared_view(view, source) == ENOSPC
        && view->columns == old_columns
        && view->shared_table_reservation == NULL
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "shared pointer table one-byte denial is atomic");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        before + auto_metadata_bytes(1)
        + shared_view_table_bytes(1), memory_order_release);
    CHECK(col_rel_install_shared_view(view, source) == 0
        && view->shared_table_reservation
        && view->shared_table_reservation->bytes
        == shared_view_table_bytes(1),
        "shared pointer table exact-fit retry");
    if (!view->shared_table_reservation)
        goto cleanup;
    CHECK(wl_columnar_relation_accounting_complete(view, ref),
        "complete accounting accepts governed shared pointer table");
    wl_columnar_memory_reservation_t *table_token
        = view->shared_table_reservation;
    view->shared_table_reservation = NULL;
    CHECK(!wl_columnar_relation_accounting_complete(view, ref),
        "complete accounting rejects missing shared-table token");
    view->shared_table_reservation = table_token;
    table_token->governor = NULL;
    CHECK(!wl_columnar_relation_accounting_complete(view, ref),
        "complete accounting rejects wrong shared-table governor");
    table_token->governor = wl_columnar_memory_governor_ref_get(ref);
    table_token->bytes--;
    CHECK(!wl_columnar_relation_accounting_complete(view, ref),
        "complete accounting rejects undersized shared-table token");
    table_token->bytes++;
    uint64_t table_state = atomic_load_explicit(&table_token->state,
            memory_order_relaxed);
    atomic_store_explicit(&table_token->state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_release);
    CHECK(!wl_columnar_relation_accounting_complete(view, ref),
        "complete accounting rejects uncommitted shared-table token");
    atomic_store_explicit(&table_token->state, table_state,
        memory_order_release);
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    wl_columnar_memory_reservation_t *first_token
        = view->shared_table_reservation;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        before + auto_metadata_bytes(1) + shared_view_table_bytes(1),
        memory_order_release);
    CHECK(col_rel_install_shared_view(view, source) == 0
        && view->shared_table_reservation != first_token
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "shared view replacement retires prior table token");
    old_columns = view->columns;
    old_flags = view->col_shared;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        before + COL_REL_INIT_CAP * sizeof(int64_t) - 1u,
        memory_order_release);
    CHECK(col_rel_cow_unshare(view, 0) == ENOMEM
        && view->columns == old_columns && view->col_shared == old_flags
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "shared table COW denial preserves token and buffers");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        before + COL_REL_INIT_CAP * sizeof(int64_t),
        memory_order_release);
    CHECK(col_rel_cow_unshare(view, 0) == 0
        && view->columns == old_columns && view->col_shared == NULL
        && view->shared_table_reservation->bytes
        == replacement_table_bytes(1)
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == before + COL_REL_INIT_CAP * sizeof(int64_t)
        - sizeof(bool),
        "COW retains pointer table and credits freed flags");
    CHECK(wl_columnar_relation_accounting_complete(view, ref),
        "complete accounting accepts compact shared-table token after COW");
    view->shared_table_reservation->bytes--;
    CHECK(!wl_columnar_relation_accounting_complete(view, ref),
        "complete accounting checks reduced shared-table token size");
    view->shared_table_reservation->bytes++;
    CHECK(col_rel_compact(view) == 0
        && view->columns == NULL
        && view->shared_table_reservation == NULL
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == charged_fixed_bytes(view),
        "empty compaction releases shared table token");
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "shared table teardown returns all credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_attach_existing_shared_table(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *source = col_rel_new_auto("attach-table-source", 1);
    col_rel_t *view = col_rel_new_auto("attach-table-view", 1);
    wl_columnar_memory_governor_ref_t *ref = NULL;
    uint64_t exact = heap_auto_bytes("attach-table-view", 1)
        + shared_view_table_bytes(1);

    CHECK(source && view
        && col_rel_install_shared_view(view, source) == 0,
        "ungoverned shared table fixture");
    if (!source || !view || !view->col_shared)
        goto cleanup;
    CHECK(wl_columnar_relation_accounting_complete(view, NULL),
        "unmanaged shared table needs no governed token");
    make_resolution(&resolution, exact - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(view, ref) == ENOSPC
        && view->memory_governor == NULL
        && view->shared_table_reservation == NULL
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "preexisting shared table attach one-byte denial");
    if (!ref)
        goto cleanup;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, exact, memory_order_release);
    CHECK(col_rel_attach_memory_governor(view, ref) == 0
        && view->shared_table_reservation
        && view->shared_table_reservation->bytes
        == shared_view_table_bytes(1)
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == exact,
        "preexisting shared table attach exact fit");
    CHECK(wl_columnar_relation_accounting_complete(view, ref),
        "attached shared table has complete governed accounting");
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "preexisting shared table attach teardown");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_append_type_metadata_admission(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *source = col_rel_new_auto("typed-source", 1);
    col_rel_t *dst = col_rel_new_auto("typed-destination", 1);
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    int64_t value = 17;
    uint64_t before;
    uint64_t new_metadata;
    wl_columnar_memory_reservation_t *old_token;
    struct ArrowSchema *old_child;

    CHECK(source && dst && col_rel_set_column_types(source, &type, 1u) == 0
        && col_rel_append_row(source, &value) == 0,
        "typed append metadata fixture");
    if (!source || !dst)
        goto cleanup;
    make_resolution(&resolution, UINT64_MAX);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(dst, ref) == 0,
        "typed append governor");
    if (!ref || !dst->memory_governor)
        goto cleanup;
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    old_token = dst->metadata_reservation;
    old_child = dst->schema.children[0];
    new_metadata = old_token->bytes + sizeof(type);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + new_metadata - 1u,
        memory_order_release);
    CHECK(col_rel_append_all(dst, source, NULL) == ENOSPC
        && !dst->column_types && dst->nrows == 0
        && dst->schema.children[0] == old_child
        && dst->metadata_reservation == old_token
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "typed append denial preserves schema and token");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + new_metadata,
        memory_order_release);
    CHECK(col_rel_append_all(dst, source, NULL) == 0
        && dst->column_types && dst->nrows == 1
        && dst->columns[0][0] == value
        && dst->metadata_reservation->bytes == new_metadata,
        "typed append exact-fit retry publishes metadata");
cleanup:
    col_rel_destroy(dst);
    col_rel_destroy(source);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_rename_metadata_admission(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *heap = col_rel_new_auto("rename-old", 1);
    delta_pool_t *pool = NULL;
    col_rel_t *pooled = NULL;
    uint64_t before;
    uint64_t old_charge;
    uint64_t new_charge;
    char *old_name;

    CHECK(heap != NULL, "heap rename fixture");
    if (!heap)
        return;
    make_resolution(&resolution, UINT64_MAX);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(heap, ref) == 0,
        "heap rename governor");
    if (!ref || !heap->memory_governor)
        goto cleanup;
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    old_charge = heap->descriptor_reservation->bytes;
    new_charge = heap_descriptor_bytes("rename-longer");
    old_name = heap->name;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + new_charge - 1u,
        memory_order_release);
    CHECK(wl_columnar_relation_rename_checked(heap, "rename-longer")
        == ENOSPC && heap->name == old_name
        && heap->descriptor_reservation->bytes == old_charge
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "heap rename denial retains descriptor name and token");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + new_charge,
        memory_order_release);
    CHECK(wl_columnar_relation_rename_checked(heap, "rename-longer") == 0
        && strcmp(heap->name, "rename-longer") == 0
        && heap->descriptor_reservation->bytes == new_charge
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == before - old_charge + new_charge,
        "heap rename exact-fit retry transfers descriptor token");
    col_rel_destroy(heap);
    heap = NULL;
    pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    pooled = pool ? col_rel_pool_new_auto(pool, NULL, "pool-old", 1) : NULL;
    CHECK(pooled && pooled->pool_owned, "pooled rename fixture");
    if (!pooled)
        goto cleanup;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, UINT64_MAX, memory_order_release);
    CHECK(col_rel_attach_memory_governor(pooled, ref) == 0,
        "pooled rename governor");
    if (!pooled->memory_governor)
        goto cleanup;
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    old_charge = pooled->metadata_reservation->bytes;
    new_charge = old_charge - strlen("pool-old") - 1u
        + strlen("pool-longer") + 1u;
    old_name = pooled->name;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + new_charge - 1u,
        memory_order_release);
    CHECK(wl_columnar_relation_rename_checked(pooled, "pool-longer")
        == ENOSPC && pooled->name == old_name
        && pooled->metadata_reservation->bytes == old_charge,
        "pooled rename denial retains name and metadata token");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + new_charge,
        memory_order_release);
    CHECK(wl_columnar_relation_rename_checked(pooled, "pool-longer") == 0
        && strcmp(pooled->name, "pool-longer") == 0
        && pooled->metadata_reservation->bytes == new_charge
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == new_charge,
        "pooled rename exact-fit retry transfers metadata token");
cleanup:
    col_rel_destroy(heap);
    if (pooled)
        col_rel_free_contents(pooled);
    delta_pool_destroy(pool);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "rename tokens released");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_replacement_metadata_admission(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *dst = col_rel_new_auto("replacement-target", 2);
    col_rel_t *candidate = col_rel_new_auto("replacement-source", 2);
    col_rel_replacement_t replacement = { 0 };
    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64
    };
    const col_rel_logical_col_t wide = {
        WIRELOG_COMPOUND_KIND_INLINE, 2u, 0u
    };
    int64_t row[] = { 41, 42 };
    uint64_t before;
    uint64_t data_bytes;
    uint64_t metadata_bytes;
    uint64_t scratch_bytes;
    uint64_t view_before;
    int64_t **old_columns;
    wl_columnar_memory_reservation_t *old_token;

    CHECK(dst && candidate && col_rel_set_column_types(candidate, types,
        2u) == 0 && col_rel_apply_compound_schema(candidate, &wide, 1u)
        == 0 && col_rel_append_row(candidate, row) == 0,
        "replacement metadata fixture");
    if (!dst || !candidate || !candidate->compound_arity_map)
        goto cleanup;
    make_resolution(&resolution, UINT64_MAX);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(dst, ref) == 0,
        "replacement metadata governor");
    if (!ref || !dst->memory_governor)
        goto cleanup;
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    data_bytes = (uint64_t)candidate->ncols * candidate->capacity
        * sizeof(int64_t);
    metadata_bytes = auto_metadata_bytes(2) + sizeof(types)
        + sizeof(uint32_t);
    scratch_bytes = sizeof(col_rel_t) + strlen(candidate->name) + 1u;
    view_before = dst->view_generation;
    old_columns = dst->columns;
    old_token = dst->metadata_reservation;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + data_bytes + metadata_bytes - 1u,
        memory_order_release);
    CHECK(col_rel_prepare_replacement(dst, candidate, &replacement)
        == ENOSPC && !replacement.staged && !replacement.writer_acquired
        && dst->columns == old_columns
        && dst->metadata_reservation == old_token
        && dst->view_generation == view_before
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "replacement metadata denial rolls back all reservations");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + data_bytes + metadata_bytes
        + scratch_bytes - 1u, memory_order_release);
    CHECK(col_rel_prepare_replacement(dst, candidate, &replacement)
        == ENOSPC && !replacement.staged
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "replacement scratch descriptor denial rolls back metadata");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, before + data_bytes + metadata_bytes
        + scratch_bytes, memory_order_release);
    CHECK(col_rel_prepare_replacement(dst, candidate, &replacement) == 0
        && replacement.staged && replacement.metadata_owned
        && replacement.metadata_pending.bytes == metadata_bytes
        && replacement.staged_descriptor_reserved
        && replacement.staged_descriptor_reservation.bytes == scratch_bytes
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == before + data_bytes + metadata_bytes + scratch_bytes,
        "replacement exact-fit preparation retains all staged credits");
    col_rel_discard_replacement(&replacement);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before
        && dst->columns == old_columns && dst->view_generation == view_before,
        "replacement discard releases staged metadata and scratch");
    CHECK(col_rel_prepare_replacement(dst, candidate, &replacement) == 0,
        "replacement exact-fit retry");
    if (replacement.staged) {
        col_rel_commit_replacement_locked(dst, &replacement);
        CHECK(dst->columns != old_columns && dst->columns[0][0] == row[0]
            && dst->compound_arity_len == 1u
            && dst->compound_arity_map[0] == 2u
            && dst->column_types[0] == WIRELOG_TYPE_INT64
            && dst->metadata_reservation
            && dst->metadata_reservation->bytes == metadata_bytes
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == dst->descriptor_reservation->bytes + metadata_bytes
            + data_bytes,
            "replacement commit transfers exact metadata token");
    }
cleanup:
    col_rel_discard_replacement(&replacement);
    col_rel_destroy(dst);
    col_rel_destroy(candidate);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "replacement metadata cleanup releases all credits");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_replacement_aux_admission(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *dst = col_rel_new_auto("aux-target", 2);
    col_rel_t *candidate = col_rel_new_auto("aux-source", 2);
    col_rel_replacement_t replacement = { 0 };
    uint64_t before;
    uint64_t data_bytes;
    uint64_t metadata_bytes;
    uint64_t aux_bytes;
    uint64_t scratch_bytes;
    int64_t **old_columns;

    CHECK(dst && candidate, "replacement aux fixture");
    if (!dst || !candidate)
        goto cleanup;
    candidate->merge_buf_cap = 5;
    candidate->merge_columns = col_columns_alloc(2, 5);
    candidate->retract_backup_capacity = 3;
    candidate->retract_backup_nrows = 1;
    candidate->retract_backup_columns = col_columns_alloc(2, 3);
    candidate->dedup_cap = 8;
    candidate->dedup_slots = calloc(8, sizeof(uint64_t));
    CHECK(candidate->merge_columns && candidate->retract_backup_columns
        && candidate->dedup_slots, "replacement aux buffers");
    if (!candidate->merge_columns || !candidate->retract_backup_columns
        || !candidate->dedup_slots)
        goto cleanup;
    candidate->merge_columns[0][0] = 31;
    candidate->retract_backup_columns[1][0] = 32;
    candidate->dedup_slots[0] = 33;
    candidate->dedup_count = 1;
    make_resolution(&resolution, UINT64_MAX);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(dst, ref) == 0,
        "replacement aux governor");
    if (!ref || !dst->memory_governor)
        goto cleanup;
    before = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    old_columns = dst->columns;
    data_bytes = (uint64_t)candidate->ncols * candidate->capacity
        * sizeof(int64_t);
    metadata_bytes = auto_metadata_bytes(2);
    aux_bytes = 2u * (sizeof(int64_t *) + 5u * sizeof(int64_t))
        + 2u * (sizeof(int64_t *) + 3u * sizeof(int64_t))
        + 8u * sizeof(uint64_t);
    scratch_bytes = sizeof(col_rel_t) + strlen(candidate->name) + 1u;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        before + data_bytes + metadata_bytes + aux_bytes - 1u,
        memory_order_release);
    CHECK(col_rel_prepare_replacement(dst, candidate, &replacement)
        == ENOSPC && !replacement.staged && !replacement.writer_acquired
        && dst->columns == old_columns
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "replacement aux one-byte denial preserves old relation");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        before + data_bytes + metadata_bytes + aux_bytes + scratch_bytes,
        memory_order_release);
    CHECK(col_rel_prepare_replacement(dst, candidate, &replacement) == 0
        && replacement.aux_reserved
        && replacement.aux_pending.bytes == aux_bytes
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == before + data_bytes + metadata_bytes + aux_bytes + scratch_bytes,
        "replacement aux exact-fit preparation");
    col_rel_discard_replacement(&replacement);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == before,
        "replacement aux discard releases credit");
    CHECK(col_rel_prepare_replacement(dst, candidate, &replacement) == 0,
        "replacement aux retry");
    if (replacement.staged) {
        col_rel_commit_replacement_locked(dst, &replacement);
        CHECK(dst->aux_reserved_bytes == aux_bytes
            && dst->aux_reservation.bytes == aux_bytes
            && dst->merge_columns[0][0] == 31
            && dst->retract_backup_columns[1][0] == 32
            && dst->dedup_slots[0] == 33
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == dst->descriptor_reservation->bytes + metadata_bytes
            + data_bytes + aux_bytes,
            "replacement aux commit transfers exact credit and buffers");
        CHECK(wl_columnar_relation_accounting_complete(dst, ref),
            "owner accounting accepts committed aux token");
        dst->aux_reserved_bytes = 0;
        CHECK(!wl_columnar_relation_accounting_complete(dst, ref),
            "owner accounting rejects stale committed aux token");
        dst->aux_reserved_bytes = aux_bytes + 1;
        CHECK(!wl_columnar_relation_accounting_complete(dst, ref),
            "owner accounting rejects undercharged aux token");
        dst->aux_reserved_bytes = aux_bytes;
        wl_columnar_memory_governor_t *aux_governor
            = dst->aux_reservation.governor;
        dst->aux_reservation.governor = NULL;
        CHECK(!wl_columnar_relation_accounting_complete(dst, ref),
            "owner accounting rejects aux token bound to another governor");
        dst->aux_reservation.governor = aux_governor;
        dst->aux_reservation.bytes = aux_bytes - 1;
        CHECK(!wl_columnar_relation_accounting_complete(dst, ref),
            "owner accounting rejects undersized aux token");
        dst->aux_reservation.bytes = aux_bytes;
        before = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref));
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                ref)->usable_bytes,
            before + data_bytes + metadata_bytes + aux_bytes
            + scratch_bytes, memory_order_release);
        CHECK(col_rel_prepare_replacement(dst, candidate, &replacement) == 0,
            "replacement with existing aux token prepares");
        if (replacement.staged) {
            col_rel_commit_replacement_locked(dst, &replacement);
            CHECK(dst->aux_reservation.bytes == aux_bytes
                && wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(ref)) == before,
                "replacement frees old aux before releasing its credit");
        }
    }
cleanup:
    col_rel_discard_replacement(&replacement);
    col_rel_destroy(dst);
    col_rel_destroy(candidate);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "replacement aux cleanup releases credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

int
main(void)
{
    test_heap_descriptor_admission();
    test_governed_clear_preserves_live_descriptor_charge();
    test_relation_accounting_aux_reservation();
    test_schema_metadata_adoption_and_replacement();
    test_schema_metadata_first_allocation();
    test_nested_arrow_metadata_adoption();
    test_pool_metadata_name_reuse();
    test_compound_map_admission_and_bounds();
    test_shared_view_metadata_admission();
    test_shared_view_table_lifetime();
    test_attach_existing_shared_table();
    test_append_type_metadata_admission();
    test_rename_metadata_admission();
    test_replacement_metadata_admission();
    test_replacement_aux_admission();
    test_governed_logical_copy();
    test_governed_empty_compound_copy();
    test_physical_timestamp_capacity();
    test_cow_retained_timestamp_capacity_admission();
    test_governed_pool_clone_fallback();
    test_governed_delta_restore_overwrite_order();
    test_resize_allocation_cleanup_precedes_rollback();
    test_cow_private_cleanup_precedes_rollback();
#ifndef _WIN32
    test_terminal_release_policy();
#endif
    test_relation_retirement_token();
    test_relation_retirement_rejects_borrowed_ownership();
    test_cow_exact_fit_and_denial();
    test_cow_multi_column_cleanup();
    test_append_transitions();
    test_arena_promotion_with_timestamps();
    test_arena_promotion_denial_preserves_state();
    test_unmanaged_arena_append_all_preserves_ownership();
    test_unmanaged_arena_append_all_reconciles_timestamps();
    test_arena_append_all_admission_boundary();
    test_heap_append_all_admission_boundary();
    test_cow_capacity_denial_preserves_state();
    test_cow_ledger_reconcile_is_exact_once();
    if (failures != 0)
        return 1;
    puts("memory admission relation: PASS");
    return 0;
}
