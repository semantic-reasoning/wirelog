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

static int failures;

#define CHECK(condition, message) do { \
            if (!(condition)) { \
                fprintf(stderr, "FAIL: %s\n", message); \
                failures++; \
            } \
} while (0)

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

    make_resolution(&resolution, private_bytes);
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
        col_rel_radix_sort_int64(view);
        CHECK(view->columns[0][0] == 1 && view->columns[0][1] == 5
            && view->columns[0][2] == 9,
            "COW append/sort did not preserve copied rows");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == private_bytes,
            "exact-fit COW reservation");
        col_rel_destroy(view);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "COW reservation release");
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution, private_bytes - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    source = NULL;
    view = ref ? make_shared_view(ref, &source) : NULL;
    CHECK(view != NULL, "COW denial setup");
    if (view) {
        int64_t *borrowed = view->columns[0];
        col_rel_radix_sort_int64(view);
        CHECK(view->col_shared != NULL && view->columns[0] == borrowed,
            "denied COW changed ownership");
        CHECK(view->columns[0][0] == 9 && view->columns[0][1] == 1,
            "denied COW changed rows");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "denied COW left reservation");
        col_rel_destroy(view);
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
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

    make_resolution(&resolution, private_bytes);
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
                wl_columnar_memory_governor_ref_get(ref)) == private_bytes,
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

    make_resolution(&resolution, bytes);
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
                wl_columnar_memory_governor_ref_get(ref)) == bytes,
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

    make_resolution(&resolution, grown_bytes);
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
                wl_columnar_memory_governor_ref_get(ref)) == grown_bytes,
            "append_row COW reservation");
        col_rel_destroy(view);
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution, grown_bytes);
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
        * (sizeof(int64_t) + sizeof(col_delta_timestamp_t)));
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

    make_resolution(&resolution, bytes - 1u);
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
                wl_columnar_memory_governor_ref_get(ref)) == 0,
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

    make_resolution(&resolution, exact_bytes);
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
                wl_columnar_memory_governor_ref_get(ref)) == exact_bytes,
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

    make_resolution(&resolution, exact_bytes - 1u);
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
                wl_columnar_memory_governor_ref_get(ref)) == 0,
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

    make_resolution(&resolution, exact_bytes);
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
                wl_columnar_memory_governor_ref_get(ref)) == exact_bytes,
            "heap append_all exact-fit reservation");
    }
    col_rel_destroy(src);
    col_rel_destroy(dst);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution, exact_bytes - 1u);
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
                wl_columnar_memory_governor_ref_get(ref)) == 0,
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

    make_resolution(&resolution, grown_bytes - 1u);
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
                wl_columnar_memory_governor_ref_get(ref)) == 0,
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
        col_rel_radix_sort_int64(view);
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

int
main(void)
{
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
