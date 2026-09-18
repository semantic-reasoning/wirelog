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
        WL_IGNORE_RESULT(col_rel_radix_sort_int64(view));
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
        WL_IGNORE_RESULT(col_rel_radix_sort_int64(view));
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
        wl_columnar_memory_governor_ref_get(ref)->budget_bytes = grown_bytes;
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(ref)
            ->usable_bytes, grown_bytes, memory_order_relaxed);
        CHECK(col_rel_append_row(view, &value) == 0
            && view->capacity == COL_REL_INIT_CAP * 2u
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == grown_bytes,
            "COW capacity denial retry");
        col_rel_destroy(view);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "COW capacity retry teardown balance");
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

    make_resolution(&resolution, bytes - 1u);
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
    col_delta_timestamp_t *view_timestamps = realloc(view->timestamps,
            (size_t)physical * sizeof(*view_timestamps));
    CHECK(view_timestamps != NULL, "expand view timestamp capacity");
    if (!view_timestamps) goto cleanup;
    view->timestamps = view_timestamps;
    view->timestamp_capacity = physical;
    int64_t *old_column = view->columns[0];
    bool *old_shared = view->col_shared;
    col_delta_timestamp_t *old_timestamps = view->timestamps;
    uint64_t old_view_generation = view->view_generation;
    uint64_t old_storage_generation = view->storage_generation;
    uint64_t old_aliases = col_rel_storage_alias_borrow_count(source);
    col_delta_timestamp_t old_timestamp = view->timestamps[0];
    CHECK(col_rel_cow_unshare(view, 0) == ENOMEM
        && view->columns[0] == old_column && view->col_shared == old_shared
        && view->timestamps == old_timestamps
        && view->timestamps[0].multiplicity == old_timestamp.multiplicity
        && view->view_generation == old_view_generation
        && view->storage_generation == old_storage_generation
        && col_rel_storage_alias_borrow_count(source) == old_aliases
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "retained timestamp capacity denial preserves state");
    wl_columnar_memory_governor_ref_get(ref)->budget_bytes = bytes;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(ref)
        ->usable_bytes, bytes, memory_order_relaxed);
    CHECK(col_rel_cow_unshare(view, 0) == 0
        && view->col_shared == NULL
        && view->columns[0] != old_column
        && view->timestamps == old_timestamps
        && view->timestamp_capacity == physical
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == bytes,
        "retained timestamp capacity exact-fit retry");
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "retained timestamp capacity teardown balance");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_cow_reduced_timestamp_capacity_admission(void)
{
    const uint32_t columns_capacity = COL_REL_INIT_CAP;
    const uint32_t physical = COL_REL_INIT_CAP / 2u;
    const uint64_t bytes = (uint64_t)columns_capacity * sizeof(int64_t)
        + (uint64_t)physical * sizeof(col_delta_timestamp_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *source = col_rel_new_auto("reduced-source", 1);
    col_rel_t *view = col_rel_new_auto("reduced-view", 1);
    int64_t value = 17;

    make_resolution(&resolution, bytes - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && source && view, "reduced timestamp COW setup");
    if (!ref || !source || !view) goto cleanup;
    CHECK(col_rel_append_row(source, &value) == 0
        && col_rel_enable_timestamps(source) == 0,
        "reduced timestamp seed");
    if (!source->timestamps) goto cleanup;
    col_delta_timestamp_t *reduced = realloc(source->timestamps,
            (size_t)physical * sizeof(*reduced));
    CHECK(reduced != NULL, "reduce physical timestamp capacity");
    if (!reduced) goto cleanup;
    source->timestamps = reduced;
    source->timestamp_capacity = physical;
    source->timestamps[0] = (col_delta_timestamp_t){
        .iteration = 7, .stratum = 3, .worker = 2, .multiplicity = -4
    };
    CHECK(col_rel_attach_memory_governor(view, ref) == 0
        && col_rel_install_shared_view(view, source) == 0,
        "install reduced timestamp view");
    col_delta_timestamp_t *view_timestamps = realloc(view->timestamps,
            (size_t)physical * sizeof(*view_timestamps));
    CHECK(view_timestamps != NULL, "reduce view timestamp capacity");
    if (!view_timestamps) goto cleanup;
    view->timestamps = view_timestamps;
    view->timestamp_capacity = physical;
    int64_t *old_column = view->columns[0];
    col_delta_timestamp_t *old_timestamps = view->timestamps;
    col_delta_timestamp_t old_timestamp = view->timestamps[0];
    uint64_t old_view_generation = view->view_generation;
    uint64_t old_storage_generation = view->storage_generation;
    uint64_t old_aliases = col_rel_storage_alias_borrow_count(source);
    CHECK(col_rel_cow_unshare(view, 0) == ENOMEM
        && view->columns[0] == old_column && view->timestamps == old_timestamps
        && memcmp(&view->timestamps[0], &old_timestamp,
        sizeof(old_timestamp)) == 0
        && view->col_shared != NULL
        && view->view_generation == old_view_generation
        && view->storage_generation == old_storage_generation
        && col_rel_storage_alias_borrow_count(source) == old_aliases
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "reduced timestamp denial preserves state");
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(ref);
    budget->budget_bytes = bytes;
    atomic_store_explicit(&budget->usable_bytes, bytes, memory_order_relaxed);
    CHECK(col_rel_cow_unshare(view, 0) == 0
        && view->col_shared == NULL && view->columns[0] != old_column
        && view->timestamps == old_timestamps
        && memcmp(&view->timestamps[0], &old_timestamp,
        sizeof(old_timestamp)) == 0
        && view->timestamp_capacity == physical
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == bytes,
        "reduced timestamp exact-fit retry");
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "reduced timestamp teardown balance");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_cow_timestamp_growth_replacement(void)
{
    const uint32_t old_capacity = COL_REL_INIT_CAP;
    const uint32_t physical = COL_REL_INIT_CAP / 2u;
    const uint32_t grown_capacity = old_capacity * 2u;
    const uint64_t grown_bytes = (uint64_t)grown_capacity * sizeof(int64_t)
        + (uint64_t)grown_capacity * sizeof(col_delta_timestamp_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *source = col_rel_new_auto("growth-source", 1);
    col_rel_t *view = col_rel_new_auto("growth-view", 1);
    int64_t value = 21;

    make_resolution(&resolution, grown_bytes - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && source && view, "timestamp growth setup");
    if (!ref || !source || !view) goto cleanup;
    CHECK(col_rel_append_row(source, &value) == 0
        && col_rel_enable_timestamps(source) == 0,
        "timestamp growth seed");
    if (!source->timestamps) goto cleanup;
    col_delta_timestamp_t *reduced = realloc(source->timestamps,
            (size_t)physical * sizeof(*reduced));
    CHECK(reduced != NULL, "timestamp growth reduction");
    if (!reduced) goto cleanup;
    source->timestamps = reduced;
    source->timestamp_capacity = physical;
    source->timestamps[0] = (col_delta_timestamp_t){
        .iteration = 11, .stratum = 5, .worker = 1, .multiplicity = 6
    };
    CHECK(col_rel_attach_memory_governor(view, ref) == 0
        && col_rel_install_shared_view(view, source) == 0,
        "timestamp growth shared view");
    col_delta_timestamp_t *view_timestamps = realloc(view->timestamps,
            (size_t)physical * sizeof(*view_timestamps));
    CHECK(view_timestamps != NULL, "timestamp growth view reduction");
    if (!view_timestamps) goto cleanup;
    view->timestamps = view_timestamps;
    view->timestamp_capacity = physical;
    col_delta_timestamp_t expected = view->timestamps[0];
    int64_t *old_column = view->columns[0];
    CHECK(col_rel_cow_unshare(view, grown_capacity) == ENOMEM
        && view->capacity == old_capacity &&
        view->timestamp_capacity == physical
        && view->columns[0] == old_column && view->col_shared != NULL
        && memcmp(&view->timestamps[0], &expected, sizeof(expected)) == 0
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "timestamp growth denial preserves state");
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(ref);
    budget->budget_bytes = grown_bytes;
    atomic_store_explicit(&budget->usable_bytes, grown_bytes,
        memory_order_relaxed);
    CHECK(col_rel_cow_unshare(view, grown_capacity) == 0
        && view->capacity == grown_capacity
        && view->timestamp_capacity == grown_capacity
        && memcmp(&view->timestamps[0], &expected, sizeof(expected)) == 0
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == grown_bytes,
        "timestamp growth exact-fit retry");
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "timestamp growth teardown balance");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

int
main(void)
{
    test_physical_timestamp_capacity();
    test_cow_retained_timestamp_capacity_admission();
    test_cow_reduced_timestamp_capacity_admission();
    test_cow_timestamp_growth_replacement();
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
