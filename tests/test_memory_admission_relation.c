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
        == overwrite_expected_peak + rel->descriptor_reserved_bytes
        + rel->metadata_reserved_bytes,
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
descriptor_bytes(const char *name)
{
    return sizeof(col_rel_t) + strlen(name) + 1u;
}

static uint64_t
metadata_bytes(const col_rel_t *rel)
{
    uint64_t bytes = rel->schema_ok ? 3u : 0u;
    if (rel->col_names)
        bytes += (uint64_t)rel->ncols * sizeof(*rel->col_names);
    if (rel->column_types)
        bytes += (uint64_t)rel->ncols * sizeof(*rel->column_types);
    if (rel->schema_ok)
        bytes += (uint64_t)rel->ncols
            * (sizeof(struct ArrowSchema *) + sizeof(struct ArrowSchema)
            + 2u);
    for (uint32_t i = 0; i < rel->ncols; i++) {
        const char *name = rel->col_names ? rel->col_names[i] : NULL;
        if (name && rel->col_names)
            bytes += strlen(name) + 1u;
        if (rel->schema_ok)
            bytes += name ? strlen(name) + 1u : 1u;
    }
    return bytes;
}

static uint64_t
auto_metadata_bytes(uint32_t ncols)
{
    uint64_t bytes = 3u + (uint64_t)ncols
        * (sizeof(char *) + sizeof(struct ArrowSchema *)
        + sizeof(struct ArrowSchema) + 2u);
    for (uint32_t i = 0; i < ncols; i++) {
        char name[32];
        snprintf(name, sizeof(name), "col%u", i);
        bytes += 2u * (strlen(name) + 1u);
    }
    return bytes;
}

static void
test_heap_descriptor_admission(void)
{
    const char *name = "descriptor";
    const uint64_t bytes = descriptor_bytes(name);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *relation = (col_rel_t *)(uintptr_t)1;

    make_resolution(&resolution, bytes - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && wl_columnar_relation_alloc_governed(&relation, name, ref)
        == ENOSPC && relation == NULL,
        "descriptor one-byte denial clears output");
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "descriptor denial leaves no reservation");
        wl_columnar_memory_governor_ref_release(ref);
    }

    make_resolution(&resolution, bytes);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    relation = NULL;
    CHECK(ref && wl_columnar_relation_alloc_governed(&relation, name, ref)
        == 0 && relation && relation->descriptor_reserved_bytes == bytes,
        "descriptor exact-fit allocation");
    if (relation) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == bytes,
            "descriptor exact-fit charge");
        CHECK(col_rel_destroy_checked(relation) == 0,
            "checked destroy frees admitted descriptor");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "physical descriptor free releases charge");
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
}

static void
test_heap_descriptor_attach_and_replacement(void)
{
    const uint64_t bytes = descriptor_bytes("target");
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *target = col_rel_new_auto("target", 1);
    col_rel_t *candidate = col_rel_new_auto("candidate", 1);
    col_rel_replacement_t replacement;
    int64_t value = 7;

    make_resolution(&resolution, bytes - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(target && ref && col_rel_attach_memory_governor(target, ref)
        == ENOSPC && target->memory_governor == NULL
        && target->descriptor_reserved_bytes == 0
        && target->memory_budget_denial_pending,
        "legacy attach denial is transactional");
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "legacy attach denial leaves no reservation");
        wl_columnar_memory_governor_ref_release(ref);
    }

    make_resolution(&resolution, bytes + metadata_bytes(target)
        + metadata_bytes(candidate)
        + COL_REL_INIT_CAP * sizeof(int64_t));
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    if (target && candidate && ref) {
        CHECK(col_rel_attach_memory_governor(target, ref) == 0
            && target->descriptor_reserved_bytes == bytes
            && !target->memory_budget_denial_pending,
            "legacy attach exact descriptor charge");
        CHECK(col_rel_append_row(candidate, &value) == 0,
            "replacement candidate setup");
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        atomic_store_explicit(&governor->usable_bytes,
            bytes + metadata_bytes(target) + metadata_bytes(candidate)
            + COL_REL_INIT_CAP * sizeof(int64_t) - 1u,
            memory_order_release);
        memset(&replacement, 0, sizeof(replacement));
        wl_columnar_memory_reservation_init(&replacement.reservation);
        CHECK(col_rel_prepare_replacement(target, candidate, &replacement)
            == ENOMEM && target->memory_budget_denial_pending
            && wl_columnar_memory_reserved(governor)
            == bytes + target->metadata_reserved_bytes,
            "replacement metadata one-byte denial keeps old token");
        atomic_store_explicit(&governor->usable_bytes,
            bytes + metadata_bytes(target) + metadata_bytes(candidate)
            + COL_REL_INIT_CAP * sizeof(int64_t),
            memory_order_release);
        int rc = col_rel_prepare_replacement(target, candidate,
                &replacement);
        CHECK(rc == 0, "replacement preparation keeps descriptor charge");
        if (rc == 0) {
            CHECK(replacement.metadata_reservation_active
                && replacement.metadata_reserved_bytes
                == metadata_bytes(candidate)
                && wl_columnar_memory_reserved(governor)
                == bytes + target->metadata_reserved_bytes
                + replacement.reserved_bytes
                + replacement.metadata_reserved_bytes,
                "replacement owns old and new metadata during staging");
            col_rel_commit_replacement_locked(target, &replacement);
        }
        col_rel_discard_replacement(&replacement);
        CHECK(target->descriptor_reserved_bytes == bytes
            && target->name && strcmp(target->name, "target") == 0
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == bytes + target->metadata_reserved_bytes
            + target->retained_reserved_bytes,
            "replacement preserves descriptor owner and name");
    }
    col_rel_destroy(candidate);
    if (target)
        CHECK(col_rel_destroy_checked(target) == 0,
            "checked destruction releases descriptor");
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "replacement teardown releases descriptor once");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_pool_descriptor_excluded(void)
{
    wl_columnar_memory_resolution_t resolution;
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    wl_arena_t *arena = wl_arena_create(4096);
    col_rel_t *relation = pool && arena
        ? col_rel_pool_new_auto(pool, arena, "pool", 1) : NULL;
    make_resolution(&resolution, relation ? metadata_bytes(relation) : 0);
    wl_columnar_memory_governor_ref_t *ref
        = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && relation && col_rel_attach_memory_governor(relation, ref)
        == 0 && relation->descriptor_reserved_bytes == 0
        && relation->metadata_reserved_bytes == metadata_bytes(relation),
        "pool descriptor excluded from heap charge");
    if (relation)
        col_rel_free_contents(relation);
    if (pool)
        delta_pool_destroy(pool);
    if (arena)
        wl_arena_free(arena);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "pool teardown leaves no descriptor charge");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_schema_metadata_admission(void)
{
    const uint64_t descriptor = descriptor_bytes("metadata-schema");
    const uint64_t names_and_arrow = auto_metadata_bytes(1);
    const uint64_t retained = COL_REL_INIT_CAP * sizeof(int64_t);
    const uint64_t typed = names_and_arrow
        + sizeof(wirelog_column_type_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    wl_columnar_memory_governor_t *governor;
    col_rel_t *rel = NULL;
    const wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    uint64_t view_before;
    struct ArrowSchema *old_child;

    make_resolution(&resolution, descriptor + retained + names_and_arrow);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && wl_columnar_relation_alloc_governed(&rel,
        "metadata-schema", ref) == 0, "metadata schema setup");
    if (!ref || !rel)
        goto cleanup;
    governor = wl_columnar_memory_governor_ref_get(ref);
    atomic_store_explicit(&governor->usable_bytes,
        descriptor + names_and_arrow - 1u, memory_order_release);
    CHECK(col_rel_set_schema(rel, 1, NULL) == ENOSPC
        && rel->ncols == 0 && rel->metadata_reserved_bytes == 0
        && !rel->memory_budget_denial_pending
        && wl_columnar_memory_reserved(governor) == descriptor,
        "schema metadata one-byte denial precedes allocation");
    atomic_store_explicit(&governor->usable_bytes,
        descriptor + retained + names_and_arrow - 1u,
        memory_order_release);
    CHECK(col_rel_set_schema(rel, 1, NULL) == ENOSPC
        && rel->ncols == 0 && rel->col_names == NULL && !rel->schema_ok
        && rel->metadata_reserved_bytes == 0
        && !rel->memory_budget_denial_pending
        && wl_columnar_memory_reserved(governor) == descriptor,
        "schema one-byte denial preserves empty image");
    atomic_store_explicit(&governor->usable_bytes,
        descriptor + retained + names_and_arrow, memory_order_release);
    wl_columnar_relation_test_fail_next_metadata_alloc();
    CHECK(col_rel_set_schema(rel, 1, NULL) == ENOMEM
        && rel->ncols == 0 && rel->metadata_reserved_bytes == 0
        && wl_columnar_memory_reserved(governor) == descriptor,
        "schema allocation failure rolls back admission");
    CHECK(col_rel_set_schema(rel, 1, NULL) == 0
        && rel->metadata_reserved_bytes == names_and_arrow
        && rel->metadata_reservation.identity == &rel->metadata_reservation
        && atomic_load_explicit(&rel->metadata_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        && atomic_load_explicit(&rel->metadata_reservation.owner_bits,
        memory_order_acquire) == (uintptr_t)rel
        && wl_columnar_memory_reserved(governor)
        == descriptor + retained + names_and_arrow,
        "schema retry installs exact owned metadata");
    view_before = rel->view_generation;
    old_child = rel->schema.children[0];
    atomic_store_explicit(&governor->usable_bytes,
        descriptor + retained + names_and_arrow + typed - 1u,
        memory_order_release);
    CHECK(col_rel_set_column_types(rel, &type, 1) == ENOSPC
        && rel->schema.children[0] == old_child
        && rel->column_types == NULL
        && !rel->memory_budget_denial_pending
        && rel->view_generation == view_before
        && rel->metadata_reserved_bytes == names_and_arrow
        && wl_columnar_memory_reserved(governor)
        == descriptor + retained + names_and_arrow,
        "type one-byte denial preserves old image and generation");
    atomic_store_explicit(&governor->usable_bytes,
        descriptor + retained + names_and_arrow + typed,
        memory_order_release);
    wl_columnar_relation_test_fail_next_metadata_alloc();
    CHECK(col_rel_set_column_types(rel, &type, 1) == ENOMEM
        && rel->schema.children[0] == old_child
        && rel->view_generation == view_before
        && wl_columnar_memory_reserved(governor)
        == descriptor + retained + names_and_arrow,
        "type allocation failure preserves old metadata token");
    CHECK(col_rel_set_column_types(rel, &type, 1) == 0
        && rel->schema.children[0] != old_child
        && rel->metadata_reserved_bytes == typed
        && rel->view_generation == view_before + 1u
        && wl_columnar_memory_reserved(governor)
        == descriptor + retained + typed,
        "type retry releases old credit after overlap");
cleanup:
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "metadata teardown releases token once");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_legacy_metadata_attach(void)
{
    col_rel_t *rel = col_rel_new_auto("legacy-metadata", 1);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    uint64_t descriptor, metadata;

    CHECK(rel != NULL, "legacy metadata fixture");
    if (!rel)
        return;
    descriptor = descriptor_bytes("legacy-metadata");
    metadata = metadata_bytes(rel);
    make_resolution(&resolution, descriptor + metadata - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(rel, ref) == ENOSPC
        && rel->memory_governor == NULL
        && rel->metadata_reserved_bytes == 0
        && rel->descriptor_reserved_bytes == 0
        && rel->memory_budget_denial_pending
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "populated attach one-byte denial rolls back both tokens");
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
    make_resolution(&resolution, descriptor + metadata);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(rel, ref) == 0
        && rel->metadata_reserved_bytes == metadata
        && rel->descriptor_reserved_bytes == descriptor
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor + metadata,
        "populated attach exact fit owns metadata");
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "populated attach teardown releases both tokens");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_zero_width_schema_metadata(void)
{
    const uint64_t descriptor = descriptor_bytes("zero-metadata");
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    wl_columnar_memory_governor_t *governor;
    col_rel_t *rel = NULL;

    make_resolution(&resolution, descriptor + 3u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && wl_columnar_relation_alloc_governed(&rel,
        "zero-metadata", ref) == 0, "zero-width metadata setup");
    if (!ref || !rel)
        goto cleanup;
    governor = wl_columnar_memory_governor_ref_get(ref);
    atomic_store_explicit(&governor->usable_bytes, descriptor + 2u,
        memory_order_release);
    CHECK(col_rel_set_schema(rel, 0, NULL) == ENOSPC
        && !rel->schema_ok && rel->capacity == 0
        && rel->metadata_reserved_bytes == 0
        && wl_columnar_memory_reserved(governor) == descriptor,
        "zero-width Arrow root one-byte denial");
    atomic_store_explicit(&governor->usable_bytes, descriptor + 3u,
        memory_order_release);
    CHECK(col_rel_set_schema(rel, 0, NULL) == 0
        && rel->schema_ok && rel->capacity == 0
        && rel->metadata_reserved_bytes == 3u
        && wl_columnar_memory_reserved(governor) == descriptor + 3u,
        "zero-width Arrow root exact fit");
cleanup:
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "zero-width metadata teardown");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_heap_descriptor_rename(void)
{
    const char *initial = "descriptor";
    const char *long_name = "descriptor-longer";
    const char *same_length = "DESCRIPTOR-LONGER";
    const uint64_t old_bytes = descriptor_bytes(initial);
    const uint64_t grown_bytes = descriptor_bytes(long_name);
    const uint64_t small_bytes = descriptor_bytes("x");
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    wl_columnar_memory_governor_t *governor;
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_t *relation = NULL;

    make_resolution(&resolution, old_bytes + grown_bytes);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && wl_columnar_relation_alloc_governed(&relation,
        initial, ref) == 0, "rename fixture allocation");
    if (!ref || !relation)
        goto cleanup;
    governor = wl_columnar_memory_governor_ref_get(ref);
    atomic_store_explicit(&governor->usable_bytes,
        old_bytes + grown_bytes - 1u, memory_order_release);
    CHECK(wl_columnar_relation_rename_checked(relation, long_name) == ENOSPC
        && strcmp(relation->name, initial) == 0
        && wl_columnar_memory_reserved(governor) == old_bytes,
        "rename one-byte denial keeps old name and token");
    atomic_store_explicit(&governor->usable_bytes,
        old_bytes + grown_bytes, memory_order_release);
    CHECK(wl_columnar_relation_rename_checked(relation, long_name) == 0
        && strcmp(relation->name, long_name) == 0
        && wl_columnar_memory_reserved(governor) == grown_bytes,
        "growing rename admits exact overlap and releases old charge");
    atomic_store_explicit(&governor->usable_bytes,
        grown_bytes * 2u, memory_order_release);
    CHECK(wl_columnar_relation_rename_checked(relation, same_length) == 0
        && strcmp(relation->name, same_length) == 0
        && wl_columnar_memory_reserved(governor) == grown_bytes,
        "same-length rename replaces token once");
    CHECK(wl_columnar_source_access_reader_acquire(
            &relation->descriptor_access, &reader) == 0,
        "rename descriptor reader setup");
    CHECK(wl_columnar_relation_rename_checked(relation, "blocked") == EBUSY
        && strcmp(relation->name, same_length) == 0
        && wl_columnar_memory_reserved(governor) == grown_bytes,
        "writer refusal keeps name and token");
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0,
        "rename descriptor reader release");
    atomic_store_explicit(&governor->usable_bytes,
        grown_bytes + small_bytes, memory_order_release);
    CHECK(wl_columnar_relation_rename_checked(relation, "x") == 0
        && strcmp(relation->name, "x") == 0
        && wl_columnar_memory_reserved(governor) == small_bytes,
        "shrinking rename releases old name charge");
cleanup:
    col_rel_destroy(relation);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "rename teardown releases final descriptor charge");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_governed_null_name_copy(void)
{
    const uint64_t descriptor = sizeof(col_rel_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = col_rel_new_auto("unnamed-source", 0);
    col_rel_t *copy = (col_rel_t *)(uintptr_t)1;

    CHECK(source != NULL, "null-name copy source");
    if (!source)
        return;
    free(source->name);
    source->name = NULL;
    const uint64_t bytes = descriptor + metadata_bytes(source);
    make_resolution(&resolution, bytes - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && wl_columnar_relation_deep_copy_governed(source,
        &copy, ref) == ENOSPC && copy == NULL,
        "null-name copy preserves typed one-byte denial");
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "denied null-name copy rolls back charge");
        wl_columnar_memory_governor_ref_release(ref);
    }
    make_resolution(&resolution, bytes);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    copy = NULL;
    CHECK(ref && wl_columnar_relation_deep_copy_governed(source,
        &copy, ref) == 0 && copy && copy->name == NULL
        && copy->descriptor_reserved_bytes == descriptor
        && copy->metadata_reserved_bytes == metadata_bytes(source),
        "null-name copy admits zero name bytes exactly");
    col_rel_destroy(copy);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "null-name copy teardown releases descriptor");
        wl_columnar_memory_governor_ref_release(ref);
    }
    col_rel_destroy(source);
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

    make_resolution(&resolution, private_bytes + descriptor_bytes("view")
        + auto_metadata_bytes(1));
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
            == private_bytes + descriptor_bytes("view")
            + view->metadata_reserved_bytes,
            "exact-fit COW reservation");
        col_rel_destroy(view);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "COW reservation release");
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution, private_bytes - 1u
        + descriptor_bytes("view") + auto_metadata_bytes(1));
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
            == descriptor_bytes("view") + view->metadata_reserved_bytes,
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
    make_resolution(&resolution, expected + descriptor_bytes("logical-copy")
        + metadata_bytes(source));
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
            == expected + descriptor_bytes("logical-copy")
            + copy->metadata_reserved_bytes,
            "logical copy carries exact payload reservation");
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
        expected + descriptor_bytes("logical-copy")
        + metadata_bytes(source) - 1u,
        memory_order_release);
    CHECK(wl_columnar_relation_deep_copy_governed(source, &copy,
        ref) == ENOMEM && !copy
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0
        && source->nrows == 1 && source->columns[0][0] == value,
        "denied logical copy leaves source and governor unchanged");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        expected + descriptor_bytes("logical-copy")
        + metadata_bytes(source),
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
    make_resolution(&resolution, expected
        + descriptor_bytes("empty-compound-copy")
        + metadata_bytes(source));
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

    make_resolution(&resolution, private_bytes
        + descriptor_bytes("multi-view") + metadata_bytes(view));
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
            == private_bytes + descriptor_bytes("multi-view")
            + view->metadata_reserved_bytes,
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

    make_resolution(&resolution, bytes + auto_metadata_bytes(1));
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
            == bytes + relation->metadata_reserved_bytes,
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

    make_resolution(&resolution, grown_bytes
        + descriptor_bytes("full-view") + auto_metadata_bytes(1));
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
            == grown_bytes + descriptor_bytes("full-view")
            + view->metadata_reserved_bytes,
            "append_row COW reservation");
        col_rel_destroy(view);
        col_rel_destroy(source);
    }
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution, grown_bytes
        + descriptor_bytes("full-view") + auto_metadata_bytes(1));
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
        + descriptor_bytes("full-view") + auto_metadata_bytes(1));
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

    make_resolution(&resolution, bytes + auto_metadata_bytes(1) - 1u);
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
            == relation->metadata_reserved_bytes,
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

    make_resolution(&resolution, exact_bytes + auto_metadata_bytes(1));
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
            == exact_bytes + dst->metadata_reserved_bytes,
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
        exact_bytes + auto_metadata_bytes(1) - 1u);
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
            == dst->metadata_reserved_bytes,
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

    make_resolution(&resolution, exact_bytes
        + descriptor_bytes("heap-append-exact")
        + auto_metadata_bytes(1));
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
            == exact_bytes + descriptor_bytes("heap-append-exact")
            + dst->metadata_reserved_bytes,
            "heap append_all exact-fit reservation");
    }
    col_rel_destroy(src);
    col_rel_destroy(dst);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);

    make_resolution(&resolution, exact_bytes - 1u
        + descriptor_bytes("heap-append-denied")
        + auto_metadata_bytes(1));
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
            == descriptor_bytes("heap-append-denied")
            + dst->metadata_reserved_bytes,
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

    make_resolution(&resolution, grown_bytes - 1u
        + descriptor_bytes("full-view") + auto_metadata_bytes(1));
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
            == descriptor_bytes("full-view")
            + view->metadata_reserved_bytes,
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

    make_resolution(&resolution, bytes - 1u
        + descriptor_bytes("skew-view") + auto_metadata_bytes(1));
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
        == descriptor_bytes("skew-view")
        + view->metadata_reserved_bytes,
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
        uint64_t heap_floor = route == 1 ? 0 : descriptor_bytes("retry");
        uint64_t metadata_floor = auto_metadata_bytes(1);
        uint32_t used = pool ? pool->slot_used : 0;
        atomic_store_explicit(&g->usable_bytes,
            baseline + payload + heap_floor + metadata_floor - 1,
            memory_order_release);
        clone = wl_columnar_relation_pool_new_like_governed(pool, "denied", src,
                ref);
        CHECK(!clone, "constructor cannot escape admission via fallback");
        CHECK(!pool || pool->slot_used == used, "failed slot restored");
        CHECK(wl_columnar_memory_reserved(g) == baseline,
            "failed reservation restored");
        if (clone) goto cleanup;
        atomic_store_explicit(&g->usable_bytes,
            baseline + payload + heap_floor + metadata_floor,
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
        == sizeof(restored) + rel->descriptor_reserved_bytes
        + rel->metadata_reserved_bytes,
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
        == descriptor_bytes("view") + view->metadata_reserved_bytes,
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

int
main(void)
{
    test_heap_descriptor_admission();
    test_heap_descriptor_attach_and_replacement();
    test_pool_descriptor_excluded();
    test_schema_metadata_admission();
    test_legacy_metadata_attach();
    test_zero_width_schema_metadata();
    test_heap_descriptor_rename();
    test_governed_null_name_copy();
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
