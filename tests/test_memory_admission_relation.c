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
static uint64_t overwrite_expected_ledger;
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
        && rel->retained_reserved_bytes == overwrite_expected_peak
        && col_rel_owned_ledger_bytes(rel) == overwrite_expected_ledger
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
    return sizeof(col_rel_t) + strlen(name) + 1u
           + sizeof(wl_columnar_memory_reservation_t);
}

static uint64_t
metadata_bytes(const col_rel_t *rel)
{
    uint64_t bytes = rel->schema_ok ? 3u : 0u;
    bytes += (uint64_t)rel->compound_arity_len * sizeof(uint32_t);
    if (rel->col_names)
        bytes += (uint64_t)rel->ncols * sizeof(*rel->col_names);
    if (rel->column_types)
        bytes += (uint64_t)rel->ncols * sizeof(*rel->column_types);
    if (rel->col_shared)
        bytes += (uint64_t)rel->ncols
            * (sizeof(*rel->columns) + sizeof(*rel->col_shared));
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

static uint64_t
shared_table_bytes(uint32_t ncols)
{
    return (uint64_t)ncols * (sizeof(int64_t *) + sizeof(bool));
}

static uint64_t
private_payload_bytes(uint32_t ncols, uint32_t capacity,
    uint32_t timestamp_capacity)
{
    return (uint64_t)ncols * sizeof(int64_t *)
           + (uint64_t)ncols * (capacity ? capacity : 1u) * sizeof(int64_t)
           + (uint64_t)ncols * sizeof(int64_t)
           + (uint64_t)timestamp_capacity * sizeof(col_delta_timestamp_t);
}

static uint64_t
attach_payload_bytes(const col_rel_t *rel)
{
    uint64_t bytes = 0;
    if (!col_rel_retained_live_bytes(rel, &bytes))
        return UINT64_MAX;
    return bytes + (rel->ncols && !rel->row_scratch
        ? (uint64_t)rel->ncols * sizeof(int64_t) : 0u);
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

    uint64_t target_payload = attach_payload_bytes(target);
    uint64_t candidate_payload = private_payload_bytes(1,
            candidate->capacity, 0);
    uint64_t exact = bytes + metadata_bytes(target) + target_payload
        + metadata_bytes(candidate) + candidate_payload;
    make_resolution(&resolution, exact);
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
            exact - 1u,
            memory_order_release);
        memset(&replacement, 0, sizeof(replacement));
        wl_columnar_memory_reservation_init(&replacement.reservation);
        CHECK(col_rel_prepare_replacement(target, candidate, &replacement)
            == ENOMEM && target->memory_budget_denial_pending
            && wl_columnar_memory_reserved(governor)
            == bytes + target->metadata_reserved_bytes + target_payload,
            "replacement metadata one-byte denial keeps old token");
        atomic_store_explicit(&governor->usable_bytes,
            exact,
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
                + target_payload
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
    make_resolution(&resolution, relation ? metadata_bytes(relation)
        + strlen(relation->name) + 1u
        + attach_payload_bytes(relation) : 0);
    wl_columnar_memory_governor_ref_t *ref
        = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && relation && col_rel_attach_memory_governor(relation, ref)
        == 0 && relation->descriptor_reserved_bytes == 0
        && relation->pool_name_reserved_bytes == strlen(relation->name) + 1u
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
test_checked_new_like_boundaries(void)
{
    col_rel_t *source = col_rel_new_auto("like-source", 1);
    const wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    const col_rel_logical_col_t logical = {
        WIRELOG_COMPOUND_KIND_SIDE, 2u, 1u
    };
    CHECK(source && col_rel_set_column_types(source, &type, 1) == 0
        && col_rel_apply_compound_schema(source, &logical, 1) == 0,
        "checked new-like source");
    if (!source) return;
    const uint64_t payload = private_payload_bytes(1,
            COL_REL_INIT_CAP, 0);
    const uint64_t typed = auto_metadata_bytes(1) + sizeof(type);
    const uint64_t mapped = typed + sizeof(uint32_t);
    for (unsigned route = 0; route < 2; route++) {
        for (unsigned short_budget = 0; short_budget < 2; short_budget++) {
            const char *name = route ? "pool-like" : "heap-like";
            uint64_t identity = route ? strlen(name) + 1u
                : descriptor_bytes(name);
            uint64_t exact = identity + payload + typed + mapped;
            wl_columnar_memory_resolution_t resolution;
            make_resolution(&resolution, exact - short_budget);
            wl_columnar_memory_governor_ref_t *ref
                = wl_columnar_memory_governor_ref_create(&resolution);
            delta_pool_t *pool = route
                ? delta_pool_create(1, sizeof(col_rel_t), 4096) : NULL;
            col_rel_t *copy = (col_rel_t *)(uintptr_t)1;
            CHECK(ref && (!route || pool), "checked new-like governor/pool");
            if (!ref || (route && !pool)) {
                delta_pool_destroy(pool);
                if (ref) wl_columnar_memory_governor_ref_release(ref);
                continue;
            }
            int rc = route
                ? wl_columnar_relation_pool_new_like_governed_checked(
                &copy, pool, name, source, ref)
                : wl_columnar_relation_new_like_governed_checked(
                &copy, name, source, ref);
            CHECK(short_budget ? rc == ENOSPC && copy == NULL
                : rc == 0 && copy != NULL,
                "checked new-like exact/one-byte boundary");
            if (short_budget) {
                CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(ref)) == 0
                    && (!pool || pool->slot_used == 0),
                    "checked denial restores token and slot");
            } else if (copy) {
                CHECK(copy->column_types
                    && copy->column_types[0] == WIRELOG_TYPE_FLOAT
                    && copy->compound_arity_len == 1
                    && copy->metadata_reserved_bytes == mapped
                    && copy->pool_name_reserved_bytes
                    == (route ? strlen(name) + 1u : 0u)
                    && copy->descriptor_reserved_bytes
                    == (route ? 0u : descriptor_bytes(name))
                    && wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(ref))
                    == identity + payload + mapped,
                    "checked clone owns exact final shape");
                if (copy != (col_rel_t *)(uintptr_t)1)
                    col_rel_destroy(copy);
            }
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(ref)) == 0,
                "checked clone teardown releases all credits");
            delta_pool_destroy(pool);
            wl_columnar_memory_governor_ref_release(ref);
        }
    }
    CHECK(col_rel_enable_timestamps(source) == 0,
        "timestamped new-like source");
    uint64_t timestamp_bytes = (uint64_t)COL_REL_INIT_CAP
        * sizeof(col_delta_timestamp_t);
    uint64_t timestamp_peak = descriptor_bytes("timed-like") + payload
        + mapped + timestamp_bytes;
    for (unsigned short_budget = 0; short_budget < 2; short_budget++) {
        wl_columnar_memory_resolution_t timed_resolution;
        make_resolution(&timed_resolution, timestamp_peak - short_budget);
        wl_columnar_memory_governor_ref_t *timed_ref
            = wl_columnar_memory_governor_ref_create(&timed_resolution);
        col_rel_t *timed = (col_rel_t *)(uintptr_t)1;
        CHECK(timed_ref != NULL, "timestamp new-like governor");
        if (!timed_ref) continue;
        int timed_rc = wl_columnar_relation_new_like_governed_checked(
            &timed, "timed-like", source, timed_ref);
        CHECK(short_budget ? timed_rc == ENOSPC && timed == NULL
            : timed_rc == 0 && timed && timed->timestamps
            && timed->retained_reserved_bytes
            == payload + timestamp_bytes,
            "timestamp new-like exact/one-byte boundary");
        col_rel_destroy(timed);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(timed_ref)) == 0,
            "timestamp new-like teardown");
        wl_columnar_memory_governor_ref_release(timed_ref);
    }
    wl_columnar_memory_resolution_t resolution;
    make_resolution(&resolution, 1u << 20);
    wl_columnar_memory_governor_ref_t *ref
        = wl_columnar_memory_governor_ref_create(&resolution);
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    if (ref && pool) {
        col_rel_t *copy = (col_rel_t *)(uintptr_t)1;
        wl_columnar_source_access_writer_t writer = { 0 };
        CHECK(wl_columnar_source_access_writer_acquire(
                &source->descriptor_access, &writer) == 0,
            "checked source writer gate");
        CHECK(wl_columnar_relation_new_like_governed_checked(&copy,
            "busy", source, ref) == EBUSY && copy == NULL,
            "checked clone refuses busy source before publication");
        CHECK(wl_columnar_source_access_writer_release(&writer) == 0,
            "checked source writer release");
        wl_columnar_relation_test_fail_next_metadata_alloc();
        CHECK(wl_columnar_relation_pool_new_like_governed_checked(&copy,
            pool, "fault", source, ref) == ENOMEM && copy == NULL
            && pool->slot_used == 0
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "pool allocation failure restores slot and credit");
        col_rel_t *occupied = col_rel_pool_new_auto(pool, NULL, "used", 0);
        CHECK(occupied && occupied->pool_owned, "fallback occupied slot");
        CHECK(wl_columnar_relation_pool_new_like_governed_checked(&copy,
            pool, "fallback", source, ref) == 0 && copy
            && !copy->pool_owned && copy->descriptor_reserved_bytes > 0,
            "exhausted pool falls back to admitted heap");
        col_rel_destroy(copy);
        col_rel_destroy(occupied);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "heap fallback teardown");
        source->compound_arity_len = 2;
        copy = (col_rel_t *)(uintptr_t)1;
        CHECK(wl_columnar_relation_pool_new_like_governed_checked(&copy,
            pool, "invalid", source, ref) == EINVAL && copy == NULL,
            "checked pool constructor rejects malformed source");
        source->compound_arity_len = 1;
    }
    delta_pool_destroy(pool);
    if (ref) wl_columnar_memory_governor_ref_release(ref);
    col_rel_destroy(source);
}

static void
test_pool_name_lifecycle(void)
{
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    col_rel_t *rel = pool
        ? col_rel_pool_new_auto(pool, NULL, "pool-old", 1) : NULL;
    wl_columnar_memory_resolution_t resolution;
    uint64_t name_bytes = strlen("pool-old") + 1u;
    uint64_t metadata = rel ? metadata_bytes(rel) : 0;
    uint64_t payload = rel ? attach_payload_bytes(rel) : 0;
    make_resolution(&resolution, name_bytes + metadata + payload - 1u);
    wl_columnar_memory_governor_ref_t *ref
        = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(rel && ref, "legacy pool-name fixture");
    if (!rel || !ref) goto cleanup;
    CHECK(col_rel_attach_memory_governor(rel, ref) == ENOSPC
        && rel->memory_governor == NULL
        && rel->pool_name_reserved_bytes == 0
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "legacy pool attach one-byte denial is atomic");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        name_bytes + metadata + payload, memory_order_release);
    CHECK(col_rel_attach_memory_governor(rel, ref) == 0
        && rel->descriptor_reserved_bytes == 0
        && rel->pool_name_reserved_bytes == name_bytes,
        "legacy pool attach admits exact name and metadata");
    uint64_t live = name_bytes + metadata + payload;
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == live,
        "legacy pool exact credit");
    CHECK(wl_columnar_relation_rename_checked(rel, "pool-longer") == ENOSPC
        && strcmp(rel->name, "pool-old") == 0
        && rel->pool_name_reserved_bytes == name_bytes,
        "governed pool rename denial keeps old name/token");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        1u << 20, memory_order_release);
    CHECK(wl_columnar_relation_rename_checked(rel, "pool-longer") == 0
        && rel->pool_name_reserved_bytes == strlen("pool-longer") + 1u,
        "governed pool rename grows exact token");
    CHECK(wl_columnar_relation_rename_checked(rel, "p") == 0
        && rel->pool_name_reserved_bytes == 2u,
        "governed pool rename shrinks exact token");
    col_rel_t *candidate = col_rel_new_auto("candidate", 1);
    col_rel_replacement_t replacement = { 0 };
    CHECK(candidate && col_rel_prepare_replacement(rel, candidate,
        &replacement) == 0, "pool replacement prepares");
    if (replacement.staged)
        col_rel_commit_replacement_locked(rel, &replacement);
    col_rel_discard_replacement(&replacement);
    CHECK(rel->pool_name_reserved_bytes == 2u
        && rel->pool_name_reservation.bytes == 2u
        && atomic_load_explicit(&rel->pool_name_reservation.owner_bits,
        memory_order_acquire) == (uintptr_t)rel
        && strcmp(rel->name, "p") == 0,
        "pool replacement preserves name token");
    col_rel_destroy(candidate);
cleanup:
    if (rel) col_rel_free_contents(rel);
    delta_pool_destroy(pool);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "pool-name teardown releases credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_checked_new_like_schema_type_denial(void)
{
    col_rel_t *schema_source = col_rel_new_auto("schema-src", 1);
    col_rel_t *typed_source = col_rel_new_auto("typed-src", 1);
    const wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    CHECK(schema_source && typed_source
        && col_rel_set_column_types(typed_source, &type, 1) == 0,
        "schema/type denial sources");
    if (!schema_source || !typed_source) goto cleanup;
    uint64_t payload = private_payload_bytes(1, COL_REL_INIT_CAP, 0);
    uint64_t schema = auto_metadata_bytes(1);
    uint64_t typed = schema + sizeof(type);
    for (unsigned route = 0; route < 2; route++) {
        for (unsigned stage = 0; stage < 2; stage++) {
            const char *name = route ? "pool-stage" : "heap-stage";
            const col_rel_t *source = stage ? typed_source : schema_source;
            uint64_t identity = route ? strlen(name) + 1u
                : descriptor_bytes(name);
            uint64_t peak = identity + payload
                + (stage ? schema + typed : schema);
            for (unsigned short_budget = 0; short_budget < 2;
                short_budget++) {
                wl_columnar_memory_resolution_t resolution;
                make_resolution(&resolution, peak - short_budget);
                wl_columnar_memory_governor_ref_t *ref
                    = wl_columnar_memory_governor_ref_create(&resolution);
                delta_pool_t *pool = route
                    ? delta_pool_create(1, sizeof(col_rel_t), 4096) : NULL;
                col_rel_t *copy = (col_rel_t *)(uintptr_t)1;
                CHECK(ref && (!route || pool), "schema/type stage fixture");
                if (!ref || (route && !pool)) {
                    delta_pool_destroy(pool);
                    if (ref) wl_columnar_memory_governor_ref_release(ref);
                    continue;
                }
                int rc = route
                    ? wl_columnar_relation_pool_new_like_governed_checked(
                    &copy, pool, name, source, ref)
                    : wl_columnar_relation_new_like_governed_checked(&copy,
                        name, source, ref);
                CHECK(short_budget ? rc == ENOSPC && copy == NULL
                    : rc == 0 && copy != NULL,
                    "schema/type stage exact/one-byte typed result");
                if (copy != (col_rel_t *)(uintptr_t)1)
                    col_rel_destroy(copy);
                CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(ref)) == 0
                    && (!pool || !short_budget || pool->slot_used == 0),
                    "schema/type stage rollback/teardown");
                delta_pool_destroy(pool);
                wl_columnar_memory_governor_ref_release(ref);
            }
        }
    }
cleanup:
    col_rel_destroy(typed_source);
    col_rel_destroy(schema_source);
}

static void
test_checked_new_like_identity_overflow(void)
{
    col_rel_t *source = col_rel_new_auto("overflow-source", 1);
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 4096);
    wl_columnar_memory_resolution_t resolution;
    make_resolution(&resolution, 1u << 20);
    wl_columnar_memory_governor_ref_t *ref
        = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(source && pool && ref, "identity-overflow fixture");
    if (source && pool && ref) {
        col_rel_t *out = (col_rel_t *)(uintptr_t)1;
        CHECK(col_rel_test_set_next_identity(UINT64_MAX) == 0
            && wl_columnar_relation_new_like_governed_checked(&out,
            "overflow", source, ref) == EOVERFLOW && out == NULL,
            "checked heap reports identity overflow");
        out = (col_rel_t *)(uintptr_t)1;
        CHECK(wl_columnar_relation_pool_new_like_governed_checked(&out,
            pool, "overflow", source, ref) == EOVERFLOW && out == NULL
            && pool->slot_used == 0
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "checked pool reports identity overflow without slot use");
    }
    col_rel_destroy(source);
    delta_pool_destroy(pool);
    if (ref) wl_columnar_memory_governor_ref_release(ref);
}

static void
test_schema_metadata_admission(void)
{
    const uint64_t descriptor = descriptor_bytes("metadata-schema");
    const uint64_t names_and_arrow = auto_metadata_bytes(1);
    const uint64_t retained = private_payload_bytes(1,
            COL_REL_INIT_CAP, 0);
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
        && rel->memory_budget_denial_pending
        && wl_columnar_memory_reserved(governor) == descriptor,
        "schema metadata one-byte denial precedes allocation");
    atomic_store_explicit(&governor->usable_bytes,
        descriptor + retained + names_and_arrow - 1u,
        memory_order_release);
    CHECK(col_rel_set_schema(rel, 1, NULL) == ENOMEM
        && rel->ncols == 0 && rel->col_names == NULL && !rel->schema_ok
        && rel->metadata_reserved_bytes == 0
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
    uint64_t payload = attach_payload_bytes(rel);
    make_resolution(&resolution, descriptor + metadata + payload - 1u);
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
    make_resolution(&resolution, descriptor + metadata + payload);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(rel, ref) == 0
        && rel->metadata_reserved_bytes == metadata
        && rel->descriptor_reserved_bytes == descriptor
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor + metadata + payload,
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
    const uint64_t descriptor = sizeof(col_rel_t)
        + sizeof(wl_columnar_memory_reservation_t);
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
    const uint64_t private_bytes = private_payload_bytes(1, 64u, 0);
    const uint64_t incoming = sizeof(int64_t *) + 64u * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = NULL;
    col_rel_t *view;

    make_resolution(&resolution, 1024 * 1024);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    view = ref ? make_shared_view(ref, &source) : NULL;
    CHECK(view != NULL, "COW exact-fit setup");
    if (view) {
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        uint64_t old_reserved = wl_columnar_memory_reserved(governor);
        atomic_store_explicit(&governor->usable_bytes, old_reserved + incoming,
            memory_order_release);
        int64_t *borrowed = view->columns[0];
        int64_t value = 5;
        CHECK(col_rel_append_row(view, &value) == 0,
            "spare-capacity append COW");
        CHECK(view->col_shared == NULL, "exact-fit COW was published");
        CHECK(view->columns[0] != borrowed, "COW retained borrowed buffer");
        atomic_store_explicit(&governor->usable_bytes, 1024 * 1024,
            memory_order_release);
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

    make_resolution(&resolution, 1024 * 1024);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    source = NULL;
    view = ref ? make_shared_view(ref, &source) : NULL;
    CHECK(view != NULL, "COW denial setup");
    if (view) {
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        uint64_t old_reserved = wl_columnar_memory_reserved(governor);
        atomic_store_explicit(&governor->usable_bytes,
            old_reserved + incoming - 1u, memory_order_release);
        int64_t *borrowed = view->columns[0];
        WL_IGNORE_RESULT(col_rel_radix_sort_int64(view));
        CHECK(view->col_shared != NULL && view->columns[0] == borrowed,
            "denied COW changed ownership");
        CHECK(view->columns[0][0] == 9 && view->columns[0][1] == 1,
            "denied COW changed rows");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == old_reserved,
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
    expected = private_payload_bytes(source->ncols, source->capacity,
            timestamp_capacity);
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
    uint64_t expected = private_payload_bytes(source->ncols,
            source->capacity, 0);
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
    const uint64_t private_bytes = private_payload_bytes(2, 64u, 0);
    const uint64_t incoming = 2u * sizeof(int64_t *)
        + 2u * 64u * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = col_rel_new_auto("multi-source", 2);
    col_rel_t *view = col_rel_new_auto("multi-view", 2);
    int64_t first[] = {9, 90};
    int64_t second[] = {1, 10};
    int64_t appended[] = {5, 50};

    make_resolution(&resolution, 1024 * 1024);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && source && view, "multi-column COW setup");
    if (ref && source && view) {
        CHECK(col_rel_append_row(source, first) == 0
            && col_rel_append_row(source, second) == 0
            && col_rel_attach_memory_governor(view, ref) == 0
            && col_rel_install_shared_view(view, source) == 0,
            "multi-column shared view setup");
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        uint64_t old_reserved = wl_columnar_memory_reserved(governor);
        atomic_store_explicit(&governor->usable_bytes, old_reserved + incoming,
            memory_order_release);
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
    const uint64_t bytes = private_payload_bytes(1, capacity, capacity);
    const uint64_t incoming = sizeof(int64_t *)
        + (uint64_t)capacity * sizeof(int64_t)
        + (uint64_t)capacity * sizeof(col_delta_timestamp_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    delta_pool_t *pool;
    wl_arena_t *arena;
    col_rel_t *relation;
    int64_t value = 42;

    make_resolution(&resolution, 1024 * 1024);
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
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        atomic_store_explicit(&governor->usable_bytes,
            wl_columnar_memory_reserved(governor) + incoming,
            memory_order_release);
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
            == bytes + relation->metadata_reserved_bytes
            + relation->pool_name_reserved_bytes,
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
    const uint32_t grown_capacity = COL_REL_INIT_CAP * 2u;
    const uint64_t grown_bytes = private_payload_bytes(1, grown_capacity, 0);
    const uint64_t incoming_grid = sizeof(int64_t *)
        + (uint64_t)grown_capacity * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = NULL;
    col_rel_t *view;
    int64_t value = 11;

    make_resolution(&resolution, 1024 * 1024);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    view = ref ? make_full_shared_view(ref, &source, false) : NULL;
    CHECK(view != NULL, "append_row transition setup");
    if (view) {
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        atomic_store_explicit(&governor->usable_bytes,
            wl_columnar_memory_reserved(governor) + incoming_grid,
            memory_order_release);
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

    make_resolution(&resolution, 1024 * 1024);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    source = NULL;
    view = ref ? make_full_shared_view(ref, &source, false) : NULL;
    CHECK(view != NULL, "append_all transition setup");
    if (view) {
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        atomic_store_explicit(&governor->usable_bytes,
            wl_columnar_memory_reserved(governor) + incoming_grid,
            memory_order_release);
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

    make_resolution(&resolution, 1024 * 1024);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    source = NULL;
    view = ref ? make_full_shared_view(ref, &source, true) : NULL;
    CHECK(view != NULL, "timestamp transition setup");
    if (view) {
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        atomic_store_explicit(&governor->usable_bytes,
            wl_columnar_memory_reserved(governor) + incoming_grid
            + (uint64_t)grown_capacity * sizeof(col_delta_timestamp_t),
            memory_order_release);
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
    const uint64_t incoming = sizeof(int64_t *)
        + (uint64_t)COL_REL_INIT_CAP * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    delta_pool_t *pool = NULL;
    wl_arena_t *arena = NULL;
    col_rel_t *relation = NULL;
    int64_t value = 7;

    make_resolution(&resolution, 1024 * 1024);
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
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        uint64_t old_reserved = wl_columnar_memory_reserved(governor);
        atomic_store_explicit(&governor->usable_bytes,
            old_reserved + incoming - 1u, memory_order_release);
        CHECK(col_rel_promote_arena_admitted(relation) == ENOMEM,
            "arena denial result");
        CHECK(relation->arena_owned && relation->columns[0] == old_columns
            && relation->columns[0][0] == value,
            "arena denial changed relation");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == old_reserved,
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
    const uint32_t grown_capacity = COL_REL_INIT_CAP * 2u;
    const uint64_t exact_bytes = private_payload_bytes(1,
            grown_capacity, 0);
    const uint64_t incoming = sizeof(int64_t *)
        + (uint64_t)grown_capacity * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    delta_pool_t *pool = NULL;
    wl_arena_t *arena = NULL;
    col_rel_t *dst;
    col_rel_t *src;
    int64_t value = 53;

    make_resolution(&resolution, 1024 * 1024);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    dst = ref ? make_full_arena_relation(&pool, &arena,
            "arena-append-exact", value) : NULL;
    src = col_rel_new_auto("arena-append-source", 1);
    CHECK(dst && src, "arena append_all exact-fit setup");
    if (dst && src) {
        CHECK(col_rel_attach_memory_governor(dst, ref) == 0,
            "arena append_all exact-fit governor");
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        atomic_store_explicit(&governor->usable_bytes,
            wl_columnar_memory_reserved(governor) + incoming,
            memory_order_release);
        CHECK(col_rel_append_row(src, &value) == 0
            && col_rel_append_all(dst, src, arena) == 0,
            "arena append_all exact-fit admission");
        CHECK(!dst->arena_owned && dst->capacity == COL_REL_INIT_CAP * 2u
            && dst->nrows == COL_REL_INIT_CAP + 1u,
            "arena append_all exact-fit state");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == exact_bytes + dst->metadata_reserved_bytes
            + dst->pool_name_reserved_bytes,
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

    make_resolution(&resolution, 1024 * 1024);
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
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        uint64_t old_reserved = wl_columnar_memory_reserved(governor);
        atomic_store_explicit(&governor->usable_bytes,
            old_reserved + incoming - 1u, memory_order_release);
        CHECK(col_rel_append_row(src, &value) == 0
            && col_rel_append_all(dst, src, arena) == ENOMEM,
            "arena append_all one-byte denial");
        CHECK(dst->arena_owned && dst->columns[0] == old_column
            && dst->capacity == old_capacity && dst->nrows == old_rows,
            "arena append_all denial changed state");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == old_reserved,
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
    const uint32_t grown_capacity = COL_REL_INIT_CAP * 2u;
    const uint64_t exact_bytes = private_payload_bytes(1,
            grown_capacity, grown_capacity);
    const uint64_t incoming = sizeof(int64_t *)
        + (uint64_t)grown_capacity * (sizeof(int64_t)
        + sizeof(col_delta_timestamp_t));
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *dst;
    col_rel_t *src;
    int64_t value = 61;

    make_resolution(&resolution, 1024 * 1024);
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
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        atomic_store_explicit(&governor->usable_bytes,
            wl_columnar_memory_reserved(governor) + incoming,
            memory_order_release);
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

    make_resolution(&resolution, 1024 * 1024);
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
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        uint64_t old_reserved = wl_columnar_memory_reserved(governor);
        atomic_store_explicit(&governor->usable_bytes,
            old_reserved + incoming - 1u, memory_order_release);
        CHECK(col_rel_append_all(dst, src, NULL) == ENOMEM,
            "heap append_all one-byte denial");
        CHECK(dst->columns[0] == old_columns
            && dst->timestamps == old_timestamps
            && dst->capacity == old_capacity && dst->nrows == old_rows
            && dst->storage_generation == old_storage,
            "heap append_all denial changed state");
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == old_reserved,
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
    const uint64_t incoming = sizeof(int64_t *)
        + (uint64_t)(COL_REL_INIT_CAP * 2u) * sizeof(int64_t);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source = NULL;
    col_rel_t *view;
    int64_t value = 23;

    make_resolution(&resolution, 1024 * 1024);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    view = ref ? make_full_shared_view(ref, &source, false) : NULL;
    CHECK(view != NULL, "COW capacity denial setup");
    if (view) {
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(ref);
        uint64_t old_reserved = wl_columnar_memory_reserved(governor);
        atomic_store_explicit(&governor->usable_bytes,
            old_reserved + incoming - 1u, memory_order_release);
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
            == old_reserved,
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
    uint64_t alias_payload = 0;
    CHECK(view->nrows == 0 && view->capacity == 0 && view->col_shared
        && view->col_shared[0] && !view->columns[0]
        && col_rel_retained_live_bytes(view, &alias_payload)
        && alias_payload == bytes,
        "empty shared lane retains exact physical timestamp charge");
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
    const uint64_t bytes = private_payload_bytes(1, COL_REL_INIT_CAP,
            physical);
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *source = NULL;
    col_rel_t *view = NULL;
    int64_t value = 9;

    const uint64_t old_timestamp_bytes = (uint64_t)physical
        * sizeof(col_delta_timestamp_t);
    make_resolution(&resolution, 1u << 20);
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
    uint64_t base = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    uint64_t incoming_grid = bytes - old_timestamp_bytes
        - sizeof(int64_t);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, base + incoming_grid - 1u,
        memory_order_release);
    int64_t *old_column = view->columns[0];
    bool *old_shared = view->col_shared;
    CHECK(col_rel_cow_unshare(view, 0) == ENOMEM
        && view->columns[0] == old_column && view->col_shared == old_shared
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor_bytes("skew-view")
        + view->metadata_reserved_bytes + old_timestamp_bytes
        + sizeof(int64_t),
        "retained timestamp capacity denial preserves state");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        base + incoming_grid, memory_order_release);
    CHECK(col_rel_cow_unshare(view, 0) == 0
        && view->retained_reserved_bytes == bytes
        && view->timestamp_capacity == physical
        && view->columns[0] != old_column
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == bytes + descriptor_bytes("skew-view")
        + view->metadata_reserved_bytes,
        "exact physical timestamp capacity admits COW retry");
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "physical timestamp COW teardown releases exact credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_governed_shared_view_timestamp_transaction(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *source = col_rel_new_auto("shared-ts-source", 1);
    col_rel_t *dst = col_rel_new_auto("shared-ts-dst", 1);
    wl_columnar_memory_governor_ref_t *ref = NULL;
    int64_t value = 23;
    make_resolution(&resolution, 1u << 20);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && source && dst && col_rel_append_row(source, &value) == 0
        && col_rel_enable_timestamps(source) == 0,
        "governed shared timestamp fixture");
    if (!ref || !source || !dst || !source->timestamps)
        goto cleanup;
    uint32_t physical = source->capacity + 7u;
    col_delta_timestamp_t *expanded = realloc(source->timestamps,
            (size_t)physical * sizeof(*expanded));
    CHECK(expanded != NULL, "shared source physical timestamp expansion");
    if (!expanded)
        goto cleanup;
    source->timestamps = expanded;
    source->timestamp_capacity = physical;
    CHECK(col_rel_attach_memory_governor(dst, ref) == 0,
        "governed shared timestamp destination attach");
    if (!dst->memory_governor)
        goto cleanup;
    uint64_t base = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    uint64_t new_metadata = auto_metadata_bytes(1)
        + shared_table_bytes(1);
    uint64_t new_retained = (uint64_t)physical
        * sizeof(col_delta_timestamp_t) + sizeof(int64_t);
    uint64_t old_retained = dst->retained_reserved_bytes;
    int64_t **old_columns = dst->columns;
    uint64_t old_view = dst->view_generation;
    uint64_t old_storage = dst->storage_generation;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, base + new_metadata - 1u,
        memory_order_release);
    CHECK(col_rel_install_shared_view(dst, source) == ENOSPC
        && dst->columns == old_columns && dst->col_shared == NULL
        && dst->view_generation == old_view
        && dst->storage_generation == old_storage
        && source->storage_alias_borrows == 0u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == base,
        "shared metadata one-byte denial preserves old image");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, base + new_metadata + new_retained - 1u,
        memory_order_release);
    CHECK(col_rel_install_shared_view(dst, source) == ENOSPC
        && dst->columns == old_columns && dst->col_shared == NULL
        && dst->retained_reserved_bytes == old_retained
        && source->storage_alias_borrows == 0u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == base,
        "shared physical timestamp one-byte denial rolls metadata back");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, base + new_metadata + new_retained,
        memory_order_release);
    wl_columnar_relation_test_fail_next_metadata_alloc();
    CHECK(col_rel_install_shared_view(dst, source) == ENOMEM
        && dst->columns == old_columns && dst->col_shared == NULL
        && dst->view_generation == old_view
        && source->storage_alias_borrows == 0u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == base,
        "shared post-admission allocator failure preserves old image");
    CHECK(col_rel_install_shared_view(dst, source) == 0
        && dst->timestamp_capacity == physical
        && dst->retained_reserved_bytes == new_retained
        && dst->metadata_reserved_bytes == new_metadata
        && source->storage_alias_borrows == 1u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == dst->descriptor_reserved_bytes + new_metadata + new_retained,
        "shared exact physical timestamp image publishes once");
cleanup:
    col_rel_destroy(dst);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "shared timestamp transaction teardown releases credits");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_governed_shared_view_dst_only_timestamps(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *source = col_rel_new_auto("dst-ts-source", 1);
    col_rel_t *dst = col_rel_new_auto("dst-ts-view", 1);
    int64_t value = 5;
    make_resolution(&resolution, 1u << 20);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && source && dst && col_rel_append_row(source, &value) == 0
        && col_rel_enable_timestamps(dst) == 0,
        "destination-only timestamp fixture");
    if (!ref || !source || !dst || !dst->timestamps)
        goto cleanup;
    CHECK(col_rel_attach_memory_governor(dst, ref) == 0
        && col_rel_reserve_capacity_admitted(dst, dst->capacity, NULL) == 0,
        "destination-only old retained image admission");
    if (!dst->retained_reserved_bytes)
        goto cleanup;
    uint64_t base = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    uint64_t new_metadata = auto_metadata_bytes(1)
        + shared_table_bytes(1);
    uint64_t new_retained = (uint64_t)source->capacity
        * sizeof(col_delta_timestamp_t) + sizeof(int64_t);
    int64_t **old_columns = dst->columns;
    col_delta_timestamp_t *old_timestamps = dst->timestamps;
    uint64_t old_retained = dst->retained_reserved_bytes;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, base + new_metadata + new_retained - 1u,
        memory_order_release);
    CHECK(col_rel_install_shared_view(dst, source) == ENOSPC
        && dst->columns == old_columns && dst->timestamps == old_timestamps
        && dst->retained_reserved_bytes == old_retained
        && source->storage_alias_borrows == 0u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == base,
        "destination-only timestamp one-byte overlap denial");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, base + new_metadata + new_retained,
        memory_order_release);
    CHECK(col_rel_install_shared_view(dst, source) == 0
        && dst->timestamps != old_timestamps
        && dst->timestamp_capacity == source->capacity
        && dst->retained_reserved_bytes == new_retained
        && source->storage_alias_borrows == 1u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == dst->descriptor_reserved_bytes + new_metadata + new_retained,
        "destination-only timestamp exact overlap publishes once");
cleanup:
    col_rel_destroy(dst);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "destination-only timestamp teardown releases credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_shared_view_dst_only_timestamp_extent(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *source = col_rel_new_auto("long-source", 1);
    col_rel_t *dst = col_rel_new_auto("short-ts-view", 1);
    int64_t value = 5;
    make_resolution(&resolution, 1u << 20);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && source && dst && col_rel_enable_timestamps(dst) == 0,
        "destination-only timestamp extent fixture");
    if (!ref || !source || !dst || !dst->timestamps)
        goto cleanup;
    uint32_t old_timestamp_capacity = dst->timestamp_capacity;
    for (uint32_t row = 0; row <= old_timestamp_capacity; row++) {
        value = (int64_t)row;
        if (col_rel_append_row(source, &value) != 0) {
            CHECK(false, "long un-timestamped source fixture");
            goto cleanup;
        }
    }
    CHECK(source->nrows > old_timestamp_capacity
        && source->capacity >= source->nrows
        && col_rel_attach_memory_governor(dst, ref) == 0
        && col_rel_reserve_capacity_admitted(dst, dst->capacity, NULL) == 0,
        "attach short destination timestamp buffer");
    if (!dst->memory_governor || !dst->retained_reserved_bytes)
        goto cleanup;
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    uint64_t new_metadata = auto_metadata_bytes(1)
        + shared_table_bytes(1);
    uint64_t new_retained = (uint64_t)source->nrows
        * sizeof(col_delta_timestamp_t) + sizeof(int64_t);
    int64_t **old_columns = dst->columns;
    col_delta_timestamp_t *old_timestamps = dst->timestamps;
    uint64_t old_retained = dst->retained_reserved_bytes;
    uint64_t old_view = dst->view_generation;
    uint64_t old_storage = dst->storage_generation;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, baseline + new_metadata + new_retained - 1u,
        memory_order_release);
    CHECK(col_rel_install_shared_view(dst, source) == ENOSPC
        && dst->columns == old_columns
        && dst->timestamps == old_timestamps
        && dst->timestamp_capacity == old_timestamp_capacity
        && dst->retained_reserved_bytes == old_retained
        && dst->view_generation == old_view
        && dst->storage_generation == old_storage
        && source->storage_alias_borrows == 0u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == baseline,
        "destination-only extent one-byte denial preserves image");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, baseline + new_metadata + new_retained,
        memory_order_release);
    uint64_t live_bytes = 0;
    CHECK(col_rel_install_shared_view(dst, source) == 0
        && dst->nrows == source->nrows
        && dst->timestamp_capacity == source->nrows
        && dst->timestamp_capacity >= dst->nrows
        && dst->timestamps != old_timestamps
        && dst->retained_reserved_bytes == new_retained
        && col_rel_retained_live_bytes(dst, &live_bytes)
        && live_bytes == new_retained
        && source->storage_alias_borrows == 1u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == dst->descriptor_reserved_bytes + new_metadata + new_retained,
        "destination-only exact extent fits and publishes valid shape");
cleanup:
    col_rel_destroy(dst);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "destination-only extent teardown releases credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_shared_view_narrow_timestamp_appends(void)
{
    for (int mode = 0; mode < 4; mode++) {
        wl_columnar_memory_resolution_t resolution;
        col_rel_t *source = col_rel_new_auto("narrow-source", 1);
        col_rel_t *view = col_rel_new_auto("narrow-view", 1);
        col_rel_t *suffix = col_rel_new_auto("narrow-suffix", 1);
        wl_columnar_memory_governor_ref_t *ref;
        int64_t value = 7;
        int64_t batch[] = { 8, 9 };
        bool denied = false;
        make_resolution(&resolution, 1u << 20);
        ref = wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(ref && source && view && suffix
            && col_rel_append_row(source, &value) == 0
            && col_rel_enable_timestamps(source) == 0
            && col_rel_append_row(suffix, &batch[0]) == 0
            && col_rel_append_row(suffix, &batch[1]) == 0,
            "narrow timestamp append fixture");
        if (!ref || !source || !view || !suffix || !source->timestamps)
            goto cleanup;
        if (mode == 3)
            CHECK(col_rel_enable_timestamps(suffix) == 0,
                "timestamped bulk source fixture");
        CHECK(source->capacity > source->nrows,
            "source has spare column capacity for timestamp skew");
        col_delta_timestamp_t *narrow = realloc(source->timestamps,
                sizeof(*narrow));
        CHECK(narrow != NULL, "shrink source physical timestamp buffer");
        if (!narrow)
            goto cleanup;
        source->timestamps = narrow;
        source->timestamp_capacity = source->nrows;
        CHECK(col_rel_attach_memory_governor(view, ref) == 0
            && col_rel_install_shared_view(view, source) == 0,
            "install governed narrow timestamp view");
        if (!view->col_shared)
            goto cleanup;
        uint64_t baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref));
        uint64_t replacement = private_payload_bytes(1, view->capacity,
                view->capacity);
        uint64_t incoming = replacement - sizeof(int64_t);
        int64_t *old_column = view->columns[0];
        col_delta_timestamp_t *old_timestamps = view->timestamps;
        uint64_t old_storage = view->storage_generation;
        uint64_t old_view = view->view_generation;
        uint64_t old_retained = view->retained_reserved_bytes;
        uint64_t old_metadata = view->metadata_reserved_bytes;
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                ref)->usable_bytes, baseline + incoming - 1u,
            memory_order_release);
        int rc = mode == 0 ? col_rel_append_row(view, &batch[0])
            : mode == 1 ? col_rel_append_rows_atomic(view, batch, 2u, 1u,
                &denied)
            : col_rel_append_all(view, suffix, NULL);
        CHECK(rc == ENOMEM && view->nrows == 1u
            && view->columns[0] == old_column
            && view->timestamps == old_timestamps
            && view->timestamp_capacity == 1u
            && view->col_shared != NULL
            && view->storage_generation == old_storage
            && view->view_generation == old_view
            && view->retained_reserved_bytes == old_retained
            && view->metadata_reserved_bytes == old_metadata
            && source->storage_alias_borrows == 1u
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == baseline,
            "narrow timestamp append one-byte denial is transactional");
        if (mode == 1)
            CHECK(denied, "atomic narrow timestamp batch reports denial");
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                ref)->usable_bytes, baseline + incoming,
            memory_order_release);
        rc = mode == 0 ? col_rel_append_row(view, &batch[0])
            : mode == 1 ? col_rel_append_rows_atomic(view, batch, 2u, 1u,
                &denied)
            : col_rel_append_all(view, suffix, NULL);
        CHECK(rc == 0 && view->nrows == (mode == 0 ? 2u : 3u)
            && view->timestamp_capacity == view->capacity
            && view->col_shared == NULL
            && view->columns[0] != old_column
            && view->timestamps != old_timestamps
            && view->retained_reserved_bytes == replacement
            && source->storage_alias_borrows == 0u
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
            == view->descriptor_reserved_bytes
            + view->metadata_reserved_bytes + replacement,
            "narrow timestamp append exact fit grows physical buffer");
cleanup:
        col_rel_destroy(view);
        col_rel_destroy(suffix);
        col_rel_destroy(source);
        if (ref) {
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(ref)) == 0,
                "narrow timestamp append teardown releases credit");
            wl_columnar_memory_governor_ref_release(ref);
        }
    }
}

static void
test_owned_narrow_timestamp_append(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *rel = col_rel_new_auto("owned-narrow", 1);
    int64_t value = 11;
    make_resolution(&resolution, 1u << 20);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && rel && col_rel_append_row(rel, &value) == 0
        && col_rel_enable_timestamps(rel) == 0,
        "owned narrow timestamp fixture");
    if (!ref || !rel || !rel->timestamps)
        goto cleanup;
    col_delta_timestamp_t *narrow = realloc(rel->timestamps,
            sizeof(*narrow));
    CHECK(narrow != NULL, "owned physical timestamp shrink");
    if (!narrow)
        goto cleanup;
    rel->timestamps = narrow;
    rel->timestamp_capacity = 1u;
    CHECK(col_rel_attach_memory_governor(rel, ref) == 0,
        "attach owned narrow timestamp relation");
    if (!rel->memory_governor)
        goto cleanup;
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    uint64_t replacement = private_payload_bytes(1, rel->capacity,
            rel->capacity);
    uint64_t incoming = replacement - sizeof(int64_t);
    int64_t *old_column = rel->columns[0];
    col_delta_timestamp_t *old_timestamps = rel->timestamps;
    uint64_t old_storage = rel->storage_generation;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, baseline + incoming - 1u,
        memory_order_release);
    CHECK(col_rel_append_row(rel, &value) == ENOMEM
        && rel->nrows == 1u && rel->columns[0] == old_column
        && rel->timestamps == old_timestamps
        && rel->timestamp_capacity == 1u
        && rel->storage_generation == old_storage
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == baseline,
        "owned narrow timestamp one-byte denial preserves image");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, baseline + incoming,
        memory_order_release);
    CHECK(col_rel_append_row(rel, &value) == 0
        && rel->nrows == 2u
        && rel->timestamp_capacity == rel->capacity
        && rel->retained_reserved_bytes == replacement
        && rel->columns[0] != old_column
        && rel->timestamps != old_timestamps,
        "owned narrow timestamp exact-fit transition");
cleanup:
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "owned narrow timestamp teardown releases credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_legacy_shared_view_attach(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *source = col_rel_new_auto("legacy-shared-source", 1);
    col_rel_t *dst = col_rel_new_auto("legacy-shared-dst", 1);
    int64_t value = 3;
    CHECK(source && dst && col_rel_append_row(source, &value) == 0
        && col_rel_enable_timestamps(source) == 0
        && col_rel_install_shared_view(dst, source) == 0,
        "legacy shared-view attach fixture");
    if (!source || !dst || !dst->col_shared)
        goto cleanup;
    uint64_t exact = descriptor_bytes("legacy-shared-dst")
        + metadata_bytes(dst)
        + (uint64_t)dst->timestamp_capacity
        * sizeof(col_delta_timestamp_t) + sizeof(int64_t);
    make_resolution(&resolution, exact - 1u);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(dst, ref) == ENOSPC
        && dst->memory_governor == NULL
        && dst->metadata_reserved_bytes == 0
        && dst->retained_reserved_bytes == 0
        && source->storage_alias_borrows == 1u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "legacy shared-view attach one-byte denial rolls all tokens back");
    if (ref) wl_columnar_memory_governor_ref_release(ref);
    make_resolution(&resolution, exact);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(dst, ref) == 0
        && dst->metadata_reserved_bytes == metadata_bytes(dst)
        && dst->retained_reserved_bytes == (uint64_t)dst->timestamp_capacity
        * sizeof(col_delta_timestamp_t) + sizeof(int64_t)
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == exact,
        "legacy shared-view attach exact physical image");
    col_rel_destroy(dst);
    dst = NULL;
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "legacy shared-view attach teardown releases credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
cleanup:
    col_rel_destroy(dst);
    col_rel_destroy(source);
}

static void
test_shared_table_retirement_paths(void)
{
    wl_columnar_memory_resolution_t resolution;
    make_resolution(&resolution, 1u << 20);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    col_rel_t *source = col_rel_new_auto("table-owner", 1);
    col_rel_t *view = col_rel_new_auto("table-view", 1);
    col_rel_t *typed = col_rel_new_auto("table-typed", 1);
    int64_t value = 11;
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    CHECK(ref && source && view && typed
        && col_rel_append_row(source, &value) == 0
        && col_rel_append_row(typed, &value) == 0
        && col_rel_set_column_types(typed, &type, 1) == 0
        && col_rel_attach_memory_governor(view, ref) == 0
        && col_rel_install_shared_view(view, source) == 0,
        "shared-table retirement fixture");
    if (!ref || !source || !view || !typed || !view->col_shared)
        goto cleanup;
    uint64_t table = shared_table_bytes(1);
    CHECK(view->metadata_reserved_bytes == metadata_bytes(view)
        && view->metadata_reserved_bytes >= table,
        "shared pointer and flag tables belong to metadata token");
    CHECK(col_rel_append_all(view, typed, NULL) == 0
        && view->col_shared == NULL
        && view->column_types != NULL
        && view->metadata_reserved_bytes == metadata_bytes(view)
        && source->storage_alias_borrows == 0u,
        "typed append retires shared table credit before metadata publish");
    col_rel_destroy(view);
    view = col_rel_new_auto("table-detach", 1);
    CHECK(view && col_rel_attach_memory_governor(view, ref) == 0
        && col_rel_install_shared_view(view, source) == 0,
        "shared-table delta-detach setup");
    if (view && view->col_shared) {
        uint64_t shared_metadata = view->metadata_reserved_bytes;
        CHECK(wl_columnar_relation_delta_detach(view,
            view->relation_identity) == 0
            && view->col_shared == NULL && view->columns == NULL
            && view->metadata_reserved_bytes == shared_metadata - table
            && source->storage_alias_borrows == 0u,
            "delta detach frees tables before metadata downsize");
    }
    col_rel_destroy(view);
    view = NULL;
    col_rel_t *empty_source = col_rel_new_auto("table-empty-owner", 1);
    view = col_rel_new_auto("table-compact", 1);
    CHECK(empty_source && view
        && col_rel_attach_memory_governor(view, ref) == 0
        && col_rel_install_shared_view(view, empty_source) == 0,
        "shared-table empty compaction setup");
    if (view && view->col_shared) {
        uint64_t shared_metadata = view->metadata_reserved_bytes;
        CHECK(col_rel_compact(view) == 0 && view->col_shared == NULL
            && view->metadata_reserved_bytes == shared_metadata - table
            && empty_source->storage_alias_borrows == 0u,
            "empty compaction retires shared table credit once");
    }
    col_rel_destroy(empty_source);
cleanup:
    col_rel_destroy(view);
    col_rel_destroy(typed);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "shared-table retirement teardown releases all credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_replacement_expands_narrow_timestamps(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *target = col_rel_new_auto("narrow-target", 1);
    col_rel_t *candidate = col_rel_new_auto("narrow-candidate", 1);
    col_rel_replacement_t replacement = { 0 };
    wl_columnar_memory_governor_ref_t *ref = NULL;
    int64_t value = 17;

    CHECK(target && candidate && col_rel_append_row(candidate, &value) == 0
        && col_rel_enable_timestamps(candidate) == 0,
        "narrow replacement fixture");
    if (!target || !candidate || !candidate->timestamps)
        goto cleanup;
    col_delta_timestamp_t *narrow = realloc(candidate->timestamps,
            sizeof(*narrow));
    CHECK(narrow != NULL, "narrow timestamp allocation");
    if (!narrow)
        goto cleanup;
    candidate->timestamps = narrow;
    candidate->timestamp_capacity = 1u;
    uint64_t staged_bytes = private_payload_bytes(1,
            candidate->capacity, candidate->capacity);
    uint64_t target_payload = attach_payload_bytes(target);
    uint64_t exact = descriptor_bytes("narrow-target")
        + metadata_bytes(target) + target_payload
        + metadata_bytes(candidate) + staged_bytes;
    make_resolution(&resolution, exact - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(target, ref) == 0,
        "narrow replacement governor");
    if (!ref || !target->memory_governor)
        goto cleanup;
    CHECK(col_rel_prepare_replacement(target, candidate, &replacement)
        == ENOMEM && !replacement.staged
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor_bytes("narrow-target")
        + target->metadata_reserved_bytes + target_payload,
        "narrow source admits expanded staged timestamps one byte short");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, exact, memory_order_release);
    CHECK(col_rel_prepare_replacement(target, candidate, &replacement) == 0
        && replacement.staged
        && replacement.reserved_bytes == staged_bytes
        && replacement.staged->timestamp_capacity == candidate->capacity,
        "narrow source replacement exact staged physical width");
    col_rel_discard_replacement(&replacement);
    CHECK(col_rel_prepare_replacement(target, candidate, &replacement) == 0,
        "narrow replacement retries after discard");
    if (replacement.staged)
        col_rel_commit_replacement_locked(target, &replacement);
    col_rel_discard_replacement(&replacement);
    CHECK(target->retained_reserved_bytes == staged_bytes
        && target->timestamp_capacity == candidate->capacity,
        "narrow replacement commits physical width");
cleanup:
    col_rel_discard_replacement(&replacement);
    col_rel_destroy(candidate);
    col_rel_destroy(target);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "narrow replacement teardown releases exact credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_timestamp_shape_attach_invariant(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *rel = col_rel_new_auto("bad-timestamp", 1);
    make_resolution(&resolution, 1u << 20);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(rel && ref && col_rel_enable_timestamps(rel) == 0,
        "timestamp invariant fixture");
    if (!rel || !ref || !rel->timestamps)
        goto cleanup;
    uint32_t physical = rel->timestamp_capacity;
    rel->timestamp_capacity = 0;
    CHECK(col_rel_attach_memory_governor(rel, ref) == EINVAL
        && rel->memory_governor == NULL
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "timestamp pointer without physical capacity rejects attach");
    rel->timestamp_capacity = physical;
    CHECK(col_rel_attach_memory_governor(rel, ref) == 0,
        "valid physical capacity attaches after rejection");
cleanup:
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "timestamp invariant teardown releases credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_timestamp_only_retained_admission(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *rel = col_rel_new_auto("timestamp-only", 0);
    make_resolution(&resolution, 1u << 20);
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(rel && ref, "timestamp-only admission fixture");
    if (!rel || !ref)
        goto cleanup;
    rel->timestamps = calloc(3u, sizeof(*rel->timestamps));
    rel->timestamp_capacity = rel->timestamps ? 3u : 0u;
    uint64_t bytes = 3u * sizeof(col_delta_timestamp_t);
    uint64_t exact = descriptor_bytes("timestamp-only")
        + metadata_bytes(rel) + bytes;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, exact - 1u,
        memory_order_release);
    CHECK(rel->timestamps && col_rel_attach_memory_governor(rel, ref)
        == ENOSPC && rel->memory_governor == NULL
        && rel->retained_reserved_bytes == 0
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "timestamp-only physical bytes denied one byte short");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, exact, memory_order_release);
    CHECK(col_rel_attach_memory_governor(rel, ref) == 0
        && rel->retained_reserved_bytes == bytes
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == exact,
        "timestamp-only physical bytes admitted at exact fit");
cleanup:
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "timestamp-only teardown releases exact credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
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
        const col_rel_logical_col_t logical = {
            WIRELOG_COMPOUND_KIND_SIDE, 2u, 1u
        };
        CHECK(col_rel_apply_compound_schema(src, &logical, 1u) == 0,
            "source compound-map setup");
        if (route == 2) {
            occupied = col_rel_pool_new_like(pool, "occupied", src);
            CHECK(occupied && occupied->pool_owned, "actual pool exhaustion");
            if (!occupied) goto cleanup;
        }
        uint64_t baseline = wl_columnar_memory_reserved(g);
        uint64_t payload = private_payload_bytes(1, COL_REL_INIT_CAP, 0);
        uint64_t identity_floor = route == 1 ? strlen("retry") + 1u
            : descriptor_bytes("retry");
        uint64_t metadata_final = auto_metadata_bytes(1)
            + sizeof(uint32_t);
        uint64_t metadata_floor = auto_metadata_bytes(1)
            + metadata_final;
        uint64_t peak = baseline + payload + identity_floor
            + metadata_floor;
        uint32_t used = pool ? pool->slot_used : 0;
        atomic_store_explicit(&g->usable_bytes,
            peak - 1u,
            memory_order_release);
        clone = wl_columnar_relation_pool_new_like_governed(pool, "denied", src,
                ref);
        CHECK(!clone, "constructor cannot escape admission via fallback");
        CHECK(!pool || pool->slot_used == used, "failed slot restored");
        CHECK(wl_columnar_memory_reserved(g) == baseline,
            "failed reservation restored");
        if (clone) goto cleanup;
        atomic_store_explicit(&g->usable_bytes,
            peak,
            memory_order_release);
        clone = wl_columnar_relation_pool_new_like_governed(pool, "retry", src,
                ref);
        CHECK(clone && clone->memory_governor == ref,
            "effective governor retained");
        if (!clone) goto cleanup;
        CHECK(clone->compound_arity_len == 1
            && clone->compound_arity_map
            && clone->compound_arity_map != src->compound_arity_map
            && clone->compound_arity_map[0] == 1
            && clone->metadata_reserved_bytes == metadata_final,
            "heap or pool clone admits its private compound map");
        CHECK(clone->pool_owned == (route == 1), "expected pool or heap route");
        CHECK(clone->pool_name_reserved_bytes
            == (route == 1 ? strlen("retry") + 1u : 0u),
            "pool clone name admitted separately");
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
        + sizeof(int64_t *) + sizeof(restored);
    overwrite_expected_live = private_payload_bytes(1, 100, 0);
    overwrite_expected_ledger = sizeof(restored);
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
        == overwrite_expected_live + rel->descriptor_reserved_bytes
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
        == descriptor_bytes("view") + view->metadata_reserved_bytes
        + view->retained_reserved_bytes,
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
test_terminal_release_policy_child(bool descriptor_release)
{
    pid_t child = fork();
    CHECK(child >= 0, "terminal release test fork");
    if (child < 0) return;
    if (child == 0) {
        wl_columnar_memory_resolution_t resolution;
        make_resolution(&resolution, 1024 * 1024);
        wl_columnar_memory_governor_ref_t *ref =
            wl_columnar_memory_governor_ref_create(&resolution);
        col_rel_t *rel = NULL;
        if (!ref)
            _exit(10);
        if (descriptor_release) {
            if (wl_columnar_relation_alloc_governed(&rel,
                "terminal-release", ref) != 0)
                _exit(10);
            wl_columnar_memory_governor_test_refuse_next_release();
            col_rel_destroy(rel);
        } else {
            int64_t value = 3;
            rel = col_rel_new_auto("terminal-release", 1);
            if (!rel || col_rel_append_row(rel, &value) != 0
                || col_rel_attach_memory_governor(rel, ref) != 0)
                _exit(10);
            /* Retire the physical payload before asking the token to
             * release its credit. A refusal is terminal. */
            free(rel->columns[0]);
            free((void *)rel->columns);
            free(rel->row_scratch);
            rel->columns = NULL;
            rel->row_scratch = NULL;
            rel->capacity = 0;
            rel->nrows = 0;
            wl_columnar_memory_governor_test_refuse_next_release();
            col_rel_retire_payload_credit(rel);
        }
        _exit(0);
    }
    int status = 0;
    CHECK(waitpid(child, &status, 0) == child
        && WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT,
        descriptor_release
            ? "descriptor release refusal follows terminal policy"
            : "retained release refusal follows terminal policy");
}

static void
test_terminal_release_policy(void)
{
    test_terminal_release_policy_child(false);
    test_terminal_release_policy_child(true);
}
#endif

static void
test_compound_map_admission(void)
{
    wl_columnar_memory_resolution_t resolution;
    const col_rel_logical_col_t logical = {
        WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u
    };
    col_rel_t *rel = col_rel_new_auto("compound-map", 2);
    wl_columnar_memory_governor_ref_t *ref = NULL;
    uint64_t base, old_meta, new_meta, old_view, old_storage;
    uint32_t *old_map;
    if (!rel) {
        CHECK(false, "compound-map fixture allocation");
        return;
    }
    old_meta = metadata_bytes(rel);
    new_meta = old_meta + sizeof(uint32_t);
    base = descriptor_bytes("compound-map") + old_meta
        + attach_payload_bytes(rel);
    make_resolution(&resolution, base + new_meta - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(rel, ref) == 0,
        "compound-map base attach");
    if (!ref || rel->memory_governor != ref)
        goto cleanup;
    old_view = rel->view_generation;
    old_storage = rel->storage_generation;
    CHECK(col_rel_apply_compound_schema(rel, &logical, 1u) == ENOMEM
        && rel->compound_arity_map == NULL
        && rel->compound_arity_len == 0
        && rel->view_generation == old_view
        && rel->storage_generation == old_storage
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == base,
        "compound-map one-byte denial preserves old image");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, base + new_meta,
        memory_order_release);
    CHECK(col_rel_apply_compound_schema(rel, &logical, 1u) == 0
        && rel->compound_arity_len == 1
        && rel->metadata_reserved_bytes == metadata_bytes(rel)
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == base + sizeof(uint32_t),
        "compound-map exact-fit installation");
    old_map = rel->compound_arity_map;
    old_view = rel->view_generation;
    old_storage = rel->storage_generation;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        descriptor_bytes("compound-map") + attach_payload_bytes(rel)
        + 2u * new_meta - 1u,
        memory_order_release);
    CHECK(col_rel_apply_compound_schema(rel, &logical, 1u) == ENOMEM
        && rel->compound_arity_map == old_map
        && rel->view_generation == old_view
        && rel->storage_generation == old_storage,
        "compound-map replacement one-byte overlap denial");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        descriptor_bytes("compound-map") + attach_payload_bytes(rel)
        + 2u * new_meta,
        memory_order_release);
    wl_columnar_relation_test_fail_next_metadata_alloc();
    CHECK(col_rel_apply_compound_schema(rel, &logical, 1u) == ENOMEM
        && rel->compound_arity_map == old_map
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == base + sizeof(uint32_t),
        "compound-map post-admission allocator failure rolls back");
    CHECK(col_rel_apply_compound_schema(rel, &logical, 1u) == 0
        && rel->compound_arity_map != old_map
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == base + sizeof(uint32_t),
        "compound-map overlap retry releases old credit");
cleanup:
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "compound-map teardown releases metadata token");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_compound_map_attach_and_shared_view(void)
{
    const col_rel_logical_col_t logical = {
        WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u
    };
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *rel = col_rel_new_auto("attached-map", 2);
    col_rel_t *plain = col_rel_new_auto("plain", 2);
    col_rel_t *add = col_rel_new_auto("map-add", 2);
    wl_columnar_memory_governor_ref_t *ref = NULL;
    wl_columnar_memory_governor_ref_t *add_ref = NULL;
    uint64_t exact, view;
    CHECK(rel && plain && add && col_rel_apply_compound_schema(rel,
        &logical, 1u) == 0, "compound-map attach setup");
    if (!rel || !plain || !add || rel->compound_arity_len != 1)
        goto cleanup;
    exact = descriptor_bytes("attached-map") + metadata_bytes(rel)
        + attach_payload_bytes(rel);
    make_resolution(&resolution, exact - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(rel, ref) == ENOSPC
        && rel->memory_governor == NULL
        && rel->metadata_reserved_bytes == 0
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "compound-map legacy attach one-byte denial");
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
    make_resolution(&resolution, exact);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(rel, ref) == 0
        && rel->metadata_reserved_bytes == metadata_bytes(rel)
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == exact,
        "compound-map legacy attach exact fit");
    if (!ref || rel->memory_governor != ref)
        goto cleanup;
    uint64_t add_base = descriptor_bytes("map-add") + metadata_bytes(add)
        + attach_payload_bytes(add);
    uint64_t add_new = metadata_bytes(rel) + shared_table_bytes(rel->ncols);
    uint64_t shared_payload = (uint64_t)rel->ncols * sizeof(int64_t);
    make_resolution(&resolution, add_base + add_new + shared_payload - 1u);
    add_ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(add_ref && col_rel_attach_memory_governor(add, add_ref) == 0,
        "governed shared-view addition setup");
    if (add_ref && add->memory_governor == add_ref) {
        uint64_t add_view = add->view_generation;
        int64_t **old_columns = add->columns;
        CHECK(col_rel_install_shared_view(add, rel) == ENOSPC
            && add->compound_arity_len == 0 && add->columns == old_columns
            && add->view_generation == add_view
            && add->metadata_reserved_bytes == metadata_bytes(add)
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(add_ref)) == add_base,
            "governed shared-view map addition denial preserves old image");
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                add_ref)->usable_bytes, add_base + add_new + shared_payload,
            memory_order_release);
        CHECK(col_rel_install_shared_view(add, rel) == 0
            && add->compound_arity_len == 1
            && add->compound_arity_map[0] == 2
            && add->metadata_reserved_bytes == add_new
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(add_ref))
            == descriptor_bytes("map-add") + add_new + shared_payload,
            "governed shared-view map addition exact fit");
    }
    col_rel_destroy(add);
    add = NULL;
    uint64_t removal_new = metadata_bytes(plain)
        + shared_table_bytes(plain->ncols);
    view = rel->view_generation;
    uint32_t *old_map = rel->compound_arity_map;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, exact + removal_new + shared_payload - 1u,
        memory_order_release);
    CHECK(col_rel_install_shared_view(rel, plain) == ENOSPC
        && rel->compound_arity_len == 1 && rel->compound_arity_map == old_map
        && rel->view_generation == view
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == exact,
        "governed shared-view map removal denial preserves old image");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, exact + removal_new + shared_payload,
        memory_order_release);
    CHECK(col_rel_install_shared_view(rel, plain) == 0
        && rel->compound_arity_len == 0 && rel->compound_arity_map == NULL
        && rel->metadata_reserved_bytes == removal_new
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor_bytes("attached-map") + removal_new + shared_payload,
        "governed shared-view map removal exact fit");
    int64_t **old_shared_columns = rel->columns;
    view = rel->view_generation;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        descriptor_bytes("attached-map") + 2u * removal_new
        + 2u * shared_payload - 1u,
        memory_order_release);
    CHECK(col_rel_install_shared_view(rel, plain) == ENOSPC
        && rel->columns == old_shared_columns
        && rel->view_generation == view
        && plain->storage_alias_borrows == 1u,
        "governed shared-view replacement denies one-byte overlap");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        descriptor_bytes("attached-map") + 2u * removal_new
        + 2u * shared_payload,
        memory_order_release);
    CHECK(col_rel_install_shared_view(rel, plain) == 0
        && rel->columns != old_shared_columns
        && rel->metadata_reserved_bytes == removal_new
        && plain->storage_alias_borrows == 1u
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor_bytes("attached-map") + removal_new + shared_payload,
        "governed shared-view replacement releases retired image once");
cleanup:
    col_rel_destroy(add);
    col_rel_destroy(rel);
    col_rel_destroy(plain);
    if (add_ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(add_ref)) == 0,
            "governed shared-view addition teardown");
        wl_columnar_memory_governor_ref_release(add_ref);
    }
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "compound-map attach teardown releases token");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_compound_map_replacement(void)
{
    const col_rel_logical_col_t logical[2] = {
        { WIRELOG_COMPOUND_KIND_SIDE, 2u, 1u },
        { WIRELOG_COMPOUND_KIND_NONE, 0u, 0u },
    };
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *target = col_rel_new_auto("map-target", 2);
    col_rel_t *candidate = col_rel_new_auto("map-candidate", 2);
    col_rel_replacement_t replacement = { 0 };
    uint64_t base, planned, exact, view;
    if (!target || !candidate
        || col_rel_apply_compound_schema(candidate, logical, 2u) != 0) {
        CHECK(false, "compound replacement fixture");
        goto cleanup;
    }
    planned = private_payload_bytes(candidate->ncols,
            candidate->capacity, 0);
    base = descriptor_bytes("map-target") + metadata_bytes(target)
        + attach_payload_bytes(target);
    exact = base + planned + metadata_bytes(candidate);
    make_resolution(&resolution, exact - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    if (!ref || col_rel_attach_memory_governor(target, ref) != 0) {
        CHECK(false, "compound replacement governor attach");
        goto cleanup;
    }
    view = target->view_generation;
    CHECK(col_rel_prepare_replacement(target, candidate, &replacement)
        == ENOMEM && target->view_generation == view
        && target->compound_arity_len == 0
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == base,
        "compound replacement one-byte denial preserves old image");
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, exact, memory_order_release);
    CHECK(col_rel_prepare_replacement(target, candidate, &replacement) == 0
        && replacement.metadata_reserved_bytes == metadata_bytes(candidate)
        && replacement.staged->compound_arity_len == 2
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == exact,
        "compound replacement exact-fit prepared window");
    col_rel_discard_replacement(&replacement);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == base
        && target->compound_arity_len == 0,
        "compound replacement discard releases pending map credit");
    CHECK(col_rel_prepare_replacement(target, candidate, &replacement) == 0,
        "compound replacement prepare after discard");
    if (replacement.staged)
        col_rel_commit_replacement_locked(target, &replacement);
    col_rel_discard_replacement(&replacement);
    CHECK(target->compound_arity_len == 2
        && target->compound_arity_map
        && target->compound_arity_map[0] == 1
        && target->compound_arity_map[1] == 1
        && target->metadata_reserved_bytes == metadata_bytes(candidate)
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        == descriptor_bytes("map-target") + planned
        + metadata_bytes(candidate),
        "compound replacement commits exact private map charge");
cleanup:
    col_rel_discard_replacement(&replacement);
    col_rel_destroy(candidate);
    col_rel_destroy(target);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "compound replacement teardown releases token");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_compound_map_malformed_length(void)
{
    wl_columnar_memory_resolution_t resolution;
    col_rel_t *rel = col_rel_new_auto("bad-map", 1);
    wl_columnar_memory_governor_ref_t *ref;
    CHECK(rel != NULL, "malformed compound-map setup");
    if (!rel)
        return;
    rel->compound_arity_map = malloc(sizeof(uint32_t));
    CHECK(rel->compound_arity_map != NULL, "malformed compound-map storage");
    if (!rel->compound_arity_map) {
        col_rel_destroy(rel);
        return;
    }
    rel->compound_arity_map[0] = 1;
    make_resolution(&resolution, 1u << 20);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(rel, ref) == EOVERFLOW
        && rel->memory_governor == NULL,
        "map pointer without length rejected before read");
    rel->compound_arity_len = 2;
    CHECK(ref && col_rel_attach_memory_governor(rel, ref) == EOVERFLOW
        && rel->memory_governor == NULL,
        "map length exceeding physical width rejected before read");
    rel->compound_arity_len = 1;
    rel->compound_arity_map[0] = 0;
    CHECK(ref && col_rel_attach_memory_governor(rel, ref) == EOVERFLOW
        && rel->memory_governor == NULL,
        "zero map entry rejected before admission");
    col_rel_destroy(rel);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "malformed map leaves no reservation");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_retraction_backup_timestamp_admission(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *owner = col_rel_new_auto("backup-owner", 1);
    col_rel_t *source = col_rel_new_auto("backup-source", 1);
    col_rel_t *copy = NULL;
    col_rel_replacement_t replacement = { 0 };
    int64_t row = 17;
    CHECK(owner && source, "backup timestamp fixture");
    if (!owner || !source)
        goto cleanup;
    CHECK(col_rel_append_row(owner, &row) == 0
        && col_rel_append_row(source, &row) == 0
        && col_rel_enable_timestamps(owner) == 0,
        "backup timestamp active image");
    owner->retract_backup_columns = col_columns_alloc(1, 8);
    owner->retract_backup_timestamps = calloc(8,
            sizeof(*owner->retract_backup_timestamps));
    CHECK(owner->retract_backup_columns
        && owner->retract_backup_timestamps,
        "backup timestamp physical image");
    if (!owner->retract_backup_columns
        || !owner->retract_backup_timestamps)
        goto cleanup;
    owner->retract_backup_capacity = 8;
    owner->retract_backup_timestamp_capacity = 8;
    owner->retract_backup_nrows = 1;
    owner->retract_backup_columns[0][0] = 29;
    owner->retract_backup_timestamps[0].iteration = 91;
    uint64_t footprint = descriptor_bytes("backup-owner")
        + metadata_bytes(owner) + attach_payload_bytes(owner);
    make_resolution(&resolution, footprint - 1u);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(owner, ref) == ENOSPC
        && !owner->memory_governor
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == 0,
        "backup timestamp one-byte attach refusal is transactional");
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
    make_resolution(&resolution, footprint);
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref && col_rel_attach_memory_governor(owner, ref) == 0
        && owner->retained_reserved_bytes == attach_payload_bytes(owner)
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) == footprint,
        "backup timestamp exact attach includes both timestamp buffers");
    CHECK(col_rel_deep_copy(owner, &copy, NULL) == 0 && copy
        && copy->retract_backup_timestamps
        && copy->retract_backup_timestamps
        != owner->retract_backup_timestamps
        && copy->retract_backup_timestamps[0].iteration == 91,
        "backup timestamp deep copy owns private storage");
    CHECK(col_rel_install_shared_view(owner, source) == EBUSY
        && owner->retract_backup_timestamps[0].iteration == 91,
        "governed shared install refuses live backup before mutation");
    CHECK(col_rel_prepare_replacement(owner, source, &replacement) == EBUSY
        && owner->retract_backup_timestamps[0].iteration == 91,
        "governed replacement refuses live backup before mutation");
cleanup:
    col_rel_discard_replacement(&replacement);
    col_rel_destroy(copy);
    col_rel_destroy(owner);
    col_rel_destroy(source);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == 0,
            "backup timestamp teardown releases exact credit");
        wl_columnar_memory_governor_ref_release(ref);
    }
}

int
main(void)
{
    test_retraction_backup_timestamp_admission();
    test_compound_map_admission();
    test_compound_map_attach_and_shared_view();
    test_compound_map_replacement();
    test_compound_map_malformed_length();
    test_heap_descriptor_admission();
    test_heap_descriptor_attach_and_replacement();
    test_pool_descriptor_excluded();
    test_checked_new_like_boundaries();
    test_checked_new_like_schema_type_denial();
    test_pool_name_lifecycle();
    test_schema_metadata_admission();
    test_legacy_metadata_attach();
    test_zero_width_schema_metadata();
    test_heap_descriptor_rename();
    test_governed_null_name_copy();
    test_governed_logical_copy();
    test_governed_empty_compound_copy();
    test_physical_timestamp_capacity();
    test_cow_retained_timestamp_capacity_admission();
    test_governed_shared_view_timestamp_transaction();
    test_governed_shared_view_dst_only_timestamps();
    test_shared_view_dst_only_timestamp_extent();
    test_shared_view_narrow_timestamp_appends();
    test_owned_narrow_timestamp_append();
    test_legacy_shared_view_attach();
    test_shared_table_retirement_paths();
    test_replacement_expands_narrow_timestamps();
    test_timestamp_shape_attach_invariant();
    test_timestamp_only_retained_admission();
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
    test_checked_new_like_identity_overflow();
    if (failures != 0)
        return 1;
    puts("memory admission relation: PASS");
    return 0;
}
