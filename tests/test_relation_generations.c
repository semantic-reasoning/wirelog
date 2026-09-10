/* Relation generation contract tests (Issue #1441). */

#include "../wirelog/columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef WL_TEST_ALLOC_WRAP
void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
void *__real_realloc(void *ptr, size_t size);

static long allocation_fail_at = -1;
static long allocation_calls;

static bool
fail_this_allocation(void)
{
    return allocation_fail_at >= 0
           && allocation_calls++ == allocation_fail_at;
}

void *
__wrap_malloc(size_t size)
{
    return fail_this_allocation() ? NULL : __real_malloc(size);
}

void *
__wrap_calloc(size_t count, size_t size)
{
    return fail_this_allocation() ? NULL : __real_calloc(count, size);
}

void *
__wrap_realloc(void *ptr, size_t size)
{
    return fail_this_allocation() ? NULL : __real_realloc(ptr, size);
}
#endif

static int failures;
static col_rel_t *owned_relations[32];
static size_t owned_relation_count;

#ifdef WL_TEST_APPEND_HOOK
static col_rel_t *append_hook_expected;
static col_rel_t *append_hook_source;
static bool append_hook_called;
static bool append_hook_state_ok;
static bool append_hook_expect_growth;
static uint32_t append_hook_expected_rows;
static uint32_t append_hook_expected_capacity;
static int64_t **append_hook_source_columns;
static uint32_t append_hook_source_aliases;
static uint64_t append_hook_view_generation;
static uint64_t append_hook_storage_generation;
static int append_hook_reader_rc;

static void
append_transition_probe(col_rel_t *rel)
{
    wl_columnar_source_access_reader_t reader = { 0 };

    if (rel != append_hook_expected)
        return;
    append_hook_called = true;
    append_hook_state_ok = rel->storage_owner == append_hook_source
        && rel->storage_owner_identity == append_hook_source->relation_identity
        && rel->storage_owner_generation
        == append_hook_source->storage_generation
        && rel->nrows == append_hook_expected_rows
        && (append_hook_expect_growth
            ? rel->capacity > append_hook_expected_capacity
            : rel->capacity == append_hook_expected_capacity)
        && rel->view_generation == append_hook_view_generation
        && rel->storage_generation != append_hook_storage_generation
        && rel->col_shared == NULL
        && rel->storage_alias_borrows == 0
        && append_hook_source->columns == append_hook_source_columns
        && append_hook_source->nrows == append_hook_expected_rows
        && append_hook_source->storage_alias_borrows
        == append_hook_source_aliases;
    append_hook_reader_rc = col_rel_source_reader_acquire(rel, &reader);
    if (append_hook_reader_rc == 0)
        (void)col_rel_source_reader_release(&reader);
}
#endif

static void
cleanup_relations(void)
{
    while (owned_relation_count > 0)
        col_rel_destroy(owned_relations[--owned_relation_count]);
}

static col_rel_t *
track_relation(col_rel_t *rel)
{
    if (rel && owned_relation_count < 32)
        owned_relations[owned_relation_count++] = rel;
    return rel;
}

#define CHECK(cond, msg) do { \
            if (!(cond)) { \
                fprintf(stderr, "FAIL: %s\n", (msg)); \
                failures++; \
                cleanup_relations(); \
                return; \
            } \
} while (0)

static col_rel_t *
new_relation(void)
{
    col_rel_t *rel = NULL;
    if (col_rel_alloc(&rel, "generation_test") != 0)
        return NULL;
    if (col_rel_set_schema(rel, 1, NULL) != 0) {
        col_rel_destroy(rel);
        return NULL;
    }
    return track_relation(rel);
}

static void
test_same_row_count_mutation(void)
{
    col_rel_t *rel = new_relation();
    CHECK(rel != NULL, "relation allocation");
    int64_t row = 7;
    CHECK(col_rel_append_row(rel, &row) == 0, "append row");
    uint64_t before = rel->view_generation;
    CHECK(col_rel_set(rel, 0, 0, 9) == 0, "same-row-count cell write");
    CHECK(rel->nrows == 1 && rel->view_generation != before,
        "cell write advances the logical generation");
    cleanup_relations();
}

static void
test_storage_only_cow_and_compaction(void)
{
    col_rel_t *src = new_relation();
    col_rel_t *view = new_relation();
    CHECK(src && view, "shared-view relations");
    int64_t row = 11;
    CHECK(col_rel_append_row(src, &row) == 0, "source append");
    CHECK(col_rel_install_shared_view(view, src) == 0, "install shared view");
    uint64_t view_before = view->view_generation;
    uint64_t storage_before = view->storage_generation;
    CHECK(col_rel_cow_unshare(view, 0) == 0, "COW unshare");
    CHECK(view->view_generation == view_before,
        "COW does not change the logical view");
    CHECK(view->storage_generation != storage_before,
        "COW advances storage generation");

    col_rel_t *compact = new_relation();
    CHECK(compact != NULL, "compaction relation");
    CHECK(col_rel_append_row(compact, &row) == 0, "compaction seed");
    CHECK(col_columns_realloc_atomic(compact->columns, compact->ncols,
        compact->capacity, 128) == 0, "oversize relation buffers");
    compact->capacity = 128;
    view_before = compact->view_generation;
    storage_before = compact->storage_generation;
    col_rel_compact(compact);
    CHECK(compact->view_generation == view_before,
        "compaction preserves the logical view");
    CHECK(compact->storage_generation != storage_before,
        "compaction advances storage generation");
    cleanup_relations();
}

static void
test_flattened_storage_ownership(void)
{
    col_rel_t *source = new_relation();
    col_rel_t *alias = new_relation();
    col_rel_t *flattened = new_relation();
    int64_t row = 41;
    CHECK(source && alias && flattened, "storage ownership relations");
    CHECK(col_rel_append_row(source, &row) == 0, "storage ownership seed");
    CHECK(col_rel_install_shared_view(alias, source) == 0,
        "owner to alias publication");
    CHECK(alias->storage_owner == source
        && alias->storage_owner_identity == source->relation_identity
        && source->storage_alias_borrows == 1,
        "alias records its ultimate owner");
    CHECK(col_rel_storage_owner_destroy_status(source) == EBUSY,
        "owner reports active alias borrow");

    CHECK(col_rel_install_shared_view(flattened, alias) == 0,
        "alias-of-alias publication");
    CHECK(flattened->storage_owner == source
        && flattened->storage_owner != alias
        && source->storage_alias_borrows == 2,
        "alias-of-alias is flattened to the root");

    CHECK(col_rel_set(flattened, 0, 0, 99) == 0,
        "flattened alias COW");
    CHECK(flattened->storage_owner == flattened
        && source->storage_alias_borrows == 1
        && col_rel_get(source, 0, 0) == row
        && col_rel_get(flattened, 0, 0) == 99,
        "COW releases only the flattened alias borrow");
    cleanup_relations();
}

static void
test_copy_and_shared_semantics(void)
{
    col_rel_t *src = new_relation();
    col_rel_t *copy = NULL;
    col_rel_t *view = new_relation();
    CHECK(src && view, "copy relations");
    int64_t row = 3;
    uint64_t source_view;
    uint64_t source_storage;
    uint64_t old_view;
    uint64_t old_storage;
    CHECK(col_rel_append_row(src, &row) == 0, "copy seed");
    source_view = src->view_generation;
    source_storage = src->storage_generation;
    old_view = view->view_generation;
    old_storage = view->storage_generation;
    CHECK(col_rel_deep_copy(src, &copy, NULL) == 0, "deep copy");
    track_relation(copy);
    CHECK(copy->relation_identity != src->relation_identity,
        "deep copy receives a fresh identity");
    CHECK(copy->view_generation == src->view_generation
        && copy->storage_generation == src->storage_generation,
        "deep copy preserves the source snapshot generations");
    CHECK(col_rel_install_shared_view(view, src) == 0, "shared copy");
    CHECK(view->relation_identity != src->relation_identity,
        "shared view retains its own identity");
    CHECK(wl_columnar_relation_generation_valid(view->view_generation)
        && wl_columnar_relation_generation_valid(view->storage_generation),
        "shared view receives valid destination generations");
    CHECK(view->view_generation != old_view
        && view->storage_generation != old_storage,
        "shared-view installation invalidates the old destination epoch");
    CHECK(src->view_generation == source_view
        && src->storage_generation == source_storage,
        "shared-view installation leaves the source unchanged");
    CHECK(col_rel_set(view, 0, 0, 4) == 0, "worker-local mutation");
    CHECK(view->view_generation != src->view_generation,
        "worker-local mutation does not advance coordinator state");
    col_rel_t *failure = new_relation();
    CHECK(failure != NULL, "shared-view failure relation");
    int64_t failure_row = 99;
    CHECK(col_rel_append_row(failure, &failure_row) == 0,
        "shared-view failure seed");
    int64_t **failure_columns = failure->columns;
    uint32_t failure_rows = failure->nrows;
    failure->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    CHECK(col_rel_install_shared_view(failure, src) == EOVERFLOW,
        "shared-view overflow is rejected before publication");
    CHECK(failure->columns == failure_columns && failure->nrows == failure_rows,
        "shared-view failure leaves destination unchanged");
    cleanup_relations();
}

static void
test_source_reader_blocks_checked_destroy(void)
{
    col_rel_t *rel = new_relation();
    wl_columnar_source_access_reader_t reader = { 0 };
    char *name;
    uint64_t identity;
    uint64_t generation;
    uint32_t rows;

    CHECK(rel != NULL, "source reader relation allocation");
    name = rel->name;
    identity = rel->relation_identity;
    generation = rel->storage_generation;
    rows = rel->nrows;
    CHECK(col_rel_source_reader_acquire(rel, &reader) == 0,
        "source reader acquisition");
    CHECK(col_rel_destroy_checked(rel) == EBUSY,
        "active source reader blocks checked destroy");
    CHECK(rel->name == name && rel->relation_identity == identity
        && rel->storage_generation == generation && rel->nrows == rows,
        "busy destroy changed relation state");
    CHECK(col_rel_source_reader_release(&reader) == 0,
        "source reader release");
    CHECK(col_rel_destroy_checked(rel) == 0,
        "destroy retry after reader release");
    owned_relation_count--;
}

static void
test_source_reader_blocks_direct_append_row(void)
{
    col_rel_t *rel = new_relation();
    wl_columnar_source_access_reader_t reader = { 0 };
    int64_t first = 17;
    int64_t extra = 23;
    int64_t **columns;
    col_delta_timestamp_t *timestamps;
    uint32_t rows;
    uint32_t capacity;
    uint64_t view_generation;
    uint64_t storage_generation;
    uint64_t owner_generation;
    uint64_t reserved_bytes;
    uint32_t aliases;

    CHECK(rel != NULL, "direct append reader exclusion relation");
    CHECK(col_rel_append_row(rel, &first) == 0,
        "direct append reader exclusion seed");
    CHECK(col_rel_enable_timestamps(rel) == 0,
        "direct append reader exclusion timestamps");
    rel->timestamps[0].iteration = 41;
    rel->timestamps[0].multiplicity = 7;
    CHECK(col_rel_source_reader_acquire(rel, &reader) == 0,
        "direct spare append reader");
    columns = rel->columns;
    timestamps = rel->timestamps;
    rows = rel->nrows;
    capacity = rel->capacity;
    view_generation = rel->view_generation;
    storage_generation = rel->storage_generation;
    owner_generation = rel->storage_owner_generation;
    reserved_bytes = rel->retained_reserved_bytes;
    aliases = rel->storage_alias_borrows;
    int blocked_rc = col_rel_append_row(rel, &extra);
    int release_rc = col_rel_source_reader_release(&reader);
    CHECK(blocked_rc == EBUSY && release_rc == 0
        && rel->columns == columns
        && rel->columns[0][0] == first
        && rel->timestamps == timestamps
        && rel->timestamps[0].iteration == 41
        && rel->timestamps[0].multiplicity == 7
        && rel->nrows == rows
        && rel->capacity == capacity
        && rel->view_generation == view_generation
        && rel->storage_generation == storage_generation
        && rel->storage_owner == rel
        && rel->storage_owner_identity == rel->relation_identity
        && rel->storage_owner_generation == owner_generation
        && rel->retained_reserved_bytes == reserved_bytes
        && rel->storage_alias_borrows == aliases,
        "reader-blocked spare append is transactional");
    CHECK(col_rel_append_row(rel, &extra) == 0
        && rel->nrows == rows + 1u
        && rel->columns[0][rows] == extra,
        "direct spare append succeeds after release");
    cleanup_relations();

    rel = new_relation();
    CHECK(rel != NULL, "direct resize reader exclusion relation");
    for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++) {
        int64_t value = (int64_t)i;
        CHECK(col_rel_append_row(rel, &value) == 0,
            "direct resize reader exclusion seed");
    }
    CHECK(col_rel_enable_timestamps(rel) == 0,
        "direct resize reader exclusion timestamps");
    rel->timestamps[COL_REL_INIT_CAP - 1u].stratum = 9;
    rel->timestamps[COL_REL_INIT_CAP - 1u].multiplicity = 11;
    CHECK(col_rel_source_reader_acquire(rel, &reader) == 0,
        "direct resize append reader");
    columns = rel->columns;
    timestamps = rel->timestamps;
    rows = rel->nrows;
    capacity = rel->capacity;
    view_generation = rel->view_generation;
    storage_generation = rel->storage_generation;
    owner_generation = rel->storage_owner_generation;
    reserved_bytes = rel->retained_reserved_bytes;
    aliases = rel->storage_alias_borrows;
    blocked_rc = col_rel_append_row(rel, &extra);
    release_rc = col_rel_source_reader_release(&reader);
    CHECK(blocked_rc == EBUSY && release_rc == 0
        && rel->columns == columns
        && rel->columns[0][COL_REL_INIT_CAP - 1u]
        == (int64_t)(COL_REL_INIT_CAP - 1u)
        && rel->timestamps == timestamps
        && rel->timestamps[COL_REL_INIT_CAP - 1u].stratum == 9
        && rel->timestamps[COL_REL_INIT_CAP - 1u].multiplicity == 11
        && rel->nrows == rows
        && rel->capacity == capacity
        && rel->view_generation == view_generation
        && rel->storage_generation == storage_generation
        && rel->storage_owner == rel
        && rel->storage_owner_identity == rel->relation_identity
        && rel->storage_owner_generation == owner_generation
        && rel->retained_reserved_bytes == reserved_bytes
        && rel->storage_alias_borrows == aliases,
        "reader-blocked resize append is transactional");
    CHECK(col_rel_append_row(rel, &extra) == 0
        && rel->nrows == rows + 1u
        && rel->capacity > capacity
        && rel->columns[0][rows] == extra,
        "direct resize append succeeds after release");
    cleanup_relations();
}

static void
test_source_reader_blocks_append_row(void)
{
    col_rel_t *source = new_relation();
    col_rel_t *view = new_relation();
    wl_columnar_source_access_reader_t reader = { 0 };
    int64_t first = 17;
    int64_t extra = 23;
    int64_t **source_columns;
    int64_t **view_columns;
    int64_t *view_column0;
    col_delta_timestamp_t *source_timestamps;
    col_delta_timestamp_t *view_timestamps;
    uint32_t source_rows;
    uint32_t source_capacity;
    uint32_t view_rows;
    uint32_t view_capacity;
    uint64_t source_view;
    uint64_t source_storage;
    uint64_t source_owner_identity;
    uint64_t view_view;
    uint64_t view_storage;
    uint64_t source_owner_generation;
    uint64_t source_reserved;
    uint64_t view_reserved;
    uint32_t source_aliases;
    uint32_t view_aliases;

    CHECK(source && view, "append reader exclusion relations");
    CHECK(col_rel_append_row(source, &first) == 0,
        "append reader exclusion seed");
    CHECK(col_rel_enable_timestamps(source) == 0,
        "append reader exclusion timestamps");
    source->timestamps[0].iteration = 41;
    source->timestamps[0].multiplicity = 7;
    CHECK(col_rel_install_shared_view(view, source) == 0,
        "append reader exclusion shared view");
    source_columns = source->columns;
    source_timestamps = source->timestamps;
    source_rows = source->nrows;
    source_capacity = source->capacity;
    source_view = source->view_generation;
    source_storage = source->storage_generation;
    source_owner_identity = source->storage_owner_identity;
    source_owner_generation = source->storage_owner_generation;
    source_reserved = source->retained_reserved_bytes;
    source_aliases = source->storage_alias_borrows;
    view_columns = view->columns;
    view_column0 = view->columns[0];
    view_timestamps = view->timestamps;
    view_rows = view->nrows;
    view_capacity = view->capacity;
    view_view = view->view_generation;
    view_storage = view->storage_generation;
    view_reserved = view->retained_reserved_bytes;
    view_aliases = view->storage_alias_borrows;
#ifdef WL_TEST_APPEND_HOOK
    append_hook_source = source;
    append_hook_expected = view;
    append_hook_called = false;
    append_hook_state_ok = false;
    append_hook_expect_growth = false;
    append_hook_expected_rows = view_rows;
    append_hook_expected_capacity = view_capacity;
    append_hook_source_columns = source_columns;
    append_hook_source_aliases = source_aliases;
    append_hook_view_generation = view_view;
    append_hook_storage_generation = view_storage;
    append_hook_reader_rc = 0;
    wl_columnar_append_transition_hook = append_transition_probe;
#endif
    CHECK(col_rel_append_row(view, &extra) == 0,
        "spare-capacity alias append");
#ifdef WL_TEST_APPEND_HOOK
    wl_columnar_append_transition_hook = NULL;
    append_hook_expected = NULL;
#endif
    CHECK(append_hook_called && append_hook_state_ok
        && append_hook_reader_rc == EBUSY,
        "reader blocks spare-capacity alias append transition");
    CHECK(source->columns == source_columns
        && source->columns[0][0] == first
        && source->timestamps == source_timestamps
        && source->timestamps[0].iteration == 41
        && source->timestamps[0].multiplicity == 7
        && source->nrows == source_rows
        && source->capacity == source_capacity
        && source->view_generation == source_view
        && source->storage_generation == source_storage
        && source->storage_owner == source
        && source->storage_owner_identity == source_owner_identity
        && source->storage_owner_generation == source_owner_generation
        && source->retained_reserved_bytes == source_reserved
        && source->storage_alias_borrows == source_aliases - 1u
        && view->columns[0] != view_column0
        && view->columns[0][0] == first
        && view->timestamps == view_timestamps
        && view->timestamps[0].iteration == 41
        && view->timestamps[0].multiplicity == 7
        && view->nrows == view_rows + 1u
        && view->columns[0][view_rows] == extra
        && view->capacity == view_capacity
        && view->view_generation != view_view
        && view->storage_generation != view_storage
        && view->storage_owner == view
        && view->storage_owner_identity == view->relation_identity
        && view->retained_reserved_bytes == view_reserved
        && view->storage_alias_borrows == view_aliases
        && view->col_shared == NULL
        && source->storage_alias_borrows == source_aliases - 1u,
        "spare-capacity alias append commits after transition");
    CHECK(col_rel_source_reader_acquire(view, &reader) == 0
        && col_rel_source_reader_release(&reader) == 0,
        "view reader succeeds after spare-capacity append");
    cleanup_relations();

    source = new_relation();
    view = new_relation();
    CHECK(source && view, "resize reader exclusion relations");
    for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++) {
        int64_t value = (int64_t)i;
        CHECK(col_rel_append_row(source, &value) == 0,
            "resize reader exclusion seed");
    }
    CHECK(col_rel_enable_timestamps(source) == 0,
        "resize reader exclusion timestamps");
    source->timestamps[COL_REL_INIT_CAP - 1u].stratum = 9;
    source->timestamps[COL_REL_INIT_CAP - 1u].multiplicity = 11;
    CHECK(col_rel_install_shared_view(view, source) == 0,
        "resize reader exclusion shared view");
    source_columns = source->columns;
    source_timestamps = source->timestamps;
    source_rows = source->nrows;
    source_capacity = source->capacity;
    source_view = source->view_generation;
    source_storage = source->storage_generation;
    source_owner_identity = source->storage_owner_identity;
    source_owner_generation = source->storage_owner_generation;
    source_reserved = source->retained_reserved_bytes;
    source_aliases = source->storage_alias_borrows;
    view_columns = view->columns;
    view_column0 = view->columns[0];
    view_timestamps = view->timestamps;
    view_rows = view->nrows;
    view_capacity = view->capacity;
    view_view = view->view_generation;
    view_storage = view->storage_generation;
    view_reserved = view->retained_reserved_bytes;
    view_aliases = view->storage_alias_borrows;
#ifdef WL_TEST_APPEND_HOOK
    append_hook_source = source;
    append_hook_expected = view;
    append_hook_called = false;
    append_hook_state_ok = false;
    append_hook_expect_growth = true;
    append_hook_expected_rows = view_rows;
    append_hook_expected_capacity = view_capacity;
    append_hook_source_columns = source_columns;
    append_hook_source_aliases = source_aliases;
    append_hook_view_generation = view_view;
    append_hook_storage_generation = view_storage;
    append_hook_reader_rc = 0;
    wl_columnar_append_transition_hook = append_transition_probe;
#endif
    CHECK(col_rel_append_row(view, &extra) == 0,
        "resize alias append");
#ifdef WL_TEST_APPEND_HOOK
    wl_columnar_append_transition_hook = NULL;
    append_hook_expected = NULL;
#endif
    CHECK(append_hook_called && append_hook_state_ok
        && append_hook_reader_rc == EBUSY,
        "reader blocks resize alias append transition");
    CHECK(source->columns == source_columns
        && source->columns[0][COL_REL_INIT_CAP - 1u]
        == (int64_t)(COL_REL_INIT_CAP - 1u)
        && source->timestamps == source_timestamps
        && source->timestamps[COL_REL_INIT_CAP - 1u].stratum == 9
        && source->timestamps[COL_REL_INIT_CAP - 1u].multiplicity == 11
        && source->nrows == source_rows
        && source->capacity == source_capacity
        && source->view_generation == source_view
        && source->storage_generation == source_storage
        && source->storage_owner == source
        && source->storage_owner_identity == source_owner_identity
        && source->storage_owner_generation == source_owner_generation
        && source->retained_reserved_bytes == source_reserved
        && source->storage_alias_borrows == source_aliases - 1u
        && view->columns != view_columns
        && view->columns[0][COL_REL_INIT_CAP - 1u]
        == (int64_t)(COL_REL_INIT_CAP - 1u)
        && view->timestamps != view_timestamps
        && view->timestamps[COL_REL_INIT_CAP - 1u].stratum == 9
        && view->timestamps[COL_REL_INIT_CAP - 1u].multiplicity == 11
        && view->nrows == view_rows + 1u
        && view->capacity > view_capacity
        && view->view_generation != view_view
        && view->storage_generation != view_storage
        && view->storage_owner == view
        && view->storage_owner_identity == view->relation_identity
        && view->retained_reserved_bytes == view_reserved
        && view->storage_alias_borrows == view_aliases
        && view->columns[0][view_rows] == extra
        && view->col_shared == NULL
        && source->storage_alias_borrows == source_aliases - 1u,
        "resize alias append commits after transition");
    CHECK(col_rel_source_reader_acquire(view, &reader) == 0
        && col_rel_source_reader_release(&reader) == 0,
        "view reader succeeds after resize append");
    cleanup_relations();
}

static void
test_rollback_fresh_generation(void)
{
    col_rel_t *rel = new_relation();
    CHECK(rel != NULL, "rollback relation");
    int64_t row = 21;
    CHECK(col_rel_append_row(rel, &row) == 0, "rollback seed");
    int64_t **saved_columns = rel->columns;
    uint32_t saved_capacity = rel->capacity;
    uint32_t saved_nrows = rel->nrows;
    uint64_t saved_view = rel->view_generation;
    uint64_t saved_storage = rel->storage_generation;

    /* Exercise the same pointer-swap shape used by retraction evaluation:
     * a temporary result is published, then the original snapshot is
     * restored.  Restoration must not reuse the saved generation pair. */
    rel->columns = col_columns_alloc(rel->ncols, saved_capacity);
    if (!rel->columns) {
        CHECK(false, "temporary rollback columns");
    }
    rel->capacity = saved_capacity;
    rel->nrows = 0;
    wl_columnar_relation_touch_replacement(rel);
    col_columns_free(rel->columns, rel->ncols);
    rel->columns = saved_columns;
    rel->capacity = saved_capacity;
    rel->nrows = saved_nrows;
    wl_columnar_relation_touch_replacement(rel);
    CHECK(rel->view_generation != saved_view
        && rel->storage_generation != saved_storage,
        "rollback restoration receives fresh generations");
    CHECK(rel->columns == saved_columns && rel->nrows == saved_nrows,
        "rollback restores the original snapshot");
    cleanup_relations();
}

static void
test_overflow_boundary(void)
{
    col_rel_t *rel = new_relation();
    CHECK(rel != NULL, "overflow relation");
    rel->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    CHECK(wl_columnar_relation_generation_advance(&rel->view_generation)
        == EOVERFLOW, "checked generation overflow");
    CHECK(rel->view_generation == WL_COLUMNAR_REL_GENERATION_INVALID
        && !wl_columnar_relation_generation_valid(rel->view_generation),
        "overflow poisons equality");
    uint64_t poisoned = rel->view_generation;
    CHECK(wl_columnar_relation_generation_advance(&rel->view_generation)
        == EOVERFLOW && rel->view_generation == poisoned,
        "poisoned generation remains saturated");
    cleanup_relations();
}

static void
test_identity_exhaustion(void)
{
    col_rel_t *rel = NULL;
    CHECK(col_rel_test_set_next_identity(UINT64_MAX) == 0,
        "set identity allocator exhaustion boundary");
    CHECK(col_rel_alloc(&rel, "identity_exhaustion") == EOVERFLOW,
        "identity allocation fails at UINT64_MAX without reuse");
    CHECK(rel == NULL, "failed identity allocation does not publish relation");
}

static void
test_direct_publication_paths(void)
{
    col_rel_t *src = new_relation();
    col_rel_t *weighted = new_relation();
    CHECK(src && weighted, "direct-path relations");
    int64_t row = 42;
    CHECK(col_rel_append_row(src, &row) == 0, "direct-path source row");

    uint32_t key = 0;
    col_rel_t *parts[2] = { NULL, NULL };
    CHECK(col_rel_partition_by_key(src, &key, 1, 2, parts) == 0,
        "partition publication");
    track_relation(parts[0]);
    track_relation(parts[1]);
    for (size_t i = 0; i < 2; i++) {
        CHECK(parts[i] != NULL, "partition relation exists");
        CHECK(parts[i]->nrows == 0 ||
            wl_columnar_relation_generation_valid(parts[i]->view_generation),
            "partition view generation is valid");
    }

    uint64_t before_view = weighted->view_generation;
    uint64_t before_storage = weighted->storage_generation;
    CHECK(col_op_reduce_weighted(src, weighted) == 0,
        "weighted reduce publication");
    CHECK(weighted->view_generation != before_view,
        "weighted reduce advances view generation");
    CHECK(weighted->storage_generation != before_storage,
        "weighted reduce advances storage generation");

    cleanup_relations();
}

static void
test_shared_view_metadata_and_pool_rejection(void)
{
    col_rel_t *src = new_relation();
    col_rel_t *dst = new_relation();
    CHECK(src && dst, "metadata relations");
    if (src->col_names && src->ncols > 0) {
        free(src->col_names[0]);
        src->col_names[0] = strdup("source_name");
        CHECK(src->col_names[0] != NULL, "source column name");
    }
    int64_t row = 17;
    CHECK(col_rel_append_row(src, &row) == 0, "metadata source row");
    src->timestamps = (col_delta_timestamp_t *)calloc(src->capacity,
            sizeof(*src->timestamps));
    CHECK(src->timestamps != NULL, "source timestamps");
    src->timestamps[0].multiplicity = -3;
    src->run_count = 1;
    src->run_ends[0] = src->nrows;

    dst->timestamps = (col_delta_timestamp_t *)calloc(dst->capacity,
            sizeof(*dst->timestamps));
    dst->merge_columns = col_columns_alloc(dst->ncols, 1);
    dst->merge_buf_cap = 1;
    dst->retract_backup_columns = col_columns_alloc(dst->ncols, 1);
    dst->retract_backup_capacity = 1;
    dst->dedup_slots = (uint64_t *)calloc(8, sizeof(uint64_t));
    dst->dedup_cap = 8;
    dst->dedup_count = 1;
    CHECK(dst->timestamps && dst->merge_columns
        && dst->retract_backup_columns && dst->dedup_slots,
        "destination metadata setup");
    CHECK(col_rel_install_shared_view(dst, src) == 0,
        "metadata shared-view publication");
    CHECK(dst->timestamps && dst->timestamps[0].multiplicity == -3,
        "timestamps follow the source view");
    CHECK(dst->run_count == src->run_count
        && dst->run_ends[0] == src->run_ends[0],
        "sorted-run metadata follows the source view");
    CHECK(dst->col_names && strcmp(dst->col_names[0], "source_name") == 0
        && col_rel_col_idx(dst, "source_name") == 0,
        "shared-view names and lookup follow the source schema");
    CHECK(dst->merge_columns == NULL && dst->merge_buf_cap == 0
        && dst->retract_backup_columns == NULL
        && dst->retract_backup_capacity == 0
        && dst->dedup_slots == NULL && dst->dedup_cap == 0
        && dst->dedup_count == 0,
        "destination-local caches are invalidated");
    cleanup_relations();

    col_rel_t *like = new_relation();
    CHECK(like != NULL, "pool source relation");
    delta_pool_t *pool = delta_pool_create(4, sizeof(col_rel_t), 4096);
    CHECK(pool != NULL, "pool allocation");
    uint32_t used = pool->slot_used;
    CHECK(col_rel_test_set_next_identity(UINT64_MAX) == 0,
        "set pool identity exhaustion");
    CHECK(col_rel_pool_new_like(pool, "reject-like", like) == NULL,
        "pool-like rejects identity exhaustion");
    CHECK(pool->slot_used == used,
        "pool-like rejection does not consume a slot");
    CHECK(col_rel_test_set_next_identity(1000) == 0,
        "restore identity allocator for pool-like");
    col_rel_t *pooled_like = col_rel_pool_new_like(pool, "pooled-like", like);
    CHECK(pooled_like != NULL && pool->slot_used == used + 1,
        "pool-like allocation remains available");
    CHECK(col_rel_test_set_next_identity(UINT64_MAX) == 0,
        "set pool-auto identity exhaustion");
    CHECK(col_rel_pool_new_auto(pool, NULL, "reject-auto", 1) == NULL,
        "pool-auto rejects identity exhaustion");
    CHECK(pool->slot_used == used + 1,
        "pool-auto rejection does not consume a slot");
    CHECK(col_rel_test_set_next_identity(2000) == 0,
        "restore identity allocator for pool-auto");
    col_rel_t *pooled_auto = col_rel_pool_new_auto(pool, NULL,
            "pooled-auto", 1);
    CHECK(pooled_auto != NULL && pool->slot_used == used + 2,
        "pool-auto allocation remains available");
    col_rel_destroy(pooled_like);
    col_rel_destroy(pooled_auto);

#ifdef WL_TEST_ALLOC_WRAP
    delta_pool_t *like_sweep_pool = delta_pool_create(64, sizeof(col_rel_t),
            4096);
    delta_pool_t *auto_sweep_pool = delta_pool_create(64, sizeof(col_rel_t),
            4096);
    CHECK(like_sweep_pool && auto_sweep_pool, "pool sweep allocation");
    bool like_sweep_complete = false;
    bool auto_sweep_complete = false;
    for (long fail_at = 0; fail_at < 128
        && (!like_sweep_complete || !auto_sweep_complete); fail_at++) {
        if (!like_sweep_complete) {
            uint32_t before = like_sweep_pool->slot_used;
            allocation_calls = 0;
            allocation_fail_at = fail_at;
            col_rel_t *candidate = col_rel_pool_new_like(like_sweep_pool,
                    "oom-like", like);
            long calls = allocation_calls;
            allocation_fail_at = -1;
            if (candidate && like_sweep_pool->slot_used == before + 1) {
                like_sweep_complete = true;
                col_rel_destroy(candidate);
            } else {
                CHECK(like_sweep_pool->slot_used == before,
                    "pool-like post-slot failure rolls back the slot");
                col_rel_destroy(candidate);
                CHECK(calls > fail_at || fail_at == 0,
                    "pool-like sweep made allocation progress");
            }
        }
        if (!auto_sweep_complete) {
            uint32_t before = auto_sweep_pool->slot_used;
            allocation_calls = 0;
            allocation_fail_at = fail_at;
            col_rel_t *candidate = col_rel_pool_new_auto(auto_sweep_pool, NULL,
                    "oom-auto", 4);
            long calls = allocation_calls;
            allocation_fail_at = -1;
            if (candidate && auto_sweep_pool->slot_used == before + 1) {
                auto_sweep_complete = true;
                col_rel_destroy(candidate);
            } else {
                CHECK(auto_sweep_pool->slot_used == before,
                    "pool-auto post-slot failure rolls back the slot");
                col_rel_destroy(candidate);
                CHECK(calls > fail_at || fail_at == 0,
                    "pool-auto sweep made allocation progress");
            }
        }
    }
    allocation_fail_at = -1;
    CHECK(like_sweep_complete && auto_sweep_complete,
        "pool constructors sweep every post-slot failure");
    col_rel_t *next_like = col_rel_pool_new_like(like_sweep_pool,
            "next-like", like);
    col_rel_t *next_auto = col_rel_pool_new_auto(auto_sweep_pool, NULL,
            "next-auto", 4);
    CHECK(next_like != NULL, "pool-like remains usable after failure sweep");
    CHECK(next_auto != NULL, "pool-auto remains usable after failure sweep");
    col_rel_destroy(next_like);
    col_rel_destroy(next_auto);
    delta_pool_destroy(like_sweep_pool);
    delta_pool_destroy(auto_sweep_pool);
#endif
    delta_pool_destroy(pool);
    cleanup_relations();
}

static void
test_exact_publication_epochs(void)
{
    col_rel_t *src = new_relation();
    col_rel_t *dst = new_relation();
    CHECK(src && dst, "append-all epoch relations");
    int64_t source_row = 23;
    CHECK(col_rel_append_row(src, &source_row) == 0,
        "append-all epoch source");
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    CHECK(col_rel_set_column_types(src, &type, 1) == 0,
        "typed append-all source");
    uint64_t view_before = dst->view_generation;
    uint64_t storage_before = dst->storage_generation;
    CHECK(col_rel_append_all(dst, src, NULL) == 0,
        "typed append-all publication");
    CHECK(dst->view_generation == view_before + 1u
        && dst->storage_generation == storage_before,
        "append-all publishes exactly one view epoch without resize");
    CHECK(dst->column_types && dst->column_types[0] == WIRELOG_TYPE_INT64,
        "append-all adopts source types without a second view epoch");

    /* Exercise schema publication into a destination whose old schema was
     * explicitly released.  This is the state produced by a pool relation
     * before its first complete view is installed. */
    col_rel_t *uninitialized = new_relation();
    CHECK(uninitialized != NULL, "uninitialized shared-view destination");
    ArrowSchemaRelease(&uninitialized->schema);
    uninitialized->schema_ok = false;
    CHECK(col_rel_install_shared_view(uninitialized, src) == 0,
        "shared-view initializes destination schema");
    CHECK(uninitialized->schema_ok
        && uninitialized->schema.n_children == 1,
        "shared-view schema matches installed columns");

    col_rel_t *full_src = new_relation();
    col_rel_t *full_view = new_relation();
    CHECK(full_src && full_view, "COW resize relations");
    for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++) {
        int64_t value = (int64_t)i;
        CHECK(col_rel_append_row(full_src, &value) == 0,
            "COW resize source append");
    }
    CHECK(col_rel_install_shared_view(full_view, full_src) == 0,
        "COW resize shared-view setup");
    view_before = full_view->view_generation;
    storage_before = full_view->storage_generation;
    int64_t extra = 99;
    CHECK(col_rel_append_row(full_view, &extra) == 0,
        "COW resize append");
    CHECK(full_view->view_generation == view_before + 1u
        && full_view->storage_generation == storage_before + 1u,
        "COW plus resize publishes one storage epoch");
    cleanup_relations();
}

static void
test_shared_mutation_cow_and_append_validation(void)
{
    col_rel_t *source = new_relation();
    CHECK(source != NULL, "shared mutation source");
    int64_t first = 3;
    int64_t second = 1;
    CHECK(col_rel_append_row(source, &first) == 0
        && col_rel_append_row(source, &second) == 0,
        "shared mutation source rows");
    uint64_t source_view = source->view_generation;
    uint64_t source_storage = source->storage_generation;

    col_rel_t *set_view = new_relation();
    CHECK(set_view != NULL && col_rel_install_shared_view(set_view,
        source) == 0,
        "shared set view");
    uint64_t set_view_before = set_view->view_generation;
    uint64_t set_storage_before = set_view->storage_generation;
    CHECK(col_rel_set(set_view, 0, 0, 99) == 0,
        "shared set detaches before write");
    CHECK(source->columns[0][0] == first
        && source->view_generation == source_view
        && source->storage_generation == source_storage,
        "shared set preserves source values and generations");
    CHECK(set_view->view_generation == set_view_before + 1u
        && set_view->storage_generation == set_storage_before + 1u,
        "shared set publishes one view and storage epoch");

    col_rel_t *append_view = new_relation();
    CHECK(append_view != NULL
        && col_rel_install_shared_view(append_view, source) == 0,
        "shared append view");
    uint64_t append_view_before = append_view->view_generation;
    uint64_t append_storage_before = append_view->storage_generation;
    int64_t appended = 7;
    CHECK(col_rel_append_row(append_view, &appended) == 0,
        "shared append with spare capacity");
    CHECK(source->nrows == 2 && source->columns[0][1] == second
        && source->view_generation == source_view
        && source->storage_generation == source_storage,
        "shared append preserves source");
    CHECK(append_view->view_generation == append_view_before + 1u
        && append_view->storage_generation == append_storage_before + 1u,
        "shared append publishes one view and storage epoch");

    col_rel_t *bulk_view = new_relation();
    CHECK(bulk_view != NULL
        && col_rel_install_shared_view(bulk_view, source) == 0,
        "shared bulk append view");
    uint64_t bulk_view_before = bulk_view->view_generation;
    uint64_t bulk_storage_before = bulk_view->storage_generation;
    CHECK(col_rel_append_all(bulk_view, source, NULL) == 0,
        "shared bulk append with spare capacity");
    CHECK(source->nrows == 2 && source->columns[0][0] == first
        && bulk_view->nrows == 4
        && source->view_generation == source_view
        && source->storage_generation == source_storage,
        "shared bulk append preserves source");
    CHECK(bulk_view->view_generation == bulk_view_before + 1u
        && bulk_view->storage_generation == bulk_storage_before + 1u,
        "shared bulk append publishes one view and storage epoch");

    col_rel_t *range_view = new_relation();
    CHECK(range_view != NULL
        && col_rel_install_shared_view(range_view, source) == 0,
        "shared direct sort view");
    uint64_t range_view_before = range_view->view_generation;
    uint64_t range_storage_before = range_view->storage_generation;
    CHECK(col_rel_radix_sort(range_view, 0, range_view->nrows) == 0,
        "direct range sort detaches before reorder");
    CHECK(source->columns[0][0] == first && source->columns[0][1] == second
        && source->view_generation == source_view
        && source->storage_generation == source_storage
        && range_view->columns[0][0] == second,
        "direct range sort preserves source");
    CHECK(range_view->view_generation == range_view_before + 1u
        && range_view->storage_generation == range_storage_before + 1u,
        "direct range sort publishes one view and storage epoch");

    col_rel_t *all_view = new_relation();
    CHECK(all_view != NULL && col_rel_install_shared_view(all_view,
        source) == 0,
        "shared all-row sort view");
    uint64_t all_view_before = all_view->view_generation;
    uint64_t all_storage_before = all_view->storage_generation;
    col_rel_radix_sort_int64(all_view);
    CHECK(source->columns[0][0] == first && source->columns[0][1] == second
        && source->view_generation == source_view
        && source->storage_generation == source_storage
        && all_view->columns[0][0] == second,
        "all-row sort preserves source");
    CHECK(all_view->view_generation == all_view_before + 1u
        && all_view->storage_generation == all_storage_before + 1u,
        "all-row sort publishes one view and storage epoch");

    col_rel_t *invalid = new_relation();
    CHECK(invalid != NULL, "append validation relation");
    uint64_t invalid_view = invalid->view_generation;
    uint64_t invalid_storage = invalid->storage_generation;
    uint32_t invalid_capacity = invalid->capacity;
    invalid->ncols = 0;
    CHECK(col_rel_append_row(invalid, &first) == 0,
        "zero-column empty-tuple append");
    CHECK(invalid->capacity == invalid_capacity
        && invalid->nrows == 1
        && invalid->view_generation == invalid_view + 1u
        && invalid->storage_generation == invalid_storage,
        "zero-column append publishes one epoch");
    invalid->nrows = 0;
    invalid->view_generation = invalid_view;
    invalid->storage_generation = invalid_storage;
    invalid->ncols = 1;
    int64_t *saved_column = invalid->columns[0];
    invalid->columns[0] = NULL;
    CHECK(col_rel_append_row(invalid, &first) == EINVAL,
        "incomplete append is rejected");
    CHECK(invalid->columns[0] == NULL && invalid->capacity == invalid_capacity
        && invalid->view_generation == invalid_view
        && invalid->storage_generation == invalid_storage,
        "incomplete append is atomic");
    invalid->columns[0] = saved_column;
    col_rel_destroy(invalid);
    owned_relation_count--;

#ifdef WL_TEST_ALLOC_WRAP
    bool append_completed = false;
    for (long fail_at = 0; fail_at < 8 && !append_completed; fail_at++) {
        col_rel_t *resize = new_relation();
        CHECK(resize != NULL, "append allocation-failure relation");
        for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++) {
            int64_t value = (int64_t)i;
            CHECK(col_rel_append_row(resize, &value) == 0,
                "append allocation-failure seed");
        }
        resize->timestamps = (col_delta_timestamp_t *)calloc(
            resize->capacity, sizeof(*resize->timestamps));
        CHECK(resize->timestamps != NULL,
            "append allocation-failure timestamps");
        int64_t **old_columns = resize->columns;
        col_delta_timestamp_t *old_timestamps = resize->timestamps;
        uint32_t old_capacity = resize->capacity;
        uint32_t old_rows = resize->nrows;
        uint64_t old_view = resize->view_generation;
        uint64_t old_storage = resize->storage_generation;
        allocation_calls = 0;
        allocation_fail_at = fail_at;
        int64_t value = 1234;
        int rc = col_rel_append_row(resize, &value);
        allocation_fail_at = -1;
        if (rc == 0) {
            append_completed = true;
            CHECK(resize->nrows == old_rows + 1u
                && resize->view_generation == old_view + 1u
                && resize->storage_generation == old_storage + 1u,
                "successful append publishes one view and storage epoch");
        } else {
            CHECK(rc == ENOMEM, "append allocation failure is reported");
            CHECK(resize->columns == old_columns
                && resize->timestamps == old_timestamps
                && resize->capacity == old_capacity
                && resize->nrows == old_rows
                && resize->view_generation == old_view
                && resize->storage_generation == old_storage
                && resize->columns[0][0] == 0,
                "append allocation failure is fully transactional");
        }
        col_rel_destroy(resize);
        owned_relation_count--;
    }
    allocation_fail_at = -1;
    CHECK(append_completed, "append allocation sweep reaches success");
#endif
    cleanup_relations();
}

static void
test_large_sort_epochs(void)
{
    col_rel_t *rel = new_relation();
    CHECK(rel != NULL, "large sort relation");
    for (uint32_t i = 0; i < 50000u; i++) {
        int64_t row = (int64_t)(50000u - i);
        CHECK(col_rel_append_row(rel, &row) == 0,
            "large sort input append");
    }
    uint64_t view_before = rel->view_generation;
    col_rel_radix_sort_int64(rel);
    CHECK(rel->view_generation == view_before + 1u,
        "k16 sort advances view exactly once");
    CHECK(rel->columns[0][0] == 1 && rel->columns[0][49999] == 50000,
        "k16 sort orders rows");

    col_rel_t *view = new_relation();
    CHECK(view != NULL, "large sort shared view");
    CHECK(col_rel_install_shared_view(view, rel) == 0,
        "large sort shared-view setup");
    view_before = view->view_generation;
    uint64_t storage_before = view->storage_generation;
    col_rel_radix_sort_int64(view);
    CHECK(view->view_generation == view_before + 1u
        && view->storage_generation == storage_before + 1u,
        "shared k16 sort publishes one view and storage epoch");
    cleanup_relations();
}

static void
test_append_type_failure_atomicity(void)
{
#ifdef WL_TEST_ALLOC_WRAP
    col_rel_t *src = new_relation();
    CHECK(src != NULL, "typed append source");
    int64_t source_value = 7;
    CHECK(col_rel_append_row(src, &source_value) == 0,
        "typed append source row");
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    CHECK(col_rel_set_column_types(src, &type, 1) == 0,
        "typed append source metadata");

    bool completed = false;
    for (long fail_at = 0; fail_at < 32 && !completed; fail_at++) {
        col_rel_t *dst = new_relation();
        CHECK(dst != NULL, "typed append destination");
        for (uint32_t i = 0; i < COL_REL_INIT_CAP; i++) {
            int64_t value = (int64_t)i;
            CHECK(col_rel_append_row(dst, &value) == 0,
                "typed append destination fill");
        }
        int64_t **old_columns = dst->columns;
        uint32_t old_rows = dst->nrows;
        uint32_t old_capacity = dst->capacity;
        uint64_t old_view = dst->view_generation;
        uint64_t old_storage = dst->storage_generation;
        allocation_calls = 0;
        allocation_fail_at = fail_at;
        int rc = col_rel_append_all(dst, src, NULL);
        allocation_fail_at = -1;
        if (rc == 0) {
            completed = true;
            CHECK(dst->column_types
                && dst->column_types[0] == WIRELOG_TYPE_INT64,
                "typed append publishes metadata");
            CHECK(dst->view_generation == old_view + 1u
                && dst->storage_generation == old_storage + 1u,
                "typed append publishes one view and storage epoch");
        } else {
            CHECK(rc == ENOMEM, "typed append failure is allocation failure");
            CHECK(dst->columns == old_columns && dst->nrows == old_rows
                && dst->capacity == old_capacity
                && dst->column_types == NULL
                && dst->view_generation == old_view
                && dst->storage_generation == old_storage,
                "typed append failure is fully transactional");
        }
        col_rel_destroy(dst);
        owned_relation_count--;
    }
    allocation_fail_at = -1;
    CHECK(completed, "typed append failure sweep reaches success");
    cleanup_relations();
#else
    fputs("relation generations: typed-append OOM coverage skipped\n",
        stderr);
#endif
}

static void
test_sort_failure_atomicity(void)
{
#ifdef WL_TEST_ALLOC_WRAP
    const uint32_t sizes[] = { 16u, 64u, 50000u };
    for (size_t case_idx = 0; case_idx < sizeof(sizes) / sizeof(sizes[0]);
        case_idx++) {
        uint32_t nrows = sizes[case_idx];
        col_rel_t *rel = new_relation();
        CHECK(rel != NULL, "sort failure relation");
        int64_t *original = (int64_t *)malloc((size_t)nrows
                * sizeof(*original));
        CHECK(original != NULL, "sort failure snapshot");
        for (uint32_t i = 0; i < nrows; i++) {
            int64_t value = (int64_t)(nrows - i);
            original[i] = value;
            CHECK(col_rel_append_row(rel, &value) == 0,
                "sort failure input");
        }
        uint64_t before = rel->view_generation;
        bool completed = false;
        for (long fail_at = 0; fail_at < 32 && !completed; fail_at++) {
            allocation_calls = 0;
            allocation_fail_at = fail_at;
            int rc = col_rel_radix_sort(rel, 0, rel->nrows);
            allocation_fail_at = -1;
            if (rc == 0) {
                completed = true;
                CHECK(rel->view_generation == before + 1u,
                    "successful sort advances view exactly once");
                CHECK(rel->columns[0][0] == 1
                    && rel->columns[0][nrows - 1] == (int64_t)nrows,
                    "successful sort is ordered");
            } else {
                CHECK(rc == ENOMEM, "sort failure is an allocation failure");
                CHECK(rel->view_generation == before,
                    "failed sort preserves view generation");
                for (uint32_t i = 0; i < nrows; i++)
                    CHECK(rel->columns[0][i] == original[i],
                        "failed sort preserves row contents");
            }
        }
        allocation_fail_at = -1;
        free(original);
        CHECK(completed, "sort failure sweep reaches successful publication");
        cleanup_relations();
    }

    col_rel_t *source = new_relation();
    col_rel_t *view = new_relation();
    CHECK(source && view, "shared sort failure relations");
    for (uint32_t i = 0; i < 64u; i++) {
        int64_t value = (int64_t)(64u - i);
        CHECK(col_rel_append_row(source, &value) == 0,
            "shared sort failure input");
    }
    CHECK(col_rel_install_shared_view(view, source) == 0,
        "shared sort failure view");
    int64_t *old_column = view->columns[0];
    bool *old_flags = view->col_shared;
    uint64_t old_view = view->view_generation;
    uint64_t old_storage = view->storage_generation;
    allocation_calls = 0;
    allocation_fail_at = 4; /* backups and COW succeed; k8 setup fails. */
    col_rel_radix_sort_int64(view);
    allocation_fail_at = -1;
    CHECK(view->columns[0] == old_column,
        "shared sort failure restores borrowed column");
    CHECK(view->col_shared != NULL && view->col_shared[0],
        "shared sort failure restores borrowed flag");
    CHECK(view->col_shared != old_flags,
        "shared sort failure recreates ownership flags");
    CHECK(view->view_generation == old_view
        && view->storage_generation == old_storage,
        "shared sort failure restores generations");
    cleanup_relations();
#else
    fputs("relation generations: sort OOM coverage skipped\n", stderr);
#endif
}

static void
test_failure_atomicity(void)
{
#ifdef WL_TEST_ALLOC_WRAP
    col_rel_t *src = new_relation();
    CHECK(src != NULL, "reduce failure source");
    int64_t source_row = 5;
    CHECK(col_rel_append_row(src, &source_row) == 0,
        "reduce failure source row");
    bool reduce_failed = false;
    for (long fail_at = 0; fail_at < 8; fail_at++) {
        col_rel_t *dst = new_relation();
        CHECK(dst != NULL, "reduce failure destination");
        int64_t old_row = 91;
        CHECK(col_rel_append_row(dst, &old_row) == 0,
            "reduce failure sentinel row");
        int64_t **old_columns = dst->columns;
        uint32_t old_rows = dst->nrows;
        uint32_t old_capacity = dst->capacity;
        uint64_t old_view = dst->view_generation;
        uint64_t old_storage = dst->storage_generation;
        allocation_calls = 0;
        allocation_fail_at = fail_at;
        int rc = col_op_reduce_weighted(src, dst);
        allocation_fail_at = -1;
        if (rc == ENOMEM) {
            reduce_failed = true;
            CHECK(dst->columns == old_columns && dst->nrows == old_rows
                && dst->capacity == old_capacity
                && dst->view_generation == old_view
                && dst->storage_generation == old_storage
                && dst->columns[0][0] == 91,
                "weighted reduce failure is atomic");
        }
    }
    CHECK(reduce_failed,
        "weighted reduce failure injection reached an allocation");

    col_rel_t *prev = new_relation();
    col_rel_t *curr = new_relation();
    CHECK(prev && curr, "Mobius failure inputs");
    int64_t prev_row = 1;
    int64_t curr_row = 2;
    CHECK(col_rel_append_row(prev, &prev_row) == 0
        && col_rel_append_row(curr, &curr_row) == 0,
        "Mobius failure input rows");
    prev->timestamps = (col_delta_timestamp_t *)calloc(prev->capacity,
            sizeof(*prev->timestamps));
    curr->timestamps = (col_delta_timestamp_t *)calloc(curr->capacity,
            sizeof(*curr->timestamps));
    CHECK(prev->timestamps && curr->timestamps, "Mobius failure timestamps");
    prev->timestamps[0].multiplicity = 1;
    curr->timestamps[0].multiplicity = 2;
    bool delta_failed = false;
    for (long fail_at = 0; fail_at < 10; fail_at++) {
        col_rel_t *out = new_relation();
        CHECK(out != NULL, "Mobius failure output");
        /* new_relation has an allocated empty column array; the Mobius
         * contract accepts this typed, zero-row output only when capacity is
         * zero, so make it an explicitly pristine typed relation. */
        col_columns_free(out->columns, out->ncols);
        out->columns = NULL;
        out->capacity = 0;
        uint64_t old_view = out->view_generation;
        uint64_t old_storage = out->storage_generation;
        allocation_calls = 0;
        allocation_fail_at = fail_at;
        int rc = col_compute_delta_mobius(prev, curr, out);
        allocation_fail_at = -1;
        if (rc == ENOMEM) {
            delta_failed = true;
            CHECK(out->columns == NULL && out->timestamps == NULL
                && out->nrows == 0 && out->capacity == 0
                && out->view_generation == old_view
                && out->storage_generation == old_storage,
                "Mobius failure is atomic");
        }
    }
    CHECK(delta_failed, "Mobius failure injection reached an allocation");
    cleanup_relations();
#else
    fputs("relation generations: operator OOM coverage skipped\n",
        stderr);
#endif
}

static void
test_resize_failure_atomicity(void)
{
#ifdef WL_TEST_ALLOC_WRAP
    for (int operation = 0; operation < 3; operation++) {
        for (long fail_at = 0; fail_at < 3; fail_at++) {
            col_rel_t *src = new_relation();
            col_rel_t *dst = new_relation();
            CHECK(src && dst, "resize failure relations");
            int64_t row = 100;
            uint32_t rows = operation == 2 ? 1u : 64u;
            for (uint32_t i = 0; i < rows; i++) {
                row = (int64_t)i;
                CHECK(col_rel_append_row(src, &row) == 0,
                    "resize failure source rows");
            }
            src->timestamps = (col_delta_timestamp_t *)calloc(
                src->capacity, sizeof(*src->timestamps));
            CHECK(src->timestamps != NULL, "resize failure timestamps");
            if (operation == 1) {
                int64_t extra = 999;
                CHECK(col_rel_append_row(dst, &extra) == 0,
                    "append-all destination seed");
                /* Fill the destination to its capacity so append_all must
                 * prepare a COW+resize publication. */
                for (uint32_t i = 1; i < 64; i++)
                    CHECK(col_rel_append_row(dst, &extra) == 0,
                        "append-all destination fill");
            }
            CHECK(col_rel_install_shared_view(dst, src) == 0,
                "resize failure shared view");
            int64_t **old_columns = dst->columns;
            bool *old_shared = dst->col_shared;
            col_delta_timestamp_t *old_timestamps = dst->timestamps;
            uint32_t old_capacity = dst->capacity;
            uint32_t old_rows = dst->nrows;
            uint64_t old_view = dst->view_generation;
            uint64_t old_storage = dst->storage_generation;
            int64_t old_value = dst->columns[0][0];

            allocation_calls = 0;
            allocation_fail_at = fail_at;
            if (operation == 0) {
                int64_t extra = 1001;
                int resize_rc = col_rel_append_row(dst, &extra);
                allocation_fail_at = -1;
                CHECK(resize_rc == ENOMEM,
                    "append-row resize failure is injected");
            } else if (operation == 1) {
                int resize_rc = col_rel_append_all(dst, src, NULL);
                allocation_fail_at = -1;
                CHECK(resize_rc == ENOMEM,
                    "append-all resize failure is injected");
            } else {
                col_rel_compact(dst);
                allocation_fail_at = -1;
            }
            CHECK(dst->columns == old_columns && dst->col_shared == old_shared
                && dst->timestamps == old_timestamps
                && dst->capacity == old_capacity && dst->nrows == old_rows
                && dst->view_generation == old_view
                && dst->storage_generation == old_storage
                && dst->columns[0][0] == old_value,
                "COW and resize failure preserve the complete relation");
            cleanup_relations();
        }
    }
#else
    fputs("relation generations: resize OOM coverage skipped\n", stderr);
#endif
}

static void
test_shared_view_relation_metadata(void)
{
    col_rel_t *src = new_relation();
    col_rel_t *dst = new_relation();
    CHECK(src && dst, "relation metadata relations");

    src->has_graph_column = true;
    src->graph_col_idx = 0u;
    src->declared_ncols = 1u;
    src->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    src->compound_count = 1u;
    src->inline_physical_offset = 0u;
    src->compound_arity_map = (uint32_t *)malloc(sizeof(uint32_t));
    CHECK(src->compound_arity_map != NULL, "source compound map");
    src->compound_arity_map[0] = 1u;

    dst->has_graph_column = false;
    dst->graph_col_idx = 77u;
    dst->declared_ncols = 99u;
    dst->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
    dst->compound_count = 0u;
    dst->inline_physical_offset = 0u;
    dst->compound_arity_map = (uint32_t *)malloc(sizeof(uint32_t));
    CHECK(dst->compound_arity_map != NULL, "destination compound map");
    dst->compound_arity_map[0] = 1u;

    CHECK(col_rel_install_shared_view(dst, src) == 0,
        "copy graph and compound metadata");
    CHECK(dst->has_graph_column && dst->graph_col_idx == 0u
        && dst->declared_ncols == 1u,
        "graph and declared metadata copied");
    CHECK(dst->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
        && dst->compound_count == 1u
        && dst->inline_physical_offset == 0u
        && dst->compound_arity_map != NULL
        && dst->compound_arity_map != src->compound_arity_map
        && dst->compound_arity_map[0] == 1u,
        "inline metadata copied without aliasing");
    src->compound_arity_map[0] = 9u;
    CHECK(dst->compound_arity_map[0] == 1u,
        "shared-view compound map is a deep copy");

    free(src->compound_arity_map);
    src->compound_arity_map = (uint32_t *)malloc(sizeof(uint32_t));
    CHECK(src->compound_arity_map != NULL, "side compound map");
    src->compound_arity_map[0] = 1u;
    src->has_graph_column = false;
    src->declared_ncols = 0u;
    src->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
    src->compound_count = 0u;
    src->inline_physical_offset = 0u;
    CHECK(col_rel_install_shared_view(dst, src) == 0,
        "reinstall side metadata");
    CHECK(!dst->has_graph_column && dst->declared_ncols == 0u
        && dst->compound_kind == WIRELOG_COMPOUND_KIND_SIDE
        && dst->compound_count == 0u
        && dst->compound_arity_map != NULL
        && dst->compound_arity_map != src->compound_arity_map,
        "side metadata replaces prior inline metadata");

    free(src->compound_arity_map);
    src->compound_arity_map = NULL;
    src->compound_kind = WIRELOG_COMPOUND_KIND_NONE;
    src->compound_count = 0u;
    src->inline_physical_offset = 0u;
    CHECK(col_rel_install_shared_view(dst, src) == 0,
        "reinstall none metadata");
    CHECK(dst->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && dst->compound_count == 0u
        && dst->inline_physical_offset == 0u
        && dst->compound_arity_map == NULL,
        "NONE explicitly resets compound metadata");

    src->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    src->compound_count = 1u;
    src->compound_arity_map = (uint32_t *)calloc(1u, sizeof(uint32_t));
    CHECK(src->compound_arity_map != NULL, "malformed source map");
    uint32_t *saved_map = dst->compound_arity_map;
    uint64_t saved_view = dst->view_generation;
    uint64_t saved_storage = dst->storage_generation;
    CHECK(col_rel_install_shared_view(dst, src) == EINVAL,
        "malformed compound metadata is rejected");
    CHECK(dst->compound_arity_map == saved_map
        && dst->view_generation == saved_view
        && dst->storage_generation == saved_storage,
        "malformed source leaves destination unchanged");

#ifdef WL_TEST_ALLOC_WRAP
    free(src->compound_arity_map);
    src->compound_arity_map = (uint32_t *)malloc(sizeof(uint32_t));
    CHECK(src->compound_arity_map != NULL, "failure-sweep source map");
    src->compound_arity_map[0] = 1u;
    src->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    src->compound_count = 1u;
    src->inline_physical_offset = 0u;
    bool completed = false;
    for (long fail_at = 0; fail_at < 64 && !completed; fail_at++) {
        col_rel_t *candidate = new_relation();
        CHECK(candidate != NULL, "metadata failure destination");
        candidate->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        candidate->compound_arity_map = (uint32_t *)malloc(sizeof(uint32_t));
        CHECK(candidate->compound_arity_map != NULL,
            "metadata failure old map");
        candidate->compound_arity_map[0] = 1u;
        uint32_t *old_map = candidate->compound_arity_map;
        uint64_t old_view = candidate->view_generation;
        uint64_t old_storage = candidate->storage_generation;
        allocation_calls = 0;
        allocation_fail_at = fail_at;
        int rc = col_rel_install_shared_view(candidate, src);
        allocation_fail_at = -1;
        if (rc == 0) {
            completed = true;
        } else {
            CHECK(rc == ENOMEM, "metadata allocation failure is reported");
            CHECK(candidate->compound_arity_map == old_map
                && candidate->compound_kind == WIRELOG_COMPOUND_KIND_SIDE
                && candidate->view_generation == old_view
                && candidate->storage_generation == old_storage,
                "metadata allocation failure is transactional");
        }
        col_rel_destroy(candidate);
        owned_relation_count--;
    }
    allocation_fail_at = -1;
    CHECK(completed, "metadata allocation sweep reaches success");
#endif
    cleanup_relations();
}

int
main(void)
{
    test_same_row_count_mutation();
    test_storage_only_cow_and_compaction();
    test_flattened_storage_ownership();
    test_source_reader_blocks_checked_destroy();
    test_source_reader_blocks_direct_append_row();
    test_source_reader_blocks_append_row();
    test_copy_and_shared_semantics();
    test_rollback_fresh_generation();
    test_overflow_boundary();
    test_direct_publication_paths();
    test_shared_view_metadata_and_pool_rejection();
    test_shared_mutation_cow_and_append_validation();
    test_exact_publication_epochs();
    test_large_sort_epochs();
    test_append_type_failure_atomicity();
    test_sort_failure_atomicity();
    test_failure_atomicity();
    test_resize_failure_atomicity();
    test_shared_view_relation_metadata();
    test_identity_exhaustion();
    if (failures != 0)
        return EXIT_FAILURE;
    puts("relation generations: PASS");
    return EXIT_SUCCESS;
}
