/* Relation mutation admission and lease authority (#2033). */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/thread.h"
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    wl_columnar_relation_mutation_set_t set;
    wl_columnar_relation_mutation_descriptor_t descriptors[4];
    wl_columnar_relation_mutation_owner_t owners[4];
    wl_columnar_relation_mutation_lease_t leases[4];
    wl_columnar_relation_mutation_initialization_t initializations[4];
} fixture_t;

static int
acquire(fixture_t *f, wl_columnar_relation_mutation_role_t *roles, size_t n)
{
    return col_rel_mutation_set_acquire(&f->set, roles, n,
               f->descriptors, 4, f->owners, 4, f->leases, 4,
               f->initializations, 4);
}

static void
root_init(col_rel_t *r, uint64_t id)
{
    memset(r, 0, sizeof(*r));
    r->relation_identity = id;
    r->storage_generation = 1;
    r->view_generation = 1;
    r->storage_owner = r;
    r->storage_owner_identity = id;
    r->storage_owner_generation = 1;
    atomic_init(&r->storage_alias_borrows, 0);
    wl_columnar_source_access_gate_init(&r->descriptor_access);
    wl_columnar_source_access_gate_init(&r->source_access);
}

static void
alias_init(col_rel_t *a, col_rel_t *r, uint64_t id)
{
    root_init(a, id);
    a->storage_owner = r;
    a->storage_owner_identity = r->relation_identity;
    a->storage_owner_generation = r->storage_generation;
    assert(col_rel_storage_alias_borrow_acquire(r) == 0);
}

static void
assert_open(col_rel_t *r)
{
    assert(!wl_columnar_source_access_gate_busy(&r->descriptor_access));
    assert(!wl_columnar_source_access_gate_busy(&r->source_access));
}

static void *
cross_thread(void *arg)
{
    wl_columnar_relation_mutation_lease_t *lease = arg;
    assert(col_rel_mutation_lease_validate(lease, lease->relation) == EINVAL);
    assert(col_rel_storage_alias_release_locked(lease->relation,
        lease) == EINVAL);
    assert(col_rel_mutation_set_finish(lease->set, true) == EINVAL);
    return NULL;
}

static void
lease_tests(void)
{
    col_rel_t root, a, b;
    root_init(&root, 1);
    alias_init(&a, &root, 2);
    alias_init(&b, &root, 3);
    fixture_t f = {0};
    wl_columnar_relation_mutation_role_t roles[] = {
        {&a, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION},
        {&b, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION},
        {&a, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION}
    };
    assert(acquire(&f, roles, 3) == 0);
    assert(f.set.descriptor_count == 2 && f.set.owner_count == 1);
    assert(&f.leases[0] != &f.leases[2]);
    for (size_t i = 0; i < 3; i++)
        assert(col_rel_mutation_set_lease(&f.set, i) == &f.leases[i]);
    wl_columnar_relation_mutation_set_t set_copy = f.set;
    set_copy.identity = (uintptr_t)&set_copy;
    assert(col_rel_mutation_set_lease(&set_copy, 0) == NULL);
    assert(col_rel_mutation_set_finish(&set_copy, true) == EINVAL);
    f.leases[0].role_flags = WL_COLUMNAR_RELATION_METADATA_DETACH;
    assert(col_rel_mutation_lease_validate(&f.leases[0], &a) == EINVAL);
    f.leases[0].role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION;
    wl_columnar_relation_mutation_lease_t copy = f.leases[0];
    assert(col_rel_mutation_lease_validate(&copy, &a) == EINVAL);
    copy.identity = (uintptr_t)&copy;
    assert(col_rel_mutation_lease_validate(&copy, &a) == EINVAL);
    assert(col_rel_mutation_lease_validate(&f.leases[0], &b) == EINVAL);
    a.relation_identity++;
    assert(col_rel_mutation_lease_validate(&f.leases[0], &a) == EINVAL);
    a.relation_identity--;
    a.storage_generation++;
    assert(col_rel_mutation_lease_validate(&f.leases[0], &a) == EINVAL);
    a.storage_generation--;
    root.storage_generation++;
    assert(col_rel_mutation_lease_validate(&f.leases[0], &a) == EINVAL);
    root.storage_generation--;
    wl_thread_t thread;
    assert(wl_thread_create(&thread, cross_thread, &f.leases[0]) == 0);
    assert(wl_thread_join(&thread) == 0);
    assert(col_rel_storage_alias_release_locked(&a, &f.leases[0]) == 0);
    assert(a.storage_owner == &a &&
        col_rel_storage_alias_borrow_count(&root) == 1);
    assert(col_rel_storage_alias_release_locked(&a, &f.leases[0]) == EINVAL);
    assert(col_rel_mutation_lease_validate(&f.leases[2], &a) == EINVAL);
    assert(col_rel_storage_alias_release_locked(&b, &f.leases[1]) == 0);
    assert(col_rel_mutation_set_finish(&f.set, true) == 0);
    assert_open(&root);
    assert_open(&a);
    assert_open(&b);
    assert(col_rel_mutation_lease_validate(&f.leases[0], &a) == EINVAL);
    /* A raw source writer has no descriptor/set authority. */
    wl_columnar_source_access_writer_t raw = {0};
    assert(wl_columnar_source_access_writer_acquire(&root.source_access,
        &raw) == 0);
    copy = (wl_columnar_relation_mutation_lease_t){0};
    assert(col_rel_storage_alias_release_locked(&a, &copy) == EINVAL);
    assert(wl_columnar_source_access_writer_release(&raw) == 0);
}

static void
contention_tests(void)
{
    col_rel_t root, a;
    root_init(&root, 4);
    alias_init(&a, &root, 5);
    wl_columnar_memory_resolution_t resolution = {0};
    resolution.budget_bytes = resolution.usable_bytes = 1024;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    wl_columnar_memory_governor_t governor;
    assert(wl_columnar_memory_governor_init(&governor, &resolution)
        == WL_COLUMNAR_MEMORY_OK);
    wl_columnar_memory_reservation_init(&a.retained_reservation);
    assert(wl_columnar_memory_reserve(&governor, 64, &a.retained_reservation));
    assert(wl_columnar_memory_commit(&a.retained_reservation, &a));
    a.retained_reserved_bytes = 64;
    int64_t payload = 42;
    int64_t *columns[] = {&payload};
    root.columns = a.columns = columns;
    root.ncols = a.ncols = root.nrows = a.nrows = 1;
    fixture_t f = {0};
    wl_columnar_relation_mutation_role_t role = {&a,
                                                 WL_COLUMNAR_RELATION_PAYLOAD_MUTATION};
    wl_columnar_source_access_reader_t reader = {0};
    assert(wl_columnar_source_access_reader_acquire(&a.descriptor_access,
        &reader) == 0);
    col_rel_t before = a;
    assert(acquire(&f, &role, 1) == EBUSY);
    assert(memcmp(&a, &before, sizeof(a)) == 0);
    assert(wl_columnar_memory_reserved(&governor) == 64);
    assert(col_rel_storage_alias_borrow_count(&root) == 1);
    assert(wl_columnar_source_access_reader_release(&reader) == 0);
    assert(wl_columnar_source_access_reader_acquire(&root.source_access,
        &reader) == 0);
    before = a;
    assert(acquire(&f, &role, 1) == EBUSY);
    assert(memcmp(&a, &before, sizeof(a)) == 0);
    assert(wl_columnar_memory_reserved(&governor) == 64);
    assert(!wl_columnar_source_access_gate_busy(&a.descriptor_access));
    role.role_flags = WL_COLUMNAR_RELATION_METADATA_DETACH;
    assert(acquire(&f, &role, 1) == 0);
    assert(f.set.owner_count == 0);
    assert(col_rel_storage_alias_release_locked(&a, &f.leases[0]) == 0);
    assert(col_rel_storage_alias_borrow_count(&root) == 0);
    assert(root.columns[0][0] == 42);
    assert(col_rel_storage_owner_destroy_status(&root) == EBUSY);
    assert(col_rel_mutation_set_finish(&f.set, true) == 0);
    assert(wl_columnar_source_access_reader_release(&reader) == 0);
    role.role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION;
    assert(acquire(&f, &role, 1) == 0);
    assert(col_rel_mutation_set_finish(&f.set, true) == 0);
    assert(wl_columnar_memory_reserved(&governor) == 64);
    assert(wl_columnar_memory_release(&a.retained_reservation));
    assert(wl_columnar_memory_reserved(&governor) == 0);
}

static void
rollback_tests(void)
{
    col_rel_t relations[2];
    root_init(&relations[0], 6);
    root_init(&relations[1], 7);
    relations[0].storage_owner = NULL;
    fixture_t f = {0};
    wl_columnar_relation_mutation_role_t roles[] = {
        {&relations[1], WL_COLUMNAR_RELATION_PAYLOAD_MUTATION},
        {&relations[0], WL_COLUMNAR_RELATION_PAYLOAD_MUTATION}
    };
    wl_columnar_source_access_reader_t reader = {0};
    col_rel_t before = relations[0];
    /* Legacy ownership must remain untouched while its descriptor is read. */
    assert(wl_columnar_source_access_reader_acquire(
            &relations[0].descriptor_access,
            &reader) == 0);
    col_rel_t paused = relations[0];
    assert(acquire(&f, roles, 2) == EBUSY);
    assert(memcmp(&relations[0], &paused, sizeof(paused)) == 0);
    assert(wl_columnar_source_access_reader_release(&reader) == 0);
    /* Sorted second descriptor is busy, regardless of input order. */
    assert(wl_columnar_source_access_reader_acquire(
            &relations[1].descriptor_access,
            &reader) == 0);
    assert(acquire(&f, roles, 2) == EBUSY);
    assert(memcmp(&relations[0], &before, sizeof(before)) == 0);
    assert_open(&relations[0]);
    assert(wl_columnar_source_access_reader_release(&reader) == 0);
    /* Sorted second owner failure follows provisional initialization. */
    assert(wl_columnar_source_access_reader_acquire(&relations[1].source_access,
        &reader) == 0);
    assert(acquire(&f, roles, 2) == EBUSY);
    assert(memcmp(&relations[0], &before, sizeof(before)) == 0);
    assert_open(&relations[0]);
    assert(!wl_columnar_source_access_gate_busy(
            &relations[1].descriptor_access));
    assert(wl_columnar_source_access_reader_release(&reader) == 0);
    assert(acquire(&f, roles, 2) == 0);
    assert(relations[0].storage_owner == &relations[0]);
    assert(col_rel_mutation_set_finish(&f.set, false) == 0);
    assert(memcmp(&relations[0], &before, sizeof(before)) == 0);
    assert(acquire(&f, roles, 2) == 0);
    assert(col_rel_mutation_set_finish(&f.set, true) == 0);
    assert(relations[0].storage_owner == &relations[0]);
    assert_open(&relations[0]);
    assert_open(&relations[1]);
}

/* The metadata set must not retain a dereferenceable old-owner dependency
 * after the final borrow drops. ASan checks this finish-after-retirement path. */
static void
metadata_final_access_test(void)
{
    col_rel_t *root = malloc(sizeof(*root));
    assert(root);
    root_init(root, 10);
    col_rel_t alias;
    alias_init(&alias, root, 11);
    fixture_t f = {0};
    wl_columnar_relation_mutation_role_t role = {&alias,
                                                 WL_COLUMNAR_RELATION_METADATA_DETACH};
    assert(acquire(&f, &role, 1) == 0);
    assert(col_rel_storage_alias_release_locked(&alias, &f.leases[0]) == 0);
    assert(col_rel_storage_alias_borrow_count(root) == 0);
    free(root);
    assert(col_rel_mutation_set_finish(&f.set, true) == 0);
    assert_open(&alias);
}

static void
invalid_tests(void)
{
    col_rel_t root, alias;
    root_init(&root, 8);
    fixture_t f = {0};
    wl_columnar_relation_mutation_role_t role = {&root,
                                                 WL_COLUMNAR_RELATION_PAYLOAD_MUTATION};
    assert(acquire(&f, &role, 0) == EINVAL);
    assert(col_rel_mutation_set_finish(&f.set, true) == EINVAL);
    assert(col_rel_mutation_set_lease(&f.set, 0) == NULL);
    assert(col_rel_mutation_set_acquire(NULL, &role, 1,
        f.descriptors, 4, f.owners, 4, f.leases, 4,
        f.initializations, 4) == EINVAL);
    assert(col_rel_mutation_set_acquire(&f.set, &role, SIZE_MAX,
        f.descriptors, SIZE_MAX, f.owners, SIZE_MAX, f.leases, SIZE_MAX,
        f.initializations, SIZE_MAX) == EOVERFLOW);
    assert(acquire(&f, NULL, 1) == EINVAL);
    role.relation = NULL;
    assert(acquire(&f, &role, 1) == EINVAL);
    role.relation = &root;
    assert(col_rel_mutation_set_acquire(&f.set, &role, 1,
        f.descriptors, 0, f.owners, 4, f.leases, 4,
        f.initializations, 4) == EINVAL);
    f.descriptors[0].writer.identity = 1;
    assert(acquire(&f, &role, 1) == EINVAL);
    memset(&f, 0, sizeof(f));
    alias_init(&alias, &root, 9);
    assert(acquire(&f, &role, 1) == EBUSY);
    assert_open(&root);
    assert(root.storage_owner == &root);
    assert(col_rel_storage_alias_borrow_count(&root) == 1);
}

int
main(void)
{
    lease_tests();
    contention_tests();
    rollback_tests();
    invalid_tests();
    metadata_final_access_test();
    puts("relation mutation set tests passed");
    return 0;
}
