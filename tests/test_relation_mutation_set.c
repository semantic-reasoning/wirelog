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
    wl_columnar_relation_mutation_owner_t owners[8];
    wl_columnar_relation_mutation_lease_t leases[4];
    wl_columnar_relation_mutation_initialization_t initializations[4];
} fixture_t;

static int
acquire(fixture_t *f, wl_columnar_relation_mutation_role_t *roles, size_t n)
{
    return col_rel_mutation_set_acquire(&f->set, roles, n,
               f->descriptors, 4, f->owners, 8, f->leases, 4,
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
    assert(f.set.descriptor_count == 2 && f.set.owner_count == 3);
    assert(wl_columnar_source_access_gate_busy(&root.source_access));
    assert(wl_columnar_source_access_gate_busy(&a.source_access));
    assert(wl_columnar_source_access_gate_busy(&b.source_access));
    assert(f.owners[f.leases[0].owner_slot].owner == &root
        && f.owners[f.leases[0].self_owner_slot].owner == &a
        && f.owners[f.leases[1].owner_slot].owner == &root
        && f.owners[f.leases[1].self_owner_slot].owner == &b);
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
    assert(col_rel_mutation_lease_validate(&f.leases[0], &a) == 0
        && col_rel_mutation_lease_validate(&f.leases[1], &b) == 0
        && col_rel_mutation_lease_validate(&f.leases[2], &a) == 0);
    assert(col_rel_storage_alias_borrow_count(&root) == 2);
    assert(col_rel_mutation_set_finish(&f.set, true) == 0);
    assert_open(&root);
    assert_open(&a);
    assert_open(&b);
    assert(col_rel_mutation_lease_validate(&f.leases[0], &a) == EINVAL);
    assert(col_rel_storage_alias_borrow_count(&root) == 2);
    /* A raw source writer has no descriptor/set authority. */
    wl_columnar_source_access_writer_t raw = {0};
    assert(wl_columnar_source_access_writer_acquire(&root.source_access,
        &raw) == 0);
    copy = (wl_columnar_relation_mutation_lease_t){0};
    assert(col_rel_storage_alias_release_locked(&a, &copy) == EINVAL);
    assert(wl_columnar_source_access_writer_release(&raw) == 0);
}

static void
duplicate_role_publication_tests(void)
{
    col_rel_t relation;
    root_init(&relation, 12);
    fixture_t f = {0};
    wl_columnar_relation_mutation_role_t roles[] = {
        {&relation, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION},
        {&relation, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION}
    };
    assert(acquire(&f, roles, 2) == 0);
    assert(f.set.descriptor_count == 1 && f.set.owner_count == 1);

    /* Model an irreversible same-owner resize publication. Advancing either
     * role must advance both leases before failed-transaction cleanup. */
    relation.storage_generation = 2;
    relation.storage_owner_generation = 2;
    assert(wl_columnar_relation_test_mutation_lease_advance_storage(
            &f.leases[0], 1) == 0);
    assert(f.leases[0].storage_transitioned
        && f.leases[1].storage_transitioned);
    assert(col_rel_mutation_lease_validate(&f.leases[0], &relation) == 0
        && col_rel_mutation_lease_validate(&f.leases[1], &relation) == 0);
    /* A later operation may fail after publication. Finish must retain the
    * publication and validate its refreshed authority before unwinding. */
    assert(col_rel_mutation_set_finish(&f.set, false) == 0);
    assert_open(&relation);
}

static void
terminal_sequence_tests(void)
{
    col_rel_t relation;
    root_init(&relation, 20);
    int64_t old_value = 1, new_value = 2;
    int64_t *old_columns[] = { &old_value };
    int64_t *new_columns[] = { &new_value };
    relation.columns = old_columns;
    relation.capacity = 3;
    relation.merge_columns = new_columns;
    relation.merge_buf_cap = 4;
    relation.timestamps = malloc(sizeof(*relation.timestamps) * 3);
    assert(relation.timestamps != NULL);
    relation.timestamp_capacity = 3;

    fixture_t f = {0};
    wl_columnar_relation_mutation_role_t roles[] = {
        {&relation, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION},
        {&relation, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION}
    };
    assert(acquire(&f, roles, 2) == 0);
    wl_columnar_relation_terminal_sequence_t sequence = {0};
    assert(wl_columnar_relation_terminal_sequence_begin(&sequence,
        &f.leases[0], 0) == EINVAL);
    uint64_t original_generation = relation.storage_generation;
    f.leases[1].storage_transitioned = true;
    assert(wl_columnar_relation_terminal_sequence_begin(&sequence,
        &f.leases[0], WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP
        | WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE) == EINVAL);
    assert(f.set.terminal_sequence == NULL
        && f.set.terminal_expected_events == 0
        && f.set.terminal_observed_events == 0);
    f.leases[1].storage_transitioned = false;
    relation.storage_generation++;
    relation.storage_owner_generation++;
    assert(wl_columnar_relation_test_mutation_lease_advance_storage(
            &f.leases[0], original_generation) == 0);
    original_generation = relation.storage_generation;
    assert(f.leases[0].storage_transitioned
        && f.leases[1].storage_transitioned
        && f.leases[0].relation_generation == original_generation
        && f.leases[1].relation_generation == original_generation);
    assert(wl_columnar_relation_terminal_sequence_begin(&sequence,
        &f.leases[0], WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP
        | WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE) == 0);
    assert(wl_columnar_relation_test_terminal_sequence_validate(&sequence));
    uint32_t saved_expected = sequence.expected_events;
    sequence.expected_events = WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP;
    assert(!wl_columnar_relation_test_terminal_sequence_validate(&sequence));
    sequence.expected_events = saved_expected;
    uint32_t saved_role = roles[1].role_flags;
    col_rel_t unrelated;
    roles[1].relation = &unrelated;
    roles[1].role_flags = WL_COLUMNAR_RELATION_METADATA_DETACH;
    assert(!wl_columnar_relation_test_terminal_sequence_validate(&sequence));
    roles[1].relation = &relation;
    roles[1].role_flags = saved_role;
    saved_role = f.leases[1].role_flags;
    f.leases[1].role_flags = WL_COLUMNAR_RELATION_METADATA_DETACH;
    assert(!wl_columnar_relation_test_terminal_sequence_validate(&sequence));
    f.leases[1].role_flags = saved_role;
    bool saved_detached = f.leases[1].detached;
    f.leases[1].detached = !saved_detached;
    assert(!wl_columnar_relation_test_terminal_sequence_validate(&sequence));
    f.leases[1].detached = saved_detached;
    bool saved_transitioned = f.leases[1].storage_transitioned;
    f.leases[1].storage_transitioned = !saved_transitioned;
    assert(!wl_columnar_relation_test_terminal_sequence_validate(&sequence));
    f.leases[1].storage_transitioned = saved_transitioned;
    assert(wl_columnar_relation_test_terminal_sequence_validate(&sequence));

    int64_t **columns = relation.columns;
    uint32_t capacity = relation.capacity;
    relation.columns = relation.merge_columns;
    relation.capacity = relation.merge_buf_cap;
    relation.merge_columns = columns;
    relation.merge_buf_cap = capacity;
    wl_columnar_relation_terminal_sequence_publish_grid_swap(&sequence);
    columns = relation.columns;
    capacity = relation.capacity;
    relation.columns = relation.merge_columns;
    relation.capacity = relation.merge_buf_cap;
    relation.merge_columns = columns;
    relation.merge_buf_cap = capacity;
    assert(!wl_columnar_relation_test_terminal_sequence_validate(&sequence));
    columns = relation.columns;
    capacity = relation.capacity;
    relation.columns = relation.merge_columns;
    relation.capacity = relation.merge_buf_cap;
    relation.merge_columns = columns;
    relation.merge_buf_cap = capacity;
    assert(wl_columnar_relation_test_terminal_sequence_validate(&sequence));
    free(relation.timestamps);
    relation.timestamps = NULL;
    relation.timestamp_capacity = 0;
    wl_columnar_relation_terminal_sequence_publish_timestamp_retirement(
        &sequence);
    assert(wl_columnar_relation_terminal_sequence_finish(&sequence) == 0);
    assert(relation.storage_generation == original_generation + 2
        && relation.storage_owner_generation == original_generation + 2
        && f.leases[0].relation_generation == original_generation + 2
        && f.leases[1].relation_generation == original_generation + 2
        && f.leases[0].owner_generation == original_generation + 2
        && f.leases[1].owner_generation == original_generation + 2);
    wl_columnar_relation_terminal_sequence_t replay = {0};
    assert(wl_columnar_relation_terminal_sequence_begin(&replay,
        &f.leases[0], WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP) == EINVAL);
    assert(relation.storage_generation == original_generation + 2);
    assert(col_rel_mutation_set_finish(&f.set, true) == 0);
    assert_open(&relation);

    col_rel_t owner, alias;
    root_init(&owner, 21);
    alias_init(&alias, &owner, 22);
    alias.capacity = 1;
    alias.timestamps = malloc(sizeof(*alias.timestamps));
    assert(alias.timestamps != NULL);
    alias.timestamp_capacity = 1;
    fixture_t af = {0};
    wl_columnar_relation_mutation_role_t alias_roles[] = {
        {&alias, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION},
        {&alias, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION}
    };
    assert(acquire(&af, alias_roles, 2) == 0);
    wl_columnar_relation_terminal_sequence_t alias_sequence = {0};
    assert(wl_columnar_relation_terminal_sequence_begin(&alias_sequence,
        &af.leases[0],
        WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE) == 0);
    free(alias.timestamps);
    alias.timestamps = NULL;
    alias.timestamp_capacity = 0;
    wl_columnar_relation_terminal_sequence_publish_timestamp_retirement(
        &alias_sequence);
    assert(wl_columnar_relation_terminal_sequence_finish(&alias_sequence) == 0);
    assert(alias.storage_owner == &owner
        && alias.storage_owner_identity == owner.relation_identity
        && alias.storage_owner_generation == owner.storage_generation
        && alias.storage_generation == af.leases[0].relation_generation
        && af.leases[0].owner_generation == owner.storage_generation
        && af.leases[1].owner_generation == owner.storage_generation);
    assert(col_rel_mutation_set_finish(&af.set, true) == 0);
    assert_open(&owner);
    assert_open(&alias);

    root_init(&relation, 24);
    relation.capacity = 2;
    relation.merge_buf_cap = 3;
    fixture_t nf = {0};
    wl_columnar_relation_mutation_role_t nullary_role = {
        &relation, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
    };
    assert(acquire(&nf, &nullary_role, 1) == 0);
    wl_columnar_relation_terminal_sequence_t nullary_sequence = {0};
    assert(wl_columnar_relation_terminal_sequence_begin(&nullary_sequence,
        &nf.leases[0], WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP) == 0);
    uint32_t old_capacity = relation.capacity;
    relation.capacity = relation.merge_buf_cap;
    relation.merge_buf_cap = old_capacity;
    wl_columnar_relation_terminal_sequence_publish_grid_swap(
        &nullary_sequence);
    assert(wl_columnar_relation_terminal_sequence_finish(
            &nullary_sequence) == 0);
    assert(relation.storage_generation == 2
        && relation.storage_owner_generation == 2);
    assert(col_rel_mutation_set_finish(&nf.set, true) == 0);
    assert_open(&relation);

    col_rel_t nullary_owner, nullary_alias;
    root_init(&nullary_owner, 25);
    alias_init(&nullary_alias, &nullary_owner, 26);
    nullary_alias.capacity = 2;
    nullary_alias.merge_buf_cap = 3;
    fixture_t naf = {0};
    wl_columnar_relation_mutation_role_t nullary_alias_role = {
        &nullary_alias, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
    };
    assert(acquire(&naf, &nullary_alias_role, 1) == 0);
    wl_columnar_relation_terminal_sequence_t nullary_alias_sequence = {0};
    assert(wl_columnar_relation_terminal_sequence_begin(
            &nullary_alias_sequence, &naf.leases[0],
            WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP) == 0);
    old_capacity = nullary_alias.capacity;
    nullary_alias.capacity = nullary_alias.merge_buf_cap;
    nullary_alias.merge_buf_cap = old_capacity;
    wl_columnar_relation_terminal_sequence_publish_grid_swap(
        &nullary_alias_sequence);
    assert(wl_columnar_relation_terminal_sequence_finish(
            &nullary_alias_sequence) == 0);
    assert(nullary_alias.storage_owner == &nullary_owner
        && nullary_alias.storage_owner_generation
        == nullary_owner.storage_generation
        && nullary_alias.storage_generation == 2
        && naf.leases[0].relation_generation == 2
        && naf.leases[0].owner_generation == 1);
    assert(col_rel_mutation_set_finish(&naf.set, true) == 0);
    assert_open(&nullary_owner);
    assert_open(&nullary_alias);

    fixture_t denied = {0};
    root_init(&relation, 23);
    relation.columns = old_columns;
    relation.capacity = 3;
    relation.merge_columns = new_columns;
    relation.merge_buf_cap = 4;
    relation.timestamps = malloc(sizeof(*relation.timestamps) * 3);
    assert(relation.timestamps != NULL);
    relation.timestamp_capacity = 3;
    relation.storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 2u;
    relation.storage_owner_generation = relation.storage_generation;
    wl_columnar_relation_mutation_role_t denied_role = {
        &relation, WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
    };
    assert(acquire(&denied, &denied_role, 1) == 0);
    wl_columnar_relation_terminal_sequence_t denied_sequence = {0};
    assert(wl_columnar_relation_terminal_sequence_begin(&denied_sequence,
        &denied.leases[0], WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP
        | WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE) == EOVERFLOW);
    assert(relation.storage_generation
        == WL_COLUMNAR_REL_GENERATION_INVALID - 2u
        && relation.timestamps != NULL);
    free(relation.timestamps);
    relation.timestamps = NULL;
    relation.timestamp_capacity = 0;
    assert(col_rel_mutation_set_finish(&denied.set, false) == 0);
    assert_open(&relation);
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
    assert(wl_columnar_source_access_reader_acquire(&a.source_access,
        &reader) == 0);
    before = a;
    assert(acquire(&f, &role, 1) == EBUSY);
    assert(memcmp(&a, &before, sizeof(a)) == 0);
    assert(!wl_columnar_source_access_gate_busy(&root.source_access));
    assert(!wl_columnar_source_access_gate_busy(&a.descriptor_access));
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
        f.descriptors, 4, f.owners, 8, f.leases, 4,
        f.initializations, 4) == EINVAL);
    assert(col_rel_mutation_set_acquire(&f.set, &role, SIZE_MAX,
        f.descriptors, SIZE_MAX, f.owners, SIZE_MAX, f.leases, SIZE_MAX,
        f.initializations, SIZE_MAX) == EOVERFLOW);
    assert(acquire(&f, NULL, 1) == EINVAL);
    role.relation = NULL;
    assert(acquire(&f, &role, 1) == EINVAL);
    role.relation = &root;
    assert(col_rel_mutation_set_acquire(&f.set, &role, 1,
        f.descriptors, 0, f.owners, 8, f.leases, 4,
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

/* A held-lease batch appender (main CI regression after #2037): one
 * mutation set per storage transition instead of one per row, with the
 * same rows, capacities and generations as per-row col_rel_append_row. */
static col_rel_t *
batch_relation(const char *name)
{
    static const wirelog_column_type_t types[2] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT
    };
    col_rel_t *r = col_rel_new_auto(name, 2);
    assert(r);
    assert(col_rel_set_column_types(r, types, 2) == 0);
    assert(col_rel_enable_timestamps(r) == 0);
    return r;
}

static void
batch_row_values(uint32_t i, int64_t row[2])
{
    double value = (i % 7u == 0u) ? -0.0 : (double)i;
    row[0] = (int64_t)i;
    memcpy(&row[1], &value, sizeof(value));
}

static void
append_batch_tests(void)
{
    enum { ROWS = 1000 };
    col_rel_t *ref = batch_relation("batch_ref");
    col_rel_t *out = batch_relation("batch_out");
    int64_t row[2];
    uint64_t ref_storage_before = ref->storage_generation;
    uint64_t ref_view_before = ref->view_generation;
    for (uint32_t i = 0; i < ROWS; i++) {
        batch_row_values(i, row);
        assert(col_rel_append_row(ref, row) == 0);
    }

    wl_columnar_relation_test_set_mutation_nonce(1000);
    uint64_t storage_before = out->storage_generation;
    uint64_t view_before = out->view_generation;
    col_rel_append_batch_t batch;
    memset(&batch, 0, sizeof(batch));
    assert(col_rel_append_batch_row(&batch, row) == EINVAL);
    assert(col_rel_append_batch_end(&batch, true) == 0);
    uint64_t nonce_before = wl_columnar_relation_test_mutation_nonce_peek();
    assert(col_rel_append_batch_begin(&batch, out) == 0);
    assert(col_rel_append_batch_begin(&batch, out) == EINVAL);
    for (uint32_t i = 0; i < ROWS; i++) {
        batch_row_values(i, row);
        assert(col_rel_append_batch_row(&batch, row) == 0);
        if (i == 10) {
            /* The held gates exclude readers until the batch ends. */
            wl_columnar_source_access_reader_t reader = {0};
            assert(col_rel_source_reader_acquire(out, &reader) == EBUSY);
        }
    }
    assert(col_rel_append_batch_end(&batch, true) == 0);
    uint64_t acquisitions
        = wl_columnar_relation_test_mutation_nonce_peek() - nonce_before;
    assert(col_rel_append_batch_end(&batch, true) == 0);
    assert(col_rel_append_batch_row(&batch, row) == EINVAL);

    /* Same storage shape and publications as the per-row reference. */
    assert(out->nrows == ref->nrows && out->capacity == ref->capacity
        && out->timestamp_capacity == ref->timestamp_capacity);
    for (uint32_t c = 0; c < 2; c++)
        assert(memcmp(out->columns[c], ref->columns[c],
            ROWS * sizeof(int64_t)) == 0);
    uint64_t transitions = out->storage_generation - storage_before;
    assert(transitions == ref->storage_generation - ref_storage_before);
    assert(transitions > 1);
    assert(out->view_generation - view_before
        == ref->view_generation - ref_view_before);
    /* One set per storage epoch: at least one, never more than the
     * transitions plus the first, and far below one per row. */
    assert(acquisitions >= 1 && acquisitions <= transitions + 1);

    wl_columnar_source_access_reader_t reader = {0};
    assert(col_rel_source_reader_acquire(out, &reader) == 0);
    assert(col_rel_source_reader_release(&reader) == 0);
    assert(col_rel_destroy_checked(out) == 0);
    assert(col_rel_destroy_checked(ref) == 0);
}

/* The filter operator appends its selected rows under the batch appender:
 * a right-side filter of 4096 timestamped rows acquires a handful of
 * mutation sets (one per storage epoch plus construction), not one per
 * selected row, and produces exactly the per-row result. */
static uint8_t batch_filter_simple[] = {
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_CONST_INT, 150, 0, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_CMP_GT
};
static uint8_t batch_filter_compiled[] = {
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_ARITH_ADD,
    WL_PLAN_EXPR_CONST_INT, 151, 0, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_CMP_GT
};

static void
filter_batch_tests(void)
{
    enum { ROWS = 4096, KEPT = ROWS - 151 };
    col_rel_t *src = col_rel_new_auto("filter_src", 2);
    delta_pool_t *pool = delta_pool_create(32, sizeof(col_rel_t), 4096);
    assert(src && pool && col_rel_enable_timestamps(src) == 0);
    for (uint32_t i = 0; i < ROWS; i++) {
        int64_t row[2] = { (int64_t)(i * 3u), (int64_t)i };
        assert(col_rel_append_row(src, row) == 0);
        src->timestamps[i] = (col_delta_timestamp_t){
            .iteration = i, .stratum = 7u, .worker = i % 5u,
            .multiplicity = (int64_t)(i % 3u) + 1
        };
    }
    wl_plan_expr_buffer_t exprs[2] = {
        { batch_filter_simple, sizeof(batch_filter_simple) },
        { batch_filter_compiled, sizeof(batch_filter_compiled) }
    };
    for (int e = 0; e < 2; e++) {
        wl_columnar_relation_test_set_mutation_nonce(1000);
        uint64_t before = wl_columnar_relation_test_mutation_nonce_peek();
        col_rel_t *out = wl_columnar_filter_apply_right_filter(&exprs[e],
                src, pool, NULL);
        uint64_t acquisitions
            = wl_columnar_relation_test_mutation_nonce_peek() - before;
        assert(out && out->nrows == KEPT && out->timestamps);
        /* Per-row acquisition would be at least KEPT; growing 64 -> 4096
         * takes six storage epochs. */
        assert(acquisitions >= 1 && acquisitions <= 16);
        for (uint32_t i = 0; i < KEPT; i++) {
            uint32_t s = i + 151u;
            assert(out->columns[0][i] == (int64_t)(s * 3u)
                && out->columns[1][i] == (int64_t)s
                && out->timestamps[i].iteration == s
                && out->timestamps[i].stratum == 7u
                && out->timestamps[i].worker == s % 5u
                && out->timestamps[i].multiplicity == (int64_t)(s % 3u) + 1);
        }
        /* The batch has ended: the result is readable and destroyable. */
        wl_columnar_source_access_reader_t reader = {0};
        assert(col_rel_source_reader_acquire(out, &reader) == 0);
        assert(col_rel_source_reader_release(&reader) == 0);
        assert(col_rel_destroy_checked(out) == 0);
    }
    delta_pool_destroy(pool);
    assert(col_rel_destroy_checked(src) == 0);
}

/* The filter operator itself, on its three fills: timestamped simple
 * predicate, untimestamped bulk copy, and the compiled slow path.  These are
 * the paths the evaluator runs; each must take a handful of mutation sets,
 * not one per selected row. */
static void
filter_op_batch_tests(void)
{
    enum { ROWS = 4096, KEPT = ROWS - 151 };
    struct {
        bool timestamped;
        uint8_t *expr;
        uint32_t size;
    } cases[] = {
        { true, batch_filter_simple, sizeof(batch_filter_simple) },
        { false, batch_filter_simple, sizeof(batch_filter_simple) },
        { true, batch_filter_compiled, sizeof(batch_filter_compiled) },
    };
    for (size_t k = 0; k < sizeof(cases) / sizeof(cases[0]); k++) {
        col_rel_t *src = col_rel_new_auto("filter_op_src", 2);
        assert(src);
        if (cases[k].timestamped)
            assert(col_rel_enable_timestamps(src) == 0);
        for (uint32_t i = 0; i < ROWS; i++) {
            int64_t row[2] = { (int64_t)(i * 3u), (int64_t)i };
            assert(col_rel_append_row(src, row) == 0);
            if (cases[k].timestamped)
                src->timestamps[i] = (col_delta_timestamp_t){
                    .iteration = i, .stratum = 9u, .worker = 1u,
                    .multiplicity = 1
                };
        }
        wl_plan_op_t op;
        memset(&op, 0, sizeof(op));
        op.filter_expr.data = cases[k].expr;
        op.filter_expr.size = cases[k].size;
        eval_stack_t stack;
        eval_stack_init(&stack);
        assert(eval_stack_push(&stack, src, true) == 0);
        wl_columnar_relation_test_set_mutation_nonce(1000);
        uint64_t before = wl_columnar_relation_test_mutation_nonce_peek();
        assert(wl_columnar_filter_op(&op, &stack, &(wl_col_session_t){ 0 })
            == 0);
        uint64_t acquisitions
            = wl_columnar_relation_test_mutation_nonce_peek() - before;
        eval_entry_t result = eval_stack_pop(&stack);
        col_rel_t *out = result.rel;
        assert(out && out->nrows == KEPT
            && (out->timestamps != NULL) == cases[k].timestamped);
        assert(acquisitions >= 1 && acquisitions <= 16);
        for (uint32_t i = 0; i < KEPT; i++) {
            uint32_t s = i + 151u;
            assert(out->columns[0][i] == (int64_t)(s * 3u)
                && out->columns[1][i] == (int64_t)s);
            if (cases[k].timestamped)
                assert(out->timestamps[i].iteration == s
                    && out->timestamps[i].stratum == 9u);
        }
        assert(col_rel_destroy_checked(out) == 0);
    }
}

/* A bare single-threaded session for driving operators directly. */
static wl_col_session_t *
join_session(void)
{
    wl_col_session_t *s = calloc(1, sizeof(*s));
    assert(s);
    s->frontier_ops = &col_frontier_epoch_ops;
    s->delta_pool = delta_pool_create(256, sizeof(col_rel_t), 1024 * 1024);
    assert(s->delta_pool);
    wl_mem_ledger_init(&s->mem_ledger, 0);
    return s;
}

static void
join_session_destroy(wl_col_session_t *s)
{
    wl_workqueue_destroy(s->wq);
    for (uint32_t i = 0; i < s->nrels; i++)
        col_rel_destroy(s->rels[i]);
    free(s->rels);
    for (uint32_t i = 0; i < s->arr_count; i++) {
        free(s->arr_entries[i].rel_name);
        free(s->arr_entries[i].key_cols);
        arr_free_contents(&s->arr_entries[i].arr);
    }
    free(s->arr_entries);
    col_session_free_diff_arrangements(s);
    col_session_free_delta_arrangements(s);
    col_session_free_filt_arrangements(s);
    col_mat_cache_clear(&s->mat_cache);
    /* Cached right filters own their relations, as in session teardown. */
    assert(s->filt_cache_active_pins == 0);
    for (uint32_t i = 0; i < s->filt_cache_count; i++) {
        free(s->filt_cache[i].rel_name);
        free(s->filt_cache[i].filter_data);
        if (s->filt_cache[i].metadata_reservation) {
            (void)wl_columnar_memory_rollback(
                s->filt_cache[i].metadata_reservation);
            free(s->filt_cache[i].metadata_reservation);
        }
        if (s->filt_cache[i].filtered)
            col_rel_destroy(s->filt_cache[i].filtered);
    }
    free(s->filt_cache);
    if (s->filt_cache_array_reservation) {
        (void)wl_columnar_memory_rollback(s->filt_cache_array_reservation);
        free(s->filt_cache_array_reservation);
    }
    session_rel_free_hash(s);
    delta_pool_destroy(s->delta_pool);
    free(s);
}

/* Batch failure paths (#2072 acceptance, covering #2073's appender). */
static wl_columnar_memory_governor_ref_t *
enforcing_governor(uint64_t budget)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = budget;
    resolution.usable_bytes = budget;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    wl_columnar_memory_governor_ref_t *ref
        = wl_columnar_memory_governor_ref_create(&resolution);
    assert(ref);
    return ref;
}

static uint64_t
governor_reserved(wl_columnar_memory_governor_ref_t *ref)
{
    return wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                   ref));
}

/* A growth the governor refuses mid-batch fails that row with the denial
 * evidence set, keeps the batch active and the rows before it, and still
 * ends and destroys cleanly. */
static void
batch_denial_tests(void)
{
    wl_columnar_memory_governor_ref_t *ref = enforcing_governor(1u << 24);
    col_rel_t *r = NULL;
    assert(wl_columnar_relation_new_auto_governed("batch_denied", 2,
        COL_REL_INIT_CAP, false, ref, &r) == 0);
    col_rel_append_batch_t batch = { 0 };
    int64_t row[2] = { 0, 0 };
    assert(col_rel_append_batch_begin(&batch, r) == 0);
    uint32_t first_growth = COL_REL_INIT_CAP * 2u;
    for (uint32_t i = 0; i < first_growth; i++) {
        row[0] = (int64_t)i;
        assert(col_rel_append_batch_row(&batch, row) == 0);
    }
    assert(r->nrows == first_growth && r->capacity == first_growth
        && batch.single.lease.storage_transitioned);
    /* Calibrated from what is held now, not from a byte count: the next
    * growth needs at least a second column set, far above the slack. */
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, governor_reserved(ref) + 64u,
        memory_order_release);
    uint64_t storage = r->storage_generation;
    r->memory_budget_denial_pending = false;
    assert(col_rel_append_batch_row(&batch, row) == ENOMEM);
    assert(r->memory_budget_denial_pending && batch.active
        && r->nrows == first_growth && r->capacity == first_growth
        && r->storage_generation == storage);
    assert(col_rel_append_batch_end(&batch, false) == 0);
    assert(col_rel_destroy_checked(r) == 0);
    assert(governor_reserved(ref) == 0);
    wl_columnar_memory_governor_ref_release(ref);
}

/* A lease renewal that cannot acquire leaves the batch inactive with the
 * relation unchanged and its gates released. */
static void
batch_renewal_failure_tests(void)
{
    col_rel_t *r = col_rel_new_auto("batch_renewal", 2);
    assert(r);
    col_rel_append_batch_t batch = { 0 };
    int64_t row[2] = { 0, 0 };
    wl_columnar_relation_test_set_mutation_nonce(1000);
    assert(col_rel_append_batch_begin(&batch, r) == 0);
    while (!(batch.single.lease.storage_transitioned
        && r->nrows == r->capacity)) {
        row[0]++;
        assert(col_rel_append_batch_row(&batch, row) == 0);
    }
    uint32_t nrows = r->nrows, capacity = r->capacity;
    uint64_t storage = r->storage_generation;
    wl_columnar_relation_test_set_mutation_nonce(UINT64_MAX);
    int rc = col_rel_append_batch_row(&batch, row);
    wl_columnar_relation_test_set_mutation_nonce(1000);
    assert(rc == EOVERFLOW && !batch.active);
    assert(r->nrows == nrows && r->capacity == capacity
        && r->storage_generation == storage);
    wl_columnar_source_access_reader_t reader = {0};
    assert(col_rel_source_reader_acquire(r, &reader) == 0);
    assert(col_rel_source_reader_release(&reader) == 0);
    assert(col_rel_append_batch_end(&batch, true) == 0);
    assert(col_rel_destroy_checked(r) == 0);
}

/* The filter operator's fills under a governor that refuses their second
 * growth: ENOSPC with the session's denial flag, the input disposed, and
 * every byte the attempt reserved returned. */
enum { DENIAL_SRC_ROWS = 512 };

static col_rel_t *
denial_source(bool timestamped)
{
    col_rel_t *src = col_rel_new_auto("denial_src", 2);
    assert(src);
    if (timestamped)
        assert(col_rel_enable_timestamps(src) == 0);
    for (uint32_t i = 0; i < DENIAL_SRC_ROWS; i++) {
        int64_t row[2] = { (int64_t)i, (int64_t)i };
        assert(col_rel_append_row(src, row) == 0);
    }
    return src;
}

/* col1 < keep (simple), or col1 + 1 < keep + 1 (compiled slow path). */
static void
denial_expr(bool compiled, int64_t keep, uint8_t *buf, uint32_t *size)
{
    uint32_t n = 0;
    buf[n++] = WL_PLAN_EXPR_VAR;
    buf[n++] = 4;
    buf[n++] = 0;
    memcpy(buf + n, "col1", 4);
    n += 4;
    if (compiled) {
        int64_t one = 1;
        buf[n++] = WL_PLAN_EXPR_CONST_INT;
        memcpy(buf + n, &one, 8);
        n += 8;
        buf[n++] = WL_PLAN_EXPR_ARITH_ADD;
        keep += 1;
    }
    buf[n++] = WL_PLAN_EXPR_CONST_INT;
    memcpy(buf + n, &keep, 8);
    n += 8;
    buf[n++] = WL_PLAN_EXPR_CMP_LT;
    *size = n;
}

typedef enum { DENY_FILTER_OP, DENY_RIGHT_FILTER } deny_entry_t;

/* Run one filter selecting @keep of DENIAL_SRC_ROWS under @budget. */
static int
denial_run(deny_entry_t entry, bool timestamped, bool compiled, int64_t keep,
    uint64_t budget, bool check_cleanup)
{
    wl_col_session_t *sess = join_session();
    wl_columnar_memory_governor_ref_t *ref = enforcing_governor(budget);
    sess->memory_governor = ref;
    col_rel_t *src = denial_source(timestamped);
    uint8_t buf[64];
    uint32_t size = 0;
    denial_expr(compiled, keep, buf, &size);
    uint64_t baseline = governor_reserved(ref);
    int rc;
    if (entry == DENY_FILTER_OP) {
        wl_plan_op_t op;
        memset(&op, 0, sizeof(op));
        op.filter_expr.data = buf;
        op.filter_expr.size = size;
        eval_stack_t stack;
        eval_stack_init(&stack);
        assert(eval_stack_push(&stack, src, true) == 0);
        rc = wl_columnar_filter_op(&op, &stack, sess);
        if (rc == 0) {
            eval_entry_t result = eval_stack_pop(&stack);
            assert(result.rel->nrows == (uint32_t)keep);
            assert(col_rel_destroy_checked(result.rel) == 0);
        } else if (check_cleanup) {
            assert(stack.top == 0);
        }
    } else {
        wl_plan_expr_buffer_t expr = { buf, size };
        col_rel_t *out = NULL;
        rc = wl_columnar_filter_apply_right_filter_governed_checked(&expr,
                src, sess->delta_pool, NULL, ref, sess, &out);
        if (rc == 0) {
            assert(out && out->nrows == (uint32_t)keep);
            assert(col_rel_destroy_checked(out) == 0);
        } else {
            assert(out == NULL);
        }
        assert(col_rel_destroy_checked(src) == 0);
    }
    if (rc != 0 && check_cleanup) {
        assert(rc == ENOSPC && sess->memory_budget_denied);
        assert(governor_reserved(ref) == baseline);
    }
    sess->memory_governor = NULL;
    join_session_destroy(sess);
    wl_columnar_memory_governor_ref_release(ref);
    return rc;
}

static void
filter_denial_tests(void)
{
    static const struct {
        deny_entry_t entry;
        bool timestamped, compiled;
    } cases[] = {
        { DENY_FILTER_OP, true, false },
        { DENY_FILTER_OP, true, true },
        { DENY_RIGHT_FILTER, true, false },
        { DENY_RIGHT_FILTER, false, true },
    };
    for (size_t k = 0; k < sizeof(cases) / sizeof(cases[0]); k++) {
        /* Smallest budget at which a 128-row selection (one growth past the
         * 64-row floor) succeeds; a 256-row selection needs a second, larger
         * growth and must be refused at that budget. */
        uint64_t lo = 0, hi = 1u << 24;
        assert(denial_run(cases[k].entry, cases[k].timestamped,
            cases[k].compiled, 128, hi, false) == 0);
        while (hi - lo > 1) {
            uint64_t mid = lo + (hi - lo) / 2u;
            if (denial_run(cases[k].entry, cases[k].timestamped,
                cases[k].compiled, 128, mid, false) == 0)
                hi = mid;
            else
                lo = mid;
        }
        assert(denial_run(cases[k].entry, cases[k].timestamped,
            cases[k].compiled, 256, hi, true) == ENOSPC);
    }
}

int
main(void)
{
    lease_tests();
    duplicate_role_publication_tests();
    terminal_sequence_tests();
    contention_tests();
    rollback_tests();
    invalid_tests();
    metadata_final_access_test();
    append_batch_tests();
    filter_batch_tests();
    filter_op_batch_tests();
    batch_denial_tests();
    batch_renewal_failure_tests();
    filter_denial_tests();
    {
        col_rel_t root;
        fixture_t f = {0};
        root_init(&root, 3);
        wl_columnar_relation_mutation_role_t role = { &root,
                                                      WL_COLUMNAR_RELATION_PAYLOAD_MUTATION };
        wl_columnar_relation_test_set_mutation_nonce(UINT64_MAX);
        assert(acquire(&f, &role, 1) == EOVERFLOW);
        assert(f.set.identity == 0 && f.set.acquisition_nonce == 0
            && atomic_load_explicit(&root.descriptor_access.state,
            memory_order_relaxed) == 0);
    }
    puts("relation mutation set tests passed");
    return 0;
}
