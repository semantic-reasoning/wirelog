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

/* Each writer token a lease names must stay live for lease validation and
 * finish, including the canonical self-owned case where the owner and self
 * slots are the same token (#2036). */
static void
expect_token_required(fixture_t *f, wl_columnar_source_access_writer_t *token,
    col_rel_t *relation)
{
    assert(col_rel_mutation_lease_validate(&f->leases[0], relation) == 0);
    token->thread_valid = false;
    assert(col_rel_mutation_lease_validate(&f->leases[0], relation) == EINVAL);
    assert(col_rel_mutation_set_lease(&f->set, 0) == NULL);
    assert(col_rel_mutation_set_finish(&f->set, true) == EINVAL);
    token->thread_valid = true;
    uintptr_t identity = token->identity;
    token->identity = 0;
    assert(col_rel_mutation_lease_validate(&f->leases[0], relation) == EINVAL);
    assert(col_rel_mutation_set_finish(&f->set, true) == EINVAL);
    token->identity = identity;
    assert(col_rel_mutation_lease_validate(&f->leases[0], relation) == 0);
}

static void
writer_token_liveness_tests(void)
{
    col_rel_t root, alias;
    fixture_t f = {0};
    root_init(&root, 31);
    wl_columnar_relation_mutation_role_t role = { &root,
                                                  WL_COLUMNAR_RELATION_PAYLOAD_MUTATION };
    assert(acquire(&f, &role, 1) == 0);
    assert(f.set.owner_count == 1
        && f.leases[0].owner_slot == f.leases[0].self_owner_slot);
    expect_token_required(&f,
        &f.descriptors[f.leases[0].descriptor_slot].writer,
        &root);
    expect_token_required(&f, &f.owners[f.leases[0].owner_slot].writer, &root);
    assert(col_rel_mutation_set_finish(&f.set, true) == 0);
    assert_open(&root);

    memset(&f, 0, sizeof(f));
    alias_init(&alias, &root, 32);
    role.relation = &alias;
    assert(acquire(&f, &role, 1) == 0);
    assert(f.set.owner_count == 2
        && f.leases[0].owner_slot != f.leases[0].self_owner_slot
        && f.owners[f.leases[0].owner_slot].owner == &root);
    expect_token_required(&f,
        &f.descriptors[f.leases[0].descriptor_slot].writer,
        &alias);
    expect_token_required(&f, &f.owners[f.leases[0].owner_slot].writer, &alias);
    expect_token_required(&f, &f.owners[f.leases[0].self_owner_slot].writer,
        &alias);
    assert(col_rel_mutation_set_finish(&f.set, false) == 0);
    assert_open(&root);
    assert_open(&alias);
    assert(col_rel_storage_alias_borrow_release(&root) == 0);
}

/* Only the internal single-relation path may skip the caller-storage overlap
 * scan; the public entry point still rejects aliased bookkeeping (#2036). */
static void
overlapping_storage_tests(void)
{
    col_rel_t root;
    fixture_t f = {0};
    root_init(&root, 33);
    wl_columnar_relation_mutation_role_t role = { &root,
                                                  WL_COLUMNAR_RELATION_PAYLOAD_MUTATION };
    union {
        wl_columnar_relation_mutation_lease_t lease;
        wl_columnar_relation_mutation_initialization_t initialization;
    } shared;
    memset(&shared, 0, sizeof(shared));
    assert(col_rel_mutation_set_acquire(&f.set, &role, 1,
        f.descriptors, 4, f.owners, 8, &shared.lease, 1,
        &shared.initialization, 1) == EINVAL);
    assert(col_rel_mutation_set_acquire(&f.set, &role, 1,
        (wl_columnar_relation_mutation_descriptor_t *)f.owners, 4, f.owners, 8,
        f.leases, 4, f.initializations, 4) == EINVAL);
    assert(f.set.identity == 0);
    assert_open(&root);
    assert(acquire(&f, &role, 1) == 0);
    assert(col_rel_mutation_set_finish(&f.set, true) == 0);
    assert_open(&root);
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
    uint64_t validations_before
        = wl_columnar_relation_test_lease_validations_peek();
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
    uint64_t validations = wl_columnar_relation_test_lease_validations_peek()
        - validations_before;
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
    /* Issue #2072: the held lease is fully validated when an epoch starts
     * and a bounded number of times around each storage transition (the
     * renewal, the transition's own publication steps, the first row after
     * it), not on every row. */
    assert(validations <= 4u * transitions + 2u && validations < ROWS / 10u);

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

/* The public batch append copies whole rows at once (#2036): every cell must
 * land in its column at the existing row offset, float -0.0 must be stored as
 * +0.0, stale timestamp slots must be cleared, and an invalid float anywhere
 * in the batch must leave the relation unchanged. */
static void
public_batch_copy_tests(void)
{
    col_rel_t *r = batch_relation("public_batch_copy");
    int64_t first[2] = { 5, 0 };
    assert(col_rel_append_rows_atomic(r, first, 1, 2, NULL) == 0);
    assert(col_rel_reserve_capacity_admitted(r, 8, NULL) == 0);
    for (uint32_t i = 1; i < r->timestamp_capacity; i++)
        memset(&r->timestamps[i], 0xa5, sizeof(r->timestamps[i]));
    const int64_t negative_zero = (int64_t)UINT64_C(0x8000000000000000);
    const int64_t one = (int64_t)UINT64_C(0x3ff0000000000000);
    const int64_t minus_two = (int64_t)UINT64_C(0xc000000000000000);
    int64_t rows[3][2] = { { 7, negative_zero }, { -8, one },
                           { 9, minus_two } };
    bool denied = true;
    assert(col_rel_append_rows_atomic(r, &rows[0][0], 3, 2, &denied) == 0
        && !denied && r->nrows == 4);
    const int64_t expect[4][2] = { { 5, 0 }, { 7, 0 }, { -8, one },
                                   { 9, minus_two } };
    col_delta_timestamp_t zero;
    memset(&zero, 0, sizeof(zero));
    for (uint32_t i = 0; i < 4; i++) {
        assert(r->columns[0][i] == expect[i][0]
            && r->columns[1][i] == expect[i][1]);
        assert(memcmp(&r->timestamps[i], &zero, sizeof(zero)) == 0);
    }
    col_delta_timestamp_t stale;
    memset(&stale, 0xa5, sizeof(stale));
    assert(memcmp(&r->timestamps[4], &stale, sizeof(stale)) == 0);

    int64_t invalid[2][2] = { { 10, one },
                              { 11, (int64_t)UINT64_C(0x7ff8000000000000) } };
    uint64_t view = r->view_generation;
    assert(col_rel_append_rows_atomic(r, &invalid[0][0], 2, 2, &denied)
        == EINVAL && !denied);
    assert(r->nrows == 4 && r->view_generation == view
        && memcmp(&r->timestamps[4], &stale, sizeof(stale)) == 0);
    assert(col_rel_destroy_checked(r) == 0);
}

/* A wide all-FLOAT batch is validated as a whole before any mutation: the
 * copy checks each cell only as it stores it, so a non-finite value in the
 * very last cell must still leave the relation unchanged.  -0.0 is still
 * normalized in every column, including when the input aliases the
 * relation's own column storage and is staged in scratch first. */
static void
public_float_batch_tests(void)
{
    enum { COLS = 32, ROWS = 256 };
    wirelog_column_type_t types[COLS];
    for (uint32_t c = 0; c < COLS; c++)
        types[c] = WIRELOG_TYPE_FLOAT;
    col_rel_t *r = col_rel_new_auto("public_float_batch", COLS);
    assert(r && col_rel_set_column_types(r, types, COLS) == 0);
    static int64_t rows[ROWS][COLS];
    for (uint32_t i = 0; i < ROWS; i++) {
        for (uint32_t c = 0; c < COLS; c++) {
            double value = (i + c) % 5u == 0u ? -0.0 : (double)i + c * 0.5;
            memcpy(&rows[i][c], &value, sizeof(value));
        }
    }
    bool denied = true;
    assert(col_rel_append_rows_atomic(r, &rows[0][0], ROWS, COLS, &denied)
        == 0 && !denied && r->nrows == ROWS);
    for (uint32_t i = 0; i < ROWS; i++) {
        for (uint32_t c = 0; c < COLS; c++) {
            int64_t expect = (i + c) % 5u == 0u ? 0 : rows[i][c];
            assert(r->columns[c][i] == expect);
        }
    }
    const int64_t bad[2] = { (int64_t)UINT64_C(0x7ff8000000000000),
                             (int64_t)UINT64_C(0xfff0000000000000) };
    for (int b = 0; b < 2; b++) {
        int64_t saved = rows[ROWS - 1][COLS - 1];
        rows[ROWS - 1][COLS - 1] = bad[b];
        uint64_t view = r->view_generation;
        assert(col_rel_append_rows_atomic(r, &rows[0][0], ROWS, COLS, &denied)
            == EINVAL && !denied);
        assert(r->nrows == ROWS && r->view_generation == view);
        rows[ROWS - 1][COLS - 1] = saved;
    }
    assert(col_rel_destroy_checked(r) == 0);

    /* The same batch read from column 0 of the destination itself. */
    enum { CELLS = ROWS * COLS };
    r = col_rel_new_auto("public_float_batch_alias", COLS);
    assert(r && col_rel_set_column_types(r, types, COLS) == 0);
    assert(col_rel_reserve_capacity_admitted(r, CELLS + ROWS, NULL) == 0
        && r->capacity >= CELLS + ROWS);
    int64_t *alias = r->columns[0];
    static int64_t snapshot[CELLS];
    memcpy(alias, &rows[0][0], sizeof(snapshot));
    memcpy(snapshot, alias, sizeof(snapshot));
    assert(col_rel_append_rows_atomic(r, alias, ROWS, COLS, &denied) == 0
        && !denied && r->nrows == ROWS);
    for (uint32_t i = 0; i < ROWS; i++) {
        for (uint32_t c = 0; c < COLS; c++) {
            int64_t in = snapshot[(size_t)i * COLS + c];
            assert(r->columns[c][i]
                == ((i + c) % 5u == 0u ? 0 : in));
        }
    }
    for (int b = 0; b < 2; b++) {
        alias = r->columns[0];
        alias[CELLS - 1] = bad[b];
        memcpy(snapshot, alias, sizeof(snapshot));
        uint64_t view = r->view_generation;
        assert(col_rel_append_rows_atomic(r, alias, ROWS, COLS, &denied)
            == EINVAL && !denied);
        assert(r->nrows == ROWS && r->view_generation == view
            && memcmp(r->columns[0], snapshot, sizeof(snapshot)) == 0);
    }
    assert(col_rel_destroy_checked(r) == 0);
}

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

/* #2072: the serial join probe loops append each joined pair under one
 * batch lease instead of a col_rel_set lease per cell.  Each case runs one
 * serial path on a 4096-row output and checks the mutation sets it took,
 * the rows it produced and which path ran. */
static col_rel_t *
join_rel(const char *name, uint32_t ncols)
{
    static const char *const names[] = { "k", "v" };
    col_rel_t *r = col_rel_new_auto(name, ncols);
    assert(r && col_rel_set_schema(r, ncols, names) == 0);
    return r;
}

static uint8_t join_pass_all_filter[] = {
    WL_PLAN_EXPR_VAR, 1, 0, 'k',
    WL_PLAN_EXPR_CONST_INT, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    WL_PLAN_EXPR_CMP_GT
};

typedef enum {
    JOIN_ARRANGEMENT, JOIN_UNARY, JOIN_DIFF_PERSISTENT, JOIN_DIFF_EPHEMERAL
} join_case_t;

static void
join_batch_case(join_case_t which)
{
    enum { KEYS = 64, FANOUT = 64, ROWS = KEYS * FANOUT };
    static const char *const keys[] = { "k" };
    bool unary = which == JOIN_UNARY;
    wl_col_session_t *sess = join_session();
    /* Keyed cases: left(k, v) has one row per key, right(k, v) FANOUT rows
     * per key.  Unary: left has FANOUT rows per key, right(k) one per key. */
    col_rel_t *left = join_rel("left", 2);
    col_rel_t *right = join_rel("right", unary ? 1u : 2u);
    for (uint32_t i = 0; i < (unary ? ROWS : KEYS); i++) {
        int64_t row[2] = { (int64_t)(i % KEYS), (int64_t)i };
        assert(col_rel_append_row(left, row) == 0);
    }
    for (uint32_t i = 0; i < (unary ? KEYS : ROWS); i++) {
        int64_t row[2] = { (int64_t)(i % KEYS), 1000 + (int64_t)i };
        assert(col_rel_append_row(right, row) == 0);
    }
    assert(session_add_rel(sess, right) == 0);

    wl_plan_op_t op;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    op.left_keys = keys;
    op.right_keys = keys;
    op.delta_mode = WL_DELTA_FORCE_FULL;
    if (which == JOIN_DIFF_EPHEMERAL) {
        op.right_filter_expr.data = join_pass_all_filter;
        op.right_filter_expr.size = sizeof(join_pass_all_filter);
    }

    eval_stack_t stack;
    eval_stack_init(&stack);
    assert(eval_stack_push(&stack, left, true) == 0);
    wl_columnar_relation_test_set_mutation_nonce(1000);
    uint64_t before = wl_columnar_relation_test_mutation_nonce_peek();
    int rc = (which == JOIN_DIFF_PERSISTENT || which == JOIN_DIFF_EPHEMERAL)
        ? wl_columnar_join_diff_op(&op, &stack, sess)
        : wl_columnar_join_op(&op, &stack, sess);
    uint64_t acquisitions
        = wl_columnar_relation_test_mutation_nonce_peek() - before;
    assert(rc == 0 && stack.top == 1);
    eval_entry_t result = eval_stack_pop(&stack);
    col_rel_t *out = result.rel;

    /* The path this case exists for is the one that ran. */
    if (which == JOIN_ARRANGEMENT)
        assert(sess->arr_count > 0);
    /* The unary hash path builds its own table; the same keyed shape with
     * two columns on the right registers an arrangement instead. */
    if (which == JOIN_UNARY)
        assert(sess->arr_count == 0);
    if (which == JOIN_DIFF_PERSISTENT)
        assert(sess->diff_arr_count > 0);
    if (which == JOIN_DIFF_EPHEMERAL)
        assert(sess->diff_arr_count == 0);

    /* Every key pairs every right row with its left row exactly once. */
    uint32_t ncols = unary ? 3u : 4u;
    assert(out && out->nrows == ROWS && out->ncols == ncols);
    static bool seen[ROWS];
    memset(seen, 0, sizeof(seen));
    for (uint32_t r = 0; r < out->nrows; r++) {
        int64_t k = out->columns[0][r];
        int64_t lv = out->columns[1][r];
        int64_t rk = out->columns[2][r];
        assert(k >= 0 && k < KEYS && rk == k);
        uint32_t slot;
        if (unary) {
            assert(lv >= 0 && lv < ROWS && lv % KEYS == k);
            slot = (uint32_t)lv;
        } else {
            int64_t rv = out->columns[3][r] - 1000;
            assert(lv == k && rv >= 0 && rv < ROWS && rv % KEYS == k);
            slot = (uint32_t)rv;
        }
        assert(!seen[slot]);
        seen[slot] = true;
    }
    /* A lease per cell needs at least ROWS * ncols sets, one per row at
     * least ROWS; growing 64 -> 4096 takes seven storage epochs. */
    assert(acquisitions >= 1 && acquisitions <= 16);

    if (result.owned)
        assert(col_rel_destroy_checked(out) == 0);
    join_session_destroy(sess);
}

static void
join_batch_tests(void)
{
    join_batch_case(JOIN_ARRANGEMENT);
    join_batch_case(JOIN_UNARY);
    join_batch_case(JOIN_DIFF_PERSISTENT);
    join_batch_case(JOIN_DIFF_EPHEMERAL);
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

typedef struct {
    col_rel_append_batch_t *batch;
    const int64_t *row;
    int rc;
} batch_thread_arg_t;

static void *
batch_row_other_thread(void *arg)
{
    batch_thread_arg_t *a = arg;
    a->rc = col_rel_append_batch_row(a->batch, a->row);
    return NULL;
}

/* Issue #2072: rows after the first in an epoch take a cheaper lease check.
 * It must still refuse a row from another thread, through a copy of the
 * batch, after the set was finished behind the batch, and after the
 * relation's identity or a storage generation changed behind it. */
static void
batch_lease_check_tests(void)
{
    col_rel_t *r = col_rel_new_auto("batch_check", 2);
    assert(r);
    col_rel_append_batch_t batch = { 0 };
    int64_t row[2] = { 0, 0 };
    assert(col_rel_append_batch_begin(&batch, r) == 0);
    for (int i = 0; i < 8; i++) {
        row[0]++;
        assert(col_rel_append_batch_row(&batch, row) == 0);
    }
    /* An append through the held lease validates it once, not twice. */
    uint64_t validations_before
        = wl_columnar_relation_test_lease_validations_peek();
    row[0]++;
    assert(col_rel_append_row_with_lease(r, row, &batch.single.lease) == 0);
    assert(wl_columnar_relation_test_lease_validations_peek()
        - validations_before == 1u);
    uint32_t nrows = r->nrows;

    batch_thread_arg_t arg = { &batch, row, 0 };
    wl_thread_t thread;
    assert(wl_thread_create(&thread, batch_row_other_thread, &arg) == 0);
    assert(wl_thread_join(&thread) == 0);
    assert(arg.rc == EINVAL && batch.active && r->nrows == nrows);

    col_rel_append_batch_t copy;
    memcpy(&copy, &batch, sizeof(copy));
    assert(col_rel_append_batch_row(&copy, row) == EINVAL);
    assert(r->nrows == nrows);

    r->relation_identity++;
    assert(col_rel_append_batch_row(&batch, row) == EINVAL);
    r->relation_identity--;
    r->storage_generation++;
    assert(col_rel_append_batch_row(&batch, row) == EINVAL);
    r->storage_generation--;
    r->storage_owner_generation++;
    assert(col_rel_append_batch_row(&batch, row) == EINVAL);
    r->storage_owner_generation--;
    assert(r->nrows == nrows && batch.active);
    row[0]++;
    assert(col_rel_append_batch_row(&batch, row) == 0);
    nrows = r->nrows;

    assert(col_rel_mutation_set_finish(&batch.single.set, true) == 0);
    assert(col_rel_append_batch_row(&batch, row) == EINVAL);
    assert(r->nrows == nrows);
    assert(col_rel_append_batch_end(&batch, true) == EINVAL);
    assert(!batch.active);
    assert_open(r);
    assert(col_rel_destroy_checked(r) == 0);
}

/* A batch on a shared view is checked in full until its first append copies
 * the view, then cheaply; the rows equal a per-row reference and the source
 * is left untouched. */
static void
batch_shared_view_tests(void)
{
    enum { BASE = 100, ROWS = 300 };
    col_rel_t *src = col_rel_new_auto("batch_view_src", 2);
    col_rel_t *ref = col_rel_new_auto("batch_view_ref", 2);
    col_rel_t *view = col_rel_new_auto("batch_view", 2);
    assert(src && ref && view);
    int64_t row[2];
    for (uint32_t i = 0; i < BASE; i++) {
        row[0] = i;
        row[1] = -(int64_t)i;
        assert(col_rel_append_row(src, row) == 0);
        assert(col_rel_append_row(ref, row) == 0);
    }
    for (uint32_t i = BASE; i < ROWS; i++) {
        row[0] = i;
        row[1] = -(int64_t)i;
        assert(col_rel_append_row(ref, row) == 0);
    }
    assert(col_rel_install_shared_view(view, src) == 0);
    assert(view->col_shared);
    col_rel_append_batch_t batch = { 0 };
    uint64_t validations_before
        = wl_columnar_relation_test_lease_validations_peek();
    assert(col_rel_append_batch_begin(&batch, view) == 0);
    for (uint32_t i = BASE; i < ROWS; i++) {
        row[0] = i;
        row[1] = -(int64_t)i;
        assert(col_rel_append_batch_row(&batch, row) == 0);
    }
    assert(col_rel_append_batch_end(&batch, true) == 0);
    assert(wl_columnar_relation_test_lease_validations_peek()
        - validations_before < (ROWS - BASE) / 10u);
    assert(!view->col_shared && view->nrows == ROWS && src->nrows == BASE);
    for (uint32_t c = 0; c < 2; c++) {
        assert(memcmp(view->columns[c], ref->columns[c],
            ROWS * sizeof(int64_t)) == 0);
        assert(memcmp(src->columns[c], ref->columns[c],
            BASE * sizeof(int64_t)) == 0);
    }
    assert(col_rel_destroy_checked(view) == 0);
    assert(col_rel_destroy_checked(ref) == 0);
    assert(col_rel_destroy_checked(src) == 0);
}

/* A first row on a shared view that fails before copying the view leaves
 * the batch on its alias lease; the retried row must still take the full
 * check, and succeeds. */
static void
batch_shared_view_retry_tests(void)
{
    col_rel_t *src = col_rel_new_auto("batch_retry_src", 2);
    col_rel_t *view = col_rel_new_auto("batch_retry_view", 2);
    assert(src && view);
    int64_t row[2] = { 1, 2 };
    assert(col_rel_append_row(src, row) == 0);
    assert(col_rel_install_shared_view(view, src) == 0);
    col_rel_append_batch_t batch = { 0 };
    assert(col_rel_append_batch_begin(&batch, view) == 0);
    uint64_t view_generation = view->view_generation;
    view->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    row[0] = 3;
    assert(col_rel_append_batch_row(&batch, row) == EOVERFLOW);
    view->view_generation = view_generation;
    assert(view->col_shared && view->nrows == 1 && batch.active);
    assert(col_rel_append_batch_row(&batch, row) == 0);
    assert(col_rel_append_batch_end(&batch, true) == 0);
    assert(!view->col_shared && view->nrows == 2 && src->nrows == 1);
    assert(view->columns[0][0] == 1 && view->columns[1][0] == 2
        && view->columns[0][1] == 3 && view->columns[1][1] == 2);
    assert(col_rel_destroy_checked(view) == 0);
    assert(col_rel_destroy_checked(src) == 0);
}

/* A lease renewal attempted from another thread cannot finish the held set,
 * so the batch stays active with its gates held, and the owning thread can
 * still end it. */
static void
batch_renewal_wrong_thread_tests(void)
{
    col_rel_t *r = col_rel_new_auto("batch_renewal_thread", 2);
    assert(r);
    col_rel_append_batch_t batch = { 0 };
    int64_t row[2] = { 0, 0 };
    assert(col_rel_append_batch_begin(&batch, r) == 0);
    while (!(batch.single.lease.storage_transitioned
        && r->nrows == r->capacity)) {
        row[0]++;
        assert(col_rel_append_batch_row(&batch, row) == 0);
    }
    uint32_t nrows = r->nrows;
    batch_thread_arg_t arg = { &batch, row, 0 };
    wl_thread_t thread;
    assert(wl_thread_create(&thread, batch_row_other_thread, &arg) == 0);
    assert(wl_thread_join(&thread) == 0);
    assert(arg.rc == EINVAL && batch.active && r->nrows == nrows);
    wl_columnar_source_access_reader_t reader = {0};
    assert(col_rel_source_reader_acquire(r, &reader) == EBUSY);
    /* The renewal's new lease is fully validated once before its row; the
     * row's growth publishes without validating it again. */
    uint64_t validations_before
        = wl_columnar_relation_test_lease_validations_peek();
    row[0]++;
    assert(col_rel_append_batch_row(&batch, row) == 0);
    assert(wl_columnar_relation_test_lease_validations_peek()
        - validations_before == 1u);
    assert(col_rel_append_batch_end(&batch, true) == 0);
    assert_open(r);
    assert(col_rel_destroy_checked(r) == 0);
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
        { DENY_FILTER_OP, false, false },
        { DENY_FILTER_OP, false, true },
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

/* A join whose second output growth the governor refuses ends its batch
 * before destroying the output: ENOSPC with the session's denial flag and
 * every reserved byte returned.  A cross join on a session without workers
 * runs the serial merge loop; eight right rows per left row make 16 left
 * rows one growth past the 64-row floor and 32 left rows two. */
static int
join_denial_run(uint32_t left_rows, uint64_t budget, bool check_cleanup)
{
    wl_col_session_t *sess = join_session();
    wl_columnar_memory_governor_ref_t *ref = enforcing_governor(budget);
    sess->memory_governor = ref;
    col_rel_t *left = join_rel("left", 2);
    col_rel_t *right = join_rel("right", 2);
    for (uint32_t i = 0; i < left_rows; i++) {
        int64_t row[2] = { (int64_t)i, (int64_t)i };
        assert(col_rel_append_row(left, row) == 0);
    }
    for (uint32_t i = 0; i < 8u; i++) {
        int64_t row[2] = { (int64_t)i, 1000 + (int64_t)i };
        assert(col_rel_append_row(right, row) == 0);
    }
    assert(session_add_rel(sess, right) == 0);
    wl_plan_op_t op;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 0;
    op.delta_mode = WL_DELTA_FORCE_FULL;
    eval_stack_t stack;
    eval_stack_init(&stack);
    assert(eval_stack_push(&stack, left, true) == 0);
    uint64_t baseline = governor_reserved(ref);
    int rc = wl_columnar_join_op(&op, &stack, sess);
    if (rc == 0) {
        eval_entry_t result = eval_stack_pop(&stack);
        assert(result.rel->nrows == left_rows * 8u);
        if (result.owned)
            assert(col_rel_destroy_checked(result.rel) == 0);
    } else if (check_cleanup) {
        assert(rc == ENOSPC && sess->memory_budget_denied
            && stack.top == 0);
        assert(governor_reserved(ref) == baseline);
    }
    sess->memory_governor = NULL;
    join_session_destroy(sess);
    wl_columnar_memory_governor_ref_release(ref);
    return rc;
}

static void
join_denial_tests(void)
{
    uint64_t lo = 0, hi = 1u << 24;
    assert(join_denial_run(16, hi, false) == 0);
    while (hi - lo > 1) {
        uint64_t mid = lo + (hi - lo) / 2u;
        if (join_denial_run(16, mid, false) == 0)
            hi = mid;
        else
            lo = mid;
    }
    assert(join_denial_run(32, hi, true) == ENOSPC);
}

/* #2072: antijoin and the serial semijoin probe append their output under
 * one batch lease.  left(k, v) has 4096 rows over 128 keys and right(k)
 * holds the even keys, so each operator keeps half the left rows. */
enum { SEMI_KEYS = 128, SEMI_ROWS = 4096 };

static col_rel_t *
semi_left(void)
{
    col_rel_t *left = join_rel("left", 2);
    for (uint32_t i = 0; i < SEMI_ROWS; i++) {
        int64_t row[2] = { (int64_t)(i % SEMI_KEYS), (int64_t)i };
        assert(col_rel_append_row(left, row) == 0);
    }
    return left;
}

static void
semi_right(wl_col_session_t *sess, uint32_t right_rows)
{
    col_rel_t *right = join_rel("right", 1);
    for (uint32_t i = 0; i < right_rows; i++) {
        int64_t row[1] = { (int64_t)(2u * i) };
        assert(col_rel_append_row(right, row) == 0);
    }
    assert(session_add_rel(sess, right) == 0);
}

static void
semi_op(wl_plan_op_t *op, bool anti, const uint32_t *project,
    uint32_t project_count)
{
    static const char *const keys[] = { "k" };
    memset(op, 0, sizeof(*op));
    op->op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN;
    op->right_relation = "right";
    op->key_count = 1;
    op->left_keys = keys;
    op->right_keys = keys;
    op->delta_mode = WL_DELTA_FORCE_FULL;
    op->project_indices = (uint32_t *)project;
    op->project_count = project_count;
}

static int
semi_run(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    return op->op == WL_PLAN_OP_ANTIJOIN
           ? wl_columnar_antijoin_op(op, stack, sess)
           : wl_columnar_semijoin_op(op, stack, sess);
}

static void
semi_batch_case(bool anti, bool project)
{
    static const uint32_t swap[] = { 1, 0 };
    wl_col_session_t *sess = join_session();
    semi_right(sess, SEMI_KEYS / 2u);
    wl_plan_op_t op;
    semi_op(&op, anti, project ? swap : NULL, project ? 2u : 0u);
    eval_stack_t stack;
    eval_stack_init(&stack);
    assert(eval_stack_push(&stack, semi_left(), true) == 0);
    wl_columnar_relation_test_set_mutation_nonce(1000);
    uint64_t before = wl_columnar_relation_test_mutation_nonce_peek();
    int rc = semi_run(&op, &stack, sess);
    uint64_t acquisitions
        = wl_columnar_relation_test_mutation_nonce_peek() - before;
    assert(rc == 0 && stack.top == 1);
    eval_entry_t result = eval_stack_pop(&stack);
    col_rel_t *out = result.rel;
    /* Rows keep left order: semijoin keeps the even keys, antijoin the odd
     * ones, and a projection swaps the columns. */
    assert(out && out->nrows == SEMI_ROWS / 2u && out->ncols == 2);
    uint32_t r = 0;
    for (uint32_t i = 0; i < SEMI_ROWS; i++) {
        int64_t k = (int64_t)(i % SEMI_KEYS);
        if ((k % 2 == 0) == anti)
            continue;
        assert(out->columns[project ? 1 : 0][r] == k);
        assert(out->columns[project ? 0 : 1][r] == (int64_t)i);
        r++;
    }
    assert(r == out->nrows);
    /* A lease per row needs at least 2048 sets; growing 64 -> 2048 takes six
     * storage epochs. */
    assert(acquisitions >= 1 && acquisitions <= 16);
    if (result.owned)
        assert(col_rel_destroy_checked(out) == 0);
    join_session_destroy(sess);
}

static void
semi_batch_tests(void)
{
    semi_batch_case(true, false);
    semi_batch_case(false, false);
    semi_batch_case(false, true);
}

/* Antijoin and semijoin under a governor that refuses their output's second
 * growth: ENOSPC with the denial flag, the input disposed, and every byte
 * the attempt reserved returned, which needs the output destroyed and so
 * the batch ended first.  The first @kept left rows are the ones the
 * operator keeps. */
static int
semi_denial_run(bool anti, uint32_t kept, uint64_t budget,
    bool check_cleanup)
{
    wl_col_session_t *sess = join_session();
    wl_columnar_memory_governor_ref_t *ref = enforcing_governor(budget);
    sess->memory_governor = ref;
    /* right(k) holds 0..7; left rows with k < 8 match. */
    semi_right(sess, 0);
    col_rel_t *right = sess->rels[sess->nrels - 1];
    for (uint32_t i = 0; i < 8u; i++) {
        int64_t row[1] = { (int64_t)i };
        assert(col_rel_append_row(right, row) == 0);
    }
    col_rel_t *left = join_rel("left", 2);
    for (uint32_t i = 0; i < 512u; i++) {
        /* Kept rows first, then rows the operator drops. */
        bool keep = i < kept;
        int64_t k = (keep != anti) ? (int64_t)(i % 8u) : 100 + (int64_t)i;
        int64_t row[2] = { k, (int64_t)i };
        assert(col_rel_append_row(left, row) == 0);
    }
    wl_plan_op_t op;
    semi_op(&op, anti, NULL, 0);
    eval_stack_t stack;
    eval_stack_init(&stack);
    assert(eval_stack_push(&stack, left, true) == 0);
    uint64_t baseline = governor_reserved(ref);
    int rc = semi_run(&op, &stack, sess);
    if (rc == 0) {
        eval_entry_t result = eval_stack_pop(&stack);
        assert(result.rel->nrows == kept);
        if (result.owned)
            assert(col_rel_destroy_checked(result.rel) == 0);
    } else if (check_cleanup) {
        assert(rc == ENOSPC && sess->memory_budget_denied
            && stack.top == 0);
        assert(governor_reserved(ref) == baseline);
    }
    sess->memory_governor = NULL;
    join_session_destroy(sess);
    wl_columnar_memory_governor_ref_release(ref);
    return rc;
}

static void
semi_denial_tests(void)
{
    for (int anti = 0; anti < 2; anti++) {
        /* Smallest budget at which 128 kept rows (one growth past the 64-row
         * floor) succeed; 256 kept rows need a second growth. */
        uint64_t lo = 0, hi = 1u << 24;
        assert(semi_denial_run(anti, 128, hi, false) == 0);
        while (hi - lo > 1) {
            uint64_t mid = lo + (hi - lo) / 2u;
            if (semi_denial_run(anti, 128, mid, false) == 0)
                hi = mid;
            else
                lo = mid;
        }
        assert(semi_denial_run(anti, 256, hi, true) == ENOSPC);
    }
}

/* The map operator appends its output rows under the batch appender (#2122):
 * a map over 4096 rows acquires a handful of mutation sets, not one per row,
 * with or without a governor, and produces exactly the per-row result. */
static uint8_t map_compiled_add[] = {
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_ARITH_ADD
};
/* col1 / 2 + hash(col1) * 0.  The compiler takes no hash, so the map
 * evaluates this with the interpreter (map_run asserts so). */
static uint8_t map_interpreted_div[] = {
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_CONST_INT, 2, 0, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_ARITH_DIV,
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_ARITH_HASH,
    WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_ARITH_MUL,
    WL_PLAN_EXPR_ARITH_ADD
};
/* 1000 / (col1 - 2000) + hash(col1) * 0, by the interpreter: fails at row
 * 2000 of a map over col1 = 0..4095. */
static uint8_t map_div_fails_mid_loop[] = {
    WL_PLAN_EXPR_CONST_INT, 0xe8, 0x03, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_CONST_INT, 0xd0, 0x07, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_ARITH_SUB,
    WL_PLAN_EXPR_ARITH_DIV,
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_ARITH_HASH,
    WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_ARITH_MUL,
    WL_PLAN_EXPR_ARITH_ADD
};
/* col1 + (INT64_MAX - 2000), compiled: overflows from row 2001 on. */
static uint8_t map_add_overflows_mid_loop[] = {
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_CONST_INT, 0x2f, 0xf8, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
    WL_PLAN_EXPR_ARITH_ADD
};

enum { MAP_ROWS = 4096 };

static col_rel_t *
map_source(void)
{
    col_rel_t *src = col_rel_new_auto("map_src", 2);
    assert(src);
    for (uint32_t i = 0; i < MAP_ROWS; i++) {
        int64_t row[2] = { (int64_t)(i * 3u), (int64_t)i };
        assert(col_rel_append_row(src, row) == 0);
    }
    return src;
}

/* Run a two-column map: column 0 is col0, column 1 is @expr over the row,
 * or col1 when @expr is NULL.  @interpreted says whether the compiler must
 * refuse @expr, which sends the map to the interpreter. */
static int
map_run(wl_col_session_t *sess, uint8_t *expr, uint32_t size,
    bool interpreted, uint64_t *acquisitions, col_rel_t **out,
    uint32_t *stack_top)
{
    static const uint32_t project[2] = { 0, 1 };
    wl_plan_expr_buffer_t exprs[2] = { { NULL, 0 }, { expr, size } };
    wl_plan_op_t op;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_MAP;
    op.project_indices = project;
    op.project_count = 2;
    if (expr) {
        int compile_rc = 0;
        wl_columnar_expr_compiled_t *compiled
            = wl_columnar_expr_compile_governed(expr, size, NULL, NULL, NULL,
                &compile_rc);
        assert((compiled == NULL) == interpreted);
        wl_columnar_expr_compiled_free(compiled);
        op.map_exprs = exprs;
        op.map_expr_count = 2;
    }
    eval_stack_t stack;
    eval_stack_init(&stack);
    assert(eval_stack_push(&stack, map_source(), true) == 0);
    wl_columnar_relation_test_set_mutation_nonce(1000);
    uint64_t before = wl_columnar_relation_test_mutation_nonce_peek();
    int rc = col_op_map(&op, &stack, sess);
    *acquisitions = wl_columnar_relation_test_mutation_nonce_peek() - before;
    *stack_top = stack.top;
    *out = NULL;
    if (rc == 0) {
        eval_entry_t result = eval_stack_pop(&stack);
        *out = result.rel;
    }
    return rc;
}

static void
map_op_batch_tests(void)
{
    static const struct {
        uint8_t *expr;
        uint32_t size;
        bool interpreted;
        int64_t add;
        int64_t div;
    } cases[] = {
        { NULL, 0, false, 0, 1 },
        { map_compiled_add, sizeof(map_compiled_add), false, 1, 1 },
        { map_interpreted_div, sizeof(map_interpreted_div), true, 0, 2 },
    };
    for (int governed = 0; governed < 2; governed++) {
        for (size_t k = 0; k < sizeof(cases) / sizeof(cases[0]); k++) {
            wl_col_session_t *sess = join_session();
            wl_columnar_memory_governor_ref_t *ref = governed
                ? enforcing_governor(1u << 26) : NULL;
            sess->memory_governor = ref;
            uint64_t acquisitions = 0;
            col_rel_t *out = NULL;
            uint32_t top = 0;
            assert(map_run(sess, cases[k].expr, cases[k].size,
                cases[k].interpreted, &acquisitions, &out, &top) == 0);
            assert(out && out->nrows == MAP_ROWS && out->ncols == 2);
            assert(acquisitions >= 1 && acquisitions <= 16);
            for (uint32_t i = 0; i < MAP_ROWS; i++)
                assert(out->columns[0][i] == (int64_t)(i * 3u)
                    && out->columns[1][i]
                    == (int64_t)i / cases[k].div + cases[k].add);
            assert(col_rel_destroy_checked(out) == 0);
            sess->memory_governor = NULL;
            join_session_destroy(sess);
            if (ref) {
                assert(governor_reserved(ref) == 0);
                wl_columnar_memory_governor_ref_release(ref);
            }
        }
    }
}

/* An expression that fails part-way through the rows, interpreted or
 * compiled, fails the map with ERANGE after the rows before it were
 * appended under the batch, consumes its input, and leaves no output and no
 * reservation. */
static void
map_error_tests(void)
{
    static const struct {
        uint8_t *expr;
        uint32_t size;
        bool interpreted;
    } cases[] = {
        { map_div_fails_mid_loop, sizeof(map_div_fails_mid_loop), true },
        { map_add_overflows_mid_loop, sizeof(map_add_overflows_mid_loop),
          false },
    };
    for (int governed = 0; governed < 2; governed++) {
        for (size_t k = 0; k < sizeof(cases) / sizeof(cases[0]); k++) {
            wl_col_session_t *sess = join_session();
            wl_columnar_memory_governor_ref_t *ref = governed
                ? enforcing_governor(1u << 26) : NULL;
            sess->memory_governor = ref;
            uint64_t acquisitions = 0;
            col_rel_t *out = NULL;
            uint32_t top = 1;
            assert(map_run(sess, cases[k].expr, cases[k].size,
                cases[k].interpreted, &acquisitions, &out, &top) == ERANGE);
            assert(out == NULL && top == 0);
            sess->memory_governor = NULL;
            join_session_destroy(sess);
            if (ref) {
                assert(governor_reserved(ref) == 0);
                wl_columnar_memory_governor_ref_release(ref);
            }
        }
    }
}

int
main(void)
{
    lease_tests();
    writer_token_liveness_tests();
    overlapping_storage_tests();
    duplicate_role_publication_tests();
    terminal_sequence_tests();
    contention_tests();
    rollback_tests();
    invalid_tests();
    metadata_final_access_test();
    append_batch_tests();
    public_batch_copy_tests();
    public_float_batch_tests();
    filter_batch_tests();
    filter_op_batch_tests();
    join_batch_tests();
    batch_denial_tests();
    batch_renewal_failure_tests();
    batch_lease_check_tests();
    batch_shared_view_tests();
    batch_shared_view_retry_tests();
    batch_renewal_wrong_thread_tests();
    semi_batch_tests();
    semi_denial_tests();
    filter_denial_tests();
    join_denial_tests();
    map_op_batch_tests();
    map_error_tests();
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
