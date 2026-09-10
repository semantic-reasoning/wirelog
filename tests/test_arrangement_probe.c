/* Bound primary-arrangement probe lease contract tests (Issue #1496). */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/thread.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int failures;

#define CHECK(condition, message) do { \
            if (!(condition)) { \
                fprintf(stderr, "FAIL: %s\n", (message)); \
                failures++; \
            } \
} while (0)

static void
noop_cb(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user;
}

static int
make_session(wl_session_t **out_session, wl_plan_t **out_plan,
    wirelog_program_t **out_program)
{
    static const char source[] =
        ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(3, 4).\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;

    if (!program)
        return EINVAL;
    wl_fusion_apply(program, NULL);
    wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    if (wl_plan_from_program(program, &plan) != 0
        || wl_session_create(wl_backend_columnar(), plan, 1, &session) != 0
        || wl_session_load_facts(session, program) != 0
        || wl_session_snapshot(session, noop_cb, NULL) != 0) {
        if (session)
            wl_session_destroy(session);
        if (plan)
            wl_plan_free(plan);
        wirelog_program_free(program);
        return EINVAL;
    }
    *out_session = session;
    *out_plan = plan;
    *out_program = program;
    return 0;
}

static col_arr_entry_t *
find_entry(wl_col_session_t *session, const col_arrangement_t *arr)
{
    for (uint32_t i = 0; i < session->arr_count; i++) {
        if (&session->arr_entries[i].arr == arr)
            return &session->arr_entries[i];
    }
    return NULL;
}

struct release_probe_arg {
    col_arrangement_probe_t *probe;
    int rc;
};

static void *
release_probe_from_other_thread(void *opaque)
{
    struct release_probe_arg *arg = opaque;
    arg->rc = col_arrangement_probe_release(arg->probe);
    return NULL;
}

static void
test_alias_storage_owner_probe(void)
{
    wl_session_t *session = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    col_rel_t *owner = NULL;
    col_arrangement_probe_t probe = { 0 };
    wl_columnar_source_access_writer_t owner_writer = { 0 };
    uint32_t key_cols[] = { 0 };

    CHECK(make_session(&session, &plan, &program) == 0,
        "alias session setup");
    if (!session)
        goto cleanup;

    wl_col_session_t *col_session = COL_SESSION(session);
    col_rel_t *source = session_find_rel(col_session, "edge");
    CHECK(source != NULL, "alias source lookup");
    if (!source)
        goto cleanup;

    CHECK(col_rel_deep_copy(source, &owner, NULL) == 0,
        "alias owner setup");
    if (!owner)
        goto cleanup;
    uint64_t owner_identity = owner->relation_identity;
    uint64_t owner_generation = owner->storage_owner_generation;
    CHECK(col_rel_install_shared_view(source, owner) == 0,
        "install alias shared view");
    CHECK(source->storage_owner == owner
        && source->storage_owner_identity == owner_identity
        && source->storage_owner_generation == owner_generation
        && owner->storage_alias_borrows == 1,
        "source is bound to ultimate storage owner");

    col_arrangement_t *arr = col_session_get_arrangement(session, "edge",
            key_cols, 1);
    col_arr_entry_t *entry = find_entry(col_session, arr);
    CHECK(arr && entry, "alias arrangement setup");
    if (!arr || !entry)
        goto cleanup;

    CHECK(col_session_acquire_arrangement_probe(session, arr, source,
        &probe) == 0, "alias probe acquire");
    CHECK(probe.active && probe.source == source && probe.storage_owner == owner
        && probe.storage_owner_identity == owner_identity
        && probe.storage_owner_generation == owner_generation
        && probe.arrangement_pin.active && entry->pin_count == 1
        && wl_columnar_relation_snapshot_equal(probe.source_snapshot,
        wl_columnar_relation_snapshot(source)),
        "probe captures exact source and ultimate owner generation");
    CHECK(atomic_load_explicit(&owner->source_access.state,
        memory_order_acquire) == 1,
        "alias probe holds owner source reader");
    CHECK(wl_columnar_source_access_writer_acquire(&owner->source_access,
        &owner_writer) == EBUSY,
        "active alias probe blocks owner writer");
    CHECK(col_rel_destroy_checked(owner) == EBUSY,
        "active alias probe blocks owner destroy");

    CHECK(col_arrangement_probe_release(&probe) == 0,
        "alias probe release");
    CHECK(!probe.active && entry->pin_count == 0
        && atomic_load_explicit(&owner->source_access.state,
        memory_order_acquire) == 0,
        "alias release balances owner reader and arrangement pin");
    CHECK(wl_columnar_source_access_writer_acquire(&owner->source_access,
        &owner_writer) == 0,
        "owner writer succeeds after alias probe release");
    CHECK(wl_columnar_source_access_writer_release(&owner_writer) == 0,
        "owner writer release after alias probe release");
    CHECK(col_rel_destroy_checked(owner) == EBUSY,
        "source alias still blocks owner destroy after probe release");

    wl_session_destroy(session);
    session = NULL;
    CHECK(col_rel_destroy_checked(owner) == 0,
        "owner destroy succeeds after alias teardown");
    owner = NULL;

cleanup:
    if (probe.active)
        (void)col_arrangement_probe_release(&probe);
    if (owner_writer.owner)
        (void)wl_columnar_source_access_writer_release(&owner_writer);
    if (session)
        wl_session_destroy(session);
    if (owner)
        (void)col_rel_destroy_checked(owner);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

int
main(void)
{
    wl_session_t *session = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    col_rel_t *same_name = NULL;
    col_arrangement_probe_t probe = { 0 };
    col_arrangement_probe_bundle_t bundle = { 0 };
    col_arrangement_probe_t *bundle_probe = NULL;
    col_arrangement_probe_t *nested_probe = NULL;
    col_arrangement_probe_t *second_probe = NULL;
    wl_columnar_source_access_writer_t writer = { 0 };
    wl_columnar_source_access_writer_t dependency_writer = { 0 };
    struct release_probe_arg release_arg = { &probe, 0 };
    thread_t release_thread;
    uint32_t key_cols[] = { 0 };

    test_alias_storage_owner_probe();

    CHECK(make_session(&session, &plan, &program) == 0,
        "session setup");
    if (!session)
        return 1;

    wl_col_session_t *col_session = COL_SESSION(session);
    col_rel_t *source = session_find_rel(col_session, "edge");
    col_arrangement_t *arr = col_session_get_arrangement(session, "edge",
            key_cols, 1);
    col_arr_entry_t *entry = find_entry(col_session, arr);
    CHECK(source && arr && entry, "source arrangement setup");
    if (!source || !arr || !entry)
        goto cleanup;

    uint64_t acquire_clock = entry->lru_clock;
    CHECK(col_session_acquire_arrangement_probe(session, arr, source,
        &probe) == 0, "successful probe acquire");
    CHECK(probe.active && probe.identity == (uintptr_t)&probe
        && probe.arr == arr && probe.source == source
        && probe.arrangement_pin.active && entry->pin_count == 1
        && wl_columnar_relation_snapshot_equal(probe.source_snapshot,
        wl_columnar_relation_snapshot(source)),
        "probe binds exact arrangement, source, and generation");
    CHECK(entry->lru_clock == acquire_clock,
        "exact-entry acquire does not re-enter name-based getter");
    CHECK(atomic_load_explicit(&source->source_access.state,
        memory_order_acquire) == 1, "probe holds one source reader");
    CHECK(thread_create(&release_thread, release_probe_from_other_thread,
        &release_arg) == 0, "wrong-thread release setup");
    CHECK(thread_join(&release_thread) == 0 && release_arg.rc == EINVAL,
        "wrong-thread release is rejected");
    CHECK(probe.active && probe.arrangement_pin.active
        && entry->pin_count == 1
        && atomic_load_explicit(&source->source_access.state,
        memory_order_acquire) == 1,
        "wrong-thread release retains both leases");
    CHECK(col_arrangement_probe_release(&probe) == 0,
        "successful probe release");
    CHECK(!probe.active && entry->pin_count == 0
        && atomic_load_explicit(&source->source_access.state,
        memory_order_acquire) == 0, "release balances both leases");
    CHECK(col_arrangement_probe_release(&probe) == EINVAL,
        "double release is rejected");

    col_arrangement_t *second_arr = col_session_get_arrangement(session,
            "edge", (uint32_t[]){ 1 }, 1);
    CHECK(second_arr != NULL, "second arrangement setup");
    col_arrangement_probe_bundle_init(&bundle);
    CHECK(bundle.active, "bundle init activates scope");
    CHECK(col_arrangement_probe_bundle_acquire(&bundle, session, arr, source,
        &bundle_probe) == 0 && bundle_probe != NULL,
        "bundle acquires primary probe");
    CHECK(col_arrangement_probe_bundle_acquire(&bundle, session, arr, source,
        &nested_probe) == 0 && nested_probe == bundle_probe
        && bundle.count == 1 && bundle.slots[0].ref_count == 2,
        "nested same-source lease is coalesced and counted");
    if (second_arr)
        CHECK(col_arrangement_probe_bundle_acquire(&bundle, session,
            second_arr, source, &second_probe) == 0
            && second_probe != bundle_probe && bundle.count == 2,
            "bundle acquires a second arrangement dependency");
    CHECK(atomic_load_explicit(&source->source_access.state,
        memory_order_acquire) == 2,
        "bundle holds one reader per distinct arrangement dependency");
    CHECK(col_arrangement_probe_bundle_release(&bundle) == 0
        && !bundle.active && bundle.count == 0
        && atomic_load_explicit(&source->source_access.state,
        memory_order_acquire) == 0,
        "bundle releases dependencies in reverse order");

    col_rel_t *dependency = session_find_rel(col_session, "path");
    col_arrangement_t *dependency_arr = dependency
        ? col_session_get_arrangement(session, "path", key_cols, 1) : NULL;
    CHECK(dependency && dependency_arr, "dependency arrangement setup");
    col_arrangement_probe_bundle_init(&bundle);
    CHECK(bundle.active, "partial rollback bundle init activates scope");
    CHECK(col_arrangement_probe_bundle_acquire(&bundle, session, arr, source,
        &bundle_probe) == 0, "partial rollback acquires first dependency");
    if (dependency && dependency_arr) {
        CHECK(wl_columnar_source_access_writer_acquire(
            &dependency->source_access, &dependency_writer) == 0,
            "partial rollback dependency writer setup");
        CHECK(col_arrangement_probe_bundle_acquire(&bundle, session,
            dependency_arr, dependency, &second_probe) == EBUSY
            && bundle.count == 0 && !bundle.slots[0].probe.active
            && atomic_load_explicit(&source->source_access.state,
            memory_order_acquire) == 0 && entry->pin_count == 0,
            "later dependency failure rolls back earlier leases");
        CHECK(wl_columnar_source_access_writer_release(&dependency_writer)
            == 0, "partial rollback dependency writer release");
    }
    CHECK(col_arrangement_probe_bundle_release(&bundle) == 0,
        "partial rollback leaves an empty releasable bundle");

    col_arrangement_probe_bundle_init(&bundle);
    CHECK(col_arrangement_probe_bundle_acquire(&bundle, session, arr, source,
        &bundle_probe) == 0, "capacity rollback acquires first dependency");
    if (second_arr) {
        /* Fill the bounded scope marker to exercise the full-capacity path;
         * the active slot remains a real lease and must be unwound. */
        bundle.count = COL_ARRANGEMENT_PROBE_BUNDLE_MAX;
        CHECK(col_arrangement_probe_bundle_acquire(&bundle, session,
            second_arr, source, &second_probe) == EOVERFLOW
            && bundle.count == 0
            && atomic_load_explicit(&source->source_access.state,
            memory_order_acquire) == 0 && entry->pin_count == 0,
            "capacity failure rolls back earlier leases");
    }
    CHECK(col_arrangement_probe_bundle_release(&bundle) == 0,
        "capacity rollback leaves an empty releasable bundle");

    col_arrangement_probe_bundle_init(&bundle);
    CHECK(bundle.active, "bundle rollback init activates scope");
    CHECK(wl_columnar_source_access_writer_acquire(&source->source_access,
        &writer) == 0, "bundle rollback writer setup");
    CHECK(col_arrangement_probe_bundle_acquire(&bundle, session, arr, source,
        &bundle_probe) == EBUSY && bundle.count == 0
        && !bundle_probe, "failed bundle acquire rolls back cleanly");
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0,
        "bundle rollback writer release");
    CHECK(col_arrangement_probe_bundle_release(&bundle) == 0,
        "empty bundle release");

    CHECK(col_rel_alloc(&same_name, "edge") == 0,
        "same-name relation setup");
    CHECK(col_session_acquire_arrangement_probe(session, arr, same_name,
        &probe) == EBUSY, "same-name relation cannot replace exact source");
    CHECK(!probe.active && entry->pin_count == 0
        && atomic_load_explicit(&same_name->source_access.state,
        memory_order_acquire) == 0,
        "identity rejection leaves both sources unleased");

    col_arrangement_t saved_arr = *arr;
    col_relation_snapshot_t saved_snapshot = entry->source_snapshot;
    uint64_t saved_clock = entry->lru_clock;
    CHECK(wl_columnar_source_access_writer_acquire(&source->source_access,
        &writer) == 0, "source writer setup");
    CHECK(col_session_acquire_arrangement_probe(session, arr, source,
        &probe) == EBUSY, "source writer EBUSY is propagated");
    CHECK(!probe.active && entry->pin_count == 0
        && entry->lru_clock == saved_clock
        && arr->ht_head == saved_arr.ht_head
        && arr->ht_next == saved_arr.ht_next
        && arr->nbuckets == saved_arr.nbuckets
        && arr->indexed_rows == saved_arr.indexed_rows
        && arr->generation == saved_arr.generation
        && wl_columnar_relation_snapshot_equal(entry->source_snapshot,
        saved_snapshot), "EBUSY preserves arrangement and lease state");
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0,
        "source writer release");

cleanup:
    if (bundle.active)
        (void)col_arrangement_probe_bundle_release(&bundle);
    if (probe.active)
        (void)col_arrangement_probe_release(&probe);
    if (writer.owner)
        (void)wl_columnar_source_access_writer_release(&writer);
    if (dependency_writer.owner)
        (void)wl_columnar_source_access_writer_release(&dependency_writer);
    if (same_name)
        (void)col_rel_destroy_checked(same_name);
    wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(program);
    if (failures != 0)
        return 1;
    puts("arrangement_probe: OK");
    return 0;
}
