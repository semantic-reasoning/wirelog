/*
 * columnar/relation.c - wirelog Columnar Relation Storage
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Relation lifecycle, schema, and row-append operations.
 * Extracted from backend/columnar_nanoarrow.c.
 */

#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* wl_atomic_u64 and the atomic_*_explicit shims come from mem_ledger.h
 * (via internal.h): C11 <stdatomic.h> elsewhere, _Interlocked* on MSVC,
 * which has no C11 atomics in its default C mode.  Relaxed ordering is
 * sufficient: the counter only has to hand out distinct values. */
static wl_atomic_u64 wl_next_relation_identity = 1u;
static wl_atomic_u64 wl_next_mutation_set_nonce = 1u;

#ifdef WL_TEST_MUTATION_SET_HOOK
void
wl_columnar_relation_test_set_mutation_nonce(uint64_t next_nonce)
{
    atomic_store_explicit(&wl_next_mutation_set_nonce, next_nonce,
        memory_order_relaxed);
}

/* The next nonce to be handed out: a delta across a single-threaded window
 * counts the mutation sets acquired in it. */
uint64_t
wl_columnar_relation_test_mutation_nonce_peek(void)
{
    return atomic_load_explicit(&wl_next_mutation_set_nonce,
               memory_order_relaxed);
}
#endif

static int
col_rel_mutation_set_nonce_allocate(uint64_t *nonce)
{
    uint64_t observed;
    if (!nonce)
        return EINVAL;
    observed = atomic_load_explicit(&wl_next_mutation_set_nonce,
            memory_order_relaxed);
    for (;;) {
        if (observed == 0 || observed == UINT64_MAX)
            return EOVERFLOW;
        uint64_t desired = observed + 1u;
        if (atomic_compare_exchange_weak_explicit(&wl_next_mutation_set_nonce,
            &observed, desired, memory_order_relaxed, memory_order_relaxed)) {
            *nonce = observed;
            return 0;
        }
    }
}

#ifdef WL_TEST_APPEND_HOOK
wl_columnar_append_transition_hook_t wl_columnar_append_transition_hook;
#endif

#ifdef WL_SESSION_TEST_HOOKS
wl_columnar_radix_admission_test_hook_t
    wl_columnar_radix_admission_test_hook;
#ifdef _MSC_VER
#define WL_RELATION_TIMESTAMP_TEST_THREAD_LOCAL __declspec(thread)
#else
#define WL_RELATION_TIMESTAMP_TEST_THREAD_LOCAL _Thread_local
#endif
static WL_RELATION_TIMESTAMP_TEST_THREAD_LOCAL bool
wl_columnar_relation_fail_next_governed_timestamp_alloc;
#undef WL_RELATION_TIMESTAMP_TEST_THREAD_LOCAL

void
wl_columnar_relation_test_fail_next_governed_timestamp_alloc(void)
{
    wl_columnar_relation_fail_next_governed_timestamp_alloc = true;
}

bool
wl_columnar_relation_test_governed_timestamp_alloc_pending(void)
{
    return wl_columnar_relation_fail_next_governed_timestamp_alloc;
}
#endif

#ifdef WL_TEST_SET_HOOK
wl_columnar_set_transition_hook_t wl_columnar_set_transition_hook;
#endif

#ifdef WL_TEST_RELATION_RESIZE_HOOK
bool wl_columnar_relation_test_fail_prepare_resize;
void (*wl_columnar_relation_test_reader_after_descriptor)(const col_rel_t *);
void (*wl_columnar_relation_test_rebind_after_descriptor)(const col_rel_t *);
void (*wl_columnar_relation_test_rebind_before_publication)(const col_rel_t *);
static bool wl_columnar_relation_test_fail_compact_rollback;
static bool wl_columnar_relation_test_fail_compact_commit;
static bool wl_columnar_relation_test_fail_reservation_commit;
static bool wl_columnar_relation_test_fail_metadata_alloc;
static bool wl_columnar_relation_test_watch_rollback_cleanup;
static bool wl_columnar_relation_test_resize_columns_retired;
static bool wl_columnar_relation_test_rollback_cleanup_observed;

void
wl_columnar_relation_test_fail_next_prepare_resize(void)
{
    wl_columnar_relation_test_fail_prepare_resize = true;
}

void
wl_columnar_relation_test_clear_prepare_resize(void)
{
    wl_columnar_relation_test_fail_prepare_resize = false;
}

void
wl_columnar_relation_test_fail_next_compact_rollback(void)
{
    wl_columnar_relation_test_fail_compact_rollback = true;
}

void
wl_columnar_relation_test_fail_next_compact_commit(void)
{
    wl_columnar_relation_test_fail_compact_commit = true;
}

void
wl_columnar_relation_test_fail_next_reservation_commit(void)
{
    wl_columnar_relation_test_fail_reservation_commit = true;
}

void
wl_columnar_relation_test_fail_next_metadata_alloc(void)
{
    wl_columnar_relation_test_fail_metadata_alloc = true;
}

void
wl_columnar_relation_test_watch_next_rollback_cleanup(void)
{
    wl_columnar_relation_test_watch_rollback_cleanup = true;
    wl_columnar_relation_test_resize_columns_retired = false;
    wl_columnar_relation_test_rollback_cleanup_observed = false;
}

bool
wl_columnar_relation_test_rollback_cleanup_was_ordered(void)
{
    return wl_columnar_relation_test_rollback_cleanup_observed;
}

#ifdef _MSC_VER
#define WL_RELATION_TEST_THREAD_LOCAL __declspec(thread)
#else
#define WL_RELATION_TEST_THREAD_LOCAL _Thread_local
#endif
static WL_RELATION_TEST_THREAD_LOCAL bool
wl_columnar_relation_fail_governed_copy_payload_alloc;
#undef WL_RELATION_TEST_THREAD_LOCAL
static bool wl_columnar_relation_fail_mutable_image_overflow;
void (*wl_columnar_relation_test_after_governed_copy_admission)(
    const col_rel_t *);
void (*wl_columnar_relation_test_after_retired_storage_free)(const col_rel_t *);

void
wl_columnar_relation_test_fail_next_governed_copy_payload_alloc(void)
{
    wl_columnar_relation_fail_governed_copy_payload_alloc = true;
}

bool
wl_columnar_relation_test_governed_copy_payload_alloc_pending(void)
{
    return wl_columnar_relation_fail_governed_copy_payload_alloc;
}

void
wl_columnar_relation_test_clear_governed_copy_payload_alloc(void)
{
    wl_columnar_relation_fail_governed_copy_payload_alloc = false;
}

void
wl_columnar_relation_test_fail_next_mutable_image_overflow(void)
{
    wl_columnar_relation_fail_mutable_image_overflow = true;
}

void
wl_columnar_relation_test_clear_mutable_image_overflow(void)
{
    wl_columnar_relation_fail_mutable_image_overflow = false;
}
static bool wl_columnar_relation_fail_commit_publication;

void
wl_columnar_relation_test_fail_next_commit_publication(void)
{
    wl_columnar_relation_fail_commit_publication = true;
}
#endif

#ifdef WL_TEST_RELATION_RESIZE_HOOK
static void
wl_columnar_relation_test_note_resize_columns_retired(void)
{
    if (wl_columnar_relation_test_watch_rollback_cleanup)
        wl_columnar_relation_test_resize_columns_retired = true;
}
#endif

static void *
wl_columnar_relation_radix_malloc(size_t size, const char *site)
{
#ifdef WL_TEST_CONSOLIDATE_ALLOC_HOOK
    if (wl_columnar_consolidate_alloc_hook
        && wl_columnar_consolidate_alloc_hook(site))
        return NULL;
#else
    (void)site;
#endif
    return malloc(size);
}

static int
col_rel_new_identity(uint64_t *out)
{
    uint64_t current = atomic_load_explicit(&wl_next_relation_identity,
            memory_order_relaxed);
    for (;;) {
        if (current == 0u || current == UINT64_MAX)
            return EOVERFLOW;
        uint64_t next = current + 1u;
        if (atomic_compare_exchange_weak_explicit(&wl_next_relation_identity,
            &current, next, memory_order_relaxed, memory_order_relaxed)) {
            *out = current;
            return 0;
        }
    }
}

/* Test-only seam used to exercise the terminal allocator state without
 * manufacturing UINT64_MAX relations in a production process. */
int
col_rel_test_set_next_identity(uint64_t next)
{
    atomic_store_explicit(&wl_next_relation_identity, next,
        memory_order_relaxed);
    return 0;
}

#if defined(_MSC_VER)
#define WL_COLUMNAR_RELATION_NOINLINE __declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
#define WL_COLUMNAR_RELATION_NOINLINE __attribute__((noinline))
#else
#define WL_COLUMNAR_RELATION_NOINLINE
#endif

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __ARM_NEON__
#include <arm_neon.h>
#endif

/*
 * Portable software-prefetch macros (Issue #363).
 *
 * WL_PREFETCH_R(addr) — read prefetch, L2 temporal (gather source).
 * WL_PREFETCH_W(addr) — write prefetch, L2 temporal (scatter destination).
 *
 * MSVC does not support __builtin_prefetch; map to _mm_prefetch with
 * _MM_HINT_T1 (L2 locality) for both read and write variants since MSVC
 * does not expose a write-intent hint via _mm_prefetch.
 */
#if defined(_MSC_VER)
#include <xmmintrin.h>
#define WL_PREFETCH_R(addr) _mm_prefetch((const char *)(addr), _MM_HINT_T1)
#define WL_PREFETCH_W(addr) _mm_prefetch((const char *)(addr), _MM_HINT_T1)
#elif defined(__GNUC__) || defined(__clang__)
#define WL_PREFETCH_R(addr) __builtin_prefetch((addr), 0, 1)
#define WL_PREFETCH_W(addr) __builtin_prefetch((addr), 1, 1)
#else
#define WL_PREFETCH_R(addr) ((void)(addr))
#define WL_PREFETCH_W(addr) ((void)(addr))
#endif

#ifdef WL_RADIX_BENCH
static bool
wl_columnar_relation_radix_bench_enabled(void)
{
    const char *env = getenv("WIRELOG_RADIX_BENCH_LOG");

    return env && env[0] != '\0' && strcmp(env, "0") != 0;
}
#endif

/* ---- COW helpers --------------------------------------------------------- */

static int col_rel_mutation_set_acquire_impl(
    wl_columnar_relation_mutation_set_t *set,
    const wl_columnar_relation_mutation_role_t *roles, size_t role_count,
    wl_columnar_relation_mutation_descriptor_t *descriptors,
    size_t descriptor_cap,
    wl_columnar_relation_mutation_owner_t *owners, size_t owner_cap,
    wl_columnar_relation_mutation_lease_t *leases, size_t lease_cap,
    wl_columnar_relation_mutation_initialization_t *initializations,
    size_t initialization_cap, bool storage_disjoint);

static int
col_rel_mutation_single_acquire(col_rel_t *relation,
    wl_columnar_relation_mutation_single_t *single)
{
    single->role.relation = relation;
    single->role.role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION;
    /* Distinct members of one object never overlap, so the caller-storage
     * overlap scan cannot reject this layout. */
    return col_rel_mutation_set_acquire_impl(&single->set, &single->role, 1,
               &single->descriptor, 1, single->owners, 2, &single->lease, 1,
               &single->initialization, 1, true);
}

static int col_rel_grow_owned_transition_legacy_impl(col_rel_t *r,
    uint32_t new_cap, bool defer_alias_release);
static int col_rel_cow_unshare_legacy_impl(col_rel_t *r, uint32_t new_cap,
    bool defer_alias_release);
static int col_rel_grow_owned_transition_publish_impl(col_rel_t *r,
    uint32_t new_cap, bool defer_alias_release,
    wl_columnar_relation_mutation_lease_t *lease);
static int col_rel_cow_unshare_publish_impl(col_rel_t *r, uint32_t new_cap,
    bool defer_alias_release,
    wl_columnar_relation_mutation_lease_t *lease);
static int col_rel_mutation_lease_advance_storage(
    wl_columnar_relation_mutation_lease_t *lease, uint64_t prior_generation);
static bool col_rel_timestamp_shape_valid(const col_rel_t *r);
static int col_rel_mutation_lease_validate_binding(
    const wl_columnar_relation_mutation_lease_t *lease,
    const col_rel_t *expected_relation);

/* Append-all, batch and replacement transactions retain their existing
 * private authority until those complete transactions migrate. */
static int
col_rel_grow_owned_transition_legacy_impl(col_rel_t *r, uint32_t new_cap,
    bool defer_alias_release)
{
    return col_rel_grow_owned_transition_publish_impl(r, new_cap,
               defer_alias_release, NULL);
}

static int
col_rel_cow_unshare_legacy_impl(col_rel_t *r, uint32_t new_cap,
    bool defer_alias_release)
{
    return col_rel_cow_unshare_publish_impl(r, new_cap, defer_alias_release,
               NULL);
}

static int
col_rel_grow_owned_transition(col_rel_t *r, uint32_t new_cap)
{
    return col_rel_grow_owned_transition_legacy_impl(r, new_cap, false);
}

static void
col_rel_storage_owner_init(col_rel_t *r)
{
    if (!r)
        return;
    r->storage_owner = r;
    r->storage_owner_identity = r->relation_identity;
    r->storage_owner_generation = r->storage_generation;
    atomic_store_explicit(&r->storage_alias_borrows, 0,
        memory_order_relaxed);
    wl_columnar_source_access_gate_init(&r->source_access);
    wl_columnar_source_access_gate_init(&r->descriptor_access);
}

uint64_t
col_rel_storage_alias_borrow_count(const col_rel_t *owner)
{
    if (!owner)
        return 0;
    return atomic_load_explicit(&owner->storage_alias_borrows,
               memory_order_acquire);
}

int
col_rel_storage_alias_borrow_acquire(col_rel_t *owner)
{
    uint64_t observed;

    if (!owner || owner->storage_owner != owner
        || owner->storage_owner_identity != owner->relation_identity)
        return EINVAL;
    observed = atomic_load_explicit(&owner->storage_alias_borrows,
            memory_order_relaxed);
    for (;;) {
        if (observed == UINT64_MAX)
            return EOVERFLOW;
        if (atomic_compare_exchange_weak_explicit(
                &owner->storage_alias_borrows, &observed, observed + 1,
                memory_order_release, memory_order_relaxed))
            return 0;
    }
}

int
col_rel_storage_alias_borrow_release(col_rel_t *owner)
{
    uint64_t observed;

    if (!owner || owner->storage_owner != owner
        || owner->storage_owner_identity != owner->relation_identity)
        return EINVAL;
    observed = atomic_load_explicit(&owner->storage_alias_borrows,
            memory_order_acquire);
    for (;;) {
        if (observed == 0)
            return EINVAL;
        if (atomic_compare_exchange_weak_explicit(
                &owner->storage_alias_borrows, &observed, observed - 1,
                memory_order_acq_rel, memory_order_acquire))
            return 0;
    }
}

int
col_rel_storage_owner_resolve(const col_rel_t *src, col_rel_t **out_owner)
{
    col_rel_t *owner;
    if (!src || !out_owner)
        return EINVAL;
    owner = src->storage_owner;
    if (!owner)
        return EINVAL;
    if (owner->storage_owner != owner
        || owner->storage_owner_identity != owner->relation_identity
        || owner->storage_owner_generation == 0
        || !wl_columnar_relation_generation_valid(
            owner->storage_owner_generation))
        return EINVAL;
    if (owner != src && src->storage_owner != owner)
        return EINVAL;
    *out_owner = owner;
    return 0;
}

/* Mutation sets own only caller-provided storage. No allocation is permitted
 * between descriptor admission and finish, including failure unwind. */
static bool
col_rel_mutation_writer_inert(const wl_columnar_source_access_writer_t *writer)
{
    return !writer->owner && !writer->secondary_owner && !writer->identity
           && !writer->thread_valid;
}

static void
col_rel_mutation_set_unwind(wl_columnar_relation_mutation_set_t *set,
    bool commit)
{
    while (set->owners_acquired)
        (void)wl_columnar_source_access_writer_release(
            &set->owners[--set->owners_acquired].writer);
    if (!commit) {
        for (size_t i = set->initialization_count; i > 0; i--) {
            wl_columnar_relation_mutation_initialization_t *init
                = &set->initializations[i - 1];
            init->relation->storage_owner = NULL;
            init->relation->storage_owner_identity = init->owner_identity;
            init->relation->storage_owner_generation = init->owner_generation;
            atomic_store_explicit(&init->relation->storage_alias_borrows,
                init->borrows, memory_order_relaxed);
        }
    }
    while (set->descriptors_acquired)
        (void)wl_columnar_source_access_writer_release(
            &set->descriptors[--set->descriptors_acquired].writer);
    memset(set->descriptors, 0,
        set->descriptor_count * sizeof(*set->descriptors));
    memset(set->owners, 0, set->owner_count * sizeof(*set->owners));
    memset(set->leases, 0, set->lease_count * sizeof(*set->leases));
    memset(set->initializations, 0,
        set->initialization_count * sizeof(*set->initializations));
    memset(set, 0, sizeof(*set));
}

int
col_rel_mutation_set_acquire(wl_columnar_relation_mutation_set_t *set,
    const wl_columnar_relation_mutation_role_t *roles, size_t role_count,
    wl_columnar_relation_mutation_descriptor_t *descriptors,
    size_t descriptor_cap,
    wl_columnar_relation_mutation_owner_t *owners, size_t owner_cap,
    wl_columnar_relation_mutation_lease_t *leases, size_t lease_cap,
    wl_columnar_relation_mutation_initialization_t *initializations,
    size_t initialization_cap)
{
    return col_rel_mutation_set_acquire_impl(set, roles, role_count,
               descriptors, descriptor_cap, owners, owner_cap, leases,
               lease_cap, initializations, initialization_cap, false);
}

static int
col_rel_mutation_set_acquire_impl(wl_columnar_relation_mutation_set_t *set,
    const wl_columnar_relation_mutation_role_t *roles, size_t role_count,
    wl_columnar_relation_mutation_descriptor_t *descriptors,
    size_t descriptor_cap,
    wl_columnar_relation_mutation_owner_t *owners, size_t owner_cap,
    wl_columnar_relation_mutation_lease_t *leases, size_t lease_cap,
    wl_columnar_relation_mutation_initialization_t *initializations,
    size_t initialization_cap, bool storage_disjoint)
{
    int rc = EINVAL;
    size_t max_owners;
    if (!role_count)
        return EINVAL;
    if (role_count > SIZE_MAX / 2u)
        return EOVERFLOW;
    max_owners = role_count * 2u;
    if (!set || set->identity || set->roles || set->descriptors ||
        set->acquisition_nonce ||
        set->owners || set->leases
        || set->initializations || set->descriptor_count || set->owner_count
        || set->lease_count || set->initialization_count
        || set->descriptors_acquired || set->owners_acquired
        || set->terminal_sequence || set->terminal_expected_events
        || set->terminal_observed_events || set->terminal_sequence_consumed
        || set->published
        || !role_count || !roles || !descriptors || !owners || !leases
        || !initializations || descriptor_cap < role_count
        || owner_cap < max_owners || lease_cap < role_count
        || initialization_cap < role_count)
        return EINVAL;
    if (role_count > SIZE_MAX / sizeof(*descriptors)
        || max_owners > SIZE_MAX / sizeof(*owners)
        || role_count > SIZE_MAX / sizeof(*leases)
        || role_count > SIZE_MAX / sizeof(*initializations))
        return EOVERFLOW;
    /* Reject overlapping caller storage before writing any bookkeeping. */
    uintptr_t starts[] = {(uintptr_t)set, (uintptr_t)roles,
                          (uintptr_t)descriptors, (uintptr_t)owners,
                          (uintptr_t)leases,
                          (uintptr_t)initializations};
    size_t lengths[] = {sizeof(*set), role_count * sizeof(*roles),
                        role_count * sizeof(*descriptors),
                        max_owners * sizeof(*owners),
                        role_count * sizeof(*leases),
                        role_count * sizeof(*initializations)};
    for (size_t i = 0; !storage_disjoint
        && i < sizeof(starts) / sizeof(starts[0]); i++) {
        if (lengths[i] > UINTPTR_MAX - starts[i])
            return EOVERFLOW;
        for (size_t j = 0; j < i; j++)
            if (starts[i] < starts[j] + lengths[j]
                && starts[j] < starts[i] + lengths[i])
                return EINVAL;
    }
    for (size_t i = 0; i < role_count; i++) {
        if (!roles[i].relation
            || (roles[i].role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
            && roles[i].role_flags != WL_COLUMNAR_RELATION_METADATA_DETACH)
            || descriptors[i].relation
            || !col_rel_mutation_writer_inert(&descriptors[i].writer)
            || leases[i].identity || leases[i].acquisition_nonce
            || leases[i].set || leases[i].relation
            || leases[i].owner || leases[i].descriptor_slot
            || leases[i].owner_slot || leases[i].self_owner_slot
            || leases[i].relation_identity
            || leases[i].relation_generation || leases[i].owner_identity
            || leases[i].owner_generation || leases[i].role_flags
            || leases[i].detached || leases[i].storage_transitioned
            || initializations[i].relation
            || initializations[i].owner_identity
            || initializations[i].owner_generation ||
            initializations[i].borrows)
            return EINVAL;
    }
    for (size_t i = 0; i < max_owners; i++)
        if (owners[i].owner
            || !col_rel_mutation_writer_inert(&owners[i].writer))
            return EINVAL;
    uint64_t acquisition_nonce;
    rc = col_rel_mutation_set_nonce_allocate(&acquisition_nonce);
    if (rc)
        return rc;
    set->identity = (uintptr_t)set;
    set->acquisition_nonce = acquisition_nonce;
    set->roles = roles;
    set->descriptors = descriptors;
    set->owners = owners;
    set->leases = leases;
    set->initializations = initializations;
    set->lease_count = role_count;
    for (size_t i = 0; i < role_count; i++) {
        size_t j = 0;
        while (j < set->descriptor_count
            && descriptors[j].relation != roles[i].relation)
            j++;
        if (j == set->descriptor_count)
            descriptors[set->descriptor_count++].relation = roles[i].relation;
    }
    for (size_t i = 1; i < set->descriptor_count; i++) {
        col_rel_t *relation = descriptors[i].relation;
        size_t j = i;
        while (j && (uintptr_t)&descriptors[j - 1].relation->descriptor_access
            > (uintptr_t)&relation->descriptor_access) {
            descriptors[j].relation = descriptors[j - 1].relation;
            j--;
        }
        descriptors[j].relation = relation;
    }
    for (size_t i = 0; i < set->descriptor_count; i++) {
        rc = wl_columnar_source_access_writer_acquire(
            &descriptors[i].relation->descriptor_access,
            &descriptors[i].writer);
        if (rc)
            goto fail;
        set->descriptors_acquired++;
    }
    for (size_t i = 0; i < set->descriptor_count; i++) {
        col_rel_t *relation = descriptors[i].relation;
        if (!relation->storage_owner) {
            if (!relation->relation_identity
                || col_rel_storage_alias_borrow_count(relation)
                || !wl_columnar_relation_generation_valid(
                    relation->storage_generation)) {
                rc = EINVAL;
                goto fail;
            }
            wl_columnar_relation_mutation_initialization_t *init
                = &initializations[set->initialization_count++];
            init->relation = relation;
            init->owner_identity = relation->storage_owner_identity;
            init->owner_generation = relation->storage_owner_generation;
            init->borrows = col_rel_storage_alias_borrow_count(relation);
            relation->storage_owner = relation;
            relation->storage_owner_identity = relation->relation_identity;
            relation->storage_owner_generation = relation->storage_generation;
            atomic_store_explicit(&relation->storage_alias_borrows, 0,
                memory_order_relaxed);
        }
    }
    for (size_t i = 0; i < role_count; i++) {
        col_rel_t *relation = roles[i].relation;
        col_rel_t *owner;
        rc = col_rel_storage_owner_resolve(relation, &owner);
        if (rc)
            goto fail;
        if (!relation->relation_identity || !owner->relation_identity
            || !wl_columnar_relation_generation_valid(
                relation->storage_generation)
            || !wl_columnar_relation_generation_valid(owner->storage_generation)
            || relation->storage_owner_identity != owner->relation_identity
            || relation->storage_owner_generation !=
            owner->storage_generation) {
            rc = EINVAL;
            goto fail;
        }
        wl_columnar_relation_mutation_lease_t *lease = &leases[i];
        lease->set = set;
        lease->acquisition_nonce = set->acquisition_nonce;
        lease->relation = relation;
        lease->owner = owner;
        lease->role_flags = roles[i].role_flags;
        lease->relation_identity = relation->relation_identity;
        lease->relation_generation = relation->storage_generation;
        lease->owner_identity = owner->relation_identity;
        lease->owner_generation = owner->storage_generation;
        for (size_t j = 0; j < set->descriptor_count; j++)
            if (descriptors[j].relation == relation)
                lease->descriptor_slot = j;
        if (roles[i].role_flags == WL_COLUMNAR_RELATION_PAYLOAD_MUTATION) {
            size_t j = 0;
            while (j < set->owner_count && owners[j].owner != owner)
                j++;
            if (j == set->owner_count)
                owners[set->owner_count++].owner = owner;
            j = 0;
            while (j < set->owner_count && owners[j].owner != relation)
                j++;
            if (j == set->owner_count)
                owners[set->owner_count++].owner = relation;
        }
    }
    for (size_t i = 1; i < set->owner_count; i++) {
        col_rel_t *owner = owners[i].owner;
        size_t j = i;
        while (j && (uintptr_t)&owners[j - 1].owner->source_access
            > (uintptr_t)&owner->source_access) {
            owners[j].owner = owners[j - 1].owner;
            j--;
        }
        owners[j].owner = owner;
    }
    for (size_t i = 0; i < set->owner_count; i++) {
        rc = wl_columnar_source_access_writer_acquire(
            &owners[i].owner->source_access, &owners[i].writer);
        if (rc)
            goto fail;
        set->owners_acquired++;
    }
    for (size_t i = 0; i < role_count; i++) {
        wl_columnar_relation_mutation_lease_t *lease = &leases[i];
        if (lease->role_flags == WL_COLUMNAR_RELATION_PAYLOAD_MUTATION) {
            for (size_t j = 0; j < set->owner_count; j++)
                if (owners[j].owner == lease->owner)
                    lease->owner_slot = j;
            for (size_t j = 0; j < set->owner_count; j++)
                if (owners[j].owner == lease->relation)
                    lease->self_owner_slot = j;
            if (lease->self_owner_slot >= set->owner_count
                || owners[lease->self_owner_slot].owner != lease->relation) {
                rc = EINVAL;
                goto fail;
            }
            if (lease->relation == lease->owner
                && col_rel_storage_alias_borrow_count(lease->owner)) {
                rc = EBUSY;
                goto fail;
            }
        }
        lease->identity = (uintptr_t)lease;
    }
    /* Ownership snapshots must still match after source admission.  This
     * call acquired every descriptor and owner token on this thread and has
     * released none, so only the binding needs to be rechecked. */
    for (size_t i = 0; i < role_count; i++) {
        rc = col_rel_mutation_lease_validate_binding(&leases[i],
                roles[i].relation);
        if (rc)
            goto fail;
    }
    return 0;
fail:
    col_rel_mutation_set_unwind(set, false);
    return rc;
}

wl_columnar_relation_mutation_lease_t *
col_rel_mutation_set_lease(wl_columnar_relation_mutation_set_t *set,
    size_t index)
{
    if (!set || set->identity != (uintptr_t)set || !set->leases
        || index >= set->lease_count || set->leases[index].set != set
        || col_rel_mutation_lease_validate(&set->leases[index],
        set->leases[index].relation))
        return NULL;
    return &set->leases[index];
}

/* Every lease check except writer-token liveness: set membership, slot
 * structure, canonical owner, and identity/generation snapshots.  Callers
 * that use this directly must already prove, on this thread and without an
 * intervening release, that every descriptor and owner token of the set is a
 * live writer for its gate. */
static int
col_rel_mutation_lease_validate_binding(
    const wl_columnar_relation_mutation_lease_t *lease,
    const col_rel_t *expected_relation)
{
    if (!lease || lease->identity != (uintptr_t)lease || !lease->set
        || !lease->acquisition_nonce
        || !expected_relation || lease->relation != expected_relation
        || lease->detached)
        return EINVAL;
    const wl_columnar_relation_mutation_set_t *set = lease->set;
    if (set->identity != (uintptr_t)set || !set->acquisition_nonce
        || lease->acquisition_nonce != set->acquisition_nonce
        || !set->roles || !set->leases ||
        !set->descriptors
        || !set->owners || set->descriptors_acquired != set->descriptor_count
        || set->owners_acquired != set->owner_count
        || lease->descriptor_slot >= set->descriptor_count)
        return EINVAL;
    bool member = false;
    for (size_t i = 0; i < set->lease_count; i++)
        if (&set->leases[i] == lease) {
            if (set->roles[i].relation != expected_relation
                || set->roles[i].role_flags != lease->role_flags)
                return EINVAL;
            member = true;
        }
    if (!member)
        return EINVAL;
    if (set->descriptors[lease->descriptor_slot].relation != expected_relation)
        return EINVAL;
    col_rel_t *owner;
    if (col_rel_storage_owner_resolve(expected_relation, &owner)
        || owner != lease->owner
        || expected_relation->relation_identity != lease->relation_identity
        || expected_relation->storage_generation != lease->relation_generation
        || expected_relation->storage_owner_identity != lease->owner_identity
        || expected_relation->storage_owner_generation !=
        lease->owner_generation
        || owner->relation_identity != lease->owner_identity
        || owner->storage_generation != lease->owner_generation
        || !wl_columnar_relation_generation_valid(lease->relation_generation)
        || !wl_columnar_relation_generation_valid(lease->owner_generation))
        return EINVAL;
    if (lease->role_flags == WL_COLUMNAR_RELATION_METADATA_DETACH)
        return 0;
    if (lease->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || lease->owner_slot >= set->owner_count
        || set->owners[lease->owner_slot].owner != owner
        || lease->self_owner_slot >= set->owner_count
        || set->owners[lease->self_owner_slot].owner != expected_relation)
        return EINVAL;
    return 0;
}

int
col_rel_mutation_lease_validate(
    const wl_columnar_relation_mutation_lease_t *lease,
    const col_rel_t *expected_relation)
{
    int rc = col_rel_mutation_lease_validate_binding(lease, expected_relation);
    if (rc)
        return rc;
    const wl_columnar_relation_mutation_set_t *set = lease->set;
    if (wl_columnar_source_access_writer_validate(
            &set->descriptors[lease->descriptor_slot].writer,
            &expected_relation->descriptor_access))
        return EINVAL;
    if (lease->role_flags == WL_COLUMNAR_RELATION_METADATA_DETACH)
        return 0;
    if (wl_columnar_source_access_writer_validate(
            &set->owners[lease->self_owner_slot].writer,
            &expected_relation->source_access))
        return EINVAL;
    /* The binding proved owners[owner_slot] is the canonical owner and
     * owners[self_owner_slot] is this relation, so equal slots name the
     * token and gate validated just above. */
    if (lease->owner_slot == lease->self_owner_slot)
        return 0;
    return wl_columnar_source_access_writer_validate(
        &set->owners[lease->owner_slot].writer,
        &lease->owner->source_access);
}

/* A leased publisher may advance this lease only for the exact storage
 * generation it just published. Mutation-set acquisition holds both the
 * current owner and the prospective self-owner source gates, so alias COW
 * can continue under a real token for the newly canonical relation. */
static int
col_rel_mutation_lease_advance_storage(
    wl_columnar_relation_mutation_lease_t *lease, uint64_t prior_generation)
{
    if (!lease || lease->identity != (uintptr_t)lease || !lease->set
        || (lease->detached && lease->owner == lease->relation)
        || lease->storage_transitioned
        || lease->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || !lease->relation || !lease->owner
        || lease->relation_generation != prior_generation
        || prior_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EINVAL;
    wl_columnar_relation_mutation_set_t *set = lease->set;
    col_rel_t *relation = lease->relation;
    if (set->identity != (uintptr_t)set || !set->leases || !set->roles
        || !set->descriptors || !set->owners
        || set->descriptors_acquired != set->descriptor_count
        || set->owners_acquired != set->owner_count
        || lease->descriptor_slot >= set->descriptor_count
        || lease->owner_slot >= set->owner_count
        || lease->self_owner_slot >= set->owner_count
        || set->descriptors[lease->descriptor_slot].relation != relation
        || set->owners[lease->owner_slot].owner != lease->owner
        || set->owners[lease->self_owner_slot].owner != relation)
        return EINVAL;
    bool member = false;
    col_rel_t *prior_owner = lease->owner;
    uint64_t prior_owner_identity = lease->owner_identity;
    uint64_t prior_owner_generation = lease->owner_generation;
    for (size_t i = 0; i < set->lease_count; i++)
        if (&set->leases[i] == lease
            && set->roles[i].relation == relation
            && set->roles[i].role_flags == lease->role_flags) {
            member = true;
        }
    if (!member || !relation
        || relation->relation_identity != lease->relation_identity
        || wl_columnar_source_access_writer_validate(
            &set->descriptors[lease->descriptor_slot].writer,
            &relation->descriptor_access)
        || wl_columnar_source_access_writer_validate(
            &set->owners[lease->owner_slot].writer,
            &lease->owner->source_access)
        || wl_columnar_source_access_writer_validate(
            &set->owners[lease->self_owner_slot].writer,
            &relation->source_access))
        return EINVAL;

    uint64_t next_generation = prior_generation + 1u;
    if (relation->storage_generation != next_generation
        || relation->storage_owner != relation
        || relation->storage_owner_identity != relation->relation_identity
        || relation->storage_owner_generation != next_generation
        || !wl_columnar_relation_generation_valid(next_generation))
        return EINVAL;

    if (prior_owner == relation) {
        /* Same-owner reserve/resize: the identity and matching token stay
         * fixed while storage and owner generation advance together. */
        if (lease->detached || lease->owner_slot != lease->self_owner_slot
            || lease->owner_identity != relation->relation_identity
            || lease->owner_generation != prior_generation
            || relation->storage_owner_identity != lease->owner_identity)
            return EINVAL;
    } else {
        /* Alias COW: only a just-released binding may move to the exact
         * pre-acquired self-owner token; arbitrary stale leases cannot. */
        if (!lease->detached || prior_owner == NULL
            || prior_owner_identity != prior_owner->relation_identity
            || prior_owner_generation != prior_owner->storage_generation)
            return EINVAL;
    }

    /* A mutation set may hold the same descriptor in multiple payload
     * roles. Validate every matching lease against the pre-publication
     * binding before advancing any of them, then advance them together so
     * commit validation observes one consistent published generation. */
    for (size_t i = 0; i < set->lease_count; i++) {
        wl_columnar_relation_mutation_lease_t *same = &set->leases[i];
        if (set->roles[i].relation != relation
            || set->roles[i].role_flags
            != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION)
            continue;
        if (same->set != set || same->identity != (uintptr_t)same
            || same->relation != relation || same->storage_transitioned
            || same->relation_identity != lease->relation_identity
            || same->relation_generation != prior_generation
            || same->owner != prior_owner
            || same->owner_identity != prior_owner_identity
            || same->owner_generation != prior_owner_generation
            || same->descriptor_slot != lease->descriptor_slot
            || same->owner_slot != lease->owner_slot
            || same->self_owner_slot != lease->self_owner_slot
            || same->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION)
            return EINVAL;
    }
    for (size_t i = 0; i < set->lease_count; i++) {
        wl_columnar_relation_mutation_lease_t *same = &set->leases[i];
        if (set->roles[i].relation != relation
            || set->roles[i].role_flags
            != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION)
            continue;
        if (prior_owner != relation) {
            same->owner = relation;
            same->owner_slot = same->self_owner_slot;
            same->owner_identity = relation->relation_identity;
            same->detached = false;
        }
        same->relation_generation = next_generation;
        same->owner_generation = next_generation;
        same->storage_transitioned = true;
    }
    set->published = true;
    return 0;
}

#ifdef WL_TEST_MUTATION_SET_HOOK
int
wl_columnar_relation_test_mutation_lease_advance_storage(
    wl_columnar_relation_mutation_lease_t *lease, uint64_t prior_generation)
{
    return col_rel_mutation_lease_advance_storage(lease, prior_generation);
}
#endif

int
col_rel_mutation_set_finish(wl_columnar_relation_mutation_set_t *set,
    bool commit)
{
    if (!set || set->identity != (uintptr_t)set
        || set->descriptors_acquired != set->descriptor_count
        || set->owners_acquired != set->owner_count)
        return EINVAL;
    if (set->terminal_sequence)
        abort();
    if (!set->leases || !set->roles || !set->descriptors || !set->owners
        || !set->initializations || !set->lease_count || !set->descriptor_count)
        return EINVAL;
    for (size_t i = 0; i < set->lease_count; i++)
        if (set->leases[i].set != set
            || set->leases[i].identity != (uintptr_t)&set->leases[i])
            return EINVAL;
    for (size_t i = 0; i < set->descriptor_count; i++)
        if (wl_columnar_source_access_writer_validate(
                &set->descriptors[i].writer,
                &set->descriptors[i].relation->descriptor_access))
            return EINVAL;
    for (size_t i = 0; i < set->owner_count; i++)
        if (wl_columnar_source_access_writer_validate(&set->owners[i].writer,
            &set->owners[i].owner->source_access))
            return EINVAL;
    /* Every descriptor and owner token was validated just above, and each
     * binding names one of those exact token/gate pairs. */
    if (commit || set->published)
        for (size_t i = 0; i < set->lease_count; i++)
            if (set->roles[i].role_flags
                == WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
                && col_rel_mutation_lease_validate_binding(&set->leases[i],
                set->roles[i].relation) != 0)
                abort();
    col_rel_mutation_set_unwind(set, commit || set->published);
    return 0;
}

static uint32_t
wl_columnar_relation_terminal_event_count(uint32_t events)
{
    uint32_t count = 0;
    for (uint32_t bits = events; bits; bits &= bits - 1u)
        count++;
    return count;
}

static bool
wl_columnar_relation_terminal_tokens_valid(
    const wl_columnar_relation_terminal_sequence_t *sequence,
    bool validate_postconditions)
{
    if (!sequence || sequence->identity != (uintptr_t)sequence
        || !sequence->armed || sequence->consumed || !sequence->set
        || !sequence->lease || !sequence->relation || !sequence->owner)
        return false;
    const uint32_t allowed_events =
        WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP
        | WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE;
    wl_columnar_relation_mutation_set_t *set = sequence->set;
    const wl_columnar_relation_mutation_lease_t *lease = sequence->lease;
    col_rel_t *relation = sequence->relation;
    if (set->identity != (uintptr_t)set || set->terminal_sequence != sequence
        || set->terminal_sequence_consumed
        || !sequence->expected_events
        || (sequence->expected_events & ~allowed_events)
        || set->terminal_expected_events != sequence->expected_events
        || set->terminal_observed_events != sequence->observed_events
        || (sequence->observed_events & ~sequence->expected_events)
        || lease->set != set
        || lease->identity != (uintptr_t)lease || lease->relation != relation
        || lease->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || !set->roles || !set->leases || !set->descriptors || !set->owners
        || lease->descriptor_slot != sequence->descriptor_slot
        || lease->self_owner_slot != sequence->self_owner_slot
        || set->descriptors_acquired != set->descriptor_count
        || set->owners_acquired != set->owner_count
        || sequence->descriptor_slot >= set->descriptor_count
        || sequence->owner_slot >= set->owner_count
        || sequence->self_owner_slot >= set->owner_count
        || set->descriptors[sequence->descriptor_slot].relation != relation
        || set->owners[sequence->owner_slot].owner != sequence->owner
        || set->owners[sequence->self_owner_slot].owner != relation
        || set->descriptors[sequence->descriptor_slot].writer.identity
        != sequence->descriptor_writer_identity
        || set->owners[sequence->owner_slot].writer.identity
        != sequence->owner_writer_identity
        || set->owners[sequence->self_owner_slot].writer.identity
        != sequence->self_writer_identity
        || wl_columnar_source_access_writer_validate(
            &set->descriptors[sequence->descriptor_slot].writer,
            &relation->descriptor_access)
        || wl_columnar_source_access_writer_validate(
            &set->owners[sequence->owner_slot].writer,
            &sequence->owner->source_access)
        || wl_columnar_source_access_writer_validate(
            &set->owners[sequence->self_owner_slot].writer,
            &relation->source_access))
        return false;
    uint32_t observed_steps =
        wl_columnar_relation_terminal_event_count(sequence->observed_events);
    uint64_t observed_generation = sequence->relation_generation
        + observed_steps;
    bool lease_member = false;
    for (size_t i = 0; i < set->lease_count; i++) {
        const wl_columnar_relation_mutation_lease_t *same = &set->leases[i];
        const wl_columnar_relation_mutation_role_t *role = &set->roles[i];
        if (same == lease) {
            if (role->relation != relation
                || role->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
                || same->role_flags
                != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION)
                return false;
            lease_member = true;
        }
        if (role->relation != relation && same->relation != relation)
            continue;
        if (role->relation != relation || same->relation != relation
            || same->set != set || same->identity != (uintptr_t)same
            || same->role_flags != role->role_flags)
            return false;
        if (role->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION)
            continue;
        if (same->owner != sequence->owner
            || same->relation_identity != sequence->relation_identity
            || same->owner_identity != sequence->owner_identity
            || same->relation_generation != sequence->relation_generation
            || same->owner_generation != sequence->owner_generation
            || same->descriptor_slot != sequence->descriptor_slot
            || same->owner_slot != sequence->owner_slot
            || same->self_owner_slot != sequence->self_owner_slot
            || same->detached != sequence->detached_before
            || same->storage_transitioned
            != sequence->storage_transitioned_before)
            return false;
    }
    bool grid_swapped = (sequence->observed_events
        & WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP) != 0;
    bool timestamps_retired = (sequence->observed_events
        & WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE) != 0;
    return lease_member
           && relation->relation_identity == sequence->relation_identity
           && relation->storage_owner == sequence->owner
           && relation->storage_owner_identity == sequence->owner_identity
           && (!validate_postconditions
           || relation->storage_generation == observed_generation)
           && relation->storage_owner_generation
           == (sequence->owner == relation ? observed_generation
                                            : sequence->owner_generation)
           && sequence->owner->relation_identity == sequence->owner_identity
           && sequence->owner->storage_generation
           == (sequence->owner == relation ? observed_generation
                                            : sequence->owner_generation)
           && (!validate_postconditions || (grid_swapped
            ? (relation->columns == sequence->merge_columns_before
           && relation->capacity == sequence->merge_capacity_before
           && relation->merge_columns == sequence->columns_before
           && relation->merge_buf_cap == sequence->capacity_before)
            : (relation->columns == sequence->columns_before
           && relation->capacity == sequence->capacity_before
           && relation->merge_columns
           == sequence->merge_columns_before
           && relation->merge_buf_cap
           == sequence->merge_capacity_before)))
           && (!validate_postconditions || (timestamps_retired
            ? (relation->timestamps == NULL
           && relation->timestamp_capacity == 0)
            : (relation->timestamps == sequence->timestamps_before
           && relation->timestamp_capacity
           == sequence->timestamp_capacity_before)));
}

int
wl_columnar_relation_terminal_sequence_begin(
    wl_columnar_relation_terminal_sequence_t *sequence,
    wl_columnar_relation_mutation_lease_t *lease, uint32_t expected_events)
{
    const uint32_t allowed = WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP
        | WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE;
    if (!sequence || sequence->identity || sequence->armed
        || sequence->consumed || !lease || (expected_events & ~allowed)
        || !expected_events
        || col_rel_mutation_lease_validate(lease, lease->relation) != 0)
        return EINVAL;
    col_rel_t *relation = lease->relation;
    bool retires_timestamps = (expected_events
        & WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE) != 0;
    if (retires_timestamps != (relation->timestamps != NULL)
        || (retires_timestamps && (relation->timestamp_capacity == 0
        || !col_rel_timestamp_shape_valid(relation)))
        || ((expected_events & WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP)
        && ((relation->ncols && (!relation->columns
        || !relation->merge_columns))
        || (lease->owner != relation && relation->ncols != 0)
        || relation->merge_buf_cap == 0)))
        return EINVAL;
    uint32_t steps = wl_columnar_relation_terminal_event_count(
        expected_events);
    if (steps == 0
        || relation->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - (uint64_t)steps)
        return EOVERFLOW;
    if (lease->set->terminal_sequence
        || lease->set->terminal_sequence_consumed)
        return EINVAL;
    if (lease->set->terminal_expected_events
        || lease->set->terminal_observed_events)
        return EINVAL;
    if (lease->owner_slot >= lease->set->owner_count
        || lease->self_owner_slot >= lease->set->owner_count)
        return EINVAL;
    memset(sequence, 0, sizeof(*sequence));
    sequence->identity = (uintptr_t)sequence;
    sequence->set = lease->set;
    sequence->lease = lease;
    sequence->relation = relation;
    sequence->owner = lease->owner;
    sequence->descriptor_slot = lease->descriptor_slot;
    sequence->owner_slot = lease->owner_slot;
    sequence->self_owner_slot = lease->self_owner_slot;
    sequence->descriptor_writer_identity =
        lease->set->descriptors[lease->descriptor_slot].writer.identity;
    sequence->owner_writer_identity =
        lease->set->owners[lease->owner_slot].writer.identity;
    sequence->self_writer_identity =
        lease->set->owners[lease->self_owner_slot].writer.identity;
    sequence->relation_identity = lease->relation_identity;
    sequence->owner_identity = lease->owner_identity;
    sequence->relation_generation = lease->relation_generation;
    sequence->owner_generation = lease->owner_generation;
    sequence->columns_before = relation->columns;
    sequence->merge_columns_before = relation->merge_columns;
    sequence->timestamps_before = relation->timestamps;
    sequence->capacity_before = relation->capacity;
    sequence->merge_capacity_before = relation->merge_buf_cap;
    sequence->timestamp_capacity_before = relation->timestamp_capacity;
    sequence->expected_events = expected_events;
    sequence->detached_before = lease->detached;
    sequence->storage_transitioned_before = lease->storage_transitioned;
    sequence->armed = true;
    lease->set->terminal_sequence = sequence;
    lease->set->terminal_expected_events = expected_events;
    lease->set->terminal_observed_events = 0;
    if (!wl_columnar_relation_terminal_tokens_valid(sequence, true)) {
        lease->set->terminal_sequence = NULL;
        lease->set->terminal_expected_events = 0;
        lease->set->terminal_observed_events = 0;
        memset(sequence, 0, sizeof(*sequence));
        return EINVAL;
    }
    lease->set->published = true;
    return 0;
}

static void
wl_columnar_relation_terminal_publish_event(
    wl_columnar_relation_terminal_sequence_t *sequence, uint32_t event)
{
    if (!wl_columnar_relation_terminal_tokens_valid(sequence, false)
        || !sequence->set->published
        || (event != WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP
        && event != WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE)
        || !(sequence->expected_events & event)
        || (sequence->observed_events & event))
        abort();
    col_rel_t *relation = sequence->relation;
    if (event == WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP) {
        if (relation->columns != sequence->merge_columns_before
            || relation->capacity != sequence->merge_capacity_before
            || relation->merge_columns != sequence->columns_before
            || relation->merge_buf_cap != sequence->capacity_before)
            abort();
    } else if (relation->timestamps != NULL
        || relation->timestamp_capacity != 0
        || sequence->timestamps_before == NULL
        || sequence->timestamp_capacity_before == 0) {
        abort();
    }
    wl_columnar_relation_touch_storage(relation);
    sequence->observed_events |= event;
    sequence->set->terminal_observed_events |= event;
}

#ifdef WL_TEST_MUTATION_SET_HOOK
bool
wl_columnar_relation_test_terminal_sequence_validate(
    const wl_columnar_relation_terminal_sequence_t *sequence)
{
    return wl_columnar_relation_terminal_tokens_valid(sequence, true);
}
#endif

void
wl_columnar_relation_terminal_sequence_publish_grid_swap(
    wl_columnar_relation_terminal_sequence_t *sequence)
{
    wl_columnar_relation_terminal_publish_event(sequence,
        WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP);
}

void
wl_columnar_relation_terminal_sequence_publish_timestamp_retirement(
    wl_columnar_relation_terminal_sequence_t *sequence)
{
    wl_columnar_relation_terminal_publish_event(sequence,
        WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE);
}

int
wl_columnar_relation_terminal_sequence_finish(
    wl_columnar_relation_terminal_sequence_t *sequence)
{
    if (!wl_columnar_relation_terminal_tokens_valid(sequence, true)
        || sequence->observed_events != sequence->expected_events)
        return EINVAL;
    wl_columnar_relation_mutation_set_t *set = sequence->set;
    col_rel_t *relation = sequence->relation;
    uint32_t steps = wl_columnar_relation_terminal_event_count(
        sequence->expected_events);
    uint64_t final_generation = sequence->relation_generation + steps;
    if (relation->storage_generation != final_generation
        || (sequence->owner == relation
        && (relation->storage_owner != relation
        || relation->storage_owner_identity != relation->relation_identity
        || relation->storage_owner_generation != final_generation))
        || (sequence->owner != relation
        && (relation->storage_owner != sequence->owner
        || relation->storage_owner_identity != sequence->owner_identity
        || relation->storage_owner_generation != sequence->owner_generation
        || ((sequence->expected_events
        & WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP)
        && relation->ncols != 0))))
        return EINVAL;
    for (size_t i = 0; i < set->lease_count; i++) {
        wl_columnar_relation_mutation_lease_t *same = &set->leases[i];
        if (set->roles[i].relation != relation
            || set->roles[i].role_flags
            != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION)
            continue;
        same->relation_generation = final_generation;
        if (sequence->owner == relation)
            same->owner_generation = final_generation;
    }
    sequence->consumed = true;
    sequence->armed = false;
    set->terminal_sequence = NULL;
    set->terminal_expected_events = 0;
    set->terminal_observed_events = 0;
    set->terminal_sequence_consumed = true;
    set->published = true;
    return 0;
}

int
col_rel_storage_alias_release_locked(col_rel_t *alias,
    wl_columnar_relation_mutation_lease_t *lease)
{
    int rc = col_rel_mutation_lease_validate(lease, alias);
    if (rc)
        return rc;
    col_rel_t *owner = lease->owner;
    if (alias == owner)
        return 0;
    if (!col_rel_storage_alias_borrow_count(owner)
        || col_rel_storage_alias_borrow_count(alias))
        return EINVAL;
    alias->storage_owner = alias;
    alias->storage_owner_identity = alias->relation_identity;
    alias->storage_owner_generation = alias->storage_generation;
    lease->detached = true;
    /* This is deliberately the final access to owner. Our still-live borrow
     * prevents teardown before the decrement; a peer reader owns its own gate. */
    uint64_t prior = atomic_fetch_sub_explicit(&owner->storage_alias_borrows,
            1, memory_order_acq_rel);
    /* A zero prior value means an invariant was violated outside this
     * descriptor's authority. Do not continue with a wrapped borrow count. */
    if (!prior)
        abort();
    return 0;
}

int
col_rel_replacement_cohort_validate(
    const col_rel_t *destination,
    const wl_columnar_source_access_cohort_t *cohort,
    col_rel_t *const *borrower_refs, size_t borrower_count)
{
    col_rel_t *owner;
    size_t aliases = 0;
    int rc = wl_columnar_source_access_cohort_validate(cohort);

    if (rc != 0)
        return rc;
    if (!destination || !borrower_refs || borrower_count != cohort->reader_count
        || col_rel_storage_owner_resolve(destination, &owner) != 0
        || owner != destination || destination->col_shared
        || cohort->source != &destination->source_access
        || destination->storage_owner_generation
        != destination->storage_generation)
        return EINVAL;
    for (size_t i = 0; i < borrower_count; i++) {
        const col_rel_t *borrower = borrower_refs[i];
        bool unique = true;
        if (!borrower
            || cohort->reader_refs[i]->secondary_owner
            != &borrower->descriptor_access
            || col_rel_storage_owner_resolve(borrower, &owner) != 0
            || owner != destination
            || borrower->storage_owner_identity
            != destination->relation_identity
            || borrower->storage_owner_generation
            != destination->storage_generation
            || borrower->ncols != destination->ncols
            || borrower->nrows != destination->nrows
            || borrower->capacity != destination->capacity
            || (borrower->ncols
            && (!borrower->columns || !destination->columns)))
            return EINVAL;
        if (borrower == destination)
            continue;
        if (!borrower->col_shared
            || col_rel_storage_alias_borrow_count(borrower) != 0)
            return EINVAL;
        for (uint32_t c = 0; c < borrower->ncols; c++)
            if (!borrower->col_shared[c]
                || borrower->columns[c] != destination->columns[c])
                return EINVAL;
        for (size_t j = 0; j < i; j++)
            if (borrower_refs[j] == borrower)
                unique = false;
        if (unique)
            aliases++;
    }
    uint64_t live_aliases = col_rel_storage_alias_borrow_count(destination);
    if (aliases > live_aliases)
        return EINVAL;
    return aliases == live_aliases ? 0 : EBUSY;
}

int
col_rel_storage_alias_release(col_rel_t *alias)
{
    col_rel_t *owner;
    if (!alias || !alias->storage_owner)
        return 0;
    if (alias->storage_owner == alias)
        return 0;
    owner = alias->storage_owner;
    if (owner->storage_owner != owner
        || owner->storage_owner_identity != owner->relation_identity
        || col_rel_storage_alias_borrow_count(owner) == 0
        || alias->storage_owner_identity != owner->relation_identity)
        return EINVAL;
    if (col_rel_storage_alias_borrow_release(owner) != 0)
        return EINVAL;
    /* The alias descriptor may be pinned by the caller's operation token.
    * Reset ownership metadata without reinitializing either live gate. */
    alias->storage_owner = alias;
    alias->storage_owner_identity = alias->relation_identity;
    alias->storage_owner_generation = alias->storage_generation;
    atomic_store_explicit(&alias->storage_alias_borrows, 0,
        memory_order_relaxed);
    return 0;
}

int
col_rel_storage_owner_destroy_status(const col_rel_t *owner)
{
    if (!owner || owner->storage_owner != owner
        || owner->storage_owner_identity != owner->relation_identity)
        return EINVAL;
    if (col_rel_storage_alias_borrow_count(owner) != 0
        || wl_columnar_source_access_gate_busy(&owner->source_access))
        return EBUSY;
    return 0;
}

/* Legacy descriptors can predate storage_owner metadata. Initialize that
 * metadata only under descriptor exclusion; never reinitialize their access
 * gates, since a writer sentinel marks terminal teardown. */
int
col_rel_storage_owner_ensure_initialized(col_rel_t *rel)
{
    wl_columnar_source_access_writer_t descriptor_writer = { 0 };
    col_rel_t *owner = NULL;
    bool initialized;
    int rc;

    if (!rel)
        return EINVAL;
    rc = wl_columnar_source_access_gate_reader_acquire(
        &rel->descriptor_access);
    if (rc != 0)
        return rc;
    initialized = rel->storage_owner != NULL;
    rc = wl_columnar_source_access_gate_reader_release(
        &rel->descriptor_access);
    if (rc != 0 || initialized)
        return rc;

    rc = wl_columnar_source_access_writer_acquire(&rel->descriptor_access,
            &descriptor_writer);
    if (rc != 0)
        return rc;
    if (!rel->storage_owner) {
        if (rel->relation_identity == 0
            || !wl_columnar_relation_generation_valid(
                rel->storage_generation)) {
            rc = EINVAL;
        } else {
            rel->storage_owner = rel;
            rel->storage_owner_identity = rel->relation_identity;
            rel->storage_owner_generation = rel->storage_generation;
            rc = 0;
        }
    } else {
        rc = col_rel_storage_owner_resolve(rel, &owner);
    }
    if (wl_columnar_source_access_writer_release(&descriptor_writer) != 0
        && rc == 0)
        rc = EINVAL;
    return rc;
}

int
col_rel_source_reader_acquire(const col_rel_t *rel,
    wl_columnar_source_access_reader_t *token)
{
    col_rel_t *owner = NULL;
    int rc;
    if (!rel || !token)
        return EINVAL;
    rc = col_rel_storage_owner_ensure_initialized((col_rel_t *)rel);
    if (rc != 0)
        return rc;
    rc = wl_columnar_source_access_gate_reader_acquire(
        (wl_columnar_source_access_gate_t *)&rel->descriptor_access);
    if (rc != 0)
        return rc;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_reader_after_descriptor)
        wl_columnar_relation_test_reader_after_descriptor(rel);
#endif
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc != 0) {
        (void)wl_columnar_source_access_gate_reader_release(
            (wl_columnar_source_access_gate_t *)&rel->descriptor_access);
        return rc;
    }
    rc = wl_columnar_source_access_reader_acquire(&owner->source_access,
            token);
    if (rc != 0) {
        (void)wl_columnar_source_access_gate_reader_release(
            (wl_columnar_source_access_gate_t *)&rel->descriptor_access);
    } else {
        token->secondary_owner
            = (wl_columnar_source_access_gate_t *)&rel->descriptor_access;
    }
    return rc;
}

int
col_rel_source_reader_acquire_transferable(const col_rel_t *rel,
    wl_columnar_source_access_reader_t *token)
{
    col_rel_t *owner = NULL;
    int rc;
    if (!rel || !token)
        return EINVAL;
    rc = col_rel_storage_owner_ensure_initialized((col_rel_t *)rel);
    if (rc != 0)
        return rc;
    rc = wl_columnar_source_access_gate_reader_acquire(
        (wl_columnar_source_access_gate_t *)&rel->descriptor_access);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc != 0) {
        (void)wl_columnar_source_access_gate_reader_release(
            (wl_columnar_source_access_gate_t *)&rel->descriptor_access);
        return rc;
    }
    rc = wl_columnar_source_access_reader_acquire_transferable(
        &owner->source_access, token);
    if (rc != 0) {
        (void)wl_columnar_source_access_gate_reader_release(
            (wl_columnar_source_access_gate_t *)&rel->descriptor_access);
    } else {
        token->secondary_owner
            = (wl_columnar_source_access_gate_t *)&rel->descriptor_access;
    }
    return rc;
}

int
col_rel_source_reader_release(wl_columnar_source_access_reader_t *token)
{
    return wl_columnar_source_access_reader_release(token);
}

int
col_rel_source_writer_acquire(const col_rel_t *rel,
    wl_columnar_source_access_writer_t *token)
{
    col_rel_t *owner = NULL;
    int rc;

    if (!rel || !token)
        return EINVAL;
    rc = wl_columnar_source_access_gate_reader_acquire(
        (wl_columnar_source_access_gate_t *)&rel->descriptor_access);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc != 0) {
        (void)wl_columnar_source_access_gate_reader_release(
            (wl_columnar_source_access_gate_t *)&rel->descriptor_access);
        return rc;
    }
    rc = wl_columnar_source_access_writer_acquire(&owner->source_access,
            token);
    if (rc != 0) {
        (void)wl_columnar_source_access_gate_reader_release(
            (wl_columnar_source_access_gate_t *)&rel->descriptor_access);
        return rc;
    }
    token->secondary_owner
        = (wl_columnar_source_access_gate_t *)&rel->descriptor_access;
    return 0;
}

/* Metadata/type publication rewrites the canonical relation descriptor and,
 * for types, may reinterpret existing row lanes.  A live storage alias must
 * therefore be denied rather than allowed to observe a mixed descriptor. */
static int
col_rel_published_writer_acquire(const col_rel_t *rel,
    wl_columnar_source_access_writer_t *token)
{
    col_rel_t *owner = NULL;
    int rc;

    if (!rel || !token)
        return EINVAL;
    rc = col_rel_source_writer_acquire(rel, token);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc != 0) {
        (void)wl_columnar_source_access_writer_release(token);
        return rc;
    }
    if (col_rel_storage_alias_borrow_count(owner) != 0) {
        (void)wl_columnar_source_access_writer_release(token);
        return EBUSY;
    }
    return 0;
}

int
col_rel_set(col_rel_t *r, uint32_t row, uint32_t col, int64_t val)
{
    wl_columnar_relation_mutation_single_t single = { 0 };
    int rc;
    if (!r)
        return EINVAL;
    rc = col_rel_mutation_single_acquire(r, &single);
    if (rc)
        return rc;
    if (!r->columns || row >= r->capacity || col >= r->ncols
        || !r->columns[col]) {
        rc = EINVAL;
        goto finish;
    }
    if (r->column_types && r->column_types[col] == WIRELOG_TYPE_FLOAT) {
        if (!wl_columnar_float_bits_valid(val)) {
            rc = EINVAL;
            goto finish;
        }
        if (wl_columnar_float_bits_zero(val))
            val = 0;
    }
    bool borrowed = r->col_shared != NULL;
    if (borrowed) {
        rc = col_rel_cow_unshare_publish_impl(r, 0, false, &single.lease);
        if (rc)
            goto finish;
    }
#ifdef WL_TEST_SET_HOOK
    if (borrowed && wl_columnar_set_transition_hook)
        wl_columnar_set_transition_hook(r);
#endif
    r->columns[col][row] = val;
    wl_columnar_relation_touch_view(r);
    rc = 0;
finish:
    {
        int finish_rc = col_rel_mutation_set_finish(&single.set, rc == 0);
        return rc ? rc : finish_rc;
    }
}

/* Prepare a complete private replacement for a relation resize.  This is
 * deliberately independent of the current ownership mode: a shared or arena
 * relation is copied into heap storage and is not changed until the caller
 * reaches its publication point. */
static int
col_rel_prepare_resize(const col_rel_t *r, uint32_t new_cap,
    int64_t ***out_columns, col_delta_timestamp_t **out_timestamps)
{
    int64_t **columns = NULL;
    col_delta_timestamp_t *timestamps = NULL;

#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_prepare_resize) {
        wl_columnar_relation_test_fail_prepare_resize = false;
        return ENOMEM;
    }
#endif

    if (!r || !out_columns || !out_timestamps)
        return EINVAL;
    if (r->ncols > 0) {
        columns = col_columns_alloc(r->ncols, new_cap);
        if (!columns)
            return ENOMEM;
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->columns && r->columns[c] && r->nrows > 0)
                memcpy(columns[c], r->columns[c],
                    (size_t)r->nrows * sizeof(int64_t));
        }
    }
    if (r->timestamps) {
        timestamps = (col_delta_timestamp_t *)malloc(
            (size_t)new_cap * sizeof(*timestamps));
        if (!timestamps) {
            col_columns_free(columns, r->ncols);
            return ENOMEM;
        }
        if (r->nrows > 0)
            memcpy(timestamps, r->timestamps,
                (size_t)r->nrows * sizeof(*timestamps));
        if (new_cap > r->nrows)
            memset(timestamps + r->nrows, 0,
                (size_t)(new_cap - r->nrows) * sizeof(*timestamps));
    }
    *out_columns = columns;
    *out_timestamps = timestamps;
    return 0;
}

static void
col_rel_release_column_storage(col_rel_t *r)
{
    if (!r || !r->columns)
        return;
    if (r->col_shared) {
        for (uint32_t c = 0; c < r->ncols; c++)
            if (!r->col_shared[c])
                free(r->columns[c]);
        free((void *)r->columns);
        free(r->col_shared);
    } else if (r->arena_owned) {
        free((void *)r->columns);
    } else {
        col_columns_free(r->columns, r->ncols);
    }
    r->columns = NULL;
    r->col_shared = NULL;
}

static void
col_rel_publish_resize(col_rel_t *r, int64_t **columns,
    col_delta_timestamp_t *timestamps, uint32_t new_cap)
{
    col_rel_release_column_storage(r);
    free(r->timestamps);
    r->columns = columns;
    r->timestamps = timestamps;
    r->timestamp_capacity = timestamps ? new_cap : 0;
    r->capacity = new_cap;
    r->arena_owned = false;
}

uint64_t
col_rel_owned_ledger_bytes(const col_rel_t *r)
{
    if (!r || r->arena_owned || !r->columns || r->capacity == 0)
        return 0;
    uint64_t owned_cols = 0;
    int64_t **columns = r->columns;
    bool *shared = r->col_shared;
    for (uint32_t c = 0; c < r->ncols; c++, columns++) {
        /* columns/shared are allocated together for exactly r->ncols slots. */
        if ((!shared || !shared[c]) && *columns) /* NOLINT(clang-analyzer-security.ArrayBound) */
            owned_cols++;
    }
    if (owned_cols == 0)
        return 0;
    if (owned_cols > UINT64_MAX / r->capacity / sizeof(int64_t))
        return UINT64_MAX;
    return owned_cols * r->capacity * sizeof(int64_t);
}

uint64_t
col_rel_timestamp_ledger_bytes(const col_rel_t *r)
{
    if (!r || !r->timestamps || r->timestamp_capacity == 0)
        return 0;
    /* capacity is uint32_t, so the product cannot overflow uint64_t. */
    return (uint64_t)r->timestamp_capacity * sizeof(col_delta_timestamp_t);
}

uint64_t
col_rel_transport_bytes(const col_rel_t *r)
{
    uint64_t bytes;
    return col_rel_retained_live_bytes(r, &bytes) ? bytes : UINT64_MAX;
}

/* Count prospective or live heap buffers with independent physical widths.
* A nonzero timestamp width represents an allocated timestamps[] buffer. */
static bool
col_rel_retained_shape_bytes(uint32_t owned_columns,
    uint32_t column_capacity, uint32_t timestamp_capacity, uint64_t *out)
{
    uint64_t columns;
    uint64_t timestamps;

    if (!out || !wl_columnar_memory_size_mul(owned_columns,
        column_capacity, &columns)
        || !wl_columnar_memory_size_mul(columns, sizeof(int64_t), &columns)
        || !wl_columnar_memory_size_mul(timestamp_capacity,
        sizeof(col_delta_timestamp_t), &timestamps)
        || !wl_columnar_memory_size_add(columns, timestamps, out))
        return false;
    return true;
}

/* Exact relation-owned payload.  Shared columns[] and col_shared[] tables
 * belong to metadata; the ordinary and persistent grid pointer tables belong
 * here. col_columns_alloc() allocates one lane even at zero row capacity. */
typedef struct {
    uint32_t ncols;
    uint32_t owned_columns;
    uint32_t column_capacity;
    uint32_t timestamp_capacity;
    uint32_t merge_capacity;
    uint32_t backup_capacity;
    uint32_t backup_timestamp_capacity;
    bool active_table;
    bool merge_table;
    bool backup_table;
    bool scratch;
} col_rel_payload_shape_t;

static bool
col_rel_payload_grid_bytes(uint32_t ncols, uint32_t owned_columns,
    uint32_t capacity, bool table, uint64_t *out)
{
    uint64_t lanes, pointers = 0;
    if (!out || owned_columns > ncols
        || !wl_columnar_memory_size_mul(owned_columns,
        capacity ? capacity : 1u, &lanes)
        || !wl_columnar_memory_size_mul(lanes, sizeof(int64_t), &lanes)
        || (table && !wl_columnar_memory_size_mul(ncols,
        sizeof(int64_t *), &pointers))
        || !wl_columnar_memory_size_add(lanes, pointers, out))
        return false;
    return true;
}

static bool
col_rel_payload_shape_bytes(const col_rel_payload_shape_t *shape,
    uint64_t *out)
{
    uint64_t active, merge, backup, timestamps, backup_timestamps;
    uint64_t scratch = 0, sum;
    if (!shape || !out
        || !col_rel_payload_grid_bytes(shape->ncols,
        shape->owned_columns, shape->column_capacity,
        shape->active_table, &active)
        || !col_rel_payload_grid_bytes(shape->ncols,
        shape->merge_table ? shape->ncols : 0,
        shape->merge_capacity, shape->merge_table, &merge)
        || !col_rel_payload_grid_bytes(shape->ncols,
        shape->backup_table ? shape->ncols : 0,
        shape->backup_capacity, shape->backup_table, &backup)
        || !wl_columnar_memory_size_mul(shape->timestamp_capacity,
        sizeof(col_delta_timestamp_t), &timestamps)
        || !wl_columnar_memory_size_mul(shape->backup_timestamp_capacity,
        sizeof(col_delta_timestamp_t), &backup_timestamps)
        || (shape->scratch && !wl_columnar_memory_size_mul(shape->ncols,
        sizeof(int64_t), &scratch))
        || !wl_columnar_memory_size_add(active, merge, &sum)
        || !wl_columnar_memory_size_add(sum, backup, &sum)
        || !wl_columnar_memory_size_add(sum, timestamps, &sum)
        || !wl_columnar_memory_size_add(sum, backup_timestamps, &sum)
        || !wl_columnar_memory_size_add(sum, scratch, out))
        return false;
    return true;
}

static bool
col_rel_payload_live_shape(const col_rel_t *r, col_rel_payload_shape_t *shape)
{
    if (!r || !shape || (r->columns && r->columns == r->merge_columns)
        || (r->columns && r->columns == r->retract_backup_columns)
        || (r->merge_columns
        && r->merge_columns == r->retract_backup_columns)
        || (r->merge_columns && r->ncols == 0)
        || (r->retract_backup_columns && r->ncols == 0))
        return false;
    *shape = (col_rel_payload_shape_t){
        .ncols = r->ncols,
        .column_capacity = r->capacity,
        .timestamp_capacity = r->timestamp_capacity,
        .merge_capacity = r->merge_buf_cap,
        .backup_capacity = r->retract_backup_capacity,
        .backup_timestamp_capacity = r->retract_backup_timestamp_capacity,
        .active_table = r->columns && !r->col_shared,
        .merge_table = r->merge_columns != NULL,
        .backup_table = r->retract_backup_columns != NULL,
        .scratch = r->row_scratch != NULL,
    };
    if (r->columns && !r->arena_owned)
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (!r->columns[c] && (!r->col_shared || !r->col_shared[c]
                || r->capacity != 0 || r->nrows != 0))
                return false;
            if (!r->col_shared || !r->col_shared[c])
                shape->owned_columns++;
        }
    return true;
}

/* Prospective private columns while the existing merge/backup/scratch image
 * remains resident. Callers dropping an auxiliary buffer clear its fields in
 * the shape before evaluating it. */
static bool
col_rel_payload_private_bytes(const col_rel_t *r, uint32_t ncols,
    uint32_t capacity, uint32_t timestamp_capacity, bool scratch,
    uint64_t *out)
{
    col_rel_payload_shape_t shape;
    if (!col_rel_payload_live_shape(r, &shape))
        return false;
    shape.ncols = ncols;
    shape.owned_columns = ncols;
    shape.column_capacity = capacity;
    shape.timestamp_capacity = timestamp_capacity;
    shape.active_table = ncols != 0;
    shape.scratch = scratch;
    return col_rel_payload_shape_bytes(&shape, out);
}

static bool
col_rel_timestamp_shape_valid(const col_rel_t *r)
{
    return r && (r->timestamps != NULL) == (r->timestamp_capacity != 0)
           && (!r->timestamps || r->timestamp_capacity >= r->nrows)
           && (r->retract_backup_timestamps != NULL)
           == (r->retract_backup_timestamp_capacity != 0)
           && (!r->retract_backup_timestamps
           || r->retract_backup_timestamp_capacity
           >= r->retract_backup_nrows);
}

bool
col_rel_retained_live_bytes(const col_rel_t *r, uint64_t *out)
{
    col_rel_payload_shape_t shape;
    if (!r || !out || !col_rel_timestamp_shape_valid(r)
        || (r->ncols && r->capacity && !r->columns)
        || !col_rel_payload_live_shape(r, &shape))
        return false;
    return col_rel_payload_shape_bytes(&shape, out);
}

static void
wl_columnar_relation_retire_payload_credit_known(col_rel_t *r, uint64_t exact)
{
    if (!r || !r->memory_governor)
        return;
    if (atomic_load_explicit(&r->retained_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING)
        return;
    if (exact > r->retained_reserved_bytes)
        abort();
    if (exact == r->retained_reserved_bytes)
        return;
    if (exact == 0) {
        if (!wl_columnar_memory_release(&r->retained_reservation))
            abort();
        wl_columnar_memory_reservation_init(&r->retained_reservation);
    } else if (!wl_columnar_memory_reservation_downsize(
            &r->retained_reservation, exact))
        abort();
    r->retained_reserved_bytes = exact;
}

void
col_rel_retire_payload_credit(col_rel_t *r)
{
    uint64_t exact;
    if (!r || !r->memory_governor)
        return;
    if (atomic_load_explicit(&r->retained_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING)
        return; /* A failed replacement retains both images for retry. */
    if (!col_rel_retained_live_bytes(r, &exact))
        abort();
    wl_columnar_relation_retire_payload_credit_known(r, exact);
}

int
col_rel_reserve_merge_grid(col_rel_t *r, uint32_t capacity)
{
    int64_t **prepared, **old;
    uint64_t incoming = 0, exact;
    bool growth = false;
    if (!r || (r->merge_columns && r->merge_buf_cap == 0))
        return EINVAL;
    if (r->merge_columns && r->merge_buf_cap >= capacity)
        return 0;
    if (r->ncols == 0) {
        r->merge_buf_cap = capacity;
        return 0;
    }
    if (!col_rel_payload_grid_bytes(r->ncols, r->ncols, capacity,
        true, &incoming) || incoming > SIZE_MAX)
        return EOVERFLOW;
    if (r->memory_governor) {
        wl_columnar_memory_admission_status_t status;
        if (!r->retained_reserved_bytes)
            return EINVAL;
        status = wl_columnar_memory_begin_growth(
            &r->retained_reservation, incoming);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                r->memory_budget_denial_pending = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOMEM
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
        }
        growth = true;
    }
    prepared = col_columns_alloc(r->ncols, capacity);
    if (!prepared) {
        if (growth && !wl_columnar_memory_rollback_growth(
                &r->retained_reservation))
            abort();
        return ENOMEM;
    }
    if (r->merge_columns) {
        size_t copied = (size_t)(r->merge_buf_cap < capacity
            ? r->merge_buf_cap : capacity) * sizeof(int64_t);
        for (uint32_t c = 0; c < r->ncols; c++)
            memcpy(prepared[c], r->merge_columns[c], copied);
    }
    if (growth && !wl_columnar_memory_commit_growth(
            &r->retained_reservation)) {
        col_columns_free(prepared, r->ncols);
        if (!wl_columnar_memory_rollback_growth(&r->retained_reservation))
            abort();
        return EINVAL;
    }
    old = r->merge_columns;
    r->merge_columns = prepared;
    r->merge_buf_cap = capacity;
    col_columns_free(old, r->ncols);
    if (growth) {
        if (!col_rel_retained_live_bytes(r, &exact)
            || exact > r->retained_reservation.bytes
            || (exact < r->retained_reservation.bytes
            && !wl_columnar_memory_reservation_downsize(
                &r->retained_reservation, exact)))
            abort();
        r->retained_reserved_bytes = exact;
    }
    return 0;
}

int
col_rel_reserve_merge_grid_with_lease(col_rel_t *r, uint32_t capacity,
    wl_columnar_relation_mutation_lease_t *lease)
{
    if (!r || !lease || lease->role_flags
        != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || col_rel_mutation_lease_validate(lease, r))
        return EINVAL;
    return col_rel_reserve_merge_grid(r, capacity);
}

static void
col_rel_release_reservation_or_abort(
    wl_columnar_memory_reservation_t *reservation)
{
    if (reservation && reservation->bytes != 0
        && !wl_columnar_memory_release(reservation))
        abort();
}

static void
col_rel_reservation_rollback(wl_columnar_memory_reservation_t *reservation)
{
    if (!reservation)
        return;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_watch_rollback_cleanup) {
        wl_columnar_relation_test_rollback_cleanup_observed
            = wl_columnar_relation_test_resize_columns_retired
            && reservation->bytes != 0
            && atomic_load_explicit(&reservation->state,
                memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED;
        wl_columnar_relation_test_watch_rollback_cleanup = false;
    }
#endif
    col_rel_release_reservation_or_abort(reservation);
}

static bool
col_rel_prospective_metadata_bytes(uint32_t ncols,
    const char *const *names, bool names_owned, bool generated_names,
    bool types, bool schema, uint32_t compound_len, bool shared_tables,
    uint64_t *out)
{
    uint64_t bytes = schema ? 3u : 0u;
    if (!out)
        return false;
    if (ncols > 0) {
        if (names_owned && !wl_columnar_memory_size_add(bytes,
            (uint64_t)ncols * sizeof(char *), &bytes))
            return false;
        if (types && !wl_columnar_memory_size_add(bytes,
            (uint64_t)ncols * sizeof(wirelog_column_type_t), &bytes))
            return false;
        if (schema && !wl_columnar_memory_size_add(bytes,
            (uint64_t)ncols * (sizeof(struct ArrowSchema *)
            + sizeof(struct ArrowSchema) + 2u), &bytes))
            return false;
        for (uint32_t i = 0; i < ncols; i++) {
            char fallback[32];
            const char *name = names ? names[i] : NULL;
            if (!name && generated_names) {
                snprintf(fallback, sizeof(fallback), "col%u", i);
                name = fallback;
            }
            if (names_owned && name
                && !wl_columnar_memory_size_add(bytes,
                strlen(name) + 1u, &bytes))
                return false;
            if (schema && !wl_columnar_memory_size_add(bytes,
                name ? strlen(name) + 1u : 1u, &bytes))
                return false;
        }
    }
    if (!wl_columnar_memory_size_add(bytes,
        (uint64_t)compound_len * sizeof(uint32_t), &bytes))
        return false;
    if (shared_tables && (!wl_columnar_memory_size_add(bytes,
        (uint64_t)ncols * (sizeof(int64_t *) + sizeof(bool)), &bytes)))
        return false;
    *out = bytes;
    return true;
}

static bool
col_rel_compound_map_valid_for(const col_rel_t *r, uint32_t ncols)
{
    uint32_t width = 0;
    if (!r || (r->compound_arity_map == NULL)
        != (r->compound_arity_len == 0)
        || r->compound_arity_len > ncols)
        return false;
    if (r->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        && r->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        && r->compound_kind != WIRELOG_COMPOUND_KIND_SIDE)
        return false;
    if (r->compound_kind == WIRELOG_COMPOUND_KIND_INLINE) {
        if (r->compound_arity_len == 0 || r->compound_count == 0
            || r->compound_count > r->compound_arity_len
            || r->inline_physical_offset >= ncols)
            return false;
    } else if (r->compound_count != 0 || r->inline_physical_offset != 0)
        return false;
    for (uint32_t i = 0; i < r->compound_arity_len; i++) {
        uint32_t arity = r->compound_arity_map[i];
        if (arity == 0 || arity > ncols - width)
            return false;
        width += arity;
    }
    return r->compound_arity_len == 0
        ? r->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        : width == ncols;
}

bool
wl_columnar_relation_compound_map_valid(const col_rel_t *r)
{
    return r && col_rel_compound_map_valid_for(r, r->ncols);
}

static bool
col_rel_current_metadata_bytes(const col_rel_t *r, uint64_t *out)
{
    if (!r || !out || !wl_columnar_relation_compound_map_valid(r)
        || (r->col_shared && (!r->columns || r->ncols == 0)))
        return false;
    /* Wirelog builds flat struct schemas. Reject an externally changed tree
    * rather than admitting bytes the flat-shape formula cannot describe. */
    if (r->schema_ok) {
        const struct ArrowSchema *schema = &r->schema;
        if (!schema->release || !schema->format
            || strcmp(schema->format, "+s") != 0
            || schema->name || schema->metadata || schema->dictionary
            || schema->private_data || schema->n_children != r->ncols
            || (r->ncols > 0 && !schema->children))
            return false;
        for (uint32_t i = 0; i < r->ncols; i++) {
            const struct ArrowSchema *child = schema->children[i];
            const char *name = r->col_names && r->col_names[i]
                ? r->col_names[i] : "";
            const char *format = r->column_types
                && r->column_types[i] == WIRELOG_TYPE_FLOAT ? "g" : "l";
            if (!child || !child->release || !child->format
                || strcmp(child->format, format) != 0 || !child->name
                || strcmp(child->name, name) != 0 || child->metadata
                || child->n_children || child->children || child->dictionary
                || child->private_data)
                return false;
        }
    }
    return col_rel_prospective_metadata_bytes(r->ncols,
               (const char *const *)r->col_names, r->col_names != NULL, false,
               r->column_types != NULL, r->schema_ok, r->compound_arity_len,
               r->col_shared != NULL,
               out);
}

/* A shared view owns every copied name, including an allocated empty string
 * for each absent source name. Its borrowed column pointers and sharing flags
 * are also heap-owned metadata, while the borrowed column data is not. */
static bool
col_rel_shared_view_metadata_bytes(const col_rel_t *src, uint64_t *out)
{
    uint64_t bytes;
    if (!src || !out || !col_rel_prospective_metadata_bytes(src->ncols,
        (const char *const *)src->col_names, src->ncols > 0, false,
        src->column_types != NULL, true, src->compound_arity_len,
        src->ncols > 0, &bytes))
        return false;
    for (uint32_t c = 0; c < src->ncols; c++)
        if ((!src->col_names || !src->col_names[c])
            && !wl_columnar_memory_size_add(bytes, 1u, &bytes))
            return false;
    *out = bytes;
    return true;
}

static int
col_rel_reserve_metadata(col_rel_t *r, uint64_t bytes,
    wl_columnar_memory_reservation_t *pending)
{
    wl_columnar_memory_admission_status_t status;
    wl_columnar_memory_reservation_init(pending);
    if (!r->memory_governor || bytes == 0)
        return 0;
    status = wl_columnar_memory_reserve_checked(
        wl_columnar_memory_governor_ref_get(r->memory_governor), bytes,
        pending);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return 0;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
        r->memory_budget_denial_pending = true;
    int rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
        : EINVAL;
    wl_columnar_memory_reservation_init(pending);
    return rc;
}

static void
col_rel_publish_metadata(col_rel_t *r,
    wl_columnar_memory_reservation_t *pending, uint64_t bytes,
    wl_columnar_memory_reservation_t *old)
{
    wl_columnar_memory_reservation_init(old);
    if (!r->memory_governor)
        return;
    if (r->metadata_reserved_bytes
        && !wl_columnar_memory_reservation_move(old,
        &r->metadata_reservation))
        abort();
    if (bytes && (!wl_columnar_memory_reservation_move(
            &r->metadata_reservation, pending)
        || !wl_columnar_memory_commit(&r->metadata_reservation, r)))
        abort();
    r->metadata_reserved_bytes = bytes;
}

/* The shared pointer and flag tables are part of the unified metadata
 * image. Once COW/resize physically retires them, reduce that image in
 * place. Ordinary owned-column pointer tables belong to payload. */
static void
col_rel_retire_shared_table_credit(col_rel_t *r)
{
    uint64_t tables;
    uint64_t remaining;
    if (!r || !r->memory_governor || r->metadata_reserved_bytes == 0)
        return;
    if (r->col_shared || !wl_columnar_memory_size_mul(r->ncols,
        sizeof(int64_t *) + sizeof(bool), &tables)
        || tables == 0 || tables > r->metadata_reserved_bytes)
        abort();
    remaining = r->metadata_reserved_bytes - tables;
    if (remaining) {
        if (!wl_columnar_memory_reservation_downsize(
                &r->metadata_reservation, remaining))
            abort();
    } else if (!wl_columnar_memory_release(&r->metadata_reservation))
        abort();
    r->metadata_reserved_bytes = remaining;
}

typedef wl_columnar_relation_payload_transaction_t col_rel_payload_txn_t;

static void
col_rel_payload_txn_init(col_rel_payload_txn_t *txn)
{
    memset(txn, 0, sizeof(*txn));
    wl_columnar_memory_reservation_init(&txn->fresh);
}

static void
col_rel_payload_txn_rollback(col_rel_t *r, col_rel_payload_txn_t *txn)
{
    if (txn->growth) {
#ifdef WL_TEST_RELATION_RESIZE_HOOK
        if (wl_columnar_relation_test_watch_rollback_cleanup) {
            wl_columnar_relation_test_rollback_cleanup_observed
                = wl_columnar_relation_test_resize_columns_retired
                && txn->incoming_bytes != 0
                && atomic_load_explicit(&r->retained_reservation.state,
                    memory_order_acquire)
                == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING;
            wl_columnar_relation_test_watch_rollback_cleanup = false;
        }
#endif
        if (!wl_columnar_memory_rollback_growth(&r->retained_reservation))
            abort();
        txn->growth = false;
    } else
        col_rel_reservation_rollback(&txn->fresh);
}

/* Private reservation result: 0 needs no token, 1 owns a pending
 * transaction, and a negative errno reports failure. */
static int
col_rel_payload_txn_begin(col_rel_t *r, uint64_t final_bytes,
    uint64_t incoming_bytes, col_rel_payload_txn_t *txn)
{
    wl_columnar_memory_admission_status_t status;
    col_rel_payload_txn_init(txn);
    if (!r) {
        return -EINVAL;
    }
    if (!r->memory_governor)
        return 0;
    if (incoming_bytes == 0 && final_bytes <= r->retained_reserved_bytes)
        return 0;
    if (r->retained_reserved_bytes && incoming_bytes) {
        status = wl_columnar_memory_begin_growth(
            &r->retained_reservation, incoming_bytes);
        if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            txn->growth = true;
            txn->incoming_bytes = incoming_bytes;
            return 1;
        }
    } else if (final_bytes) {
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(r->memory_governor),
            final_bytes, &txn->fresh);
        if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return 1;
    } else
        return 0;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
        r->memory_budget_denial_pending = true;
        return -ENOMEM;
    }
    return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
        ? -EOVERFLOW : -EINVAL;
}

static int wl_columnar_relation_work_tail(wl_evaluation_control_t *, uint64_t);

/* Execution rc and pending ownership are distinct from the legacy 0/1/-errno
 * transaction convention. Any control failure propagates without errno mapping. */
static int
wl_columnar_relation_payload_begin_controlled(col_rel_t *r,
    uint64_t final_bytes,
    uint64_t incoming, col_rel_payload_txn_t *txn, bool *active,
    wl_evaluation_control_t *control)
{
    col_rel_payload_txn_init(txn);
    if (!r)
        return EINVAL;
    *active = false;
    if (!r->memory_governor ||
        (!incoming && final_bytes <= r->retained_reserved_bytes))
        return 0;
    wl_columnar_memory_admission_status_t status;
    int rc;
    if (r->retained_reserved_bytes && incoming) {
        rc = wl_columnar_memory_governor_begin_growth_controlled(
            &r->retained_reservation, incoming, &status, control);
        if (rc != 0)
            return rc;
        if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            txn->growth = true; txn->incoming_bytes = incoming;
            *active = true;
            return 0;
        }
    } else if (final_bytes) {
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(r->memory_governor),
            final_bytes, &txn->fresh, &status, control);
        if (rc != 0)
            return rc;
        if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            *active = true;
            return 0;
        }
    } else
        return 0;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
        r->memory_budget_denial_pending = true;
        return ENOMEM;
    }
    return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

static int
col_rel_reserve_retained_shape(col_rel_t *r, uint32_t capacity,
    uint32_t timestamp_capacity, bool allocate_grid,
    bool allocate_timestamps, bool allocate_scratch,
    col_rel_payload_txn_t *txn)
{
    uint64_t final_bytes, incoming = 0, part;
    if (!r || !txn || (r->arena_owned || r->col_shared)) {
        return -EINVAL;
    }
    if (!col_rel_payload_private_bytes(r, r->ncols, capacity,
        timestamp_capacity, r->ncols != 0, &final_bytes)
        || (allocate_grid && !col_rel_payload_grid_bytes(r->ncols,
        r->ncols, capacity, r->ncols != 0, &incoming))
        || (allocate_timestamps && (!wl_columnar_memory_size_mul(
            timestamp_capacity, sizeof(col_delta_timestamp_t), &part)
        || !wl_columnar_memory_size_add(incoming, part, &incoming)))
        || (allocate_scratch && (!wl_columnar_memory_size_mul(r->ncols,
        sizeof(int64_t), &part)
        || !wl_columnar_memory_size_add(incoming, part, &incoming)))) {
        return -EOVERFLOW;
    }
    return col_rel_payload_txn_begin(r, final_bytes, incoming, txn);
}

static int
col_rel_reserve_retained(col_rel_t *r, uint32_t capacity,
    col_rel_payload_txn_t *txn)
{
    return col_rel_reserve_retained_shape(r, capacity,
               r->timestamps ? capacity : 0, true,
               r->timestamps != NULL, false, txn);
}

static int
col_rel_publish_retained_reservation(col_rel_t *r,
    wl_columnar_memory_reservation_t *pending, uint64_t bytes,
    wl_columnar_memory_reservation_t *previous_out)
{
    uint64_t old_bytes;
    wl_columnar_memory_reservation_t previous;

    if (!r || !pending)
        return EINVAL;
    if (!r->memory_governor || bytes == 0)
        return 0;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_reservation_commit) {
        wl_columnar_relation_test_fail_reservation_commit = false;
        return ENOMEM;
    }
#endif
    if (!wl_columnar_memory_commit(pending, r))
        return ENOMEM;
    old_bytes = r->retained_reserved_bytes;
    wl_columnar_memory_reservation_init(&previous);
    if (old_bytes > 0
        && !wl_columnar_memory_reservation_move(
            &previous, &r->retained_reservation)) {
        return ENOMEM;
    }
    if (!wl_columnar_memory_reservation_move(&r->retained_reservation,
        pending)) {
        if (old_bytes > 0) {
            wl_columnar_memory_reservation_init(&r->retained_reservation);
            if (!wl_columnar_memory_reservation_move(
                    &r->retained_reservation, &previous))
                abort();
        }
        return ENOMEM;
    }
    if (old_bytes > 0) {
        if (previous_out) {
            if (!wl_columnar_memory_reservation_move(previous_out,
                &previous)) {
                if (!wl_columnar_memory_release(
                        &r->retained_reservation)
                    || !wl_columnar_memory_reservation_move(
                        &r->retained_reservation, &previous))
                    abort();
                return ENOMEM;
            }
        } else if (!wl_columnar_memory_release(&previous))
            abort();
    }
    r->retained_reserved_bytes = bytes;
    return 0;
}

static int
col_rel_publish_payload_txn(col_rel_t *r, col_rel_payload_txn_t *txn,
    uint64_t final_bytes, wl_columnar_memory_reservation_t *previous_out)
{
    if (previous_out)
        wl_columnar_memory_reservation_init(previous_out);
    if (!r->memory_governor)
        return 0;
    if (!txn->growth)
        return txn->fresh.bytes
            ? col_rel_publish_retained_reservation(r, &txn->fresh,
                   final_bytes, previous_out) : 0;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_reservation_commit) {
        wl_columnar_relation_test_fail_reservation_commit = false;
        return ENOMEM;
    }
#endif
    if (!wl_columnar_memory_commit_growth(&r->retained_reservation))
        return ENOMEM;
    r->retained_reserved_bytes += txn->incoming_bytes;
    txn->growth = false;
    return 0;
}

/* Canonical token movement cannot be allowed to fail after G's final cutoff.
 * All destinations have been initialized by the controlled caller. */
static int
wl_columnar_relation_publish_retained_controlled(col_rel_t *r,
    wl_columnar_memory_reservation_t *pending, uint64_t bytes,
    wl_columnar_memory_reservation_t *previous_out,
    wl_evaluation_control_t *control)
{
    if (!r || !pending)
        return EINVAL;
    if (!r->memory_governor || !bytes)
        return 0;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_reservation_commit) {
        wl_columnar_relation_test_fail_reservation_commit = false;
        return ENOMEM;
    }
#endif
    int rc = wl_evaluation_control_charge(control, 6);
    if (rc != 0)
        return rc;
    uint64_t old_bytes = r->retained_reserved_bytes;
    uint64_t state = atomic_load_explicit(&r->retained_reservation.state,
            memory_order_acquire);
    if (pending->identity != pending ||
        r->retained_reservation.identity != &r->retained_reservation
        || pending == &r->retained_reservation
        || (old_bytes ? state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        : state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        && state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
        || (previous_out &&
        (previous_out == pending || previous_out == &r->retained_reservation
        || previous_out->identity != previous_out
        || (atomic_load_explicit(&previous_out->state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        && atomic_load_explicit(&previous_out->state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED))))
        return ENOMEM;
    wl_columnar_memory_reservation_t previous;
    wl_columnar_memory_reservation_init(&previous);
    rc = wl_columnar_relation_work_tail(control, 6);
    if (rc != 0)
        return rc;
    bool committed;
    rc = wl_columnar_memory_governor_commit_controlled(pending, r, &committed,
            control);
    if (rc != 0)
        return rc;
    if (!committed)
        return ENOMEM;
    /* All remaining moves are fixed, prevalidated, non-failing publication. */
    if (old_bytes && !wl_columnar_memory_reservation_move(&previous,
        &r->retained_reservation))
        abort();
    if (!wl_columnar_memory_reservation_move(&r->retained_reservation, pending))
        abort();
    if (old_bytes) {
        if (previous_out) {
            if (!wl_columnar_memory_reservation_move(previous_out, &previous))
                abort();
        } else
            col_rel_release_reservation_or_abort(&previous);
    }
    r->retained_reserved_bytes = bytes;
    return 0;
}

static int
wl_columnar_relation_publish_payload_controlled(col_rel_t *r,
    col_rel_payload_txn_t *txn, uint64_t final_bytes,
    wl_columnar_memory_reservation_t *previous_out,
    wl_evaluation_control_t *control)
{
    if (!r->memory_governor)
        return 0;
    if (!txn->growth)
        return txn->fresh.bytes ?
               wl_columnar_relation_publish_retained_controlled(r,
                   &txn->fresh, final_bytes, previous_out, control) : 0;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_reservation_commit) {
        wl_columnar_relation_test_fail_reservation_commit = false;
        return ENOMEM;
    }
#endif
    int rc = wl_evaluation_control_charge(control, 4);
    if (rc != 0)
        return rc;
    if (r->retained_reservation.identity != &r->retained_reservation
        || txn->incoming_bytes > UINT64_MAX - r->retained_reserved_bytes
        || (previous_out && (previous_out->identity != previous_out
        || atomic_load_explicit(&previous_out->state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY)))
        return ENOMEM;
    rc = wl_columnar_relation_work_tail(control, 4);
    if (rc != 0)
        return rc;
    bool committed;
    rc = wl_columnar_memory_governor_commit_growth_controlled(
        &r->retained_reservation, &committed, control);
    if (rc != 0)
        return rc;
    if (!committed)
        return ENOMEM;
    r->retained_reserved_bytes += txn->incoming_bytes;
    txn->growth = false;
    return 0;
}

/* A replacement reservation covers the new buffers while the old buffers
 * are still live.  Release the old token only after those buffers have been
 * retired.  A failed release would make the governor's accounting
 * unrecoverable, so terminate rather than silently losing the token. */
static void
col_rel_release_retired_reservation(
    wl_columnar_memory_reservation_t *previous)
{
    col_rel_release_reservation_or_abort(previous);
}

static bool
wl_columnar_relation_compaction_rollback_growth(
    wl_columnar_memory_reservation_t *reservation)
{
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_compact_rollback) {
        wl_columnar_relation_test_fail_compact_rollback = false;
        return false;
    }
#endif
    return wl_columnar_memory_rollback_growth(reservation);
}

static bool
wl_columnar_relation_compaction_commit_growth(
    wl_columnar_memory_reservation_t *reservation)
{
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_compact_commit) {
        wl_columnar_relation_test_fail_compact_commit = false;
        return false;
    }
#endif
    return wl_columnar_memory_commit_growth(reservation);
}

/* Reserve the complete private shape of an ownership transition.  The
 * ordinary retained path deliberately rejects shared and arena-backed
 * relations because those buffers are not yet heap-owned; transition paths
 * use this helper instead and charge the replacement only once. Column-only
 * COW retains physical timestamp capacity; growth supplies its new allocation
 * capacity. These shapes must not be inferred from the same column bound. */
static int
col_rel_reserve_transition(col_rel_t *r, uint32_t capacity,
    uint32_t timestamp_capacity, bool allocate_timestamps,
    col_rel_payload_txn_t *pending,
    uint64_t *bytes_out)
{
    uint64_t bytes, incoming, timestamps;

    if (!r || !pending || !bytes_out || !col_rel_timestamp_shape_valid(r)) {
        return -EINVAL;
    }
    col_rel_payload_txn_init(pending);
    if (!col_rel_payload_private_bytes(r, r->ncols, capacity,
        timestamp_capacity, r->ncols != 0, &bytes)) {
        return -EOVERFLOW;
    }
    *bytes_out = bytes;
    if (!col_rel_payload_grid_bytes(r->ncols, r->ncols, capacity,
        r->ncols != 0, &incoming)
        || !wl_columnar_memory_size_mul(allocate_timestamps
            ? timestamp_capacity : 0,
        sizeof(col_delta_timestamp_t), &timestamps)
        || !wl_columnar_memory_size_add(incoming, timestamps, &incoming)) {
        return -EOVERFLOW;
    }
    return col_rel_payload_txn_begin(r, bytes, incoming, pending);
}

/* Migrate an arena relation, or grow a relation while it still contains
 * borrowed/arena storage.  Every replacement buffer and timestamp array is
 * prepared before the relation is changed, so admission or allocation
 * failure leaves the old ownership and reservation untouched. */
static int
col_rel_grow_owned_transition_publish_impl(col_rel_t *r, uint32_t new_cap,
    bool defer_alias_release, wl_columnar_relation_mutation_lease_t *lease)
{
    col_rel_payload_txn_t pending;
    wl_columnar_memory_reservation_t previous;
    int pending_rc;
    uint64_t new_bytes;
    int64_t **new_columns = NULL;
    col_delta_timestamp_t *new_timestamps = NULL;
    int64_t **old_columns;
    col_delta_timestamp_t *old_timestamps;
    bool *old_shared_flags;
    bool old_arena;
    uint64_t ledger_before;
    uint64_t prior_generation;

    if (lease && (lease->storage_transitioned
        || lease->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || col_rel_mutation_lease_validate(lease, r)))
        return EINVAL;
    prior_generation = lease ? lease->relation_generation
        : (r ? r->storage_generation : 0);
    wl_columnar_memory_reservation_init(&previous);

    if (!r || (r->ncols && !r->columns) || new_cap < r->nrows)
        return EINVAL;
    pending_rc = col_rel_reserve_transition(r, new_cap,
            r->timestamps ? new_cap : 0, r->timestamps != NULL, &pending,
            &new_bytes);
    if (pending_rc < 0)
        return -pending_rc;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_prepare_resize) {
        wl_columnar_relation_test_fail_prepare_resize = false;
        goto fail;
    }
#endif
    if (r->ncols)
        new_columns = col_columns_alloc(r->ncols, new_cap);
    if (r->ncols && !new_columns)
        goto fail;
    for (uint32_t c = 0; c < r->ncols; c++)
        memcpy(new_columns[c], r->columns[c],
            (size_t)r->nrows * sizeof(int64_t));
    if (r->timestamps) {
        new_timestamps = (col_delta_timestamp_t *)malloc(
            (size_t)new_cap * sizeof(*new_timestamps));
        if (!new_timestamps)
            goto fail;
        memcpy(new_timestamps, r->timestamps,
            (size_t)r->nrows * sizeof(*new_timestamps));
    }
    if (pending_rc > 0
        && col_rel_publish_payload_txn(r, &pending, new_bytes,
        &previous) != 0)
        goto fail;

    old_columns = r->columns;
    old_timestamps = r->timestamps;
    old_shared_flags = r->col_shared;
    old_arena = r->arena_owned;
    ledger_before = col_rel_owned_ledger_bytes(r);
    r->columns = new_columns;
    r->timestamps = new_timestamps;
    r->timestamp_capacity = new_timestamps ? new_cap : 0;
    r->capacity = new_cap;
    r->arena_owned = false;
    r->col_shared = NULL;
    if (old_arena) {
        free((void *)old_columns);
    } else {
        for (uint32_t c = 0; c < r->ncols; c++)
            if (!old_shared_flags || !old_shared_flags[c])
                free(old_columns[c]);
        free((void *)old_columns);
    }
    free(old_shared_flags);
    free(old_timestamps);
    if (old_shared_flags)
        col_rel_retire_shared_table_credit(r);
    col_rel_release_retired_reservation(&previous);
    col_rel_retire_payload_credit(r);
    col_rel_ledger_reconcile(r, ledger_before);
    if (lease) {
        if (!defer_alias_release) {
            /* Publication cannot fail from here. Invalid authority means a
             * broken invariant, never a recoverable partial-success return. */
            if (col_rel_storage_alias_release_locked(r, lease))
                abort();
            wl_columnar_relation_touch_storage(r);
            if (col_rel_mutation_lease_advance_storage(lease,
                prior_generation) != 0)
                abort();
        }
    } else {
        if (!defer_alias_release)
            (void)col_rel_storage_alias_release(r);
        wl_columnar_relation_touch_storage(r);
    }
    return 0;

fail:
    col_columns_free(new_columns, r->ncols);
    free(new_timestamps);
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    wl_columnar_relation_test_note_resize_columns_retired();
#endif
    col_rel_payload_txn_rollback(r, &pending);
    return ENOMEM;
}

int
col_rel_promote_arena_admitted(col_rel_t *r)
{
    if (!r || !r->arena_owned)
        return 0;
    r->memory_budget_denial_pending = false;
    return col_rel_grow_owned_transition(r, r->capacity);
}

/* Copy-on-write at the current capacity.  Shared buffers are never freed;
 * owned columns remain in place, and the reservation is published only once
 * every private replacement has been copied successfully. */
int
col_rel_cow_unshare(col_rel_t *r, uint32_t new_cap)
{
    wl_columnar_relation_mutation_single_t single = { 0 };
    int rc;
    if (!r)
        return 0;
    rc = col_rel_mutation_single_acquire(r, &single);
    if (rc)
        return rc;
    /* ENOMEM provenance is an outward result channel: quota refusal sets
     * it again; allocation failure keeps it clear. Guard contention above
     * leaves prior evidence untouched. It is not mutation rollback state. */
    r->memory_budget_denial_pending = false;
    rc = col_rel_cow_unshare_publish_impl(r, new_cap, false, &single.lease);
    int finish_rc = col_rel_mutation_set_finish(&single.set, rc == 0);
    return rc ? rc : finish_rc;
}

int
col_rel_cow_unshare_with_lease(col_rel_t *r,
    wl_columnar_relation_mutation_lease_t *lease)
{
    if (!r || !lease || lease->role_flags
        != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || col_rel_mutation_lease_validate(lease, r))
        return EINVAL;
    return col_rel_cow_unshare_publish_impl(r, 0, false, lease);
}

int
wl_columnar_relation_privatize_shared_view_with_lease(col_rel_t *r,
    wl_columnar_relation_mutation_lease_t *lease)
{
    int rc;
    if (!r || !lease || lease->role_flags
        != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || col_rel_mutation_lease_validate(lease, r))
        return EINVAL;
    if (r->col_shared)
        return col_rel_cow_unshare_publish_impl(r, 0, false, lease);
    if (lease->owner == r)
        return 0;
    if (r->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EOVERFLOW;
    uint64_t prior_generation = r->storage_generation;
    rc = col_rel_storage_alias_release_locked(r, lease);
    if (rc)
        return rc;
    wl_columnar_relation_touch_storage(r);
    if (col_rel_mutation_lease_advance_storage(lease, prior_generation) != 0)
        abort();
    return 0;
}

static int
col_rel_cow_unshare_publish_impl(col_rel_t *r, uint32_t new_cap,
    bool defer_alias_release,
    wl_columnar_relation_mutation_lease_t *lease)
{
    col_rel_payload_txn_t pending;
    wl_columnar_memory_reservation_t previous;
    int pending_rc;
    uint64_t new_bytes;
    int64_t **private_cols = NULL;
    int64_t **old_columns;
    bool *old_shared;
    uint32_t capacity;
    uint64_t ledger_before;
    uint64_t prior_generation;

    if (lease && (lease->storage_transitioned
        || lease->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || col_rel_mutation_lease_validate(lease, r)))
        return EINVAL;
    prior_generation = lease ? lease->relation_generation
        : (r ? r->storage_generation : 0);
    wl_columnar_memory_reservation_init(&previous);

    if (!r || !r->col_shared)
        return 0;
    capacity = new_cap ? new_cap : (r->capacity ? r->capacity
                                                    : COL_REL_INIT_CAP);
    if (capacity > r->capacity)
        return col_rel_grow_owned_transition_publish_impl(r, capacity,
                   defer_alias_release, lease);
    pending_rc = col_rel_reserve_transition(r, capacity,
            r->timestamps ? r->timestamp_capacity : 0, false, &pending,
            &new_bytes);
    if (pending_rc < 0)
        return -pending_rc;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_prepare_resize) {
        wl_columnar_relation_test_fail_prepare_resize = false;
        goto fail;
    }
#endif
    private_cols = col_columns_alloc(r->ncols, capacity);
    if (r->ncols && !private_cols)
        goto fail;
    for (uint32_t c = 0; c < r->ncols; c++)
        memcpy(private_cols[c], r->columns[c],
            (size_t)r->nrows * sizeof(int64_t));
    if (pending_rc > 0
        && col_rel_publish_payload_txn(r, &pending, new_bytes,
        &previous) != 0)
        goto fail;
    ledger_before = col_rel_owned_ledger_bytes(r);
    old_columns = r->columns;
    old_shared = r->col_shared;
    r->columns = private_cols;
    r->col_shared = NULL;
    private_cols = NULL;
    for (uint32_t c = 0; c < r->ncols; c++)
        if (!old_shared[c])
            free(old_columns[c]);
    free((void *)old_columns);
    free(old_shared);
    col_rel_retire_shared_table_credit(r);
    col_rel_release_retired_reservation(&previous);
    col_rel_retire_payload_credit(r);
    col_rel_ledger_reconcile(r, ledger_before);
    /* Private columns replaced the borrowed view: one storage epoch. */
    if (lease) {
        if (!defer_alias_release) {
            /* Publication cannot fail from here. Invalid authority means a
             * broken invariant, never a recoverable partial-success return. */
            if (col_rel_storage_alias_release_locked(r, lease))
                abort();
            wl_columnar_relation_touch_storage(r);
            if (col_rel_mutation_lease_advance_storage(lease,
                prior_generation) != 0)
                abort();
        }
    } else {
        if (!defer_alias_release)
            (void)col_rel_storage_alias_release(r);
        wl_columnar_relation_touch_storage(r);
    }
    return 0;

fail:
    col_columns_free(private_cols, r->ncols);
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    wl_columnar_relation_test_note_resize_columns_retired();
#endif
    col_rel_payload_txn_rollback(r, &pending);
    return ENOMEM;
}

int
col_rel_attach_memory_governor(col_rel_t *r,
    wl_columnar_memory_governor_ref_t *memory_governor)
{
    uint64_t bytes = 0, metadata_bytes = 0, pool_name_bytes = 0;
    uint64_t dedup_bytes = 0;
    uint64_t retained_bytes = 0;
    col_rel_payload_shape_t payload;
    int64_t *prepared_scratch = NULL;
    wl_columnar_memory_admission_status_t status;
    wl_columnar_memory_reservation_t descriptor_pending, metadata_pending;
    wl_columnar_memory_reservation_t pool_name_pending, retained_pending;
    wl_columnar_memory_reservation_t dedup_pending;

    if (!r || !memory_governor)
        return EINVAL;
    if (!col_rel_timestamp_shape_valid(r))
        return EINVAL;
    if (wl_columnar_eval_dedup_set_bytes(r, &dedup_bytes) != 0)
        return EINVAL;
    if (r->memory_governor == memory_governor)
        return 0;
    if (r->memory_governor || r->retained_reserved_bytes != 0
        || r->metadata_reserved_bytes != 0
        || r->pool_name_reserved_bytes != 0)
        return EBUSY;
    if (!col_rel_current_metadata_bytes(r, &metadata_bytes))
        return EOVERFLOW;
    if (!col_rel_payload_live_shape(r, &payload))
        return EINVAL;
    payload.scratch = r->ncols != 0;
    if (!col_rel_payload_shape_bytes(&payload, &retained_bytes))
        return EOVERFLOW;
    wl_columnar_memory_reservation_init(&descriptor_pending);
    wl_columnar_memory_reservation_init(&metadata_pending);
    wl_columnar_memory_reservation_init(&pool_name_pending);
    wl_columnar_memory_reservation_init(&retained_pending);
    wl_columnar_memory_reservation_init(&dedup_pending);
    if (!r->pool_owned) {
        if (r->name && strlen(r->name) == SIZE_MAX)
            return EOVERFLOW;
        if (!wl_columnar_memory_size_add(sizeof(*r),
            r->name ? (uint64_t)strlen(r->name) + 1u : 0u, &bytes))
            return EOVERFLOW;
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(memory_governor), bytes,
            &descriptor_pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                r->memory_budget_denial_pending = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
    } else if (r->name) {
        if (strlen(r->name) == SIZE_MAX)
            return EOVERFLOW;
        pool_name_bytes = (uint64_t)strlen(r->name) + 1u;
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(memory_governor),
            pool_name_bytes, &pool_name_pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                r->memory_budget_denial_pending = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
    }
    if (metadata_bytes) {
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(memory_governor),
            metadata_bytes, &metadata_pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            col_rel_reservation_rollback(&descriptor_pending);
            col_rel_reservation_rollback(&pool_name_pending);
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                r->memory_budget_denial_pending = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
    }
    if (retained_bytes) {
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(memory_governor),
            retained_bytes, &retained_pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            col_rel_reservation_rollback(&descriptor_pending);
            col_rel_reservation_rollback(&pool_name_pending);
            col_rel_reservation_rollback(&metadata_pending);
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                r->memory_budget_denial_pending = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
    }
    if (dedup_bytes) {
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(memory_governor),
            dedup_bytes, &dedup_pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            col_rel_reservation_rollback(&descriptor_pending);
            col_rel_reservation_rollback(&pool_name_pending);
            col_rel_reservation_rollback(&metadata_pending);
            col_rel_reservation_rollback(&retained_pending);
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                r->memory_budget_denial_pending = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
    }
    if (r->ncols && !r->row_scratch) {
        prepared_scratch = (int64_t *)malloc((size_t)r->ncols
                * sizeof(*prepared_scratch));
        if (!prepared_scratch) {
            col_rel_reservation_rollback(&descriptor_pending);
            col_rel_reservation_rollback(&pool_name_pending);
            col_rel_reservation_rollback(&metadata_pending);
            col_rel_reservation_rollback(&retained_pending);
            col_rel_reservation_rollback(&dedup_pending);
            return ENOMEM;
        }
    }
    wl_columnar_memory_reservation_init(&r->descriptor_reservation);
    wl_columnar_memory_reservation_init(&r->metadata_reservation);
    wl_columnar_memory_reservation_init(&r->pool_name_reservation);
    wl_columnar_memory_reservation_init(&r->dedup_reservation);
    wl_columnar_memory_reservation_init(&r->retained_reservation);
    if (bytes && (!wl_columnar_memory_reservation_move(
            &r->descriptor_reservation, &descriptor_pending)
        || !wl_columnar_memory_commit(&r->descriptor_reservation, r)))
        abort();
    if (metadata_bytes && (!wl_columnar_memory_reservation_move(
            &r->metadata_reservation, &metadata_pending)
        || !wl_columnar_memory_commit(&r->metadata_reservation, r)))
        abort();
    if (pool_name_bytes && (!wl_columnar_memory_reservation_move(
            &r->pool_name_reservation, &pool_name_pending)
        || !wl_columnar_memory_commit(&r->pool_name_reservation, r)))
        abort();
    if (retained_bytes && (!wl_columnar_memory_reservation_move(
            &r->retained_reservation, &retained_pending)
        || !wl_columnar_memory_commit(&r->retained_reservation, r)))
        abort();
    if (dedup_bytes && (!wl_columnar_memory_reservation_move(
            &r->dedup_reservation, &dedup_pending)
        || !wl_columnar_memory_commit(&r->dedup_reservation, r)))
        abort();
    r->descriptor_reserved_bytes = bytes;
    r->metadata_reserved_bytes = metadata_bytes;
    r->pool_name_reserved_bytes = pool_name_bytes;
    r->retained_reserved_bytes = retained_bytes;
    r->dedup_reserved_bytes = dedup_bytes;
    if (prepared_scratch)
        r->row_scratch = prepared_scratch;
    r->memory_governor = memory_governor;
    r->memory_budget_denial_pending = false;
    wl_columnar_memory_governor_ref_retain(memory_governor);
    return 0;
}

int
wl_columnar_relation_admit_existing_metadata(col_rel_t *dst,
    const col_rel_t *src)
{
    wl_columnar_memory_reservation_t pending;
    uint64_t bytes;
    int rc;

    if (!dst || !src || !dst->memory_governor
        || dst->metadata_reserved_bytes)
        return EINVAL;
    if (!col_rel_current_metadata_bytes(src, &bytes))
        return EOVERFLOW;
    rc = col_rel_reserve_metadata(dst, bytes, &pending);
    if (rc != 0)
        return rc;
    if (bytes && (!wl_columnar_memory_reservation_move(
            &dst->metadata_reservation, &pending)
        || !wl_columnar_memory_commit(&dst->metadata_reservation, dst)))
        abort();
    dst->metadata_reserved_bytes = bytes;
    return 0;
}

/* A private pool descriptor may be promoted from an ungoverned arena into a
 * governed heap descriptor. Admit its existing payload before publication;
 * the descriptor and metadata have already been staged by session.c. */
int
wl_columnar_relation_admit_existing_payload(col_rel_t *r)
{
    col_rel_payload_shape_t shape;
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_admission_status_t status;
    int64_t *scratch = NULL;
    uint64_t bytes;

    if (!r || !r->memory_governor || r->retained_reserved_bytes
        || !col_rel_timestamp_shape_valid(r)
        || !col_rel_payload_live_shape(r, &shape))
        return EINVAL;
    shape.scratch = r->ncols != 0;
    if (!col_rel_payload_shape_bytes(&shape, &bytes))
        return EOVERFLOW;
    wl_columnar_memory_reservation_init(&pending);
    if (bytes) {
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(r->memory_governor), bytes,
            &pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                r->memory_budget_denial_pending = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
    }
    if (r->ncols && !r->row_scratch) {
        scratch = malloc((size_t)r->ncols * sizeof(*scratch));
        if (!scratch) {
            col_rel_reservation_rollback(&pending);
            return ENOMEM;
        }
    }
    if (bytes && (!wl_columnar_memory_reservation_move(
            &r->retained_reservation, &pending)
        || !wl_columnar_memory_commit(&r->retained_reservation, r)))
        abort();
    r->retained_reserved_bytes = bytes;
    if (scratch)
        r->row_scratch = scratch;
    return 0;
}

int
col_rel_enable_timestamps_locked(col_rel_t *r)
{
    col_rel_payload_txn_t pending;
    int reserve_rc;
    int failure_rc = ENOMEM;
    uint64_t new_bytes;
    col_delta_timestamp_t *timestamps;

    if (!r || !col_rel_timestamp_shape_valid(r))
        return EINVAL;
    if (r->timestamps || r->capacity == 0)
        return 0;
    if (!r->memory_governor) {
        r->timestamps = (col_delta_timestamp_t *)calloc(
            r->capacity, sizeof(*r->timestamps));
        if (!r->timestamps)
            return ENOMEM;
        r->timestamp_capacity = r->capacity;
        wl_columnar_relation_touch_storage(r);
        return 0;
    }
    reserve_rc = col_rel_reserve_retained_shape(
        r, r->capacity, r->capacity, false, true, false, &pending);
    if (reserve_rc < 0)
        return -reserve_rc;
    if (!col_rel_payload_private_bytes(r, r->ncols, r->capacity,
        r->capacity, true, &new_bytes)) {
        failure_rc = EOVERFLOW;
        goto fail;
    }
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_prepare_resize) {
        wl_columnar_relation_test_fail_prepare_resize = false;
        goto fail;
    }
#endif
#ifdef WL_SESSION_TEST_HOOKS
    bool fail_alloc = wl_columnar_relation_fail_next_governed_timestamp_alloc;
    wl_columnar_relation_fail_next_governed_timestamp_alloc = false;
    timestamps = fail_alloc ? NULL : (col_delta_timestamp_t *)calloc(
        r->capacity, sizeof(*timestamps));
#else
    timestamps = (col_delta_timestamp_t *)calloc(
        r->capacity, sizeof(*timestamps));
#endif
    if (!timestamps)
        goto fail;
    r->timestamps = timestamps;
    r->timestamp_capacity = timestamps ? r->capacity : 0;
    if (reserve_rc > 0
        && col_rel_publish_payload_txn(r, &pending, new_bytes,
        NULL) != 0) {
        r->timestamps = NULL;
        r->timestamp_capacity = 0;
        free(timestamps);
        goto fail;
    }
    col_rel_ledger_reconcile(r, col_rel_owned_ledger_bytes(r));
    wl_columnar_relation_touch_storage(r);
    return 0;

fail:
    col_rel_payload_txn_rollback(r, &pending);
    return failure_rc;
}

int
col_rel_enable_timestamps(col_rel_t *r)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc;
    int release_rc;

    if (!r)
        return EINVAL;
    if (r->timestamps || r->capacity == 0)
        return 0;
    rc = col_rel_published_writer_acquire(r, &writer);
    if (rc != 0)
        return rc;
    r->memory_budget_denial_pending = false;
    rc = col_rel_enable_timestamps_locked(r);
    release_rc = wl_columnar_source_access_writer_release(&writer);
    if (rc == 0 && release_rc != 0)
        rc = release_rc;
    return rc;
}

/*
 * ledger_sync_timestamps: bring the TIMESTAMP charge for r->timestamps in
 * line with its current size (Issue #1380).  Every relation.c growth and
 * shrink path already brackets the column change with
 * col_rel_ledger_reconcile(), so piggybacking here covers the realloc of
 * timestamps in the same paths, plus any eval-layer calloc of timestamps
 * on an attached relation at its next reconcile or release.
 */
static void
ledger_sync_timestamps(col_rel_t *r)
{
    uint64_t now = col_rel_timestamp_ledger_bytes(r);
    if (now > r->ledger_ts_bytes) {
        wl_mem_ledger_alloc(r->mem_ledger, WL_MEM_SUBSYS_TIMESTAMP,
            now - r->ledger_ts_bytes);
    } else if (r->ledger_ts_bytes > now) {
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_TIMESTAMP,
            r->ledger_ts_bytes - now);
    }
    r->ledger_ts_bytes = now;
}

static void
wl_columnar_relation_ledger_reconcile_known(col_rel_t *r,
    uint64_t before_bytes, uint64_t after_bytes)
{
    if (!r || !r->mem_ledger)
        return;
    if (after_bytes > before_bytes) {
        wl_mem_ledger_alloc(r->mem_ledger, WL_MEM_SUBSYS_RELATION,
            after_bytes - before_bytes);
    } else if (before_bytes > after_bytes) {
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_RELATION,
            before_bytes - after_bytes);
    }
    ledger_sync_timestamps(r);
}

/* Aggregate finite FORWARD actions, across original retirement segments:
 * growth/token/grid64 + metadata112 + row/view8 + two ledger applies48 +
 * timestamp gauge1 + successful final G writes4 =237 <= one quantum.
 * Width-based old-image/known Arrow destruction and credit retirement are
 * mandatory cleanup, not a claim of a contiguous bounded instruction region. */
typedef char wl_columnar_relation_publication_quantum[
        (64u + 112u + 8u + 2u * 24u + 1u + 4u <=
        WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

/* Exclusive attached-ledger writer authority belongs to the controlled caller.
 * Requests describe only deltas. The final apply reads CURRENT counters after
 * all callbacks and preserves RELATION then TIMESTAMP raw reconciliation order. */
static int
wl_columnar_relation_ledger_prepare_exclusive(col_rel_t *r,
    uint64_t before, uint64_t after, uint64_t timestamps,
    wl_columnar_mem_ledger_exclusive_prepared_t *prepared,
    wl_evaluation_control_t *control)
{
    prepared->ledger = NULL;
    prepared->count = 0;
    if (!r->mem_ledger)
        return 0;
    /* <=24 scalar request construction/shape/gauge actions; apply gauge1 is
    * included here, before the final callback-free publication sequence. */
    int rc = wl_evaluation_control_charge(control, 24);
    if (rc != 0)
        return rc;
    wl_columnar_mem_ledger_exclusive_request_t requests[2];
    uint32_t count = 0;
    if (before != after) {
        requests[count].subsystem = WL_MEM_SUBSYS_RELATION;
        requests[count].allocate = after > before;
        requests[count++].bytes = after >
            before ? after - before : before - after;
    }
    if (timestamps != r->ledger_ts_bytes) {
        requests[count].subsystem = WL_MEM_SUBSYS_TIMESTAMP;
        requests[count].allocate = timestamps > r->ledger_ts_bytes;
        requests[count++].bytes = timestamps > r->ledger_ts_bytes
            ? timestamps - r->ledger_ts_bytes : r->ledger_ts_bytes - timestamps;
    }
    rc = wl_columnar_mem_ledger_prepare_exclusive_controlled(r->mem_ledger,
            requests, count, prepared, control);
    if (rc != 0)
        return rc;
    return wl_columnar_relation_work_tail(control,
               24u + (count ? 8u + 32u * count : 0u));
}

static void
wl_columnar_relation_ledger_apply_exclusive(col_rel_t *r, uint64_t timestamps,
    const wl_columnar_mem_ledger_exclusive_prepared_t *prepared)
{
    if (!r->mem_ledger)
        return;
    wl_columnar_mem_ledger_apply_exclusive(prepared);
    r->ledger_ts_bytes = timestamps;
}

void
col_rel_ledger_reconcile(col_rel_t *r, uint64_t before_bytes)
{
    if (!r || !r->mem_ledger)
        return;
    wl_columnar_relation_ledger_reconcile_known(r, before_bytes,
        col_rel_owned_ledger_bytes(r));
}

void
col_rel_ledger_release(col_rel_t *r)
{
    if (!r || !r->mem_ledger)
        return;
    uint64_t bytes = col_rel_owned_ledger_bytes(r);
    if (bytes > 0)
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_RELATION, bytes);
    if (r->ledger_ts_bytes > 0) {
        wl_mem_ledger_free(r->mem_ledger, WL_MEM_SUBSYS_TIMESTAMP,
            r->ledger_ts_bytes);
        r->ledger_ts_bytes = 0;
    }
}

/* ---- lifecycle ---------------------------------------------------------- */

static void
col_rel_free_contents_impl(col_rel_t *r, bool preserve_access_gates)
{
    uint64_t ledger_before;
    uint64_t descriptor_bytes;
    wl_columnar_memory_governor_ref_t *descriptor_governor;
    wl_columnar_memory_reservation_t descriptor_reservation;

    if (!r)
        return;
    if (r->storage_owner == r
        && col_rel_storage_alias_borrow_count(r) > 0)
        return;
    descriptor_bytes = r->descriptor_reserved_bytes;
    descriptor_governor = descriptor_bytes ? r->memory_governor : NULL;
    wl_columnar_memory_reservation_init(&descriptor_reservation);
    if (descriptor_bytes
        && !wl_columnar_memory_reservation_move(&descriptor_reservation,
        &r->descriptor_reservation))
        abort();
    /* Alias bookkeeping is released before the relation is zeroed.  The
     * owner itself is not denied destruction here; #1494 wires this status
     * into the synchronous teardown transaction. */
    (void)col_rel_storage_alias_release(r);
    ledger_before = col_rel_owned_ledger_bytes(r);
    /* Release only currently heap-owned, non-borrowed data buffers.  The
     * physical free precedes every accounting/admission decrement so a
     * concurrent admission cannot reuse credit while these buffers remain
     * resident. */
    free(r->name);
    if (r->pool_name_reserved_bytes
        && !wl_columnar_memory_release(&r->pool_name_reservation))
        abort();
    r->pool_name_reserved_bytes = 0;
    if (!r->arena_owned) {
        if (r->col_shared && r->columns) {
            /* Free only non-shared columns (6B zero-copy sharing) */
            for (uint32_t c = 0; c < r->ncols; c++) {
                if (!r->col_shared[c])
                    free(r->columns[c]);
            }
            free((void *)r->columns);
        } else {
            col_columns_free(r->columns, r->ncols);
        }
    } else {
        /* Arena owns column buffers; only free the columns array itself */
        free((void *)r->columns);
    }
    free(r->col_shared);
    r->columns = NULL;
    r->col_shared = NULL;
    col_columns_free(r->retract_backup_columns, r->ncols);
    free(r->retract_backup_timestamps);
    col_columns_free(r->merge_columns, r->ncols);
    free(r->row_scratch);
    free(r->timestamps);
    r->timestamps = NULL;
    r->timestamp_capacity = 0;
    if (r->col_names) {
        for (uint32_t i = 0; i < r->ncols; i++)
            free(r->col_names[i]);
        free((void *)r->col_names);
    }
    free(r->column_types);
    wl_columnar_eval_dedup_set_clear(r);
    free(r->compound_arity_map);
    if (r->schema_ok)
        ArrowSchemaRelease(&r->schema);
    if (r->metadata_reserved_bytes
        && !wl_columnar_memory_release(&r->metadata_reservation))
        abort();
    r->metadata_reserved_bytes = 0;
    col_rel_ledger_reconcile(r, ledger_before);
    if (r->memory_governor) {
        if (r->retained_reserved_bytes > 0
            && !wl_columnar_memory_release(&r->retained_reservation))
            abort();
        r->retained_reserved_bytes = 0;
        if (!descriptor_bytes)
            wl_columnar_memory_governor_ref_release(r->memory_governor);
        r->memory_governor = NULL;
    }
    if (preserve_access_gates) {
        /* Checked destruction holds the descriptor writer (and, for roots,
         * the source writer). Keep those terminal states intact while the
         * pool tombstone remains addressable or the heap descriptor is freed.
         * In particular, never memset atomic gates while readers may probe
         * them: a zeroed gate could admit a reader into retiring storage. */
        memset(r, 0, offsetof(col_rel_t, storage_alias_borrows));
    } else {
        memset(r, 0, sizeof(*r));
    }
    if (descriptor_bytes) {
        r->memory_governor = descriptor_governor;
        r->descriptor_reserved_bytes = descriptor_bytes;
        wl_columnar_memory_reservation_init(&r->descriptor_reservation);
        if (!wl_columnar_memory_reservation_move(&r->descriptor_reservation,
            &descriptor_reservation))
            abort();
    }
}

void
col_rel_free_contents(col_rel_t *r)
{
    /* A governed heap descriptor must reach physical free before returning
     * its descriptor admission. Use checked destruction for that lifecycle. */
    if (r && !r->pool_owned && r->descriptor_reserved_bytes != 0)
        abort();
    col_rel_free_contents_impl(r, false);
}

/* Checked destruction and retirement leave the descriptor token embedded
 * through content cleanup. Return its credit only after the physical struct
 * has been freed. */
static void
col_rel_free_heap_after_contents(col_rel_t *r)
{
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor = NULL;

    wl_columnar_memory_reservation_init(&reservation);
    if (r->descriptor_reserved_bytes) {
        governor = r->memory_governor;
        if (!governor || !wl_columnar_memory_reservation_move(&reservation,
            &r->descriptor_reservation))
            abort();
    }
    free(r);
    if (governor) {
        if (!wl_columnar_memory_release(&reservation))
            abort();
        wl_columnar_memory_governor_ref_release(governor);
    }
}

/*
 * col_rel_destroy:
 * Free contents and, if heap-allocated (pool_owned == false), the struct
 * itself.  Pool-owned structs have their memory reclaimed on pool_reset();
 * calling free() on them would corrupt the pool allocator.
 */
int
col_rel_destroy_checked(col_rel_t *r)
{
    col_rel_t *owner = NULL;
    wl_columnar_source_access_writer_t descriptor_writer = { 0 };
    wl_columnar_source_access_writer_t owner_writer = { 0 };
    wl_columnar_source_access_reader_t owner_reader = { 0 };
    bool owner_writer_held = false;
    bool from_pool;
    int rc;

    if (!r)
        return 0;
    rc = wl_columnar_source_access_writer_acquire(&r->descriptor_access,
            &descriptor_writer);
    if (rc != 0)
        return rc == EINVAL ? rc : EBUSY;
    rc = col_rel_storage_owner_resolve(r, &owner);
    if (rc != 0)
        goto fail;
    if (owner == r) {
        rc = wl_columnar_source_access_writer_acquire(&owner->source_access,
                &owner_writer);
        if (rc != 0) {
            rc = EBUSY;
            goto fail;
        }
        owner_writer_held = true;
        /* Recheck after exclusive owner admission. A concurrent publication
         * cannot add a borrow between this check and terminal destruction. */
        if (col_rel_storage_alias_borrow_count(owner) != 0) {
            rc = EBUSY;
            goto fail;
        }
    } else {
        /* Alias retirement needs its own descriptor exclusively, but only a
         * reader on the canonical owner: unrelated peer readers may coexist
         * while this alias drops its borrow. */
        rc = wl_columnar_source_access_reader_acquire(&owner->source_access,
                &owner_reader);
        if (rc != 0) {
            rc = EBUSY;
            goto fail;
        }
        rc = col_rel_storage_alias_release(r);
        if (rc != 0)
            goto fail;
    }

    from_pool = r->pool_owned;
    col_rel_free_contents_impl(r, true); /* memset zeroes pool_owned */
    if (owner_reader.owner) {
        rc = wl_columnar_source_access_reader_release(&owner_reader);
        if (rc != 0)
            return EINVAL;
    }
    atomic_store_explicit(&r->source_access.state,
        WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_release);
    atomic_store_explicit(&r->descriptor_access.state,
        WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_release);
    if (!from_pool) {
        col_rel_free_heap_after_contents(r);
    } else {
        /* Keep a pool slot inert until its owning pool resets/reuses it. */
        r->pool_owned = true;
        r->relation_identity = 0;
        r->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID;
        r->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID;
        r->storage_owner = NULL;
        r->storage_owner_identity = 0;
        r->storage_owner_generation = 0;
        atomic_store_explicit(&r->storage_alias_borrows, 0,
            memory_order_relaxed);
        wl_columnar_memory_reservation_init(&r->retained_reservation);
    }
    /* If pool_owned: struct memory freed on pool_reset(), skip free(). */
    return 0;

fail:
    if (owner_reader.owner)
        (void)wl_columnar_source_access_reader_release(&owner_reader);
    if (owner_writer_held)
        (void)wl_columnar_source_access_writer_release(&owner_writer);
    if (descriptor_writer.owner)
        (void)wl_columnar_source_access_writer_release(&descriptor_writer);
    return rc;
}

void
wl_columnar_relation_retirement_init(
    wl_columnar_relation_retirement_token_t *token)
{
    if (token)
        memset(token, 0, sizeof(*token));
}

static bool
wl_columnar_relation_retirement_reservation_valid(const col_rel_t *relation)
{
    uint64_t state;
    uint64_t bytes;
    wl_columnar_memory_governor_t *governor;

    if (relation->retained_reserved_bytes == 0)
        return true;
    if (!relation->memory_governor
        || relation->retained_reservation.identity
        != &relation->retained_reservation
        || relation->retained_reservation.bytes
        != relation->retained_reserved_bytes
        || relation->retained_reservation.governor
        != wl_columnar_memory_governor_ref_get(relation->memory_governor)
        || atomic_load_explicit(&relation->retained_reservation.owner_bits,
        memory_order_acquire) != (uintptr_t)relation)
        return false;
    state = atomic_load_explicit(&relation->retained_reservation.state,
            memory_order_acquire);
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED) {
        if (relation->retained_reservation.replacement_bytes != 0)
            return false;
        bytes = relation->retained_reservation.bytes;
    } else if (state == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING) {
        if (relation->retained_reservation.replacement_bytes
            > UINT64_MAX - relation->retained_reservation.bytes)
            return false;
        bytes = relation->retained_reservation.bytes
            + relation->retained_reservation.replacement_bytes;
    } else {
        return false;
    }
    governor = relation->retained_reservation.governor;
    return wl_columnar_memory_reserved(governor) >= bytes;
}

static bool
wl_columnar_relation_retirement_valid_locked(const col_rel_t *relation,
    bool transient_backing_allowed)
{
    return relation
           && (transient_backing_allowed
           || (!relation->pool_owned && !relation->arena_owned))
           && relation->storage_owner == relation && !relation->col_shared
           && relation->storage_owner_identity == relation->relation_identity
           && relation->storage_owner_generation == relation->storage_generation
           && wl_columnar_relation_generation_valid(
        relation->storage_owner_generation)
           && col_rel_storage_alias_borrow_count(relation) == 0
           && wl_columnar_relation_retirement_reservation_valid(relation);
}

static bool
wl_columnar_relation_retirement_writer_valid(
    const wl_columnar_source_access_writer_t *writer,
    const wl_columnar_source_access_gate_t *gate)
{
    return writer && writer->owner == gate
           && writer->identity == (uintptr_t)writer
           && wl_columnar_source_access_writer_thread_equal(writer)
           && atomic_load_explicit(&gate->state, memory_order_acquire)
           == WL_COLUMNAR_SOURCE_ACCESS_WRITER;
}

static int
wl_columnar_relation_retirement_prepare_impl(col_rel_t *relation,
    wl_columnar_relation_retirement_token_t *token,
    bool transient_backing_allowed)
{
    int rc;

    if (!relation || !token)
        return EINVAL;
    if (token->relation || token->descriptor_writer.owner
        || token->source_writer.owner || token->descriptor_writer.identity
        || token->source_writer.identity)
        return EBUSY;
    /* Acquire the descriptor first, then inspect its ownership metadata while
    * stable. A failed structural check releases the gate without mutation. */
    rc = wl_columnar_source_access_writer_acquire(
        &relation->descriptor_access, &token->descriptor_writer);
    if (rc != 0)
        return rc == EINVAL ? rc : EBUSY;
    if ((!transient_backing_allowed
        && (relation->pool_owned || relation->arena_owned))
        || relation->storage_owner != relation || relation->col_shared
        || relation->storage_owner_identity != relation->relation_identity
        || relation->storage_owner_generation != relation->storage_generation
        || !wl_columnar_relation_generation_valid(
            relation->storage_owner_generation)) {
        (void)wl_columnar_source_access_writer_release(
            &token->descriptor_writer);
        return EBUSY;
    }
    rc = wl_columnar_source_access_writer_acquire(&relation->source_access,
            &token->source_writer);
    if (rc != 0) {
        (void)wl_columnar_source_access_writer_release(
            &token->descriptor_writer);
        return rc == EINVAL ? rc : EBUSY;
    }
    /* Recheck alias state after exclusive admission; no alias may be added
     * while this source writer is held. */
    if (!wl_columnar_relation_retirement_valid_locked(relation,
        transient_backing_allowed)) {
        (void)wl_columnar_source_access_writer_release(&token->source_writer);
        (void)wl_columnar_source_access_writer_release(
            &token->descriptor_writer);
        return EBUSY;
    }
    token->relation = relation;
    token->transient_backing_allowed = transient_backing_allowed;
    return 0;
}

int
wl_columnar_relation_retirement_prepare(col_rel_t *relation,
    wl_columnar_relation_retirement_token_t *token)
{
    return wl_columnar_relation_retirement_prepare_impl(relation, token,
               false);
}

int
wl_columnar_relation_retirement_prepare_transient(col_rel_t *relation,
    wl_columnar_relation_retirement_token_t *token)
{
    return wl_columnar_relation_retirement_prepare_impl(relation, token,
               true);
}

int
wl_columnar_relation_retirement_cancel(
    wl_columnar_relation_retirement_token_t *token)
{
    int rc;

    if (!token)
        return EINVAL;
    if (!token->relation)
        return token->descriptor_writer.owner || token->source_writer.owner
            ? EINVAL : 0;
    if (token->source_writer.owner) {
        rc = wl_columnar_source_access_writer_release(&token->source_writer);
        if (rc != 0)
            return rc;
    }
    if (token->descriptor_writer.owner) {
        rc = wl_columnar_source_access_writer_release(
            &token->descriptor_writer);
        if (rc != 0)
            return rc;
    }
    memset(token, 0, sizeof(*token));
    return 0;
}

void
wl_columnar_relation_retirement_commit(
    wl_columnar_relation_retirement_token_t *token)
{
    col_rel_t *relation;
    bool from_pool;

    if (!token || !token->relation
        || !token->descriptor_writer.owner || !token->source_writer.owner
        || token->descriptor_writer.owner != &token->relation->descriptor_access
        || token->source_writer.owner != &token->relation->source_access
        || !wl_columnar_relation_retirement_writer_valid(
            &token->descriptor_writer, &token->relation->descriptor_access)
        || !wl_columnar_relation_retirement_writer_valid(
            &token->source_writer, &token->relation->source_access)
        || !wl_columnar_relation_retirement_valid_locked(token->relation,
        token->transient_backing_allowed))
        abort();
    relation = token->relation;
    from_pool = relation->pool_owned;
    /* Do not release terminal writers: their gate storage is part of the
     * descriptor about to be freed. Physical contents retire before the
     * retained governor credit is returned by free_contents_impl(). */
    memset(token, 0, sizeof(*token));
    col_rel_free_contents_impl(relation, true);
    if (!from_pool) {
        col_rel_free_heap_after_contents(relation);
    } else {
        relation->pool_owned = true;
        relation->relation_identity = 0;
        relation->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID;
        relation->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID;
        relation->storage_owner = NULL;
        relation->storage_owner_identity = 0;
        relation->storage_owner_generation = 0;
        atomic_store_explicit(&relation->storage_alias_borrows, 0,
            memory_order_relaxed);
        wl_columnar_memory_reservation_init(&relation->retained_reservation);
    }
}

static int
col_rel_pool_destroy_pass_checked(delta_pool_t *pool, uint32_t first_slot,
    uint32_t slot_limit, bool aliases)
{
    int result = 0;

    if (!pool)
        return first_slot == 0 && slot_limit == 0 ? 0 : EINVAL;
    if (first_slot > slot_limit || slot_limit > pool->slot_used
        || (slot_limit > 0 && (!pool->slab
        || pool->slot_size < sizeof(col_rel_t)))) {
        fprintf(stderr,
            "wirelog: invalid pool teardown range "
            "(first=%u limit=%u used=%u slab=%p slot_size=%zu)\n",
            first_slot, slot_limit, pool->slot_used, (void *)pool->slab,
            pool->slot_size);
        return EINVAL;
    }

    for (uint32_t slot = first_slot; slot < slot_limit; slot++) {
        col_rel_t *relation = (col_rel_t *)(pool->slab
            + (size_t)slot * pool->slot_size);
        if (relation->relation_identity == 0 && !relation->storage_owner)
            continue; /* Already retired pool slot. */
        bool is_alias = relation->storage_owner
            && relation->storage_owner != relation;
        if (is_alias != aliases)
            continue;

        int rc = col_rel_destroy_checked(relation);
        if (rc != 0 && rc != EBUSY)
            fprintf(stderr,
                "wirelog: invalid pool relation during checked teardown "
                "(slot=%u aliases=%d identity=%llu owner=%p rc=%d)\n",
                slot, aliases ? 1 : 0,
                (unsigned long long)relation->relation_identity,
                (void *)relation->storage_owner, rc);
        if (rc != 0 && result == 0)
            result = rc;
    }
    return result;
}

/* Pool-backed aliases must be retired before roots. Keep these as separate
 * passes so a session can retire aliases from both its heap relation array
 * and its pool before attempting to destroy either set of roots. */
int
col_rel_pool_destroy_aliases_checked(delta_pool_t *pool,
    uint32_t first_slot, uint32_t slot_limit)
{
    return col_rel_pool_destroy_pass_checked(pool, first_slot, slot_limit,
               true);
}

int
col_rel_pool_destroy_roots_checked(delta_pool_t *pool,
    uint32_t first_slot, uint32_t slot_limit)
{
    return col_rel_pool_destroy_pass_checked(pool, first_slot, slot_limit,
               false);
}

void
col_rel_destroy(col_rel_t *r)
{
    (void)col_rel_destroy_checked(r);
}

/* Initialize Arrow metadata independently of heap or arena column storage. */
static int
wl_columnar_relation_init_arrow_schema(col_rel_t *r)
{
    if (r->schema_ok) {
        ArrowSchemaRelease(&r->schema);
        r->schema_ok = false;
    }
    ArrowSchemaInit(&r->schema);
    if (ArrowSchemaSetTypeStruct(&r->schema, (int64_t)r->ncols)
        != NANOARROW_OK)
        goto fail;
    for (uint32_t i = 0; i < r->ncols; i++) {
        enum ArrowType type = r->column_types
            && r->column_types[i] == WIRELOG_TYPE_FLOAT
            ? NANOARROW_TYPE_DOUBLE : NANOARROW_TYPE_INT64;
        ArrowSchemaRelease(r->schema.children[i]);
        if (ArrowSchemaInitFromType(r->schema.children[i], type)
            != NANOARROW_OK
            || ArrowSchemaSetName(r->schema.children[i],
            r->col_names && r->col_names[i] ? r->col_names[i] : "")
            != NANOARROW_OK)
            goto fail;
    }
    r->schema_ok = true;
    return 0;
fail:
    ArrowSchemaRelease(&r->schema);
    return ENOMEM;
}

/*
 * col_rel_set_schema:
 * Initialise ncols, col_names[], data buffer, and ArrowSchema.
 * Called lazily on first insert (EDB) or when relation is first produced.
 * Returns 0 on success, ENOMEM/EINVAL on failure.
 */
static int
col_rel_set_schema_impl_capacity(col_rel_t *r, uint32_t ncols,
    const char *const *col_names, uint32_t initial_capacity,
    bool with_timestamps, bool *out_denied)
{
    col_rel_t image = { 0 };
    col_rel_payload_txn_t pending;
    wl_columnar_memory_reservation_t metadata_pending;
    wl_columnar_memory_reservation_t old_metadata;
    int pending_rc;
    int failure_rc = ENOMEM;
    uint64_t retained_bytes = 0, metadata_bytes = 0;
    bool promote_nullary_schema;
    bool image_owns_timestamps = false;
    struct ArrowSchema old_schema;
    bool old_schema_ok;

    if (out_denied)
        *out_denied = false;
    if (!r || !col_rel_timestamp_shape_valid(r))
        return EINVAL;
    promote_nullary_schema = r->schema_ok && r->ncols == 0 && ncols > 0;
    if (r->schema_ok && !promote_nullary_schema)
        return 0; /* already initialised */
    if (promote_nullary_schema) {
        if (r->nrows != 0 || r->columns || r->col_names)
            return EINVAL;
        if (r->timestamps && r->timestamp_capacity < r->capacity)
            return EINVAL;
        initial_capacity = r->capacity;
        if (r->timestamp_capacity > initial_capacity)
            initial_capacity = r->timestamp_capacity;
    }
    if (!col_rel_compound_map_valid_for(r, ncols))
        return EINVAL;
    if (!col_rel_prospective_metadata_bytes(ncols, col_names, ncols > 0,
        true, r->column_types != NULL, true, r->compound_arity_len, false,
        &metadata_bytes))
        return EOVERFLOW;
    failure_rc = col_rel_reserve_metadata(r, metadata_bytes,
            &metadata_pending);
    if (failure_rc != 0) {
        if (out_denied && failure_rc == ENOSPC)
            *out_denied = true;
        return failure_rc;
    }
    failure_rc = ENOMEM;
    image.ncols = ncols;
    image.capacity = initial_capacity;
    image.memory_governor = r->memory_governor;
    image.retained_reserved_bytes = r->retained_reserved_bytes;
    image.arena_owned = r->arena_owned;
    image.col_shared = r->col_shared;
    image.column_types = r->column_types;
    /* Existing timestamps remain owned by the source until publication. */
    image.timestamps = r->timestamps;
    image.timestamp_capacity = r->timestamp_capacity;
    if ((with_timestamps || image.timestamps)
        && image.timestamp_capacity < image.capacity)
        image.timestamp_capacity = image.capacity;
    uint64_t incoming_grid = 0, incoming_scratch = 0;
    if (!col_rel_payload_private_bytes(r, ncols, image.capacity,
        image.timestamp_capacity, r->memory_governor && ncols != 0,
        &retained_bytes)
        || !col_rel_payload_grid_bytes(ncols, ncols, image.capacity,
        ncols != 0, &incoming_grid)
        || !wl_columnar_memory_size_mul(ncols, sizeof(int64_t),
        &incoming_scratch)
        || !wl_columnar_memory_size_add(incoming_grid,
        r->memory_governor ? incoming_scratch : 0,
        &incoming_grid)) {
        col_rel_reservation_rollback(&metadata_pending);
        return EOVERFLOW;
    }
    if ((with_timestamps || image.timestamps) && image.timestamp_capacity
        && (!image.timestamps || r->timestamp_capacity < image.capacity)) {
        uint64_t timestamp_bytes;
        if (!wl_columnar_memory_size_mul(image.timestamp_capacity,
            sizeof(col_delta_timestamp_t), &timestamp_bytes)
            || !wl_columnar_memory_size_add(incoming_grid, timestamp_bytes,
            &incoming_grid)) {
            col_rel_reservation_rollback(&metadata_pending);
            return EOVERFLOW;
        }
    }
    pending_rc = col_rel_payload_txn_begin(r, retained_bytes,
            incoming_grid, &pending);
    if (pending_rc < 0) {
        col_rel_reservation_rollback(&metadata_pending);
        if (out_denied && r->memory_budget_denial_pending)
            *out_denied = true;
        return -pending_rc;
    }

#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_metadata_alloc) {
        wl_columnar_relation_test_fail_metadata_alloc = false;
        goto fail;
    }
#endif
    if (ncols > 0) {
        image.columns = col_columns_alloc(ncols, image.capacity);
        if (!image.columns)
            goto fail;
        if (r->memory_governor) {
            image.row_scratch = (int64_t *)malloc((size_t)ncols
                    * sizeof(*image.row_scratch));
            if (!image.row_scratch)
                goto fail;
        }
        image.col_names = (char **)calloc(ncols, sizeof(char *));
        if (!image.col_names)
            goto fail;
        for (uint32_t i = 0; i < ncols; i++) {
            if (col_names && col_names[i]) {
                image.col_names[i] = wl_strdup(col_names[i]);
            } else {
                char buf[32];
                snprintf(buf, sizeof(buf), "col%u", i);
                image.col_names[i] = wl_strdup(buf);
            }
            if (!image.col_names[i])
                goto fail;
        }
    }
    if ((with_timestamps || image.timestamps) && image.capacity > 0
        && (!image.timestamps || r->timestamp_capacity < image.capacity)) {
        image.timestamps = (col_delta_timestamp_t *)calloc(image.capacity,
                sizeof(*image.timestamps));
        if (!image.timestamps)
            goto fail;
        if (r->timestamps && r->nrows)
            memcpy(image.timestamps, r->timestamps,
                (size_t)r->nrows * sizeof(*image.timestamps));
        image.timestamp_capacity = image.capacity;
        image_owns_timestamps = true;
    }
    if (wl_columnar_relation_init_arrow_schema(&image) != 0)
        goto fail;
    if (pending_rc > 0
        && col_rel_publish_payload_txn(r, &pending,
        retained_bytes, NULL) != 0)
        goto fail;
    old_schema = r->schema;
    old_schema_ok = r->schema_ok;
    r->ncols = ncols;
    r->capacity = image.capacity;
    r->columns = image.columns;
    r->row_scratch = image.row_scratch;
    r->col_names = image.col_names;
    if (image_owns_timestamps)
        free(r->timestamps);
    r->timestamps = image.timestamps;
    r->timestamp_capacity = image.timestamp_capacity;
    r->schema = image.schema;
    r->schema_ok = true;
    col_rel_publish_metadata(r, &metadata_pending, metadata_bytes,
        &old_metadata);
    if (old_schema_ok)
        ArrowSchemaRelease(&old_schema);
    col_rel_release_reservation_or_abort(&old_metadata);
    wl_columnar_relation_touch_view(r);
    return 0;

fail:
    if (image.schema.release)
        ArrowSchemaRelease(&image.schema);
    col_columns_free(image.columns, ncols);
    free(image.row_scratch);
    if (image.col_names) {
        for (uint32_t i = 0; i < ncols; i++)
            free(image.col_names[i]);
        free((void *)image.col_names);
    }
    if (image_owns_timestamps)
        free(image.timestamps);
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    wl_columnar_relation_test_note_resize_columns_retired();
#endif
    col_rel_payload_txn_rollback(r, &pending);
    col_rel_reservation_rollback(&metadata_pending);
    if (out_denied && r->memory_budget_denial_pending)
        *out_denied = true;
    return failure_rc;
}

static int
col_rel_set_schema_impl(col_rel_t *r, uint32_t ncols,
    const char *const *col_names)
{
    return col_rel_set_schema_impl_capacity(r, ncols, col_names,
               COL_REL_INIT_CAP, false, NULL);
}

int
col_rel_set_schema(col_rel_t *r, uint32_t ncols, const char *const *col_names)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc;
    int release_rc;

    if (!r)
        return EINVAL;
    rc = col_rel_published_writer_acquire(r, &writer);
    if (rc != 0)
        return rc;
    r->memory_budget_denial_pending = false;
    rc = col_rel_set_schema_impl(r, ncols, col_names);
    if (rc == ENOMEM && r->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC)
        r->memory_budget_denial_pending = false;
    release_rc = wl_columnar_source_access_writer_release(&writer);
    if (rc == 0 && release_rc != 0)
        rc = release_rc;
    return rc;
}

static int
col_rel_prepare_column_types(const col_rel_t *r,
    const wirelog_column_type_t *types, uint32_t ncols,
    wirelog_column_type_t **out_types, struct ArrowSchema *out_schema)
{
    col_rel_t image = { 0 };
    wirelog_column_type_t *copy = NULL;
    if (!r || !out_types || !out_schema || ncols != r->ncols)
        return EINVAL;
    if (ncols > 0) {
        copy = (wirelog_column_type_t *)malloc(
            (size_t)ncols * sizeof(*copy));
        if (!copy)
            return ENOMEM;
        for (uint32_t i = 0; i < ncols; i++)
            copy[i] = types ? types[i] : WIRELOG_TYPE_INT64;
    }
    if (r->nrows > 0 && !r->column_types) {
        for (uint32_t i = 0; i < ncols; i++) {
            if (copy && copy[i] == WIRELOG_TYPE_FLOAT) {
                free(copy);
                return EINVAL;
            }
        }
    }
    for (uint32_t c = 0; c < ncols; c++) {
        if (!copy || copy[c] != WIRELOG_TYPE_FLOAT)
            continue;
        for (uint32_t row = 0; row < r->nrows; row++) {
            if (!wl_columnar_float_bits_valid(r->columns[c][row])) {
                free(copy);
                return EINVAL;
            }
        }
    }

    image.ncols = ncols;
    image.col_names = r->col_names;
    image.column_types = copy;
    if (wl_columnar_relation_init_arrow_schema(&image) != 0) {
        free(copy);
        return ENOMEM;
    }
    *out_schema = image.schema;
    *out_types = copy;
    return 0;
}

static int
col_rel_set_column_types_impl(col_rel_t *r,
    const wirelog_column_type_t *types, uint32_t ncols, bool publish)
{
    wl_columnar_memory_reservation_t pending, old_metadata;
    wirelog_column_type_t *copy = NULL, *old_types;
    struct ArrowSchema prepared_schema, old_schema;
    bool old_schema_ok;
    uint64_t metadata_bytes;
    int rc;

    if (!r || ncols != r->ncols)
        return EINVAL;
    if (!wl_columnar_relation_compound_map_valid(r))
        return EINVAL;
    if (!col_rel_prospective_metadata_bytes(ncols,
        (const char *const *)r->col_names, r->col_names != NULL, false,
        true, true, r->compound_arity_len, r->col_shared != NULL,
        &metadata_bytes))
        return EOVERFLOW;
    rc = col_rel_reserve_metadata(r, metadata_bytes, &pending);
    if (rc != 0)
        return rc;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_metadata_alloc) {
        wl_columnar_relation_test_fail_metadata_alloc = false;
        col_rel_reservation_rollback(&pending);
        return ENOMEM;
    }
#endif
    rc = col_rel_prepare_column_types(r, types, ncols, &copy,
            &prepared_schema);
    if (rc != 0) {
        col_rel_reservation_rollback(&pending);
        return rc;
    }
    old_types = r->column_types;
    old_schema_ok = r->schema_ok;
    old_schema = r->schema;
    r->column_types = copy;
    r->schema = prepared_schema;
    r->schema_ok = true;
    col_rel_publish_metadata(r, &pending, metadata_bytes, &old_metadata);
    free(old_types);
    if (old_schema_ok)
        ArrowSchemaRelease(&old_schema);
    col_rel_release_reservation_or_abort(&old_metadata);
    if (r->column_types) {
        for (uint32_t c = 0; c < ncols; c++) {
            if (r->column_types[c] != WIRELOG_TYPE_FLOAT)
                continue;
            for (uint32_t row = 0; row < r->nrows; row++) {
                if (wl_columnar_float_bits_zero(r->columns[c][row]))
                    r->columns[c][row] = 0;
            }
        }
    }
    if (publish)
        wl_columnar_relation_touch_view(r);
    return 0;
}

int
col_rel_set_column_types(col_rel_t *r, const wirelog_column_type_t *types,
    uint32_t ncols)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc;
    int release_rc;

    if (!r)
        return EINVAL;
    rc = col_rel_published_writer_acquire(r, &writer);
    if (rc != 0)
        return rc;
    r->memory_budget_denial_pending = false;
    rc = col_rel_set_column_types_impl(r, types, ncols, true);
    if (rc == ENOMEM && r->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC)
        r->memory_budget_denial_pending = false;
    release_rc = wl_columnar_source_access_writer_release(&writer);
    if (rc == 0 && release_rc != 0)
        rc = release_rc;
    return rc;
}

int
wl_columnar_relation_alloc_governed(col_rel_t **out, const char *name,
    wl_columnar_memory_governor_ref_t *governor)
{
    col_rel_t *r;
    uint64_t bytes = 0;
    wl_columnar_memory_reservation_t pending;
    bool admitted = false;
    int rc;

    if (!out || !governor)
        return EINVAL;
    *out = NULL;
    if (name && strlen(name) == SIZE_MAX)
        return EOVERFLOW;
    if (!wl_columnar_memory_size_add(sizeof(col_rel_t),
        name ? (uint64_t)strlen(name) + 1u : 0u, &bytes))
        return EOVERFLOW;
    wl_columnar_memory_reservation_init(&pending);
    wl_columnar_memory_admission_status_t status
        = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(governor), bytes, &pending);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
            : EINVAL;
    admitted = true;
    r = (col_rel_t *)calloc(1, sizeof(col_rel_t));
    if (!r) {
        rc = ENOMEM;
        goto fail;
    }
    r->name = name ? wl_strdup(name) : NULL;
    if (name && !r->name) {
        rc = ENOMEM;
        goto fail;
    }
    rc = col_rel_new_identity(&r->relation_identity);
    if (rc != 0) {
        rc = EOVERFLOW;
        goto fail;
    }
    wl_columnar_memory_reservation_init(&r->descriptor_reservation);
    if (!wl_columnar_memory_reservation_move(&r->descriptor_reservation,
        &pending) || !wl_columnar_memory_commit(&r->descriptor_reservation,
        r)) {
        rc = EINVAL;
        goto fail;
    }
    r->descriptor_reserved_bytes = bytes;
    r->memory_governor = governor;
    wl_columnar_memory_governor_ref_retain(governor);
    wl_columnar_memory_reservation_init(&r->metadata_reservation);
    wl_columnar_memory_reservation_init(&r->pool_name_reservation);
    wl_columnar_memory_reservation_init(&r->dedup_reservation);
    wl_columnar_memory_reservation_init(&r->retained_reservation);
    r->view_generation = 1u;
    r->storage_generation = 1u;
    col_rel_storage_owner_init(r);
    *out = r;
    return 0;
fail:
    if (r) {
        if (r->descriptor_reservation.identity
            == &r->descriptor_reservation)
            (void)wl_columnar_memory_rollback(&r->descriptor_reservation);
        free(r->name);
        free(r);
    }
    if (admitted)
        (void)wl_columnar_memory_rollback(&pending);
    return rc;
}

int
col_rel_alloc(col_rel_t **out, const char *name)
{
    if (!out || !name)
        return EINVAL;
    *out = NULL;
    col_rel_t *r = (col_rel_t *)calloc(1, sizeof(col_rel_t));
    if (!r)
        return ENOMEM;
    r->name = wl_strdup(name);
    if (!r->name) {
        free(r);
        return ENOMEM;
    }
    if (col_rel_new_identity(&r->relation_identity) != 0) {
        free(r->name);
        free(r);
        return EOVERFLOW;
    }
    r->view_generation = 1u;
    r->storage_generation = 1u;
    r->pool_owned = false;
    col_rel_storage_owner_init(r);
    *out = r;
    return 0;
}

/* ------------------------------------------------------------------------ */
/* Compound-column layout (Issue #532 Task 2).                              */
/* ------------------------------------------------------------------------ */

/* Reject an INLINE column that violates the inline-tier invariants. Returns
 * EINVAL on violation, 0 when the column is acceptable. Non-INLINE columns
 * are always acceptable at this layer (SIDE widths are handled below and
 * NONE columns are scalars).
 *
 * K-Fusion C1 (§5): INLINE columns must fit within arity<=MAX_ARITY and
 * depth<=MAX_DEPTH; wider/deeper functors must lower to the SIDE tier.
 * Violations are structured logs (error=arity_overflow|depth_overflow) so
 * operators can diagnose schema-mismatch without abort. */
static int
col_rel_validate_inline_col(const col_rel_logical_col_t *col)
{
    if (col->kind != WIRELOG_COMPOUND_KIND_INLINE)
        return 0;
    if (col->arity == 0u || col->arity > WL_COMPOUND_INLINE_MAX_ARITY) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=validate path=inline error=arity_overflow "
            "expected=1..%u got=%u",
            (unsigned)WL_COMPOUND_INLINE_MAX_ARITY, col->arity);
        return EINVAL;
    }
    if (col->depth > WL_COMPOUND_INLINE_MAX_DEPTH) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=validate path=inline error=depth_overflow "
            "expected=0..%u got=%u",
            (unsigned)WL_COMPOUND_INLINE_MAX_DEPTH, col->depth);
        return EINVAL;
    }
    return 0;
}

/* Width contribution of one logical column in the physical schema.
 * NONE/SIDE contribute a single slot (SIDE stores only the opaque handle
 * into the side-relation). INLINE contributes `arity` slots; callers must
 * have already accepted the column via col_rel_validate_inline_col. */
static uint32_t
col_rel_slot_width(const col_rel_logical_col_t *col)
{
    if (col->kind == WIRELOG_COMPOUND_KIND_INLINE)
        return col->arity;
    return 1u;
}

int
col_rel_compute_physical_layout(const col_rel_logical_col_t *logical_cols,
    uint32_t logical_ncols,
    uint32_t *out_physical_ncols,
    uint32_t *out_offset_map,
    uint32_t *out_inline_physical_offset,
    uint32_t *out_compound_count)
{
    if (!logical_cols || logical_ncols == 0u) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=layout error=bad_input logical_cols=%p logical_ncols=%u",
            (const void *)logical_cols, logical_ncols);
        return EINVAL;
    }

    /* Validate before touching outputs so EINVAL leaves everything intact.
    * Per-column failures are already logged inside validate_inline_col. */
    for (uint32_t i = 0; i < logical_ncols; i++) {
        int rc = col_rel_validate_inline_col(&logical_cols[i]);
        if (rc != 0) {
            WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
                "event=layout error=validation logical_col=%u", i);
            return rc;
        }
    }

    uint32_t physical = 0u;
    uint32_t compound_count = 0u;
    uint32_t first_inline_offset = 0u;
    bool saw_inline = false;

    for (uint32_t i = 0; i < logical_ncols; i++) {
        if (out_offset_map)
            out_offset_map[i] = physical;
        if (logical_cols[i].kind == WIRELOG_COMPOUND_KIND_INLINE) {
            if (!saw_inline) {
                first_inline_offset = physical;
                saw_inline = true;
            }
            compound_count++;
        }
        physical += col_rel_slot_width(&logical_cols[i]);
    }

    if (out_physical_ncols)
        *out_physical_ncols = physical;
    if (out_inline_physical_offset)
        *out_inline_physical_offset = first_inline_offset;
    if (out_compound_count)
        *out_compound_count = compound_count;
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_DEBUG,
        "event=layout path=%s logical=%u physical=%u compound=%u "
        "first_inline_offset=%u",
        compound_count > 0u ? "inline" : "scalar",
        logical_ncols, physical, compound_count, first_inline_offset);
    return 0;
}

static int
col_rel_apply_compound_schema_impl(col_rel_t *r,
    const col_rel_logical_col_t *logical_cols,
    uint32_t logical_ncols)
{
    if (!r) {
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_WARN,
            "event=apply_schema error=null_relation");
        return EINVAL;
    }

    /* Reuse the pure layout routine for validation + offset math so the
     * two entry points cannot drift. We don't need the per-column offsets
     * here, but the prefix sum walks every column exactly once. */
    uint32_t compound_count = 0u;
    uint32_t inline_offset = 0u;
    uint32_t physical_ncols = 0u;
    uint64_t map_bytes = 0;
    if (!wl_columnar_memory_size_mul(logical_ncols, sizeof(uint32_t),
        &map_bytes) || map_bytes > SIZE_MAX)
        return EOVERFLOW;
    int rc = col_rel_compute_physical_layout(logical_cols, logical_ncols,
            &physical_ncols, NULL, &inline_offset, &compound_count);
    if (rc != 0)
        return rc;
    if (r->ncols != 0 && physical_ncols != r->ncols)
        return EINVAL;
    if (r->ncols != 0 && !wl_columnar_relation_compound_map_valid(r))
        return EINVAL;
    if (!wl_columnar_relation_generation_valid(r->view_generation)
        || r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EOVERFLOW;

    uint64_t metadata_bytes = 0;
    wl_columnar_memory_reservation_t pending, old_metadata;
    if (!col_rel_prospective_metadata_bytes(r->ncols,
        (const char *const *)r->col_names, r->col_names != NULL, false,
        r->column_types != NULL, r->schema_ok, logical_ncols,
        r->col_shared != NULL,
        &metadata_bytes))
        return EOVERFLOW;
    rc = col_rel_reserve_metadata(r, metadata_bytes, &pending);
    if (rc != 0)
        return rc == ENOSPC ? ENOMEM : rc;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_metadata_alloc) {
        wl_columnar_relation_test_fail_metadata_alloc = false;
        col_rel_reservation_rollback(&pending);
        return ENOMEM;
    }
#endif

    uint32_t *arity_map = logical_ncols
        ? (uint32_t *)malloc((size_t)map_bytes)
        : NULL;
    if (logical_ncols && !arity_map) {
        col_rel_reservation_rollback(&pending);
        WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_ERROR,
            "event=apply_schema error=oom bytes=%zu",
            (size_t)map_bytes);
        return ENOMEM;
    }

    bool has_inline = false;
    bool has_side = false;
    for (uint32_t i = 0; i < logical_ncols; i++) {
        arity_map[i] = col_rel_slot_width(&logical_cols[i]);
        if (logical_cols[i].kind == WIRELOG_COMPOUND_KIND_INLINE)
            has_inline = true;
        else if (logical_cols[i].kind == WIRELOG_COMPOUND_KIND_SIDE)
            has_side = true;
    }

    /* The pending token covers the new map while the old map is still live. */
    uint32_t *old_map = r->compound_arity_map;
    r->compound_arity_map = arity_map;
    r->compound_arity_len = logical_ncols;
    r->compound_count = compound_count;
    r->inline_physical_offset = inline_offset;
    const char *path_tag;
    if (has_inline) {
        r->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
        path_tag = "inline";
    } else if (has_side) {
        r->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        path_tag = "side";
    } else {
        r->compound_kind = WIRELOG_COMPOUND_KIND_NONE;
        path_tag = "none";
    }
    col_rel_publish_metadata(r, &pending, metadata_bytes, &old_metadata);
    free(old_map);
    col_rel_release_reservation_or_abort(&old_metadata);
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_DEBUG,
        "event=apply_schema path=%s rel=%s logical=%u compound_count=%u "
        "inline_offset=%u",
        path_tag, r->name ? r->name : "(anon)", logical_ncols,
        compound_count, inline_offset);
    wl_columnar_relation_touch_view(r);
    return 0;
}

bool
col_rel_retained_bytes_for(const col_rel_t *r, uint32_t capacity,
    uint64_t *out)
{
    if (!r || !out)
        return false;
    if (!col_rel_timestamp_shape_valid(r))
        return false;
    return col_rel_retained_shape_bytes(r->ncols, capacity,
               r->timestamps ? capacity : 0, out);
}

/* Admit and grow the physical capacity of @r to at least @new_cap as one
 * transaction (Issue #1446).  Shared and arena-backed relations take the
 * ownership-transition path; heap-owned relations admit the new footprint
 * (retained relations only), prepare private buffers, commit the token and
 * publish.  A call with @new_cap <= capacity admits the buffers the
 * relation already owns when its retained token does not cover them yet,
 * without reallocating: a fresh heap relation starts with unadmitted
 * capacity and bulk writers must not publish rows into it unadmitted.
 * Nothing observable changes on failure. Only an actual governor DENIED
 * verdict sets the optional denial result. Arithmetic and invalid-state
 * failures retain their typed status. */
static int
wl_columnar_relation_reserve_capacity_impl(col_rel_t *r, uint32_t new_cap,
    bool *denied)
{
    uint32_t target;
    col_rel_payload_txn_t pending;
    int pending_rc;
    uint64_t bytes = 0;

    if (denied)
        *denied = false;
    if (!r || !col_rel_timestamp_shape_valid(r))
        return EINVAL;
    target = new_cap > r->capacity ? new_cap : r->capacity;
    if (target > r->capacity) {
        if (r->col_shared || r->arena_owned) {
            /* Ownership transitions stage columns and timestamps together;
             * admission happens before any source buffer is copied and the
             * helper publishes the storage generation on success. */
            int rc = col_rel_grow_owned_transition(r, target);
            if (denied)
                *denied = rc == ENOMEM && r->memory_budget_denial_pending;
            return rc;
        }
        uint64_t ledger_before = col_rel_owned_ledger_bytes(r);
        bool admitted = r->memory_governor != NULL;
        wl_columnar_memory_reservation_t previous;
        int64_t **new_cols = NULL;
        col_delta_timestamp_t *new_ts = NULL;
        int prepare_rc;

        wl_columnar_memory_reservation_init(&previous);

        pending_rc = 0;
        if (admitted) {
            pending_rc = col_rel_reserve_retained(r, target, &pending);
            if (pending_rc < 0) {
                if (denied)
                    *denied = pending_rc == -ENOMEM;
                return -pending_rc;
            }
            if (!col_rel_payload_private_bytes(r, r->ncols, target,
                r->timestamps ? target : 0, r->ncols != 0, &bytes)) {
                col_rel_payload_txn_rollback(r, &pending);
                return EOVERFLOW;
            }
        }
        prepare_rc = col_rel_prepare_resize(r, target, &new_cols, &new_ts);
        if (prepare_rc != 0) {
            if (admitted)
                col_rel_payload_txn_rollback(r, &pending);
            return prepare_rc;
        }
        if (admitted && pending_rc > 0
            && col_rel_publish_payload_txn(r, &pending, bytes,
            &previous) != 0) {
            col_columns_free(new_cols, r->ncols);
            free(new_ts);
#ifdef WL_TEST_RELATION_RESIZE_HOOK
            wl_columnar_relation_test_note_resize_columns_retired();
#endif
            col_rel_payload_txn_rollback(r, &pending);
            return ENOMEM;
        }
        col_rel_publish_resize(r, new_cols, new_ts, target);
        col_rel_release_retired_reservation(&previous);
        col_rel_retire_payload_credit(r);
        col_rel_ledger_reconcile(r, ledger_before);
        wl_columnar_relation_touch_storage(r);
        return 0;
    }
    /* No reallocation: admit the heap buffers the relation already owns when
     * the retained token does not cover them yet.  A nullary relation owns
     * no column buffers. */
    if (!r->memory_governor || r->col_shared || r->arena_owned)
        return 0;
    if (!col_rel_retained_live_bytes(r, &bytes))
        return EOVERFLOW;
    if (bytes <= r->retained_reserved_bytes)
        return 0;
    wl_columnar_memory_reservation_t existing_pending;
    wl_columnar_memory_reservation_init(&existing_pending);
    wl_columnar_memory_admission_status_t status =
        wl_columnar_memory_reserve_growth(
        wl_columnar_memory_governor_ref_get(r->memory_governor),
        r->retained_reserved_bytes, bytes, &existing_pending);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        bool budget_denied = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED;
        if (denied)
            *denied = budget_denied;
        if (budget_denied)
            r->memory_budget_denial_pending = true;
        return budget_denied ? ENOMEM
            : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
            : EINVAL;
    }
    if (col_rel_publish_retained_reservation(r, &existing_pending, bytes,
        NULL) != 0) {
        col_rel_reservation_rollback(&existing_pending);
        return ENOMEM;
    }
    return 0;
}

int
col_rel_reserve_capacity_admitted(col_rel_t *r, uint32_t new_cap,
    bool *denied)
{
    /* The caller owns an unpublished relation or holds its writer. */
    if (r)
        r->memory_budget_denial_pending = false;
    return wl_columnar_relation_reserve_capacity_impl(r, new_cap, denied);
}

int
col_rel_apply_compound_schema(col_rel_t *r,
    const col_rel_logical_col_t *logical_cols,
    uint32_t logical_ncols)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc;
    int release_rc;

    if (!r)
        return EINVAL;
    rc = col_rel_published_writer_acquire(r, &writer);
    if (rc != 0)
        return rc;
    rc = col_rel_apply_compound_schema_impl(r, logical_cols, logical_ncols);
    release_rc = wl_columnar_source_access_writer_release(&writer);
    if (rc == 0 && release_rc != 0)
        rc = release_rc;
    return rc;
}

/* Whether appending one row to r replaces its storage: a shared view is
 * copied on write, and full column or timestamp capacity grows.  The append
 * publishes a storage transition exactly when this holds, which is why the
 * batch appender below uses it to start a fresh lease before such a row. */
static bool
col_rel_append_replaces_storage(const col_rel_t *r)
{
    return r->col_shared || r->nrows >= r->capacity
           || (r->timestamps && r->nrows >= r->timestamp_capacity);
}

static int
col_rel_append_row_impl(col_rel_t *r, const int64_t *row,
    wl_columnar_source_access_writer_t *writer, bool writer_held,
    wl_columnar_relation_mutation_lease_t *lease)
{
    bool alias_release_pending = false;
    int64_t inline_row[16];
    int64_t *staged_row = NULL;
    wl_columnar_memory_reservation_t row_admission;
    bool row_credit_held = false;
    int rc;
    uint64_t prior_storage_generation = 0;

    wl_columnar_memory_reservation_init(&row_admission);

    if (!r || !row || !writer)
        return EINVAL;
    if (!lease) {
        col_rel_t *owner = NULL;
        /* Transitional raw authority: owner resolution follows its existing
         * stable-binding caller contract; no descriptor gate is acquired.
         * This checks canonical owner authority only: owner-only tokens and
         * tokens pinning a different same-owner descriptor remain legacy
         * callers. Exact target authority requires the two-role consolidation
         * lease migration (#2033). Validate before shape or payload reads. */
        if (!writer_held || !writer->owner
            || writer->identity != (uintptr_t)writer
            || !writer->thread_valid
            || !wl_columnar_source_access_writer_thread_equal(writer)
            || col_rel_storage_owner_resolve(r, &owner) != 0
            || !wl_columnar_relation_retirement_writer_valid(writer,
            &owner->source_access))
            return EINVAL;
        r->memory_budget_denial_pending = false;
    }
    if (lease && (lease->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || col_rel_mutation_lease_validate(lease, r)))
        return EINVAL;
    prior_storage_generation = lease ? lease->relation_generation
        : r->storage_generation;
    if (!col_rel_timestamp_shape_valid(r) || r->nrows > r->capacity)
        return EINVAL;
    bool replaces_storage = col_rel_append_replaces_storage(r);
    if (r->nrows == UINT32_MAX
        || r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || (replaces_storage && r->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u))
        return EOVERFLOW;
    /* Do all structural validation before any resize, COW, or timestamp
     * publication.  In particular, a zero-column or partially initialized
     * relation must fail without consuming capacity or a generation epoch. */
    /* Zero-arity relations are valid empty-tuple relations.  A relation with
     * physical columns, however, must have every column buffer present. */
    if (r->ncols == 0
        && (r->relation_identity == 0 || r->view_generation == 0
        || r->storage_generation == 0))
        return EINVAL;
    bool empty_unallocated = r->nrows == 0 && r->capacity == 0;
    if (r->ncols > 0 && !r->columns && !empty_unallocated)
        return EINVAL;
    for (uint32_t c = 0; c < r->ncols; c++)
        if ((!r->columns || !r->columns[c]) && !empty_unallocated)
            return EINVAL;
    bool row_overlaps = false;
    size_t row_bytes = 0;
    {
        if (r->nrows == UINT32_MAX)
            return EOVERFLOW;
        uint64_t bytes = (uint64_t)r->ncols * sizeof(*row);
        uint64_t column_bytes = (uint64_t)r->capacity * sizeof(*row);
        uintptr_t row_start = (uintptr_t)row;
        if (bytes > SIZE_MAX || bytes > UINTPTR_MAX - row_start)
            return EOVERFLOW;
        row_bytes = (size_t)bytes;
        for (uint32_t c = 0; r->columns && c < r->ncols; c++) {
            uintptr_t column_start = (uintptr_t)r->columns[c];
            if (column_bytes > SIZE_MAX
                || column_bytes > UINTPTR_MAX - column_start)
                return EOVERFLOW;
            if (row_bytes && column_bytes
                && row_start < column_start + column_bytes
                && column_start < row_start + row_bytes)
                row_overlaps = true;
        }
    }
    if (r->column_types) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->column_types[c] == WIRELOG_TYPE_FLOAT
                && !wl_columnar_float_bits_valid(row[c]))
                return EINVAL;
        }
    }

    /* A valid input row may be a slice of a column that publication retires.
     * Capture the complete tuple while its descriptor and source are closed,
     * even when this append currently needs no replacement. */
    if (row_overlaps) {
        if (r->ncols <= sizeof(inline_row) / sizeof(inline_row[0])) {
            memcpy(inline_row, row, row_bytes);
            row = inline_row;
        } else {
            if (r->memory_governor) {
                wl_columnar_memory_admission_status_t status
                    = wl_columnar_memory_reserve_checked(
                        wl_columnar_memory_governor_ref_get(r->memory_governor),
                        row_bytes, &row_admission);
                if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                    && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
                        r->memory_budget_denial_pending = true;
                        rc = ENOMEM;
                    } else
                        rc = status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                            ? EOVERFLOW : EINVAL;
                    goto release_writer;
                }
                /* Rejected tokens retain bytes but own no governor credit. */
                row_credit_held = true;
                if (!wl_columnar_memory_commit(&row_admission, &row_admission))
                    goto enomem;
            }
            staged_row = malloc(row_bytes);
            if (!staged_row)
                goto enomem;
            memcpy(staged_row, row, row_bytes);
            row = staged_row;
        }
    }

    bool needs_resize = r->nrows >= r->capacity;
    bool timestamp_short = r->timestamps
        && r->nrows >= r->timestamp_capacity;
    if (needs_resize || timestamp_short) {
        if (lease && lease->storage_transitioned) {
            rc = EINVAL;
            goto release_writer;
        }
        /* Canonical-owner storage replacement frees the old buffers.  Live
         * shared views still point at those buffers, so refuse growth until
         * the aliases are released.  A shared-view destination is allowed
         * to COW/grow because it replaces its own alias safely. */
        if (r->storage_owner == r
            && col_rel_storage_alias_borrow_count(r) > 0) {
            rc = EBUSY;
            goto release_writer;
        }
        uint64_t ledger_before = col_rel_owned_ledger_bytes(r);
        uint32_t new_cap = needs_resize
            ? (r->capacity ? r->capacity * 2 : COL_REL_INIT_CAP)
            : r->capacity;
        if (needs_resize && new_cap <= r->capacity) {
            rc = EOVERFLOW;
            goto release_writer;
        }
        if (r->col_shared || r->arena_owned || timestamp_short) {
            /* Ownership transitions stage columns and timestamps together;
             * admission happens before any source buffer is copied and the
             * helper publishes the storage generation on success. */
            int transition_rc = lease
                ? col_rel_grow_owned_transition_publish_impl(r, new_cap, false,
                    lease)
                : col_rel_grow_owned_transition_legacy_impl(r, new_cap, true);
            if (transition_rc != 0) {
                rc = transition_rc;
                goto release_writer;
            }
            alias_release_pending = true;
        } else {
            /* Heap-owned growth is one transaction: admit the new footprint
             * (retained relations only), prepare private buffers, commit the
             * token, then publish.  Nothing observable changes on failure. */
            col_rel_payload_txn_t pending;
            int pending_rc = 0;
            uint64_t new_bytes = 0;
            bool admitted = r->memory_governor != NULL;
            wl_columnar_memory_reservation_t previous;
            wl_columnar_memory_reservation_init(&previous);
            if (admitted) {
                pending_rc = col_rel_reserve_retained(r, new_cap, &pending);
                if (pending_rc < 0) {
                    rc = -pending_rc;
                    goto release_writer;
                }
                if (!col_rel_payload_private_bytes(r, r->ncols, new_cap,
                    r->timestamps ? new_cap : 0, r->ncols != 0,
                    &new_bytes)) {
                    col_rel_payload_txn_rollback(r, &pending);
                    rc = EOVERFLOW;
                    goto release_writer;
                }
            }
            int64_t **new_cols = NULL;
            col_delta_timestamp_t *new_ts = NULL;
            int prepare_rc = col_rel_prepare_resize(r, new_cap, &new_cols,
                    &new_ts);
            if (prepare_rc != 0) {
                if (admitted)
                    col_rel_payload_txn_rollback(r, &pending);
                rc = prepare_rc;
                goto release_writer;
            }
            if (admitted && pending_rc > 0
                && col_rel_publish_payload_txn(r, &pending,
                new_bytes, &previous) != 0) {
                col_columns_free(new_cols, r->ncols);
                free(new_ts);
#ifdef WL_TEST_RELATION_RESIZE_HOOK
                wl_columnar_relation_test_note_resize_columns_retired();
#endif
                col_rel_payload_txn_rollback(r, &pending);
                goto enomem;
            }
            col_rel_publish_resize(r, new_cols, new_ts, new_cap);
            col_rel_release_retired_reservation(&previous);
            col_rel_retire_payload_credit(r);
            col_rel_ledger_reconcile(r, ledger_before);
            wl_columnar_relation_touch_storage(r);
            if (lease && col_rel_mutation_lease_advance_storage(lease,
                prior_storage_generation) != 0)
                abort();
        }
    }
    /* A shared view can still have spare capacity.  Privatize it before the
     * in-place row write even when no capacity growth is needed. */
    if (r->col_shared) {
        rc = lease
            ? col_rel_cow_unshare_publish_impl(r, 0, false, lease)
            : col_rel_cow_unshare_legacy_impl(r, 0, true);
        if (rc != 0)
            goto release_writer;
        alias_release_pending = true;
    }
#ifdef WL_TEST_APPEND_HOOK
    if (alias_release_pending && wl_columnar_append_transition_hook)
        wl_columnar_append_transition_hook(r);
#endif
    if (r->timestamps)
        memset(&r->timestamps[r->nrows], 0, sizeof(col_delta_timestamp_t));
    /* Structural and value validation above makes this raw copy
    * non-failing; retain the check as a defensive invariant. */
    if (col_rel_row_copy_in_raw(r, r->nrows, row) != 0) {
        /* All shape/value checks and input staging preceded publication.
         * A leased publication cannot report recoverable partial failure. */
        if (lease)
            abort();
        goto einval;
    }
    if (r->column_types) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->column_types[c] == WIRELOG_TYPE_FLOAT
                && wl_columnar_float_bits_zero(r->columns[c][r->nrows]))
                r->columns[c][r->nrows] = 0;
        }
    }
    r->nrows++;
    wl_columnar_relation_touch_view(r);
    rc = 0;
    goto release_writer;

enomem:
    rc = ENOMEM;
    goto release_writer;
einval:
    rc = EINVAL;
release_writer:
    free(staged_row);
    if (row_credit_held)
        col_rel_release_reservation_or_abort(&row_admission);
    if (!lease && alias_release_pending) {
        int alias_rc = col_rel_storage_alias_release(r);
        if (alias_rc != 0 && rc == 0)
            rc = alias_rc;
    }
    if (!lease && !writer_held
        && wl_columnar_source_access_writer_release(writer) != 0 && rc == 0)
        rc = EINVAL;
    return rc;
}

int
col_rel_append_row(col_rel_t *r, const int64_t *row)
{
    wl_columnar_relation_mutation_single_t single = { 0 };
    if (!r || !row)
        return EINVAL;
    int rc = col_rel_mutation_single_acquire(r, &single);
    if (rc)
        return rc;
    /* Outer attempt provenance starts only after both admissions succeed. */
    r->memory_budget_denial_pending = false;
    rc = col_rel_append_row_impl(r, row,
            &single.owners[single.lease.owner_slot].writer, true,
            &single.lease);
    int finish_rc = col_rel_mutation_set_finish(&single.set, rc == 0);
    return rc ? rc : finish_rc;
}

int
col_rel_append_row_with_lease(col_rel_t *r, const int64_t *row,
    wl_columnar_relation_mutation_lease_t *lease)
{
    if (!r || !row || !lease || lease->role_flags
        != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || col_rel_mutation_lease_validate(lease, r))
        return EINVAL;
    return col_rel_append_row_impl(r, row,
               &lease->set->owners[lease->owner_slot].writer, true, lease);
}

int
col_rel_append_batch_begin(col_rel_append_batch_t *batch, col_rel_t *r)
{
    if (!batch || !r || batch->active)
        return EINVAL;
    memset(batch, 0, sizeof(*batch));
    int rc = col_rel_mutation_single_acquire(r, &batch->single);
    if (rc) {
        memset(batch, 0, sizeof(*batch));
        return rc;
    }
    batch->relation = r;
    batch->active = true;
    return 0;
}

int
col_rel_append_batch_row(col_rel_append_batch_t *batch, const int64_t *row)
{
    if (!batch || !batch->active || !row)
        return EINVAL;
    col_rel_t *r = batch->relation;
    /* A lease publishes at most one storage transition.  Once it has, a row
     * that would replace storage again starts the next epoch's lease. */
    if (batch->single.lease.storage_transitioned
        && col_rel_append_replaces_storage(r)) {
        int rc = col_rel_mutation_set_finish(&batch->single.set, true);
        if (rc == 0) {
            memset(&batch->single, 0, sizeof(batch->single));
            rc = col_rel_mutation_single_acquire(r, &batch->single);
        }
        if (rc) {
            batch->active = false;
            return rc;
        }
    }
    /* Each row's denial evidence starts clean, as for col_rel_append_row. */
    r->memory_budget_denial_pending = false;
    return col_rel_append_row_impl(r, row,
               &batch->single.owners[batch->single.lease.owner_slot].writer,
               true, &batch->single.lease);
}

int
col_rel_append_batch_end(col_rel_append_batch_t *batch, bool commit)
{
    if (!batch || !batch->active)
        return 0;
    batch->active = false;
    return col_rel_mutation_set_finish(&batch->single.set, commit);
}

static int
col_rel_capacity_for_rows(uint32_t current, uint32_t required,
    uint32_t *out_capacity)
{
    uint32_t capacity = current ? current : COL_REL_INIT_CAP;

    if (!out_capacity)
        return EINVAL;
    while (capacity < required) {
        if (capacity > UINT32_MAX / 2u)
            return EOVERFLOW;
        capacity *= 2u;
    }
    *out_capacity = capacity;
    return 0;
}

static int wl_columnar_relation_reserve_rows_impl(col_rel_t *r,
    uint32_t additional,
    wl_columnar_source_access_writer_t *writer,
    bool *out_alias_release_pending, bool begin_operation,
    wl_columnar_relation_mutation_lease_t *lease);

int
col_rel_append_rows_atomic(col_rel_t *r, const int64_t *rows,
    uint32_t num_rows, uint32_t num_cols, bool *denied)
{
    wl_columnar_relation_mutation_single_t single = { 0 };
    wl_columnar_memory_reservation_t scratch_admission;
    int64_t inline_rows[32];
    int64_t *staged_rows = NULL;
    bool scratch_credit_held = false;
    bool overlaps = false;
    bool schema_was_unset;
    uint32_t required_rows;
    uint32_t capacity;
    uint64_t cells;
    uint64_t input_bytes;
    int rc;

    if (denied)
        *denied = false;
    if (!r || (!rows && num_rows != 0))
        return EINVAL;
    if (num_rows == 0)
        return 0;
    if (!wl_columnar_memory_size_mul(num_rows, num_cols, &cells)
        || !wl_columnar_memory_size_mul(cells, sizeof(int64_t),
        &input_bytes)
        || input_bytes > SIZE_MAX)
        return EOVERFLOW;
    wl_columnar_memory_reservation_init(&scratch_admission);
    rc = col_rel_mutation_single_acquire(r, &single);
    if (rc != 0)
        return rc;

    /* Consume stale denial evidence at this operation boundary.  A later
     * ENOMEM is called a budget denial only if this operation sets the bit. */
    r->memory_budget_denial_pending = false;

    if (!col_rel_timestamp_shape_valid(r) || r->nrows > r->capacity) {
        rc = EINVAL;
        goto finish;
    }
    if (num_rows > UINT32_MAX - r->nrows) {
        rc = EOVERFLOW;
        goto finish;
    }
    required_rows = r->nrows + num_rows;
    schema_was_unset = !r->schema_ok;
    if (!schema_was_unset && num_cols != r->ncols) {
        rc = EINVAL;
        goto finish;
    }
    if (r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID
        - (schema_was_unset ? 2u : 1u)
        || ((required_rows > r->capacity || r->col_shared || r->arena_owned
        || (r->timestamps && required_rows > r->timestamp_capacity))
        && r->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)) {
        rc = EOVERFLOW;
        goto finish;
    }

    /* Validate the entire batch before COW, capacity, timestamps, rows, or
     * generations can change. */
    if (r->ncols > 0) {
        bool empty_unallocated = r->nrows == 0 && r->capacity == 0;
        if ((!r->columns && !empty_unallocated)
            || r->capacity < r->nrows) {
            rc = EINVAL;
            goto finish;
        }
        for (uint32_t c = 0; r->columns && c < r->ncols; c++) {
            if (!r->columns[c]) {
                rc = EINVAL;
                goto finish;
            }
        }
    }
    /* Arrow metadata describes prospective types even before physical columns
     * have been published. Never index an unverified prospective width. */
    if (r->column_types) {
        if (schema_was_unset && (r->schema.n_children < 0
            || (uint64_t)r->schema.n_children != num_cols)) {
            rc = EINVAL;
            goto finish;
        }
    }
    uintptr_t input_start = (uintptr_t)rows;
    if (input_bytes > UINTPTR_MAX - input_start) {
        rc = EOVERFLOW;
        goto finish;
    }
    uint64_t column_bytes;
    if (!wl_columnar_memory_size_mul(r->capacity, sizeof(int64_t),
        &column_bytes) || column_bytes > SIZE_MAX) {
        rc = EOVERFLOW;
        goto finish;
    }
    for (uint32_t c = 0; r->columns && c < r->ncols; c++) {
        uintptr_t column_start = (uintptr_t)r->columns[c];
        if (column_bytes > UINTPTR_MAX - column_start) {
            rc = EOVERFLOW;
            goto finish;
        }
        if (input_bytes && column_bytes
            && input_start < column_start + column_bytes
            && column_start < input_start + input_bytes)
            overlaps = true;
    }
    if (r->column_types) {
        for (uint32_t row = 0; row < num_rows; row++) {
            for (uint32_t col = 0; col < num_cols; col++) {
                if (r->column_types[col] == WIRELOG_TYPE_FLOAT
                    && !wl_columnar_float_bits_valid(
                        rows[(size_t)row * num_cols + col])) {
                    rc = EINVAL;
                    goto finish;
                }
            }
        }
    }
    if (overlaps) {
        if (input_bytes <= sizeof(inline_rows)) {
            memcpy(inline_rows, rows, (size_t)input_bytes);
            rows = inline_rows;
        } else {
            if (r->memory_governor) {
                wl_columnar_memory_admission_status_t status
                    = wl_columnar_memory_reserve_checked(
                        wl_columnar_memory_governor_ref_get(r->memory_governor),
                        input_bytes, &scratch_admission);
                if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                    && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
                        r->memory_budget_denial_pending = true;
                        rc = ENOMEM;
                    } else
                        rc = status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                            ? EOVERFLOW : EINVAL;
                    goto finish;
                }
                scratch_credit_held = true;
                if (!wl_columnar_memory_commit(&scratch_admission,
                    &scratch_admission)) {
                    rc = ENOMEM;
                    goto finish;
                }
            }
            staged_rows = malloc((size_t)input_bytes);
            if (!staged_rows) {
                rc = ENOMEM;
                goto finish;
            }
            memcpy(staged_rows, rows, (size_t)input_bytes);
            rows = staged_rows;
        }
    }

    /* Publish a lazy schema only after every validation that can reject the
     * batch has completed.  Schema construction itself performs admission
     * and rolls back on allocation failure; its capacity covers the batch,
     * so no fallible reserve is needed after schema publication. */
    if (schema_was_unset) {
        rc = col_rel_capacity_for_rows(r->capacity, required_rows,
                &capacity);
        if (rc != 0)
            goto finish;
        rc = col_rel_set_schema_impl_capacity(r, num_cols, NULL, capacity,
                false, denied);
        if (rc != 0)
            goto finish;
    }

    if (!schema_was_unset) {
        rc = wl_columnar_relation_reserve_rows_impl(r, num_rows, NULL,
                NULL, false, &single.lease);
        if (rc != 0)
            goto finish;
    } else {
        /* No recoverable work remains after schema publication. */
        assert(r->schema_ok && r->ncols == num_cols);
        assert(r->capacity >= required_rows);
        assert(col_rel_timestamp_shape_valid(r));
    }

    /* Capacity/admission and COW are now complete.  The prevalidated raw
     * copies below do not allocate and cannot fail while this writer is held. */
    if (r->timestamps)
        memset(&r->timestamps[r->nrows], 0,
            (size_t)num_rows * sizeof(*r->timestamps));
    if (col_rel_rows_copy_in_raw(r, r->nrows, rows, num_rows) != 0)
        abort();
    r->nrows = required_rows;
    wl_columnar_relation_touch_view(r);
    rc = 0;
    if (denied)
        *denied = false;

finish:
    free(staged_rows);
    if (scratch_credit_held)
        col_rel_release_reservation_or_abort(&scratch_admission);
    int finish_rc = col_rel_mutation_set_finish(&single.set, rc == 0);
    if (rc == 0)
        rc = finish_rc;
    if (rc != 0 && denied && r->memory_budget_denial_pending)
        *denied = true;
    return rc;
}

/* Reserve the complete destination shape before a multi-row operation starts.
 * The caller holds the destination writer.  This keeps subsequent locked
 * appends allocation-free, so a destination admission failure cannot leave
 * a partially emitted delta after the source has been transformed. */
static int
wl_columnar_relation_reserve_rows_impl(col_rel_t *r, uint32_t additional,
    wl_columnar_source_access_writer_t *writer,
    bool *out_alias_release_pending, bool begin_operation,
    wl_columnar_relation_mutation_lease_t *lease)
{
    col_rel_t *owner = NULL;
    int rc;
    uint64_t prior_storage_generation = 0;
    if (!r)
        return EINVAL;
    if (lease) {
        if (writer || out_alias_release_pending || begin_operation
            || lease->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
            || col_rel_mutation_lease_validate(lease, r))
            return EINVAL;
        prior_storage_generation = lease->relation_generation;
    } else {
        if (!writer || !out_alias_release_pending)
            return EINVAL;
        rc = col_rel_storage_owner_resolve(r, &owner);
        if (rc != 0 || writer->owner != &owner->source_access
            || writer->identity != (uintptr_t)writer
            || !wl_columnar_source_access_writer_thread_equal(writer))
            return rc != 0 ? rc : EINVAL;
        if (begin_operation)
            r->memory_budget_denial_pending = false;
        *out_alias_release_pending = false;
    }
    if (additional > UINT32_MAX - r->nrows)
        return EOVERFLOW;
    uint32_t required = r->nrows + additional;
    bool alias_release_pending = r->storage_owner
        && r->storage_owner != r;
    /* A shared view can have spare capacity but still needs COW before the
     * first locked append.  Perform that fallible transition up front. */
    bool timestamp_short = r->timestamps
        && required > r->timestamp_capacity;
    bool will_replace_storage = (required <= r->capacity && timestamp_short)
        || (r->col_shared && required <= r->capacity)
        || required > r->capacity;
    if (lease && lease->storage_transitioned && will_replace_storage)
        return EINVAL;
    if (required <= r->capacity && timestamp_short) {
        if (r->storage_owner == r
            && col_rel_storage_alias_borrow_count(r) > 0)
            return EBUSY;
        int rc = lease
            ? col_rel_grow_owned_transition_publish_impl(r, r->capacity,
                false, lease)
            : col_rel_grow_owned_transition_legacy_impl(r, r->capacity, true);
        if (rc == 0 && !lease)
            *out_alias_release_pending = alias_release_pending;
        return rc;
    }
    if (r->col_shared && required <= r->capacity) {
        int rc = lease
            ? col_rel_cow_unshare_publish_impl(r, 0, false, lease)
            : col_rel_cow_unshare_legacy_impl(r, 0, true);
        if (rc == 0 && !lease)
            *out_alias_release_pending = alias_release_pending;
        return rc;
    }
    if (required <= r->capacity)
        return wl_columnar_relation_reserve_capacity_impl(r, r->capacity, NULL);
    if (r->storage_owner == r
        && col_rel_storage_alias_borrow_count(r) > 0)
        return EBUSY;

    uint32_t new_cap = r->capacity ? r->capacity : COL_REL_INIT_CAP;
    while (new_cap < required) {
        if (new_cap > UINT32_MAX / 2u)
            return EOVERFLOW;
        new_cap *= 2u;
    }
    if (r->col_shared || r->arena_owned) {
        int rc = lease
            ? col_rel_grow_owned_transition_publish_impl(r, new_cap, false,
                lease)
            : col_rel_grow_owned_transition_legacy_impl(r, new_cap, true);
        if (rc == 0 && !lease)
            *out_alias_release_pending = alias_release_pending;
        return rc;
    }

    uint64_t ledger_before = col_rel_owned_ledger_bytes(r);
    col_rel_payload_txn_t pending;
    int pending_rc = 0;
    uint64_t new_bytes = 0;
    bool admitted = r->memory_governor != NULL;
    wl_columnar_memory_reservation_t previous;
    wl_columnar_memory_reservation_init(&previous);
    if (admitted) {
        pending_rc = col_rel_reserve_retained(r, new_cap, &pending);
        if (pending_rc < 0)
            return -pending_rc;
        if (!col_rel_payload_private_bytes(r, r->ncols, new_cap,
            r->timestamps ? new_cap : 0, r->ncols != 0, &new_bytes)) {
            if (pending_rc > 0)
                col_rel_payload_txn_rollback(r, &pending);
            return EOVERFLOW;
        }
    }
    int64_t **new_cols = NULL;
    col_delta_timestamp_t *new_ts = NULL;
    rc = col_rel_prepare_resize(r, new_cap, &new_cols, &new_ts);
    if (rc != 0) {
        if (admitted)
            col_rel_payload_txn_rollback(r, &pending);
        return rc;
    }
    if (admitted && pending_rc > 0
        && col_rel_publish_payload_txn(r, &pending, new_bytes,
        &previous) != 0) {
        col_columns_free(new_cols, r->ncols);
        free(new_ts);
#ifdef WL_TEST_RELATION_RESIZE_HOOK
        wl_columnar_relation_test_note_resize_columns_retired();
#endif
        col_rel_payload_txn_rollback(r, &pending);
        return ENOMEM;
    }
    col_rel_publish_resize(r, new_cols, new_ts, new_cap);
    col_rel_release_retired_reservation(&previous);
    col_rel_retire_payload_credit(r);
    col_rel_ledger_reconcile(r, ledger_before);
    wl_columnar_relation_touch_storage(r);
    if (lease && col_rel_mutation_lease_advance_storage(lease,
        prior_storage_generation) != 0)
        abort();
    return 0;
}

int
col_rel_reserve_rows_with_lease(col_rel_t *r, uint32_t additional,
    wl_columnar_relation_mutation_lease_t *lease)
{
    if (!r || !lease || lease->role_flags
        != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || col_rel_mutation_lease_validate(lease, r))
        return EINVAL;
    return wl_columnar_relation_reserve_rows_impl(r, additional, NULL, NULL,
               false, lease);
}

int
col_rel_reset_rows_locked(col_rel_t *r,
    wl_columnar_source_access_writer_t *writer)
{
    col_rel_t *owner = NULL;
    if (!r || !writer || !writer->owner
        || writer->identity != (uintptr_t)writer
        || !writer->thread_valid
        || !wl_columnar_source_access_writer_thread_equal(writer)
        || col_rel_storage_owner_resolve(r, &owner) != 0
        || writer->owner != &owner->source_access)
        return EINVAL;
    if (col_rel_storage_alias_borrow_count(owner) > 0)
        return EBUSY;
    r->nrows = 0;
    r->sorted_nrows = 0;
    r->base_nrows = 0;
    r->run_count = 0;
    memset(r->run_ends, 0, sizeof(r->run_ends));
    uint64_t ledger_before = col_rel_owned_ledger_bytes(r);
    free(r->timestamps);
    r->timestamps = NULL;
    r->timestamp_capacity = 0;
    wl_columnar_eval_dedup_set_clear(r);
    col_rel_retire_payload_credit(r);
    col_rel_ledger_reconcile(r, ledger_before);
    wl_columnar_relation_touch_replacement(r);
    return 0;
}

static void
wl_columnar_relation_delta_ledger_columns(col_rel_t *rel, uint64_t before)
{
    if (!rel->mem_ledger)
        return;
    uint64_t after = col_rel_owned_ledger_bytes(rel);
    if (after > before)
        wl_mem_ledger_alloc(rel->mem_ledger, WL_MEM_SUBSYS_RELATION,
            after - before);
    else if (before > after)
        wl_mem_ledger_free(rel->mem_ledger, WL_MEM_SUBSYS_RELATION,
            before - after);
    /* Timestamp storage is unchanged even though logical capacity changes.
     * Keep its physical charge until ordinary growth or destruction frees it. */
}

/* Delta snapshots change only row storage; schema and auxiliary buffers stay
 * with the registered descriptor. Aliases retire under a descriptor writer
 * and an owner reader, so sibling aliases need not compete for a writer. */
int
wl_columnar_relation_delta_detach(col_rel_t *rel, uint64_t expected_identity)
{
    wl_columnar_source_access_writer_t descriptor = { 0 }, writer = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_t *owner = NULL;
    uint64_t before;
    bool had_shared;
    int rc;
    if (!rel)
        return EINVAL;
    rc = wl_columnar_source_access_writer_acquire(&rel->descriptor_access,
            &descriptor);
    if (rc != 0)
        return rc;
    if (rel->relation_identity != expected_identity
        || col_rel_storage_owner_resolve(rel, &owner) != 0) {
        rc = EINVAL;
        goto done;
    }
    if (rel->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || rel->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u) {
        rc = EOVERFLOW;
        goto done;
    }
    if (owner == rel) {
        rc = wl_columnar_source_access_writer_acquire(&owner->source_access,
                &writer);
        if (rc == 0 && col_rel_storage_alias_borrow_count(owner) != 0)
            rc = EBUSY;
    } else {
        rc = wl_columnar_source_access_reader_acquire(&owner->source_access,
                &reader);
    }
    if (rc != 0)
        goto done;
    before = col_rel_owned_ledger_bytes(rel);
    had_shared = rel->col_shared != NULL;
    /* The temporary owner reader keeps the old root alive after its alias
     * contribution is removed and until all borrowed pointers are detached. */
    rc = col_rel_storage_alias_release(rel);
    if (rc != 0)
        goto done;
    if (rel->columns) {
        if (!rel->arena_owned)
            for (uint32_t c = 0; c < rel->ncols; c++)
                if (!rel->col_shared || !rel->col_shared[c])
                    free(rel->columns[c]);
        free((void *)rel->columns);
    }
    free(rel->col_shared);
    rel->columns = NULL;
    rel->col_shared = NULL;
    if (had_shared)
        col_rel_retire_shared_table_credit(rel);
    rel->arena_owned = false;
    rel->nrows = 0;
    rel->capacity = 0;
    col_rel_retire_payload_credit(rel);
    rel->sorted_nrows = 0;
    rel->run_count = 0;
    memset(rel->run_ends, 0, sizeof(rel->run_ends));
    wl_columnar_relation_delta_ledger_columns(rel, before);
    wl_columnar_relation_touch_replacement(rel);
done:
    if (reader.owner)
        (void)wl_columnar_source_access_reader_release(&reader);
    if (writer.owner)
        (void)wl_columnar_source_access_writer_release(&writer);
    (void)wl_columnar_source_access_writer_release(&descriptor);
    return rc;
}

static int
wl_columnar_relation_delta_restore_flat_impl(col_rel_t *rel,
    uint64_t expected_identity, const int64_t *rows,
    uint32_t nrows, uint32_t ncols, bool overwrite)
{
    wl_columnar_source_access_writer_t descriptor = { 0 }, writer = { 0 };
    col_rel_payload_txn_t pending;
    wl_columnar_memory_reservation_t previous;
    col_rel_t *owner = NULL;
    int64_t **columns = NULL;
    int64_t **old_columns = NULL;
    bool *old_col_shared = NULL;
    bool old_arena_owned = false;
    uint64_t bytes = 0;
    int pending_rc = 0, rc;
    if (!rel || !rows || !nrows || !ncols)
        return EINVAL;
    col_rel_payload_txn_init(&pending);
    wl_columnar_memory_reservation_init(&previous);
    rc = wl_columnar_source_access_writer_acquire(&rel->descriptor_access,
            &descriptor);
    if (rc != 0)
        return rc;
    if (rel->relation_identity != expected_identity) {
        rc = EINVAL;
        goto done;
    }
    /* Preserve populated partial evaluation results unless the caller is the
     * incomplete-evaluation rollback path and explicitly requests replacement.
     * The overwrite path still allocates the replacement before publishing it.
     */
    if (rel->columns && !overwrite) {
        rc = 0;
        goto done;
    }
    if (rel->ncols != ncols
        || !col_rel_timestamp_shape_valid(rel)
        || (rel->timestamps && rel->timestamp_capacity < nrows)
        || col_rel_storage_owner_resolve(rel, &owner) != 0 || owner != rel) {
        rc = EINVAL;
        goto done;
    }
    rc = wl_columnar_source_access_writer_acquire(&rel->source_access, &writer);
    if (rc != 0)
        goto done;
    if (col_rel_storage_alias_borrow_count(rel) != 0) {
        rc = EBUSY;
        goto done;
    }
    if (rel->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || rel->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || !col_rel_payload_private_bytes(rel, ncols, nrows,
        rel->timestamp_capacity, ncols != 0, &bytes)) {
        rc = EOVERFLOW;
        goto done;
    }
    pending_rc = col_rel_reserve_retained_shape(rel, nrows,
            rel->timestamp_capacity, true, false, false, &pending);
    if (pending_rc < 0) {
        rc = ENOMEM;
        goto done;
    }
    columns = col_columns_alloc(ncols, nrows);
    if (!columns) {
        rc = ENOMEM;
        goto done;
    }
    for (uint32_t row = 0; row < nrows; row++)
        for (uint32_t col = 0; col < ncols; col++)
            columns[col][row] = rows[(size_t)row * ncols + col];
    if (pending_rc > 0
        && col_rel_publish_payload_txn(rel, &pending, bytes,
        &previous) != 0) {
        rc = ENOMEM;
        goto done;
    }
    uint64_t before = col_rel_owned_ledger_bytes(rel);
    if (overwrite && rel->columns) {
        old_columns = rel->columns;
        old_col_shared = rel->col_shared;
        old_arena_owned = rel->arena_owned;
    }
    rel->columns = columns;
    columns = NULL;
    rel->col_shared = NULL;
    rel->arena_owned = false;
    rel->nrows = nrows;
    rel->capacity = nrows;
    rel->sorted_nrows = nrows;
    rel->run_count = 1;
    rel->run_ends[0] = nrows;
    wl_columnar_relation_delta_ledger_columns(rel, before);
    wl_columnar_relation_touch_replacement(rel);
    if (old_columns) {
        if (!old_arena_owned)
            for (uint32_t c = 0; c < rel->ncols; c++)
                if (!old_col_shared || !old_col_shared[c])
                    free(old_columns[c]);
        free((void *)old_columns);
        free(old_col_shared);
        if (old_col_shared)
            col_rel_retire_shared_table_credit(rel);
    }
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (old_columns && wl_columnar_relation_test_after_retired_storage_free)
        wl_columnar_relation_test_after_retired_storage_free(rel);
#endif
    col_rel_release_retired_reservation(&previous);
    col_rel_retire_payload_credit(rel);
    rc = 0;
done:
    if (columns)
        col_columns_free(columns, ncols);
    if (rc != 0 && pending_rc > 0)
        col_rel_payload_txn_rollback(rel, &pending);
    if (writer.owner)
        (void)wl_columnar_source_access_writer_release(&writer);
    (void)wl_columnar_source_access_writer_release(&descriptor);
    return rc;
}

int
wl_columnar_relation_delta_restore_flat(col_rel_t *rel,
    uint64_t expected_identity, const int64_t *rows,
    uint32_t nrows, uint32_t ncols)
{
    return wl_columnar_relation_delta_restore_flat_impl(rel,
               expected_identity, rows, nrows, ncols, false);
}

int
wl_columnar_relation_delta_restore_flat_overwrite(col_rel_t *rel,
    uint64_t expected_identity, const int64_t *rows,
    uint32_t nrows, uint32_t ncols)
{
    return wl_columnar_relation_delta_restore_flat_impl(rel,
               expected_identity, rows, nrows, ncols, true);
}

/* Copy all rows from src into dst (must have same ncols).
 * If src has timestamps and dst has timestamp tracking enabled, the source
 * timestamps are propagated to the newly appended rows.
 * Optimized (Issue #300): bulk memcpy instead of per-row append. */
static int
col_rel_append_all_impl(col_rel_t *dst, const col_rel_t *src,
    wl_arena_t *arena, bool require_empty)
{
    col_rel_t *src_owner = NULL;
    col_rel_t *dst_owner = NULL;
    wl_columnar_source_access_reader_t source_reader = { 0 };
    wl_columnar_source_access_writer_t destination_writer = { 0 };
    bool source_reader_acquired = false;
    bool destination_writer_acquired = false;
    bool alias_release_pending = false;
    wirelog_column_type_t *prepared_types = NULL;
    struct ArrowSchema prepared_schema;
    bool prepared_schema_ok = false;
    wl_columnar_memory_reservation_t metadata_pending, old_metadata;
    uint64_t metadata_bytes = 0;
    int rc;
    (void)arena;
    if (!dst || !src)
        return EINVAL;
    wl_columnar_memory_reservation_init(&metadata_pending);
    if (!require_empty && dst->ncols != src->ncols)
        return EINVAL;
    if (!require_empty
        && (!wl_columnar_relation_compound_map_valid(dst)
        || !wl_columnar_relation_compound_map_valid(src)))
        return EINVAL;
    if (!require_empty && src->nrows == 0) {
        /* An empty source still completes a destination ownership
         * transition.  In particular, a reused TDD destination may be a
         * shared view whose old source lease must not survive a successful
         * fallback merely because there are no rows to copy. */
        if (dst->col_shared) {
            wl_columnar_source_access_writer_t writer = { 0 };
            rc = col_rel_source_writer_acquire(dst, &writer);
            if (rc != 0)
                return rc;
            /* Explicit temporary legacy path: append_all already owns its
             * source writer and migrates as a complete read-source unit. */
            rc = col_rel_cow_unshare_legacy_impl(dst, 0, false);
            if (wl_columnar_source_access_writer_release(&writer) != 0
                && rc == 0)
                rc = EINVAL;
            return rc;
        }
        return 0;
    }

    /* Preserve the historical zero-initialized, zero-column arithmetic path.
     * These stack relations have no generation or owner metadata, so owner
     * resolution would turn a formerly valid row-count operation into EINVAL.
     * Initialized zero-column relations have an owner and use the normal
     * source-access gate below. */
    if (!require_empty && dst->ncols == 0 && src->ncols == 0
        && !dst->storage_owner && !src->storage_owner) {
        if (src->nrows > UINT32_MAX - dst->nrows)
            return EOVERFLOW;
        dst->nrows += src->nrows;
        wl_columnar_relation_touch_view(dst);
        return 0;
    }

    /* Resolve canonical owners before reading mutable source or destination
     * state.  A same-owner append uses one writer token: the source reader
     * and destination writer cannot coexist on one gate. */
    rc = col_rel_storage_owner_resolve(src, &src_owner);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(dst, &dst_owner);
    if (rc != 0)
        return rc;
    if (src_owner == dst_owner) {
        rc = wl_columnar_source_access_writer_acquire(
            &dst_owner->source_access, &destination_writer);
        if (rc != 0)
            return rc;
        destination_writer_acquired = true;
    } else {
        rc = wl_columnar_source_access_reader_acquire(
            &src_owner->source_access, &source_reader);
        if (rc != 0)
            return rc;
        source_reader_acquired = true;
        rc = wl_columnar_source_access_writer_acquire(
            &dst_owner->source_access, &destination_writer);
        if (rc != 0)
            goto cleanup;
        destination_writer_acquired = true;
    }

    if (require_empty) {
        /* This check is paired with the writer admission above.  A caller
         * may have observed an empty destination before another writer
         * populated it; appending after that race would lose sorted order. */
        if (dst->nrows != 0) {
            rc = EBUSY;
            goto cleanup;
        }
        if (dst->ncols != src->ncols
            || !wl_columnar_relation_compound_map_valid(dst)
            || !wl_columnar_relation_compound_map_valid(src)) {
            rc = EINVAL;
            goto cleanup;
        }
        if (src->nrows == 0) {
            /* The canonical writer is already held.  Use the under-writer
             * path so public COW may acquire its own gate independently. */
            if (dst->col_shared) {
                dst->memory_budget_denial_pending = false;
                rc = col_rel_cow_unshare_legacy_impl(dst, 0, false);
            } else {
                rc = 0;
            }
            goto cleanup;
        }
    }

    /* A typed relation must never receive lanes whose physical meaning is
     * unknown or differs from its own.  Legacy untyped relations are treated
     * as int64, which preserves the pre-typed append contract. */
    if (dst->column_types || src->column_types) {
        for (uint32_t c = 0; c < dst->ncols; c++) {
            wirelog_column_type_t dst_type = dst->column_types
                ? dst->column_types[c] : WIRELOG_TYPE_INT64;
            wirelog_column_type_t src_type = src->column_types
                ? src->column_types[c] : WIRELOG_TYPE_INT64;
            /* Existing integer-family relations all use the same int64
             * storage lane, and legacy producers do not carry metadata.
             * Float lanes are different: accepting an untyped or differently
             * typed float source would reinterpret its bits. */
            if ((dst_type == WIRELOG_TYPE_FLOAT
                || src_type == WIRELOG_TYPE_FLOAT)
                && dst_type != src_type)
                goto invalid;
            if (!dst->column_types && src->column_types
                && src_type == WIRELOG_TYPE_FLOAT && dst->nrows > 0)
                goto invalid;
        }
    }

    if (src->column_types) {
        for (uint32_t row = 0; row < src->nrows; row++) {
            for (uint32_t c = 0; c < src->ncols; c++) {
                if (src->column_types[c] == WIRELOG_TYPE_FLOAT
                    && !wl_columnar_float_bits_valid(src->columns[c][row]))
                    goto invalid;
            }
        }
        if (!dst->column_types) {
            for (uint32_t row = 0; row < dst->nrows; row++) {
                for (uint32_t c = 0; c < dst->ncols; c++) {
                    if (src->column_types[c] == WIRELOG_TYPE_FLOAT
                        && !wl_columnar_float_bits_valid(
                            dst->columns[c][row]))
                        goto invalid;
                }
            }
        }
    }

    if (src->nrows > UINT32_MAX - dst->nrows)
        goto overflow;

    uint32_t dst_base = dst->nrows;
    uint32_t new_nrows = dst->nrows + src->nrows;

    /* Replacing canonical-owner storage frees buffers still referenced by
     * live aliases.  Keep the raw-pointer alias model safe by refusing only
     * owner growth; alias destinations may still detach and grow privately. */
    if (dst == dst_owner
        && col_rel_storage_alias_borrow_count(dst_owner) > 0
        && (new_nrows > dst->capacity
        || (dst->timestamps
        && new_nrows > dst->timestamp_capacity))) {
        rc = EBUSY;
        goto cleanup;
    }

    /* Type metadata is part of the append transaction.  Prepare its owned
     * copy and complete Arrow schema before resizing or changing dst. */
    if (!dst->column_types && src->column_types) {
        if (!col_rel_prospective_metadata_bytes(dst->ncols,
            (const char *const *)dst->col_names, dst->col_names != NULL,
            false, true, true, dst->compound_arity_len, false,
            &metadata_bytes))
            goto overflow;
        rc = col_rel_reserve_metadata(dst, metadata_bytes,
                &metadata_pending);
        if (rc != 0)
            goto cleanup;
        int type_rc = col_rel_prepare_column_types(dst, src->column_types,
                dst->ncols, &prepared_types, &prepared_schema);
        if (type_rc != 0) {
            rc = type_rc;
            goto cleanup;
        }
        prepared_schema_ok = true;
    }

    /* Ensure dst has sufficient capacity. */
    if (new_nrows > dst->capacity
        || (dst->timestamps && new_nrows > dst->timestamp_capacity)) {
        uint64_t ledger_before = col_rel_owned_ledger_bytes(dst);
        bool transitioned = false;
        uint32_t new_cap = new_nrows > dst->capacity
            ? (dst->capacity ? dst->capacity * 2 : COL_REL_INIT_CAP)
            : dst->capacity;
        while (new_cap < new_nrows) {
            uint32_t next_cap = new_cap * 2;
            if (next_cap <= new_cap) /* overflow guard */
                goto overflow;
            new_cap = next_cap;
        }

        if (dst->arena_owned && !dst->col_shared && !dst->memory_governor
            && arena) {
            /* An unmanaged arena relation still belongs to its caller's
             * arena.  Grow its columns in that arena and retain the ownership
             * flag; admission-based heap promotion is only for retained
             * relations (or for the legacy no-arena fallback below). */
            int64_t **new_columns = (int64_t **)calloc(dst->ncols,
                    sizeof(*new_columns));
            col_delta_timestamp_t *new_timestamps = NULL;
            if (!new_columns) {
                rc = ENOMEM;
                goto cleanup;
            }
            for (uint32_t c = 0; c < dst->ncols; c++) {
                new_columns[c] = (int64_t *)wl_arena_alloc(arena,
                        (size_t)new_cap * sizeof(int64_t));
                if (!new_columns[c]) {
                    free((void *)new_columns);
                    rc = ENOMEM;
                    goto cleanup;
                }
                memcpy(new_columns[c], dst->columns[c],
                    (size_t)dst->nrows * sizeof(int64_t));
            }
            if (dst->timestamps) {
                new_timestamps = (col_delta_timestamp_t *)malloc(
                    (size_t)new_cap * sizeof(*new_timestamps));
                if (!new_timestamps) {
                    free((void *)new_columns);
                    rc = ENOMEM;
                    goto cleanup;
                }
                memcpy(new_timestamps, dst->timestamps,
                    (size_t)dst->nrows * sizeof(*new_timestamps));
            }
            free((void *)dst->columns);
            free(dst->timestamps);
            dst->columns = new_columns;
            dst->timestamps = new_timestamps;
            dst->timestamp_capacity = new_timestamps ? new_cap : 0;
            dst->capacity = new_cap;
            /* Arena columns are not charged to RELATION, but timestamps are
             * heap-backed and may have grown.  Keep the attached ledger in
             * sync even though this ownership-preserving path is marked as a
             * transition below. */
            col_rel_ledger_reconcile(dst, ledger_before);
            wl_columnar_relation_touch_storage(dst);
            transitioned = true;
        } else if (dst->col_shared || dst->arena_owned
            || (dst->timestamps
            && new_nrows > dst->timestamp_capacity)) {
            rc = col_rel_grow_owned_transition_legacy_impl(dst, new_cap, true);
            if (rc != 0)
                goto cleanup;
            transitioned = true;
            alias_release_pending = true;
        } else {
            /* Heap-owned growth uses the same admission transaction as
             * append_row.  The helper reconciles the ledger and advances
             * storage generation exactly once. */
            rc = col_rel_reserve_capacity_admitted(dst, new_cap, NULL);
            if (rc != 0)
                goto cleanup;
            transitioned = true;
        }
        if (!transitioned) {
            col_rel_ledger_reconcile(dst, ledger_before);
            wl_columnar_relation_touch_storage(dst);
        }
    }

    /* Bulk append also mutates spare capacity, so a shared view must be
     * privatized even when the destination does not grow. */
    if (dst->col_shared) {
        rc = col_rel_cow_unshare_legacy_impl(dst, 0, true);
        if (rc != 0)
            goto cleanup;
        alias_release_pending = true;
    }

#ifdef WL_TEST_APPEND_HOOK
    if (alias_release_pending && wl_columnar_append_transition_hook)
        wl_columnar_append_transition_hook(dst);
#endif

    /* Install the prepared type metadata only after all potentially failing
     * destination growth has completed.  A rejected append therefore cannot
     * change an untyped destination's schema. */
    if (prepared_schema_ok) {
        struct ArrowSchema old_schema = dst->schema;
        bool old_schema_ok = dst->schema_ok;
        dst->column_types = prepared_types;
        dst->schema = prepared_schema;
        dst->schema_ok = true;
        col_rel_publish_metadata(dst, &metadata_pending,
            metadata_bytes, &old_metadata);
        if (old_schema_ok)
            ArrowSchemaRelease(&old_schema);
        col_rel_release_reservation_or_abort(&old_metadata);
        prepared_types = NULL;
        prepared_schema_ok = false;
    }

    /* Bulk copy all rows per-column.  All fallible preparation is complete. */
    for (uint32_t c = 0; c < dst->ncols; c++)
        memcpy(dst->columns[c] + dst_base, src->columns[c],
            (size_t)src->nrows * sizeof(int64_t));
    if (dst->column_types) {
        for (uint32_t c = 0; c < dst->ncols; c++) {
            if (dst->column_types[c] != WIRELOG_TYPE_FLOAT)
                continue;
            for (uint32_t row = dst_base; row < new_nrows; row++) {
                if (wl_columnar_float_bits_zero(dst->columns[c][row]))
                    dst->columns[c][row] = 0;
            }
        }
    }
    dst->nrows = new_nrows;

    /* Copy timestamps if both have tracking enabled */
    if (src->timestamps && dst->timestamps)
        memcpy(&dst->timestamps[dst_base], src->timestamps,
            src->nrows * sizeof(col_delta_timestamp_t));

    wl_columnar_relation_touch_view(dst);

    rc = 0;
    goto cleanup;

invalid:
    rc = EINVAL;
    goto cleanup;
overflow:
    rc = EOVERFLOW;
    goto cleanup;
cleanup:
    if (metadata_pending.bytes)
        col_rel_reservation_rollback(&metadata_pending);
    free(prepared_types);
    if (prepared_schema_ok)
        ArrowSchemaRelease(&prepared_schema);
    if (alias_release_pending) {
        int alias_rc = col_rel_storage_alias_release(dst);
        if (alias_rc != 0 && rc == 0)
            rc = alias_rc;
    }
    if (destination_writer_acquired
        && wl_columnar_source_access_writer_release(&destination_writer) != 0
        && rc == 0)
        rc = EINVAL;
    if (source_reader_acquired
        && wl_columnar_source_access_reader_release(&source_reader) != 0
        && rc == 0)
        rc = EINVAL;
    return rc;
}

int
col_rel_append_all(col_rel_t *dst, const col_rel_t *src, wl_arena_t *arena)
{
    return col_rel_append_all_impl(dst, src, arena, false);
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_relation_test_work_checkpoint)(
    wl_evaluation_control_t *control, uint64_t work);
void (*wl_columnar_relation_test_metadata_checkpoint)(
    wl_evaluation_control_t *control, int phase, uint64_t progress);
bool wl_columnar_relation_test_fail_metadata_commit;
#endif

/* Observe cancellation after each real bounded region, including its tail. */
static int
wl_columnar_relation_work_tail(wl_evaluation_control_t *control, uint64_t work)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_work_checkpoint)
        wl_columnar_relation_test_work_checkpoint(control, work);
#else
    (void)work;
#endif
    return wl_evaluation_control_charge(control, 0);
}

/* Forward gate attempts are result work. Releases are mandatory cleanup. */
#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_relation_test_before_reader_cas)(wl_evaluation_control_t *,
    wl_columnar_source_access_gate_t *, uint64_t);
void (*wl_columnar_relation_test_after_reader_cas)(wl_evaluation_control_t *,
    wl_columnar_source_access_gate_t *, bool);
void (*wl_columnar_relation_test_before_writer_cas)(wl_evaluation_control_t *,
    wl_columnar_source_access_gate_t *);
void (*wl_columnar_relation_test_after_lazy_commit)(wl_evaluation_control_t *,
    wl_columnar_source_access_writer_t *);
void (*wl_columnar_relation_test_before_nonce_cas)(wl_evaluation_control_t *,
    uint64_t);
void (*wl_columnar_relation_test_single_before_cleanup)(
    wl_evaluation_control_t *,
    wl_columnar_relation_mutation_single_t *);
#endif

int
wl_columnar_relation_reader_acquire_controlled(
    wl_columnar_source_access_gate_t *gate,
    wl_columnar_source_access_reader_t *token, wl_evaluation_control_t *control)
{
    if (!control)
        return wl_columnar_source_access_reader_acquire(gate, token);
    if (!gate || !token || token->owner || token->secondary_owner
        || token->identity || token->thread_valid || token->transferable)
        return EINVAL;
    uint64_t observed = atomic_load_explicit(&gate->state,
            memory_order_relaxed);
    for (;;) {
        int rc = wl_evaluation_control_charge(control, 1);
        if (rc)
            return rc;
        if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER) {
            (void)wl_columnar_relation_work_tail(control, 1);
            return EBUSY;
        }
        if (observed == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u) {
            (void)wl_columnar_relation_work_tail(control, 1);
            return EOVERFLOW;
        }
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_before_reader_cas)
            wl_columnar_relation_test_before_reader_cas(control, gate,
                observed);
#endif
#ifdef _MSC_VER
        bool acquired = atomic_compare_exchange_weak_explicit(&gate->state,
                &observed, observed + 1u, memory_order_acquire,
                memory_order_relaxed);
#else
        bool acquired = atomic_compare_exchange_strong_explicit(&gate->state,
                &observed, observed + 1u, memory_order_acquire,
                memory_order_relaxed);
#endif
        if (acquired)
            wl_columnar_source_access_reader_initialize(gate, token, false);
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_after_reader_cas)
            wl_columnar_relation_test_after_reader_cas(control, gate, acquired);
#endif
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc && acquired) {
            int cleanup = wl_columnar_source_access_reader_release(token);
            return cleanup ? cleanup : rc;
        }
        if (rc || acquired)
            return rc;
    }
}

int
wl_columnar_relation_writer_acquire_controlled(
    wl_columnar_source_access_gate_t *gate,
    wl_columnar_source_access_writer_t *token, wl_evaluation_control_t *control)
{
    if (!control)
        return wl_columnar_source_access_writer_acquire(gate, token);
    if (!gate || !token || token->owner || token->secondary_owner
        || token->identity || token->thread_valid)
        return EINVAL;
    int rc = wl_evaluation_control_charge(control, 1);
    if (rc)
        return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_before_writer_cas)
        wl_columnar_relation_test_before_writer_cas(control, gate);
#endif
    uint64_t expected = 0;
#ifdef _MSC_VER
    bool acquired = atomic_compare_exchange_weak_explicit(&gate->state,
            &expected, WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_acquire,
            memory_order_relaxed);
#else
    bool acquired = atomic_compare_exchange_strong_explicit(&gate->state,
            &expected, WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_acquire,
            memory_order_relaxed);
#endif
    if (!acquired) {
        (void)wl_columnar_relation_work_tail(control, 1);
        return EBUSY;
    }
    wl_columnar_source_access_writer_initialize(gate, token);
    rc = wl_columnar_relation_work_tail(control, 1);
    if (rc) {
        int cleanup = wl_columnar_source_access_writer_release(token);
        if (cleanup)
            rc = cleanup;
    }
    return rc;
}

typedef struct {
    col_rel_t *relation;
    wl_columnar_source_access_reader_t reader;
    wl_columnar_source_access_reader_t descriptor;
    wl_columnar_source_access_writer_t writer;
    uint64_t prior_identity;
    uint64_t prior_generation;
    bool journal;
    bool reader_held;
    bool descriptor_held;
    bool writer_held;
} wl_columnar_relation_source_admission_t;

static int
wl_columnar_relation_cleanup_result(int result, int cleanup)
{
    return cleanup > 0 && result <= 0 ? cleanup : result;
}

/* Finite direct one-hop resolution: charge six invariant metadata checks. */
static int
wl_columnar_relation_resolve_controlled(const col_rel_t *src,
    col_rel_t **owner, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 6);
    if (rc)
        return rc;
    rc = col_rel_storage_owner_resolve(src, owner);
    int stop = wl_columnar_relation_work_tail(control, 6);
    return rc ? rc : stop;
}

/* Restore only while the actual exclusive descriptor token remains valid. */
static int
wl_columnar_relation_source_finish_writer(
    wl_columnar_relation_source_admission_t *ctx, int rc,
    wl_evaluation_control_t *control, bool map_busy)
{
    if (!ctx->writer_held)
        return rc;
    int valid = wl_columnar_source_access_writer_validate(&ctx->writer,
            &ctx->relation->descriptor_access);
    if (ctx->journal && rc != 0 && !valid) {
        ctx->relation->storage_owner = NULL;
        ctx->relation->storage_owner_identity = ctx->prior_identity;
        ctx->relation->storage_owner_generation = ctx->prior_generation;
    }
    rc = wl_columnar_relation_cleanup_result(rc,
            map_busy && valid ? EBUSY : valid);
#ifdef WL_SESSION_TEST_HOOKS
    if (!rc && ctx->journal && wl_columnar_relation_test_after_lazy_commit)
        wl_columnar_relation_test_after_lazy_commit(control, &ctx->writer);
#else
    (void)control;
#endif
    int cleanup = wl_columnar_source_access_writer_release(&ctx->writer);
    if (!cleanup || !ctx->writer.owner)
        ctx->writer_held = false;
    return wl_columnar_relation_cleanup_result(rc,
               map_busy && cleanup ? EBUSY : cleanup);
}

/* All protected reads must finish before this last cutoff. A lazy writer is
 * retained through owner cleanup and the cutoff, never a secondary reader. */
static int
wl_columnar_relation_source_cutoff(wl_columnar_relation_source_admission_t *ctx,
    int rc, wl_evaluation_control_t *control, bool map_busy)
{
    int first_cleanup = 0;
    if (ctx->reader_held) {
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_new_like_before_reader_release)
            wl_columnar_relation_test_new_like_before_reader_release(control,
                &ctx->reader);
#endif
        int cleanup = col_rel_source_reader_release(&ctx->reader);
        if (!cleanup || !ctx->reader.owner)
            ctx->reader_held = false;
        first_cleanup = wl_columnar_relation_cleanup_result(first_cleanup,
                map_busy && cleanup ? EBUSY : cleanup);
    }
    if (ctx->descriptor_held) {
        int cleanup =
            wl_columnar_source_access_reader_release(&ctx->descriptor);
        if (!cleanup || !ctx->descriptor.owner)
            ctx->descriptor_held = false;
        first_cleanup = wl_columnar_relation_cleanup_result(first_cleanup,
                map_busy && cleanup ? EBUSY : cleanup);
    }
    /* Observe the last cutoff even when cleanup won over a negative stop:
     * an already latched positive accounting error is execution provenance. */
    int cutoff = wl_evaluation_control_charge(control, 0);
    if (rc <= 0 && cutoff > 0)
        rc = cutoff;
    else if (!rc)
        rc = cutoff;
    return wl_columnar_relation_cleanup_result(rc, first_cleanup);
}

static int
wl_columnar_relation_source_finish(wl_columnar_relation_source_admission_t *ctx,
    int rc, wl_evaluation_control_t *control, bool map_busy)
{
    int result = wl_columnar_relation_source_cutoff(ctx, rc, control, map_busy);
    result = wl_columnar_relation_source_finish_writer(ctx, result, control,
            map_busy);
    return result;
}

static int
wl_columnar_relation_source_acquire_controlled(const col_rel_t *src,
    wl_columnar_relation_source_admission_t *ctx,
    wl_evaluation_control_t *control)
{
    ctx->relation = (col_rel_t *)src;
    col_rel_t *owner = NULL;
    int rc = wl_columnar_relation_reader_acquire_controlled(
        &ctx->relation->descriptor_access, &ctx->descriptor, control);
    if (rc)
        return rc;
    ctx->descriptor_held = true;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc)
        goto fail;
    bool initialized = src->storage_owner != NULL;
    rc = wl_columnar_relation_work_tail(control, 1);
    if (rc)
        goto fail;
    if (!initialized) {
        rc = wl_columnar_source_access_reader_release(&ctx->descriptor);
        if (rc)
            goto fail;
        ctx->descriptor_held = false;
        rc = wl_columnar_relation_writer_acquire_controlled(
            &ctx->relation->descriptor_access, &ctx->writer, control);
        if (rc)
            goto fail;
        ctx->writer_held = true;
        rc = wl_evaluation_control_charge(control, 4);
        if (rc)
            goto fail;
        if (!src->storage_owner) {
            if (!src->relation_identity
                || !wl_columnar_relation_generation_valid(
                    src->storage_generation)) {
                rc = EINVAL;
                goto fail;
            }
            ctx->prior_identity = src->storage_owner_identity;
            ctx->prior_generation = src->storage_owner_generation;
            ctx->journal = true;
            ctx->relation->storage_owner = ctx->relation;
            ctx->relation->storage_owner_identity = src->relation_identity;
            ctx->relation->storage_owner_generation = src->storage_generation;
        }
        rc = wl_columnar_relation_work_tail(control, 4);
        if (rc)
            goto fail;
    }
    rc = wl_columnar_relation_resolve_controlled(src, &owner, control);
    if (rc)
        goto fail;
    rc = wl_columnar_relation_reader_acquire_controlled(&owner->source_access,
            &ctx->reader, control);
    if (rc)
        goto fail;
    ctx->reader_held = true;
    if (ctx->descriptor_held) {
        ctx->reader.secondary_owner = ctx->descriptor.owner;
        /* Transfer this actual descriptor pin into the combined reader. */
        ctx->descriptor.owner = NULL;
        ctx->descriptor.identity = 0;
        ctx->descriptor.thread_valid = false;
        ctx->descriptor_held = false;
    }
    return 0;
fail:
    return wl_columnar_relation_source_finish(ctx, rc, control, false);
}

int
wl_columnar_relation_source_group_release_readers(
    wl_columnar_relation_source_group_t *group, int rc)
{
    for (uint32_t i = group->owner_count; i > 0; i--) {
        wl_columnar_relation_source_group_owner_t *owner = &group->owners[i -
                1];
        if (!owner->held)
            continue;
        int cleanup = wl_columnar_source_access_reader_release(&owner->reader);
        if (!owner->reader.owner)
            owner->held = false;
        rc = wl_columnar_relation_cleanup_result(rc, cleanup);
    }
    for (uint32_t i = group->descriptor_count; i > 0; i--) {
        wl_columnar_relation_source_group_descriptor_t *desc =
            &group->descriptors[i - 1];
        if (!desc->reader_held)
            continue;
        int cleanup = wl_columnar_source_access_reader_release(&desc->reader);
        if (!desc->reader.owner)
            desc->reader_held = false;
        rc = wl_columnar_relation_cleanup_result(rc, cleanup);
    }
    return rc;
}

int
wl_columnar_relation_source_group_validate_writers(
    const wl_columnar_relation_source_group_t *group)
{
    for (uint32_t i = 0; i < group->descriptor_count; i++) {
        const wl_columnar_relation_source_group_descriptor_t *desc =
            &group->descriptors[i];
        if (desc->writer_held && wl_columnar_source_access_writer_validate(
                &desc->writer, &desc->relation->descriptor_access))
            return EINVAL;
        if (desc->journal && !desc->writer_held)
            return EINVAL;
    }
    return 0;
}

void
wl_columnar_relation_source_group_commit(
    wl_columnar_relation_source_group_t *group)
{
    group->committed = true;
    for (uint32_t i = 0; i < group->descriptor_count; i++)
        group->descriptors[i].journal = false;
}

/* Mandatory cleanup has no control dependency, including pending-frame retry.
 * Restore only own pending journals under their actual descriptor writers. */
int
wl_columnar_relation_source_group_drain(
    wl_columnar_relation_source_group_t *group,
    int rc)
{
    rc = wl_columnar_relation_source_group_release_readers(group, rc);
    bool retired = true;
    for (uint32_t i = group->descriptor_count; i > 0; i--) {
        wl_columnar_relation_source_group_descriptor_t *desc =
            &group->descriptors[i - 1];
        if (desc->journal) {
            int valid = desc->writer_held ?
                wl_columnar_source_access_writer_validate(&desc->writer,
                    &desc->relation->descriptor_access) : EINVAL;
            if (!valid) {
                desc->relation->storage_owner = NULL;
                desc->relation->storage_owner_identity = desc->prior_identity;
                desc->relation->storage_owner_generation =
                    desc->prior_generation;
                desc->journal = false;
            }
            rc = wl_columnar_relation_cleanup_result(rc, valid);
            /* Genuinely lost authority cannot restore metadata. Keep that
             * pending disposition reachable rather than pretend rollback. */
            if (valid)
                retired = false;
        }
        if (desc->writer_held) {
            int cleanup =
                wl_columnar_source_access_writer_release(&desc->writer);
            if (!desc->writer.owner)
                desc->writer_held = false;
            rc = wl_columnar_relation_cleanup_result(rc, cleanup);
        }
        if (desc->reader_held || desc->writer_held)
            retired = false;
    }
    for (uint32_t i = 0; i < group->owner_count; i++)
        if (group->owners[i].held)
            retired = false;
    if (retired) {
        /* Every actual token retired and every held/journal flag is false.
         * Prior scalar snapshots are inactive and overwritten before reuse.
         * Eleven finite map/pointer/count stores fit final group12. */
        group->descriptors[0].relation = NULL;
        group->descriptors[1].relation = NULL;
        group->owners[0].owner = NULL;
        group->owners[1].owner = NULL;
        group->input_descriptor[0] = 0;
        group->input_descriptor[1] = 0;
        group->input_owner[0] = 0;
        group->input_owner[1] = 0;
        group->descriptor_count = 0;
        group->owner_count = 0;
        group->committed = false;
    }
    return rc;
}

int
wl_columnar_relation_source_group_acquire_controlled(
    wl_columnar_relation_source_group_t *group, col_rel_t *left,
    col_rel_t *right,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 80);
    if (rc)
        return rc;
    if (!group || !left || group->descriptor_count || group->owner_count
        || group->committed)
        return EINVAL;
    /*41 entry/inert/flag predicates, <=16 setup/maps and bookkeeping slack.
     * Never treat an existing caller token as this group's new acquisition. */
    for (uint32_t i = 0; i < 2; i++) {
        const wl_columnar_relation_source_group_descriptor_t *desc =
            &group->descriptors[i];
        const wl_columnar_relation_source_group_owner_t *owner =
            &group->owners[i];
        if (desc->reader.owner || desc->reader.secondary_owner
            || desc->reader.identity || desc->reader.thread_valid
            || desc->reader.transferable ||
            !col_rel_mutation_writer_inert(&desc->writer)
            || desc->reader_held || desc->writer_held || desc->journal
            || owner->reader.owner || owner->reader.secondary_owner
            || owner->reader.identity || owner->reader.thread_valid
            || owner->reader.transferable || owner->held)
            return EINVAL;
    }
    group->descriptors[0].relation = left;
    group->descriptor_count = 1;
    group->input_descriptor[0] = 0;
    group->input_descriptor[1] = 0;
    if (right && right != left) {
        group->descriptors[1].relation = right;
        group->descriptor_count = 2;
        group->input_descriptor[1] = 1;
    }
    rc = wl_columnar_relation_work_tail(control, 80);
    if (rc)
        return rc;
    for (uint32_t i = 0; i < group->descriptor_count; i++) {
        wl_columnar_relation_source_group_descriptor_t *desc =
            &group->descriptors[i];
        col_rel_t *r = desc->relation, *owner = NULL;
        rc =
            wl_columnar_relation_reader_acquire_controlled(
            &r->descriptor_access,
            &desc->reader, control);
        desc->reader_held = desc->reader.owner != NULL;
        if (rc)
            return rc;
        rc = wl_evaluation_control_charge(control, 1);
        if (rc)
            return rc;
        bool initialized = r->storage_owner != NULL;
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc)
            return rc;
        if (!initialized) {
            rc = wl_columnar_source_access_reader_release(&desc->reader);
            desc->reader_held = desc->reader.owner != NULL;
            if (rc)
                return rc;
            rc =
                wl_columnar_relation_writer_acquire_controlled(
                &r->descriptor_access,
                &desc->writer, control);
            desc->writer_held = desc->writer.owner != NULL;
            if (rc)
                return rc;
            rc = wl_evaluation_control_charge(control, 4);
            if (rc)
                return rc;
            if (!r->storage_owner) {
                if (!r->relation_identity ||
                    !wl_columnar_relation_generation_valid(
                        r->storage_generation))
                    return EINVAL;
                desc->prior_identity = r->storage_owner_identity;
                desc->prior_generation = r->storage_owner_generation;
                desc->journal = true;
                r->storage_owner = r;
                r->storage_owner_identity = r->relation_identity;
                r->storage_owner_generation = r->storage_generation;
            }
            rc = wl_columnar_relation_work_tail(control, 4);
            if (rc)
                return rc;
        }
        rc = wl_columnar_relation_resolve_controlled(r, &owner, control);
        if (rc)
            return rc;
        /* <=2 comparisons and fixed mapping writes are in setup80. */
        uint32_t slot = 0;
        if (!group->owner_count || group->owners[0].owner != owner) {
            slot = group->owner_count++;
            group->owners[slot].owner = owner;
        }
        for (uint32_t input = 0; input < 2; input++)
            if (group->input_descriptor[input] == i)
                group->input_owner[input] = slot;
    }
    for (uint32_t i = 0; i < group->owner_count; i++) {
        wl_columnar_relation_source_group_owner_t *owner = &group->owners[i];
        rc =
            wl_columnar_relation_reader_acquire_controlled(
            &owner->owner->source_access,
            &owner->reader, control);
        owner->held = owner->reader.owner != NULL;
        if (rc)
            return rc;
    }
    return 0;
}

static int
wl_columnar_relation_nonce_controlled(uint64_t *nonce,
    wl_evaluation_control_t *control)
{
    uint64_t observed = atomic_load_explicit(&wl_next_mutation_set_nonce,
            memory_order_relaxed);
    for (;;) {
        int rc = wl_evaluation_control_charge(control, 1);
        if (rc)
            return rc;
        if (!observed || observed == UINT64_MAX)
            return EOVERFLOW;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_before_nonce_cas)
            wl_columnar_relation_test_before_nonce_cas(control, observed);
#endif
#ifdef _MSC_VER
        bool acquired = atomic_compare_exchange_weak_explicit(
            &wl_next_mutation_set_nonce, &observed, observed + 1u,
            memory_order_relaxed, memory_order_relaxed);
#else
        bool acquired = atomic_compare_exchange_strong_explicit(
            &wl_next_mutation_set_nonce, &observed, observed + 1u,
            memory_order_relaxed, memory_order_relaxed);
#endif
        if (acquired)
            *nonce = observed;
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc || acquired)
            return rc;
    }
}

/* Fixed one-role admission owns at most two owner tokens. This finalizer must
 * not use generic finish/unwind, which can skip cleanup or ignore its errors. */
int
wl_columnar_relation_mutation_single_finish_controlled(
    wl_columnar_relation_mutation_single_t *single, int rc,
    wl_evaluation_control_t *control)
{
    if (!control) {
        if (!single)
            return EINVAL;
        int cleanup = col_rel_mutation_set_finish(&single->set, rc == 0);
        return wl_columnar_relation_cleanup_result(rc, cleanup);
    }
    if (!single || single->set.identity != (uintptr_t)&single->set)
        return EINVAL;
    wl_columnar_relation_mutation_set_t *set = &single->set;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_single_before_cleanup)
        wl_columnar_relation_test_single_before_cleanup(control, single);
#else
    (void)control;
#endif
    int execution = rc;
    bool all_retired = true;
    for (size_t i = set->owners_acquired; i > 0; i--) {
        wl_columnar_source_access_writer_t *writer = &single->owners[i -
                1].writer;
        int cleanup = wl_columnar_source_access_writer_release(writer);
        rc = wl_columnar_relation_cleanup_result(rc, cleanup);
        if (writer->owner)
            all_retired = false;
    }
    int cutoff = wl_evaluation_control_charge(control, 0);
    if (execution <= 0 && cutoff > 0)
        rc = cutoff;
    else if (!rc)
        rc = cutoff;
    /* A stopped/failed admission may have initialized legacy metadata. */
    if (rc && set->initialization_count) {
        int valid = wl_columnar_source_access_writer_validate(
            &single->descriptor.writer,
            &single->initialization.relation->descriptor_access);
        if (!valid) {
            col_rel_t *r = single->initialization.relation;
            r->storage_owner = NULL;
            r->storage_owner_identity = single->initialization.owner_identity;
            r->storage_owner_generation =
                single->initialization.owner_generation;
            atomic_store_explicit(&r->storage_alias_borrows,
                single->initialization.borrows, memory_order_relaxed);
        }
        rc = wl_columnar_relation_cleanup_result(rc, valid);
    }
    if (set->descriptors_acquired) {
        int cleanup =
            wl_columnar_source_access_writer_release(
            &single->descriptor.writer);
        rc = wl_columnar_relation_cleanup_result(rc, cleanup);
        if (single->descriptor.writer.owner)
            all_retired = false;
    }
    if (all_retired) {
        single->set = (wl_columnar_relation_mutation_set_t){0};
        single->descriptor = (wl_columnar_relation_mutation_descriptor_t){0};
        single->owners[0] = (wl_columnar_relation_mutation_owner_t){0};
        single->owners[1] = (wl_columnar_relation_mutation_owner_t){0};
        single->lease = (wl_columnar_relation_mutation_lease_t){0};
        single->initialization =
            (wl_columnar_relation_mutation_initialization_t){0};
    }
    return rc;
}

int
wl_columnar_relation_mutation_single_acquire_controlled(col_rel_t *relation,
    wl_columnar_relation_mutation_single_t *single,
    wl_evaluation_control_t *control)
{
    if (!control)
        return col_rel_mutation_single_acquire(relation, single);
    if (!relation || !single)
        return EINVAL;
    wl_columnar_relation_mutation_set_t *set = &single->set;
    int rc = wl_evaluation_control_charge(control, 32);
    if (rc)
        return rc;
    if (set->identity || set->acquisition_nonce || set->roles ||
        set->descriptors
        || set->owners || set->leases || set->initializations
        || set->descriptor_count || set->owner_count || set->lease_count
        || set->initialization_count || set->descriptors_acquired
        || set->owners_acquired || set->terminal_sequence
        || set->terminal_expected_events || set->terminal_observed_events
        || set->terminal_sequence_consumed || set->published
        || single->descriptor.relation
        || !col_rel_mutation_writer_inert(&single->descriptor.writer)
        || single->owners[0].owner || single->owners[1].owner
        || !col_rel_mutation_writer_inert(&single->owners[0].writer)
        || !col_rel_mutation_writer_inert(&single->owners[1].writer)
        || single->lease.identity || single->lease.acquisition_nonce
        || single->lease.set || single->lease.relation || single->lease.owner
        || single->lease.descriptor_slot || single->lease.owner_slot
        || single->lease.self_owner_slot || single->lease.relation_identity
        || single->lease.relation_generation || single->lease.owner_identity
        || single->lease.owner_generation || single->lease.role_flags
        || single->lease.detached || single->lease.storage_transitioned
        || single->initialization.relation ||
        single->initialization.owner_identity
        || single->initialization.owner_generation ||
        single->initialization.borrows)
        return EINVAL;
    rc = wl_columnar_relation_work_tail(control, 32);
    if (rc)
        return rc;
    uint64_t nonce;
    rc = wl_columnar_relation_nonce_controlled(&nonce, control);
    if (rc)
        return rc;
    single->role.relation = relation;
    single->role.role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION;
    *set = (wl_columnar_relation_mutation_set_t){
        .identity = (uintptr_t)set, .acquisition_nonce = nonce,
        .roles = &single->role, .descriptors = &single->descriptor,
        .owners = single->owners, .leases = &single->lease,
        .initializations = &single->initialization,
        .descriptor_count = 1, .lease_count = 1
    };
    single->descriptor.relation = relation;
    rc =
        wl_columnar_relation_writer_acquire_controlled(
        &relation->descriptor_access,
        &single->descriptor.writer, control);
    if (rc)
        goto fail;
    set->descriptors_acquired = 1;
    rc = wl_evaluation_control_charge(control, 6);
    if (rc)
        goto fail;
    if (!relation->storage_owner) {
        if (!relation->relation_identity ||
            col_rel_storage_alias_borrow_count(relation)
            || !wl_columnar_relation_generation_valid(
                relation->storage_generation)) {
            rc = EINVAL;
            goto fail;
        }
        single->initialization =
            (wl_columnar_relation_mutation_initialization_t){
            .relation = relation,
            .owner_identity = relation->storage_owner_identity,
            .owner_generation = relation->storage_owner_generation,
            .borrows = col_rel_storage_alias_borrow_count(relation)
        };
        set->initialization_count = 1;
        relation->storage_owner = relation;
        relation->storage_owner_identity = relation->relation_identity;
        relation->storage_owner_generation = relation->storage_generation;
        atomic_store_explicit(&relation->storage_alias_borrows, 0,
            memory_order_relaxed);
    }
    rc = wl_columnar_relation_work_tail(control, 6);
    if (rc)
        goto fail;
    col_rel_t *owner = NULL;
    rc = wl_columnar_relation_resolve_controlled(relation, &owner, control);
    if (rc)
        goto fail;
    rc = wl_evaluation_control_charge(control, 12);
    if (rc)
        goto fail;
    if (!relation->relation_identity || !owner->relation_identity
        || !wl_columnar_relation_generation_valid(relation->storage_generation)
        || !wl_columnar_relation_generation_valid(owner->storage_generation)
        || relation->storage_owner_identity != owner->relation_identity
        || relation->storage_owner_generation != owner->storage_generation) {
        rc = EINVAL;
        goto fail;
    }
    single->owners[0].owner = owner;
    set->owner_count = 1;
    if (relation != owner) {
        single->owners[1].owner = relation;
        set->owner_count = 2;
        if ((uintptr_t)&owner->source_access >
            (uintptr_t)&relation->source_access) {
            single->owners[0].owner = relation;
            single->owners[1].owner = owner;
        }
    }
    single->lease = (wl_columnar_relation_mutation_lease_t){
        .set = set, .acquisition_nonce = nonce, .relation = relation,
        .owner = owner,
        .role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION,
        .relation_identity = relation->relation_identity,
        .relation_generation = relation->storage_generation,
        .owner_identity = owner->relation_identity,
        .owner_generation = owner->storage_generation,
        .owner_slot = single->owners[0].owner == owner ? 0u : 1u,
        .self_owner_slot = single->owners[0].owner == relation ? 0u : 1u
    };
    rc = wl_columnar_relation_work_tail(control, 12);
    if (rc)
        goto fail;
    for (size_t i = 0; i < set->owner_count; i++) {
        rc = wl_columnar_relation_writer_acquire_controlled(
            &single->owners[i].owner->source_access,
            &single->owners[i].writer, control);
        if (rc)
            goto fail;
        set->owners_acquired++;
    }
    rc = wl_evaluation_control_charge(control, 1);
    if (rc)
        goto fail;
    if (owner == relation && col_rel_storage_alias_borrow_count(owner)) {
        rc = EBUSY;
        goto fail;
    }
    single->lease.identity = (uintptr_t)&single->lease;
    rc = wl_columnar_relation_work_tail(control, 1);
    if (rc)
        goto fail;
    /* Generic validation is finite here: exactly one lease and <=2 owners. */
    rc = wl_evaluation_control_charge(control, 32);
    if (rc)
        goto fail;
    rc = col_rel_mutation_lease_validate(&single->lease, relation);
    int stop = wl_columnar_relation_work_tail(control, 32);
    if (!rc)
        rc = stop;
    if (!rc)
        return 0;
fail:
    return wl_columnar_relation_mutation_single_finish_controlled(single, rc,
               control);
}

static void
wl_columnar_relation_metadata_checkpoint(wl_evaluation_control_t *control,
    int phase, uint64_t progress)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_metadata_checkpoint)
        wl_columnar_relation_test_metadata_checkpoint(control, phase, progress);
#else
    (void)control;
    (void)phase;
    (void)progress;
#endif
}

static int
wl_columnar_relation_reserve_metadata_controlled(col_rel_t *r, uint64_t bytes,
    wl_columnar_memory_reservation_t *pending, wl_evaluation_control_t *control)
{
    wl_columnar_memory_reservation_init(pending);
    if (!r->memory_governor || !bytes)
        return 0;
    wl_columnar_memory_admission_status_t status;
    int rc = wl_columnar_memory_governor_reserve_checked_controlled(
        wl_columnar_memory_governor_ref_get(r->memory_governor), bytes,
        pending, &status, control);
    if (rc != 0)
        return rc;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return 0;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
        r->memory_budget_denial_pending = true;
    wl_columnar_memory_reservation_init(pending);
    return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

/* The scalar metadata metric includes visited/copied characters and NUL.
 * These phase IDs stay private; only test builds expose their symbolic names. */
static int
wl_columnar_relation_name_length_controlled(const char *name,
    uint64_t *length, wl_evaluation_control_t *control)
{
    uint64_t n = 0;
    for (;;) {
        int rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            return rc;
        if (n == UINT64_MAX || n >= SIZE_MAX || n >= INT64_MAX)
            return EOVERFLOW;
        char value = name[n];
        n++;
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            return rc;
        if (!value) {
            *length = n;
            return 0;
        }
    }
}

static int
wl_columnar_relation_build_schema_record_controlled(const col_rel_t *dst,
    const uint64_t *lengths, const wirelog_column_type_t *types,
    struct ArrowSchema *schema, bool *schema_initialized,
    wl_evaluation_control_t *control)
{
    uint64_t table_bytes;
    *schema_initialized = false;
    int rc;
    if (!wl_columnar_memory_size_mul(dst->ncols, sizeof(struct ArrowSchema *),
        &table_bytes)
        || table_bytes > SIZE_MAX || table_bytes > INT64_MAX)
        return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 3);
    if (rc != 0)
        goto done;
    ArrowSchemaInit(schema);
    *schema_initialized = true;
    rc = ArrowSchemaSetFormat(schema, "+s");
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_work_tail(control, 3);
    if (rc != 0)
        goto done;
    if (dst->ncols == 0)
        return 0;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc != 0)
        goto done;
    schema->children = (struct ArrowSchema **)ArrowMalloc((int64_t)table_bytes);
    if (!schema->children && table_bytes) {
        rc = ENOMEM;
        goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 1);
    if (rc != 0)
        goto done;
    /* n_children stays zero until every uninitialized pointer has been cleared. */
    for (uint32_t c = 0; c < dst->ncols;) {
        uint32_t n = dst->ncols - c;
        if (n > WL_EVALUATION_CONTROL_QUANTUM)
            n = (uint32_t)WL_EVALUATION_CONTROL_QUANTUM;
        rc = wl_evaluation_control_charge(control, n);
        if (rc != 0)
            goto done;
        for (uint32_t i = 0; i < n; i++)
            schema->children[c + i] = NULL;
        c += n;
        wl_columnar_relation_metadata_checkpoint(control, 2, c);
        rc = wl_columnar_relation_work_tail(control, n);
        if (rc != 0)
            goto done;
    }
    for (uint32_t c = 0; c < dst->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
        if (wl_columnar_relation_test_fail_metadata_alloc) {
            wl_columnar_relation_test_fail_metadata_alloc = false;
            rc = ENOMEM;
            goto done;
        }
#endif
        struct ArrowSchema *child =
            (struct ArrowSchema *)ArrowMalloc(sizeof(*child));
        if (!child) {
            rc = ENOMEM;
            goto done;
        }
        ArrowSchemaInit(child);
        schema->children[c] = child;
        schema->n_children = (int64_t)c + 1;
        wl_columnar_relation_metadata_checkpoint(control, 3, c + 1u);
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
        rc = wl_evaluation_control_charge(control, 2);
        if (rc != 0)
            goto done;
        rc = ArrowSchemaSetFormat(child,
                types && types[c] == WIRELOG_TYPE_FLOAT ? "g" : "l");
        if (rc != 0)
            goto done;
        rc = wl_columnar_relation_work_tail(control, 2);
        if (rc != 0)
            goto done;
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        const char *name = dst->col_names &&
            dst->col_names[c] ? dst->col_names[c] : "";
        uint64_t length;
        if (lengths) length = lengths[c];
        else {
            rc = wl_columnar_relation_name_length_controlled(name, &length,
                    control);
            if (rc) goto done;
        }
        child->name = (const char *)ArrowMalloc((int64_t)length);
        if (!child->name) {
            rc = ENOMEM;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
        for (uint64_t pos = 0; pos < length;) {
            uint64_t n = length - pos;
            if (n > WL_EVALUATION_CONTROL_QUANTUM)
                n = WL_EVALUATION_CONTROL_QUANTUM;
            rc = wl_evaluation_control_charge(control, n);
            if (rc != 0)
                goto done;
            memcpy((char *)child->name + pos, name + pos, (size_t)n);
            pos += n;
            wl_columnar_relation_metadata_checkpoint(control, 4, pos);
            rc = wl_columnar_relation_work_tail(control, n);
            if (rc != 0)
                goto done;
        }
    }
    return 0;
done:
    if (*schema_initialized) {
        ArrowSchemaRelease(schema);
        *schema_initialized = false;
    }
    return rc;
}

static int
wl_columnar_relation_build_schema_controlled(const col_rel_t *dst,
    const uint64_t *lengths, const wirelog_column_type_t *types,
    struct ArrowSchema *schema, wl_evaluation_control_t *control)
{
    bool initialized = false;
    return wl_columnar_relation_build_schema_record_controlled(dst, lengths,
               types, schema, &initialized, control);
}

static int
wl_columnar_relation_prepare_types_controlled(const col_rel_t *dst,
    const wirelog_column_type_t *types, wirelog_column_type_t **types_out,
    struct ArrowSchema *schema, uint64_t *metadata_bytes,
    wl_columnar_memory_reservation_t *pending, wl_evaluation_control_t *control)
{
    uint64_t length_bytes, types_bytes, table_bytes;
    uint64_t *lengths = NULL;
    wirelog_column_type_t *copy = NULL;
    wl_columnar_memory_reservation_t scratch;
    uint64_t bytes = 3;
    int rc;
    wl_columnar_memory_reservation_init(&scratch);
    wl_columnar_memory_reservation_init(pending);
    if (!wl_columnar_memory_size_mul(dst->ncols, sizeof(*lengths),
        &length_bytes)
        || !wl_columnar_memory_size_mul(dst->ncols, sizeof(*copy), &types_bytes)
        || !wl_columnar_memory_size_mul(dst->ncols,
        sizeof(struct ArrowSchema *), &table_bytes)
        || length_bytes > SIZE_MAX || types_bytes > SIZE_MAX ||
        table_bytes > SIZE_MAX)
        return EOVERFLOW;
    /* Scratch is physically live beside both canonical and incoming metadata.
     * Admit it independently before allocating the length cache. */
    rc = wl_columnar_relation_reserve_metadata_controlled((col_rel_t *)dst,
            length_bytes, &scratch, control);
    if (rc != 0)
        goto done;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc != 0)
        goto done;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_metadata_alloc) {
        wl_columnar_relation_test_fail_metadata_alloc = false;
        rc = ENOMEM;
        goto done;
    }
#endif
    lengths = (uint64_t *)malloc((size_t)length_bytes);
    if (!lengths && length_bytes) {
        rc = ENOMEM;
        goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 1);
    if (rc != 0)
        goto done;
    for (uint32_t c = 0; c < dst->ncols; c++) {
        const char *name = dst->col_names &&
            dst->col_names[c] ? dst->col_names[c] : "";
        uint64_t length, fixed = sizeof(struct ArrowSchema *)
            + sizeof(struct ArrowSchema) + 2u + sizeof(*copy);
        rc = wl_columnar_relation_name_length_controlled(name, &length,
                control);
        if (rc != 0)
            goto done;
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        lengths[c] = length;
        if (dst->col_names) {
            if (!wl_columnar_memory_size_add(fixed, sizeof(char *), &fixed)
                || (dst->col_names[c] && !wl_columnar_memory_size_add(fixed,
                length, &fixed))) {
                rc = EOVERFLOW;
                goto done;
            }
        }
        if (!wl_columnar_memory_size_add(bytes, fixed, &bytes)
            || !wl_columnar_memory_size_add(bytes, length, &bytes)) {
            rc = EOVERFLOW;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    rc = wl_columnar_relation_reserve_metadata_controlled((col_rel_t *)dst,
            bytes, pending, control);
    if (rc != 0)
        goto done;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc != 0)
        goto done;
    copy = (wirelog_column_type_t *)malloc((size_t)types_bytes);
    if (!copy && types_bytes) {
        rc = ENOMEM;
        goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 1);
    if (rc != 0)
        goto done;
    for (uint32_t c = 0; c < dst->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        copy[c] = types[c];
        wl_columnar_relation_metadata_checkpoint(control, 1, c + 1u);
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    rc = wl_columnar_relation_build_schema_controlled(dst, lengths, copy,
            schema, control);
    if (rc != 0)
        goto done;
    *metadata_bytes = bytes;
    *types_out = copy;
    copy = NULL;
    rc = 0;
done:
    free(lengths);
    if (scratch.bytes)
        col_rel_reservation_rollback(&scratch);
    free(copy);
    /* Caller owns pending on success and failure; release it exactly once. */
    return rc;
}

static int
wl_columnar_relation_prepare_types_record(
    wl_columnar_relation_bulk_append_t *record,
    const col_rel_t *dst, const wirelog_column_type_t *types,
    bool cache_lengths, wl_evaluation_control_t *control)
{
    uint64_t length_bytes, types_bytes, table_bytes, bytes = 3;
    wl_columnar_memory_reservation_t *pending = &record->metadata_pending;
    struct ArrowSchema *schema = &record->prepared_schema;
    int rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    if (!wl_columnar_memory_size_mul(dst->ncols, sizeof(*record->lengths),
        &length_bytes)
        || !wl_columnar_memory_size_mul(dst->ncols,
        sizeof(*record->prepared_types), &types_bytes)
        || !wl_columnar_memory_size_mul(dst->ncols,
        sizeof(struct ArrowSchema *), &table_bytes)
        || length_bytes > SIZE_MAX || types_bytes > SIZE_MAX ||
        table_bytes > SIZE_MAX)
        return EOVERFLOW;
    if (cache_lengths) {
        /* Scratch is physically live beside both canonical and incoming metadata.
         * Admit it independently before allocating the length cache. */
        rc = wl_evaluation_control_charge(control,
                sizeof(record->scratch) + 16u);
        if (rc) goto done;
        rc = wl_columnar_relation_reserve_metadata_controlled((col_rel_t *)dst,
                length_bytes, &record->scratch, control);
        if (rc != 0)
            goto done;
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
        if (wl_columnar_relation_test_fail_metadata_alloc) {
            wl_columnar_relation_test_fail_metadata_alloc = false;
            rc = ENOMEM;
            goto done;
        }
#endif
        record->lengths = (uint64_t *)malloc((size_t)length_bytes);
        if (!record->lengths && length_bytes) {
            rc = ENOMEM;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    for (uint32_t c = 0; c < dst->ncols; c++) {
        const char *name = dst->col_names &&
            dst->col_names[c] ? dst->col_names[c] : "";
        uint64_t length, fixed = sizeof(struct ArrowSchema *)
            + sizeof(struct ArrowSchema) + 2u + sizeof(*record->prepared_types);
        rc = wl_columnar_relation_name_length_controlled(name, &length,
                control);
        if (rc != 0)
            goto done;
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        if (cache_lengths) record->lengths[c] = length;
        if (dst->col_names) {
            if (!wl_columnar_memory_size_add(fixed, sizeof(char *), &fixed)
                || (dst->col_names[c] && !wl_columnar_memory_size_add(fixed,
                length, &fixed))) {
                rc = EOVERFLOW;
                goto done;
            }
        }
        if (!wl_columnar_memory_size_add(bytes, fixed, &bytes)
            || !wl_columnar_memory_size_add(bytes, length, &bytes)) {
            rc = EOVERFLOW;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    rc = wl_evaluation_control_charge(control, sizeof(*pending) + 16u);
    if (rc) goto done;
    rc = wl_columnar_relation_reserve_metadata_controlled((col_rel_t *)dst,
            bytes, pending, control);
    if (rc != 0)
        goto done;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc != 0)
        goto done;
    record->prepared_types =
        (wirelog_column_type_t *)malloc((size_t)types_bytes);
    if (!record->prepared_types && types_bytes) {
        rc = ENOMEM;
        goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 1);
    if (rc != 0)
        goto done;
    for (uint32_t c = 0; c < dst->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        record->prepared_types[c] = types[c];
        wl_columnar_relation_metadata_checkpoint(control, 1, c + 1u);
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    rc = wl_columnar_relation_build_schema_record_controlled(dst,
            record->lengths, record->prepared_types,
            schema, &record->prepared_schema_ok, control);
    if (rc != 0)
        goto done;
    record->metadata_bytes = bytes;

    rc = 0;
    return rc;
done:
    return rc;
}

static int
wl_columnar_relation_bulk_prepare_types(
    wl_columnar_relation_bulk_append_t *record,
    const col_rel_t *dst, const wirelog_column_type_t *types,
    wl_evaluation_control_t *control)
{
    return wl_columnar_relation_prepare_types_record(record, dst, types, true,
               control);
}

static bool
wl_columnar_relation_metadata_tokens_valid(col_rel_t *dst,
    const wl_columnar_memory_reservation_t *pending,
    const wl_columnar_memory_reservation_t *previous, uint64_t bytes,
    bool committed)
{
    if (!dst->memory_governor)
        return true;
    if (previous->bytes || previous->identity != previous
        || atomic_load_explicit(&previous->state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY)
        return false;
    if (pending->bytes != bytes)
        return false;
    if (pending->bytes && (pending->identity != pending
        || pending->governor !=
        wl_columnar_memory_governor_ref_get(dst->memory_governor)
        || atomic_load_explicit(&pending->owner_bits, memory_order_acquire)
        != (committed ? (uint64_t)(uintptr_t)dst : 0)
        || atomic_load_explicit(&pending->state, memory_order_acquire)
        != (committed ? WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            : WL_COLUMNAR_MEMORY_RESERVATION_RESERVED)))
        return false;
    if (dst->metadata_reserved_bytes
        && (dst->metadata_reservation.identity != &dst->metadata_reservation
        || dst->metadata_reservation.governor != pending->governor
        || dst->metadata_reservation.bytes != dst->metadata_reserved_bytes
        || atomic_load_explicit(&dst->metadata_reservation.state,
        memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || atomic_load_explicit(&dst->metadata_reservation.owner_bits,
        memory_order_acquire)
        != (uint64_t)(uintptr_t)dst))
        return false;
    return true;
}

/* New token has already been committed to dst. No second commit is allowed. */
static void
wl_columnar_relation_publish_committed_metadata(col_rel_t *dst,
    wl_columnar_memory_reservation_t *pending, uint64_t bytes,
    wl_columnar_memory_reservation_t *previous)
{
    if (!dst->memory_governor)
        return;
    if (bytes && (pending->bytes != bytes
        || atomic_load_explicit(&pending->state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || atomic_load_explicit(&pending->owner_bits, memory_order_acquire)
        != (uint64_t)(uintptr_t)dst))
        abort();
    if (dst->metadata_reserved_bytes
        && !wl_columnar_memory_reservation_move(previous,
        &dst->metadata_reservation))
        abort();
    if (bytes &&
        !wl_columnar_memory_reservation_move(&dst->metadata_reservation,
        pending))
        abort();
    dst->metadata_reserved_bytes = bytes;
}

int
wl_columnar_relation_join_types_controlled(col_rel_t *out,
    const wirelog_column_type_t *types, wl_evaluation_control_t *control)
{
    wirelog_column_type_t *copy = NULL;
    struct ArrowSchema schema;
    wl_columnar_memory_reservation_t pending, previous;
    uint64_t bytes = 0;
    bool schema_owned = false;
    wl_columnar_memory_reservation_init(&pending);
    wl_columnar_memory_reservation_init(&previous);
    if (!control || !out || out->nrows || out->column_types
        || (out->ncols && !types))
        return EINVAL;
    if (out->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EOVERFLOW;
    struct ArrowSchema old_schema = out->schema;
    bool old_schema_owned = out->schema_ok;
    int rc = wl_columnar_relation_prepare_types_controlled(out, types, &copy,
            &schema, &bytes, &pending, control);
    if (rc)
        goto done;
    schema_owned = true;
    rc = wl_evaluation_control_charge(control, 112);
    if (rc)
        goto done;
    if (!wl_columnar_relation_metadata_tokens_valid(out, &pending, &previous,
        bytes, false)) {
        rc = ENOMEM;
        goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 112);
    if (rc)
        goto done;
    if (out->memory_governor && bytes) {
        bool committed;
        rc = wl_columnar_memory_governor_commit_controlled(&pending, out,
                &committed, control);
        if (rc)
            goto done;
        if (!committed) {
            rc = ENOMEM;
            goto done;
        }
    }
    wl_columnar_relation_metadata_checkpoint(control,
        5, bytes);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc)
        goto done;
    out->column_types = copy;
    out->schema = schema;
    out->schema_ok = true;
    copy = NULL; schema_owned = false;
    wl_columnar_relation_publish_committed_metadata(out, &pending, bytes,
        &previous);
    if (old_schema_owned)
        ArrowSchemaRelease(&old_schema);
    col_rel_release_retired_reservation(&previous);
    wl_columnar_relation_touch_view(out);
    return 0;
done:
    if (schema_owned)
        ArrowSchemaRelease(&schema);
    free(copy);
    if (pending.bytes)
        col_rel_reservation_rollback(&pending);
    return rc;
}

static int
wl_columnar_relation_copy_cells_controlled(int64_t *to,
    const int64_t *from, uint32_t count, bool normalize,
    wl_evaluation_control_t *control)
{
    for (uint32_t row = 0; row < count;) {
        uint32_t n = count - row;
        if (n > WL_EVALUATION_CONTROL_QUANTUM)
            n = (uint32_t)WL_EVALUATION_CONTROL_QUANTUM;
        int rc = wl_evaluation_control_charge(control, n);
        if (rc != 0)
            return rc;
        memcpy(to + row, from + row, (size_t)n * sizeof(int64_t));
        rc = wl_columnar_relation_work_tail(control, n);
        if (rc != 0)
            return rc;
        if (normalize) {
            rc = wl_evaluation_control_charge(control, n);
            if (rc != 0)
                return rc;
            for (uint32_t i = 0; i < n; i++) {
                int64_t *cell = &to[row + i];
                if (wl_columnar_float_bits_zero(*cell))
                    *cell = 0;
            }
            rc = wl_columnar_relation_work_tail(control, n);
            if (rc != 0)
                return rc;
        }
        row += n;
    }
    return 0;
}

static int
wl_columnar_relation_copy_timestamps_controlled(col_delta_timestamp_t *to,
    const col_delta_timestamp_t *from, uint32_t count,
    wl_evaluation_control_t *control)
{
    for (uint32_t row = 0; row < count;) {
        uint32_t n = count - row;
        if (n > WL_EVALUATION_CONTROL_QUANTUM / 5u)
            n = (uint32_t)(WL_EVALUATION_CONTROL_QUANTUM / 5u);
        int rc = wl_evaluation_control_charge(control, (uint64_t)n * 5u);
        if (rc != 0)
            return rc;
        for (uint32_t i = 0; i < n; i++) {
            to[row + i].iteration = from[row + i].iteration;
            to[row + i].stratum = from[row + i].stratum;
            to[row + i].worker = from[row + i].worker;
            to[row + i]._reserved = from[row + i]._reserved;
            to[row + i].multiplicity = from[row + i].multiplicity;
        }
        rc = wl_columnar_relation_work_tail(control, (uint64_t)n * 5u);
        if (rc != 0)
            return rc;
        row += n;
    }
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_relation_test_reserve_checkpoint)(
    wl_evaluation_control_t *, int, uint64_t);
#endif

static void
wl_columnar_relation_reserve_checkpoint(wl_evaluation_control_t *control,
    int phase, uint64_t progress)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_reserve_checkpoint)
        wl_columnar_relation_test_reserve_checkpoint(control, phase, progress);
#else
    (void)control; (void)phase; (void)progress;
#endif
}

/* All byte amounts are already checked fixed-field expressions. Allocation
 * duration and retirement frees are runtime/mandatory cleanup exclusions. */
static int
wl_columnar_relation_prepare_growth_controlled(col_rel_t *dst,
    const col_rel_payload_shape_t *old_shape, uint32_t capacity,
    uint64_t lane_bytes, uint64_t table_bytes, uint64_t timestamp_bytes,
    bool zero_timestamp_tail,
    int64_t ***columns_out, col_delta_timestamp_t **timestamps_out,
    wl_evaluation_control_t *control)
{
    int64_t **columns = NULL;
    col_delta_timestamp_t *timestamps = NULL;
    uint32_t allocated = 0;
    int rc = 0;
    (void)capacity;
    if (table_bytes) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto failed;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
        if (wl_columnar_relation_test_fail_prepare_resize) {
            wl_columnar_relation_test_fail_prepare_resize = false;
            rc = ENOMEM;
            goto failed;
        }
#endif
        columns = (int64_t **)malloc((size_t)table_bytes);
        if (!columns) {
            rc = ENOMEM;
            goto failed;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto failed;
    }
    for (uint32_t c = 0; c < old_shape->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto failed;
        columns[c] = (int64_t *)malloc((size_t)lane_bytes);
        if (!columns[c]) {
            rc = ENOMEM;
            goto failed;
        }
        allocated++;
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto failed;
        if (zero_timestamp_tail)
            wl_columnar_relation_reserve_checkpoint(control,
                WL_COLUMNAR_RELATION_RESERVE_COLUMN, c + 1u);
        if (dst->nrows > 0) {
            if (zero_timestamp_tail)
                wl_columnar_relation_reserve_checkpoint(control,
                    WL_COLUMNAR_RELATION_RESERVE_CELLS, c);
            rc = wl_columnar_relation_copy_cells_controlled(columns[c],
                    dst->columns[c], dst->nrows, false, control);
            if (rc != 0)
                goto failed;
        }
    }
    if (timestamp_bytes) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto failed;
        timestamps = (col_delta_timestamp_t *)malloc((size_t)timestamp_bytes);
        if (!timestamps) {
            rc = ENOMEM;
            goto failed;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto failed;
        if (zero_timestamp_tail)
            wl_columnar_relation_reserve_checkpoint(control,
                WL_COLUMNAR_RELATION_RESERVE_TIMESTAMP_LIVE, 0);
        rc = wl_columnar_relation_copy_timestamps_controlled(timestamps,
                dst->timestamps, dst->nrows, control);
        if (rc != 0)
            goto failed;
        if (zero_timestamp_tail) {
            wl_columnar_relation_reserve_checkpoint(control,
                WL_COLUMNAR_RELATION_RESERVE_TIMESTAMP_ZERO, 0);
            for (uint32_t row = dst->nrows; row < capacity;) {
                uint32_t n = capacity - row;
                if (n > WL_EVALUATION_CONTROL_QUANTUM / 5u)
                    n = (uint32_t)(WL_EVALUATION_CONTROL_QUANTUM / 5u);
                rc = wl_evaluation_control_charge(control, (uint64_t)n * 5u);
                if (rc != 0)
                    goto failed;
                for (uint32_t i = 0; i < n; i++) {
                    timestamps[row + i].iteration = 0;
                    timestamps[row + i].stratum = 0;
                    timestamps[row + i].worker = 0;
                    timestamps[row + i]._reserved = 0;
                    timestamps[row + i].multiplicity = 0;
                }
                row += n;
                rc = wl_columnar_relation_work_tail(control, (uint64_t)n * 5u);
                if (rc != 0)
                    goto failed;
            }
        }
    }
    *columns_out = columns;
    *timestamps_out = timestamps;
    return 0;
failed:
    for (uint32_t c = 0; c < allocated; c++)
        free(columns[c]);
    free(columns);
    free(timestamps);
    return rc;
}

/* Raw authority: unpublished/exclusive caller-owned relation or held writer;
 * no lease is accepted/advanced. Every fallible check precedes publication. */
int
wl_columnar_relation_reserve_capacity_admitted_controlled(col_rel_t *r,
    uint32_t requested, bool *denied, wl_evaluation_control_t *control)
{
    col_rel_payload_shape_t old_shape, final_shape;
    col_rel_payload_txn_t pending;
    wl_columnar_memory_reservation_t existing, previous;
    int64_t **columns = NULL;
    col_delta_timestamp_t *timestamps = NULL;
    uint64_t old_bytes, final_bytes, incoming, lane_bytes, table_bytes,
        ts_bytes;
    uint64_t ledger_before, ledger_after;
    bool transaction = false;
    wl_columnar_mem_ledger_exclusive_prepared_t ledger_prepared;
    int rc;
    if (!control)
        return col_rel_reserve_capacity_admitted(r, requested, denied);
    if (denied)
        *denied = false;
    if (r)
        r->memory_budget_denial_pending = false;
    if (!r)
        return EINVAL;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        return rc;
    if (!col_rel_timestamp_shape_valid(r) || r->nrows > r->capacity)
        return EINVAL;
    bool growth = requested > r->capacity;
    if (r->col_shared || r->arena_owned)
        return growth ? ENOTSUP : wl_evaluation_control_charge(control, 0);
    if (growth && r->storage_owner && r->storage_owner != r)
        return ENOTSUP;
    if (!growth && !r->memory_governor)
        return wl_evaluation_control_charge(control, 0);
    if (growth) {
        if (col_rel_storage_alias_borrow_count(r))
            return EBUSY;
        if (!wl_columnar_relation_generation_valid(r->storage_generation))
            return EINVAL;
        if (r->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
            return EOVERFLOW;
    }
    if ((r->ncols && r->capacity && !r->columns)
        || (r->columns && r->columns == r->merge_columns)
        || (r->columns && r->columns == r->retract_backup_columns)
        || (r->merge_columns && r->merge_columns == r->retract_backup_columns)
        || (!r->ncols && (r->merge_columns || r->retract_backup_columns)))
        return EINVAL;
    old_shape = (col_rel_payload_shape_t) {
        .ncols = r->ncols, .column_capacity = r->capacity,
        .timestamp_capacity = r->timestamp_capacity,
        .merge_capacity = r->merge_buf_cap,
        .backup_capacity = r->retract_backup_capacity,
        .backup_timestamp_capacity = r->retract_backup_timestamp_capacity,
        .active_table = r->columns != NULL,
        .merge_table = r->merge_columns != NULL,
        .backup_table = r->retract_backup_columns != NULL,
        .scratch = r->row_scratch != NULL,
    };
    if (r->columns) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc != 0)
                return rc;
            if (!r->columns[c])
                return EINVAL;
            old_shape.owned_columns++;
            wl_columnar_relation_reserve_checkpoint(control,
                WL_COLUMNAR_RELATION_RESERVE_SHAPE, c + 1u);
            rc = wl_columnar_relation_work_tail(control, 1);
            if (rc != 0)
                return rc;
        }
    }
    if (!col_rel_payload_shape_bytes(&old_shape, &old_bytes))
        return EOVERFLOW;
    if (r->memory_governor) {
        uint64_t state = atomic_load_explicit(&r->retained_reservation.state,
                memory_order_acquire);
        if (r->retained_reservation.identity != &r->retained_reservation
            || (r->retained_reserved_bytes
            ? !wl_columnar_relation_retirement_reservation_valid(r)
            || state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            : state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
            && state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED))
            return EINVAL;
    }
    wl_columnar_memory_reservation_init(&existing);
    wl_columnar_memory_reservation_init(&previous);
    if (!growth) {
        if (old_bytes <= r->retained_reserved_bytes)
            return wl_evaluation_control_charge(control, 0);
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_growth_controlled(
            wl_columnar_memory_governor_ref_get(r->memory_governor),
            r->retained_reserved_bytes, old_bytes, &existing, &status, control);
        if (rc != 0)
            return rc;
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            bool refusal = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED;
            if (denied) *denied = refusal;
            r->memory_budget_denial_pending = refusal;
            return refusal ? ENOMEM : status ==
                   WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
        }
        rc = wl_evaluation_control_charge(control, 96);
        if (rc != 0) {
            col_rel_reservation_rollback(&existing);
            return rc;
        }
        rc = wl_columnar_relation_work_tail(control, 96);
        if (rc != 0) {
            col_rel_reservation_rollback(&existing);
            return rc;
        }
        wl_columnar_relation_reserve_checkpoint(control,
            WL_COLUMNAR_RELATION_RESERVE_FINAL, 0);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc == 0)
            rc = wl_columnar_relation_publish_retained_controlled(r, &existing,
                    old_bytes,
                    NULL, control);
        if (rc != 0)
            col_rel_reservation_rollback(&existing);
        return rc;
    }
    final_shape = old_shape;
    final_shape.owned_columns = r->ncols;
    final_shape.column_capacity = requested;
    final_shape.timestamp_capacity = r->timestamps ? requested : 0;
    final_shape.active_table = r->ncols != 0;
    if (!col_rel_payload_shape_bytes(&final_shape, &final_bytes)
        || !col_rel_payload_grid_bytes(r->ncols, r->ncols, requested,
        r->ncols != 0, &incoming)
        || !wl_columnar_memory_size_mul(requested, sizeof(int64_t), &lane_bytes)
        || !wl_columnar_memory_size_mul(r->ncols, sizeof(int64_t *),
        &table_bytes)
        || !wl_columnar_memory_size_mul(final_shape.timestamp_capacity,
        sizeof(col_delta_timestamp_t), &ts_bytes)
        || !wl_columnar_memory_size_add(incoming, ts_bytes, &incoming)
        || !wl_columnar_memory_size_mul(old_shape.owned_columns, r->capacity,
        &ledger_before)
        || !wl_columnar_memory_size_mul(ledger_before, sizeof(int64_t),
        &ledger_before)
        || !wl_columnar_memory_size_mul(r->ncols, requested, &ledger_after)
        || !wl_columnar_memory_size_mul(ledger_after, sizeof(int64_t),
        &ledger_after)
        || lane_bytes > SIZE_MAX || table_bytes > SIZE_MAX ||
        ts_bytes > SIZE_MAX)
        return EOVERFLOW;
    rc = wl_columnar_relation_payload_begin_controlled(r, final_bytes, incoming,
            &pending, &transaction, control);
    if (rc != 0) {
        if (denied) *denied = r->memory_budget_denial_pending;
        return rc;
    }
    rc = wl_columnar_relation_prepare_growth_controlled(r, &old_shape,
            requested,
            lane_bytes, table_bytes, ts_bytes, true, &columns, &timestamps,
            control);
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_ledger_prepare_exclusive(r, ledger_before,
            ledger_after, ts_bytes, &ledger_prepared, control);
    if (rc != 0)
        goto done;
    rc = wl_evaluation_control_charge(control, 64);
    if (rc != 0)
        goto done;
    wl_columnar_relation_reserve_checkpoint(control,
        WL_COLUMNAR_RELATION_RESERVE_FINAL, 0);
    rc = wl_columnar_relation_work_tail(control, 64);
    if (rc != 0)
        goto done;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        goto done;
    if (transaction) {
        rc = wl_columnar_relation_publish_payload_controlled(r, &pending,
                final_bytes, &previous, control);
        if (rc != 0)
            goto done;
        transaction = false;
    }
    /* Non-failing canonical publication: no control polling beyond this point. */
    int64_t **old_columns = r->columns;
    col_delta_timestamp_t *old_timestamps = r->timestamps;
    r->columns = columns; r->timestamps = timestamps;
    r->capacity = requested; r->timestamp_capacity = timestamps ? requested : 0;
    columns = NULL; timestamps = NULL;
    wl_columnar_relation_touch_storage(r);
    col_columns_free(old_columns, r->ncols); free(old_timestamps);
    col_rel_release_retired_reservation(&previous);
    wl_columnar_relation_retire_payload_credit_known(r, final_bytes);
    wl_columnar_relation_ledger_apply_exclusive(r, ts_bytes, &ledger_prepared);
    return 0;
done:
    col_columns_free(columns, r->ncols); free(timestamps);
    if (transaction)
        col_rel_payload_txn_rollback(r, &pending);
    return rc;
}

/* Mandatory single-role cleanup never observes control after a published row.
 * Only our pending initialization may be restored, under its real descriptor
 * writer. An independent initializer has no journal in this set. */
static int
wl_columnar_relation_row_finish(wl_columnar_relation_mutation_single_t *single,
    int rc, bool restore, wl_evaluation_control_t *control)
{
    wl_columnar_relation_mutation_set_t *set = &single->set;
    if (set->identity != (uintptr_t)set)
        return wl_columnar_relation_cleanup_result(rc, EINVAL);
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_single_before_cleanup)
        wl_columnar_relation_test_single_before_cleanup(control, single);
#else
    (void)control;
#endif
    if (restore && set->initialization_count) {
        col_rel_t *r = single->initialization.relation;
        int valid = wl_columnar_source_access_writer_validate(
            &single->descriptor.writer, &r->descriptor_access);
        if (!valid) {
            r->storage_owner = NULL;
            r->storage_owner_identity = single->initialization.owner_identity;
            r->storage_owner_generation =
                single->initialization.owner_generation;
            atomic_store_explicit(&r->storage_alias_borrows,
                single->initialization.borrows, memory_order_relaxed);
        }
        rc = wl_columnar_relation_cleanup_result(rc, valid);
    }
    bool retired = true;
    for (size_t i = set->owners_acquired; i > 0; i--) {
        wl_columnar_source_access_writer_t *writer = &single->owners[i -
                1].writer;
        rc = wl_columnar_relation_cleanup_result(rc,
                wl_columnar_source_access_writer_release(writer));
        if (writer->owner)
            retired = false;
    }
    if (set->descriptors_acquired) {
        rc = wl_columnar_relation_cleanup_result(rc,
                wl_columnar_source_access_writer_release(
                    &single->descriptor.writer));
        if (single->descriptor.writer.owner)
            retired = false;
    }
    if (retired)
        *single = (wl_columnar_relation_mutation_single_t){0};
    return rc;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_relation_test_row_checkpoint)(
    wl_evaluation_control_t *, int, uint64_t);
bool wl_columnar_relation_test_fail_row_scratch_alloc;
bool wl_columnar_relation_test_fail_row_scratch_commit;
#endif
static void
wl_columnar_relation_row_checkpoint(wl_evaluation_control_t *control,
    int stage, uint64_t progress)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_row_checkpoint)
        wl_columnar_relation_test_row_checkpoint(control, stage, progress);
#else
    (void)control; (void)stage; (void)progress;
#endif
}

/* One validated tuple is copied into unreachable spare cells or private growth.
 * The prefix is always raw, including FLOAT bit patterns. */
static int
wl_columnar_relation_row_copy_controlled(col_rel_t *r, int64_t **columns,
    uint32_t index, const int64_t *row, wl_evaluation_control_t *control)
{
    for (uint32_t c = 0; c < r->ncols; c++) {
        uint64_t work = r->column_types ? 3u : 1u;
        int rc = wl_evaluation_control_charge(control, work);
        if (rc)
            return rc;
        int64_t value = row[c];
        if (r->column_types && r->column_types[c] == WIRELOG_TYPE_FLOAT
            && wl_columnar_float_bits_zero(value))
            value = 0;
        columns[c][index] = value;
        wl_columnar_relation_row_checkpoint(control,
            WL_COLUMNAR_RELATION_ROW_COPY, c + 1u);
        rc = wl_columnar_relation_work_tail(control, work);
        if (rc)
            return rc;
    }
    return 0;
}

enum { WL_COLUMNAR_RELATION_ROW_PUBLICATION_WORK = 173 };
typedef char wl_columnar_relation_row_publication_bound[
        WL_COLUMNAR_RELATION_ROW_PUBLICATION_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
    ? 1 : -1];

/* Fixed forward publication allowance: payload/token 64, lease/journal 48,
 * rows/view 8, two ledger requests 48 and timestamp gauge 1, final G 4 =173.
 * Bounds are admitted reservations, not exact processed-work telemetry. Old
 * grids and scratch retirement are mandatory cleanup, physical before credit.
 */
static int
wl_columnar_relation_row_body(col_rel_t *r, const int64_t *row,
    wl_columnar_relation_mutation_single_t *single,
    wl_evaluation_control_t *control)
{
    wl_columnar_relation_mutation_lease_t *lease = &single->lease;
    col_rel_payload_shape_t old_shape, final_shape;
    col_rel_payload_txn_t pending;
    wl_columnar_memory_reservation_t previous, scratch_credit;
    wl_columnar_mem_ledger_exclusive_prepared_t ledger_prepared;
    int64_t inline_row[16], *scratch = NULL, **columns = NULL;
    col_delta_timestamp_t *timestamps = NULL;
    bool transaction = false, scratch_held = false;
    uint64_t final_bytes = 0, ts_bytes = 0;
    int rc = wl_evaluation_control_charge(control, 32);
    if (rc)
        return rc;
    rc = col_rel_mutation_lease_validate(lease, r);
    if (rc)
        return rc;
    rc = wl_columnar_relation_work_tail(control, 32);
    if (rc)
        return rc;
    rc = wl_evaluation_control_charge(control, 64);
    if (rc)
        return rc;
    if (!col_rel_timestamp_shape_valid(r) || r->nrows > r->capacity)
        return EINVAL;
    bool replaces = col_rel_append_replaces_storage(r);
    if (r->nrows == UINT32_MAX
        || r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || (replaces && r->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u))
        return EOVERFLOW;
    if (!r->ncols && (!r->relation_identity || !r->view_generation
        || !r->storage_generation))
        return EINVAL;
    if (r->col_shared || r->arena_owned || r->storage_owner != r
        || r->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || r->compound_count || r->inline_physical_offset
        || r->compound_arity_map || r->compound_arity_len)
        return ENOTSUP;
    bool empty = !r->nrows && !r->capacity;
    if ((r->ncols && !r->columns && !empty)
        || (r->columns && r->columns == r->merge_columns)
        || (r->columns && r->columns == r->retract_backup_columns)
        || (r->merge_columns && r->merge_columns == r->retract_backup_columns)
        || (!r->ncols && (r->merge_columns || r->retract_backup_columns)))
        return EINVAL;
    old_shape = (col_rel_payload_shape_t){
        .ncols = r->ncols, .column_capacity = r->capacity,
        .timestamp_capacity = r->timestamp_capacity,
        .merge_capacity = r->merge_buf_cap,
        .backup_capacity = r->retract_backup_capacity,
        .backup_timestamp_capacity = r->retract_backup_timestamp_capacity,
        .active_table = r->columns != NULL,
        .merge_table = r->merge_columns != NULL,
        .backup_table = r->retract_backup_columns != NULL,
        .scratch = r->row_scratch != NULL,
    };
    rc = wl_columnar_relation_work_tail(control, 64);
    if (rc)
        return rc;
    for (uint32_t c = 0; c < r->ncols; c++) {
        uint64_t work = 1u + (r->merge_columns != NULL)
            + (r->retract_backup_columns != NULL);
        rc = wl_evaluation_control_charge(control, work);
        if (rc)
            return rc;
        if ((!r->columns || !r->columns[c]) && !empty)
            return EINVAL;
        if (r->columns && r->columns[c])
            old_shape.owned_columns++;
        if ((r->merge_columns && !r->merge_columns[c])
            || (r->retract_backup_columns && !r->retract_backup_columns[c]))
            return EINVAL;
        wl_columnar_relation_row_checkpoint(control,
            WL_COLUMNAR_RELATION_ROW_STRUCTURE, c + 1u);
        rc = wl_columnar_relation_work_tail(control, work);
        if (rc)
            return rc;
    }
    rc = wl_evaluation_control_charge(control, 16);
    if (rc)
        return rc;
    uint64_t row_bytes, lane_bytes, table_bytes;
    uintptr_t start = (uintptr_t)row;
    if (!wl_columnar_memory_size_mul(r->ncols, sizeof(*row), &row_bytes)
        || row_bytes > SIZE_MAX || row_bytes > UINTPTR_MAX - start
        || !wl_columnar_memory_size_mul(r->capacity, sizeof(*row), &lane_bytes))
        return EOVERFLOW;
    rc = wl_columnar_relation_work_tail(control, 16);
    if (rc)
        return rc;
    bool overlap = false;
    for (uint32_t c = 0; r->columns && c < r->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc)
            return rc;
        uintptr_t col = (uintptr_t)r->columns[c];
        if (lane_bytes > SIZE_MAX || lane_bytes > UINTPTR_MAX - col)
            return EOVERFLOW;
        if (row_bytes && lane_bytes && start < col + lane_bytes
            && col < start + row_bytes)
            overlap = true;
        wl_columnar_relation_row_checkpoint(control,
            WL_COLUMNAR_RELATION_ROW_OVERLAP, c + 1u);
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc)
            return rc;
    }
    for (uint32_t c = 0; r->column_types && c < r->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc)
            return rc;
        if (r->column_types[c] == WIRELOG_TYPE_FLOAT
            && !wl_columnar_float_bits_valid(row[c]))
            return EINVAL;
        wl_columnar_relation_row_checkpoint(control,
            WL_COLUMNAR_RELATION_ROW_TYPES, c + 1u);
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc)
            return rc;
    }
    wl_columnar_memory_reservation_init(&scratch_credit);
    wl_columnar_memory_reservation_init(&previous);
    if (overlap) {
        int64_t *capture = inline_row;
        if (r->ncols > 16) {
            if (r->memory_governor) {
                wl_columnar_memory_admission_status_t status;
                rc = wl_columnar_memory_governor_reserve_checked_controlled(
                    wl_columnar_memory_governor_ref_get(r->memory_governor),
                    row_bytes, &scratch_credit, &status, control);
                if (rc)
                    goto done;
                if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                    && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                    r->memory_budget_denial_pending = status ==
                        WL_COLUMNAR_MEMORY_ADMISSION_DENIED;
                    rc = r->memory_budget_denial_pending ? ENOMEM
                        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                        ? EOVERFLOW : EINVAL;
                    goto done;
                }
                scratch_held = true;
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_relation_test_fail_row_scratch_commit) {
                    if (atomic_load(&scratch_credit.state)
                        != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
                        || !scratch_credit.bytes)
                        abort();
                    wl_columnar_relation_test_fail_row_scratch_commit = false;
                    rc = ENOMEM;
                    goto done;
                }
#endif
                bool committed;
                rc = wl_columnar_memory_governor_commit_controlled(
                    &scratch_credit, &scratch_credit, &committed, control);
                if (rc)
                    goto done;
                if (!committed) {
                    rc = ENOMEM;
                    goto done;
                }
            }
            rc = wl_evaluation_control_charge(control, 1);
            if (rc)
                goto done;
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_relation_test_fail_row_scratch_alloc) {
                wl_columnar_relation_test_fail_row_scratch_alloc = false;
                rc = ENOMEM;
                goto done;
            }
#endif
            scratch = malloc((size_t)row_bytes);
            if (!scratch) {
                rc = ENOMEM;
                goto done;
            }
            rc = wl_columnar_relation_work_tail(control, 1);
            if (rc)
                goto done;
            capture = scratch;
        }
        for (uint32_t c = 0; c < r->ncols; c++) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc)
                goto done;
            capture[c] = row[c];
            wl_columnar_relation_row_checkpoint(control,
                WL_COLUMNAR_RELATION_ROW_CAPTURE, c + 1u);
            rc = wl_columnar_relation_work_tail(control, 1);
            if (rc)
                goto done;
        }
        row = capture;
    }
    bool resize = r->nrows >= r->capacity;
    bool short_ts = r->timestamps && r->nrows >= r->timestamp_capacity;
    bool growth = resize || short_ts;
    uint32_t capacity = resize ? (r->capacity ? r->capacity * 2u
        : COL_REL_INIT_CAP) : r->capacity;
    if (growth && (lease->storage_transitioned || lease->detached
        || lease->owner != r || lease->owner_slot != lease->self_owner_slot
        || lease->owner_generation != r->storage_generation)) {
        rc = EINVAL;
        goto done;
    }
    if (resize && capacity <= r->capacity) {
        rc = EOVERFLOW;
        goto done;
    }
    if (growth) {
        rc = wl_evaluation_control_charge(control, 64);
        if (rc)
            goto done;
        uint64_t incoming, before, after;
        final_shape = old_shape;
        final_shape.owned_columns = r->ncols;
        final_shape.active_table = r->ncols != 0;
        final_shape.column_capacity = capacity;
        final_shape.timestamp_capacity = r->timestamps ? capacity : 0;
        if (!col_rel_payload_shape_bytes(&final_shape, &final_bytes)
            || !col_rel_payload_grid_bytes(r->ncols, r->ncols, capacity,
            r->ncols != 0, &incoming)
            || !wl_columnar_memory_size_mul(capacity, sizeof(int64_t),
            &lane_bytes)
            || !wl_columnar_memory_size_mul(r->ncols, sizeof(int64_t *),
            &table_bytes)
            || !wl_columnar_memory_size_mul(final_shape.timestamp_capacity,
            sizeof(col_delta_timestamp_t), &ts_bytes)
            || !wl_columnar_memory_size_add(incoming, ts_bytes, &incoming)
            || !wl_columnar_memory_size_mul(old_shape.owned_columns,
            r->capacity, &before)
            || !wl_columnar_memory_size_mul(before, sizeof(int64_t), &before)
            || !wl_columnar_memory_size_mul(r->ncols, capacity, &after)
            || !wl_columnar_memory_size_mul(after, sizeof(int64_t), &after)
            || lane_bytes > SIZE_MAX || table_bytes > SIZE_MAX ||
            ts_bytes > SIZE_MAX) {
            rc = EOVERFLOW;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 64);
        if (rc)
            goto done;
        rc = wl_columnar_relation_payload_begin_controlled(r, final_bytes,
                incoming, &pending, &transaction, control);
        if (rc)
            goto done;
        rc = wl_columnar_relation_prepare_growth_controlled(r, &old_shape,
                capacity, lane_bytes, table_bytes, ts_bytes, !short_ts,
                &columns, &timestamps, control);
        if (rc)
            goto done;
        rc = wl_columnar_relation_ledger_prepare_exclusive(r, before, after,
                ts_bytes, &ledger_prepared, control);
        if (rc)
            goto done;
    }
    rc = wl_columnar_relation_row_copy_controlled(r,
            growth ? columns : r->columns, r->nrows, row, control);
    if (rc)
        goto done;
    /* Nonshort growth already initialized this whole tail, including the row. */
    if (r->timestamps && (!growth || short_ts)) {
        rc = wl_evaluation_control_charge(control, 5);
        if (rc)
            goto done;
        col_delta_timestamp_t *ts = growth ? timestamps : r->timestamps;
        ts[r->nrows] = (col_delta_timestamp_t){0};
        rc = wl_columnar_relation_work_tail(control, 5);
        if (rc)
            goto done;
    }
    if (single->set.initialization_count
        && (single->set.initialization_count != 1
        || single->initialization.relation != r
        || wl_columnar_source_access_writer_validate(
            &single->descriptor.writer, &r->descriptor_access))) {
        rc = EINVAL;
        goto done;
    }
    rc = wl_evaluation_control_charge(control,
            WL_COLUMNAR_RELATION_ROW_PUBLICATION_WORK);
    if (rc)
        goto done;
    wl_columnar_relation_row_checkpoint(control,
        WL_COLUMNAR_RELATION_ROW_FINAL, 0);
    rc = wl_columnar_relation_work_tail(control,
            WL_COLUMNAR_RELATION_ROW_PUBLICATION_WORK);
    if (rc)
        goto done;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc)
        goto done;
    if (transaction) {
        rc = wl_columnar_relation_publish_payload_controlled(r, &pending,
                final_bytes, &previous, control);
        if (rc)
            goto done;
        transaction = false;
    }
    /* No callbacks/control after canonical credit: commit init with the row. */
    single->set.initialization_count = 0;
    single->initialization =
        (wl_columnar_relation_mutation_initialization_t){0};
    if (growth) {
        int64_t **old_columns = r->columns;
        col_delta_timestamp_t *old_ts = r->timestamps;
        r->columns = columns; r->timestamps = timestamps;
        r->capacity = capacity;
        r->timestamp_capacity = timestamps ? capacity : 0;
        columns = NULL; timestamps = NULL;
        wl_columnar_relation_touch_storage(r);
        /* The fixed single role and same-owner advancement predicates were
         * checked before the cutoff; no generic role scan or fallible advance. */
        lease->relation_generation = r->storage_generation;
        lease->owner_generation = r->storage_generation;
        lease->storage_transitioned = true;
        single->set.published = true;
        col_columns_free(old_columns, r->ncols);
        free(old_ts);
        col_rel_release_retired_reservation(&previous);
        wl_columnar_relation_retire_payload_credit_known(r, final_bytes);
        wl_columnar_relation_ledger_apply_exclusive(r, ts_bytes,
            &ledger_prepared);
    }
    r->nrows++;
    wl_columnar_relation_touch_view(r);
    rc = 0;
done:
    col_columns_free(columns, r->ncols);
    free(timestamps);
    if (transaction)
        col_rel_payload_txn_rollback(r, &pending);
    free(scratch);
    if (scratch_held)
        col_rel_reservation_rollback(&scratch_credit);
    return rc;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_relation_test_private_checkpoint)(wl_evaluation_control_t *,
    wl_columnar_relation_private_batch_t *, int, uint64_t);
#endif

static void
wl_columnar_relation_private_checkpoint(wl_evaluation_control_t *control,
    wl_columnar_relation_private_batch_t *record, int phase, uint64_t progress)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_private_checkpoint)
        wl_columnar_relation_test_private_checkpoint(control, record, phase,
            progress);
#else
    (void)control; (void)record; (void)phase; (void)progress;
#endif
}

bool
wl_columnar_relation_private_batch_disposable(
    const wl_columnar_relation_private_batch_t *record)
{
    if (!record || !record->ready) return false;
    if (record->phase == WL_COLUMNAR_RELATION_PRIVATE_RESETTING) return true;
    if (record->phase != WL_COLUMNAR_RELATION_PRIVATE_INERT
        && record->phase != WL_COLUMNAR_RELATION_PRIVATE_RETIRED) return false;
    const wl_columnar_relation_mutation_single_t *single =
        &record->batch.single;
    return !record->batch.active
           && col_rel_mutation_writer_inert(&single->descriptor.writer)
           && col_rel_mutation_writer_inert(&single->owners[0].writer)
           && col_rel_mutation_writer_inert(&single->owners[1].writer);
}

int
wl_columnar_relation_private_batch_retire(
    wl_columnar_relation_private_batch_t *record, int rc)
{
    if (!record || !record->ready) return EINVAL;
    /* RESETTING is a terminal discard marker, never a partially valid single. */
    if (record->phase == WL_COLUMNAR_RELATION_PRIVATE_RESETTING
        || record->phase == WL_COLUMNAR_RELATION_PRIVATE_RETIRED
        || record->phase == WL_COLUMNAR_RELATION_PRIVATE_INERT) return rc;
    if (record->phase != WL_COLUMNAR_RELATION_PRIVATE_INSTALLING
        && record->phase != WL_COLUMNAR_RELATION_PRIVATE_HELD
        && record->phase != WL_COLUMNAR_RELATION_PRIVATE_RETIRING)
        return wl_columnar_relation_cleanup_result(rc, EINVAL);
    record->phase = WL_COLUMNAR_RELATION_PRIVATE_RETIRING;
    record->batch.active = false;
    wl_columnar_relation_mutation_single_t *single = &record->batch.single;
    if (!col_rel_mutation_writer_inert(&single->owners[1].writer)
        || single->set.initialization_count)
        return wl_columnar_relation_cleanup_result(rc, EINVAL);
    /* Inspect actual tokens, including a descriptor whose counter is not yet
     * published. Owner exclusion retires before descriptor exclusion. */
    wl_columnar_source_access_writer_t *owner = &single->owners[0].writer;
    wl_columnar_source_access_writer_t *descriptor = &single->descriptor.writer;
    if (owner->owner) {
        int valid = single->owners[0].owner == record->batch.relation
            ? wl_columnar_source_access_writer_validate(owner,
                &record->batch.relation->source_access) : EINVAL;
        rc = wl_columnar_relation_cleanup_result(rc, valid ? valid
                : wl_columnar_source_access_writer_release(owner));
    } else if (!col_rel_mutation_writer_inert(owner))
        rc = wl_columnar_relation_cleanup_result(rc, EINVAL);
    if (descriptor->owner) {
        int valid = single->descriptor.relation == record->batch.relation
            ? wl_columnar_source_access_writer_validate(descriptor,
                &record->batch.relation->descriptor_access) : EINVAL;
        rc = wl_columnar_relation_cleanup_result(rc, valid ? valid
                : wl_columnar_source_access_writer_release(descriptor));
    } else if (!col_rel_mutation_writer_inert(descriptor))
        rc = wl_columnar_relation_cleanup_result(rc, EINVAL);
    if (col_rel_mutation_writer_inert(owner)
        && col_rel_mutation_writer_inert(descriptor))
        record->phase = WL_COLUMNAR_RELATION_PRIVATE_RETIRED;
    return rc;
}

static int
wl_columnar_relation_private_single_reset(
    wl_columnar_relation_private_batch_t *record,
    wl_evaluation_control_t *control)
{
    if (!record || !record->ready) return EINVAL;
    if (record->phase == WL_COLUMNAR_RELATION_PRIVATE_RESETTING) return EBUSY;
    if (record->phase != WL_COLUMNAR_RELATION_PRIVATE_INERT
        && record->phase != WL_COLUMNAR_RELATION_PRIVATE_RETIRED) return EBUSY;
    wl_columnar_relation_mutation_single_t *single = &record->batch.single;
    int rc = wl_evaluation_control_charge(control, 64);
    if (rc) return rc;
    if (!col_rel_mutation_writer_inert(&single->descriptor.writer)
        || !col_rel_mutation_writer_inert(&single->owners[0].writer)
        || !col_rel_mutation_writer_inert(
            &single->owners[1].writer)) return EBUSY;
    rc = wl_columnar_relation_work_tail(control, 64);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    record->phase = WL_COLUMNAR_RELATION_PRIVATE_RESETTING;
    rc = wl_columnar_relation_work_tail(control, 8);
    if (rc) return rc;
    for (size_t offset = 0; offset < sizeof(*single);) {
        size_t bytes = sizeof(*single) - offset;
        if (bytes > 128) bytes = 128;
        rc = wl_evaluation_control_charge(control, bytes);
        if (rc) return rc;
        memset((char *)single + offset, 0, bytes);
        offset += bytes;
        wl_columnar_relation_private_checkpoint(control, record, 1, offset);
        rc = wl_columnar_relation_work_tail(control, bytes);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        rc = wl_columnar_relation_work_tail(control, 8);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    rc = wl_columnar_relation_work_tail(control, 8);
    if (rc) return rc;
    record->phase = WL_COLUMNAR_RELATION_PRIVATE_INERT;
    return 0;
}

static int
wl_columnar_relation_private_single_acquire(
    wl_columnar_relation_private_batch_t *record, col_rel_t *r,
    wl_evaluation_control_t *control)
{
    wl_columnar_relation_mutation_single_t *single = &record->batch.single;
    wl_columnar_relation_mutation_set_t *set = &single->set;
    uint64_t nonce;
    int rc = wl_columnar_relation_nonce_controlled(&nonce, control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    record->phase = WL_COLUMNAR_RELATION_PRIVATE_INSTALLING;
    rc = wl_columnar_relation_work_tail(control, 8);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 96);
    if (rc) goto fail;
    single->role.relation = r;
    single->role.role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION;
    set->identity = (uintptr_t)set; set->acquisition_nonce = nonce;
    set->roles = &single->role; set->descriptors = &single->descriptor;
    set->owners = single->owners; set->leases = &single->lease;
    set->initializations = &single->initialization;
    set->descriptor_count = 1; set->lease_count = 1;
    single->descriptor.relation = r;
    rc = wl_columnar_relation_work_tail(control, 96);
    if (rc) goto fail;
    rc = wl_columnar_relation_writer_acquire_controlled(&r->descriptor_access,
            &single->descriptor.writer, control);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) goto fail;
    set->descriptors_acquired = 1;
    wl_columnar_relation_private_checkpoint(control, record, 2, 0);
    rc = wl_columnar_relation_work_tail(control, 8);
    if (rc) goto fail;
    col_rel_t *owner = NULL;
    rc = wl_columnar_relation_resolve_controlled(r, &owner, control);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 96);
    if (rc) goto fail;
    if (owner != r || !r->relation_identity
        || !wl_columnar_relation_generation_valid(r->storage_generation)
        || r->storage_owner_identity != r->relation_identity
        || r->storage_owner_generation != r->storage_generation
        || col_rel_storage_alias_borrow_count(r)) {
        rc = EINVAL; goto fail;
    }
    single->owners[0].owner = r; set->owner_count = 1;
    rc = wl_columnar_relation_work_tail(control, 96);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) goto fail;
    single->lease.set = set; single->lease.acquisition_nonce = nonce;
    single->lease.relation = r; single->lease.owner = r;
    single->lease.role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION;
    single->lease.relation_identity = r->relation_identity;
    single->lease.relation_generation = r->storage_generation;
    single->lease.owner_identity = r->relation_identity;
    single->lease.owner_generation = r->storage_generation;
    rc = wl_columnar_relation_work_tail(control, 128);
    if (rc) goto fail;
    rc = wl_columnar_relation_writer_acquire_controlled(&r->source_access,
            &single->owners[0].writer, control);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) goto fail;
    set->owners_acquired = 1;
    wl_columnar_relation_private_checkpoint(control, record, 3, 0);
    rc = wl_columnar_relation_work_tail(control, 8);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) goto fail;
    if (col_rel_storage_alias_borrow_count(r)) {
        rc = EBUSY; goto fail;
    }
    single->lease.identity = (uintptr_t)&single->lease;
    rc = wl_columnar_relation_work_tail(control, 16);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) goto fail;
    rc = col_rel_mutation_lease_validate(&single->lease, r);
    int stop = wl_columnar_relation_work_tail(control, 32);
    if (!rc) rc = stop;
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) goto fail;
    rc = wl_columnar_relation_work_tail(control, 8);
    if (rc) goto fail;
    record->phase = WL_COLUMNAR_RELATION_PRIVATE_HELD;
    return 0;
fail:
    return wl_columnar_relation_private_batch_retire(record, rc);
}

int
wl_columnar_relation_private_append_batch_begin_controlled(
    wl_columnar_relation_private_batch_t *record, col_rel_t *r,
    wl_evaluation_control_t *control)
{
    if (!record || !record->ready || !r || !control) return EINVAL;
    if (record->phase == WL_COLUMNAR_RELATION_PRIVATE_RESETTING) return EBUSY;
    if (record->phase != WL_COLUMNAR_RELATION_PRIVATE_INERT
        && record->phase != WL_COLUMNAR_RELATION_PRIVATE_RETIRED) return EBUSY;
    int rc = wl_evaluation_control_charge(control, 64);
    if (rc) return rc;
    if (record->batch.active || r->storage_owner != r
        || col_rel_storage_alias_borrow_count(r) || !r->relation_identity)
        return EBUSY;
    rc = wl_columnar_relation_work_tail(control, 64);
    if (rc) return rc;
    rc = wl_columnar_relation_private_single_reset(record, control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    record->batch.relation = r; record->batch.controlled_row_committed = false;
    rc = wl_columnar_relation_work_tail(control, 16);
    if (rc) return rc;
    rc = wl_columnar_relation_private_single_acquire(record, r, control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return wl_columnar_relation_private_batch_retire(record, rc);
    rc = wl_columnar_relation_work_tail(control, 16);
    if (rc) return wl_columnar_relation_private_batch_retire(record, rc);
    record->batch.active = true;
    return 0;
}

int
wl_columnar_relation_private_append_batch_row_controlled(
    wl_columnar_relation_private_batch_t *record, const int64_t *row,
    wl_evaluation_control_t *control)
{
    if (!record || !record->ready || !control || !row
        || record->phase != WL_COLUMNAR_RELATION_PRIVATE_HELD
        || !record->batch.active) return EINVAL;
    col_rel_t *r = record->batch.relation;
    if (record->batch.single.lease.storage_transitioned
        && col_rel_append_replaces_storage(r)) {
        wl_columnar_relation_private_checkpoint(control, record, 4, r->nrows);
        int rc = wl_columnar_relation_private_batch_retire(record, 0);
        if (rc) return rc;
        rc = wl_columnar_relation_private_append_batch_begin_controlled(record,
                r, control);
        if (rc) return rc;
    }
    r->memory_budget_denial_pending = false;
    int rc = wl_columnar_relation_row_body(r, row, &record->batch.single,
            control);
    if (!rc) record->batch.controlled_row_committed = true;
    return rc;
}

int
wl_columnar_relation_private_append_batch_end_controlled(
    wl_columnar_relation_private_batch_t *record)
{
    return wl_columnar_relation_private_batch_retire(record, 0);
}

int
wl_columnar_relation_private_cell_controlled(
    wl_columnar_relation_private_batch_t *record, uint32_t row, uint32_t col,
    int64_t value, wl_evaluation_control_t *control)
{
    if (!record || !record->ready || !control
        || record->phase != WL_COLUMNAR_RELATION_PRIVATE_HELD) return EINVAL;
    col_rel_append_batch_t *batch = &record->batch;
    col_rel_t *r = batch->relation;
    wl_columnar_relation_mutation_single_t *single = &batch->single;
    int rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    wl_columnar_relation_private_checkpoint(control, record, 7, row);
    if (!batch->active
        || single->set.identity != (uintptr_t)&single->set
        || single->set.roles != &single->role
        || single->set.descriptors != &single->descriptor
        || single->set.owners != single->owners
        || single->set.leases != &single->lease
        || single->set.initializations != &single->initialization
        || single->lease.identity != (uintptr_t)&single->lease
        || single->lease.set != &single->set
        || single->lease.relation != r || single->descriptor.relation != r
        || single->set.descriptor_count != 1
        || single->set.lease_count != 1 || single->set.owner_count != 1
        || single->set.initialization_count || single->owners[0].owner != r
        || !col_rel_mutation_writer_inert(&single->owners[1].writer)
        || single->lease.owner_slot || single->lease.self_owner_slot
        || single->lease.owner != r) return EINVAL;
    rc = col_rel_mutation_lease_validate(&single->lease, r);
    int stop = wl_columnar_relation_work_tail(control, 32);
    if (!rc) rc = stop;
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 48);
    if (rc) return rc;
    wl_columnar_relation_private_checkpoint(control, record, 5, row);
    if (r->nrows > r->capacity || row >= r->nrows || row >= r->capacity
        || col >= r->ncols || !r->columns
        || !r->columns[col] || r->storage_owner != r || r->col_shared
        || r->arena_owned || r->compound_kind != WIRELOG_COMPOUND_KIND_NONE)
        return EINVAL;
    if (r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EOVERFLOW;
    wirelog_column_type_t type = r->column_types ? r->column_types[col]
        : WIRELOG_TYPE_INT64;
    if (type != WIRELOG_TYPE_INT64 && type != WIRELOG_TYPE_FLOAT) return EINVAL;
    if (type == WIRELOG_TYPE_FLOAT) {
        if (!wl_columnar_float_bits_valid(value)) return EINVAL;
        value = (int64_t)wl_columnar_float_canonical_bits(value);
    }
    uint64_t generation = r->view_generation + 1u;
    rc = wl_columnar_relation_work_tail(control, 48);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    rc = wl_columnar_relation_work_tail(control, 16);
    if (rc) return rc;
    r->columns[col][row] = value;
    r->view_generation = generation;
    single->set.published = true;
    batch->controlled_row_committed = true;
    return 0;
}

int
wl_columnar_relation_append_row_controlled(col_rel_t *r, const int64_t *row,
    wl_evaluation_control_t *control)
{
    if (!control)
        return col_rel_append_row(r, row);
    if (!r || !row)
        return EINVAL;
    wl_columnar_relation_mutation_single_t single = {0};
    int rc = wl_columnar_relation_mutation_single_acquire_controlled(r,
            &single, control);
    if (rc)
        return rc;
    r->memory_budget_denial_pending = false;
    rc = wl_columnar_relation_row_body(r, row, &single, control);
    return wl_columnar_relation_row_finish(&single, rc, rc != 0, control);
}

int
wl_columnar_relation_append_batch_begin_controlled(
    col_rel_append_batch_t *batch,
    col_rel_t *r, wl_evaluation_control_t *control)
{
    if (!control)
        return col_rel_append_batch_begin(batch, r);
    if (!batch || !r || batch->active)
        return EINVAL;
    *batch = (col_rel_append_batch_t){0};
    int rc = wl_columnar_relation_mutation_single_acquire_controlled(r,
            &batch->single, control);
    if (rc)
        return rc;
    batch->relation = r;
    batch->active = true;
    return 0;
}

int
wl_columnar_relation_append_batch_row_controlled(col_rel_append_batch_t *batch,
    const int64_t *row, wl_evaluation_control_t *control)
{
    if (!control)
        return col_rel_append_batch_row(batch, row);
    if (!batch || !batch->active || !row)
        return EINVAL;
    col_rel_t *r = batch->relation;
    if (batch->single.lease.storage_transitioned
        && col_rel_append_replaces_storage(r)) {
        int rc = wl_columnar_relation_row_finish(&batch->single, 0, false,
                control);
        batch->active = false;
        if (rc)
            return rc;
        batch->controlled_row_committed = false;
        rc = wl_columnar_relation_mutation_single_acquire_controlled(r,
                &batch->single, control);
        if (rc)
            return rc;
        batch->active = true;
    }
    r->memory_budget_denial_pending = false;
    int rc = wl_columnar_relation_row_body(r, row, &batch->single, control);
    if (!rc)
        batch->controlled_row_committed = true;
    return rc;
}

int
wl_columnar_relation_append_batch_end_controlled(col_rel_append_batch_t *batch,
    bool commit, wl_evaluation_control_t *control)
{
    if (!control)
        return col_rel_append_batch_end(batch, commit);
    if (!batch || !batch->active)
        return 0;
    batch->active = false;
    if (!batch->controlled_row_committed && commit)
        return wl_columnar_relation_mutation_single_finish_controlled(
            &batch->single, 0, control);
    return wl_columnar_relation_row_finish(&batch->single, 0,
               !batch->controlled_row_committed, control);
}

int
wl_columnar_relation_append_all_controlled(col_rel_t *dst,
    const col_rel_t *src, wl_arena_t *arena, wl_evaluation_control_t *control)
{
    col_rel_t *src_owner = NULL, *dst_owner = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_source_access_writer_t writer = { 0 };
    bool read_held = false, write_held = false;
    uint32_t base, count, new_capacity = 0;
    uint32_t owned_columns = 0;
    bool growth = false, transaction_active = false;
    bool install_types = false, prepared_schema_ok = false;
    bool metadata_committed = false;
    wirelog_column_type_t *prepared_types = NULL;
    struct ArrowSchema prepared_schema;
    wl_columnar_memory_reservation_t metadata_pending, old_metadata;
    uint64_t metadata_bytes = 0;
    int64_t **prepared_columns = NULL;
    col_delta_timestamp_t *prepared_timestamps = NULL;
    int64_t **output_columns;
    col_delta_timestamp_t *output_timestamps;
    col_rel_payload_txn_t pending;
    wl_columnar_memory_reservation_t previous;
    uint64_t final_bytes = 0, ledger_before = 0, ledger_after = 0;
    wl_columnar_mem_ledger_exclusive_prepared_t ledger_prepared;
    uint64_t ledger_timestamps = 0;
    int rc;
    if (!control)
        return col_rel_append_all(dst, src, arena);
    wl_columnar_memory_reservation_init(&metadata_pending);
    wl_columnar_memory_reservation_init(&old_metadata);
    wl_columnar_memory_reservation_init(&previous);
    if (!dst || !src || dst->ncols != src->ncols)
        return EINVAL;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        return rc;
    /* NONE is a plain representation, not a compound shape to defer. */
    if ((dst->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && (dst->compound_count || dst->inline_physical_offset))
        || (src->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && (src->compound_count || src->inline_physical_offset)))
        return EINVAL;
    /* Reject compound metadata before any potentially long map validation. */
    if (dst->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || src->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || dst->compound_arity_map || src->compound_arity_map
        || dst->compound_arity_len || src->compound_arity_len
        || dst->col_shared || src->col_shared || dst->arena_owned
        || src->arena_owned || arena)
        return ENOTSUP;
    rc = wl_columnar_relation_resolve_controlled(src, &src_owner, control);
    if (rc != 0)
        return rc;
    rc = wl_columnar_relation_resolve_controlled(dst, &dst_owner, control);
    if (rc != 0)
        return rc;
    if (src_owner != src || dst_owner != dst)
        return ENOTSUP;
    if (src_owner != dst_owner) {
        rc = wl_columnar_relation_reader_acquire_controlled(
            &src_owner->source_access, &reader, control);
        if (rc != 0)
            return rc;
        read_held = true;
    }
    rc = wl_columnar_relation_writer_acquire_controlled(
        &dst_owner->source_access, &writer, control);
    if (rc != 0)
        goto done;
    write_held = true;
    if (col_rel_storage_alias_borrow_count(dst_owner) != 0) {
        rc = ENOTSUP;
        goto done;
    }
    if (src->nrows > UINT32_MAX - dst->nrows) {
        rc = EOVERFLOW;
        goto done;
    }
    if (!col_rel_timestamp_shape_valid(dst)
        || !col_rel_timestamp_shape_valid(src)
        || src->nrows > src->capacity || dst->nrows > dst->capacity) {
        rc = EINVAL;
        goto done;
    }
    base = dst->nrows;
    count = src->nrows;
    install_types = !dst->column_types && src->column_types && count != 0;
    if (!wl_columnar_relation_generation_valid(dst->view_generation)
        || dst->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || (src->ncols && (!src->columns || !dst->columns))) {
        rc = EINVAL;
        goto done;
    }
    growth = base + count > dst->capacity
        || (dst->timestamps && base + count > dst->timestamp_capacity);
    if (growth &&
        (!wl_columnar_relation_generation_valid(dst->storage_generation)
        || dst->storage_generation >=
        WL_COLUMNAR_REL_GENERATION_INVALID - 1u)) {
        rc = EOVERFLOW;
        goto done;
    }
    for (uint32_t c = 0; c < dst->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        wirelog_column_type_t dt = dst->column_types
            ? dst->column_types[c] : WIRELOG_TYPE_INT64;
        wirelog_column_type_t st = src->column_types
            ? src->column_types[c] : WIRELOG_TYPE_INT64;
        if (!src->columns[c] || !dst->columns[c]
            || ((dt == WIRELOG_TYPE_FLOAT || st == WIRELOG_TYPE_FLOAT)
            && dt != st && count != 0)) {
            rc = EINVAL;
            goto done;
        }
        owned_columns++;
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    /* Empty inputs still poll, but need no second uncharged width walk. */
    if (count == 0) {
        rc = wl_evaluation_control_charge(control, 0);
        goto done;
    }
    /* Legacy typed validation visits every scalar, even integer lanes. */
    if (src->column_types) {
        for (uint32_t row = 0; row < count; row++) {
            for (uint32_t c = 0; c < src->ncols; c++) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                if (src->column_types[c] == WIRELOG_TYPE_FLOAT
                    && !wl_columnar_float_bits_valid(src->columns[c][row])) {
                    rc = EINVAL;
                    goto done;
                }
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
            }
        }
    }
    if (install_types && base != 0) {
        for (uint32_t row = 0; row < base; row++) {
            for (uint32_t c = 0; c < dst->ncols; c++) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                if (src->column_types[c] == WIRELOG_TYPE_FLOAT
                    && !wl_columnar_float_bits_valid(dst->columns[c][row])) {
                    rc = EINVAL;
                    goto done;
                }
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
            }
        }
    }
    if (install_types) {
        dst->memory_budget_denial_pending = false;
        rc = wl_columnar_relation_prepare_types_controlled(dst,
                src->column_types,
                &prepared_types, &prepared_schema, &metadata_bytes,
                &metadata_pending, control);
        if (rc != 0)
            goto done;
        prepared_schema_ok = true;
    }
    output_columns = dst->columns;
    output_timestamps = dst->timestamps;
    if (growth) {
        col_rel_payload_shape_t old_shape = {
            .ncols = dst->ncols, .owned_columns = owned_columns,
            .column_capacity = dst->capacity,
            .timestamp_capacity = dst->timestamp_capacity,
            .merge_capacity = dst->merge_buf_cap,
            .backup_capacity = dst->retract_backup_capacity,
            .backup_timestamp_capacity = dst->retract_backup_timestamp_capacity,
            .active_table = dst->columns != NULL,
            .merge_table = dst->merge_columns != NULL,
            .backup_table = dst->retract_backup_columns != NULL,
            .scratch = dst->row_scratch != NULL,
        };
        col_rel_payload_shape_t final_shape = old_shape;
        uint64_t incoming, old_bytes, lane_bytes, table_bytes, timestamp_bytes;
        if ((dst->columns && dst->columns == dst->merge_columns)
            || (dst->columns && dst->columns == dst->retract_backup_columns)
            || (dst->merge_columns &&
            dst->merge_columns == dst->retract_backup_columns)
            || (!dst->ncols &&
            (dst->merge_columns || dst->retract_backup_columns))) {
            rc = EINVAL;
            goto done;
        }
        new_capacity = dst->capacity;
        if (new_capacity < base + count) {
            new_capacity = dst->capacity ? dst->capacity : COL_REL_INIT_CAP;
            while (new_capacity < base + count) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                if (new_capacity > UINT32_MAX / 2u) {
                    rc = EOVERFLOW;
                    goto done;
                }
                new_capacity *= 2u;
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
            }
        }
        final_shape.column_capacity = new_capacity;
        final_shape.timestamp_capacity = dst->timestamps ? new_capacity : 0;
        final_shape.active_table = dst->ncols != 0;
        if (!col_rel_payload_shape_bytes(&old_shape, &old_bytes)
            || !col_rel_payload_shape_bytes(&final_shape, &final_bytes)
            || !col_rel_payload_grid_bytes(dst->ncols, owned_columns,
            new_capacity, dst->ncols != 0, &incoming)
            || !wl_columnar_memory_size_mul(new_capacity ? new_capacity : 1u,
            sizeof(int64_t), &lane_bytes)
            || !wl_columnar_memory_size_mul(dst->ncols, sizeof(int64_t *),
            &table_bytes)
            || !wl_columnar_memory_size_mul(final_shape.timestamp_capacity,
            sizeof(col_delta_timestamp_t), &timestamp_bytes)
            || !wl_columnar_memory_size_add(incoming, timestamp_bytes,
            &incoming)
            || !wl_columnar_memory_size_mul(owned_columns, dst->capacity,
            &ledger_before)
            || !wl_columnar_memory_size_mul(ledger_before, sizeof(int64_t),
            &ledger_before)
            || !wl_columnar_memory_size_mul(owned_columns, new_capacity,
            &ledger_after)
            || !wl_columnar_memory_size_mul(ledger_after, sizeof(int64_t),
            &ledger_after)
            || lane_bytes > SIZE_MAX || table_bytes > SIZE_MAX ||
            timestamp_bytes > SIZE_MAX) {
            rc = EOVERFLOW;
            goto done;
        }
        (void)old_bytes; /* Validated complete old shape, including auxiliaries. */
        dst->memory_budget_denial_pending = false;
        rc = wl_columnar_relation_payload_begin_controlled(dst, final_bytes,
                incoming, &pending, &transaction_active, control);
        if (rc != 0)
            goto done;
        rc = wl_columnar_relation_prepare_growth_controlled(dst, &old_shape,
                new_capacity, lane_bytes, table_bytes, timestamp_bytes, false,
                &prepared_columns, &prepared_timestamps, control);
        if (rc != 0)
            goto done;
        output_columns = prepared_columns;
        output_timestamps = prepared_timestamps;
    }
    for (uint32_t c = 0; c < dst->ncols; c++) {
        rc = wl_columnar_relation_copy_cells_controlled(output_columns[c] +
                base,
                src->columns[c], count,
                (prepared_types ? prepared_types[c]
                : dst->column_types ? dst->column_types[c] : WIRELOG_TYPE_INT64)
                == WIRELOG_TYPE_FLOAT, control);
        if (rc != 0)
            goto done;
    }
    if (src->timestamps && output_timestamps) {
        rc = wl_columnar_relation_copy_timestamps_controlled(output_timestamps +
                base,
                src->timestamps, count, control);
        if (rc != 0)
            goto done;
    }
    if (prepared_schema_ok
        && !wl_columnar_relation_metadata_tokens_valid(dst, &metadata_pending,
        &old_metadata, metadata_bytes, false)) {
        rc = EINVAL;
        goto done;
    }
    if (growth) {
        ledger_timestamps = prepared_timestamps
            ? (uint64_t)new_capacity * sizeof(*prepared_timestamps) : 0;
        rc = wl_columnar_relation_ledger_prepare_exclusive(dst, ledger_before,
                ledger_after, ledger_timestamps, &ledger_prepared, control);
        if (rc != 0)
            goto done;
    }
    /* Admit canonical pointer/schema/row publication work BEFORE the final
     * governor credit commitment. No charge or stop after that commitment. */
    uint64_t publication_work = (growth ? 64u : 0u)
        + (prepared_schema_ok ? 112u : 0u) + (count ? 8u : 0u);
    rc = wl_evaluation_control_charge(control, publication_work);
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_work_tail(control, publication_work);
    if (rc == 0 && prepared_schema_ok) {
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_fail_metadata_commit) {
            wl_columnar_relation_test_fail_metadata_commit = false;
            rc = ENOMEM;
            goto done;
        }
#endif
        if (metadata_pending.bytes) {
            bool committed;
            rc =
                wl_columnar_memory_governor_commit_controlled(&metadata_pending,
                    dst, &committed, control);
            if (rc != 0)
                goto done;
            if (!committed) {
                rc = ENOMEM;
                goto done;
            }
        }
        metadata_committed = metadata_pending.bytes != 0;
        wl_columnar_relation_metadata_checkpoint(control, 5,
            metadata_pending.bytes);
    }
    if (rc == 0 && growth) {
        rc = wl_columnar_relation_publish_payload_controlled(dst, &pending,
                final_bytes, &previous, control);
        if (rc != 0)
            goto done;
        transaction_active = false;
        int64_t **old_columns = dst->columns;
        col_delta_timestamp_t *old_timestamps = dst->timestamps;
        dst->columns = prepared_columns;
        dst->timestamps = prepared_timestamps;
        dst->capacity = new_capacity;
        dst->timestamp_capacity = prepared_timestamps ? new_capacity : 0;
        prepared_columns = NULL;
        prepared_timestamps = NULL;
        wl_columnar_relation_touch_storage(dst);
        /* Retirement is mandatory cleanup; credit covers both images until
         * every old physical buffer has been retired. No result work here. */
        col_columns_free(old_columns, dst->ncols);
        free(old_timestamps);
        col_rel_release_retired_reservation(&previous);
        wl_columnar_relation_retire_payload_credit_known(dst, final_bytes);
        wl_columnar_relation_ledger_apply_exclusive(dst, ledger_timestamps,
            &ledger_prepared);
    }
    if (rc == 0 && !growth)
        rc = wl_evaluation_control_charge(control, 0);
    if (rc == 0 && prepared_schema_ok) {
        struct ArrowSchema old_schema = dst->schema;
        bool old_schema_ok = dst->schema_ok;
        wl_columnar_relation_publish_committed_metadata(dst,
            &metadata_pending, metadata_bytes, &old_metadata);
        metadata_committed = false;
        dst->column_types = prepared_types;
        dst->schema = prepared_schema;
        dst->schema_ok = true;
        prepared_types = NULL;
        prepared_schema_ok = false;
        if (old_schema_ok)
            ArrowSchemaRelease(&old_schema);
        col_rel_release_reservation_or_abort(&old_metadata);
    }
    if (rc == 0 && count != 0) {
        dst->nrows = base + count;
        wl_columnar_relation_touch_view(dst);
    }
done:
    free(prepared_types);
    if (prepared_schema_ok)
        ArrowSchemaRelease(&prepared_schema);
    if (metadata_pending.bytes) {
        if (metadata_committed)
            col_rel_release_reservation_or_abort(&metadata_pending);
        else
            col_rel_reservation_rollback(&metadata_pending);
    }
    if (prepared_columns)
        col_columns_free(prepared_columns, dst->ncols);
    free(prepared_timestamps);
    if (transaction_active)
        col_rel_payload_txn_rollback(dst, &pending);
    /* A genuine cleanup failure has precedence over a control disposition. */
    if (write_held) {
        int cleanup = wl_columnar_source_access_writer_release(&writer);
        if (cleanup != 0 && rc <= 0)
            rc = cleanup;
    }
    if (read_held) {
        int cleanup = wl_columnar_source_access_reader_release(&reader);
        if (cleanup != 0 && rc <= 0)
            rc = cleanup;
    }
    return rc;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_relation_test_bulk_checkpoint)(
    wl_evaluation_control_t *, int, wl_columnar_relation_bulk_append_t *);
#endif
static void
wl_columnar_relation_bulk_checkpoint(wl_evaluation_control_t *control,
    int phase, wl_columnar_relation_bulk_append_t *record)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_bulk_checkpoint)
        wl_columnar_relation_test_bulk_checkpoint(control, phase, record);
#else
    (void)control; (void)phase; (void)record;
#endif
}

/* Dispose physical ownership before corresponding credit. A false retirement
 * preserves the original token, destination pin and active record for retry. */
static int
wl_columnar_relation_bulk_retire_token(wl_columnar_memory_reservation_t *token)
{
    if (!token->bytes) return 0;
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    bool retired = state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        ? wl_columnar_memory_rollback(token)
        : state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        ? wl_columnar_memory_release(token) : false;
    /* Successful release retains scalar byte history in the governor API.
     * Reset only after the actual return, never on a refused retirement. */
    if (retired) wl_columnar_memory_reservation_init(token);
    return retired ? 0 : EINVAL;
}

int
wl_columnar_relation_bulk_append_retire(
    wl_columnar_relation_bulk_append_t *record,
    int rc)
{
    if (!record || !record->ready) return EINVAL;
    if (!record->active) return rc;
    if (record->reset) {
        /* Logical RESET never replaces or retires any payload credit. */
        int cleanup = 0;
        if (record->writer.owner)
            cleanup = wl_columnar_source_access_writer_release(&record->writer);
        rc = wl_columnar_relation_cleanup_result(rc, cleanup);
        if (!record->writer.owner && record->descriptor_writer.owner)
            cleanup =
                wl_columnar_source_access_writer_release(
                &record->descriptor_writer);
        rc = wl_columnar_relation_cleanup_result(rc, cleanup);
        if (!record->writer.owner && !record->descriptor_writer.owner)
            record->active = false;
        return rc;
    }
    if (record->compact) {
        col_columns_free(record->compact_retired.merge_columns, record->ncols);
        record->compact_retired.merge_columns = NULL;
        if (record->published && record->compact_retired.ledger_pending) {
            /* Primary and merge buffers are retired below before credit. */
            col_columns_free(record->old_columns, record->ncols);
            record->old_columns = NULL;
            free(record->old_timestamps); record->old_timestamps = NULL;
            wl_columnar_relation_ledger_apply_exclusive(record->dst,
                record->ledger_timestamps, &record->ledger_prepared);
            record->compact_retired.ledger_pending = false;
        }
    }
    free(record->lengths); record->lengths = NULL;
    free(record->prepared_types); record->prepared_types = NULL;
    if (record->prepared_schema_ok) {
        ArrowSchemaRelease(&record->prepared_schema);
        record->prepared_schema_ok = false;
    }
    if (record->prepared_columns) {
        col_columns_free(record->prepared_columns, record->ncols);
        record->prepared_columns = NULL;
    }
    free(record->prepared_timestamps); record->prepared_timestamps = NULL;
    if (record->old_columns) {
        col_columns_free(record->old_columns, record->ncols);
        record->old_columns = NULL;
    }
    free(record->old_timestamps); record->old_timestamps = NULL;
    if (record->old_schema_ok) {
        ArrowSchemaRelease(&record->old_schema);
        record->old_schema_ok = false;
    }
    int cleanup = wl_columnar_relation_bulk_retire_token(&record->scratch);
    rc = wl_columnar_relation_cleanup_result(rc, cleanup);
    cleanup = wl_columnar_relation_bulk_retire_token(&record->metadata_pending);
    rc = wl_columnar_relation_cleanup_result(rc, cleanup);
    if (!record->metadata_pending.bytes) record->metadata_committed = false;
    cleanup = wl_columnar_relation_bulk_retire_token(&record->old_metadata);
    rc = wl_columnar_relation_cleanup_result(rc, cleanup);
    cleanup = wl_columnar_relation_bulk_retire_token(&record->previous);
    rc = wl_columnar_relation_cleanup_result(rc, cleanup);
    if (record->transaction_active) {
        bool retired = record->pending.growth
            ? wl_columnar_memory_rollback_growth(
            &record->dst->retained_reservation)
            : !record->pending.fresh.bytes
            || wl_columnar_memory_rollback(&record->pending.fresh);
        if (retired) {
            record->pending.growth = false;
            record->transaction_active = false;
            if (record->pending.fresh.bytes)
                wl_columnar_memory_reservation_init(&record->pending.fresh);
        } else rc = wl_columnar_relation_cleanup_result(rc, EINVAL);
    }
    if (record->published && record->growth && record->dst->memory_governor
        && record->dst->retained_reserved_bytes > record->final_bytes) {
        bool retired = record->final_bytes
            ? wl_columnar_memory_reservation_downsize(
            &record->dst->retained_reservation, record->final_bytes)
            : wl_columnar_memory_release(&record->dst->retained_reservation);
        if (retired) record->dst->retained_reserved_bytes = record->final_bytes;
        else rc = wl_columnar_relation_cleanup_result(rc, EINVAL);
    }
    /* Credit failure keeps the destination writer through the actual retry. */
    bool credit_pending = record->scratch.bytes ||
        record->metadata_pending.bytes
        || record->old_metadata.bytes || record->previous.bytes
        || record->transaction_active || (record->published && record->growth
        && record->dst->memory_governor
        && record->dst->retained_reserved_bytes != record->final_bytes);
    if (!credit_pending) {
        if (record->writer.owner) {
            cleanup = wl_columnar_source_access_writer_release(&record->writer);
            rc = wl_columnar_relation_cleanup_result(rc, cleanup);
        }
        if (!record->writer.owner && record->descriptor_writer.owner) {
            if (record->journal && !record->published) {
                cleanup = wl_columnar_source_access_writer_validate(
                    &record->descriptor_writer,
                    &record->dst->descriptor_access);
                if (!cleanup) {
                    record->dst->storage_owner = NULL;
                    record->dst->storage_owner_identity =
                        record->prior_identity;
                    record->dst->storage_owner_generation =
                        record->prior_generation;
                    record->journal = false;
                }
                rc = wl_columnar_relation_cleanup_result(rc, cleanup);
                if (cleanup) return rc;
            }
            cleanup = wl_columnar_source_access_writer_release(
                &record->descriptor_writer);
            rc = wl_columnar_relation_cleanup_result(rc, cleanup);
        }
    }
    rc = wl_columnar_relation_source_group_drain(&record->sources, rc);
    if (!credit_pending && !record->writer.owner
        && !record->descriptor_writer.owner
        && !record->sources.descriptor_count && !record->sources.owner_count)
        record->active = false;
    return rc;
}

/* Empty private REDUCE schema preparation uses the original bulk record.
 * Names are pinned by its descriptor writer and rescanned in bounded pieces;
 * no governed length cache exists beside the raw metadata admission. */
int
wl_columnar_relation_private_types_controlled(
    wl_columnar_relation_bulk_append_t *record, col_rel_t *dst,
    const wirelog_column_type_t *types, wl_evaluation_control_t *control)
{
    if (!record || !record->ready || !dst || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 80);
    if (rc) return rc;
    if (record->active) return EBUSY;
    if (!col_rel_mutation_writer_inert(&record->writer)
        || !col_rel_mutation_writer_inert(&record->descriptor_writer)
        || record->sources.descriptor_count || record->sources.owner_count
        || record->sources.committed || record->lengths ||
        record->prepared_types
        || record->prepared_columns || record->prepared_timestamps
        || record->old_columns || record->old_timestamps
        || record->old_schema_ok || record->prepared_schema_ok
        || record->transaction_active || record->scratch.bytes
        || record->metadata_pending.bytes || record->old_metadata.bytes
        || record->previous.bytes || record->pending.fresh.bytes
        || record->pending.growth) return EINVAL;
    rc = wl_columnar_relation_work_tail(control, 80);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    record->active = true;
    record->dst = dst;
    record->compact = false; record->reset = false;
    record->published = false; record->journal = false;
    record->growth = false; record->transaction_active = false;
    record->metadata_committed = false;
    record->prepared_schema_ok = false; record->old_schema_ok = false;
    record->ncols = 0; record->owned_columns = 0;
    record->metadata_bytes = 0; record->final_bytes = 0;
    record->ledger_timestamps = 0; record->initialized_tokens = 0;
    record->pending.growth = false; record->pending.incoming_bytes = 0;
    rc = wl_columnar_relation_work_tail(control, 32);
    if (rc) goto done;
    wl_columnar_memory_reservation_t *tokens[] = {&record->scratch,
                                                  &record->metadata_pending,
                                                  &record->old_metadata,
                                                  &record->previous,
                                                  &record->pending.fresh};
    for (unsigned i = 0; i < 5; i++) {
        rc = wl_evaluation_control_charge(control, 128);
        if (rc) goto done;
        wl_columnar_memory_reservation_init(tokens[i]);
        record->initialized_tokens++;
        wl_columnar_relation_bulk_checkpoint(control, 6, record);
        rc = wl_columnar_relation_work_tail(control, 128);
        if (rc) goto done;
    }
    rc = wl_columnar_relation_writer_acquire_controlled(&dst->descriptor_access,
            &record->descriptor_writer, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) goto done;
    record->journal = dst->storage_owner == NULL;
    record->prior_identity = dst->storage_owner_identity;
    record->prior_generation = dst->storage_owner_generation;
    col_rel_t *owner = NULL;
    rc = wl_columnar_relation_resolve_controlled(dst, &owner, control);
    if (rc) goto done;
    if (owner != dst) {
        rc = ENOTSUP; goto done;
    }
    rc = wl_columnar_relation_writer_acquire_controlled(&dst->source_access,
            &record->writer, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) goto done;
    if (dst->nrows || dst->column_types || (dst->ncols && !types)
        || dst->nrows > dst->capacity) {
        rc = EINVAL; goto done;
    }
    if (dst->arena_owned || dst->col_shared || dst->timestamps
        || dst->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || dst->compound_count || dst->inline_physical_offset
        || dst->compound_arity_map || dst->compound_arity_len
        || col_rel_storage_alias_borrow_count(dst)) {
        rc = ENOTSUP; goto done;
    }
    if (dst->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u) {
        rc = EOVERFLOW; goto done;
    }
    record->ncols = dst->ncols;
    rc = wl_columnar_relation_work_tail(control, 32);
    if (rc) goto done;
    rc = wl_columnar_relation_prepare_types_record(record, dst, types, false,
            control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 112);
    if (rc) goto done;
    if (!wl_columnar_relation_metadata_tokens_valid(dst,
        &record->metadata_pending, &record->old_metadata,
        record->metadata_bytes, false)) {
        rc = ENOMEM; goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 112);
    if (rc) goto done;
    if (dst->memory_governor && record->metadata_bytes) {
        bool accepted = false;
        rc = wl_columnar_memory_governor_commit_controlled(
            &record->metadata_pending, dst, &accepted, control);
        record->metadata_committed = accepted;
        if (rc) goto done;
        if (!accepted) {
            rc = ENOMEM; goto done;
        }
    }
    wl_columnar_relation_metadata_checkpoint(control, 5,
        record->metadata_bytes);
    rc = wl_evaluation_control_charge(control, 112);
    if (rc) goto done;
    rc = wl_columnar_source_access_writer_validate(&record->descriptor_writer,
            &dst->descriptor_access);
    if (!rc) rc = wl_columnar_source_access_writer_validate(&record->writer,
                &dst->source_access);
    if (rc) goto done;
    if (!wl_columnar_relation_metadata_tokens_valid(dst,
        &record->metadata_pending, &record->old_metadata,
        record->metadata_bytes, true)) {
        rc = EINVAL; goto done;
    }
    /* Tokens are exclusively held: the earlier check and accepted commit
     * establish the existing fixed metadata transport contract. */
    rc = wl_columnar_relation_work_tail(control, 112);
    if (rc) goto done;
    record->old_schema = dst->schema;
    record->old_schema_ok = dst->schema_ok;
    dst->column_types = record->prepared_types;
    dst->schema = record->prepared_schema;
    dst->schema_ok = true;
    record->prepared_types = NULL;
    record->prepared_schema_ok = false;
    wl_columnar_relation_publish_committed_metadata(dst,
        &record->metadata_pending, record->metadata_bytes,
        &record->old_metadata);
    record->metadata_committed = false;
    record->published = true;
    wl_columnar_relation_touch_view(dst);
    rc = wl_evaluation_control_charge(control, 0);
done:
    return wl_columnar_relation_bulk_append_retire(record, rc);
}

int
wl_columnar_relation_bulk_append_controlled(
    wl_columnar_relation_bulk_append_t *record,
    col_rel_t *dst, const col_rel_t *src, wl_evaluation_control_t *control)
{
    if (!record || !record->ready || !control || !dst || !src) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 80);
    if (rc) return rc;
    if (record->active) return EBUSY; /* consumed records only retire */
    if (!col_rel_mutation_writer_inert(&record->writer)
        || !col_rel_mutation_writer_inert(&record->descriptor_writer)
        || record->sources.descriptor_count || record->sources.owner_count
        || record->sources.committed || record->lengths ||
        record->prepared_types
        || record->prepared_columns || record->prepared_timestamps
        || record->old_columns || record->old_timestamps ||
        record->old_schema_ok
        || record->prepared_schema_ok || record->transaction_active
        || record->scratch.bytes || record->metadata_pending.bytes
        || record->old_metadata.bytes || record->previous.bytes
        || record->pending.fresh.bytes || record->pending.growth) return EINVAL;
    rc = wl_columnar_relation_work_tail(control, 80);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    /* No live token or physical owner survives a retired attempt. Install the
     * active phase before bounded reset so a stop has a durable disposition. */
    record->active = true;
    record->compact = false;
    record->reset = false;
    record->published = false;
    record->dst = dst;
    record->journal = false;
    record->growth = false;
    record->transaction_active = false;
    record->metadata_committed = false;
    record->prepared_schema_ok = false;
    record->old_schema_ok = false;
    record->ncols = 0;
    record->owned_columns = 0;
    record->metadata_bytes = 0;
    record->final_bytes = 0;
    record->ledger_timestamps = 0;
    record->initialized_tokens = 0;
    record->pending.growth = false;
    record->pending.incoming_bytes = 0;
    rc = wl_columnar_relation_work_tail(control, 32);
    if (rc) goto done;
    /* Each init includes a physical memset of the reservation. Five such
     * tokens exceed one quantum; admit/poll each original slot separately. */
    wl_columnar_memory_reservation_t *tokens[5] = {&record->scratch,
                                                   &record->metadata_pending,
                                                   &record->old_metadata,
                                                   &record->previous,
                                                   &record->pending.fresh};
    for (unsigned i = 0; i < 5; i++) {
        rc = wl_evaluation_control_charge(control, 128);
        if (rc) goto done;
        wl_columnar_memory_reservation_init(tokens[i]);
        record->initialized_tokens++;
        wl_columnar_relation_bulk_checkpoint(control, 6, record);
        rc = wl_columnar_relation_work_tail(control, 128);
        if (rc) goto done;
    }
    wl_columnar_relation_bulk_checkpoint(control, 3, record);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) goto done;
    if (src == dst) {
        rc = ENOTSUP; goto done;
    }
    rc = wl_columnar_relation_source_group_acquire_controlled(&record->sources,
            (col_rel_t *)src, NULL, control);
    if (rc) goto done;
    rc = wl_columnar_relation_writer_acquire_controlled(&dst->descriptor_access,
            &record->descriptor_writer, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) goto done;
    record->journal = dst->storage_owner == NULL;
    record->prior_identity = dst->storage_owner_identity;
    record->prior_generation = dst->storage_owner_generation;
    col_rel_t *dst_owner = NULL;
    rc = wl_columnar_relation_resolve_controlled(dst, &dst_owner, control);
    if (rc) goto done;
    if (dst_owner != dst || record->sources.owners[0].owner != src) {
        rc = ENOTSUP; goto done;
    }
    rc = wl_columnar_relation_writer_acquire_controlled(&dst->source_access,
            &record->writer, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) goto done;
    record->ncols = dst->ncols;
    if (dst->ncols != src->ncols) {
        rc = EINVAL; goto done;
    }
    if (dst->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || src->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || dst->compound_count || src->compound_count
        || dst->inline_physical_offset || src->inline_physical_offset
        || dst->compound_arity_map || src->compound_arity_map
        || dst->compound_arity_len || src->compound_arity_len
        || dst->col_shared || src->col_shared || dst->arena_owned ||
        src->arena_owned) {
        rc = ENOTSUP; goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 32);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 64);
    if (rc) goto done;
    bool install_types = false;
    int64_t **output_columns;
    col_delta_timestamp_t *output_timestamps;
    uint64_t ledger_before = 0, ledger_after = 0;
    if (col_rel_storage_alias_borrow_count(dst) != 0) {
        rc = ENOTSUP;
        goto done;
    }
    if (src->nrows > UINT32_MAX - dst->nrows) {
        rc = EOVERFLOW;
        goto done;
    }
    if (!col_rel_timestamp_shape_valid(dst)
        || !col_rel_timestamp_shape_valid(src)
        || src->nrows > src->capacity || dst->nrows > dst->capacity) {
        rc = EINVAL;
        goto done;
    }
    record->base = dst->nrows;
    record->count = src->nrows;
    install_types = !dst->column_types && src->column_types &&
        record->count != 0;
    /* Real empty compaction retires the old grid. Existing growth constructs
     * every new lane before copying; zero old rows never read that grid. These
     * four fixed predicates remain inside the admitted 64-unit shape header. */
    bool empty_grid = !dst->columns && !dst->capacity && !dst->nrows;
    if (!wl_columnar_relation_generation_valid(dst->view_generation)
        || dst->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || (src->ncols && (!src->columns || (!dst->columns && !empty_grid)))) {
        rc = EINVAL;
        goto done;
    }
    record->growth = record->base + record->count > dst->capacity
        || (dst->timestamps &&
        record->base + record->count > dst->timestamp_capacity);
    if (record->growth &&
        (!wl_columnar_relation_generation_valid(dst->storage_generation)
        || dst->storage_generation >=
        WL_COLUMNAR_REL_GENERATION_INVALID - 1u)) {
        rc = EOVERFLOW;
        goto done;
    }
    for (uint32_t c = 0; c < dst->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        wirelog_column_type_t dt = dst->column_types
            ? dst->column_types[c] : WIRELOG_TYPE_INT64;
        wirelog_column_type_t st = src->column_types
            ? src->column_types[c] : WIRELOG_TYPE_INT64;
        if (!src->columns[c] || (dst->columns && !dst->columns[c])
            || ((dt == WIRELOG_TYPE_FLOAT || st == WIRELOG_TYPE_FLOAT)
            && dt != st && record->count != 0)) {
            rc = EINVAL;
            goto done;
        }
        record->owned_columns++;
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    /* Empty inputs still poll, but need no second uncharged width walk. */
    if (record->count == 0) {
        rc = wl_evaluation_control_charge(control, 0);
        goto done;
    }
    /* Legacy typed validation visits every scalar, even integer lanes. */
    if (src->column_types) {
        for (uint32_t row = 0; row < record->count; row++) {
            for (uint32_t c = 0; c < src->ncols; c++) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                if (src->column_types[c] == WIRELOG_TYPE_FLOAT
                    && !wl_columnar_float_bits_valid(src->columns[c][row])) {
                    rc = EINVAL;
                    goto done;
                }
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
            }
        }
    }
    if (install_types && record->base != 0) {
        for (uint32_t row = 0; row < record->base; row++) {
            for (uint32_t c = 0; c < dst->ncols; c++) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                if (src->column_types[c] == WIRELOG_TYPE_FLOAT
                    && !wl_columnar_float_bits_valid(dst->columns[c][row])) {
                    rc = EINVAL;
                    goto done;
                }
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
            }
        }
    }
    if (install_types) {
        dst->memory_budget_denial_pending = false;
        rc = wl_columnar_relation_bulk_prepare_types(record, dst,
                src->column_types, control);
        if (rc != 0)
            goto done;
        record->prepared_schema_ok = true;
    }
    output_columns = dst->columns;
    output_timestamps = dst->timestamps;
    if (record->growth) {
        col_rel_payload_shape_t old_shape = {
            .ncols = dst->ncols, .owned_columns = record->owned_columns,
            .column_capacity = dst->capacity,
            .timestamp_capacity = dst->timestamp_capacity,
            .merge_capacity = dst->merge_buf_cap,
            .backup_capacity = dst->retract_backup_capacity,
            .backup_timestamp_capacity = dst->retract_backup_timestamp_capacity,
            .active_table = dst->columns != NULL,
            .merge_table = dst->merge_columns != NULL,
            .backup_table = dst->retract_backup_columns != NULL,
            .scratch = dst->row_scratch != NULL,
        };
        col_rel_payload_shape_t final_shape = old_shape;
        uint64_t incoming, old_bytes, lane_bytes, table_bytes, timestamp_bytes;
        if ((dst->columns && dst->columns == dst->merge_columns)
            || (dst->columns && dst->columns == dst->retract_backup_columns)
            || (dst->merge_columns &&
            dst->merge_columns == dst->retract_backup_columns)
            || (!dst->ncols &&
            (dst->merge_columns || dst->retract_backup_columns))) {
            rc = EINVAL;
            goto done;
        }
        record->new_capacity = dst->capacity;
        if (record->new_capacity < record->base + record->count) {
            record->new_capacity =
                dst->capacity ? dst->capacity : COL_REL_INIT_CAP;
            while (record->new_capacity < record->base + record->count) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                if (record->new_capacity > UINT32_MAX / 2u) {
                    rc = EOVERFLOW;
                    goto done;
                }
                record->new_capacity *= 2u;
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
            }
        }
        final_shape.column_capacity = record->new_capacity;
        final_shape.timestamp_capacity =
            dst->timestamps ? record->new_capacity : 0;
        final_shape.active_table = dst->ncols != 0;
        if (!col_rel_payload_shape_bytes(&old_shape, &old_bytes)
            || !col_rel_payload_shape_bytes(&final_shape, &record->final_bytes)
            || !col_rel_payload_grid_bytes(dst->ncols, record->owned_columns,
            record->new_capacity, dst->ncols != 0, &incoming)
            || !wl_columnar_memory_size_mul(record->new_capacity ?
            record->new_capacity : 1u,
            sizeof(int64_t), &lane_bytes)
            || !wl_columnar_memory_size_mul(dst->ncols, sizeof(int64_t *),
            &table_bytes)
            || !wl_columnar_memory_size_mul(final_shape.timestamp_capacity,
            sizeof(col_delta_timestamp_t), &timestamp_bytes)
            || !wl_columnar_memory_size_add(incoming, timestamp_bytes,
            &incoming)
            || !wl_columnar_memory_size_mul(record->owned_columns,
            dst->capacity,
            &ledger_before)
            || !wl_columnar_memory_size_mul(ledger_before, sizeof(int64_t),
            &ledger_before)
            || !wl_columnar_memory_size_mul(record->owned_columns,
            record->new_capacity,
            &ledger_after)
            || !wl_columnar_memory_size_mul(ledger_after, sizeof(int64_t),
            &ledger_after)
            || lane_bytes > SIZE_MAX || table_bytes > SIZE_MAX ||
            timestamp_bytes > SIZE_MAX) {
            rc = EOVERFLOW;
            goto done;
        }
        (void)old_bytes; /* Validated complete old shape, including auxiliaries. */
        dst->memory_budget_denial_pending = false;
        /* The shared helper performs a no-live transaction reset before its
         * bounded governor operation. Admit the actual reset bytes here. */
        rc = wl_evaluation_control_charge(control,
                sizeof(record->pending) + sizeof(record->pending.fresh) + 16u);
        if (rc) goto done;
        rc = wl_columnar_relation_payload_begin_controlled(dst,
                record->final_bytes,
                incoming, &record->pending, &record->transaction_active,
                control);
        if (rc != 0)
            goto done;
        wl_columnar_relation_bulk_checkpoint(control, 4, record);
        rc = wl_columnar_relation_prepare_growth_controlled(dst, &old_shape,
                record->new_capacity, lane_bytes, table_bytes, timestamp_bytes,
                false,
                &record->prepared_columns, &record->prepared_timestamps,
                control);
        if (rc != 0)
            goto done;
        output_columns = record->prepared_columns;
        output_timestamps = record->prepared_timestamps;
    }
    for (uint32_t c = 0; c < dst->ncols; c++) {
        rc = wl_columnar_relation_copy_cells_controlled(output_columns[c] +
                record->base,
                src->columns[c], record->count,
                (record->prepared_types ? record->prepared_types[c]
                : dst->column_types ? dst->column_types[c] : WIRELOG_TYPE_INT64)
                == WIRELOG_TYPE_FLOAT, control);
        if (rc != 0)
            goto done;
    }
    if (src->timestamps && output_timestamps) {
        rc = wl_columnar_relation_copy_timestamps_controlled(output_timestamps +
                record->base,
                src->timestamps, record->count, control);
        if (rc != 0)
            goto done;
    }
    if (record->prepared_schema_ok
        && !wl_columnar_relation_metadata_tokens_valid(dst,
        &record->metadata_pending,
        &record->old_metadata, record->metadata_bytes, false)) {
        rc = EINVAL;
        goto done;
    }
    if (record->growth) {
        record->ledger_timestamps = record->prepared_timestamps
            ? (uint64_t)record->new_capacity *
            sizeof(*record->prepared_timestamps) : 0;
        rc = wl_columnar_relation_ledger_prepare_exclusive(dst, ledger_before,
                ledger_after, record->ledger_timestamps,
                &record->ledger_prepared, control);
        if (rc != 0)
            goto done;
    }

    /* Move destinations and source credit states are checked BEFORE the last
     * controlled commitment. There is no automatic previous credit token. */
    rc = wl_evaluation_control_charge(control, 24);
    if (rc) goto done;
    if (dst->memory_governor && record->growth) {
        uint64_t state = atomic_load_explicit(&dst->retained_reservation.state,
                memory_order_acquire);
        if (dst->retained_reservation.identity != &dst->retained_reservation
            || record->previous.identity != &record->previous
            || record->previous.bytes
            || (record->pending.growth
                ? state != WL_COLUMNAR_MEMORY_RESERVATION_REPLACING
            || record->pending.incoming_bytes >
            UINT64_MAX - dst->retained_reserved_bytes
                : record->pending.fresh.bytes &&
            (record->pending.fresh.identity != &record->pending.fresh
            || atomic_load_explicit(&record->pending.fresh.state,
            memory_order_acquire) !=
            WL_COLUMNAR_MEMORY_RESERVATION_RESERVED))) {
            rc = EINVAL; goto done;
        }
    }
    rc = wl_columnar_relation_work_tail(control, 24);
    if (rc) goto done;
    uint64_t publication_work = (record->growth ? 64u : 0u)
        + (record->prepared_schema_ok ? 112u : 0u) + 8u;
    rc = wl_evaluation_control_charge(control, publication_work);
    if (rc) goto done;
    rc = wl_columnar_relation_work_tail(control, publication_work);
    if (rc) goto done;
    if (record->prepared_schema_ok && record->metadata_pending.bytes) {
        bool committed = false;
        rc =
            wl_columnar_memory_governor_commit_controlled(
            &record->metadata_pending,
            dst, &committed, control);
        record->metadata_committed = committed;
        if (rc || !committed) {
            if (!rc) {
                rc = ENOMEM;
            }
            goto done;
        }
        /* Reversible metadata credit only: canonical schema/token unchanged.
         * For no-growth this is terminal; a late request cannot split install. */
        if (record->growth) {
            wl_columnar_relation_bulk_checkpoint(control, 1, record);
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) goto done;
            rc = wl_evaluation_control_charge(control, 16);
            if (rc) goto done;
            if (record->metadata_pending.identity != &record->metadata_pending
                || record->metadata_pending.bytes != record->metadata_bytes
                || record->metadata_pending.governor !=
                wl_columnar_memory_governor_ref_get(dst->memory_governor)
                || atomic_load_explicit(&record->metadata_pending.state,
                memory_order_acquire) !=
                WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                || atomic_load_explicit(&record->metadata_pending.owner_bits,
                memory_order_acquire) != (uint64_t)(uintptr_t)dst) {
                rc = EINVAL; goto done;
            }
            rc = wl_columnar_relation_work_tail(control, 16);
            if (rc) goto done;
        }
    }
    if (record->growth && dst->memory_governor) {
        bool committed = false;
        if (record->pending.growth) {
            rc = wl_columnar_memory_governor_commit_growth_controlled(
                &dst->retained_reservation, &committed, control);
            if (rc || !committed) {
                if (!rc) {
                    rc = ENOMEM;
                }
                goto done;
            }
            dst->retained_reserved_bytes += record->pending.incoming_bytes;
            record->pending.growth = false;
        } else if (record->pending.fresh.bytes) {
            rc =
                wl_columnar_memory_governor_commit_controlled(
                &record->pending.fresh,
                dst, &committed, control);
            if (rc || !committed) {
                if (!rc) {
                    rc = ENOMEM;
                }
                goto done;
            }
            if (dst->retained_reserved_bytes &&
                !wl_columnar_memory_reservation_move(&record->previous,
                &dst->retained_reservation)) abort();
            if (!wl_columnar_memory_reservation_move(&dst->retained_reservation,
                &record->pending.fresh)) abort();
            dst->retained_reserved_bytes = record->final_bytes;
        }
        record->transaction_active = false;
    }
    /* First irreversible canonical event. All fixed stores were admitted;
     * no control calls or fallible preparation inside this installation. */
    record->published = true;
    if (record->growth) {
        record->old_columns = dst->columns;
        record->old_timestamps = dst->timestamps;
        dst->columns = record->prepared_columns;
        dst->timestamps = record->prepared_timestamps;
        dst->capacity = record->new_capacity;
        dst->timestamp_capacity =
            record->prepared_timestamps ? record->new_capacity : 0;
        record->prepared_columns = NULL;
        record->prepared_timestamps = NULL;
        wl_columnar_relation_touch_storage(dst);
        wl_columnar_relation_ledger_apply_exclusive(dst,
            record->ledger_timestamps,
            &record->ledger_prepared);
    }
    if (record->prepared_schema_ok) {
        record->old_schema = dst->schema;
        record->old_schema_ok = dst->schema_ok;
        wl_columnar_relation_publish_committed_metadata(dst,
            &record->metadata_pending, record->metadata_bytes,
            &record->old_metadata);
        record->metadata_committed = false;
        dst->column_types = record->prepared_types;
        dst->schema = record->prepared_schema;
        dst->schema_ok = true;
        record->prepared_types = NULL;
        record->prepared_schema_ok = false;
    }
    dst->nrows = record->base + record->count;
    wl_columnar_relation_touch_view(dst);
done:
    if (rc && !record->published)
        wl_columnar_relation_bulk_checkpoint(control, 5, record);
    rc = wl_columnar_relation_bulk_append_retire(record, rc);
    if (record->published && !record->active)
        wl_columnar_relation_bulk_checkpoint(control, 2, record);
    return rc;
}

int
wl_columnar_relation_append_all_if_empty(col_rel_t *dst,
    const col_rel_t *src)
{
    return col_rel_append_all_impl(dst, src, NULL, true);
}

/* ---- compaction ---------------------------------------------------------- */

static bool
wl_columnar_relation_reset_token_valid(
    const wl_columnar_memory_reservation_t *token,
    uint64_t bytes, const col_rel_t *rel)
{
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (!bytes) return (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
               || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
               && !token->replacement_bytes;
    return state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
           && token->identity == token && token->bytes == bytes
           && token->governor ==
           wl_columnar_memory_governor_ref_get(rel->memory_governor)
           && atomic_load_explicit(&token->owner_bits,
               memory_order_acquire) == (uintptr_t)rel
           && !token->replacement_bytes
           && token->transition_kind == WL_COLUMNAR_MEMORY_TRANSITION_NONE;
}

int
wl_columnar_relation_reset_controlled(
    wl_columnar_relation_bulk_append_t *record,
    col_rel_t *rel, bool publish, wl_evaluation_control_t *control)
{
    if (!record || !record->ready || !rel || !control) return EINVAL;
    if (record->active) return EBUSY;
    int rc = wl_evaluation_control_charge(control, 96);
    if (rc) return rc;
    if (record->writer.owner || record->descriptor_writer.owner
        || record->sources.descriptor_count || record->sources.owner_count
        || record->transaction_active || record->pending.growth
        || record->pending.fresh.bytes || record->scratch.bytes
        || record->metadata_pending.bytes || record->old_metadata.bytes
        || record->previous.bytes || record->prepared_columns ||
        record->old_columns
        || record->prepared_timestamps || record->old_timestamps
        || record->prepared_schema_ok || record->old_schema_ok) return EINVAL;
    record->active = true; record->reset = true;
    record->compact = false; record->growth = false; record->published = false;
    record->dst = rel;
    rc = wl_columnar_relation_writer_acquire_controlled(&rel->descriptor_access,
            &record->descriptor_writer, control);
    if (rc) goto done;
    rc = wl_columnar_relation_writer_acquire_controlled(&rel->source_access,
            &record->writer, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 192);
    if (rc) goto done;
    if (rel->storage_owner != rel || !rel->relation_identity
        || rel->storage_owner_identity != rel->relation_identity
        || rel->storage_owner_generation != rel->storage_generation
        || rel->arena_owned || rel->pool_owned || rel->col_shared
        || col_rel_storage_alias_borrow_count(rel)
        || rel->compound_kind != WIRELOG_COMPOUND_KIND_NONE ||
        rel->compound_count
        || rel->compound_arity_map || rel->compound_arity_len ||
        rel->inline_physical_offset
        || rel->timestamps || rel->timestamp_capacity || rel->ledger_ts_bytes
        || rel->dedup_slots || rel->dedup_cap || rel->dedup_count ||
        rel->dedup_reserved_bytes
        || rel->retract_backup_columns || rel->retract_backup_timestamps
        || rel->retract_backup_capacity ||
        rel->retract_backup_timestamp_capacity
        || rel->retract_backup_nrows || rel->retract_backup_base_nrows
        || rel->retract_backup_sorted_nrows || rel->retract_backup_run_count
        || rel->pool_name_reserved_bytes || !rel->memory_governor) {
        rc = ENOTSUP; goto done;
    }
    if (!wl_columnar_relation_generation_valid(rel->view_generation)
        || !wl_columnar_relation_generation_valid(rel->storage_generation)
        || rel->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || rel->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u) {
        rc = EOVERFLOW; goto done;
    }
    if (rel->nrows > rel->capacity || rel->sorted_nrows > rel->nrows
        || rel->base_nrows > rel->nrows || rel->run_count > COL_MAX_RUNS
        || (rel->ncols && rel->capacity && !rel->columns)
        || (rel->ncols && !rel->row_scratch)
        || (!!rel->merge_columns != !!rel->merge_buf_cap)
        || (rel->columns && rel->columns == rel->merge_columns)
        || (rel->schema_ok && !rel->schema.release)
        || (!rel->schema_ok && rel->schema.release)
        || !wl_columnar_relation_reset_token_valid(&rel->retained_reservation,
        rel->retained_reserved_bytes, rel)
        || !wl_columnar_relation_reset_token_valid(&rel->descriptor_reservation,
        rel->descriptor_reserved_bytes, rel)
        || !wl_columnar_relation_reset_token_valid(&rel->metadata_reservation,
        rel->metadata_reserved_bytes, rel)
        || !wl_columnar_relation_reset_token_valid(&rel->dedup_reservation, 0,
        rel)
        || !wl_columnar_relation_reset_token_valid(&rel->pool_name_reservation,
        0, rel)) {
        rc = EINVAL; goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 192);
    if (rc) goto done;
    bool has_float = false;
    for (uint32_t c = 0; c < rel->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) goto done;
        if ((rel->columns && !rel->columns[c])
            || (rel->merge_columns && !rel->merge_columns[c])
            || (rel->col_names && !rel->col_names[c])
            || (rel->column_types &&
            rel->column_types[c] != WIRELOG_TYPE_INT64
            && rel->column_types[c] != WIRELOG_TYPE_FLOAT)) {
            rc = ENOTSUP; goto done;
        }
        if (rel->column_types && rel->column_types[c] == WIRELOG_TYPE_FLOAT)
            has_float = true;
        rc = wl_columnar_relation_work_tail(control, 16);
        if (rc) goto done;
    }
    if (has_float) {
        rc = wl_evaluation_control_charge(control, 48);
        if (rc) goto done;
        if (!rel->schema_ok || !rel->schema.release || !rel->schema.format
            || rel->schema.format[0] != '+' || rel->schema.format[1] != 's'
            || rel->schema.format[2] || rel->schema.n_children < 0
            || (uint64_t)rel->schema.n_children != rel->ncols
            || (rel->ncols && !rel->schema.children)) {
            rc = EINVAL; goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 48);
        if (rc) goto done;
        for (uint32_t c = 0; c < rel->ncols; c++) {
            rc = wl_evaluation_control_charge(control, 48);
            if (rc) goto done;
            const struct ArrowSchema *child = rel->schema.children[c];
            char format = rel->column_types[c] ==
                WIRELOG_TYPE_FLOAT ? 'g' : 'l';
            if (!child || !child->release || child->n_children
                || child->dictionary || !child->format
                || child->format[0] != format || child->format[1]) {
                rc = EINVAL; goto done;
            }
            rc = wl_columnar_relation_work_tail(control, 48);
            if (rc) goto done;
        }
    }
    uint32_t previous_end = 0;
    for (uint32_t run = 0; run < COL_MAX_RUNS; run++) {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) goto done;
        if (rel->retract_backup_run_ends[run]) {
            rc = ENOTSUP; goto done;
        }
        if (run < rel->run_count) {
            if (rel->run_ends[run] < previous_end ||
                rel->run_ends[run] > rel->nrows) {
                rc = EINVAL; goto done;
            }
            previous_end = rel->run_ends[run];
        }
        rc = wl_columnar_relation_work_tail(control, 8);
        if (rc) goto done;
    }
    col_rel_payload_shape_t shape = {.ncols = rel->ncols,
                                     .owned_columns =
                                         rel->columns ? rel->ncols : 0,
                                     .column_capacity = rel->capacity,
                                     .merge_capacity = rel->merge_buf_cap,
                                     .active_table = rel->columns != NULL,
                                     .merge_table = rel->merge_columns != NULL,
                                     .scratch = rel->row_scratch != NULL};
    uint64_t bytes;
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) goto done;
    if (!col_rel_payload_shape_bytes(&shape, &bytes)
        || bytes != rel->retained_reserved_bytes) {
        rc = EINVAL; goto done;
    }
    if (publish) wl_columnar_relation_bulk_checkpoint(control, 8, record);
    rc = wl_columnar_relation_work_tail(control, 128);
    if (rc || !publish) goto done;
    rel->nrows = rel->sorted_nrows = rel->base_nrows = rel->run_count = 0;
    memset(rel->run_ends, 0, sizeof(rel->run_ends));
    wl_columnar_relation_touch_replacement(rel);
    record->published = true;
    /* Caller advances the consumed reset cursor before any retirement. */
    wl_columnar_relation_bulk_checkpoint(control, 9, record);
    return 0;
done:
    return wl_columnar_relation_bulk_append_retire(record, rc);
}

int
wl_columnar_relation_compact_controlled(
    wl_columnar_relation_bulk_append_t *record, col_rel_t *dst,
    wl_evaluation_control_t *control)
{
    if (!record || !record->ready || !dst || !control) return EINVAL;
    if (record->active) return EBUSY;
    int rc = wl_evaluation_control_charge(control, 96);
    if (rc) return rc;
    if (record->writer.owner || record->descriptor_writer.owner
        || record->sources.descriptor_count || record->sources.owner_count
        || record->prepared_columns || record->old_columns
        || record->prepared_timestamps || record->old_timestamps
        || record->transaction_active || record->scratch.bytes
        || record->metadata_pending.bytes || record->old_metadata.bytes
        || record->previous.bytes || record->prepared_schema_ok
        || record->old_schema_ok) return EINVAL;
    record->compact = true;
    record->reset = false;
    record->active = true;
    record->published = false;
    record->dst = dst;
    record->journal = false;
    record->growth = false;
    record->ncols = 0;
    record->compact_retired.merge_columns = NULL;
    record->compact_retired.ledger_pending = false;
    record->transaction_active = false;
    record->initialized_tokens = 0;
    record->pending.growth = false;
    rc = wl_columnar_relation_work_tail(control, 96);
    if (rc) goto done;
    wl_columnar_memory_reservation_t *tokens[5] = {
        &record->scratch, &record->metadata_pending, &record->old_metadata,
        &record->previous, &record->pending.fresh
    };
    for (uint32_t i = 0; i < 5; i++) {
        rc = wl_evaluation_control_charge(control, 128);
        if (rc) goto done;
        wl_columnar_memory_reservation_init(tokens[i]);
        record->initialized_tokens++;
        rc = wl_columnar_relation_work_tail(control, 128);
        if (rc) goto done;
    }
    /* Preserve the raw zero-arity no-op without requiring a writer against
     * existing readers. Shape is observed under the original source group. */
    rc = wl_columnar_relation_source_group_acquire_controlled(&record->sources,
            dst, NULL, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) goto done;
    record->ncols = dst->ncols;
    rc = wl_columnar_relation_work_tail(control, 16);
    if (rc || !record->ncols) goto done;
    rc = wl_columnar_relation_source_group_drain(&record->sources, 0);
    if (rc) goto done;
    rc = wl_columnar_relation_writer_acquire_controlled(&dst->descriptor_access,
            &record->descriptor_writer, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) goto done;
    if (dst->storage_owner != dst || dst->arena_owned || dst->pool_owned
        || dst->col_shared || dst->compound_count
        || dst->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || dst->inline_physical_offset || dst->retract_backup_columns
        || col_rel_storage_alias_borrow_count(dst)) {
        rc = ENOTSUP; goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 32);
    if (rc) goto done;
    rc = wl_columnar_relation_writer_acquire_controlled(&dst->source_access,
            &record->writer, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 64);
    if (rc) goto done;
    record->ncols = dst->ncols;
    if (!dst->ncols) goto done; /* Raw zero-arity compaction is a no-op. */
    if (!col_rel_timestamp_shape_valid(dst) || dst->nrows > dst->capacity
        || (dst->nrows && !dst->columns)
        || (dst->columns && dst->columns == dst->merge_columns)) {
        rc = EINVAL; goto done;
    }
    bool shrink = dst->nrows && dst->capacity > (uint64_t)dst->nrows * 4u;
    bool empty = !dst->nrows;
    record->new_capacity = empty ? 0 : dst->capacity;
    if (shrink) {
        if (dst->memory_governor && !dst->retained_reserved_bytes) {
            rc = EBUSY; goto done;
        }
        record->new_capacity = dst->nrows > UINT32_MAX / 2u
            ? UINT32_MAX : dst->nrows * 2u;
        if (record->new_capacity < COL_REL_INIT_CAP)
            record->new_capacity = COL_REL_INIT_CAP;
    }
    if ((shrink || (empty && (dst->columns || dst->timestamps)))
        && (!wl_columnar_relation_generation_valid(dst->storage_generation)
        || dst->storage_generation >=
        WL_COLUMNAR_REL_GENERATION_INVALID - 1u)){
        rc = EOVERFLOW; goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 64);
    if (rc) goto done;
    for (uint32_t c = 0; c < dst->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) goto done;
        if (dst->nrows && !dst->columns[c]) {
            rc = EINVAL; goto done;
        }
        if (dst->merge_columns && !dst->merge_columns[c]) {
            rc = EINVAL; goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 8);
        if (rc) goto done;
    }
    col_rel_payload_shape_t old_shape = {
        .ncols = dst->ncols, .owned_columns = dst->columns ? dst->ncols : 0,
        .column_capacity = dst->capacity,
        .timestamp_capacity = dst->timestamp_capacity,
        .merge_capacity = dst->merge_buf_cap,
        .active_table = dst->columns != NULL,
        .merge_table = dst->merge_columns != NULL,
        .scratch = dst->row_scratch != NULL,
    };
    col_rel_payload_shape_t final_shape = old_shape;
    final_shape.column_capacity = record->new_capacity;
    final_shape.timestamp_capacity = !empty && dst->timestamps
        ? record->new_capacity : 0;
    final_shape.active_table = !empty && dst->columns;
    final_shape.owned_columns = final_shape.active_table ? dst->ncols : 0;
    final_shape.merge_capacity = 0;
    final_shape.merge_table = false;
    uint64_t lane_bytes, table_bytes, timestamp_bytes, incoming, before, after;
    if (!col_rel_payload_shape_bytes(&final_shape, &record->final_bytes)
        || !wl_columnar_memory_size_mul(record->new_capacity, sizeof(int64_t),
        &lane_bytes)
        || !wl_columnar_memory_size_mul(dst->ncols, sizeof(int64_t *),
        &table_bytes)
        || !wl_columnar_memory_size_mul(final_shape.timestamp_capacity,
        sizeof(col_delta_timestamp_t), &timestamp_bytes)
        || !col_rel_payload_grid_bytes(dst->ncols, dst->ncols,
        record->new_capacity, true, &incoming)
        || !wl_columnar_memory_size_add(incoming, timestamp_bytes, &incoming)
        || !wl_columnar_memory_size_mul((uint64_t)dst->capacity
        + dst->merge_buf_cap, (uint64_t)dst->ncols * sizeof(int64_t), &before)
        || !wl_columnar_memory_size_mul(record->new_capacity,
        (uint64_t)dst->ncols * sizeof(int64_t), &after)
        || lane_bytes > SIZE_MAX || table_bytes > SIZE_MAX
        || timestamp_bytes > SIZE_MAX) {
        rc = EOVERFLOW; goto done;
    }
    if (shrink) {
        rc = wl_evaluation_control_charge(control, 128);
        if (rc) goto done;
        if (dst->memory_governor) {
            wl_columnar_memory_admission_status_t status;
            rc = wl_columnar_memory_governor_begin_growth_controlled(
                &dst->retained_reservation, incoming, &status, control);
            if (rc) goto done;
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
                /* Optional overlap was never admitted. Match raw batch
                 * compaction: keep the entire existing payload, including
                 * merge buffers, and do not latch a fatal budget denial. */
                wl_columnar_relation_bulk_checkpoint(control, 7, record);
                rc = wl_columnar_relation_work_tail(control, 0);
                goto done;
            }
            if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                rc = status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                    ? EOVERFLOW : EINVAL;
                goto done;
            }
            record->pending.growth = true;
            record->pending.incoming_bytes = incoming;
            record->transaction_active = true;
        }
        rc = wl_columnar_relation_prepare_growth_controlled(dst, &old_shape,
                record->new_capacity, lane_bytes, table_bytes, timestamp_bytes,
                false, &record->prepared_columns, &record->prepared_timestamps,
                control);
        if (rc == ENOMEM) {
            /* Raw compaction treats an allocator refusal as best effort. */
            col_columns_free(record->prepared_columns, dst->ncols);
            record->prepared_columns = NULL;
            free(record->prepared_timestamps);
            record->prepared_timestamps = NULL;
            if (record->transaction_active) {
                if (!record->pending.growth ||
                    !wl_columnar_memory_rollback_growth(
                        &dst->retained_reservation)) {
                    rc = EINVAL; goto done;
                }
                record->pending.growth = false;
                record->transaction_active = false;
            }
            shrink = false;
            record->new_capacity = dst->capacity;
            final_shape.column_capacity = dst->capacity;
            final_shape.timestamp_capacity = dst->timestamp_capacity;
            if (!col_rel_payload_shape_bytes(&final_shape, &record->final_bytes)
                || !wl_columnar_memory_size_mul(dst->timestamp_capacity,
                sizeof(col_delta_timestamp_t), &timestamp_bytes)
                || !wl_columnar_memory_size_mul(dst->capacity,
                (uint64_t)dst->ncols * sizeof(int64_t), &after)){
                rc = EOVERFLOW; goto done;
            }
            rc = 0;
        }
        if (rc) goto done;
    }
    record->ledger_timestamps = timestamp_bytes;
    rc = wl_columnar_relation_ledger_prepare_exclusive(dst, before, after,
            timestamp_bytes, &record->ledger_prepared, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) goto done;
    if (record->pending.growth && (dst->retained_reservation.identity
        != &dst->retained_reservation
        || atomic_load_explicit(&dst->retained_reservation.state,
        memory_order_acquire) != WL_COLUMNAR_MEMORY_RESERVATION_REPLACING
        || incoming > UINT64_MAX - dst->retained_reserved_bytes)) {
        rc = EINVAL; goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 128);
    if (rc) goto done;
    if (record->pending.growth) {
        bool committed = false;
        rc = wl_columnar_memory_governor_commit_growth_controlled(
            &dst->retained_reservation, &committed, control);
        if (rc || !committed) {
            if (!rc) {
                rc = EINVAL;
            }
            goto done;
        }
        dst->retained_reserved_bytes += incoming;
        record->pending.growth = false;
        record->transaction_active = false;
    }
    record->published = true;
    record->growth = true; /* Retire actual old aggregate before downsize. */
    record->compact_retired.ledger_pending = true;
    record->compact_retired.merge_columns = dst->merge_columns;
    dst->merge_columns = NULL; dst->merge_buf_cap = 0;
    if (shrink || empty) {
        record->old_columns = dst->columns;
        record->old_timestamps = dst->timestamps;
        dst->columns = record->prepared_columns;
        dst->timestamps = record->prepared_timestamps;
        record->prepared_columns = NULL; record->prepared_timestamps = NULL;
        dst->capacity = record->new_capacity;
        dst->timestamp_capacity = timestamp_bytes ? record->new_capacity : 0;
        if (shrink || record->old_columns || record->old_timestamps)
            wl_columnar_relation_touch_storage(dst);
    }
    if (dst->sorted_nrows > dst->nrows) dst->sorted_nrows = dst->nrows;
    if (dst->base_nrows > dst->nrows) dst->base_nrows = dst->nrows;
done:
    return wl_columnar_relation_bulk_append_retire(record, rc);
}

int
wl_columnar_relation_combined_reader_controlled(col_rel_t *rel,
    wl_columnar_source_access_reader_t *reader,
    wl_evaluation_control_t *control)
{
    if (!rel || !reader || !control || reader->owner || reader->secondary_owner
        || reader->identity || reader->thread_valid) return EINVAL;
    wl_columnar_source_access_gate_t *descriptor = &rel->descriptor_access;
    uint64_t observed = atomic_load_explicit(&descriptor->state,
            memory_order_relaxed);
    for (;;) {
        int rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        if (observed >= WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u) return EBUSY;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_before_reader_cas)
            wl_columnar_relation_test_before_reader_cas(control, descriptor,
                observed);
#endif
#ifdef _MSC_VER
        bool acquired =
            atomic_compare_exchange_weak_explicit(&descriptor->state,
#else
        bool acquired =
            atomic_compare_exchange_strong_explicit(&descriptor->state,
#endif
                &observed, observed + 1u, memory_order_acquire,
                memory_order_relaxed);
        if (acquired) reader->secondary_owner = descriptor;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_after_reader_cas)
            wl_columnar_relation_test_after_reader_cas(control, descriptor,
                acquired);
#endif
        rc = wl_columnar_relation_work_tail(control, 8);
        if (rc) return rc; /* Actual descriptor-only disposition stays live. */
        if (acquired) break;
    }
    int rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    if (rel->storage_owner != rel ||
        !rel->storage_owner_identity) return ENOTSUP;
    rc = wl_columnar_relation_work_tail(control, 16);
    if (rc) return rc;
    wl_columnar_source_access_gate_t *source = &rel->source_access;
    observed = atomic_load_explicit(&source->state, memory_order_relaxed);
    for (;;) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return rc;
        if (observed >= WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u) return EBUSY;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_before_reader_cas)
            wl_columnar_relation_test_before_reader_cas(control, source,
                observed);
#endif
#ifdef _MSC_VER
        bool acquired = atomic_compare_exchange_weak_explicit(&source->state,
#else
        bool acquired = atomic_compare_exchange_strong_explicit(&source->state,
#endif
                &observed, observed + 1u, memory_order_acquire,
                memory_order_relaxed);
        if (acquired) {
            wl_columnar_source_access_reader_initialize(source, reader, false);
            reader->secondary_owner = descriptor;
        }
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_after_reader_cas)
            wl_columnar_relation_test_after_reader_cas(control, source,
                acquired);
#endif
        rc = wl_columnar_relation_work_tail(control, 16);
        if (rc || acquired) return rc;
    }
}

/* Calculate the compacted shape without mutating the relation. */
static bool
col_rel_compaction_shape(const col_rel_t *r, uint32_t *out_capacity,
    uint64_t *out_bytes)
{
    uint32_t tight;

    if (!r || !col_rel_timestamp_shape_valid(r) || r->nrows == 0
        || r->capacity <= (uint64_t) r->nrows * 4)
        return false;
    tight = r->nrows * 2;
    if (tight < r->nrows)
        tight = UINT32_MAX;
    if (tight < COL_REL_INIT_CAP)
        tight = COL_REL_INIT_CAP;
    if (out_capacity)
        *out_capacity = tight;
    if (out_bytes) {
        col_rel_payload_shape_t shape;
        if (!col_rel_payload_live_shape(r, &shape))
            return false;
        shape.owned_columns = r->ncols;
        shape.active_table = true;
        shape.column_capacity = tight;
        shape.timestamp_capacity = r->timestamps ? tight : 0;
        if (!col_rel_payload_shape_bytes(&shape, out_bytes))
            return false;
    }
    return true;
}

/* Only the new tight primary image overlaps the old aggregate.  Merge and
 * scratch remain resident and must not be reserved a second time. */
static bool
col_rel_compaction_incoming_bytes(const col_rel_t *r, uint32_t tight,
    uint64_t *out)
{
    uint64_t grid, timestamps;
    if (!col_rel_payload_grid_bytes(r->ncols, r->ncols, tight, true, &grid)
        || !wl_columnar_memory_size_mul(r->timestamps ? tight : 0,
        sizeof(col_delta_timestamp_t), &timestamps))
        return false;
    return wl_columnar_memory_size_add(grid, timestamps, out);
}

/*
 * col_rel_compact:
 * Shrink oversized data and timestamps buffers after bulk retraction.
 *
 * Guards:
 *   - NULL or ncols==0: no-op
 *   - nrows==0: release data, merge_buf, timestamps; zero capacities
 *   - capacity <= nrows*4: already tight enough; skip
 *
 * On success the capacity is reduced to max(nrows*2, COL_REL_INIT_CAP).
 * merge_buf is always freed (it will be re-allocated on next consolidation).
 * Allocation failures are non-fatal: the relation remains valid.  EBUSY is
 * returned when an active source reader or live owner alias excludes the
 * mutation; in that case the relation is untouched.
 * sorted_nrows is clamped to nrows if it drifted above.
 */
static int
col_rel_compact_impl(col_rel_t *r,
    wl_columnar_source_access_writer_t *writer)
{
    bool alias_release_pending = false;
    int rc;

    if (!r || !writer || r->ncols == 0)
        return 0;
    rc = 0;
    uint64_t ledger_before = col_rel_owned_ledger_bytes(r);

    if (r->nrows == 0) {
        bool storage_changed = r->columns != NULL || r->timestamps != NULL;
        if (!r->arena_owned) {
            if (r->col_shared && r->columns) {
                /* COW: free only non-shared columns (Issue #396) */
                for (uint32_t c = 0; c < r->ncols; c++) {
                    if (!r->col_shared[c])
                        free(r->columns[c]);
                }
                free((void *)r->columns);
                free(r->col_shared);
                r->col_shared = NULL;
                col_rel_retire_shared_table_credit(r);
            } else {
                col_columns_free(r->columns, r->ncols);
            }
        } else {
            free((void *)r->columns);
        }
        r->columns = NULL;
        r->capacity = 0;
        r->arena_owned = false;
        col_columns_free(r->merge_columns, r->ncols);
        r->merge_columns = NULL;
        r->merge_buf_cap = 0;
        free(r->timestamps);
        r->timestamps = NULL;
        r->timestamp_capacity = 0;
        /* Physical storage must be gone before either the ledger or the
         * retained admission is reduced.  This keeps a concurrent observer
         * from seeing capacity credit while the old buffers are still live. */
        col_rel_ledger_reconcile(r, ledger_before);
        col_rel_retire_payload_credit(r);
        r->sorted_nrows = 0;
        r->base_nrows = 0;
        if (storage_changed)
            wl_columnar_relation_touch_storage(r);
        alias_release_pending = r->storage_owner != r;
        goto free_merge_buf;
    }

    /* Only compact when buffer is more than 4x oversized.  A previous
     * replacement may have published its new storage but failed to settle
     * the governor token.  Finish that token before treating the relation as
     * already compact; otherwise a caller could observe success while a
     * live REPLACING token was stranded forever. */
    if (!col_rel_compaction_shape(r, NULL, NULL)) {
        if (r->memory_governor
            && atomic_load_explicit(&r->retained_reservation.state,
            memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING) {
            if (r->retained_reservation.transition_kind
                != WL_COLUMNAR_MEMORY_TRANSITION_GROWTH) {
                rc = EBUSY;
                goto free_merge_buf;
            }
            if (!wl_columnar_relation_compaction_rollback_growth(
                    &r->retained_reservation)) {
                rc = EAGAIN;
                goto free_merge_buf;
            }
        }
        goto free_merge_buf;
    }

    {
        uint32_t tight = 0;
        uint64_t incoming_bytes = 0;
        bool growth_started = false;
        uint64_t reservation_state;

        (void) col_rel_compaction_shape(r, &tight, NULL);
        if (!col_rel_compaction_incoming_bytes(r, tight, &incoming_bytes)) {
            rc = EOVERFLOW;
            goto compact_done;
        }

        if (r->memory_governor) {
            wl_columnar_memory_admission_status_t status;
            if (r->retained_reserved_bytes == 0) {
                rc = EBUSY;
                goto compact_done;
            }
            reservation_state = atomic_load_explicit(
                &r->retained_reservation.state, memory_order_acquire);
            if (reservation_state
                == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING) {
                if (r->retained_reservation.transition_kind
                    != WL_COLUMNAR_MEMORY_TRANSITION_GROWTH
                    || r->retained_reservation.replacement_bytes
                    != incoming_bytes) {
                    rc = EBUSY;
                    goto compact_done;
                }
            } else {
                status = wl_columnar_memory_begin_growth(
                    &r->retained_reservation, incoming_bytes);
                if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                    && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
                    goto compact_done;
            }
            growth_started = true;
        }

        /* Shrinking (and, for a shared view, privatizing) is one transaction:
         * the tight private buffers are prepared first and published only
         * once every allocation succeeded, so a failure leaves the
         * relation, its ownership flags and its generations untouched. */
        int64_t **new_cols = NULL;
        col_delta_timestamp_t *new_ts = NULL;
        int64_t **old_cols = r->columns;
        bool *old_col_shared = r->col_shared;
        col_delta_timestamp_t *old_ts = r->timestamps;
        bool old_arena_owned = r->arena_owned;
        if (col_rel_prepare_resize(r, tight, &new_cols, &new_ts) != 0) {
            if (growth_started
                && !wl_columnar_relation_compaction_rollback_growth(
                    &r->retained_reservation))
                rc = EAGAIN;
            goto compact_done;
        }
        if (growth_started) {
            if (!wl_columnar_relation_compaction_commit_growth(
                    &r->retained_reservation)) {
                col_columns_free(new_cols, r->ncols);
                free(new_ts);
                if (!wl_columnar_relation_compaction_rollback_growth(
                        &r->retained_reservation))
                    rc = EAGAIN;
                else
                    rc = EFAULT;
                goto compact_done;
            }
            r->retained_reserved_bytes += incoming_bytes;
        }
        /* The pointer swap itself cannot fail.  Keep the old storage live
         * until the replacement has been published; it remains charged by
         * the governor until the old storage is actually freed below. */
        r->columns = new_cols;
        r->col_shared = NULL;
        r->timestamps = new_ts;
        r->timestamp_capacity = new_ts ? tight : 0;
        r->capacity = tight;
        r->arena_owned = false;
        new_cols = NULL;
        new_ts = NULL;
        wl_columnar_relation_touch_storage(r);
        {
            col_rel_t old_storage = { 0 };
            old_storage.columns = old_cols;
            old_storage.col_shared = old_col_shared;
            old_storage.arena_owned = old_arena_owned;
            old_storage.ncols = r->ncols;
            col_rel_release_column_storage(&old_storage);
            free(old_ts);
            if (old_col_shared)
                col_rel_retire_shared_table_credit(r);
        }
        /* Retire credit only after the old primary image is physically gone. */
        col_rel_ledger_reconcile(r, ledger_before);
        col_rel_retire_payload_credit(r);
        alias_release_pending = r->storage_owner != r;
    }

free_merge_buf:
    col_columns_free(r->merge_columns, r->ncols);
    r->merge_columns = NULL;
    r->merge_buf_cap = 0;
    col_rel_retire_payload_credit(r);

compact_done:

    if (r->sorted_nrows > r->nrows)
        r->sorted_nrows = r->nrows;
    if (r->base_nrows > r->nrows)
        r->base_nrows = r->nrows;

    if (alias_release_pending) {
        int alias_rc = col_rel_storage_alias_release(r);
        if (alias_rc != 0 && rc == 0)
            rc = alias_rc;
    }
    return rc;
}

int
col_rel_compact(col_rel_t *r)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    col_rel_t *owner = NULL;
    int rc;

    if (!r || r->ncols == 0)
        return 0;
    rc = col_rel_storage_owner_resolve(r, &owner);
    if (rc != 0)
        return rc;
    /* A canonical owner cannot replace or discard storage while a shared
     * alias still points at it.  Check only after writer admission so an
     * alias publication cannot race a stale pre-writer observation. */
    rc = col_rel_source_writer_acquire(r, &writer);
    if (rc != 0)
        return rc;
    rc = owner == r && col_rel_storage_alias_borrow_count(owner) > 0
        ? EBUSY : col_rel_compact_impl(r, &writer);
    if (wl_columnar_source_access_writer_release(&writer) != 0 && rc == 0)
        rc = EINVAL;
    return rc;
}

int
col_rel_compact_many(col_rel_t *const *rels, uint32_t nrels)
{
    col_rel_t **owners = NULL;
    wl_columnar_source_access_writer_t *writers = NULL;
    bool *replacement_started = NULL;
    uint32_t owner_count = 0;
    int rc = 0;

    if (!rels && nrels != 0)
        return EINVAL;
    if (nrels == 0)
        return 0;
    owners = (col_rel_t **)calloc(nrels, sizeof(*owners));
    writers = (wl_columnar_source_access_writer_t *)calloc(nrels,
            sizeof(*writers));
    replacement_started = (bool *)calloc(nrels, sizeof(*replacement_started));
    if (!owners || !writers || !replacement_started) {
        free((void *)owners);
        free(writers);
        free(replacement_started);
        /* Compaction is best-effort housekeeping.  If its temporary
         * bookkeeping cannot be allocated, preserve every relation and let
         * the completed evaluation commit normally.  Source-access and
         * compaction failures below still propagate to the caller. */
        return 0;
    }

    /* Resolve and validate every owner before acquiring any writer. */
    for (uint32_t i = 0; i < nrels; i++) {
        col_rel_t *owner = NULL;
        if (!rels[i] || rels[i]->ncols == 0)
            continue;
        rc = col_rel_storage_owner_resolve(rels[i], &owner);
        if (rc != 0) {
            rc = rc != 0 ? rc : EBUSY;
            goto cleanup;
        }
        bool seen = false;
        for (uint32_t j = 0; j < owner_count; j++) {
            if (owners[j] == owner) {
                seen = true;
                break;
            }
        }
        if (!seen)
            owners[owner_count++] = owner;
    }

    /* Hold every owner writer through every compaction. */
    for (uint32_t i = 0; i < owner_count; i++) {
        rc = wl_columnar_source_access_writer_acquire(
            &owners[i]->source_access, &writers[i]);
        if (rc != 0)
            goto cleanup;
    }

    /* Admit every governed non-empty replacement before preparing or
     * publishing any member of the batch.  A denied admission therefore
     * leaves the entire batch unchanged; the individual compactor reuses
     * these held replacement states below. */
    for (uint32_t i = 0; i < nrels; i++) {
        uint64_t state;
        uint64_t incoming_bytes;
        uint32_t tight;
        wl_columnar_memory_admission_status_t status;

        if (!rels[i] || !rels[i]->memory_governor
            || !col_rel_compaction_shape(rels[i], &tight, NULL)
            || rels[i]->retained_reserved_bytes == 0)
            continue;
        if (!col_rel_compaction_incoming_bytes(rels[i], tight,
            &incoming_bytes)) {
            rc = EOVERFLOW;
            goto cleanup;
        }
        state = atomic_load_explicit(&rels[i]->retained_reservation.state,
                memory_order_acquire);
        if (state == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING) {
            bool duplicate = false;
            for (uint32_t j = 0; j < i; j++) {
                if (rels[j] == rels[i] && replacement_started[j]) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate)
                continue;
            rc = EBUSY;
            goto cleanup;
        }
        if (state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED) {
            rc = EBUSY;
            goto cleanup;
        }
        status = wl_columnar_memory_begin_growth(
            &rels[i]->retained_reservation, incoming_bytes);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            rc = 0;
            goto cleanup;
        }
        replacement_started[i] = true;
    }

    for (uint32_t i = 0; i < nrels; i++) {
        col_rel_t *owner = NULL;
        if (!rels[i] || rels[i]->ncols == 0)
            continue;
        rc = col_rel_storage_owner_resolve(rels[i], &owner);
        if (rc != 0)
            goto cleanup;
        if (owner == rels[i]
            && col_rel_storage_alias_borrow_count(owner) != 0) {
            rc = EBUSY;
            goto cleanup;
        }
    }
    for (uint32_t i = 0; i < nrels; i++) {
        if (!rels[i] || rels[i]->ncols == 0)
            continue;
        col_rel_t *owner = NULL;
        uint32_t owner_idx = 0;
        /* Re-resolve rather than caching the first pass's answer, but do
         * not assume the result: if the owner changed or the resolve now
         * fails, the writer held for it is not the right one, so fail
         * instead of compacting under the wrong lease.  The search is
         * bounded by owner_count -- an unbounded scan would run past the
         * array whenever the owner is absent. */
        rc = col_rel_storage_owner_resolve(rels[i], &owner);
        if (rc != 0)
            goto cleanup;
        while (owner_idx < owner_count && owners[owner_idx] != owner)
            owner_idx++;
        if (owner_idx == owner_count) {
            rc = EBUSY;
            goto cleanup;
        }
        rc = col_rel_compact_impl(rels[i], &writers[owner_idx]);
        if (rc != 0)
            goto cleanup;
    }

cleanup:
    for (uint32_t i = 0; i < nrels; i++) {
        if (replacement_started[i] && rels[i] && rels[i]->memory_governor
            && atomic_load_explicit(&rels[i]->retained_reservation.state,
            memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING
            && rels[i]->retained_reservation.transition_kind
            == WL_COLUMNAR_MEMORY_TRANSITION_GROWTH
            && !wl_columnar_memory_rollback_growth(
                &rels[i]->retained_reservation)
            )
            rc = EAGAIN;
    }
    for (uint32_t i = 0; i < owner_count; i++) {
        if (writers[i].owner
            && wl_columnar_source_access_writer_release(&writers[i]) != 0
            && rc == 0)
            rc = EINVAL;
    }
    free(writers);
    free((void *)owners);
    free(replacement_started);
    return rc;
}

/*
 * col_rel_install_shared_view:
 * Install src's column buffers into dst as a zero-copy shared view.
 * dst's existing columns are freed (respecting any existing col_shared flags).
 * After this call, dst->columns[c] points to src->columns[c] and
 * dst->col_shared[c] is true for all c, so col_rel_free_contents/COW
 * will not free the borrowed buffers.
 *
 * This function does not establish a lifetime reference on src.  The caller
 * must keep src (and its storage) alive, or use the pin/deferred-destruction
 * contract from Issues #1384/#1435 before installing a view.  Generation
 * publication here intentionally does not invent a second ownership model.
 *
 * dst and src must have the same ncols.
 * Returns 0 on success, ENOMEM if the col_shared flags array cannot be
 * allocated (dst remains valid with its original columns in that case).
 *
 * Issue #396: used by tdd_broadcast_deltas to eliminate O(|delta|) deep
 * copies when broadcasting the union delta to worker sessions.
 */
/* Publication permits stay on the acquiring thread's stack.  A copied gate
* token or permit cannot authorize a different descriptor's publication. */
typedef struct wl_columnar_relation_rebind_permit {
    uintptr_t identity;
    col_rel_t *destination;
    col_rel_t *old_owner;
    wl_columnar_source_access_writer_t *descriptor_writer;
    wl_columnar_source_access_reader_t *upgraded_lease;
    bool lease_descriptor_upgraded;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD thread;
#else
    pthread_t thread;
#endif
} wl_columnar_relation_rebind_permit_t;

static void
wl_columnar_relation_rebind_permit_init(
    wl_columnar_relation_rebind_permit_t *permit, col_rel_t *destination,
    col_rel_t *old_owner, wl_columnar_source_access_writer_t *descriptor_writer,
    wl_columnar_source_access_reader_t *upgraded_lease,
    bool lease_descriptor_upgraded)
{
    permit->identity = (uintptr_t)permit;
    permit->destination = destination;
    permit->old_owner = old_owner;
    permit->descriptor_writer = descriptor_writer;
    permit->upgraded_lease = upgraded_lease;
    permit->lease_descriptor_upgraded = lease_descriptor_upgraded;
#if defined(WL_HAVE_C11_THREADS)
    permit->thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    permit->thread = GetCurrentThreadId();
#else
    permit->thread = pthread_self();
#endif
}

static bool
wl_columnar_relation_rebind_permit_valid(
    const wl_columnar_relation_rebind_permit_t *permit,
    const col_rel_t *destination)
{
    if (!permit || permit->identity != (uintptr_t)permit ||
        permit->destination != destination || !destination)
        return false;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(permit->thread, thrd_current()))
        return false;
#elif defined(_WIN32) || defined(_WIN64)
    if (permit->thread != GetCurrentThreadId())
        return false;
#else
    if (!pthread_equal(permit->thread, pthread_self()))
        return false;
#endif
    if (permit->lease_descriptor_upgraded) {
        const wl_columnar_source_access_reader_t *lease =
            permit->upgraded_lease;
        return !permit->descriptor_writer && lease &&
               lease->identity == (uintptr_t)lease && lease->transferable &&
               !lease->thread_valid &&
               lease->owner == &destination->source_access &&
               lease->secondary_owner == &destination->descriptor_access &&
               atomic_load_explicit(&lease->owner->state,
                   memory_order_acquire) ==
               WL_COLUMNAR_SOURCE_ACCESS_WRITER &&
               atomic_load_explicit(&lease->secondary_owner->state,
                   memory_order_acquire) ==
               WL_COLUMNAR_SOURCE_ACCESS_WRITER;
    }
    return wl_columnar_relation_retirement_writer_valid(
        permit->descriptor_writer, &destination->descriptor_access);
}

static int
wl_columnar_relation_rebind_release_old_borrow(
    const wl_columnar_relation_rebind_permit_t *permit, col_rel_t *old_owner)
{
    if (!wl_columnar_relation_rebind_permit_valid(
            permit, permit ? permit->destination : NULL) ||
        old_owner != permit->old_owner || old_owner == permit->destination)
        return EINVAL;
    /* Validate before decrementing.  After the successful decrement the old
     * root may be destroyed immediately; do not inspect it again. */
    return col_rel_storage_alias_borrow_release(old_owner);
}

static int
col_rel_install_shared_view_unprotected(col_rel_t *dst, const col_rel_t *src,
    const wl_columnar_relation_rebind_permit_t *permit)
{
    col_rel_t *source_owner = NULL;
    col_rel_t *old_owner = NULL;
    wirelog_column_type_t *shared_types = NULL;
    int64_t **shared_columns = NULL;
    bool *shared_flags = NULL;
    col_delta_timestamp_t *shared_timestamps = NULL;
    int64_t *shared_scratch = NULL;
    uint32_t shared_timestamp_capacity = 0;
    char **shared_names = NULL;
    uint32_t *shared_arity_map = NULL;
    uint32_t shared_arity_len = 0;
    bool shared_has_graph_column = false;
    uint32_t shared_graph_col_idx = 0;
    uint32_t shared_declared_ncols = 0;
    wirelog_compound_kind_t shared_compound_kind
        = WIRELOG_COMPOUND_KIND_NONE;
    uint32_t shared_compound_count = 0;
    uint32_t shared_inline_physical_offset = 0;
    struct ArrowSchema prepared_schema;
    bool prepared_schema_ok = false;
    wl_columnar_memory_reservation_t metadata_pending, retained_pending;
    wl_columnar_memory_reservation_t old_metadata, old_retained;
    uint64_t metadata_bytes = 0, retained_bytes = 0;
    int prepare_rc = ENOMEM;
    wl_columnar_memory_reservation_init(&metadata_pending);
    wl_columnar_memory_reservation_init(&retained_pending);
    if (!dst || !src || dst == src || dst->ncols != src->ncols)
        return EINVAL;
    if (!wl_columnar_relation_rebind_permit_valid(permit, dst))
        return EINVAL;
    if (!col_rel_timestamp_shape_valid(src)
        || !col_rel_timestamp_shape_valid(dst)
        || src->nrows > src->capacity)
        return EINVAL;
    if (dst->memory_governor && (dst->retract_backup_columns
        || dst->retract_backup_timestamps))
        return EBUSY;
    if (!dst->storage_owner)
        col_rel_storage_owner_init(dst);
    if (col_rel_storage_owner_resolve(src, &source_owner) != 0
        || col_rel_storage_owner_resolve(dst, &old_owner) != 0)
        return EINVAL;
    if (permit->old_owner != old_owner)
        return EINVAL;
    /* Pool and arena owners can outlive their relation descriptors through
     * allocator reset, so they cannot be represented by a raw owner pointer
     * until the later allocator-lifetime unit adds a stable control block. */
    if (source_owner->pool_owned || source_owner->arena_owned)
        return EINVAL;
    /* A relation with live flattened aliases cannot itself become an alias:
     * doing so would invalidate the children's canonical owner. */
    if (col_rel_storage_alias_borrow_count(dst) > 0 || source_owner == dst
        || col_rel_storage_alias_borrow_count(source_owner) == UINT64_MAX)
        return source_owner == dst ? EINVAL : EBUSY;
    if (old_owner != dst
        && col_rel_storage_alias_borrow_count(old_owner) == 0)
        return EINVAL;
    /* A shared-view publication is a new epoch on the destination.  Reserve
     * the next values before replacing any buffers so exhaustion cannot leave
     * a successfully installed view with an invalid generation. */
    if (!wl_columnar_relation_generation_valid(dst->view_generation)
        || !wl_columnar_relation_generation_valid(dst->storage_generation)
        || dst->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || dst->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EOVERFLOW;
    if (src->has_graph_column && src->graph_col_idx >= src->ncols)
        return EINVAL;
    if (!wl_columnar_relation_compound_map_valid(src))
        return EINVAL;
    shared_timestamp_capacity = src->timestamps ? src->timestamp_capacity
        : dst->timestamps
            ? (dst->timestamp_capacity > src->nrows
                ? dst->timestamp_capacity : src->nrows) : 0;
    col_rel_payload_shape_t shared_payload = {
        .ncols = dst->ncols,
        .timestamp_capacity = shared_timestamp_capacity,
        .scratch = dst->memory_governor && dst->ncols != 0,
    };
    if (!col_rel_shared_view_metadata_bytes(src, &metadata_bytes)
        || !col_rel_payload_shape_bytes(&shared_payload, &retained_bytes))
        return EOVERFLOW;
    prepare_rc = col_rel_reserve_metadata(dst, metadata_bytes,
            &metadata_pending);
    if (prepare_rc != 0)
        return prepare_rc;
    if (dst->memory_governor && retained_bytes) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(dst->memory_governor),
                retained_bytes, &retained_pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                dst->memory_budget_denial_pending = true;
            prepare_rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOSPC : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
            wl_columnar_memory_reservation_init(&retained_pending);
            goto prepare_fail;
        }
    }
    prepare_rc = ENOMEM;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_fail_metadata_alloc) {
        wl_columnar_relation_test_fail_metadata_alloc = false;
        goto prepare_fail;
    }
#endif
    shared_arity_len = src->compound_arity_len;
    if (shared_arity_len) {
        uint64_t map_bytes = 0;
        if (!wl_columnar_memory_size_mul(shared_arity_len,
            sizeof(*shared_arity_map), &map_bytes) || map_bytes > SIZE_MAX) {
            prepare_rc = EOVERFLOW;
            goto prepare_fail;
        }
        shared_arity_map = (uint32_t *)malloc((size_t)map_bytes);
        if (!shared_arity_map)
            goto prepare_fail;
        memcpy(shared_arity_map, src->compound_arity_map, (size_t)map_bytes);
        shared_compound_kind = src->compound_kind;
        shared_compound_count = src->compound_count;
        shared_inline_physical_offset = src->inline_physical_offset;
    }
    shared_has_graph_column = src->has_graph_column;
    shared_graph_col_idx = src->graph_col_idx;
    shared_declared_ncols = src->declared_ncols;
    if (dst->ncols > 0) {
        shared_columns = (int64_t **)calloc(dst->ncols,
                sizeof(*shared_columns));
        shared_flags = (bool *)calloc(dst->ncols, sizeof(*shared_flags));
        if (!shared_columns || !shared_flags)
            goto prepare_fail;
        for (uint32_t c = 0; c < dst->ncols; c++) {
            shared_columns[c] = src->columns ? src->columns[c] : NULL;
            shared_flags[c] = true;
        }
    }
    if (src->column_types && src->ncols > 0) {
        shared_types = (wirelog_column_type_t *)malloc(
            (size_t)src->ncols * sizeof(*shared_types));
        if (!shared_types)
            goto prepare_fail;
        memcpy(shared_types, src->column_types,
            (size_t)src->ncols * sizeof(*shared_types));
    }
    if (src->ncols > 0) {
        shared_names = (char **)calloc(src->ncols, sizeof(*shared_names));
        if (!shared_names)
            goto prepare_fail;
        for (uint32_t c = 0; c < src->ncols; c++) {
            const char *name = src->col_names && src->col_names[c]
                ? src->col_names[c] : "";
            shared_names[c] = wl_strdup(name);
            if (!shared_names[c])
                goto prepare_fail;
        }
    }
    if (src->timestamps || dst->timestamps) {
        if (shared_timestamp_capacity > 0) {
            shared_timestamps = calloc(shared_timestamp_capacity,
                    sizeof(*shared_timestamps));
            if (!shared_timestamps)
                goto prepare_fail;
            if (src->timestamps)
                memcpy(shared_timestamps, src->timestamps,
                    (size_t)src->timestamp_capacity *
                    sizeof(*shared_timestamps));
        }
    }
    if (dst->memory_governor && dst->ncols) {
        shared_scratch = (int64_t *)malloc((size_t)dst->ncols
                * sizeof(*shared_scratch));
        if (!shared_scratch)
            goto prepare_fail;
    }
    /* Prepare a complete schema for every destination, including a relation
     * whose schema was never initialized.  The installed buffers and their
     * Arrow interpretation must be published as one transaction. */
    {
        col_rel_t image = { 0 };
        image.ncols = dst->ncols;
        image.column_types = shared_types;
        image.col_names = shared_names;
        if (wl_columnar_relation_init_arrow_schema(&image) != 0) {
            prepare_rc = EINVAL;
            goto prepare_fail;
        }
        prepared_schema = image.schema;
        prepared_schema_ok = true;
    }
    {
        int64_t **old_columns = dst->columns;
        bool *old_shared = dst->col_shared;
        bool old_arena_owned = dst->arena_owned;
        wirelog_column_type_t *old_types = dst->column_types;
        col_delta_timestamp_t *old_timestamps = dst->timestamps;
        uint32_t *old_arity_map = dst->compound_arity_map;
        char **old_names = dst->col_names;
        int64_t **old_merge = dst->merge_columns;
        int64_t **old_retract = dst->retract_backup_columns;
        col_delta_timestamp_t *old_retract_timestamps
            = dst->retract_backup_timestamps;
        uint64_t *old_dedup = dst->dedup_slots;
        wl_columnar_memory_reservation_t old_dedup_reservation;
        int64_t *old_scratch = dst->row_scratch;
        struct ArrowSchema old_schema = dst->schema;
        bool old_schema_ok = dst->schema_ok;
        uint64_t ledger_before = col_rel_owned_ledger_bytes(dst);

        /* Reserve the new owner's borrow before publication. The source
         * reader held by the wrapper prevents owner destruction until this
         * transaction completes. No fallible work follows this increment. */
        if (col_rel_storage_alias_borrow_acquire(source_owner) != 0)
            goto prepare_fail;
        wl_columnar_memory_reservation_init(&old_dedup_reservation);
        if (dst->dedup_reserved_bytes
            && !wl_columnar_memory_reservation_move(
                &old_dedup_reservation, &dst->dedup_reservation))
            abort();

        /* Commit point: every new allocation and both overlapping
         * reservations are ready. Publication itself cannot fail. */
        dst->merge_columns = NULL;
        dst->merge_buf_cap = 0;
        dst->retract_backup_columns = NULL;
        dst->retract_backup_timestamps = NULL;
        dst->retract_backup_timestamp_capacity = 0;
        dst->retract_backup_nrows = 0;
        dst->retract_backup_base_nrows = 0;
        dst->retract_backup_capacity = 0;
        dst->retract_backup_sorted_nrows = 0;
        dst->retract_backup_run_count = 0;
        memset(dst->retract_backup_run_ends, 0,
            sizeof(dst->retract_backup_run_ends));
        dst->dedup_slots = NULL;
        dst->dedup_cap = 0;
        dst->dedup_count = 0;
        dst->dedup_reserved_bytes = 0;
        dst->row_scratch = shared_scratch;
        dst->columns = shared_columns;
        dst->col_shared = shared_flags;
        dst->column_types = shared_types;
        dst->timestamps = shared_timestamps;
        dst->timestamp_capacity = shared_timestamp_capacity;
        dst->has_graph_column = shared_has_graph_column;
        dst->graph_col_idx = shared_graph_col_idx;
        dst->declared_ncols = shared_declared_ncols;
        dst->compound_kind = shared_compound_kind;
        dst->compound_count = shared_compound_count;
        dst->compound_arity_map = shared_arity_map;
        dst->compound_arity_len = shared_arity_len;
        dst->inline_physical_offset = shared_inline_physical_offset;
        dst->col_names = shared_names;
        dst->arena_owned = false;
        dst->nrows = src->nrows;
        dst->capacity = src->capacity;
        dst->sorted_nrows = src->sorted_nrows;
        dst->base_nrows = src->base_nrows;
        dst->run_count = src->run_count;
        memcpy(dst->run_ends, src->run_ends, sizeof(dst->run_ends));
        dst->schema = prepared_schema;
        dst->schema_ok = prepared_schema_ok;
        dst->storage_owner = source_owner;
        dst->storage_owner_identity = source_owner->relation_identity;
        dst->storage_owner_generation = source_owner->storage_generation;
        atomic_store_explicit(&dst->storage_alias_borrows, 0,
            memory_order_relaxed);
        dst->view_generation++;
        dst->storage_generation++;
        col_rel_publish_metadata(dst, &metadata_pending, metadata_bytes,
            &old_metadata);
        wl_columnar_memory_reservation_init(&old_retained);
        if (dst->memory_governor) {
            if (dst->retained_reserved_bytes
                && !wl_columnar_memory_reservation_move(&old_retained,
                &dst->retained_reservation))
                abort();
            dst->retained_reserved_bytes = 0;
            if (retained_bytes
                && (!wl_columnar_memory_reservation_move(
                    &dst->retained_reservation, &retained_pending)
                || !wl_columnar_memory_commit(
                    &dst->retained_reservation, dst)))
                abort();
            dst->retained_reserved_bytes = retained_bytes;
        }
        /* Retire the old physical image before returning either old credit. */
        if (!old_arena_owned && old_columns)
            for (uint32_t c = 0; c < dst->ncols; c++)
                if ((!old_shared || !old_shared[c]) && old_columns[c])
                    free(old_columns[c]);
        free((void *)old_columns);
        free(old_shared);
        free(old_types);
        free(old_timestamps);
        free(old_arity_map);
        if (old_names) {
            for (uint32_t c = 0; c < dst->ncols; c++)
                free(old_names[c]);
            free((void *)old_names);
        }
        col_columns_free(old_merge, dst->ncols);
        col_columns_free(old_retract, dst->ncols);
        free(old_retract_timestamps);
        free(old_dedup);
        col_rel_release_reservation_or_abort(&old_dedup_reservation);
        free(old_scratch);
        if (old_schema_ok)
            ArrowSchemaRelease(&old_schema);
        col_rel_ledger_reconcile(dst, ledger_before);
        col_rel_release_reservation_or_abort(&old_metadata);
        col_rel_release_reservation_or_abort(&old_retained);
        /* Drop the old borrow only after dst no longer refers to that owner,
         * so a concurrent root destroy cannot observe an unborrowed live
         * alias descriptor. */
        if (old_owner != dst
            && wl_columnar_relation_rebind_release_old_borrow(permit,
            old_owner) != 0)
            abort();
    }
    shared_columns = NULL;
    shared_flags = NULL;
    shared_types = NULL;
    shared_timestamps = NULL;
    shared_scratch = NULL;
    shared_names = NULL;
    shared_arity_map = NULL;
    return 0;

prepare_fail:
    if (prepared_schema_ok)
        ArrowSchemaRelease(&prepared_schema);
    free((void *)shared_columns);
    free(shared_flags);
    free(shared_types);
    free(shared_timestamps);
    free(shared_scratch);
    free(shared_arity_map);
    if (shared_names) {
        for (uint32_t c = 0; c < src->ncols; c++)
            free(shared_names[c]);
        free((void *)shared_names);
    }
    col_rel_reservation_rollback(&retained_pending);
    col_rel_reservation_rollback(&metadata_pending);
    return prepare_rc;
}

int
wl_columnar_relation_install_shared_view_with_lease(col_rel_t *dst,
    const col_rel_t *src,
    wl_columnar_source_access_reader_t *destination_lease)
{
    wl_columnar_source_access_reader_t source_reader = { 0 };
    wl_columnar_source_access_writer_t descriptor_writer = { 0 };
    wl_columnar_source_access_writer_t owner_writer = { 0 };
    wl_columnar_relation_rebind_permit_t permit = { 0 };
    wl_columnar_source_access_gate_t *source_descriptor;
    col_rel_t *owner = NULL;
    col_rel_t *destination_owner = NULL;
    bool descriptor_upgraded = false;
    bool source_upgraded = false;
    bool source_descriptor_held = false;
    int rc;

    if (!dst || !src || dst == src)
        return EINVAL;
    rc = col_rel_storage_owner_ensure_initialized((col_rel_t *)src);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_ensure_initialized(dst);
    if (rc != 0)
        return rc;
    source_descriptor
        = (wl_columnar_source_access_gate_t *)&src->descriptor_access;

    /* Session lifetime leases usually pin the canonical root descriptor,
     * independently of the alias being refreshed.  Preserve those readers.
     * Only a lease on dst itself can supply its descriptor exclusion. */
    if (destination_lease
        && destination_lease->secondary_owner == &dst->descriptor_access) {
        if (destination_lease->identity != (uintptr_t)destination_lease
            || !destination_lease->transferable
            || destination_lease->thread_valid
            || destination_lease->owner != &dst->source_access) {
            rc = EINVAL;
            goto cleanup;
        }
        uint64_t expected = 1u;
        do {
            descriptor_upgraded = atomic_compare_exchange_weak_explicit(
                &dst->descriptor_access.state, &expected,
                WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_acquire,
                memory_order_relaxed);
        } while (!descriptor_upgraded && expected == 1u);
        if (!descriptor_upgraded) {
            rc = EBUSY;
            goto cleanup;
        }
    } else {
        rc = wl_columnar_source_access_writer_acquire(
            &dst->descriptor_access, &descriptor_writer);
        if (rc != 0)
            goto cleanup;
    }
    rc = col_rel_storage_owner_resolve(dst, &destination_owner);
    if (rc != 0)
        goto cleanup;
    rc = wl_columnar_source_access_gate_reader_acquire(source_descriptor);
    if (rc != 0)
        goto cleanup;
    source_descriptor_held = true;
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_rebind_after_descriptor)
        wl_columnar_relation_test_rebind_after_descriptor(dst);
#endif
    rc = col_rel_storage_owner_resolve(src, &owner);
    if (rc != 0)
        goto cleanup;
    if (destination_lease
        && (destination_lease->owner != &destination_owner->source_access
        || destination_lease->secondary_owner
        != &destination_owner->descriptor_access
        || destination_lease->identity != (uintptr_t)destination_lease
        || !destination_lease->transferable
        || destination_lease->thread_valid
        || (descriptor_upgraded && destination_owner != dst))) {
        rc = EINVAL;
        goto cleanup;
    }
    if (owner != destination_owner) {
        rc = col_rel_source_reader_acquire(owner, &source_reader);
        if (rc != 0)
            goto cleanup;
    }
    if (destination_lease) {
        /* Both upgrades are nonblocking and leave the transferable token at
         * its original address.  Restore source before descriptor on every
         * outcome, including admission or allocation failure. */
        uint64_t expected = 1u;
        do {
            source_upgraded = atomic_compare_exchange_weak_explicit(
                &destination_owner->source_access.state, &expected,
                WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_acquire,
                memory_order_relaxed);
        } while (!source_upgraded && expected == 1u);
        if (!source_upgraded) {
            rc = EBUSY;
            goto cleanup;
        }
    } else {
        rc = wl_columnar_source_access_writer_acquire(
            &destination_owner->source_access, &owner_writer);
        if (rc != 0)
            goto cleanup;
    }
    wl_columnar_relation_rebind_permit_init(&permit, dst, destination_owner,
        descriptor_upgraded ? NULL : &descriptor_writer,
        destination_lease, descriptor_upgraded);
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_rebind_before_publication)
        wl_columnar_relation_test_rebind_before_publication(dst);
#endif
    rc = col_rel_install_shared_view_unprotected(dst, src, &permit);

cleanup:
    if (source_upgraded)
        atomic_store_explicit(&destination_owner->source_access.state,
            1u, memory_order_release);
    if (owner_writer.owner
        && wl_columnar_source_access_writer_release(&owner_writer) != 0
        && rc == 0)
        rc = EINVAL;
    if (source_reader.owner
        && col_rel_source_reader_release(&source_reader) != 0 && rc == 0)
        rc = EINVAL;
    if (source_descriptor_held
        && wl_columnar_source_access_gate_reader_release(source_descriptor)
        != 0 && rc == 0)
        rc = EINVAL;
    if (descriptor_upgraded)
        atomic_store_explicit(&dst->descriptor_access.state, 1u,
            memory_order_release);
    if (descriptor_writer.owner
        && wl_columnar_source_access_writer_release(&descriptor_writer) != 0
        && rc == 0)
        rc = EINVAL;
    return rc;
}

int
col_rel_install_shared_view(col_rel_t *dst, const col_rel_t *src)
{
    wl_columnar_source_access_reader_t source_owner_reader = { 0 };
    wl_columnar_relation_rebind_permit_t permit = { 0 };
    wl_columnar_source_access_writer_t dst_descriptor_writer = { 0 };
    wl_columnar_source_access_writer_t dst_owner_writer = { 0 };
    wl_columnar_source_access_gate_t *src_descriptor;
    wl_columnar_source_access_gate_t *dst_descriptor;
    col_rel_t *source_owner = NULL;
    col_rel_t *dst_owner = NULL;
    bool src_descriptor_reader = false;
    bool dst_descriptor_first = false;
    bool source_owner_reader_held = false;
    bool dst_owner_writer_held = false;
    bool dst_owner_first = false;
    int rc;

    if (!dst || !src || dst == src)
        return EINVAL;
    rc = col_rel_storage_owner_ensure_initialized((col_rel_t *)src);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_ensure_initialized(dst);
    if (rc != 0)
        return rc;
    src_descriptor =
        (wl_columnar_source_access_gate_t *)&src->descriptor_access;
    dst_descriptor = &dst->descriptor_access;
    if ((uintptr_t)src_descriptor < (uintptr_t)dst_descriptor) {
        rc = wl_columnar_source_access_gate_reader_acquire(src_descriptor);
        if (rc != 0)
            return rc;
        src_descriptor_reader = true;
        rc = wl_columnar_source_access_writer_acquire(dst_descriptor,
                &dst_descriptor_writer);
    } else {
        dst_descriptor_first = true;
        rc = wl_columnar_source_access_writer_acquire(dst_descriptor,
                &dst_descriptor_writer);
        if (rc == 0) {
            rc = wl_columnar_source_access_gate_reader_acquire(src_descriptor);
            if (rc == 0)
                src_descriptor_reader = true;
        }
    }
    if (rc != 0)
        goto cleanup;
    rc = col_rel_storage_owner_resolve(src, &source_owner);
    if (rc != 0)
        goto cleanup;
    rc = col_rel_storage_owner_resolve(dst, &dst_owner);
    if (rc != 0)
        goto cleanup;
    if (dst_owner == dst && source_owner == dst) {
        rc = EINVAL;
        goto cleanup;
    }
    if (dst_owner == dst) {
        dst_owner_first = (uintptr_t)&dst_owner->source_access
            < (uintptr_t)&source_owner->source_access;
        if (dst_owner_first) {
            rc = wl_columnar_source_access_writer_acquire(
                &dst_owner->source_access, &dst_owner_writer);
            if (rc != 0)
                goto cleanup;
            dst_owner_writer_held = true;
        }
        rc = wl_columnar_source_access_reader_acquire(
            &source_owner->source_access, &source_owner_reader);
        if (rc != 0)
            goto cleanup;
        source_owner_reader_held = true;
        if (!dst_owner_first) {
            rc = wl_columnar_source_access_writer_acquire(
                &dst_owner->source_access, &dst_owner_writer);
            if (rc != 0)
                goto cleanup;
            dst_owner_writer_held = true;
        }
    } else {
        rc = wl_columnar_source_access_reader_acquire(
            &source_owner->source_access, &source_owner_reader);
        if (rc != 0)
            goto cleanup;
        source_owner_reader_held = true;
    }
    if (dst_owner == dst
        && col_rel_storage_alias_borrow_count(dst_owner) != 0) {
        rc = EBUSY;
        goto cleanup;
    }
    wl_columnar_relation_rebind_permit_init(&permit, dst, dst_owner,
        &dst_descriptor_writer, NULL, false);
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_rebind_before_publication)
        wl_columnar_relation_test_rebind_before_publication(dst);
#endif
    rc = col_rel_install_shared_view_unprotected(dst, src, &permit);

cleanup:
    if (dst_owner_first && source_owner_reader_held
        && wl_columnar_source_access_reader_release(&source_owner_reader) != 0
        && rc == 0)
        rc = EINVAL;
    if (dst_owner_writer_held
        && wl_columnar_source_access_writer_release(&dst_owner_writer) != 0
        && rc == 0)
        rc = EINVAL;
    if (!dst_owner_first && source_owner_reader_held
        && wl_columnar_source_access_reader_release(&source_owner_reader) != 0
        && rc == 0)
        rc = EINVAL;
    if (dst_descriptor_first && src_descriptor_reader
        && wl_columnar_source_access_gate_reader_release(src_descriptor) != 0
        && rc == 0)
        rc = EINVAL;
    if (dst_descriptor_writer.owner
        && wl_columnar_source_access_writer_release(
            &dst_descriptor_writer) != 0 && rc == 0)
        rc = EINVAL;
    if (!dst_descriptor_first && src_descriptor_reader
        && wl_columnar_source_access_gate_reader_release(src_descriptor) != 0
        && rc == 0)
        rc = EINVAL;
    return rc;
}

/* ---- column name lookup ------------------------------------------------- */

int
col_rel_col_idx(const col_rel_t *r, const char *name)
{
    if (!r->col_names || !name)
        return -1;
    for (uint32_t i = 0; i < r->ncols; i++) {
        if (r->col_names[i] && strcmp(r->col_names[i], name) == 0)
            return (int)i;
    }
    /* fallback: "col<N>" convention */
    if (name[0] == 'c' && name[1] == 'o' && name[2] == 'l') {
        char *end;
        long v = strtol(name + 3, &end, 10);
        if (*end == '\0' && v >= 0 && (uint32_t)v < r->ncols)
            return (int)v;
    }
    return -1;
}

/* ---- convenience constructors ------------------------------------------- */

/*
 * col_rel_clone_compound_meta:
 * Replicate src's validated, explicitly sized compound map onto dst so
 * source and destination destroy paths stay independent.
 *
 * Src must carry a valid nonempty map. A governed destination reserves its
 * complete prospective metadata image unless the caller already did so.
 *
 * Returns 0 on success, -1 on invalid shape, -2 on allocation/admission
 * failure, and -3 on overflow. The destination is unchanged on failure.
 */
static int
col_rel_clone_compound_meta(col_rel_t *dst, const col_rel_t *src,
    bool metadata_prepaid)
{
    wl_columnar_memory_reservation_t pending, old_metadata;
    uint64_t metadata_bytes = 0;
    if (!dst || !src || !wl_columnar_relation_compound_map_valid(src)
        || src->compound_arity_len == 0 || dst->compound_arity_map)
        return -1;
    uint64_t map_bytes = 0;
    if (!wl_columnar_memory_size_mul(src->compound_arity_len,
        sizeof(uint32_t), &map_bytes) || map_bytes > SIZE_MAX)
        return -3;
    if (!metadata_prepaid) {
        if (!col_rel_prospective_metadata_bytes(dst->ncols,
            (const char *const *)dst->col_names,
            dst->col_names != NULL, false, dst->column_types != NULL,
            dst->schema_ok, src->compound_arity_len,
            dst->col_shared != NULL, &metadata_bytes))
            return -3;
        int reserve_rc = col_rel_reserve_metadata(dst, metadata_bytes,
                &pending);
        if (reserve_rc != 0)
            return reserve_rc == EOVERFLOW ? -3
                : reserve_rc == EINVAL ? -1 : -2;
    }
    uint32_t *copy = (uint32_t *)malloc((size_t)map_bytes);
    if (!copy) {
        if (!metadata_prepaid)
            col_rel_reservation_rollback(&pending);
        return -2;
    }
    memcpy(copy, src->compound_arity_map, (size_t)map_bytes);
    dst->compound_arity_map = copy;
    dst->compound_arity_len = src->compound_arity_len;
    dst->compound_kind = src->compound_kind;
    dst->compound_count = src->compound_count;
    dst->inline_physical_offset = src->inline_physical_offset;
    if (!metadata_prepaid) {
        col_rel_publish_metadata(dst, &pending, metadata_bytes,
            &old_metadata);
        col_rel_release_reservation_or_abort(&old_metadata);
    }
    return 0;
}

/* Helper: create a new owned relation with given ncols and auto-named cols. */
col_rel_t *
col_rel_new_auto(const char *name, uint32_t ncols)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    if (col_rel_set_schema(r, ncols, NULL) != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    return r;
}

int
wl_columnar_relation_new_auto_governed(const char *name, uint32_t ncols,
    uint32_t initial_capacity, bool timestamps,
    wl_columnar_memory_governor_ref_t *governor, col_rel_t **out)
{
    col_rel_t *r = NULL;
    bool denied = false;
    int rc;

    if (out)
        *out = NULL;
    if (!name || !governor || !out)
        return EINVAL;
    rc = wl_columnar_relation_alloc_governed(&r, name, governor);
    if (rc != 0)
        return rc;
    rc = col_rel_set_schema_impl_capacity(r, ncols, NULL,
            initial_capacity, timestamps, &denied);
    if (rc != 0) {
        col_rel_destroy(r);
        return rc == ENOMEM && denied ? ENOSPC : rc;
    }
    *out = r;
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_relation_test_before_identity_cas)(
    wl_evaluation_control_t *control, uint64_t expected);
void (*wl_columnar_relation_test_constructor_checkpoint)(
    wl_evaluation_control_t *control, int phase, uint64_t progress);
int64_t wl_columnar_relation_test_constructor_fail_allocation = -1;
int wl_columnar_relation_test_constructor_fail_commit;
#endif

static void
wl_columnar_relation_constructor_checkpoint(wl_evaluation_control_t *control,
    int phase, uint64_t progress)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_constructor_checkpoint)
        wl_columnar_relation_test_constructor_checkpoint(control, phase,
            progress);
#else
    (void)control;
    (void)phase;
    (void)progress;
#endif
}

static bool
wl_columnar_relation_constructor_allocation_fails(void)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_constructor_fail_allocation >= 0) {
        if (wl_columnar_relation_test_constructor_fail_allocation-- == 0) {
            wl_columnar_relation_test_constructor_fail_allocation = -1;
            return true;
        }
    }
#endif
    return false;
}

static int
wl_columnar_relation_identity_controlled(uint64_t *identity,
    wl_evaluation_control_t *control)
{
    uint64_t current = atomic_load_explicit(&wl_next_relation_identity,
            memory_order_relaxed);
    for (;;) {
        if (!current || current == UINT64_MAX)
            return EOVERFLOW;
        int rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            return rc;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_relation_test_before_identity_cas)
            wl_columnar_relation_test_before_identity_cas(control, current);
#endif
        uint64_t consumed = current;
        bool success;
#ifdef _MSC_VER
        /* Existing InterlockedCompareExchange64-backed shim is strong despite
         * its weak_explicit spelling: it never fails spuriously. */
        success =
            atomic_compare_exchange_weak_explicit(&wl_next_relation_identity,
                &current, current + 1u, memory_order_relaxed,
                memory_order_relaxed);
#else
        success =
            atomic_compare_exchange_strong_explicit(&wl_next_relation_identity,
                &current, current + 1u, memory_order_relaxed,
                memory_order_relaxed);
#endif
        wl_columnar_relation_constructor_checkpoint(control,
            WL_COLUMNAR_RELATION_CONSTRUCTOR_IDENTITY, success ? 1u : 0u);
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            return rc; /* Even successful identities are never refunded. */
        if (success) {
            *identity = consumed;
            return 0;
        }
    }
}

static int
wl_columnar_relation_constructor_reserve(
    wl_columnar_memory_governor_ref_t *governor, uint64_t bytes,
    wl_columnar_memory_reservation_t *token, wl_evaluation_control_t *control)
{
    if (!governor || !bytes)
        return 0;
    wl_columnar_memory_admission_status_t status;
    int rc = wl_columnar_memory_governor_reserve_checked_controlled(
        wl_columnar_memory_governor_ref_get(governor), bytes, token,
        &status, control);
    if (rc != 0)
        return rc;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return 0;
    /* Failed admission owns no credit, even if requested bytes were staged. */
    wl_columnar_memory_reservation_init(token);
    return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

static int
wl_columnar_relation_constructor_commit(col_rel_t *r,
    wl_columnar_memory_reservation_t *pending,
    wl_columnar_memory_reservation_t *canonical, int stage,
    wl_evaluation_control_t *control)
{
    if (!pending->bytes)
        return wl_evaluation_control_charge(control, 0);
    wl_columnar_relation_constructor_checkpoint(control,
        WL_COLUMNAR_RELATION_CONSTRUCTOR_BEFORE_COMMIT, (uint64_t)stage);
    int rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_relation_test_constructor_fail_commit == stage) {
        wl_columnar_relation_test_constructor_fail_commit = 0;
        return stage == 4 ? EINVAL : ENOMEM;
    }
#endif
    if (pending->bytes) {
        /* Prevalidate the fixed move before irreversible commitment. */
        rc = wl_evaluation_control_charge(control, 4);
        if (rc != 0)
            return rc;
        uint64_t state = atomic_load_explicit(&canonical->state,
                memory_order_acquire);
        if (pending->identity != pending || canonical->identity != canonical
            || (state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
            && state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED))
            return stage == 4 ? EINVAL : ENOMEM;
        rc = wl_columnar_relation_work_tail(control, 4);
        if (rc != 0)
            return rc;
        bool committed;
        rc = wl_columnar_memory_governor_commit_controlled(pending, r,
                &committed, control);
        if (rc != 0)
            return rc;
        if (!committed)
            return stage == 4 ? EINVAL : ENOMEM;
        if (!wl_columnar_memory_reservation_move(canonical, pending))
            abort();
    }
    wl_columnar_relation_constructor_checkpoint(control,
        WL_COLUMNAR_RELATION_CONSTRUCTOR_COMMITTED, (uint64_t)stage);
    return wl_evaluation_control_charge(control, 0);
}

static void
wl_columnar_relation_constructor_retire_token(
    wl_columnar_memory_reservation_t *token)
{
    if (!token->bytes)
        return;
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED)
        col_rel_reservation_rollback(token);
    else if (state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        col_rel_release_reservation_or_abort(token);
    else
        abort();
}

static int
wl_columnar_relation_new_private_controlled(const char *name,
    uint32_t ncols, uint32_t initial_capacity, bool timestamps,
    wl_columnar_memory_governor_ref_t *governor, const col_rel_t *source,
    bool logical_copy, col_rel_t *external_slot, uint64_t external_identity,
    col_rel_t **out, wl_evaluation_control_t *control)
{
    col_rel_t *r = NULL;
    uint64_t *lengths = NULL;
    uint32_t columns_owned = 0, names_owned = 0;
    bool governor_retained = false;
    wl_columnar_memory_reservation_t descriptor, metadata, payload, scratch;
    bool with_schema = !logical_copy || source->schema_ok;
    uint32_t timestamp_capacity = logical_copy && timestamps
        ? (source->timestamp_capacity > initial_capacity
            ? source->timestamp_capacity : initial_capacity)
        : timestamps ? initial_capacity : 0;
    uint64_t name_length = 0, descriptor_bytes;
    uint64_t metadata_bytes = with_schema ? 3u : 0u, payload_bytes;
    uint64_t lane_bytes, column_table_bytes, name_table_bytes, scratch_bytes;
    uint64_t length_bytes, timestamp_bytes, type_bytes = 0;
    col_rel_payload_shape_t shape = {
        .ncols = ncols, .owned_columns = ncols,
        .column_capacity = initial_capacity,
        .timestamp_capacity = timestamp_capacity,
        .active_table = ncols != 0, .scratch = governor && ncols != 0,
    };
    int rc;
    if (out)
        *out = NULL;
    if ((!name && !logical_copy) || !out)
        return EINVAL;
    wl_columnar_memory_reservation_init(&descriptor);
    wl_columnar_memory_reservation_init(&metadata);
    wl_columnar_memory_reservation_init(&payload);
    wl_columnar_memory_reservation_init(&scratch);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        goto done;
    if (name) {
        rc = wl_columnar_relation_name_length_controlled(name, &name_length,
                control);
        if (rc != 0)
            goto done;
    }
    if (!wl_columnar_memory_size_add(external_slot ? 0 : sizeof(*r),
        name_length, &descriptor_bytes)
        || descriptor_bytes > SIZE_MAX
        || !wl_columnar_memory_size_mul(ncols, sizeof(int64_t *),
        &column_table_bytes)
        || !wl_columnar_memory_size_mul(ncols, sizeof(char *),
        &name_table_bytes)
        || !wl_columnar_memory_size_mul(ncols, sizeof(uint64_t), &length_bytes)
        || !wl_columnar_memory_size_mul(ncols, sizeof(int64_t), &scratch_bytes)
        || !wl_columnar_memory_size_mul(initial_capacity ? initial_capacity :
        1u,
        sizeof(int64_t), &lane_bytes)
        || !wl_columnar_memory_size_mul(timestamp_capacity,
        sizeof(col_delta_timestamp_t), &timestamp_bytes)
        || column_table_bytes > SIZE_MAX || name_table_bytes > SIZE_MAX
        || length_bytes > SIZE_MAX || scratch_bytes > SIZE_MAX
        || lane_bytes > SIZE_MAX || timestamp_bytes > SIZE_MAX
        || !col_rel_payload_shape_bytes(&shape, &payload_bytes)) {
        rc = EOVERFLOW;
        goto done;
    }
    rc = wl_columnar_relation_constructor_reserve(governor, descriptor_bytes,
            &descriptor, control);
    if (rc != 0)
        goto done;
    if (external_slot) {
        r = external_slot;
        r->pool_owned = true;
    } else {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        r = wl_columnar_relation_constructor_allocation_fails() ? NULL
            : (col_rel_t *)calloc(1, sizeof(*r));
        if (!r) {
            rc = ENOMEM;
            goto done;
        }
    }
    /* Every token identity is valid before any private object cleanup. */
    wl_columnar_memory_reservation_init(&r->descriptor_reservation);
    wl_columnar_memory_reservation_init(&r->metadata_reservation);
    wl_columnar_memory_reservation_init(&r->retained_reservation);
    wl_columnar_memory_reservation_init(&r->pool_name_reservation);
    wl_columnar_memory_reservation_init(&r->dedup_reservation);
    r->memory_governor = governor;
    if (governor) {
        wl_columnar_memory_governor_ref_retain(governor);
        governor_retained = true;
    }
    if (!external_slot) {
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    if (name) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        r->name = wl_columnar_relation_constructor_allocation_fails() ? NULL
        : (char *)malloc((size_t)name_length);
        if (!r->name) {
            rc = ENOMEM;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    for (uint64_t pos = 0; pos < name_length;) {
        uint64_t n = name_length - pos;
        if (n > WL_EVALUATION_CONTROL_QUANTUM)
            n = WL_EVALUATION_CONTROL_QUANTUM;
        rc = wl_evaluation_control_charge(control, n);
        if (rc != 0)
            goto done;
        memcpy(r->name + pos, name + pos, (size_t)n);
        pos += n;
        wl_columnar_relation_constructor_checkpoint(control,
            WL_COLUMNAR_RELATION_CONSTRUCTOR_NAME, pos);
        rc = wl_columnar_relation_work_tail(control, n);
        if (rc != 0)
            goto done;
    }
    if (external_slot)
        r->relation_identity = external_identity;
    else {
        rc = wl_columnar_relation_identity_controlled(&r->relation_identity,
                control);
        if (rc != 0)
            goto done;
    }
    r->view_generation = 1;
    r->storage_generation = 1;
    col_rel_storage_owner_init(r);
    rc = wl_columnar_relation_constructor_commit(r, &descriptor,
            external_slot ? &r->pool_name_reservation :
            &r->descriptor_reservation,
            external_slot ? 4 : 1, control);
    if (rc != 0)
        goto done;
    if (external_slot)
        r->pool_name_reserved_bytes = governor ? descriptor_bytes : 0;
    else
        r->descriptor_reserved_bytes = governor ? descriptor_bytes : 0;
    r->ncols = ncols;
    r->capacity = initial_capacity;
    rc = wl_columnar_relation_constructor_reserve(governor, length_bytes,
            &scratch, control);
    if (rc != 0)
        goto done;
    if (ncols) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        lengths = wl_columnar_relation_constructor_allocation_fails() ? NULL
            : (uint64_t *)malloc((size_t)length_bytes);
        if (!lengths) {
            rc = ENOMEM;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    for (uint32_t c = 0; c < ncols; c++) {
        char generated[32];
        uint64_t length, fixed = sizeof(char *) + (with_schema
            ? sizeof(struct ArrowSchema *) + sizeof(struct ArrowSchema) + 2u
            : 0u);
        const char *column_name = NULL;
        if (source) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc != 0)
                goto done;
            column_name = source->col_names ? source->col_names[c] : NULL;
            wl_columnar_relation_constructor_checkpoint(control,
                WL_COLUMNAR_RELATION_CONSTRUCTOR_SOURCE_NAME, c + 1u);
            rc = wl_columnar_relation_work_tail(control, 1);
            if (rc != 0)
                goto done;
        }
        if (!column_name) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc != 0)
                goto done;
            int printed = snprintf(generated, sizeof(generated), "col%u", c);
            if (printed < 0 || (size_t)printed >= sizeof(generated)) {
                rc = EOVERFLOW;
                goto done;
            }
            rc = wl_columnar_relation_work_tail(control, 1);
            if (rc != 0)
                goto done;
            column_name = generated;
        }
        rc = wl_columnar_relation_name_length_controlled(column_name, &length,
                control);
        if (rc != 0)
            goto done;
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        lengths[c] = length;
        if (!wl_columnar_memory_size_add(metadata_bytes, fixed, &metadata_bytes)
            || !wl_columnar_memory_size_add(metadata_bytes, length,
            &metadata_bytes)
            || (with_schema && !wl_columnar_memory_size_add(metadata_bytes,
            length, &metadata_bytes))) {
            rc = EOVERFLOW;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    if (source && source->column_types && ncols) {
        if (!wl_columnar_memory_size_mul(ncols, sizeof(*r->column_types),
            &type_bytes) || type_bytes > SIZE_MAX
            || !wl_columnar_memory_size_add(metadata_bytes, type_bytes,
            &metadata_bytes)) {
            rc = EOVERFLOW;
            goto done;
        }
    }
    rc = wl_columnar_relation_constructor_reserve(governor, metadata_bytes,
            &metadata, control);
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_constructor_reserve(governor, payload_bytes,
            &payload, control);
    if (rc != 0)
        goto done;
    if (ncols) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        r->columns = wl_columnar_relation_constructor_allocation_fails() ? NULL
            : (int64_t **)malloc((size_t)column_table_bytes);
        if (!r->columns) {
            rc = ENOMEM;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
        for (uint32_t c = 0; c < ncols;) {
            uint32_t n = ncols - c;
            if (n > WL_EVALUATION_CONTROL_QUANTUM)
                n = (uint32_t)WL_EVALUATION_CONTROL_QUANTUM;
            rc = wl_evaluation_control_charge(control, n);
            if (rc != 0)
                goto done;
            for (uint32_t i = 0; i < n; i++)
                r->columns[c + i] = NULL;
            c += n;
            wl_columnar_relation_constructor_checkpoint(control,
                WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMNS_ZERO, c);
            rc = wl_columnar_relation_work_tail(control, n);
            if (rc != 0)
                goto done;
        }
        for (uint32_t c = 0; c < ncols; c++) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc != 0)
                goto done;
            r->columns[c] =
                wl_columnar_relation_constructor_allocation_fails() ? NULL
                : (int64_t *)malloc((size_t)lane_bytes);
            if (!r->columns[c]) {
                rc = ENOMEM;
                goto done;
            }
            columns_owned++;
            wl_columnar_relation_constructor_checkpoint(control,
                WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN, columns_owned);
            rc = wl_columnar_relation_work_tail(control, 1);
            if (rc != 0)
                goto done;
            if (logical_copy && source->nrows > 0) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                const int64_t *column = source->columns[c];
                if (!column) {
                    rc = EINVAL;
                    goto done;
                }
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
                wl_columnar_relation_constructor_checkpoint(control,
                    WL_COLUMNAR_RELATION_CONSTRUCTOR_CELLS, c);
                rc = wl_columnar_relation_copy_cells_controlled(r->columns[c],
                        column, source->nrows, false, control);
                if (rc != 0)
                    goto done;
            }
        }
        if (governor) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc != 0)
                goto done;
            r->row_scratch =
                wl_columnar_relation_constructor_allocation_fails() ? NULL
            : (int64_t *)malloc((size_t)scratch_bytes);
            if (!r->row_scratch) {
                rc = ENOMEM;
                goto done;
            }
            rc = wl_columnar_relation_work_tail(control, 1);
            if (rc != 0)
                goto done;
        }
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        r->col_names =
            wl_columnar_relation_constructor_allocation_fails() ? NULL
            : (char **)malloc((size_t)name_table_bytes);
        if (!r->col_names) {
            rc = ENOMEM;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
        for (uint32_t c = 0; c < ncols;) {
            uint32_t n = ncols - c;
            if (n > WL_EVALUATION_CONTROL_QUANTUM)
                n = (uint32_t)WL_EVALUATION_CONTROL_QUANTUM;
            rc = wl_evaluation_control_charge(control, n);
            if (rc != 0)
                goto done;
            for (uint32_t i = 0; i < n; i++)
                r->col_names[c + i] = NULL;
            c += n;
            wl_columnar_relation_constructor_checkpoint(control,
                WL_COLUMNAR_RELATION_CONSTRUCTOR_NAMES_ZERO, c);
            rc = wl_columnar_relation_work_tail(control, n);
            if (rc != 0)
                goto done;
        }
        for (uint32_t c = 0; c < ncols; c++) {
            char generated[32];
            const char *column_name = NULL;
            if (source) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                column_name = source->col_names ? source->col_names[c] : NULL;
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
            }
            if (!column_name) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                int printed = snprintf(generated, sizeof(generated), "col%u",
                        c);
                if (printed < 0 || (size_t)printed + 1u != lengths[c]) {
                    rc = EOVERFLOW;
                    goto done;
                }
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
                column_name = generated;
            }
            rc = wl_evaluation_control_charge(control, 1);
            if (rc != 0)
                goto done;
            r->col_names[c] =
                wl_columnar_relation_constructor_allocation_fails() ? NULL
                : (char *)malloc((size_t)lengths[c]);
            if (!r->col_names[c]) {
                rc = ENOMEM;
                goto done;
            }
            names_owned++;
            rc = wl_columnar_relation_work_tail(control, 1);
            if (rc != 0)
                goto done;
            for (uint64_t copied = 0; copied < lengths[c];) {
                uint64_t n = lengths[c] - copied;
                if (n > WL_EVALUATION_CONTROL_QUANTUM)
                    n = WL_EVALUATION_CONTROL_QUANTUM;
                rc = wl_evaluation_control_charge(control, n);
                if (rc != 0)
                    goto done;
                memcpy(r->col_names[c] + copied, column_name + copied,
                    (size_t)n);
                copied += n;
                wl_columnar_relation_constructor_checkpoint(control,
                    WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN_NAME_COPY,
                    copied);
                rc = wl_columnar_relation_work_tail(control, n);
                if (rc != 0)
                    goto done;
            }
            wl_columnar_relation_constructor_checkpoint(control,
                WL_COLUMNAR_RELATION_CONSTRUCTOR_GENERATED_NAME, names_owned);
            rc = wl_evaluation_control_charge(control, 0);
            if (rc != 0)
                goto done;
        }
    }
    if (type_bytes) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        r->column_types = wl_columnar_relation_constructor_allocation_fails()
            ? NULL : (wirelog_column_type_t *)malloc((size_t)type_bytes);
        if (!r->column_types) {
            rc = ENOMEM;
            goto done;
        }
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
        for (uint32_t c = 0; c < ncols;) {
            uint32_t n = ncols - c;
            if (n > WL_EVALUATION_CONTROL_QUANTUM)
                n = (uint32_t)WL_EVALUATION_CONTROL_QUANTUM;
            rc = wl_evaluation_control_charge(control, n);
            if (rc != 0)
                goto done;
            for (uint32_t i = 0; i < n; i++)
                r->column_types[c + i] = source->column_types[c + i];
            c += n;
            wl_columnar_relation_constructor_checkpoint(control,
                WL_COLUMNAR_RELATION_CONSTRUCTOR_TYPES, c);
            rc = wl_columnar_relation_work_tail(control, n);
            if (rc != 0)
                goto done;
        }
    }
    if (timestamp_bytes) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc != 0)
            goto done;
        r->timestamps =
            wl_columnar_relation_constructor_allocation_fails() ? NULL
            : logical_copy
            ? (col_delta_timestamp_t *)malloc((size_t)timestamp_bytes)
            : (col_delta_timestamp_t *)calloc(timestamp_capacity,
                sizeof(*r->timestamps));
        if (!r->timestamps) {
            rc = ENOMEM;
            goto done;
        }
        r->timestamp_capacity = timestamp_capacity;
        rc = wl_columnar_relation_work_tail(control, 1);
        if (rc != 0)
            goto done;
    }
    if (logical_copy && timestamp_capacity) {
        wl_columnar_relation_constructor_checkpoint(control,
            WL_COLUMNAR_RELATION_CONSTRUCTOR_TIMESTAMP_LIVE, 0);
        rc = wl_columnar_relation_copy_timestamps_controlled(r->timestamps,
                source->timestamps, source->nrows, control);
        if (rc != 0)
            goto done;
        wl_columnar_relation_constructor_checkpoint(control,
            WL_COLUMNAR_RELATION_CONSTRUCTOR_TIMESTAMP_ZERO, 0);
        for (uint32_t row = source->nrows; row < timestamp_capacity;) {
            uint32_t n = timestamp_capacity - row;
            if (n > WL_EVALUATION_CONTROL_QUANTUM / 5u)
                n = (uint32_t)(WL_EVALUATION_CONTROL_QUANTUM / 5u);
            rc = wl_evaluation_control_charge(control, (uint64_t)n * 5u);
            if (rc != 0)
                goto done;
            for (uint32_t i = 0; i < n; i++) {
                r->timestamps[row + i].iteration = 0;
                r->timestamps[row + i].stratum = 0;
                r->timestamps[row + i].worker = 0;
                r->timestamps[row + i]._reserved = 0;
                r->timestamps[row + i].multiplicity = 0;
            }
            row += n;
            rc = wl_columnar_relation_work_tail(control, (uint64_t)n * 5u);
            if (rc != 0)
                goto done;
        }
    }
    if (with_schema) {
        rc = wl_columnar_relation_build_schema_controlled(r, lengths,
                r->column_types, &r->schema, control);
        if (rc != 0)
            goto done;
        r->schema_ok = true;
    }
    if (logical_copy) {
        rc = wl_evaluation_control_charge(control, 4);
        if (rc != 0)
            goto done;
        r->nrows = source->nrows;
        r->base_nrows = source->base_nrows;
        r->sorted_nrows = source->sorted_nrows <= source->nrows
            ? source->sorted_nrows : 0;
        bool valid = source->run_count <= COL_MAX_RUNS;
        uint32_t previous = 0;
        rc = wl_columnar_relation_work_tail(control, 4);
        if (rc != 0)
            goto done;
        if (valid) {
            for (uint32_t i = 0; i < source->run_count; i++) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc != 0)
                    goto done;
                uint32_t end = source->run_ends[i];
                if (end <= previous || end > source->nrows)
                    valid = false;
                previous = end;
                wl_columnar_relation_constructor_checkpoint(control,
                    WL_COLUMNAR_RELATION_CONSTRUCTOR_RUN_VALIDATE, i + 1u);
                rc = wl_columnar_relation_work_tail(control, 1);
                if (rc != 0)
                    goto done;
                if (!valid)
                    break;
            }
            if (source->run_count && previous != source->nrows)
                valid = false;
        }
        rc = wl_evaluation_control_charge(control, COL_MAX_RUNS);
        if (rc != 0)
            goto done;
        r->run_count = valid ? source->run_count : 0;
        for (uint32_t i = 0; i < COL_MAX_RUNS; i++)
            r->run_ends[i] = valid ? source->run_ends[i] : 0;
        if (!valid)
            r->sorted_nrows = 0;
        wl_columnar_relation_constructor_checkpoint(control,
            WL_COLUMNAR_RELATION_CONSTRUCTOR_RUN_COPY, COL_MAX_RUNS);
        rc = wl_columnar_relation_work_tail(control, COL_MAX_RUNS);
        if (rc != 0)
            goto done;
    }
    rc = wl_columnar_relation_constructor_commit(r, &metadata,
            &r->metadata_reservation, 2, control);
    if (rc != 0)
        goto done;
    r->metadata_reserved_bytes = governor ? metadata_bytes : 0;
    rc = wl_columnar_relation_constructor_commit(r, &payload,
            &r->retained_reservation, 3, control);
    if (rc != 0)
        goto done;
    r->retained_reserved_bytes = governor ? payload_bytes : 0;
    free(lengths);
    lengths = NULL;
    wl_columnar_relation_constructor_retire_token(&scratch);
    wl_columnar_memory_reservation_init(&scratch);
    if (!logical_copy)
        wl_columnar_relation_touch_view(r);
    if (source) {
        if (!logical_copy && source->column_types)
            wl_columnar_relation_touch_view(r);
        if (!logical_copy && timestamps)
            wl_columnar_relation_touch_storage(r);
        r->declared_ncols = source->declared_ncols;
        r->has_graph_column = source->has_graph_column;
        r->graph_col_idx = source->graph_col_idx;
    }
    wl_columnar_relation_constructor_checkpoint(control,
        WL_COLUMNAR_RELATION_CONSTRUCTOR_FINAL, 0);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        goto done;
    *out = r;
    return 0;
done:
    free(lengths);
    if (r) {
        /* Never generic-destroy a partial object. Prefix counters are authority,
        * including while table entries outside that prefix are uninitialized. */
        for (uint32_t c = 0; c < columns_owned; c++)
            free(r->columns[c]);
        for (uint32_t c = 0; c < names_owned; c++)
            free(r->col_names[c]);
        free(r->columns);
        free(r->col_names);
        free(r->column_types);
        free(r->row_scratch);
        free(r->timestamps);
        free(r->name);
        if (r->schema_ok)
            ArrowSchemaRelease(&r->schema);
        /* Keep every committed token reachable after descriptor retirement. */
        if (r->descriptor_reservation.bytes
            && !wl_columnar_memory_reservation_move(&descriptor,
            &r->descriptor_reservation))
            abort();
        if (r->pool_name_reservation.bytes
            && !wl_columnar_memory_reservation_move(&descriptor,
            &r->pool_name_reservation))
            abort();
        if (r->metadata_reservation.bytes
            && !wl_columnar_memory_reservation_move(&metadata,
            &r->metadata_reservation))
            abort();
        if (r->retained_reservation.bytes
            && !wl_columnar_memory_reservation_move(&payload,
            &r->retained_reservation))
            abort();
        if (!external_slot)
            free(r);
    }
    wl_columnar_relation_constructor_retire_token(&scratch);
    wl_columnar_relation_constructor_retire_token(&payload);
    wl_columnar_relation_constructor_retire_token(&metadata);
    wl_columnar_relation_constructor_retire_token(&descriptor);
    if (governor_retained)
        wl_columnar_memory_governor_ref_release(governor);
    return rc;
}

int
wl_columnar_relation_new_auto_governed_controlled(const char *name,
    uint32_t ncols, uint32_t initial_capacity, bool timestamps,
    wl_columnar_memory_governor_ref_t *governor, col_rel_t **out,
    wl_evaluation_control_t *control)
{
    if (!control)
        return wl_columnar_relation_new_auto_governed(name, ncols,
                   initial_capacity, timestamps, governor, out);
    if (out)
        *out = NULL;
    if (!governor)
        return EINVAL;
    return wl_columnar_relation_new_private_controlled(name, ncols,
               initial_capacity, timestamps, governor, NULL, false, NULL, 0,
               out,
               control);
}

/* Helper: create owned relation copying col_names from src.
 *
 * Issue #1140: a NULL src is rejected rather than dereferenced.  Several
 * callers obtain their template from a lookup that can miss and check only
 * the returned relation, so failing here is what makes that check
 * sufficient. */
static int
wl_columnar_relation_new_like_checked_impl(col_rel_t **out,
    const char *name, const col_rel_t *src,
    wl_columnar_memory_governor_ref_t *governor, bool preserve_metadata)
{
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_t *r = NULL;
    int rc;

    if (!out)
        return EINVAL;
    *out = NULL;
    if (!name || !src)
        return EINVAL;
    rc = col_rel_source_reader_acquire(src, &reader);
    if (rc != 0)
        return rc;
    if (!wl_columnar_relation_compound_map_valid(src)
        || (src->has_graph_column && src->graph_col_idx >= src->ncols)) {
        rc = EINVAL;
        goto done;
    }
    rc = governor ? wl_columnar_relation_alloc_governed(&r, name, governor)
        : col_rel_alloc(&r, name);
    if (rc != 0)
        goto done;
    rc = col_rel_set_schema(r, src->ncols,
            (const char *const *)src->col_names);
    if (rc != 0) {
        if (rc == ENOMEM && r->memory_budget_denial_pending)
            rc = ENOSPC;
        goto done;
    }
    if (src->column_types) {
        rc = col_rel_set_column_types(r, src->column_types, src->ncols);
        if (rc != 0) {
            if (rc == ENOMEM && r->memory_budget_denial_pending)
                rc = ENOSPC;
            goto done;
        }
    }
    /* Issue #535: inherit graph-column metadata so deltas/clones route by
     * __graph_id consistently with their source relation. */
    r->has_graph_column = src->has_graph_column;
    r->graph_col_idx = src->graph_col_idx;
    r->declared_ncols = src->declared_ncols;
    /* Required metadata cannot be silently dropped: that would change
     * interpretation of the same physical columns. Deep-copy retains its
     * separate historical contract. */
    if (src->compound_arity_len > 0) {
        int clone_rc = col_rel_clone_compound_meta(r, src, false);
        if (clone_rc != 0) {
            rc = clone_rc == -1 ? EINVAL
                : clone_rc == -3 ? EOVERFLOW
                : r->memory_budget_denial_pending ? ENOSPC : ENOMEM;
            goto done;
        }
    }
    if (preserve_metadata && src->timestamps) {
        rc = col_rel_enable_timestamps(r);
        if (rc != 0) {
            if (rc == ENOMEM && r->memory_budget_denial_pending)
                rc = ENOSPC;
            goto done;
        }
    }
    rc = 0;
done:
    if (col_rel_source_reader_release(&reader) != 0 && rc == 0)
        rc = EBUSY;
    if (rc == 0)
        *out = r;
    else
        col_rel_destroy(r);
    return rc;
}

col_rel_t *
col_rel_new_like(const char *name, const col_rel_t *src)
{
    col_rel_t *r = NULL;
    (void)wl_columnar_relation_new_like_checked_impl(&r, name, src, NULL,
        false);
    return r;
}

col_rel_t *
wl_columnar_relation_new_like_governed(const char *name, const col_rel_t *src,
    wl_columnar_memory_governor_ref_t *governor)
{
    col_rel_t *r = NULL;
    (void)wl_columnar_relation_new_like_governed_checked(&r, name, src,
        governor);
    return r;
}

int
wl_columnar_relation_new_like_governed_checked(col_rel_t **out,
    const char *name, const col_rel_t *src,
    wl_columnar_memory_governor_ref_t *governor)
{
    return wl_columnar_relation_new_like_checked_impl(out, name, src,
               governor, true);
}

int
wl_columnar_relation_new_like_governed_checked_mode(const char *name,
    const col_rel_t *src, wl_columnar_memory_governor_ref_t *governor,
    bool preserve_timestamps, col_rel_t **out)
{
    return wl_columnar_relation_new_like_checked_impl(out, name, src,
               governor, preserve_timestamps);
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_relation_test_new_like_before_reader_release)(
    wl_evaluation_control_t *, wl_columnar_source_access_reader_t *);
#endif

/* Empty checked construction borrows both descriptor and canonical owner.
 * Source rows are deliberately irrelevant: only names/types/schema metadata is
 * read, and even FLOAT metadata must not trigger source cell validation. */
int
wl_columnar_relation_new_like_governed_checked_mode_controlled(const char *name,
    const col_rel_t *src, wl_columnar_memory_governor_ref_t *governor,
    bool preserve_timestamps, col_rel_t **out, wl_evaluation_control_t *control)
{
    wl_columnar_relation_source_admission_t admission = { 0 };
    col_rel_t *r = NULL;
    int rc;
    if (!control)
        return wl_columnar_relation_new_like_governed_checked_mode(name, src,
                   governor, preserve_timestamps, out);
    if (!out)
        return EINVAL;
    *out = NULL;
    if (!name || !src)
        return EINVAL;
    rc = wl_columnar_relation_source_acquire_controlled(src, &admission,
            control);
    if (rc != 0)
        return rc;
    rc = wl_evaluation_control_charge(control, 6);
    if (rc != 0)
        goto done;
    /* No wide compound map scan before its later controlled support unit. */
    if ((src->compound_arity_map == NULL) != (src->compound_arity_len == 0)
        || src->compound_arity_len > src->ncols
        || (src->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        && src->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        && src->compound_kind != WIRELOG_COMPOUND_KIND_SIDE)
        || (src->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && (src->compound_count || src->inline_physical_offset))
        || (src->has_graph_column && src->graph_col_idx >= src->ncols)) {
        rc = EINVAL;
        goto done;
    }
    if (src->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || src->compound_arity_map || src->compound_arity_len) {
        rc = ENOTSUP;
        goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 6);
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_new_private_controlled(name, src->ncols,
            COL_REL_INIT_CAP, preserve_timestamps && src->timestamps, governor,
            src, false, NULL, 0, &r, control);
done:
    rc = wl_columnar_relation_source_finish(&admission, rc, control, true);
    if (rc == 0)
        *out = r;
    else
        col_rel_destroy(r);
    return rc;
}

/* Caller retains grouped descriptor/owner authority through its operator
 * cutoff. Building this private image never finalizes the source journal. */
int
wl_columnar_relation_new_like_held_controlled(const char *name,
    const col_rel_t *src, wl_columnar_memory_governor_ref_t *governor,
    col_rel_t **out, wl_evaluation_control_t *control)
{
    if (!name || !src || !out || !control)
        return EINVAL;
    *out = NULL;
    int rc = wl_evaluation_control_charge(control, 16);
    if (rc)
        return rc;
    if (src->has_graph_column && src->graph_col_idx >= src->ncols)
        return EINVAL;
    rc = wl_columnar_relation_work_tail(control, 16);
    if (rc)
        return rc;
    return wl_columnar_relation_new_private_controlled(name, src->ncols,
               COL_REL_INIT_CAP, false, governor, src, false, NULL, 0,
               out, control);
}

int
wl_columnar_relation_new_like_governed_checked_controlled(col_rel_t **out,
    const char *name, const col_rel_t *src,
    wl_columnar_memory_governor_ref_t *governor,
    wl_evaluation_control_t *control)
{
    return wl_columnar_relation_new_like_governed_checked_mode_controlled(name,
               src, governor, true, out, control);
}

/* Pool-aware col_rel constructor wrappers.
 *
 * The struct slot is allocated from the pool slab (O(1), no free needed).
 * Data buffers and col_names are still heap-allocated so that realloc in
 * col_rel_append_row remains safe and col_names are available for column
 * lookup (col_rel_col_idx).  The pool_owned flag tells col_rel_destroy to
 * skip free() on the struct itself while still freeing heap-allocated
 * internals. */
static col_rel_t *
col_rel_pool_fallback_auto(delta_pool_t *pool, col_rel_t *r,
    const char *name, uint32_t ncols)
{
    col_rel_free_contents(r);
    if (pool->slot_used > 0)
        pool->slot_used--;
    return col_rel_new_auto(name, ncols);
}

int
wl_columnar_relation_pool_new_like_governed_checked(col_rel_t **out,
    delta_pool_t *pool, const char *name, const col_rel_t *like,
    wl_columnar_memory_governor_ref_t *governor)
{
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_memory_reservation_t name_pending;
    col_rel_t *r = NULL;
    col_rel_t *fallback = NULL;
    int rc;

    if (!out)
        return EINVAL;
    *out = NULL;
    if (!name || !like)
        return EINVAL;
    if (!pool || pool->slot_used >= pool->slot_cap)
        return wl_columnar_relation_new_like_checked_impl(out, name, like,
                   governor, false);
    rc = col_rel_source_reader_acquire(like, &reader);
    if (rc != 0)
        return rc;
    if (!wl_columnar_relation_compound_map_valid(like)
        || (like->has_graph_column && like->graph_col_idx >= like->ncols)) {
        rc = EINVAL;
        goto done;
    }
    /* Reserve the identity before consuming a slab slot.  The allocator is
     * deliberately non-wrapping; exhaustion is a hard rejection and must
     * not make a subsequent valid pool allocation appear exhausted. */
    uint64_t identity = 0;
    if (col_rel_new_identity(&identity) != 0) {
        rc = EOVERFLOW;
        goto done;
    }
    r = (col_rel_t *)delta_pool_alloc_slot(pool);
    if (!r) {
        rc = wl_columnar_relation_new_like_checked_impl(&fallback, name, like,
                governor, false);
        goto done;
    }
    r->pool_owned = true;
    r->relation_identity = identity;
    r->view_generation = 1u;
    r->storage_generation = 1u;
    col_rel_storage_owner_init(r);
    wl_columnar_memory_reservation_init(&r->descriptor_reservation);
    wl_columnar_memory_reservation_init(&r->metadata_reservation);
    wl_columnar_memory_reservation_init(&r->pool_name_reservation);
    wl_columnar_memory_reservation_init(&r->dedup_reservation);
    wl_columnar_memory_reservation_init(&r->retained_reservation);
    if (governor) {
        rc = col_rel_attach_memory_governor(r, governor);
        if (rc != 0)
            goto fail_slot;
    }
    wl_columnar_memory_reservation_init(&name_pending);
    if (governor) {
        size_t name_len = strlen(name);
        if (name_len == SIZE_MAX) {
            rc = EOVERFLOW;
            goto fail_slot;
        }
        uint64_t name_bytes = (uint64_t)name_len + 1u;
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(governor),
                name_bytes, &name_pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
            if (rc == ENOSPC)
                r->memory_budget_denial_pending = true;
            goto fail_slot;
        }
    }
    r->name = wl_strdup(name);
    if (!r->name) {
        rc = ENOMEM;
        goto fail_name;
    }
    if (governor) {
#ifdef WL_SESSION_TEST_HOOKS
        if (name_pending.bytes
            && wl_columnar_relation_test_constructor_fail_commit == 4) {
            wl_columnar_relation_test_constructor_fail_commit = 0;
            rc = EINVAL;
            goto fail_name;
        }
#endif
        if (!wl_columnar_memory_reservation_move(
                &r->pool_name_reservation, &name_pending)
            || !wl_columnar_memory_commit(&r->pool_name_reservation,
            r)) {
            rc = EINVAL;
            goto fail_name;
        }
        r->pool_name_reserved_bytes = (uint64_t)strlen(name) + 1u;
    }
    rc = col_rel_set_schema(r, like->ncols,
            (const char *const *)like->col_names);
    if (rc != 0) {
        if (rc == ENOMEM && r->memory_budget_denial_pending)
            rc = ENOSPC;
        goto fail_slot;
    }
    r->declared_ncols = like->declared_ncols;
    r->has_graph_column = like->has_graph_column;
    r->graph_col_idx = like->graph_col_idx;
    if (like->column_types) {
        rc = col_rel_set_column_types(r, like->column_types,
                like->ncols);
        if (rc != 0) {
            if (rc == ENOMEM && r->memory_budget_denial_pending)
                rc = ENOSPC;
            goto fail_slot;
        }
    }
    if (like->compound_arity_len > 0) {
        int clone_rc = col_rel_clone_compound_meta(r, like, false);
        if (clone_rc != 0) {
            rc = clone_rc == -1 ? EINVAL
                : clone_rc == -3 ? EOVERFLOW
                    : r->memory_budget_denial_pending ? ENOSPC : ENOMEM;
            goto fail_slot;
        }
    }
    r->nrows = 0;
    rc = 0;
    goto done;
fail_name:
    col_rel_reservation_rollback(&name_pending);
    col_rel_reservation_rollback(&r->pool_name_reservation);
fail_slot:
    col_rel_free_contents(r);
    pool->slot_used--;
    r = NULL;
done:
    if (col_rel_source_reader_release(&reader) != 0 && rc == 0)
        rc = EBUSY;
    if (rc == 0) {
        *out = r ? r : fallback;
    } else {
        if (r) {
            col_rel_free_contents(r);
            pool->slot_used--;
        }
        col_rel_destroy(fallback);
    }
    return rc;
}

/* Private constructor owns contents only; this wrapper owns the slab slot.
 * The pool must be exclusively and nonreentrantly owned during preparation.
 * A defensive lost-top tombstone never retires a later acquired descriptor. */
static void
wl_columnar_relation_pool_retire_private_slot(delta_pool_t *pool,
    uint32_t slot_index, col_rel_t *slot, bool complete)
{
    if (complete)
        col_rel_free_contents(slot);
    /* Mandatory private cleanup, not preparation work: all physical heap
     * contents/tokens have already been freed/retired before tombstoning. */
    memset(slot, 0, sizeof(*slot));
    wl_columnar_memory_reservation_init(&slot->descriptor_reservation);
    wl_columnar_memory_reservation_init(&slot->pool_name_reservation);
    wl_columnar_memory_reservation_init(&slot->metadata_reservation);
    wl_columnar_memory_reservation_init(&slot->retained_reservation);
    wl_columnar_memory_reservation_init(&slot->dedup_reservation);
    if (pool->slot_used == slot_index + 1u
        && pool->slab + (size_t)slot_index * pool->slot_size == (char *)slot)
        pool->slot_used = slot_index;
}

int
wl_columnar_relation_pool_new_like_governed_checked_controlled(col_rel_t **out,
    delta_pool_t *pool, const char *name, const col_rel_t *like,
    wl_columnar_memory_governor_ref_t *governor,
    wl_evaluation_control_t *control)
{
    wl_columnar_relation_source_admission_t admission = { 0 };
    col_rel_t *slot = NULL, *r = NULL;
    uint32_t slot_index = 0;
    uint64_t identity = 0;
    int rc;
    if (!control)
        return wl_columnar_relation_pool_new_like_governed_checked(out, pool,
                   name, like, governor);
    if (!out)
        return EINVAL;
    *out = NULL;
    if (!name || !like)
        return EINVAL;
    if (!pool)
        return wl_columnar_relation_new_like_governed_checked_mode_controlled(
            name, like, governor, false, out, control);
    rc = wl_evaluation_control_charge(control, 4);
    if (rc != 0)
        return rc;
#ifdef _MSC_VER
    const size_t alignment = __alignof(col_rel_t);
#else
    const size_t alignment = _Alignof(col_rel_t);
#endif
    if (!pool->slab || pool->slot_used > pool->slot_cap
        || pool->slot_size < sizeof(col_rel_t))
        return EINVAL;
    if (pool->slot_cap && pool->slot_size > SIZE_MAX / pool->slot_cap)
        return EOVERFLOW;
    if ((uintptr_t)pool->slab % alignment || pool->slot_size % alignment)
        return EINVAL;
    rc = wl_columnar_relation_work_tail(control, 4);
    if (rc != 0)
        return rc;
    if (pool->slot_used == pool->slot_cap)
        return wl_columnar_relation_new_like_governed_checked_mode_controlled(
            name, like, governor, false, out, control);
    rc = wl_columnar_relation_source_acquire_controlled(like, &admission,
            control);
    if (rc != 0)
        return rc;
    rc = wl_evaluation_control_charge(control, 6);
    if (rc != 0)
        goto done;
    if ((like->compound_arity_map == NULL) != (like->compound_arity_len == 0)
        || like->compound_arity_len > like->ncols
        || (like->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        && like->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        && like->compound_kind != WIRELOG_COMPOUND_KIND_SIDE)
        || (like->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && (like->compound_count || like->inline_physical_offset))
        || (like->has_graph_column && like->graph_col_idx >= like->ncols)) {
        rc = EINVAL;
        goto done;
    }
    if (like->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || like->compound_arity_map || like->compound_arity_len) {
        rc = ENOTSUP;
        goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 6);
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_identity_controlled(&identity, control);
    if (rc != 0)
        goto done;
    slot_index = pool->slot_used;
    char *slot_bytes = pool->slab + (size_t)slot_index * pool->slot_size;
    /* Do not read any slot atomic/pointer until every byte is initialized.
    * During this unused-slot stage hooks may only request cancellation. */
    for (size_t zeroed = 0; zeroed < pool->slot_size;) {
        size_t n = pool->slot_size - zeroed;
        if (n > WL_EVALUATION_CONTROL_QUANTUM)
            n = (size_t)WL_EVALUATION_CONTROL_QUANTUM;
        rc = wl_evaluation_control_charge(control, n);
        if (rc != 0)
            goto done;
        memset(slot_bytes + zeroed, 0, n);
        zeroed += n;
        wl_columnar_relation_constructor_checkpoint(control,
            WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ZERO, zeroed);
        rc = wl_columnar_relation_work_tail(control, n);
        if (rc != 0)
            goto done;
    }
    slot = (col_rel_t *)slot_bytes;
    pool->slot_used++;
    wl_columnar_relation_constructor_checkpoint(control,
        WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ACQUIRED, slot_index);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_new_private_controlled(name, like->ncols,
            COL_REL_INIT_CAP, false, governor, like, false, slot, identity, &r,
            control);
done:
    rc = wl_columnar_relation_source_finish(&admission, rc, control, true);
    if (rc == 0)
        *out = r;
    else if (slot)
        wl_columnar_relation_pool_retire_private_slot(pool, slot_index, slot,
            r != NULL);
    return rc;
}

int
wl_columnar_relation_pool_new_like_held_controlled(col_rel_t **out,
    delta_pool_t *pool, const char *name, const col_rel_t *like,
    wl_columnar_memory_governor_ref_t *governor,
    wl_evaluation_control_t *control)
{
    col_rel_t *slot = NULL, *r = NULL;
    uint32_t slot_index = 0;
    uint64_t identity = 0;
    int rc;
    if (!control) return EINVAL;
    if (!out)
        return EINVAL;
    *out = NULL;
    if (!name || !like)
        return EINVAL;
    if (!pool)
        return wl_columnar_relation_new_like_held_controlled(
            name, like, governor, out, control);
    rc = wl_evaluation_control_charge(control, 4);
    if (rc != 0)
        return rc;
#ifdef _MSC_VER
    const size_t alignment = __alignof(col_rel_t);
#else
    const size_t alignment = _Alignof(col_rel_t);
#endif
    if (!pool->slab || pool->slot_used > pool->slot_cap
        || pool->slot_size < sizeof(col_rel_t))
        return EINVAL;
    if (pool->slot_cap && pool->slot_size > SIZE_MAX / pool->slot_cap)
        return EOVERFLOW;
    if ((uintptr_t)pool->slab % alignment || pool->slot_size % alignment)
        return EINVAL;
    rc = wl_columnar_relation_work_tail(control, 4);
    if (rc != 0)
        return rc;
    if (pool->slot_used == pool->slot_cap)
        return wl_columnar_relation_new_like_held_controlled(
            name, like, governor, out, control);
    rc = wl_evaluation_control_charge(control, 6);
    if (rc != 0)
        goto done;
    if ((like->compound_arity_map == NULL) != (like->compound_arity_len == 0)
        || like->compound_arity_len > like->ncols
        || (like->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        && like->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        && like->compound_kind != WIRELOG_COMPOUND_KIND_SIDE)
        || (like->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && (like->compound_count || like->inline_physical_offset))
        || (like->has_graph_column && like->graph_col_idx >= like->ncols)) {
        rc = EINVAL;
        goto done;
    }
    if (like->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || like->compound_arity_map || like->compound_arity_len) {
        rc = ENOTSUP;
        goto done;
    }
    rc = wl_columnar_relation_work_tail(control, 6);
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_identity_controlled(&identity, control);
    if (rc != 0)
        goto done;
    slot_index = pool->slot_used;
    char *slot_bytes = pool->slab + (size_t)slot_index * pool->slot_size;
    /* Do not read any slot atomic/pointer until every byte is initialized.
    * During this unused-slot stage hooks may only request cancellation. */
    for (size_t zeroed = 0; zeroed < pool->slot_size;) {
        size_t n = pool->slot_size - zeroed;
        if (n > 128u)
            n = 128u;
        rc = wl_evaluation_control_charge(control, 8);
        if (rc != 0)
            goto done;
        rc = wl_evaluation_control_charge(control, n);
        if (rc != 0)
            goto done;
        memset(slot_bytes + zeroed, 0, n);
        zeroed += n;
        wl_columnar_relation_constructor_checkpoint(control,
            WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ZERO, zeroed);
        rc = wl_columnar_relation_work_tail(control, n);
        if (rc != 0)
            goto done;
    }
    slot = (col_rel_t *)slot_bytes;
    pool->slot_used++;
    wl_columnar_relation_constructor_checkpoint(control,
        WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ACQUIRED, slot_index);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_new_private_controlled(name, like->ncols,
            COL_REL_INIT_CAP, false, governor, like, false, slot, identity, &r,
            control);
done:
    if (rc == 0)
        *out = r;
    else if (slot)
        wl_columnar_relation_pool_retire_private_slot(pool, slot_index, slot,
            r != NULL);
    return rc;
}

col_rel_t *
wl_columnar_relation_pool_new_like_governed(delta_pool_t *pool,
    const char *name, const col_rel_t *like,
    wl_columnar_memory_governor_ref_t *governor)
{
    col_rel_t *r = NULL;
    (void)wl_columnar_relation_pool_new_like_governed_checked(&r, pool,
        name, like, governor);
    return r;
}

col_rel_t *
col_rel_pool_new_like(delta_pool_t *pool, const char *name,
    const col_rel_t *like)
{
    return wl_columnar_relation_pool_new_like_governed(pool, name, like, NULL);
}

col_rel_t *
col_rel_pool_new_auto(delta_pool_t *pool, wl_arena_t *arena,
    const char *name, uint32_t ncols)
{
    if (!pool)
        return col_rel_new_auto(name, ncols); /* Fallback */
    if (pool->slot_used < pool->slot_cap) {
        uint64_t identity = 0;
        if (col_rel_new_identity(&identity) != 0)
            return NULL;
        col_rel_t *r = (col_rel_t *)delta_pool_alloc_slot(pool);
        if (!r)
            return col_rel_new_auto(name, ncols);
        r->pool_owned = true;
        r->relation_identity = identity;
        r->view_generation = 1u;
        r->storage_generation = 1u;
        col_rel_storage_owner_init(r);
        wl_columnar_memory_reservation_init(&r->pool_name_reservation);
        wl_columnar_memory_reservation_init(&r->dedup_reservation);
        wl_columnar_memory_reservation_init(&r->retained_reservation);
        r->name = wl_strdup(name);
        if (!r->name) {
            return col_rel_pool_fallback_auto(pool, r, name, ncols);
        }
        r->ncols = ncols;
        r->capacity = COL_REL_INIT_CAP;
        if (ncols > 0) {
            bool use_arena = false;
            if (arena) {
                r->columns = (int64_t **)calloc(ncols,
                        sizeof(int64_t *));
                if (r->columns) {
                    bool ok = true;
                    for (uint32_t c = 0; c < ncols; c++) {
                        r->columns[c] = (int64_t *)wl_arena_alloc(arena,
                                (size_t)r->capacity * sizeof(int64_t));
                        if (!r->columns[c]) {
                            ok = false;
                            break;
                        }
                    }
                    if (ok) {
                        use_arena = true;
                        r->arena_owned = true;
                    } else {
                        free((void *)r->columns);
                        r->columns = NULL;
                    }
                }
            }
            if (!use_arena) {
                r->columns = col_columns_alloc(ncols, r->capacity);
                r->arena_owned = false;
            }
            if (!r->columns) {
                return col_rel_pool_fallback_auto(pool, r, name, ncols);
            }
            r->col_names = (char **)calloc(ncols, sizeof(char *));
            if (!r->col_names)
                return col_rel_pool_fallback_auto(pool, r, name, ncols);
            for (uint32_t i = 0; i < ncols; i++) {
                char buf[32];
                snprintf(buf, sizeof(buf), "col%u", i);
                r->col_names[i] = wl_strdup(buf);
                if (!r->col_names[i])
                    return col_rel_pool_fallback_auto(pool, r, name, ncols);
            }
        }
        if (wl_columnar_relation_init_arrow_schema(r) != 0)
            return col_rel_pool_fallback_auto(pool, r, name, ncols);
        r->nrows = 0;
        return r;
    }
    return col_rel_new_auto(name, ncols);
}

/* ---- deep copy ----------------------------------------------------------- */

/**
 * @brief Produce a fully-owned deep copy of a col_rel_t (Issue #553).
 *
 * Allocates a new heap-resident relation and clones every owned buffer
 * and metadata field so the result is byte-equivalent to @p src yet
 * shares no storage with it.  Mutating either side after the call (data
 * appends, schema upgrades, retraction, consolidation) leaves the other
 * untouched.  This is the K-Fusion isolation primitive: callers that
 * need to fork a relation across worker boundaries should use this in
 * preference to col_rel_new_like + replay.
 *
 * Field-group taxonomy (per #553 body):
 *   - Groups A/B: columns, col_names, ArrowSchema -- deep copied.  The
 *     ArrowSchema is rebuilt through the same flat-schema initializer as
 *     col_rel_set_schema. The
 *     schema struct is NEVER bit-copied because the release callback /
 *     private_data would alias and trigger a double-release on destroy.
 *   - Group C: timestamps array sized to capacity (not nrows).
 *   - Group D: persistent merge buffer sized to merge_buf_cap.
 *   - Group E: retraction backup defensively deep-copied if populated.
 *   - Group F: run_count + run_ends fixed array memcpy'd.
 *   - Group G: compound_arity_map cloned via col_rel_clone_compound_meta;
 *     corrupt arity maps degrade dst to NONE-kind (matches new_like).
 *   - Group H: has_graph_column, graph_col_idx direct copy.
 *   - Group I: pool_owned, arena_owned, mem_ledger, memory_governor and
 *     retained_reserved_bytes always reset.
 *   - Group J: dedup_*, col_shared, row_scratch always reset; any cache
 *     state will be rebuilt lazily on demand.
 *
 * Memory-ledger policy (Issue #554, design rule R-1):
 *   The deep copy is treated as a TRANSIENT relation.  dst->mem_ledger and
 *   dst->memory_governor are unconditionally set to NULL and
 *   dst->retained_reserved_bytes remains zero regardless of whether src is wired
 *   up to a ledger.  Rationale: a deep copy is a short-lived workspace
 *   relation (K-Fusion fork point, retraction backup, debug snapshot)
 *   whose buffer growth and free events must NOT charge against the
 *   originating session's RELATION subsystem budget.  If a deep copy
 *   is later promoted into a long-lived role, the caller may wire up
 *   dst->mem_ledger explicitly post-copy and the ledger will pick up
 *   subsequent realloc/free events from there on.  Source ledger
 *   counters are likewise never touched by this function.
 *
 * @param src    Relation to clone.  Must be non-NULL.  May carry inline
 *               compound metadata, sharing flags, retraction backup,
 *               etc.; the function reads through src->columns even when
 *               col_shared[c] is true (borrowed-readable contract).
 * @param out    Out-parameter for the heap-allocated copy.  Must be
 *               non-NULL.  On any failure *out is left as NULL.
 * @param arena  Reserved.  Currently ignored -- the deep copy always
 *               heap-owns its buffers so subsequent col_rel_destroy on
 *               the result is correct without an arena reference.
 *               Accepted to match the FROZEN signature in the issue
 *               body and reserve room for an arena-aware variant.
 *
 * @retval 0       Success; @p *out points at the newly allocated copy.
 * @retval EINVAL  src or out was NULL.
 * @retval ENOMEM  Allocation of dst, name, columns, col_names, schema,
 *                 timestamps, merge buffer, or retraction backup
 *                 failed.  Partial state is unwound via
 *                 col_rel_free_contents + free(dst); calloc-zero-init
 *                 makes that safe at any unwind point.
 *
 * @note The result inherits no pool/arena/ledger/governor affiliation from
 *       @p src and starts with retained_reserved_bytes == 0.  Callers that
 *       need governed ownership must use an admission path before publication.
 */
int
col_rel_deep_copy(const col_rel_t *src, col_rel_t **out, wl_arena_t *arena)
{
    (void)arena; /* reserved; see contract block in internal.h */
    if (!src || !out)
        return EINVAL;
    *out = NULL;

    col_rel_t *dst = (col_rel_t *)calloc(1, sizeof(*dst));
    if (!dst)
        return ENOMEM;

    /* Name: deep-copy the optional null-terminated owned string. */
    if (src->name) {
        dst->name = wl_strdup(src->name);
        if (!dst->name) {
            col_rel_free_contents(dst);
            col_rel_free_heap_after_contents(dst);
            return ENOMEM;
        }
    }

    /* Scalar bookkeeping (Groups A scalars + D scalars). */
    if (col_rel_new_identity(&dst->relation_identity) != 0) {
        col_rel_free_contents(dst);
        col_rel_free_heap_after_contents(dst);
        return EOVERFLOW;
    }
    dst->view_generation = src->view_generation;
    dst->storage_generation = src->storage_generation;
    col_rel_storage_owner_init(dst);
    dst->ncols = src->ncols;
    dst->declared_ncols = src->declared_ncols;
    dst->nrows = src->nrows;
    dst->capacity = src->capacity;
    dst->sorted_nrows = src->sorted_nrows;
    dst->base_nrows = src->base_nrows;

    /* Group H: graph-column metadata (Issue #535). */
    dst->has_graph_column = src->has_graph_column;
    dst->graph_col_idx = src->graph_col_idx;

    /*
     * Group I (pool_owned, arena_owned, mem_ledger): always reset.  The
     * copy is heap-allocated and never participates in pool/arena/ledger
     * accounting -- callers that need those must wire them up explicitly.
     *
     * mem_ledger policy (Issue #554, R-1):
     *   The deep copy is a TRANSIENT relation: dst->mem_ledger stays NULL
     *   so its buffer realloc/free events never charge the source
     *   session's RELATION subsystem budget.  Source ledger counters are
     *   never touched by this function (we only read src's columns and
     *   metadata; no allocations are reported to src->mem_ledger).
     *
     * Group J (dedup_slots/cap/count, col_shared, row_scratch): always
     * reset.  The dedup hash table and row scratch are caches that will
     * be rebuilt lazily; col_shared is unconditionally cleared because
     * deep copies own all of their columns.
     *
     * calloc above already zeroed every byte, so no explicit assignment
     * is required here -- the comment is the contract.
     */

    /*
     * Group A: column buffers.  Allocate a private ncols x capacity grid
     * via col_columns_alloc, then memcpy the live nrows of each column.
     * Reading through src->columns[c] is correct even when col_shared[c]
     * is true (the buffer is borrowed-readable); the copy unconditionally
     * owns its storage so col_shared stays NULL on dst (Group J).
     */
    if (src->ncols > 0u && src->capacity > 0u) {
        dst->columns = col_columns_alloc(src->ncols, src->capacity);
        if (!dst->columns) {
            col_rel_free_contents(dst);
            col_rel_free_heap_after_contents(dst);
            return ENOMEM;
        }
        if (src->columns) {
            for (uint32_t c = 0; c < src->ncols; c++) {
                if (src->columns[c] && src->nrows > 0u) {
                    memcpy(dst->columns[c], src->columns[c],
                        (size_t)src->nrows * sizeof(int64_t));
                }
            }
        }
    }

    /*
     * Group B: col_names array + ArrowSchema (private rebuild).
     *
     * Schema is rebuilt with the same helper as col_rel_set_schema rather than
     * memcpy'd; the ArrowSchema release callback would otherwise
     * alias between src and dst, leading to a double-release.
     */
    if (src->ncols > 0u && src->col_names) {
        dst->col_names = (char **)calloc(src->ncols, sizeof(char *));
        if (!dst->col_names) {
            col_rel_free_contents(dst);
            col_rel_free_heap_after_contents(dst);
            return ENOMEM;
        }
        for (uint32_t i = 0; i < src->ncols; i++) {
            if (src->col_names[i]) {
                dst->col_names[i] = wl_strdup(src->col_names[i]);
                if (!dst->col_names[i]) {
                    col_rel_free_contents(dst);
                    col_rel_free_heap_after_contents(dst);
                    return ENOMEM;
                }
            }
        }
    }
    if (src->column_types && src->ncols > 0u) {
        dst->column_types = (wirelog_column_type_t *)malloc(
            (size_t)src->ncols * sizeof(*dst->column_types));
        if (!dst->column_types) {
            col_rel_free_contents(dst);
            col_rel_free_heap_after_contents(dst);
            return ENOMEM;
        }
        memcpy(dst->column_types, src->column_types,
            (size_t)src->ncols * sizeof(*dst->column_types));
    }

    /*
     * Group D: persistent merge buffer.  When src has a merge buffer
     * sized to merge_buf_cap rows, allocate the same shape for dst and
     * memcpy merge_buf_cap entries per column.  The buffer is reused
     * across consolidation iterations, so any in-flight content is
     * preserved.  merge_buf_cap is copied unconditionally; sorted_nrows
     * and base_nrows were already taken in the scalar bookkeeping pass.
     */
    dst->merge_buf_cap = src->merge_buf_cap;
    if (src->merge_columns && src->ncols > 0u && src->merge_buf_cap > 0u) {
        dst->merge_columns = col_columns_alloc(src->ncols, src->merge_buf_cap);
        if (!dst->merge_columns) {
            col_rel_free_contents(dst);
            col_rel_free_heap_after_contents(dst);
            return ENOMEM;
        }
        for (uint32_t c = 0; c < src->ncols; c++) {
            if (src->merge_columns[c]) {
                memcpy(dst->merge_columns[c], src->merge_columns[c],
                    (size_t)src->merge_buf_cap * sizeof(int64_t));
            }
        }
    }

    /*
     * Group E: zero-copy retraction backup (Issue #300, #369).  Outside
     * a retraction step every retract_backup_* field is NULL/0, so this
     * branch is normally inert -- but if a deep copy ever lands inside
     * a retraction window, the copy must own its own backup or any
     * subsequent restore would corrupt src's columns.  We therefore
     * defensively allocate retract_backup_capacity rows and memcpy
     * retract_backup_nrows live entries per column, plus mirror the run
     * tracking scalars + fixed array.
     */
    dst->retract_backup_nrows = src->retract_backup_nrows;
    dst->retract_backup_base_nrows = src->retract_backup_base_nrows;
    dst->retract_backup_capacity = src->retract_backup_capacity;
    dst->retract_backup_timestamp_capacity
        = src->retract_backup_timestamp_capacity;
    dst->retract_backup_sorted_nrows = src->retract_backup_sorted_nrows;
    dst->retract_backup_run_count = src->retract_backup_run_count;
    memcpy(dst->retract_backup_run_ends, src->retract_backup_run_ends,
        sizeof(src->retract_backup_run_ends));
    if (src->retract_backup_columns && src->ncols > 0u
        && src->retract_backup_capacity > 0u) {
        dst->retract_backup_columns = col_columns_alloc(src->ncols,
                src->retract_backup_capacity);
        if (!dst->retract_backup_columns) {
            col_rel_free_contents(dst);
            col_rel_free_heap_after_contents(dst);
            return ENOMEM;
        }
        for (uint32_t c = 0; c < src->ncols; c++) {
            if (src->retract_backup_columns[c]
                && src->retract_backup_nrows > 0u) {
                memcpy(dst->retract_backup_columns[c],
                    src->retract_backup_columns[c],
                    (size_t)src->retract_backup_nrows * sizeof(int64_t));
            }
        }
    }
    if (src->retract_backup_timestamps) {
        dst->retract_backup_timestamps = calloc(
            src->retract_backup_timestamp_capacity,
            sizeof(*dst->retract_backup_timestamps));
        if (!dst->retract_backup_timestamps) {
            col_rel_free_contents(dst);
            col_rel_free_heap_after_contents(dst);
            return ENOMEM;
        }
        memcpy(dst->retract_backup_timestamps,
            src->retract_backup_timestamps,
            (size_t)src->retract_backup_timestamp_capacity
            * sizeof(*dst->retract_backup_timestamps));
    }

    /*
     * Group F: tiered run-tracking metadata (Issue #369).  Both fields
     * are fixed-size POD: a scalar count plus a COL_MAX_RUNS-element
     * array of run end offsets.  Direct copy (the retract-backup mirror
     * lands in Group E).
     */
    dst->run_count = src->run_count;
    memcpy(dst->run_ends, src->run_ends, sizeof(src->run_ends));

    /*
     * Group C: timestamps array.  Sized to capacity (not nrows) so that
     * append_row paths after the copy do not need to grow the timestamps
     * buffer in lockstep with the columns buffer.  Skipped when src has
     * timestamp tracking disabled (timestamps == NULL).
     */
    if (src->timestamps) {
        uint32_t timestamp_capacity = src->timestamp_capacity > dst->capacity
        ? src->timestamp_capacity : dst->capacity;
        dst->timestamps = calloc(timestamp_capacity, sizeof(*dst->timestamps));
        if (!dst->timestamps) {
            col_rel_free_contents(dst);
            col_rel_free_heap_after_contents(dst);
            return ENOMEM;
        }
        dst->timestamp_capacity = timestamp_capacity;
        memcpy(dst->timestamps, src->timestamps,
            (size_t)src->timestamp_capacity * sizeof(*dst->timestamps));
    }

    /*
     * Group G: compound-column metadata (Issue #534, #553).  Mirror the
     * col_rel_new_like / col_rel_pool_new_like clone path: when the
     * source carries an INLINE-tier arity map, deep-copy it through the
     * shared col_rel_clone_compound_meta helper.  Helper failure is
     * handled with the existing graceful-degrade contract (compound
     * metadata cleared to NONE-kind on dst). Unlike new_like constructors,
     * this legacy deep-copy contract is unchanged. Sources with
     * WIRELOG_COMPOUND_KIND_NONE leave dst at the zero default established
     * by calloc.
     */
    if (src->compound_arity_len > 0
        && wl_columnar_relation_compound_map_valid(src)) {
        (void)col_rel_clone_compound_meta(dst, src, false);
    }

    if (src->schema_ok) {
        if (wl_columnar_relation_init_arrow_schema(dst) != 0) {
            col_rel_free_contents(dst);
            col_rel_free_heap_after_contents(dst);
            return ENOMEM;
        }
    }

    *out = dst;
    return 0;
}

/* Materialized JOINs need a governed logical twin for the evaluator stack.
 * Keep this separate from the legacy transient deep copy: that API also
 * clones execution scratch and intentionally carries no governor.  Admit the
 * destination's physical columns and timestamp allocation before allocating
 * either buffer, while a source-reader lease keeps the observed shape stable. */
static int
col_rel_deep_copy_governed_impl(const col_rel_t *src, col_rel_t **out,
    wl_columnar_memory_governor_ref_t *governor, bool mutable_image)
{
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_payload_txn_t pending;
    wl_columnar_memory_reservation_t metadata_pending, old_metadata;
    col_rel_t *dst = NULL;
    uint32_t timestamp_capacity = 0;
    uint64_t reserved_bytes = 0;
    uint64_t metadata_bytes = 0;
    int reserve_rc = 0;
    int rc;

    if (!src || !out)
        return EINVAL;
    *out = NULL;
    wl_columnar_memory_reservation_init(&metadata_pending);
    if (!mutable_image) {
        rc = col_rel_source_reader_acquire(src, &reader);
        if (rc != 0)
            return rc;
    }
    if (src->nrows > src->capacity || (src->ncols > 0
        && src->capacity > 0 && !src->columns)
        || (src->timestamps != NULL) != (src->timestamp_capacity != 0)
        || !wl_columnar_relation_compound_map_valid(src)) {
        rc = EINVAL;
        goto done;
    }
    if (!governor)
        governor = src->memory_governor;
    rc = governor
        ? wl_columnar_relation_alloc_governed(&dst,
            mutable_image ? NULL : src->name, governor)
        : col_rel_alloc(&dst, src->name ? src->name : "");
    if (rc != 0)
        goto done;
    if (!src->name) {
        free(dst->name);
        dst->name = NULL;
    }
    if (!col_rel_prospective_metadata_bytes(src->ncols,
        (const char *const *)src->col_names,
        mutable_image ? src->col_names != NULL : src->ncols > 0,
        !mutable_image, src->column_types != NULL, src->schema_ok,
        src->compound_arity_len, false, &metadata_bytes)) {
        rc = EOVERFLOW;
        goto done;
    }
    rc = col_rel_reserve_metadata(dst, metadata_bytes, &metadata_pending);
    if (rc != 0)
        goto done;
    dst->ncols = src->ncols;
    dst->declared_ncols = src->declared_ncols;
    dst->has_graph_column = src->has_graph_column;
    dst->graph_col_idx = src->graph_col_idx;
    if (src->column_types && src->ncols > 0) {
        dst->column_types = malloc((size_t)src->ncols
                * sizeof(*dst->column_types));
        if (!dst->column_types) {
            rc = ENOMEM;
            goto done;
        }
        memcpy(dst->column_types, src->column_types,
            (size_t)src->ncols * sizeof(*dst->column_types));
    }
    if (src->ncols > 0 && (!mutable_image || src->col_names)) {
        dst->col_names = (char **)calloc(src->ncols,
                sizeof(*dst->col_names));
        if (!dst->col_names) {
            rc = ENOMEM;
            goto done;
        }
        for (uint32_t c = 0; c < src->ncols; c++) {
            char fallback[32];
            const char *name = src->col_names && src->col_names[c]
                ? src->col_names[c] : NULL;
            if (!name && !mutable_image) {
                snprintf(fallback, sizeof(fallback), "col%u", c);
                name = fallback;
            }
            dst->col_names[c] = name ? wl_strdup(name) : NULL;
            if (name && !dst->col_names[c]) {
                rc = ENOMEM;
                goto done;
            }
        }
    }
    if (src->schema_ok && wl_columnar_relation_init_arrow_schema(dst) != 0) {
        rc = ENOMEM;
        goto done;
    }
    col_rel_publish_metadata(dst, &metadata_pending, metadata_bytes,
        &old_metadata);
    col_rel_release_reservation_or_abort(&old_metadata);
    if (src->compound_arity_len > 0) {
        int compound_rc = !wl_columnar_relation_compound_map_valid(src)
            ? -1 : col_rel_clone_compound_meta(dst, src, true);
        if (compound_rc != 0) {
            rc = compound_rc == -2 ? ENOMEM
                : compound_rc == -3 ? EOVERFLOW : EINVAL;
            goto done;
        }
    }
    dst->capacity = src->capacity;
    timestamp_capacity = src->timestamps
        ? (mutable_image ? src->timestamp_capacity
            : (src->timestamp_capacity > src->capacity
            ? src->timestamp_capacity : src->capacity)) : 0;
    if (src->timestamps && src->timestamp_capacity < src->nrows) {
        rc = EINVAL;
        goto done;
    }
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    /* Exercise rollback after the private descriptor and metadata image have
     * already been admitted, on the real checked-size cleanup path. */
    if (mutable_image && wl_columnar_relation_fail_mutable_image_overflow) {
        wl_columnar_relation_fail_mutable_image_overflow = false;
        rc = EOVERFLOW;
        goto done;
    }
#endif
    reserve_rc = col_rel_reserve_transition(dst, dst->capacity,
            timestamp_capacity, timestamp_capacity != 0, &pending,
            &reserved_bytes);
    if (reserve_rc < 0) {
        rc = reserve_rc == -ENOMEM
            && dst->memory_budget_denial_pending
            ? ENOSPC : -reserve_rc;
        goto done;
    }
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_test_after_governed_copy_admission)
        wl_columnar_relation_test_after_governed_copy_admission(src);
#endif
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_relation_fail_governed_copy_payload_alloc) {
        wl_columnar_relation_fail_governed_copy_payload_alloc = false;
        rc = ENOMEM;
        goto rollback;
    }
#endif
    if (dst->ncols > 0) {
        dst->columns = col_columns_alloc(dst->ncols, dst->capacity);
        if (!dst->columns) {
            rc = ENOMEM;
            goto rollback;
        }
        if (src->nrows > 0) {
            if (!src->columns) {
                rc = EINVAL;
                goto rollback;
            }
            for (uint32_t c = 0; c < src->ncols; c++) {
                if (!src->columns[c]) {
                    rc = EINVAL;
                    goto rollback;
                }
                memcpy(dst->columns[c], src->columns[c],
                    (size_t)src->nrows * sizeof(int64_t));
            }
        }
    }
    if (timestamp_capacity > 0) {
        dst->timestamps = calloc(timestamp_capacity, sizeof(*dst->timestamps));
        if (!dst->timestamps) {
            rc = ENOMEM;
            goto rollback;
        }
        dst->timestamp_capacity = timestamp_capacity;
        if (src->nrows > 0)
            memcpy(dst->timestamps, src->timestamps,
                (size_t)src->nrows * sizeof(*dst->timestamps));
    }
    if (dst->memory_governor && dst->ncols > 0) {
        dst->row_scratch = (int64_t *)malloc((size_t)dst->ncols
                * sizeof(*dst->row_scratch));
        if (!dst->row_scratch) {
            rc = ENOMEM;
            goto rollback;
        }
    }
    dst->nrows = src->nrows;
    dst->base_nrows = src->base_nrows;
    dst->sorted_nrows = src->sorted_nrows <= src->nrows
        ? src->sorted_nrows : 0;
    bool run_metadata_valid = src->run_count <= COL_MAX_RUNS;
    uint32_t previous_end = 0;
    if (run_metadata_valid) {
        for (uint32_t i = 0; i < src->run_count; i++) {
            uint32_t end = src->run_ends[i];
            if (end <= previous_end || end > src->nrows) {
                run_metadata_valid = false;
                break;
            }
            previous_end = end;
        }
        if (src->run_count > 0 && previous_end != src->nrows)
            run_metadata_valid = false;
    }
    if (run_metadata_valid) {
        dst->run_count = src->run_count;
        memcpy(dst->run_ends, src->run_ends, sizeof(dst->run_ends));
    } else {
        dst->run_count = 0;
        memset(dst->run_ends, 0, sizeof(dst->run_ends));
        dst->sorted_nrows = 0;
    }
    if (reserve_rc > 0) {
        rc = col_rel_publish_payload_txn(dst, &pending,
                reserved_bytes, NULL);
        if (rc != 0)
            goto rollback;
    }
    if (mutable_image) {
        if (src->merge_columns) {
            rc = col_rel_reserve_merge_grid(dst, src->merge_buf_cap);
            if (rc != 0) {
                if (rc == ENOMEM && dst->memory_budget_denial_pending)
                    rc = ENOSPC;
                goto done;
            }
            for (uint32_t c = 0; c < src->ncols; c++)
                memcpy(dst->merge_columns[c], src->merge_columns[c],
                    (size_t)src->merge_buf_cap * sizeof(int64_t));
        } else {
            dst->merge_buf_cap = src->merge_buf_cap;
        }
        rc = wl_columnar_eval_dedup_set_clone_exact(dst, src);
        if (rc != 0)
            goto done;
    }
    *out = dst;
    dst = NULL;
    rc = 0;
    goto done;

rollback:
    if (reserve_rc > 0)
        col_rel_payload_txn_rollback(dst, &pending);
done:
    if (metadata_pending.bytes)
        col_rel_reservation_rollback(&metadata_pending);
    if (dst)
        col_rel_destroy(dst);
    if (!mutable_image) {
        int release_rc = col_rel_source_reader_release(&reader);
        if (rc == 0 && release_rc != 0) {
            col_rel_destroy(*out);
            *out = NULL;
            rc = release_rc;
        }
    }
    return rc;
}

/* Public entry point.  The body lives in a file-private impl so that in-tree
 * callers can be given variants of it without widening this header's surface. */
int
wl_columnar_relation_deep_copy_governed(const col_rel_t *src, col_rel_t **out,
    wl_columnar_memory_governor_ref_t *governor)
{
    return col_rel_deep_copy_governed_impl(src, out, governor, false);
}

/* Logical twin: raw live data, fresh ownership, no execution auxiliaries. */
int
wl_columnar_relation_deep_copy_governed_controlled(const col_rel_t *src,
    col_rel_t **out, wl_columnar_memory_governor_ref_t *governor,
    wl_evaluation_control_t *control)
{
    wl_columnar_relation_source_admission_t admission = { 0 };
    col_rel_t *r = NULL;
    int rc;
    if (!control)
        return wl_columnar_relation_deep_copy_governed(src, out, governor);
    if (!out)
        return EINVAL;
    *out = NULL;
    if (!src)
        return EINVAL;
    rc = wl_columnar_relation_source_acquire_controlled(src, &admission,
            control);
    if (rc != 0)
        return rc;
    rc = wl_evaluation_control_charge(control, 12);
    if (rc != 0)
        goto done;
    if (src->nrows > src->capacity
        || (src->ncols && src->capacity && !src->columns)
        || (src->timestamps != NULL) != (src->timestamp_capacity != 0)
        || (src->timestamps && src->timestamp_capacity < src->nrows)
        || ((src->compound_arity_map == NULL) != (src->compound_arity_len == 0))
        || src->compound_arity_len > src->ncols
        || (src->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        && src->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        && src->compound_kind != WIRELOG_COMPOUND_KIND_SIDE)
        || (src->compound_kind == WIRELOG_COMPOUND_KIND_NONE
        && (src->compound_count || src->inline_physical_offset))) {
        rc = EINVAL;
        goto done;
    }
    if (src->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || src->compound_arity_map || src->compound_arity_len) {
        rc = ENOTSUP;
        goto done;
    }
    if (!governor)
        governor = src->memory_governor;
    rc = wl_columnar_relation_work_tail(control, 12);
    if (rc != 0)
        goto done;
    rc = wl_columnar_relation_new_private_controlled(src->name, src->ncols,
            src->capacity, src->timestamps != NULL, governor, src, true, NULL,
            0, &r,
            control);
done:
    rc = wl_columnar_relation_source_finish(&admission, rc, control, false);
    if (rc == 0)
        *out = r;
    else
        col_rel_destroy(r);
    return rc;
}

static bool
col_rel_image_token_valid(const wl_columnar_memory_reservation_t *token,
    const col_rel_t *owner, uint64_t bytes)
{
    uint64_t state = atomic_load_explicit(&token->state,
            memory_order_acquire);
    if (!bytes)
        return state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
               || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED;
    return token->identity == token && token->bytes == bytes
           && token->governor
           == wl_columnar_memory_governor_ref_get(owner->memory_governor)
           && state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
           && atomic_load_explicit(&token->owner_bits,
               memory_order_acquire) == (uintptr_t)owner;
}

static int
col_rel_mutable_image_validate(const col_rel_t *r, bool source)
{
    uint64_t payload, metadata, dedup, descriptor, named_descriptor;
    if (!r || !r->memory_governor || r->pool_owned || r->arena_owned
        || r->col_shared || r->retract_backup_columns
        || r->retract_backup_timestamps
        || r->retract_backup_nrows || r->retract_backup_base_nrows
        || r->retract_backup_capacity
        || r->retract_backup_timestamp_capacity
        || r->retract_backup_sorted_nrows || r->retract_backup_run_count
        || r->storage_owner != r || r->nrows > r->capacity
        || col_rel_storage_alias_borrow_count(r) != 0
        || r->base_nrows > r->nrows
        /* BDX may clear rows while retaining the previous sorted cursor.
         * The governed copy clamps it; the original remains untouched on
         * discard and is replaced only after the private work succeeds. */
        || (!source && r->sorted_nrows > r->nrows)
        || (r->ncols && !r->row_scratch)
        || (r->ncols && (r->merge_columns != NULL)
        != (r->merge_buf_cap != 0))
        || r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || r->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EBUSY;
    if (!col_rel_retained_live_bytes(r, &payload)
        || !col_rel_current_metadata_bytes(r, &metadata))
        return EINVAL;
    int rc = wl_columnar_eval_dedup_set_bytes(r, &dedup);
    if (rc != 0)
        return rc;
    /* wl_columnar_relation_alloc_governed and col_rel_attach_memory_governor
     * both charge a governed heap descriptor sizeof(col_rel_t) plus its name
     * bytes, and a pool-owned relation is refused above, so the named form is
     * what a source reaching here normally carries.
     *
     * The struct-alone form covers a relation whose name charge lives entirely
     * in a separate pool-name token.  No production path produces it:
     * session_pool_rel_promote allocates the heap descriptor with the name
     * already charged and only then moves the pool-name token onto it, so the
     * window before session_pool_rel_finish_promotion releases that token
     * carries both charges.  Such a source is double-charged, its recorded
     * bytes would not describe what commit must transfer, and comparing it
     * against the struct alone is what refuses it. */
    descriptor = sizeof(col_rel_t);
    if (!wl_columnar_memory_size_add(descriptor,
        r->name ? strlen(r->name) + 1u : 0u, &named_descriptor))
        return EOVERFLOW;
    if (r->descriptor_reserved_bytes != (r->pool_name_reserved_bytes
        ? descriptor : named_descriptor))
        return EBUSY;
    if (!col_rel_image_token_valid(&r->descriptor_reservation, r,
        r->descriptor_reserved_bytes)
        || (r->pool_name_reserved_bytes
        && (!r->name || r->pool_name_reserved_bytes
        != strlen(r->name) + 1u))
        || !col_rel_image_token_valid(&r->pool_name_reservation, r,
        r->pool_name_reserved_bytes)
        || !col_rel_image_token_valid(&r->retained_reservation, r, payload)
        || r->retained_reserved_bytes != payload
        || !col_rel_image_token_valid(&r->metadata_reservation, r, metadata)
        || r->metadata_reserved_bytes != metadata
        || !col_rel_image_token_valid(&r->dedup_reservation, r, dedup)
        || r->dedup_reserved_bytes != dedup)
        return EBUSY;
    for (uint32_t c = 0; c < r->ncols; c++) {
        if (r->capacity && (!r->columns || !r->columns[c]))
            return EINVAL;
        if (r->merge_columns && !r->merge_columns[c])
            return EINVAL;
    }
    if (r->run_count > COL_MAX_RUNS)
        return EINVAL;
    uint32_t end = 0;
    for (uint32_t i = 0; i < r->run_count; i++) {
        if (r->run_ends[i] <= end || r->run_ends[i] > r->nrows)
            return EINVAL;
        end = r->run_ends[i];
    }
    return r->run_count && end != r->nrows ? EINVAL : 0;
}

int
col_rel_mutable_image_prepare(col_rel_t *source,
    col_rel_mutable_image_t *transaction)
{
    int rc;
    if (!source || !transaction)
        return EINVAL;
    if (transaction->source || transaction->image)
        return EBUSY;
    memset(transaction, 0, sizeof(*transaction));
    rc = col_rel_published_writer_acquire(source, &transaction->writer);
    if (rc != 0)
        return rc;
    transaction->source = source;
    rc = col_rel_mutable_image_validate(source, true);
    if (rc == 0)
        rc = col_rel_deep_copy_governed_impl(source, &transaction->image,
                source->memory_governor, true);
    if (rc != 0) {
        if (transaction->image)
            col_rel_destroy(transaction->image);
        (void)wl_columnar_source_access_writer_release(&transaction->writer);
        memset(transaction, 0, sizeof(*transaction));
    }
    return rc;
}

col_rel_t *
col_rel_mutable_image_get(col_rel_mutable_image_t *transaction)
{
    return transaction && transaction->source
           && transaction->writer.identity == (uintptr_t)&transaction->writer
        ? transaction->image : NULL;
}

void
col_rel_mutable_image_discard(col_rel_mutable_image_t *transaction)
{
    if (!transaction || !transaction->source)
        return;
    if (transaction->writer.owner != &transaction->source->source_access
        || transaction->writer.identity
        != (uintptr_t)&transaction->writer
        || !wl_columnar_source_access_writer_thread_equal(
            &transaction->writer))
        abort();
    if (transaction->image)
        col_rel_destroy(transaction->image);
    if (wl_columnar_source_access_writer_release(&transaction->writer) != 0)
        abort();
    memset(transaction, 0, sizeof(*transaction));
}

/* The image's current tokens (including growth after prepare) are moved,
* never copied. All fallible checks precede the first source mutation. */
int
col_rel_mutable_image_commit(col_rel_mutable_image_t *transaction)
{
    wl_columnar_memory_reservation_t descriptor, image_descriptor, pool_name;
    wl_columnar_memory_reservation_t old_payload, old_metadata, old_dedup;
    wl_columnar_memory_reservation_t new_payload, new_metadata, new_dedup;
    col_rel_t old, *source, *image;
    wl_columnar_memory_governor_ref_t *image_governor;
    wl_mem_ledger_t *ledger;
    char *name;
    uint64_t identity, view_generation, storage_generation;
    uint64_t gate_state, descriptor_state, ledger_before;
    uint64_t descriptor_bytes, pool_name_bytes, payload_bytes;
    uint64_t metadata_bytes, dedup_bytes;
    col_rel_t *deferred_next;
    struct wl_col_session_t *deferred_session;

    if (!transaction)
        return EINVAL;
    source = transaction->source;
    image = transaction->image;
    if (!source || !image
        || transaction->writer.owner != &source->source_access
        || transaction->writer.identity
        != (uintptr_t)&transaction->writer
        || !wl_columnar_source_access_writer_thread_equal(
            &transaction->writer))
        return EINVAL;
    if (col_rel_mutable_image_validate(source, true) != 0
        || col_rel_mutable_image_validate(image, false) != 0
        || image->name || image->pool_owned || image->memory_governor
        != source->memory_governor)
        return EBUSY;

    wl_columnar_memory_reservation_init(&descriptor);
    wl_columnar_memory_reservation_init(&image_descriptor);
    wl_columnar_memory_reservation_init(&pool_name);
    wl_columnar_memory_reservation_init(&old_payload);
    wl_columnar_memory_reservation_init(&old_metadata);
    wl_columnar_memory_reservation_init(&old_dedup);
    wl_columnar_memory_reservation_init(&new_payload);
    wl_columnar_memory_reservation_init(&new_metadata);
    wl_columnar_memory_reservation_init(&new_dedup);
    descriptor_bytes = source->descriptor_reserved_bytes;
    pool_name_bytes = source->pool_name_reserved_bytes;
    payload_bytes = image->retained_reserved_bytes;
    metadata_bytes = image->metadata_reserved_bytes;
    dedup_bytes = image->dedup_reserved_bytes;
    image_governor = image->memory_governor;
    if (!wl_columnar_memory_reservation_move(&descriptor,
        &source->descriptor_reservation)
        || !wl_columnar_memory_reservation_move(&image_descriptor,
        &image->descriptor_reservation)
        || (pool_name_bytes && !wl_columnar_memory_reservation_move(
            &pool_name, &source->pool_name_reservation))
        || (source->retained_reserved_bytes
        && !wl_columnar_memory_reservation_move(&old_payload,
        &source->retained_reservation))
        || (source->metadata_reserved_bytes
        && !wl_columnar_memory_reservation_move(&old_metadata,
        &source->metadata_reservation))
        || (source->dedup_reserved_bytes
        && !wl_columnar_memory_reservation_move(&old_dedup,
        &source->dedup_reservation))
        || (payload_bytes && (!wl_columnar_memory_transfer(
            &image->retained_reservation, source)
        || !wl_columnar_memory_reservation_move(&new_payload,
        &image->retained_reservation)))
        || (metadata_bytes && (!wl_columnar_memory_transfer(
            &image->metadata_reservation, source)
        || !wl_columnar_memory_reservation_move(&new_metadata,
        &image->metadata_reservation)))
        || (dedup_bytes && (!wl_columnar_memory_transfer(
            &image->dedup_reservation, source)
        || !wl_columnar_memory_reservation_move(&new_dedup,
        &image->dedup_reservation))))
        abort(); /* Validated writer-exclusive tokens cannot race here. */

    old = *source;
    name = old.name;
    ledger = old.mem_ledger;
    identity = old.relation_identity;
    view_generation = old.view_generation;
    storage_generation = old.storage_generation;
    ledger_before = col_rel_owned_ledger_bytes(&old);
    deferred_next = old.deferred_relation_next;
    deferred_session = old.deferred_relation_session;
    gate_state = atomic_load_explicit(&old.source_access.state,
            memory_order_acquire);
    descriptor_state = atomic_load_explicit(&old.descriptor_access.state,
            memory_order_acquire);
    *source = *image;
    free(image); /* Its descriptor credit remains live until this free. */
    transaction->image = NULL;
    source->name = name;
    source->memory_governor = old.memory_governor;
    source->mem_ledger = ledger;
    source->relation_identity = identity;
    source->memory_budget_denial_pending = old.memory_budget_denial_pending;
    source->view_generation = view_generation;
    source->storage_generation = storage_generation;
    source->deferred_relation_next = deferred_next;
    source->deferred_relation_session = deferred_session;
    source->storage_owner = source;
    source->storage_owner_identity = identity;
    source->ledger_ts_bytes = old.ledger_ts_bytes;
    source->descriptor_reserved_bytes = descriptor_bytes;
    wl_columnar_memory_reservation_init(&source->descriptor_reservation);
    if (!wl_columnar_memory_reservation_move(&source->descriptor_reservation,
        &descriptor))
        abort();
    source->pool_name_reserved_bytes = pool_name_bytes;
    wl_columnar_memory_reservation_init(&source->pool_name_reservation);
    if (pool_name_bytes && !wl_columnar_memory_reservation_move(
            &source->pool_name_reservation, &pool_name))
        abort();
    source->retained_reserved_bytes = payload_bytes;
    source->metadata_reserved_bytes = metadata_bytes;
    source->dedup_reserved_bytes = dedup_bytes;
    wl_columnar_memory_reservation_init(&source->retained_reservation);
    wl_columnar_memory_reservation_init(&source->metadata_reservation);
    wl_columnar_memory_reservation_init(&source->dedup_reservation);
    if ((payload_bytes && !wl_columnar_memory_reservation_move(
            &source->retained_reservation, &new_payload))
        || (metadata_bytes && !wl_columnar_memory_reservation_move(
            &source->metadata_reservation, &new_metadata))
        || (dedup_bytes && !wl_columnar_memory_reservation_move(
            &source->dedup_reservation, &new_dedup)))
        abort();
    atomic_store_explicit(&source->source_access.state, gate_state,
        memory_order_release);
    atomic_store_explicit(&source->descriptor_access.state, descriptor_state,
        memory_order_release);
    col_rel_ledger_reconcile(source, ledger_before);
    wl_columnar_relation_touch_replacement(source);

    old.name = NULL;
    old.mem_ledger = NULL;
    old.memory_governor = NULL;
    old.descriptor_reserved_bytes = 0;
    old.pool_name_reserved_bytes = 0;
    old.retained_reserved_bytes = 0;
    old.metadata_reserved_bytes = 0;
    old.dedup_reserved_bytes = 0;
    old.storage_owner = &old;
    old.storage_owner_identity = old.relation_identity;
    atomic_store_explicit(&old.storage_alias_borrows, 0,
        memory_order_relaxed);
    col_rel_storage_owner_init(&old);
    col_rel_free_contents(&old); /* Physical old image retires first. */
    col_rel_release_reservation_or_abort(&old_payload);
    col_rel_release_reservation_or_abort(&old_metadata);
    col_rel_release_reservation_or_abort(&old_dedup);
    col_rel_release_reservation_or_abort(&image_descriptor);
    wl_columnar_memory_governor_ref_release(image_governor);
    if (wl_columnar_source_access_writer_release(&transaction->writer) != 0)
        abort();
    memset(transaction, 0, sizeof(*transaction));
    return 0;
}

static int
col_rel_replacement_compound_entries(const col_rel_t *candidate,
    uint32_t *out_entries)
{
    if (!candidate || !out_entries
        || !wl_columnar_relation_compound_map_valid(candidate))
        return EINVAL;
    *out_entries = candidate->compound_arity_len;
    return 0;
}

static int
col_rel_replacement_validate_candidate(const col_rel_t *candidate)
{
    uint64_t ignored_bytes;
    uint32_t compound_entries;

    if (!candidate || candidate->nrows > candidate->capacity
        || (candidate->timestamps != NULL)
        != (candidate->timestamp_capacity != 0)
        || (candidate->timestamps
        && candidate->timestamp_capacity < candidate->nrows)
        || !wl_columnar_relation_generation_valid(
            candidate->view_generation)
        || !wl_columnar_relation_generation_valid(
            candidate->storage_generation))
        return EINVAL;
    if (candidate->view_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || candidate->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EOVERFLOW;
    if (!col_rel_retained_live_bytes(candidate, &ignored_bytes))
        return EOVERFLOW;
    if (candidate->ncols > 0 && candidate->capacity > 0) {
        if (!candidate->columns)
            return EINVAL;
        for (uint32_t c = 0; c < candidate->ncols; c++)
            if (!candidate->columns[c])
                return EINVAL;
    }
    if (col_rel_replacement_compound_entries(candidate, &compound_entries)
        != 0)
        return EINVAL;
    return 0;
}

static bool
col_rel_replacement_old_reservation_valid(const col_rel_t *dst)
{
    uint64_t state;

    if (!dst || dst->retained_reserved_bytes == 0)
        return true;
    if (dst->retained_reservation.identity != &dst->retained_reservation)
        return false;
    state = atomic_load_explicit(&dst->retained_reservation.state,
            memory_order_acquire);
    return state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED;
}

static int
col_rel_prepare_replacement_impl(col_rel_t *dst, const col_rel_t *candidate,
    col_rel_replacement_t *replacement, bool writer_held)
{
    uint64_t planned_bytes;
    uint64_t staged_bytes;
    uint64_t metadata_bytes = 0;
    uint64_t dedup_bytes = 0;
    uint32_t compound_entries;
    int rc;

    if (!dst || !candidate || !replacement || dst == candidate)
        return EINVAL;
    if (dst->memory_governor && (dst->retract_backup_columns
        || dst->retract_backup_timestamps))
        return EBUSY;

    rc = col_rel_replacement_validate_candidate(candidate);
    if (rc != 0)
        return rc;
    rc = wl_columnar_eval_dedup_set_bytes(candidate, &dedup_bytes);
    if (rc != 0)
        return rc;
    if (dst->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || dst->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EOVERFLOW;
    if (!col_rel_replacement_old_reservation_valid(dst))
        return EINVAL;
    /* The private legacy copy keeps at least column capacity for enabled
     * timestamps, even when the source's physical timestamp buffer is
     * narrower. Admit the staged allocation, not the source shape. */
    uint32_t staged_timestamp_capacity = candidate->timestamps
        ? (candidate->timestamp_capacity > candidate->capacity
            ? candidate->timestamp_capacity : candidate->capacity) : 0;
    /* Legacy deep_copy allocates a primary grid only for positive capacity.
     * An empty new-like candidate keeps columns NULL but still needs eager
     * governed scratch in the staged replacement. */
    col_rel_payload_shape_t staged_shape;
    if (!col_rel_payload_live_shape(candidate, &staged_shape))
        return EOVERFLOW;
    staged_shape.owned_columns = candidate->capacity ? candidate->ncols : 0;
    staged_shape.active_table = candidate->capacity && candidate->ncols;
    staged_shape.timestamp_capacity = staged_timestamp_capacity;
    staged_shape.scratch = dst->memory_governor && candidate->ncols != 0;
    if (!col_rel_payload_shape_bytes(&staged_shape, &planned_bytes))
        return EOVERFLOW;
    if (!col_rel_prospective_metadata_bytes(candidate->ncols,
        (const char *const *)candidate->col_names,
        candidate->col_names != NULL, false,
        candidate->column_types != NULL, candidate->schema_ok,
        candidate->compound_arity_len, false,
        &metadata_bytes))
        return EOVERFLOW;

    if (writer_held) {
        col_rel_t *owner = NULL;
        /* Only a token that names this exact replacement object and the
         * current thread is ours to hold and later release.  A copied or
         * otherwise forged token would make discard release a gate this
         * replacement never admitted. */
        if (!replacement->writer_acquired
            || col_rel_storage_owner_resolve(dst, &owner) != 0
            || replacement->writer.owner != &owner->source_access
            || replacement->writer.identity
            != (uintptr_t)&replacement->writer
            || !wl_columnar_source_access_writer_thread_equal(
                &replacement->writer)) {
            rc = EINVAL;
            goto fail;
        }
    } else {
        rc = col_rel_published_writer_acquire(dst, &replacement->writer);
        if (rc != 0)
            return rc;
        replacement->writer_acquired = true;
    }

    /* Reserve the complete private shape before deep_copy allocates it.  The
     * candidate may borrow arena/shared storage, but publication always owns
     * its replacement, so admission is based on the full retained shape. */
    replacement->reserved_bytes = planned_bytes;
    if (dst->memory_governor && planned_bytes > 0) {
        wl_columnar_memory_admission_status_t status;
        wl_columnar_memory_governor_t *governor
            = wl_columnar_memory_governor_ref_get(dst->memory_governor);

        status = wl_columnar_memory_reserve_checked(governor, planned_bytes,
                &replacement->reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                dst->memory_budget_denial_pending = true;
            goto allocation_failure;
        }
        if (!wl_columnar_memory_commit(&replacement->reservation, dst))
            goto allocation_failure;
        replacement->reservation_active = true;
    }

    if (dst->memory_governor && metadata_bytes > 0) {
        if (col_rel_reserve_metadata(dst, metadata_bytes,
            &replacement->metadata_reservation) != 0)
            goto allocation_failure;
        replacement->metadata_reserved_bytes = metadata_bytes;
        replacement->metadata_reservation_active = true;
    }

    if (dst->memory_governor && dedup_bytes > 0) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(dst->memory_governor),
                dedup_bytes, &replacement->dedup_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                dst->memory_budget_denial_pending = true;
            rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EBUSY;
            goto fail;
        }
        replacement->dedup_reserved_bytes = dedup_bytes;
        replacement->dedup_reservation_active = true;
    }

    rc = col_rel_deep_copy(candidate, &replacement->staged, NULL);
    if (rc != 0)
        goto fail;
    if (dst->memory_governor && replacement->staged->ncols > 0) {
        replacement->staged->row_scratch = (int64_t *)malloc(
            (size_t)replacement->staged->ncols
            * sizeof(*replacement->staged->row_scratch));
        if (!replacement->staged->row_scratch) {
            rc = ENOMEM;
            goto fail;
        }
    }
    /* col_rel_deep_copy historically degrades malformed compound metadata to
     * NONE.  A publication primitive must reject that loss instead of
     * silently publishing a different schema. */
    if (replacement->staged->compound_kind != candidate->compound_kind
        || replacement->staged->compound_count != candidate->compound_count
        || replacement->staged->compound_arity_len
        != candidate->compound_arity_len
        || replacement->staged->inline_physical_offset
        != candidate->inline_physical_offset)
        goto invalid;
    if (candidate->compound_arity_map) {
        if (col_rel_replacement_compound_entries(candidate,
            &compound_entries) != 0
            || !replacement->staged->compound_arity_map
            || memcmp(replacement->staged->compound_arity_map,
            candidate->compound_arity_map,
            (size_t)compound_entries * sizeof(uint32_t)) != 0)
            goto invalid;
    }
    if (candidate->dedup_cap > 0) {
        replacement->staged->dedup_slots = (uint64_t *)malloc(
            (size_t)candidate->dedup_cap
            * sizeof(*replacement->staged->dedup_slots));
        if (!replacement->staged->dedup_slots)
            goto allocation_failure;
        memcpy(replacement->staged->dedup_slots, candidate->dedup_slots,
            (size_t)candidate->dedup_cap
            * sizeof(*replacement->staged->dedup_slots));
        replacement->staged->dedup_cap = candidate->dedup_cap;
        replacement->staged->dedup_count = candidate->dedup_count;
    }

    staged_bytes = col_rel_transport_bytes(replacement->staged);
    if (staged_bytes == UINT64_MAX || staged_bytes != planned_bytes)
        goto overflow;
    return 0;

invalid:
    rc = EINVAL;
    goto fail;
overflow:
    rc = EOVERFLOW;
    goto fail;
allocation_failure:
    rc = ENOMEM;
fail:
    col_rel_discard_replacement(replacement);
    return rc;
}

int
col_rel_prepare_replacement(col_rel_t *dst, const col_rel_t *candidate,
    col_rel_replacement_t *replacement)
{
    if (!dst || !candidate || !replacement)
        return EINVAL;
    memset(replacement, 0, sizeof(*replacement));
    wl_columnar_memory_reservation_init(&replacement->reservation);
    wl_columnar_memory_reservation_init(&replacement->metadata_reservation);
    wl_columnar_memory_reservation_init(&replacement->dedup_reservation);
    return col_rel_prepare_replacement_impl(dst, candidate, replacement,
               false);
}

int
col_rel_prepare_replacement_locked(col_rel_t *dst,
    const col_rel_t *candidate, col_rel_replacement_t *replacement)
{
    int rc;

    if (!dst || !candidate || !replacement)
        return EINVAL;
    wl_columnar_memory_reservation_init(&replacement->metadata_reservation);
    wl_columnar_memory_reservation_init(&replacement->dedup_reservation);
    rc = col_rel_prepare_replacement_impl(dst, candidate, replacement, true);
    if (rc != 0 && replacement->writer_acquired)
        col_rel_discard_replacement(replacement);
    return rc;
}

void
col_rel_commit_replacement_retain_writer_locked(col_rel_t *dst,
    col_rel_replacement_t *replacement,
    wl_columnar_source_access_writer_t *retained_writer)
{
    col_rel_t old;
    col_rel_t *owner = NULL;
    col_rel_t *staged;
    wl_columnar_memory_reservation_t new_reservation;
    wl_columnar_memory_reservation_t descriptor_reservation;
    wl_columnar_memory_reservation_t pool_name_reservation;
    wl_columnar_memory_reservation_t old_metadata;
    wl_columnar_memory_reservation_t old_dedup;
    char *name;
    char *staged_name;
    wl_mem_ledger_t *ledger;
    wl_columnar_memory_governor_ref_t *governor;
    uint64_t identity;
    uint64_t view_generation;
    uint64_t storage_generation;
    uint64_t ledger_before;
    uint64_t ledger_ts_bytes;
    uint64_t gate_state;
    uint64_t descriptor_state;
    col_rel_t *deferred_relation_next;
    struct wl_col_session_t *deferred_relation_session;
    bool has_new_reservation;
    bool has_old_reservation;
    uint64_t old_retained_bytes;
    uint64_t descriptor_reserved_bytes;
    uint64_t pool_name_reserved_bytes;
    uint64_t metadata_reserved_bytes;
    uint64_t old_dedup_reserved_bytes;
    wl_columnar_memory_reservation_t old_reservation;

    /* Preparation establishes all fallible invariants.  Publication is a
     * no-fail operation: callers must not attempt to roll it back after the
     * destination has been replaced.  The owner-consistency check that #1621
     * added is kept, but as a precondition rather than an EINVAL return: a
     * caller that reaches here with a mismatched gate has already violated
     * the prepare/commit contract, and this function has no error channel
     * left to report it through. */
    if (!dst || !replacement || !replacement->staged || !retained_writer
        || retained_writer->owner || retained_writer->secondary_owner
        || retained_writer->identity || retained_writer->thread_valid
        || col_rel_storage_owner_resolve(dst, &owner) != 0
        || owner != dst)
        abort();
    assert(replacement->writer_acquired
        && replacement->writer.owner == &owner->source_access
        && replacement->writer.identity == (uintptr_t)&replacement->writer
        && wl_columnar_source_access_writer_thread_equal(
            &replacement->writer));
    staged = replacement->staged;
    assert(staged->nrows <= staged->capacity
        && dst->view_generation < WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        && dst->storage_generation < WL_COLUMNAR_REL_GENERATION_INVALID - 1u);

    has_new_reservation = replacement->reservation_active;
    wl_columnar_memory_reservation_init(&old_reservation);
    wl_columnar_memory_reservation_init(&new_reservation);
    wl_columnar_memory_reservation_init(&descriptor_reservation);
    wl_columnar_memory_reservation_init(&pool_name_reservation);
    wl_columnar_memory_reservation_init(&old_metadata);
    wl_columnar_memory_reservation_init(&old_dedup);
    descriptor_reserved_bytes = dst->descriptor_reserved_bytes;
    if (descriptor_reserved_bytes > 0
        && !wl_columnar_memory_reservation_move(&descriptor_reservation,
        &dst->descriptor_reservation))
        abort();
    pool_name_reserved_bytes = dst->pool_name_reserved_bytes;
    if (pool_name_reserved_bytes > 0
        && !wl_columnar_memory_reservation_move(&pool_name_reservation,
        &dst->pool_name_reservation))
        abort();
    metadata_reserved_bytes = dst->metadata_reserved_bytes;
    if (metadata_reserved_bytes
        && !wl_columnar_memory_reservation_move(&old_metadata,
        &dst->metadata_reservation))
        abort();
    old_dedup_reserved_bytes = dst->dedup_reserved_bytes;
    if (old_dedup_reserved_bytes
        && !wl_columnar_memory_reservation_move(&old_dedup,
        &dst->dedup_reservation))
        abort();
    old_retained_bytes = dst->retained_reserved_bytes;
    has_old_reservation = old_retained_bytes > 0;
    if (has_old_reservation
        && !wl_columnar_memory_reservation_move(&old_reservation,
        &dst->retained_reservation)) {
        assert(false && "retained replacement reservation must be movable");
        abort();
    }
    if (has_new_reservation
        && !wl_columnar_memory_reservation_move(&new_reservation,
        &replacement->reservation)) {
        assert(false && "prepared replacement reservation must be movable");
        abort();
    }

    old = *dst;
    name = old.name;
    staged_name = staged->name;
    ledger = old.mem_ledger;
    governor = old.memory_governor;
    identity = old.relation_identity;
    view_generation = old.view_generation;
    storage_generation = old.storage_generation;
    ledger_before = col_rel_owned_ledger_bytes(&old);
    ledger_ts_bytes = old.ledger_ts_bytes;
    deferred_relation_next = old.deferred_relation_next;
    deferred_relation_session = old.deferred_relation_session;
    gate_state = atomic_load_explicit(&old.source_access.state,
            memory_order_acquire);
    descriptor_state = atomic_load_explicit(&old.descriptor_access.state,
            memory_order_acquire);

    *dst = *staged;
    if (staged->descriptor_reserved_bytes != 0)
        abort(); /* Staged copies must not own a descriptor reservation. */
    if (staged->metadata_reserved_bytes != 0)
        abort(); /* Staged metadata is covered by the prepared token. */
    if (staged->pool_name_reserved_bytes != 0)
        abort(); /* Staged copies do not own a pool name. */
    if (staged->dedup_reserved_bytes != 0)
        abort(); /* Its dedup image has a separate prepared token. */
    dst->name = name;
    staged->name = NULL;
    free(staged_name);
    free(staged);
    replacement->staged = NULL;

    dst->relation_identity = identity;
    dst->view_generation = view_generation;
    dst->storage_generation = storage_generation;
    dst->pool_owned = old.pool_owned;
    dst->mem_ledger = ledger;
    dst->memory_governor = governor;
    dst->descriptor_reserved_bytes = descriptor_reserved_bytes;
    wl_columnar_memory_reservation_init(&dst->descriptor_reservation);
    if (descriptor_reserved_bytes > 0
        && !wl_columnar_memory_reservation_move(
            &dst->descriptor_reservation, &descriptor_reservation))
        abort();
    dst->pool_name_reserved_bytes = pool_name_reserved_bytes;
    wl_columnar_memory_reservation_init(&dst->pool_name_reservation);
    if (pool_name_reserved_bytes > 0
        && !wl_columnar_memory_reservation_move(&dst->pool_name_reservation,
        &pool_name_reservation))
        abort();
    wl_columnar_memory_reservation_init(&dst->metadata_reservation);
    dst->metadata_reserved_bytes = 0;
    if (replacement->metadata_reservation_active) {
        if (!wl_columnar_memory_commit(
                &replacement->metadata_reservation, dst)
            || !wl_columnar_memory_reservation_move(
                &dst->metadata_reservation,
                &replacement->metadata_reservation))
            abort();
        dst->metadata_reserved_bytes = replacement->metadata_reserved_bytes;
        replacement->metadata_reservation_active = false;
        replacement->metadata_reserved_bytes = 0;
    }
    wl_columnar_memory_reservation_init(&dst->dedup_reservation);
    dst->dedup_reserved_bytes = 0;
    if (replacement->dedup_reservation_active) {
        if (!wl_columnar_memory_commit(
                &replacement->dedup_reservation, dst)
            || !wl_columnar_memory_reservation_move(
                &dst->dedup_reservation,
                &replacement->dedup_reservation))
            abort();
        dst->dedup_reserved_bytes = replacement->dedup_reserved_bytes;
        replacement->dedup_reservation_active = false;
        replacement->dedup_reserved_bytes = 0;
    }
    dst->retained_reserved_bytes = 0;
    wl_columnar_memory_reservation_init(&dst->retained_reservation);
    if (has_new_reservation) {
        (void)wl_columnar_memory_reservation_move(
            &dst->retained_reservation, &new_reservation);
        dst->retained_reserved_bytes = replacement->reserved_bytes;
    }
    dst->ledger_ts_bytes = ledger_ts_bytes;
    dst->deferred_relation_next = deferred_relation_next;
    dst->deferred_relation_session = deferred_relation_session;
    dst->storage_owner = dst;
    dst->storage_owner_identity = identity;
    atomic_store_explicit(&dst->storage_alias_borrows, 0,
        memory_order_relaxed);
    atomic_store_explicit(&dst->source_access.state, gate_state,
        memory_order_release);
    atomic_store_explicit(&dst->descriptor_access.state, descriptor_state,
        memory_order_release);
    col_rel_ledger_reconcile(dst, ledger_before);
    wl_columnar_relation_touch_replacement(dst);

    replacement->reservation_active = false;
    old.name = NULL;
    old.mem_ledger = NULL;
    old.memory_governor = NULL;
    old.descriptor_reserved_bytes = 0;
    old.pool_name_reserved_bytes = 0;
    old.metadata_reserved_bytes = 0;
    old.dedup_reserved_bytes = 0;
    old.ledger_ts_bytes = 0;
    old.storage_owner = &old;
    old.storage_owner_identity = old.relation_identity;
    old.storage_alias_borrows = 0;
    col_rel_storage_owner_init(&old);
    col_rel_free_contents(&old);
    col_rel_release_reservation_or_abort(&old_metadata);
    col_rel_release_reservation_or_abort(&old_dedup);

    if (has_old_reservation
        && !wl_columnar_memory_release(&old_reservation)) {
        /* Keep the old token reachable by the caller's discard path if the
         * governor cannot release it yet.  The new token remains published
         * on dst, while this conservative token can be retried without a
         * double release. */
        if (wl_columnar_memory_reservation_move(
                &replacement->reservation, &old_reservation))
            replacement->reservation_active = true;
    }

    if (wl_columnar_source_access_writer_move(retained_writer,
        &replacement->writer) != 0)
        abort(); /* Validated writer survives the descriptor publication. */
    replacement->writer_acquired = false;
}

void
col_rel_commit_replacement_locked(col_rel_t *dst,
    col_rel_replacement_t *replacement)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    int release_rc;
    col_rel_commit_replacement_retain_writer_locked(dst, replacement,
        &writer);
    release_rc = wl_columnar_source_access_writer_release(&writer);
    assert(release_rc == 0);
    (void)release_rc;
}

#ifdef WL_TEST_RELATION_RESIZE_HOOK
int
wl_columnar_relation_test_commit_replacement_locked(
    col_rel_t *dst, col_rel_replacement_t *replacement)
{
    if (wl_columnar_relation_fail_commit_publication) {
        wl_columnar_relation_fail_commit_publication = false;
        return EBUSY;
    }
    col_rel_commit_replacement_locked(dst, replacement);
    return 0;
}
#endif

void
col_rel_discard_replacement(col_rel_replacement_t *replacement)
{
    if (!replacement)
        return;
    if (replacement->staged) {
        col_rel_destroy(replacement->staged);
        replacement->staged = NULL;
    }
    if (replacement->metadata_reservation_active) {
        col_rel_release_reservation_or_abort(
            &replacement->metadata_reservation);
        replacement->metadata_reservation_active = false;
        replacement->metadata_reserved_bytes = 0;
    }
    if (replacement->dedup_reservation_active) {
        col_rel_release_reservation_or_abort(
            &replacement->dedup_reservation);
        replacement->dedup_reservation_active = false;
        replacement->dedup_reserved_bytes = 0;
    }
    if (replacement->reservation_active) {
        (void)wl_columnar_memory_release(&replacement->reservation);
        replacement->reservation_active = false;
    }
    if (replacement->writer_acquired) {
        (void)wl_columnar_source_access_writer_release(
            &replacement->writer);
        replacement->writer_acquired = false;
    }
    replacement->reserved_bytes = 0;
}

/* ---- radix sort by single key column ----------------------------------- */

/*
 * col_radix_sort_rows_by_key: stable LSD radix sort of a row-major
 * int64_t buffer by a single key column.  Used by arrangement.c
 * (sarr_build) and lftj.c (lftj_iter_init) to replace the
 * platform-specific qsort_r path -- those call sites only need a sort
 * by one column, which radix can do in eight passes over the key's
 * bytes without any comparator callback.  See #465 for the Android
 * portability driver that motivated removing qsort_r.
 *
 * Allocates one tmp row-major scratch buffer.  Returns 0 on success,
 * -1 if scratch allocation fails (the input is then unmodified).
 */
WL_COLUMNAR_RELATION_NOINLINE int
wl_columnar_relation_radix_sort_rows_by_key_typed(int64_t *data,
    uint32_t nrows,
    uint32_t ncols, uint32_t key_col, wirelog_column_type_t key_type)
{
    if (!data || ncols == 0 || nrows <= 1 || key_col >= ncols)
        return 0;

    size_t row_bytes = (size_t)ncols * sizeof(int64_t);
    int64_t *tmp = (int64_t *)malloc((size_t)nrows * row_bytes);
    if (!tmp)
        return -1;

    int64_t *src = data;
    int64_t *dst = tmp;
    uint32_t count[256];
    uint32_t prefix[256];

    /*
     * LSD radix over the 8 bytes of the int64_t key column.  Byte 0
     * (LSB) is processed first; byte 7 (MSB) last.  Integer keys flip
     * the sign bit so unsigned bucket order matches signed int64 order;
     * float keys use the monotonic IEEE-754 transform below.  Stable:
     * rows with equal key keep their relative order.
     */
    for (int b = 0; b < 8; b++) {
        int shift = b * 8;
        int is_sign_byte = (b == 7);

        memset(count, 0, sizeof(count));
        for (uint32_t row = 0; row < nrows; row++) {
            uint64_t raw = (uint64_t)src[(size_t)row * ncols + key_col];
            uint64_t ordered = key_type == WIRELOG_TYPE_FLOAT
                ? ((raw & UINT64_C(0x8000000000000000))
                        ? ~raw : raw ^ UINT64_C(0x8000000000000000))
                : raw;
            uint8_t bv = (uint8_t)(ordered >> shift);
            if (is_sign_byte && key_type != WIRELOG_TYPE_FLOAT)
                bv ^= 0x80u;
            count[bv]++;
        }

        prefix[0] = 0;
        for (int i = 1; i < 256; i++)
            prefix[i] = prefix[i - 1] + count[i - 1];

        for (uint32_t row = 0; row < nrows; row++) {
            uint64_t raw = (uint64_t)src[(size_t)row * ncols + key_col];
            uint64_t ordered = key_type == WIRELOG_TYPE_FLOAT
                ? ((raw & UINT64_C(0x8000000000000000))
                        ? ~raw : raw ^ UINT64_C(0x8000000000000000))
                : raw;
            uint8_t bv = (uint8_t)(ordered >> shift);
            if (is_sign_byte && key_type != WIRELOG_TYPE_FLOAT)
                bv ^= 0x80u;
            uint32_t out_pos = prefix[bv]++;
            memcpy(dst + (size_t)out_pos * ncols,
                src + (size_t)row * ncols,
                row_bytes);
        }

        int64_t *t = src;
        src = dst;
        dst = t;
    }

    /* After 8 passes the sorted output is in `src`.  Copy back if the
     * final pass ended in tmp. */
    if (src != data)
        memcpy(data, src, (size_t)nrows * row_bytes);

    free(tmp);
    return 0;
}

int
col_radix_sort_rows_by_key(int64_t *data, uint32_t nrows, uint32_t ncols,
    uint32_t key_col)
{
    return wl_columnar_relation_radix_sort_rows_by_key_typed(data, nrows,
               ncols, key_col, WIRELOG_TYPE_INT64);
}

/*
 * col_rel_insertion_sort: insertion sort for small segments (Issue #343).
 *
 * Sort sub-range [start_row, start_row + nrows) of r in-place.
 * O(n^2) but low constant overhead — faster than radix sort for small N.
 */
static int
wl_columnar_radix_insertion_prepared(col_rel_t *r, uint32_t start_row,
    uint32_t nrows,
    const wl_columnar_radix_workspace_t *workspace,
    col_delta_timestamp_t *timestamps);

static int
wl_columnar_radix_k16_prepared(col_rel_t *r, uint32_t start_row, uint32_t nrows,
    const wl_columnar_radix_workspace_t *workspace,
    col_delta_timestamp_t *timestamps);

static int
wl_columnar_radix_rows_prepared(col_rel_t *r, uint32_t start_row,
    uint32_t nrows,
    const wl_columnar_radix_workspace_t *workspace,
    col_delta_timestamp_t *timestamps);

/* ======================================================================== */
/* Fused Uniform Check + Count Pass (Issue #363 Phase 1)                    */
/* ======================================================================== */

/*
 * radix_uniform_count_fused: single gather pass that combines the uniform
 * check and the count pass into one traversal over col_data.
 *
 * Returns true  if all nrows bytes are identical — caller skips the pass.
 * Returns false if not uniform; bv_cache[] and count[] are populated and
 *               ready for the prefix-sum / scatter steps.
 *
 * Benefit over calling radix_uniform_check_fast then radix_count_pass_fast:
 * non-skipped passes pay for only one set of gather loads instead of two.
 */

#ifdef __AVX2__
static bool
radix_uniform_count_fused_avx2(const int64_t *col_data, uint32_t start_row,
    const uint32_t *src, uint32_t nrows, int shift, int is_sign_byte,
    uint8_t first_bv, uint8_t *bv_cache, uint32_t *count)
{
    uint8_t xmask = is_sign_byte ? 0x80u : 0u;
    __m256i vshift = _mm256_set1_epi64x(shift);
    uint8_t uniform = 1; /* bitwise-AND accumulator; 0 once any mismatch seen */
    uint32_t i = 0;

    /* SIMD gather + byte extract + bv_cache write + uniformity tracking.
     * Prefetch col_data 16 elements (2 iterations) ahead to hide L3 gather
     * latency (Issue #363 Phase 2). */
    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);
        __m128i vidx0 = _mm_loadu_si128((const __m128i *)(src + i));
        __m128i vidx1 = _mm_loadu_si128((const __m128i *)(src + i + 4));
        __m256i vals0 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx0, 8);
        __m256i vals1 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx1, 8);
        __m256i sh0 = _mm256_srlv_epi64(vals0, vshift);
        __m256i sh1 = _mm256_srlv_epi64(vals1, vshift);

        bv_cache[i + 0] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                0) ^ xmask;
        bv_cache[i + 1] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                1) ^ xmask;
        bv_cache[i + 2] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                2) ^ xmask;
        bv_cache[i + 3] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh0,
                3) ^ xmask;
        bv_cache[i + 4] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                0) ^ xmask;
        bv_cache[i + 5] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                1) ^ xmask;
        bv_cache[i + 6] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                2) ^ xmask;
        bv_cache[i + 7] = (uint8_t)(uint64_t)_mm256_extract_epi64(sh1,
                3) ^ xmask;

        /* Branchless bitwise accumulation — avoids misprediction in hot loop */
        uniform &= (bv_cache[i + 0] == first_bv) & (bv_cache[i + 1] ==
            first_bv) &
            (bv_cache[i + 2] == first_bv) & (bv_cache[i + 3] == first_bv) &
            (bv_cache[i + 4] == first_bv) & (bv_cache[i + 5] == first_bv) &
            (bv_cache[i + 6] == first_bv) & (bv_cache[i + 7] == first_bv);
    }

    /* Scalar tail */
    for (; i < nrows; i++) {
        uint8_t bv = (uint8_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_byte)
            bv ^= 0x80u;
        bv_cache[i] = bv;
        uniform &= (uint8_t)(bv == first_bv);
    }

    if (uniform)
        return true;

    /* Not uniform: build histogram from bv_cache (sequential read) */
    memset(count, 0, 256 * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
static bool
radix_uniform_count_fused_neon(const int64_t *col_data, uint32_t start_row,
    const uint32_t *src, uint32_t nrows, int shift, int is_sign_byte,
    uint8_t first_bv, uint8_t *bv_cache, uint32_t *count)
{
    uint8_t xmask = is_sign_byte ? 0x80u : 0u;
    int64x2_t vneg_shift = vdupq_n_s64(-(int64_t)shift);
    uint8x8_t vfirst = vdup_n_u8(first_bv);
    /* Bit-AND mask: stays UINT64_MAX while all compared bytes equal first_bv */
    uint64_t uniform_mask = UINT64_MAX;
    uint32_t i = 0;

    /* NEON gather + byte extract + bv_cache write + uniformity tracking.
     * Prefetch col_data 16 elements (2 iterations) ahead to hide L3 gather
     * latency (Issue #363 Phase 2). */
    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);
        int64_t v0 = col_data[start_row + src[i + 0]];
        int64_t v1 = col_data[start_row + src[i + 1]];
        int64_t v2 = col_data[start_row + src[i + 2]];
        int64_t v3 = col_data[start_row + src[i + 3]];
        int64_t v4 = col_data[start_row + src[i + 4]];
        int64_t v5 = col_data[start_row + src[i + 5]];
        int64_t v6 = col_data[start_row + src[i + 6]];
        int64_t v7 = col_data[start_row + src[i + 7]];

        int64x2_t vec01 = vcombine_s64(
            vcreate_s64((uint64_t)v0), vcreate_s64((uint64_t)v1));
        int64x2_t vec23 = vcombine_s64(
            vcreate_s64((uint64_t)v2), vcreate_s64((uint64_t)v3));
        int64x2_t vec45 = vcombine_s64(
            vcreate_s64((uint64_t)v4), vcreate_s64((uint64_t)v5));
        int64x2_t vec67 = vcombine_s64(
            vcreate_s64((uint64_t)v6), vcreate_s64((uint64_t)v7));

        uint64x2_t sv01 = vshlq_u64(vreinterpretq_u64_s64(vec01), vneg_shift);
        uint64x2_t sv23 = vshlq_u64(vreinterpretq_u64_s64(vec23), vneg_shift);
        uint64x2_t sv45 = vshlq_u64(vreinterpretq_u64_s64(vec45), vneg_shift);
        uint64x2_t sv67 = vshlq_u64(vreinterpretq_u64_s64(vec67), vneg_shift);

        uint8_t bvals[8] = {
            (uint8_t)vgetq_lane_u64(sv01, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv01, 1) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv23, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv23, 1) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv45, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv45, 1) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv67, 0) ^ xmask,
            (uint8_t)vgetq_lane_u64(sv67, 1) ^ xmask,
        };
        uint8x8_t bvec = vld1_u8(bvals);
        vst1_u8(bv_cache + i, bvec);

        /* SIMD compare: all-0xFF if every byte equals first_bv; AND into mask */
        uint8x8_t cmp = vceq_u8(bvec, vfirst);
        uniform_mask &= vget_lane_u64(vreinterpret_u64_u8(cmp), 0);
    }

    /* Scalar tail */
    bool uniform_tail = true;
    for (; i < nrows; i++) {
        uint8_t bv = (uint8_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_byte)
            bv ^= 0x80u;
        bv_cache[i] = bv;
        if (bv != first_bv)
            uniform_tail = false;
    }

    if (uniform_mask == UINT64_MAX && uniform_tail)
        return true;

    /* Not uniform: build histogram from bv_cache (sequential read) */
    memset(count, 0, 256 * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __ARM_NEON__ */

#if !defined(__AVX2__) && !defined(__ARM_NEON__)
/*
 * Scalar fallback: single loop over all elements — builds bv_cache[] and
 * count[] in one pass and returns true if all bytes are equal to first_bv.
 * Only compiled on non-SIMD targets to avoid unused-function warnings.
 */
static bool
radix_uniform_count_fused_scalar(const int64_t *col_data, uint32_t start_row,
    const uint32_t *src, uint32_t nrows, int shift, int is_sign_byte,
    uint8_t first_bv, uint8_t *bv_cache, uint32_t *count)
{
    bool uniform = true;

    memset(count, 0, 256 * sizeof(uint32_t));
    for (uint32_t i = 0; i < nrows; i++) {
        uint8_t bv = (uint8_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_byte)
            bv ^= 0x80u;
        bv_cache[i] = bv;
        count[bv]++;
        if (bv != first_bv)
            uniform = false;
    }
    return uniform;
}
#endif /* !__AVX2__ && !__ARM_NEON__ */

/* Compile-time dispatch: AVX2 > NEON > scalar */
#ifdef __AVX2__
#define radix_uniform_count_fused_fast radix_uniform_count_fused_avx2
#elif defined(__ARM_NEON__)
#define radix_uniform_count_fused_fast radix_uniform_count_fused_neon
#else
#define radix_uniform_count_fused_fast radix_uniform_count_fused_scalar
#endif

/* ======================================================================== */
/* Fused Uniform Check + Count Pass for k=16 (Issue #363 Phase 5c ext.)    */
/* ======================================================================== */

/*
 * k=16 variants: same two-pass strategy as the k=8 functions above, but
 * operating on uint16_t bucket values and a 65536-entry count[] histogram.
 *
 * Pass 1 (SIMD): gather int64_t, extract 16-bit bucket, write bv_cache[],
 *   accumulate uniformity check.  Return true immediately if uniform.
 * Pass 2 (scalar, only when not uniform): memset(count, 0, 256KB) + histogram.
 */

#ifdef __AVX2__
static bool
radix_uniform_count_fused_k16_avx2(const int64_t *col_data,
    uint32_t start_row, const uint32_t *src, uint32_t nrows, int shift,
    int is_sign_pass, uint16_t first_bv, uint16_t *bv_cache, uint32_t *count)
{
    uint16_t xmask = is_sign_pass ? 0x8000u : 0u;
    __m256i vshift = _mm256_set1_epi64x(shift);
    __m256i vmask16 = _mm256_set1_epi64x(0xFFFF);
    uint8_t uniform = 1;
    uint32_t i = 0;

    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);
        __m128i vidx0 = _mm_loadu_si128((const __m128i *)(src + i));
        __m128i vidx1 = _mm_loadu_si128((const __m128i *)(src + i + 4));
        __m256i vals0 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx0, 8);
        __m256i vals1 = _mm256_i32gather_epi64(
            (const long long *)(col_data + start_row), vidx1, 8);
        __m256i sh0 = _mm256_and_si256(
            _mm256_srlv_epi64(vals0, vshift), vmask16);
        __m256i sh1 = _mm256_and_si256(
            _mm256_srlv_epi64(vals1, vshift), vmask16);

        uint16_t b0 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                0) ^ xmask;
        uint16_t b1 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                1) ^ xmask;
        uint16_t b2 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                2) ^ xmask;
        uint16_t b3 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh0,
                3) ^ xmask;
        uint16_t b4 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                0) ^ xmask;
        uint16_t b5 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                1) ^ xmask;
        uint16_t b6 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                2) ^ xmask;
        uint16_t b7 = (uint16_t)(uint64_t)_mm256_extract_epi64(sh1,
                3) ^ xmask;

        bv_cache[i + 0] = b0;
        bv_cache[i + 1] = b1;
        bv_cache[i + 2] = b2;
        bv_cache[i + 3] = b3;
        bv_cache[i + 4] = b4;
        bv_cache[i + 5] = b5;
        bv_cache[i + 6] = b6;
        bv_cache[i + 7] = b7;

        uniform &= (b0 == first_bv) & (b1 == first_bv) &
            (b2 == first_bv) & (b3 == first_bv) &
            (b4 == first_bv) & (b5 == first_bv) &
            (b6 == first_bv) & (b7 == first_bv);
    }

    for (; i < nrows; i++) {
        uint16_t bv =
            (uint16_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_pass)
            bv ^= 0x8000u;
        bv_cache[i] = bv;
        uniform &= (uint8_t)(bv == first_bv);
    }

    if (uniform)
        return true;

    memset(count, 0, 65536u * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
static bool
radix_uniform_count_fused_k16_neon(const int64_t *col_data,
    uint32_t start_row, const uint32_t *src, uint32_t nrows, int shift,
    int is_sign_pass, uint16_t first_bv, uint16_t *bv_cache, uint32_t *count)
{
    uint16_t xmask = is_sign_pass ? 0x8000u : 0u;
    int64x2_t vneg_shift = vdupq_n_s64(-(int64_t)shift);
    uint16x8_t vfirst = vdupq_n_u16(first_bv);
    uint64_t uniform_mask = UINT64_MAX;
    uint32_t i = 0;

    for (; i + 8 <= nrows; i += 8) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16]);

        int64_t v0 = col_data[start_row + src[i + 0]];
        int64_t v1 = col_data[start_row + src[i + 1]];
        int64_t v2 = col_data[start_row + src[i + 2]];
        int64_t v3 = col_data[start_row + src[i + 3]];
        int64_t v4 = col_data[start_row + src[i + 4]];
        int64_t v5 = col_data[start_row + src[i + 5]];
        int64_t v6 = col_data[start_row + src[i + 6]];
        int64_t v7 = col_data[start_row + src[i + 7]];

        int64x2_t vec01 = vcombine_s64(
            vcreate_s64((uint64_t)v0), vcreate_s64((uint64_t)v1));
        int64x2_t vec23 = vcombine_s64(
            vcreate_s64((uint64_t)v2), vcreate_s64((uint64_t)v3));
        int64x2_t vec45 = vcombine_s64(
            vcreate_s64((uint64_t)v4), vcreate_s64((uint64_t)v5));
        int64x2_t vec67 = vcombine_s64(
            vcreate_s64((uint64_t)v6), vcreate_s64((uint64_t)v7));

        uint64x2_t sv01 = vshlq_u64(
            vreinterpretq_u64_s64(vec01), vneg_shift);
        uint64x2_t sv23 = vshlq_u64(
            vreinterpretq_u64_s64(vec23), vneg_shift);
        uint64x2_t sv45 = vshlq_u64(
            vreinterpretq_u64_s64(vec45), vneg_shift);
        uint64x2_t sv67 = vshlq_u64(
            vreinterpretq_u64_s64(vec67), vneg_shift);

        uint16_t bvals[8] = {
            (uint16_t)vgetq_lane_u64(sv01, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv01, 1) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv23, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv23, 1) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv45, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv45, 1) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv67, 0) ^ xmask,
            (uint16_t)vgetq_lane_u64(sv67, 1) ^ xmask,
        };
        uint16x8_t bvec = vld1q_u16(bvals);
        vst1q_u16(bv_cache + i, bvec);

        uint16x8_t cmp = vceqq_u16(bvec, vfirst);
        uint64x2_t c64 = vreinterpretq_u64_u16(cmp);
        uniform_mask &= vgetq_lane_u64(c64, 0) & vgetq_lane_u64(c64, 1);
    }

    bool uniform_tail = true;
    for (; i < nrows; i++) {
        uint16_t bv =
            (uint16_t)((uint64_t)col_data[start_row + src[i]] >> shift);
        if (is_sign_pass)
            bv ^= 0x8000u;
        bv_cache[i] = bv;
        if (bv != first_bv)
            uniform_tail = false;
    }

    if (uniform_mask == UINT64_MAX && uniform_tail)
        return true;

    memset(count, 0, 65536u * sizeof(uint32_t));
    for (uint32_t j = 0; j < nrows; j++)
        count[bv_cache[j]]++;
    return false;
}
#endif /* __ARM_NEON__ */

#if !defined(__AVX2__) && !defined(__ARM_NEON__)
static bool
radix_uniform_count_fused_k16_scalar(const int64_t *col_data,
    uint32_t start_row, const uint32_t *src, uint32_t nrows, int shift,
    int is_sign_pass, uint16_t first_bv, uint16_t *bv_cache, uint32_t *count)
{
    uint16_t uacc = 0;

    memset(count, 0, 65536u * sizeof(uint32_t));
    for (uint32_t i = 0; i < nrows; i++) {
        if (i + 16u < nrows)
            WL_PREFETCH_R(col_data + start_row + src[i + 16u]);
        int64_t v = col_data[start_row + src[i]];
        uint16_t bv = (uint16_t)((uint64_t)v >> shift);
        if (is_sign_pass)
            bv ^= 0x8000u;
        bv_cache[i] = bv;
        uacc |= (uint16_t)(bv ^ first_bv);
        count[bv]++;
    }
    return uacc == 0;
}
#endif /* !__AVX2__ && !__ARM_NEON__ */

/* Compile-time dispatch for k=16: AVX2 > NEON > scalar */
#ifdef __AVX2__
#define radix_uniform_count_fused_k16_fast radix_uniform_count_fused_k16_avx2
#elif defined(__ARM_NEON__)
#define radix_uniform_count_fused_k16_fast radix_uniform_count_fused_k16_neon
#else
#define radix_uniform_count_fused_k16_fast radix_uniform_count_fused_k16_scalar
#endif

/* Prepared-only kernels. Every buffer and capacity is validated before COW;
 * no allocator, admission or fallback path is reachable from these kernels. */
static int
wl_columnar_radix_insertion_prepared(col_rel_t *r, uint32_t start_row,
    uint32_t nrows,
    const wl_columnar_radix_workspace_t *workspace,
    col_delta_timestamp_t *timestamps)
{
    uint32_t nc = r->ncols;
    int64_t *work = workspace->insertion_rows;
    for (uint32_t i = 0; i < nrows; i++)
        col_rel_row_copy_out(r, start_row + i, work + (size_t)i * nc);

    if (timestamps)
        memcpy(timestamps, r->timestamps + start_row,
            (size_t)nrows * sizeof(*timestamps));
    for (uint32_t i = 1; i < nrows; i++) {
        col_delta_timestamp_t saved_timestamp = { 0 };
        if (timestamps)
            saved_timestamp = timestamps[i];
        int64_t *tbuf = work + (size_t)nrows * nc;
        memcpy(tbuf, work + (size_t)i * nc,
            (size_t)nc * sizeof(*work));
        uint32_t j = i;
        while (j > 0) {
            /* Compare r[start_row + j - 1] against saved key
             * in tbuf (not the relation -- row i is overwritten
             * after the first shift). */
            int cmp = 0;
            for (uint32_t c = 0; c < nc; c++) {
                int64_t va = work[(size_t)(j - 1) * nc + c];
                int64_t vb = tbuf[c];
                if (r->column_types
                    && r->column_types[c] == WIRELOG_TYPE_FLOAT) {
                    int fcmp = wl_columnar_float_compare_bits(va, vb);
                    if (fcmp != 0) {
                        cmp = fcmp;
                        break;
                    }
                    continue;
                }
                if (va > vb) {
                    cmp = 1;
                    break;
                }
                if (va < vb) {
                    cmp = -1;
                    break;
                }
            }
            if (cmp <= 0)
                break;
            memcpy(work + (size_t)j * nc,
                work + (size_t)(j - 1) * nc,
                (size_t)nc * sizeof(*work));
            if (timestamps)
                timestamps[j] = timestamps[j - 1];
            j--;
        }
        if (timestamps)
            timestamps[j] = saved_timestamp;
        memcpy(work + (size_t)j * nc, tbuf,
            (size_t)nc * sizeof(*work));
    }

    for (uint32_t c = 0; c < nc; c++)
        for (uint32_t i = 0; i < nrows; i++)
            r->columns[c][start_row + i] = work[(size_t)i * nc + c];
    if (timestamps)
        memcpy(r->timestamps + start_row, timestamps,
            (size_t)nrows * sizeof(*timestamps));
    return 0;
}
static int
wl_columnar_radix_k16_prepared(col_rel_t *r, uint32_t start_row, uint32_t nrows,
    const wl_columnar_radix_workspace_t *workspace,
    col_delta_timestamp_t *timestamps)
{
    uint32_t nc = r->ncols;

    const uint32_t radix_bits = 16u;
    const uint32_t num_passes = 64u / radix_bits;  /* 4 for k=16 */
    const uint32_t hist_size = 1u << radix_bits;   /* 65536 for k=16 */

    uint32_t *perm_a = workspace->perm_a;
    uint32_t *perm_b = workspace->perm_b;
    uint16_t *bv_cache = workspace->short_values;
    uint32_t *count = workspace->count16;
    for (uint32_t i = 0; i < nrows; i++)
        perm_a[i] = i;

    uint32_t *src = perm_a;
    uint32_t *dst = perm_b;

#ifdef WL_RADIX_BENCH
    uint64_t _tU = 0, _tS = 0, _tA = 0, _t0 = 0;
    uint32_t _nSk = 0, _nPs = 0;
#endif

    for (int c = (int)nc - 1; c >= 0; c--) {
        const int64_t *col_data = r->columns[c];
        for (uint32_t pass = 0; pass < num_passes; pass++) {
            int shift = (int)(pass * radix_bits);
            int is_sign_pass = (pass == num_passes - 1);

            uint16_t first_bv;
            {
                int64_t val = col_data[start_row + src[0]];
                first_bv = (uint16_t)((uint64_t)val >> shift);
                if (is_sign_pass)
                    first_bv ^= 0x8000u;
            }
#ifdef WL_RADIX_BENCH
            _t0 = now_ns();
#endif
            /* SIMD-dispatched fused uniform check + count pass for k=16
             * (Issue #363 Phase 5c extension). */
            if (radix_uniform_count_fused_k16_fast(col_data, start_row,
                src, nrows, shift, is_sign_pass, first_bv,
                bv_cache, count)) {
#ifdef WL_RADIX_BENCH
                _tU += now_ns() - _t0;
                _nSk++;
#endif
                continue;
            }
#ifdef WL_RADIX_BENCH
            _tU += now_ns() - _t0;
            _nPs++;
            _t0 = now_ns();
#endif
            {
                uint32_t running = 0;
                for (uint32_t i = 0; i < hist_size; i++) {
                    uint32_t cnt = count[i];
                    count[i] = running;
                    running += cnt;
                }
            }
            for (uint32_t i = 0; i < nrows; i++) {
                if (i + 16u < nrows)
                    WL_PREFETCH_W(dst + count[bv_cache[i + 16u]]);
                dst[count[bv_cache[i]]++] = src[i];
            }
            uint32_t *t = src;
            src = dst;
            dst = t;
#ifdef WL_RADIX_BENCH
            _tS += now_ns() - _t0;
#endif
        }
    }

#ifdef WL_RADIX_BENCH
    _t0 = now_ns();
#endif
    int64_t *temp_col = workspace->temp_column;
    for (uint32_t c = 0; c < nc; c++) {
        int64_t *col = r->columns[c];
        for (uint32_t i = 0; i < nrows; i++) {
            if (i + 8u < nrows)
                WL_PREFETCH_R(col + start_row + src[i + 8u]);
            temp_col[i] = col[start_row + src[i]];
        }
        memcpy(col + start_row, temp_col, nrows * sizeof(int64_t));
    }
    if (timestamps) {
        for (uint32_t i = 0; i < nrows; i++)
            timestamps[i] = r->timestamps[start_row + src[i]];
        memcpy(r->timestamps + start_row, timestamps,
            (size_t)nrows * sizeof(*timestamps));
    }

#ifdef WL_RADIX_BENCH
    _tA = now_ns() - _t0;
    if (wl_columnar_relation_radix_bench_enabled()) {
        uint64_t _tot = _tU + _tS + _tA;
        fprintf(stderr,
            "[radix-bench k=16] nrows=%u nc=%u pass=%u skip=%u "
            "uniform_+_count=%.0f%% scatter=%.0f%% apply=%.0f%% "
            "total_ms=%.3f\n",
            nrows, nc, _nPs, _nSk,
            100.0 * (double)_tU / (double)(_tot + 1),
            100.0 * (double)_tS / (double)(_tot + 1),
            100.0 * (double)_tA / (double)(_tot + 1),
            (double)_tot * 1e-6);
    }
#endif
    return 0;
}

static int
wl_columnar_radix_rows_prepared(col_rel_t *r, uint32_t start_row,
    uint32_t nrows,
    const wl_columnar_radix_workspace_t *workspace,
    col_delta_timestamp_t *timestamps)
{
    if (nrows <= 1)
        return 0;

    /* IEEE-754 keys need a different sign transform from signed integers.
     * Keep the established radix fast path for integer-only relations and
     * use the typed comparator until the float radix path is introduced. */
    if (r->column_types) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->column_types[c] == WIRELOG_TYPE_FLOAT)
                goto insertion;
        }
    }

    /* Hybrid threshold: insertion sort for small segments (Issue #343) */
    if (nrows <= 32)
        goto insertion;

    /* Adaptive radix width (Issue #363 Phase 5c): dispatch to k=16 for large
     * arrays where fewer passes outweigh the larger histogram cost.
     *
     * Empirical threshold (Apple M-series, 1-col, 64-bit uniform-random keys):
     *   nrows=10K: k8=0.66ms  k16=0.81ms  k8 faster by 1.23x
     *   nrows=20K: k8=0.81ms  k16=0.91ms  k8 faster by 1.12x
     *   nrows=30K: k8=0.83ms  k16=0.89ms  k8 faster by 1.08x
     *   nrows=40K: k8=0.81ms  k16=0.82ms  near parity
     *   nrows=50K: k8=0.79ms  k16=0.78ms  k16 faster by 1.01x
     *   nrows=60K: k8=0.82ms  k16=0.80ms  k16 faster by 1.02x
     *   nrows=100K: k8=1.41ms k16=1.32ms  k16 faster by 1.07x
     * Crossover at ~40-50K rows; 50000 is a conservative round boundary.
     *
     * k=16 uses a scalar fused loop (not SIMD): the 4-pass reduction
     * already yields fewer total iterations than 8-pass k=8+SIMD, and a
     * 256KB histogram makes SIMD gather impractical (cache pressure). */
    if (nrows >= 50000u) {
        int rc = wl_columnar_radix_k16_prepared(r, start_row, nrows, workspace,
                timestamps);
        if (rc == 0)
            wl_columnar_relation_touch_view(r);
        return rc;
    }

    /* k=8 SIMD path: 8 passes × 256 buckets (1KB histogram, stack-allocated).
     * SIMD-dispatched fused uniform+count avoids a separate gather loop. */
    uint32_t nc = r->ncols;

    const uint32_t radix_bits = 8u;
    const uint32_t num_passes = 64u / radix_bits;  /* 8 for k=8 */

    uint32_t *perm_a = workspace->perm_a;
    uint32_t *perm_b = workspace->perm_b;
    uint8_t *bv_cache = workspace->byte_values;
    for (uint32_t i = 0; i < nrows; i++)
        perm_a[i] = i;

    uint32_t *src = perm_a;
    uint32_t *dst = perm_b;
    uint32_t count[256];
    uint32_t prefix[256];

#ifdef WL_RADIX_BENCH
    /* _tU = fused uniform+count (radix_uniform_count_fused_fast)
     * _tS = scatter (prefix scan + scatter loop)
     * _tA = apply permutation per-column */
    uint64_t _tU = 0, _tS = 0, _tA = 0, _t0 = 0;
    uint32_t _nSk = 0, _nPs = 0;
#endif

    /* LSD radix sort: column nc-1 (LSB) to column 0 (MSB),
     * pass 0 (LSB) to pass num_passes-1 (MSB) within each column. */
    for (int c = (int)nc - 1; c >= 0; c--) {
        const int64_t *col_data = r->columns[c];
        for (uint32_t pass = 0; pass < num_passes; pass++) {
            int shift = (int)(pass * radix_bits);
            int is_sign_pass = (pass == num_passes - 1);

            /* Skip-pass: if all values identical at this byte position, skip. */
            uint8_t first_bv;
            {
                int64_t val = col_data[start_row + src[0]];
                first_bv = (uint8_t)((uint64_t)val >> shift);
                if (is_sign_pass)
                    first_bv ^= 0x80u;
            }
            /* Fused uniform check + count pass: single gather traversal
             * (Issue #363 Phase 1).  Returns true if all nrows bytes at
             * this position are identical (skip); returns false with
             * bv_cache[] and count[] populated when not uniform. */
#ifdef WL_RADIX_BENCH
            _t0 = now_ns();
#endif
            if (radix_uniform_count_fused_fast(col_data, start_row, src,
                nrows, shift, is_sign_pass, first_bv, bv_cache, count)) {
#ifdef WL_RADIX_BENCH
                _tU += now_ns() - _t0;
                _nSk++;
#endif
                continue;
            }
#ifdef WL_RADIX_BENCH
            _tU += now_ns() - _t0;
            _nPs++;
            _t0 = now_ns();
#endif
            prefix[0] = 0;
            for (int i = 1; i < 256; i++)
                prefix[i] = prefix[i - 1] + count[i - 1];

            /* Scatter pass with software prefetch (Issue #363 Phase 2). */
            for (uint32_t i = 0; i < nrows; i++) {
                if (i + 16u < nrows)
                    WL_PREFETCH_W(dst + prefix[bv_cache[i + 16u]]);
                dst[prefix[bv_cache[i]]++] = src[i];
            }
            uint32_t *t = src;
            src = dst;
            dst = t;
#ifdef WL_RADIX_BENCH
            _tS += now_ns() - _t0;
#endif
        }
    }

#ifdef WL_RADIX_BENCH
    _t0 = now_ns();
#endif
    /* Apply permutation per-column (Issue #334): contiguous access pattern. */
    int64_t *temp_col = workspace->temp_column;
    for (uint32_t c = 0; c < nc; c++) {
        int64_t *col = r->columns[c];
        /* Gather: prefetch 8 elements ahead (Issue #363 Phase 3). */
        for (uint32_t i = 0; i < nrows; i++) {
            if (i + 8u < nrows)
                WL_PREFETCH_R(col + start_row + src[i + 8u]);
            temp_col[i] = col[start_row + src[i]];
        }
        memcpy(col + start_row, temp_col, nrows * sizeof(int64_t));
    }
    if (timestamps) {
        for (uint32_t i = 0; i < nrows; i++)
            timestamps[i] = r->timestamps[start_row + src[i]];
        memcpy(r->timestamps + start_row, timestamps,
            (size_t)nrows * sizeof(*timestamps));
    }

#ifdef WL_RADIX_BENCH
    _tA = now_ns() - _t0;
    if (wl_columnar_relation_radix_bench_enabled()) {
        uint64_t _tot = _tU + _tS + _tA;
        fprintf(stderr,
            "[radix-bench k=8] nrows=%u nc=%u pass=%u skip=%u "
            "uniform_+_count=%.0f%% scatter=%.0f%% apply=%.0f%% "
            "total_ms=%.3f\n",
            nrows, nc, _nPs, _nSk,
            100.0 * (double)_tU / (double)(_tot + 1),
            100.0 * (double)_tS / (double)(_tot + 1),
            100.0 * (double)_tA / (double)(_tot + 1),
            (double)_tot * 1e-6);
    }
#endif
    wl_columnar_relation_touch_view(r);
    return 0;

insertion:
    {
        int rc = wl_columnar_radix_insertion_prepared(r, start_row, nrows,
                workspace,
                timestamps);
        if (rc == 0)
            wl_columnar_relation_touch_view(r);
        return rc;
    }
}

static bool
col_rel_size_multiply(size_t left, size_t right, size_t *out)
{
    if (!out || (right != 0 && left > SIZE_MAX / right))
        return false;
    *out = left * right;
    return true;
}

static bool
col_rel_size_add(size_t left, size_t right, size_t *out)
{
    if (!out || left > SIZE_MAX - right)
        return false;
    *out = left + right;
    return true;
}

void
wl_columnar_radix_workspace_destroy(wl_columnar_radix_workspace_t *workspace)
{
    if (!workspace)
        return;
    free(workspace->perm_a);
    free(workspace->perm_b);
    free(workspace->bucket_values);
    free(workspace->count16);
    free(workspace->temp_column);
    free(workspace->insertion_rows);
    free(workspace->timestamps);
    if (workspace->admission_active)
        (void)wl_columnar_memory_release(&workspace->admission);
    memset(workspace, 0, sizeof(*workspace));
}

static uint64_t
wl_columnar_radix_type_fingerprint(const col_rel_t *rel)
{
    uint64_t hash = UINT64_C(14695981039346656037);
    for (uint32_t c = 0; rel->column_types && c < rel->ncols; c++) {
        hash ^= (uint64_t)rel->column_types[c];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static int
wl_columnar_relation_radix_workspace_prepare(const col_rel_t *rel,
    const uint32_t *seg_boundaries, uint32_t seg_count,
    uint64_t additional_scratch_bytes,
    wl_columnar_radix_workspace_t *workspace, bool skip_sorted)
{
    uint32_t k8_rows = 0;
    uint32_t k16_rows = 0;
    uint32_t insertion_rows = 0;
    bool has_float = false;
    size_t permutation_bytes = 0;
    size_t temp_column_bytes = 0;
    size_t bucket_bytes = 0;
    size_t count16_bytes = 0;
    size_t insertion_bytes = 0;
    size_t timestamp_bytes = 0;
    size_t workspace_bytes = 0;
    uint64_t total_scratch_bytes;
    int failure_rc = ENOMEM;

    if (!rel || !seg_boundaries || seg_count == 0 || !workspace)
        return EINVAL;
    /* A live workspace owns its buffers and reservation until destroy. */
    if (workspace->admission_active || workspace->perm_a || workspace->perm_b
        || workspace->bucket_values || workspace->byte_values
        || workspace->short_values || workspace->count16
        || workspace->temp_column || workspace->insertion_rows
        || workspace->timestamps || workspace->timestamp_capacity
        || workspace->k8_capacity || workspace->k16_capacity
        || workspace->insertion_capacity || workspace->insertion_bytes
        || workspace->prepared_relation)
        return EINVAL;
    /* Check the entire boundary sequence before comparing any row. Partial
     * relation ranges and empty segments are valid preparation inputs. */
    for (uint32_t s = 0; s < seg_count; s++) {
        if (seg_boundaries[s] > seg_boundaries[s + 1]
            || seg_boundaries[s + 1] > rel->nrows)
            return EINVAL;
    }
    memset(workspace, 0, sizeof(*workspace));
    for (uint32_t c = 0; rel->column_types && c < rel->ncols; c++)
        has_float |= rel->column_types[c] == WIRELOG_TYPE_FLOAT;

    for (uint32_t s = 0; s < seg_count; s++) {
        uint32_t start = seg_boundaries[s];
        uint32_t end = seg_boundaries[s + 1];
        uint32_t count = end - start;
        if (count <= 1)
            continue;
        bool sorted = skip_sorted;
        for (uint32_t row = start + 1; sorted && row < end; row++) {
            if (col_rel_row_cmp(rel, row - 1, row) > 0) {
                sorted = false;
                break;
            }
        }
        if (sorted)
            continue;
        if (has_float || count <= 32) {
            if (count > insertion_rows)
                insertion_rows = count;
        } else if (count >= 50000u) {
            if (count > k16_rows)
                k16_rows = count;
        } else if (count > k8_rows) {
            k8_rows = count;
        }
    }

    uint32_t permutation_rows = k8_rows > k16_rows ? k8_rows : k16_rows;
    if (permutation_rows > 0) {
        if (!col_rel_size_multiply(permutation_rows, sizeof(uint32_t),
            &permutation_bytes)
            || !col_rel_size_multiply(permutation_rows, sizeof(int64_t),
            &temp_column_bytes))
            goto overflow;
        if (!col_rel_size_multiply(permutation_bytes, 2u, &workspace_bytes)
            || !col_rel_size_add(workspace_bytes, temp_column_bytes,
            &workspace_bytes))
            goto overflow;
    }
    if (k8_rows > 0 || k16_rows > 0) {
        size_t k16_bucket_bytes;
        if (!col_rel_size_multiply(k16_rows, sizeof(uint16_t),
            &k16_bucket_bytes))
            goto overflow;
        bucket_bytes = k8_rows > k16_bucket_bytes
            ? k8_rows : k16_bucket_bytes;
        if (!col_rel_size_add(workspace_bytes, bucket_bytes,
            &workspace_bytes))
            goto overflow;
    }
    if (k16_rows > 0) {
        if (!col_rel_size_multiply(65536u, sizeof(uint32_t), &count16_bytes)
            || !col_rel_size_add(workspace_bytes, count16_bytes,
            &workspace_bytes))
            goto overflow;
    }
    if (insertion_rows > 0) {
#if SIZE_MAX <= UINT32_MAX
        if (insertion_rows == UINT32_MAX)
            goto overflow;
#endif
        size_t row_slots = (size_t)insertion_rows + 1u;
        if (!col_rel_size_multiply(row_slots, rel->ncols, &insertion_bytes)
            || !col_rel_size_multiply(insertion_bytes, sizeof(int64_t),
            &insertion_bytes)
            || !col_rel_size_add(workspace_bytes, insertion_bytes,
            &workspace_bytes))
            goto overflow;
    }
    uint32_t timestamp_rows = permutation_rows > insertion_rows
        ? permutation_rows : insertion_rows;
    if (rel->timestamps && timestamp_rows > 0) {
        if (!col_rel_size_multiply(timestamp_rows,
            sizeof(col_delta_timestamp_t), &timestamp_bytes)
            || !col_rel_size_add(workspace_bytes, timestamp_bytes,
            &workspace_bytes))
            goto overflow;
    }
    if ((uint64_t)workspace_bytes > UINT64_MAX - additional_scratch_bytes)
        goto overflow;
    total_scratch_bytes = (uint64_t)workspace_bytes
        + additional_scratch_bytes;
    if (rel->memory_governor && total_scratch_bytes > 0) {
        wl_columnar_memory_reservation_init(&workspace->admission);
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_radix_admission_test_hook)
            wl_columnar_radix_admission_test_hook(rel, workspace_bytes,
                additional_scratch_bytes, total_scratch_bytes,
                WL_COLUMNAR_RADIX_ADMISSION_BEFORE,
                WL_COLUMNAR_MEMORY_ADMISSION_INVALID);
#endif
        wl_columnar_memory_admission_status_t admission_status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(rel->memory_governor),
                total_scratch_bytes, &workspace->admission);
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_radix_admission_test_hook)
            wl_columnar_radix_admission_test_hook(rel, workspace_bytes,
                additional_scratch_bytes, total_scratch_bytes,
                WL_COLUMNAR_RADIX_ADMISSION_AFTER, admission_status);
#endif
        if (admission_status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && admission_status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (admission_status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                ((col_rel_t *)rel)->memory_budget_denial_pending = true;
            failure_rc = admission_status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOMEM : admission_status ==
                WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
            goto no_memory;
        }
        workspace->admission_active = true;
    }

    if (timestamp_bytes > 0) {
        workspace->timestamps = wl_columnar_relation_radix_malloc(
            timestamp_bytes, "radix_workspace_timestamps");
        if (!workspace->timestamps)
            goto no_memory;
        workspace->timestamp_capacity = timestamp_rows;
    }
    if (permutation_rows > 0) {
        workspace->perm_a = (uint32_t *)wl_columnar_relation_radix_malloc(
            permutation_bytes, "radix_workspace_perm_a");
        workspace->perm_b = (uint32_t *)wl_columnar_relation_radix_malloc(
            permutation_bytes, "radix_workspace_perm_b");
        workspace->temp_column = (int64_t *)wl_columnar_relation_radix_malloc(
            temp_column_bytes, "radix_workspace_temp_column");
        if (!workspace->perm_a || !workspace->perm_b
            || !workspace->temp_column)
            goto no_memory;
    }
    if (k8_rows > 0 || k16_rows > 0) {
        workspace->bucket_values = wl_columnar_relation_radix_malloc(
            bucket_bytes,
            "radix_workspace_bucket_values");
        if (!workspace->bucket_values)
            goto no_memory;
        workspace->byte_values = (uint8_t *)workspace->bucket_values;
        workspace->short_values = (uint16_t *)workspace->bucket_values;
        workspace->k8_capacity = k8_rows;
        workspace->k16_capacity = k16_rows;
    }
    if (k16_rows > 0) {
        workspace->count16 = (uint32_t *)wl_columnar_relation_radix_malloc(
            count16_bytes, "radix_workspace_count16");
        if (!workspace->count16)
            goto no_memory;
    }
    if (insertion_rows > 0) {
        workspace->insertion_rows =
            (int64_t *)wl_columnar_relation_radix_malloc(
            insertion_bytes, "radix_workspace_insertion_rows");
        if (!workspace->insertion_rows)
            goto no_memory;
        workspace->insertion_capacity = insertion_rows;
        workspace->insertion_bytes = insertion_bytes;
    }
    /* Exact provenance is available only for a complete single range. Raw
     * multi-segment consumers continue using capacity validation. */
    if (seg_count == 1) {
        workspace->prepared_relation = rel;
        workspace->prepared_identity = rel->relation_identity;
        workspace->prepared_view_generation = rel->view_generation;
        workspace->prepared_storage_generation = rel->storage_generation;
        workspace->prepared_start = seg_boundaries[0];
        workspace->prepared_count = seg_boundaries[1] - seg_boundaries[0];
        workspace->prepared_nrows = rel->nrows;
        workspace->prepared_ncols = rel->ncols;
        workspace->prepared_capacity = rel->capacity;
        workspace->prepared_timestamp_capacity = rel->timestamp_capacity;
        workspace->prepared_has_timestamps = rel->timestamps != NULL;
        workspace->prepared_has_types = rel->column_types != NULL;
        workspace->prepared_type_fingerprint =
            wl_columnar_radix_type_fingerprint(rel);
    }
    return 0;

overflow:
    failure_rc = EOVERFLOW;
no_memory:
    wl_columnar_radix_workspace_destroy(workspace);
    return failure_rc;
}

int
wl_columnar_radix_workspace_prepare(const col_rel_t *rel,
    const uint32_t *seg_boundaries, uint32_t seg_count,
    uint64_t additional_scratch_bytes, wl_columnar_radix_workspace_t *workspace)
{
    return wl_columnar_relation_radix_workspace_prepare(rel, seg_boundaries,
               seg_count, additional_scratch_bytes, workspace, true);
}

/* Check the physical scratch selected by the raw sorter before detaching a
 * shared relation. Insertion buffers depend on both row and column counts. */
static int
wl_columnar_relation_radix_workspace_validate(const col_rel_t *rel,
    uint32_t nrows, const wl_columnar_radix_workspace_t *workspace)
{
    bool insertion = nrows <= 32;
    if (rel->timestamps && (!workspace->timestamps
        || workspace->timestamp_capacity < nrows))
        return EINVAL;
    for (uint32_t c = 0; rel->column_types && c < rel->ncols; c++)
        insertion |= rel->column_types[c] == WIRELOG_TYPE_FLOAT;
    if (insertion) {
        size_t rows, bytes;
        if (!col_rel_size_add(nrows, 1, &rows)
            || !col_rel_size_multiply(rows, rel->ncols, &bytes)
            || !col_rel_size_multiply(bytes, sizeof(int64_t), &bytes))
            return EOVERFLOW;
        return workspace->insertion_rows &&
               workspace->insertion_capacity >= nrows
               && workspace->insertion_bytes >= bytes ? 0 : EINVAL;
    }
    if (!workspace->perm_a || !workspace->perm_b || !workspace->temp_column)
        return EINVAL;
    if (nrows >= 50000)
        return workspace->short_values && workspace->count16
               && workspace->k16_capacity >= nrows ? 0 : EINVAL;
    return workspace->byte_values &&
           workspace->k8_capacity >= nrows ? 0 : EINVAL;
}

/* Validate authority and physical shape before reading keys or allocating. */
static int
wl_columnar_radix_lease_preflight(col_rel_t *r, uint32_t start,
    uint32_t count, wl_columnar_relation_mutation_lease_t *lease)
{
    if (!lease || lease->role_flags != WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        || col_rel_mutation_lease_validate(lease, r))
        return EINVAL;
    if (start > r->nrows || count > r->nrows - start
        || r->nrows > r->capacity || !col_rel_timestamp_shape_valid(r)
        || (r->ncols && !r->columns))
        return EINVAL;
    for (uint32_t c = 0; c < r->ncols; c++)
        if (!r->columns[c])
            return EINVAL;
    if (!wl_columnar_relation_generation_valid(r->view_generation))
        return EINVAL;
    if (r->ncols && count > 1
        && (r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || (r->col_shared && r->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)))
        return EOVERFLOW;
    return 0;
}

void
wl_columnar_relation_radix_sequence_destroy(
    wl_columnar_relation_radix_sequence_t *sequence)
{
    if (!sequence)
        return;
    free(sequence->boundaries);
    free(sequence->needs_sort);
    if (sequence->metadata_admission_active)
        (void)wl_columnar_memory_release(&sequence->metadata_admission);
    wl_columnar_radix_workspace_destroy(&sequence->workspace);
    memset(sequence, 0, sizeof(*sequence));
}

static bool
wl_columnar_memory_reservation_inert(
    const wl_columnar_memory_reservation_t *reservation)
{
    return reservation && !reservation->governor && !reservation->bytes
           && !reservation->replacement_bytes && !reservation->transition_kind
           && !atomic_load_explicit(&reservation->owner_bits,
               memory_order_relaxed)
           && !atomic_load_explicit(&reservation->state, memory_order_relaxed)
           && !reservation->identity;
}

static bool
wl_columnar_radix_workspace_inert(const wl_columnar_radix_workspace_t *w)
{
    return w && !w->perm_a && !w->perm_b && !w->bucket_values
           && !w->byte_values && !w->short_values && !w->count16
           && !w->temp_column && !w->insertion_rows && !w->timestamps
           && !w->timestamp_capacity && !w->k8_capacity && !w->k16_capacity
           && !w->insertion_capacity && !w->insertion_bytes
           && !w->prepared_relation && !w->prepared_identity
           && !w->prepared_view_generation && !w->prepared_storage_generation
           && !w->prepared_type_fingerprint && !w->prepared_start
           && !w->prepared_count && !w->prepared_nrows && !w->prepared_ncols
           && !w->prepared_capacity && !w->prepared_timestamp_capacity
           && !w->prepared_has_timestamps && !w->prepared_has_types
           && !w->admission_active
           && wl_columnar_memory_reservation_inert(&w->admission);
}

int
wl_columnar_relation_radix_sequence_prepare_with_lease(col_rel_t *r,
    const uint32_t *bounds, uint32_t seg_count,
    wl_columnar_relation_radix_sequence_t *sequence,
    wl_columnar_relation_mutation_lease_t *lease)
{
    int rc;
    size_t boundary_count, boundary_bytes, mask_bytes;
    uint64_t extra;
    if (!sequence || sequence->identity || sequence->acquisition_nonce
        || sequence->set || sequence->lease || sequence->relation
        || sequence->owner || sequence->relation_identity
        || sequence->relation_generation || sequence->owner_identity
        || sequence->owner_generation || sequence->view_generation
        || sequence->type_fingerprint || sequence->columns
        || sequence->column_types || sequence->timestamps || sequence->nrows
        || sequence->ncols || sequence->capacity || sequence->timestamp_capacity
        || sequence->boundaries || sequence->needs_sort || sequence->seg_count
        || sequence->needs_sort_count || sequence->metadata_admission_active
        || !wl_columnar_memory_reservation_inert(
            &sequence->metadata_admission)
        || sequence->prepared || sequence->consumed
        || !wl_columnar_radix_workspace_inert(&sequence->workspace))
        return EINVAL;
    if (!r || !bounds || !seg_count)
        return EINVAL;
    if (seg_count == UINT32_MAX)
        return EOVERFLOW;
#if SIZE_MAX <= UINT32_MAX
    if (seg_count > SIZE_MAX / sizeof(uint32_t) - 1u)
        return EOVERFLOW;
#endif
    rc = wl_columnar_radix_lease_preflight(r, 0, 0, lease);
    if (rc)
        return rc;
    boundary_count = (size_t)seg_count + 1u;
    if (boundary_count > SIZE_MAX / sizeof(uint32_t))
        return EOVERFLOW;
    boundary_bytes = boundary_count * sizeof(uint32_t);
    mask_bytes = (size_t)seg_count * sizeof(uint8_t);
    if (boundary_bytes > UINT64_MAX - (uint64_t)mask_bytes)
        return EOVERFLOW;
    extra = (uint64_t)boundary_bytes + (uint64_t)mask_bytes;
    for (uint32_t i = 0; i < seg_count; i++)
        if (bounds[i] > bounds[i + 1] || bounds[i + 1] > r->nrows)
            return EINVAL;
    uint32_t needs_sort_count = 0;
    for (uint32_t i = 0; i < seg_count; i++) {
        uint32_t start = bounds[i];
        uint32_t end = bounds[i + 1];
        bool sorted = end - start <= 1u;
        if (!sorted) {
            sorted = true;
            for (uint32_t row = start + 1u; row < end; row++)
                if (col_rel_row_cmp(r, row - 1u, row) > 0) {
                    sorted = false;
                    break;
                }
        }
        needs_sort_count += !sorted;
    }
    if (needs_sort_count
        && (r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID
        - needs_sort_count
        || (r->col_shared && r->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)))
        return EOVERFLOW;
    if (needs_sort_count && r->col_shared && lease->storage_transitioned)
        return EINVAL;
    if (r->memory_governor && extra) {
        wl_columnar_memory_reservation_init(&sequence->metadata_admission);
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(r->memory_governor),
                extra, &sequence->metadata_admission);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                r->memory_budget_denial_pending = true;
            wl_columnar_relation_radix_sequence_destroy(sequence);
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOMEM
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
        }
        sequence->metadata_admission_active = true;
    }
    sequence->boundaries = malloc(boundary_bytes);
    sequence->needs_sort = calloc(seg_count, sizeof(*sequence->needs_sort));
    if (!sequence->boundaries || !sequence->needs_sort) {
        wl_columnar_relation_radix_sequence_destroy(sequence);
        return ENOMEM;
    }
    memcpy(sequence->boundaries, bounds, boundary_bytes);
    sequence->seg_count = seg_count;
    sequence->needs_sort_count = needs_sort_count;
    for (uint32_t i = 0; i < seg_count; i++) {
        uint32_t start = sequence->boundaries[i];
        uint32_t end = sequence->boundaries[i + 1];
        if (end - start <= 1u)
            continue;
        for (uint32_t row = start + 1u; row < end; row++)
            if (col_rel_row_cmp(r, row - 1u, row) > 0) {
                sequence->needs_sort[i] = 1u;
                break;
            }
    }
    rc = wl_columnar_relation_radix_workspace_prepare(r,
            sequence->boundaries, seg_count, 0, &sequence->workspace, true);
    if (rc) {
        wl_columnar_relation_radix_sequence_destroy(sequence);
        return rc;
    }
    sequence->set = lease->set;
    sequence->lease = lease;
    sequence->relation = r;
    sequence->owner = lease->owner;
    sequence->acquisition_nonce = lease->acquisition_nonce;
    sequence->relation_identity = r->relation_identity;
    sequence->relation_generation = r->storage_generation;
    sequence->owner_identity = lease->owner_identity;
    sequence->owner_generation = lease->owner_generation;
    sequence->view_generation = r->view_generation;
    sequence->type_fingerprint = wl_columnar_radix_type_fingerprint(r);
    sequence->columns = (const int64_t *const *)r->columns;
    sequence->column_types = r->column_types;
    sequence->timestamps = r->timestamps;
    sequence->nrows = r->nrows;
    sequence->ncols = r->ncols;
    sequence->capacity = r->capacity;
    sequence->timestamp_capacity = r->timestamp_capacity;
    sequence->identity = (uintptr_t)sequence;
    sequence->prepared = true;
    return 0;
}

int
wl_columnar_relation_radix_sequence_execute_with_lease(
    wl_columnar_relation_radix_sequence_t *sequence,
    wl_columnar_relation_mutation_lease_t *lease)
{
    if (!sequence || sequence->identity != (uintptr_t)sequence
        || !sequence->prepared || sequence->consumed || !lease
        || !sequence->set || !lease->set
        || sequence->lease != lease || sequence->set != lease->set
        || sequence->acquisition_nonce != lease->acquisition_nonce
        || sequence->relation != lease->relation
        || lease->set->acquisition_nonce != sequence->acquisition_nonce
        || col_rel_mutation_lease_validate(lease, sequence->relation))
        return EINVAL;
    col_rel_t *r = sequence->relation;
    if (r->relation_identity != sequence->relation_identity
        || r->storage_generation != sequence->relation_generation
        || r->view_generation != sequence->view_generation
        || lease->owner != sequence->owner
        || lease->owner_identity != sequence->owner_identity
        || lease->owner_generation != sequence->owner_generation
        || r->columns != (int64_t **)sequence->columns
        || r->column_types != sequence->column_types
        || r->timestamps != sequence->timestamps
        || r->nrows != sequence->nrows || r->ncols != sequence->ncols
        || r->capacity != sequence->capacity
        || r->timestamp_capacity != sequence->timestamp_capacity
        || wl_columnar_radix_type_fingerprint(r) != sequence->type_fingerprint)
        return EINVAL;
    if (!sequence->needs_sort_count) {
        sequence->consumed = true;
        return 0;
    }
    for (uint32_t i = 0; i < sequence->seg_count; i++) {
        if (!sequence->needs_sort[i])
            continue;
        uint32_t count = sequence->boundaries[i + 1]
            - sequence->boundaries[i];
        int rc = wl_columnar_relation_radix_workspace_validate(r, count,
                &sequence->workspace);
        if (rc)
            return rc;
    }
    if (r->col_shared) {
        int rc = col_rel_cow_unshare_publish_impl(r, 0, false, lease);
        if (rc)
            return rc;
    }
    for (uint32_t i = 0; i < sequence->seg_count; i++) {
        if (!sequence->needs_sort[i])
            continue;
        lease->set->published = true;
        sequence->consumed = true;
        int rc = wl_columnar_radix_rows_prepared(r,
                sequence->boundaries[i], sequence->boundaries[i + 1]
                - sequence->boundaries[i], &sequence->workspace,
                r->timestamps ? sequence->workspace.timestamps : NULL);
        if (rc)
            abort();
    }
    return 0;
}

int
wl_columnar_relation_radix_workspace_prepare_with_lease(col_rel_t *r,
    uint32_t start, uint32_t count,
    wl_columnar_radix_workspace_t *workspace,
    wl_columnar_relation_mutation_lease_t *lease)
{
    int rc = wl_columnar_radix_lease_preflight(r, start, count, lease);
    if (rc)
        return rc;
    uint32_t bounds[] = { start, start + count };
    return wl_columnar_relation_radix_workspace_prepare(r, bounds, 1, 0,
               workspace, false);
}

static int
wl_columnar_relation_radix_sort_with_lease_impl(col_rel_t *r, uint32_t start,
    uint32_t count, const wl_columnar_radix_workspace_t *workspace,
    wl_columnar_relation_mutation_lease_t *lease, bool consolidation_hook)
{
    int rc = wl_columnar_radix_lease_preflight(r, start, count, lease);
    if (rc)
        return rc;
    if (!workspace)
        return EINVAL;
    if (workspace->prepared_relation != r
        || workspace->prepared_identity != r->relation_identity
        || workspace->prepared_view_generation != r->view_generation
        || workspace->prepared_storage_generation != r->storage_generation
        || workspace->prepared_start != start ||
        workspace->prepared_count != count
        || workspace->prepared_nrows != r->nrows
        || workspace->prepared_ncols != r->ncols
        || workspace->prepared_capacity != r->capacity
        || workspace->prepared_timestamp_capacity != r->timestamp_capacity
        || workspace->prepared_has_timestamps != (r->timestamps != NULL)
        || workspace->prepared_has_types != (r->column_types != NULL)
        || workspace->prepared_type_fingerprint !=
        wl_columnar_radix_type_fingerprint(r))
        return EINVAL;
    if (!r->ncols || count <= 1)
        return 0;
    rc = wl_columnar_relation_radix_workspace_validate(r, count, workspace);
    if (rc || workspace->byte_values != workspace->bucket_values
        || (void *)workspace->short_values != workspace->bucket_values)
        return rc ? rc : EINVAL;
    if (r->col_shared) {
        rc = col_rel_cow_unshare_publish_impl(r, 0, false, lease);
        if (rc)
            return rc;
#ifdef WL_TEST_CONSOLIDATE_HOOK
        if (consolidation_hook
            && wl_columnar_consolidation_transition_hook)
            wl_columnar_consolidation_transition_hook(r,
                WL_COLUMNAR_CONSOLIDATION_TEST_SORT_AFTER_DETACH);
#else
        (void)consolidation_hook;
#endif
    }
    /* After detach/first row move, prepared-only kernels cannot fail. */
    rc = wl_columnar_radix_rows_prepared(r, start, count, workspace,
            r->timestamps ? workspace->timestamps : NULL);
    if (rc)
        abort();
    return 0;
}

int
wl_columnar_relation_radix_sort_with_lease(col_rel_t *r, uint32_t start,
    uint32_t count, const wl_columnar_radix_workspace_t *workspace,
    wl_columnar_relation_mutation_lease_t *lease)
{
    return wl_columnar_relation_radix_sort_with_lease_impl(r, start, count,
               workspace, lease, false);
}

int
wl_columnar_relation_radix_sort_consolidation_with_lease(col_rel_t *r,
    uint32_t start, uint32_t count,
    const wl_columnar_radix_workspace_t *workspace,
    wl_columnar_relation_mutation_lease_t *lease)
{
    return wl_columnar_relation_radix_sort_with_lease_impl(r, start, count,
               workspace, lease, true);
}

/* One descriptor-first mutation set covers range capture, preparation,
 * detach, permutation and optional full-sort metadata publication. */
static int
wl_columnar_radix_sort_admitted(col_rel_t *r, uint32_t start,
    uint32_t count, bool full_relation)
{
    wl_columnar_relation_mutation_single_t single = { 0 };
    wl_columnar_radix_workspace_t workspace = { 0 };
    if (!r)
        return EINVAL;
    if (!full_relation && count > UINT32_MAX - start)
        return EINVAL;
    int rc = col_rel_mutation_single_acquire(r, &single);
    if (rc)
        return rc;
    r->memory_budget_denial_pending = false;
    if (full_relation) {
        start = 0;
        count = r->nrows;
    }
    rc = wl_columnar_radix_lease_preflight(r, start, count, &single.lease);
    if (!rc) {
        rc = wl_columnar_relation_radix_workspace_prepare_with_lease(r,
                start, count, &workspace, &single.lease);
    }
    if (!rc)
        rc = wl_columnar_relation_radix_sort_with_lease(r, start, count,
                &workspace, &single.lease);
    if (!rc && full_relation)
        r->sorted_nrows = count;
    wl_columnar_radix_workspace_destroy(&workspace);
    if (col_rel_mutation_set_finish(&single.set, rc == 0))
        abort();
    return rc;
}

int
col_rel_radix_sort(col_rel_t *r, uint32_t start_row, uint32_t nrows)
{
    return wl_columnar_radix_sort_admitted(r, start_row, nrows, false);
}

int
col_rel_radix_sort_int64(col_rel_t *r)
{
    return r ? wl_columnar_radix_sort_admitted(r, 0, 0, true) : 0;
}

int
wl_columnar_relation_rename_checked(col_rel_t *rel, const char *name)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_reservation_t old_reservation;
    wl_columnar_memory_admission_status_t status;
    uint64_t bytes = 0;
    bool governed;
    char *replacement;
    int rc;

    if (!rel || !name)
        return EINVAL;
    rc = wl_columnar_source_access_writer_acquire(&rel->descriptor_access,
            &writer);
    if (rc != 0)
        return rc;
    governed = rel->memory_governor != NULL;
    if (governed) {
        if (strlen(name) == SIZE_MAX) {
            rc = EOVERFLOW;
            goto fail;
        }
        bytes = (uint64_t)strlen(name) + 1u;
        if (!rel->pool_owned
            && !wl_columnar_memory_size_add(sizeof(*rel), bytes, &bytes)) {
            rc = EOVERFLOW;
            goto fail;
        }
        wl_columnar_memory_reservation_init(&pending);
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(rel->memory_governor),
            bytes, &pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
            if (rc == ENOSPC)
                rel->memory_budget_denial_pending = true;
            goto fail;
        }
    }
    replacement = wl_strdup(name);
    if (!replacement) {
        rc = ENOMEM;
        if (governed && !wl_columnar_memory_rollback(&pending))
            abort();
        goto fail;
    }
    if (governed) {
        wl_columnar_memory_reservation_t *current = rel->pool_owned
            ? &rel->pool_name_reservation : &rel->descriptor_reservation;
        bool had_old = rel->pool_owned
            ? rel->pool_name_reserved_bytes != 0
            : rel->descriptor_reserved_bytes != 0;
        wl_columnar_memory_reservation_init(&old_reservation);
        if (!wl_columnar_memory_commit(&pending, rel)
            || (had_old && !wl_columnar_memory_reservation_move(
                &old_reservation, current))
            || !wl_columnar_memory_reservation_move(current, &pending))
            abort();
    }
    free(rel->name);
    rel->name = replacement;
    if (governed) {
        if (rel->pool_owned)
            rel->pool_name_reserved_bytes = bytes;
        else
            rel->descriptor_reserved_bytes = bytes;
        if (old_reservation.bytes
            && !wl_columnar_memory_release(&old_reservation))
            abort();
    }
    return wl_columnar_source_access_writer_release(&writer);
fail:
    if (wl_columnar_source_access_writer_release(&writer) != 0)
        return EINVAL;
    return rc;
}

int
wl_columnar_relation_consolidate_retire(
    wl_columnar_consolidate_record_t *record, int primary)
{
    if (!record || !record->ready) return primary > 0 ? primary : EINVAL;
    if (record->phase == WL_COLUMNAR_CONSOLIDATE_RESETTING
        || record->phase == WL_COLUMNAR_CONSOLIDATE_INERT
        || record->phase == WL_COLUMNAR_CONSOLIDATE_RETIRED) return primary;
    if (record->phase < WL_COLUMNAR_CONSOLIDATE_PREPARING
        || record->phase > WL_COLUMNAR_CONSOLIDATE_RETIRING) return primary >
               0 ? primary : EINVAL;
    if (record->lane == WL_COLUMNAR_CONSOLIDATE_HASH)
        return wl_columnar_merge_hash_retire(record, primary, NULL);
    if (record->lane == WL_COLUMNAR_CONSOLIDATE_LANE_INITIALIZING) {
        record->phase = WL_COLUMNAR_CONSOLIDATE_RETIRED;
        return primary;
    }
    if (record->lane != WL_COLUMNAR_CONSOLIDATE_RADIX
        && record->lane != WL_COLUMNAR_CONSOLIDATE_RADIX_MERGE)
        return primary > 0 ? primary : EINVAL;
    record->phase = WL_COLUMNAR_CONSOLIDATE_RETIRING;
    wl_columnar_relation_radix_sequence_t *seq = &record->sequence;
    wl_columnar_radix_workspace_t *w = &seq->workspace;
    free(seq->boundaries); seq->boundaries = NULL;
    free(seq->needs_sort); seq->needs_sort = NULL;
    free(w->perm_a); w->perm_a = NULL;
    free(w->perm_b); w->perm_b = NULL;
    free(w->temp_column); w->temp_column = NULL;
    free(w->bucket_values); w->bucket_values = NULL;
    w->byte_values = NULL; w->short_values = NULL;
    free(w->count16); w->count16 = NULL;
    free(w->insertion_rows); w->insertion_rows = NULL;
    free(record->starts); record->starts = NULL;
    if (record->lane != WL_COLUMNAR_CONSOLIDATE_RADIX_MERGE)
        free(record->ends);
    record->ends = NULL;
    wl_columnar_memory_reservation_t *tokens[3] = {
        &seq->metadata_admission, &w->admission, &record->merge_admission
    };
    bool *flags[3] = {&seq->metadata_admission_active,
                      &w->admission_active, &record->merge_admission_active};
    for (unsigned i = 0; i < 3; i++) {
        if (!*flags[i]) continue;
        uint64_t state = atomic_load(&tokens[i]->state);
        bool released = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            ? wl_columnar_memory_release(tokens[i])
            : state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            && wl_columnar_memory_rollback(tokens[i]);
        if (!released) return primary > 0 ? primary : EINVAL;
        *flags[i] = false;
    }
    record->phase = WL_COLUMNAR_CONSOLIDATE_RETIRED;
    return primary;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_consolidate_test_checkpoint)(wl_evaluation_control_t *, int,
    uint64_t, wl_columnar_consolidate_record_t *, col_rel_t *);
int wl_columnar_consolidate_test_fail_allocation = -1;
bool wl_columnar_consolidate_test_fail_commit;
#endif
void
wl_columnar_relation_consolidate_checkpoint(wl_evaluation_control_t *c,
    int stage, uint64_t progress, wl_columnar_consolidate_record_t *record,
    col_rel_t *r)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_consolidate_test_checkpoint)
        wl_columnar_consolidate_test_checkpoint(c, stage, progress, record, r);
#else
    (void)c; (void)stage; (void)progress; (void)record; (void)r;
#endif
}
static int
wl_columnar_relation_consolidate_step(wl_evaluation_control_t *c,
    int stage, uint64_t progress, col_rel_t *r)
{
    wl_columnar_relation_consolidate_checkpoint(c, stage, progress, NULL, r);
    return wl_evaluation_control_charge(c, 0);
}
/* Only a quiescent new attempt may reuse successful checked-destroy slots.
 * This validates descriptor headers, never destroys payload or returns credit. */
int
wl_columnar_relation_pool_reuse_controlled(wl_col_session_t *sess,
    wl_evaluation_control_t *c)
{
    delta_pool_t *pool = sess->delta_pool;
    int rc = wl_evaluation_control_charge(c, 64);
    if (rc) return rc;
    if (!pool || !c) return EINVAL;
    if (sess->cleanup_pending || sess->cleanup_active
        || sess->cleanup_pending_count || sess->cleanup_active_count
        || sess->cleanup_reserved_bytes || sess->serial_completion.frame
        || sess->serial_completion.mode_frame || sess->serial_completion.pending
        || sess->serial_completion.active || sess->source_leases
        || sess->coordinator || sess->deferred_relations
        || sess->deferred_relation_count || pool->arena_used) return ENOTSUP;
    char *slab = pool->slab;
    size_t stride = pool->slot_size;
    uint32_t used = pool->slot_used, cap = pool->slot_cap;
#ifdef _MSC_VER
    const size_t alignment = __alignof(col_rel_t);
#else
    const size_t alignment = _Alignof(col_rel_t);
#endif
    if (used > cap || stride < sizeof(col_rel_t) || !slab
        || (uintptr_t)slab % alignment
        || stride % alignment) return EINVAL;
    if (cap && stride > SIZE_MAX / cap) return EOVERFLOW;
    size_t bytes = stride * cap, live_bytes = stride * used;
    uintptr_t first = (uintptr_t)slab;
    if (bytes > UINTPTR_MAX - first) return EOVERFLOW;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    if (sess->nrels && !sess->rels) return EINVAL;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
        uintptr_t address = (uintptr_t)sess->rels[i];
        if (address >= first && address - first < live_bytes) return ENOTSUP;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    }
    for (uint32_t i = 0; i < used; i++) {
        col_rel_t *r = (col_rel_t *)(slab + stride * i);
        rc = wl_evaluation_control_charge(c, 96); if (rc) return rc;
        if (!r->pool_owned || r->relation_identity
            || r->view_generation != WL_COLUMNAR_REL_GENERATION_INVALID
            || r->storage_generation != WL_COLUMNAR_REL_GENERATION_INVALID
            || r->storage_owner || r->storage_owner_identity
            || r->storage_owner_generation || r->deferred_relation_next
            || r->deferred_relation_session
            || atomic_load_explicit(&r->storage_alias_borrows,
            memory_order_acquire)
            || atomic_load_explicit(&r->source_access.state,
            memory_order_acquire)
            != WL_COLUMNAR_SOURCE_ACCESS_WRITER
            || atomic_load_explicit(&r->descriptor_access.state,
            memory_order_acquire)
            != WL_COLUMNAR_SOURCE_ACCESS_WRITER) return ENOTSUP;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
        if (r->name || r->ncols || r->columns || r->column_types || r->nrows
            || r->capacity || r->col_names || r->schema_ok || r->sorted_nrows
            || r->merge_columns || r->merge_buf_cap || r->base_nrows
            || r->timestamps || r->timestamp_capacity || r->arena_owned
            || r->mem_ledger || r->memory_governor || r->row_scratch
            || r->schema.format || r->schema.name || r->schema.metadata
            || r->schema.flags || r->schema.n_children || r->schema.children
            || r->schema.dictionary || r->schema.release ||
            r->schema.private_data
            || r->descriptor_reserved_bytes || r->metadata_reserved_bytes
            || r->pool_name_reserved_bytes || r->dedup_reserved_bytes
            || r->retained_reserved_bytes || r->memory_budget_denial_pending
            || r->ledger_ts_bytes) return ENOTSUP;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        rc = wl_evaluation_control_charge(c, 96); if (rc) return rc;
        if (r->retract_backup_columns || r->retract_backup_timestamps
            || r->retract_backup_timestamp_capacity || r->retract_backup_nrows
            || r->retract_backup_base_nrows || r->retract_backup_capacity
            || r->retract_backup_sorted_nrows || r->col_shared || r->run_count
            || r->retract_backup_run_count || r->dedup_slots || r->dedup_cap
            || r->dedup_count || r->has_graph_column || r->graph_col_idx
            || r->compound_kind || r->compound_count || r->compound_arity_map
            || r->compound_arity_len || r->inline_physical_offset
            || r->declared_ncols) return ENOTSUP;
        for (uint32_t j = 0; j < COL_MAX_RUNS; j++) {
            if (r->run_ends[j] || r->retract_backup_run_ends[j]) return ENOTSUP;
        }

        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        const wl_columnar_memory_reservation_t *tokens[5] = {
            &r->descriptor_reservation, &r->metadata_reservation,
            &r->pool_name_reservation, &r->dedup_reservation,
            &r->retained_reservation
        };
        for (uint32_t j = 0; j < 5u; j++) {
            rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
            const wl_columnar_memory_reservation_t *t = tokens[j];
            if (t->governor || t->bytes || t->replacement_bytes
                || t->transition_kind || t->identity != (j == 4u ? t : NULL)
                || atomic_load_explicit(&t->owner_bits, memory_order_acquire)
                || atomic_load_explicit(&t->state, memory_order_acquire)
                != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY) return ENOTSUP;
            rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        }
        rc = wl_columnar_relation_consolidate_step(c, 122, i + 1u, r);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    if (pool != sess->delta_pool || pool->slab != slab
        || pool->slot_size != stride || pool->slot_cap != cap
        || pool->slot_used != used || pool->arena_used) return EINVAL;
    rc = wl_columnar_relation_consolidate_step(c, 123, used, NULL);
    if (rc) return rc;
    pool->slot_used = 0;
    return 0;
}

int
wl_columnar_relation_consolidate_allocate_controlled(void **pointer,
    size_t bytes,
    wl_evaluation_control_t *c)
{
    if (*pointer || !bytes) return EINVAL;
    int rc = wl_evaluation_control_charge(c, 1);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_consolidate_test_fail_allocation == 0) {
        wl_columnar_consolidate_test_fail_allocation = -1;
        return ENOMEM;
    }
    if (wl_columnar_consolidate_test_fail_allocation > 0)
        wl_columnar_consolidate_test_fail_allocation--;
#endif
    *pointer = malloc(bytes);
    if (!*pointer) return ENOMEM;
    return wl_evaluation_control_charge(c, 0);
}
static int
wl_columnar_relation_consolidate_reserve(col_rel_t *r, uint64_t bytes,
    wl_columnar_memory_reservation_t *token, bool *active,
    wl_evaluation_control_t *c)
{
    if (!bytes || !r->memory_governor) return 0;
    wl_columnar_memory_admission_status_t status;
    int rc = wl_columnar_memory_governor_reserve_checked_controlled(
        wl_columnar_memory_governor_ref_get(r->memory_governor), bytes, token,
        &status, c);
    if (rc) return rc;
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK &&
        status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY){
        if (status ==
            WL_COLUMNAR_MEMORY_ADMISSION_DENIED) r->memory_budget_denial_pending
                = true;
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOMEM
            : status ==
               WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
    }
    *active = true;
    return 0;
}
static bool
wl_columnar_relation_consolidate_reservation_empty(
    const wl_columnar_memory_reservation_t *r)
{
    return r && r->identity == r && !r->governor && !r->bytes
           && !r->replacement_bytes && !r->transition_kind
           && !atomic_load_explicit(&r->owner_bits, memory_order_relaxed)
           && atomic_load_explicit(&r->state, memory_order_relaxed)
           == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY;
}
/* Restrict generic validation to the actual single private writer before
 * entering its lease-membership loop. No caller-provided cardinality is walked. */
static int
wl_columnar_relation_consolidate_lease_validate(
    wl_columnar_relation_mutation_lease_t *lease, col_rel_t *r)
{
    if (!lease || !lease->set) return EINVAL;
    const wl_columnar_relation_mutation_set_t *set = lease->set;
    if (set->lease_count != 1 || set->leases != lease
        || set->descriptor_count != 1 || set->owner_count != 1
        || set->initialization_count || lease->owner != r
        || lease->descriptor_slot || lease->owner_slot ||
        lease->self_owner_slot)
        return EINVAL;
    return col_rel_mutation_lease_validate(lease, r);
}
int
wl_columnar_relation_consolidate_commit_controlled(
    wl_columnar_memory_reservation_t *token,
    bool active, const void *owner, wl_evaluation_control_t *c)
{
    if (!active) return 0;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_consolidate_test_fail_commit) {
        wl_columnar_consolidate_test_fail_commit = false; return ENOMEM;
    }
#endif
    bool committed;
    int rc = wl_columnar_memory_governor_commit_controlled(token, owner,
            &committed, c);
    return rc ? rc : committed ? 0 : ENOMEM;
}
int
wl_columnar_relation_radix_type_fingerprint_controlled(const col_rel_t *r,
    uint64_t *out, wl_evaluation_control_t *c)
{
    if (!r || !out || !c) return EINVAL;
    uint64_t h = UINT64_C(14695981039346656037);
    for (uint32_t col = 0; r->column_types && col < r->ncols; col++) {
        int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        h ^= r->column_types[col]; h *= UINT64_C(1099511628211);
        rc = wl_columnar_relation_consolidate_step(c, 3, col + 1u,
                (col_rel_t *)r);
        if (rc) return rc;
    }
    *out = h; return 0;
}
static int
wl_columnar_relation_consolidate_compare(const col_rel_t *r, uint32_t a,
    uint32_t b, const int64_t *saved, int *result, wl_evaluation_control_t *c,
    uint64_t *visits)
{
    for (uint32_t col = 0; col < r->ncols; col++) {
        int rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
        int64_t av = r->columns[col][a],
            bv = saved ? saved[col] : r->columns[col][b];
        if (r->column_types && r->column_types[col] == WIRELOG_TYPE_FLOAT) {
            if (!wl_columnar_float_bits_valid(av) ||
                !wl_columnar_float_bits_valid(bv)) return EINVAL;
            *result = wl_columnar_float_compare_bits(av, bv);
        } else *result = (av > bv) - (av < bv);
        rc = wl_columnar_relation_consolidate_step(c, 4, ++*visits,
                (col_rel_t *)r);
        if (rc) return rc;
        if (*result) return 0;
    }
    *result = 0; return 0;
}
static int
wl_columnar_relation_consolidate_sorted(col_rel_t *r, uint32_t start,
    uint32_t count, bool *sorted, wl_evaluation_control_t *c)
{
    uint64_t visits = 0; *sorted = true;
    if (count <= 1u) return 0;
    for (uint32_t row = start + 1u; row < start + count; row++) {
        int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        int cmp;
        rc = wl_columnar_relation_consolidate_compare(r, row - 1u, row, NULL,
                &cmp, c, &visits);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        if (cmp > 0) {
            *sorted = false; break;
        }
    }
    return 0;
}
int
wl_columnar_relation_radix_workspace_prepare_with_lease_controlled(col_rel_t *r,
    uint32_t start, uint32_t count, wl_columnar_radix_workspace_t *w,
    wl_columnar_relation_mutation_lease_t *lease, wl_evaluation_control_t *c)
{
    int rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    if (!r || !w || !lease || start || count != r->nrows
        || r->nrows > r->capacity || r->timestamps || r->col_shared ||
        r->arena_owned
        || w->perm_a || w->perm_b || w->temp_column || w->byte_values
        || w->bucket_values || w->short_values || w->count16 || w->timestamps
        || w->insertion_rows || w->insertion_bytes || w->k8_capacity
        || w->k16_capacity || w->insertion_capacity || w->timestamp_capacity
        || w->prepared_relation ||
        w->admission_active
        || !wl_columnar_relation_consolidate_reservation_empty(&w->admission)
        || wl_columnar_relation_consolidate_lease_validate(lease,
        r)) return EINVAL;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    bool has_float = false;
    for (uint32_t col = 0; col < r->ncols; col++) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        uint32_t type =
            r->column_types ? r->column_types[col] : WIRELOG_TYPE_INT64;
        if (!r->columns || !r->columns[col] || (type != WIRELOG_TYPE_INT64
            && type != WIRELOG_TYPE_FLOAT &&
            type != WIRELOG_TYPE_STRING)) return EINVAL;
        has_float |= type == WIRELOG_TYPE_FLOAT;
        rc = wl_columnar_relation_consolidate_step(c, 2, col + 1u, r);
        if (rc) return rc;
    }
    bool sorted;
    rc = wl_columnar_relation_consolidate_sorted(r, start, count, &sorted, c);
    if (rc) return rc;
    uint64_t bytes = 0, lane = 0, perm = 0, buckets = 0;
    rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
    bool insertion = !sorted && (has_float || count <= 32u);
    if (insertion) {
        if (!wl_columnar_memory_size_mul((uint64_t)count + 1u, r->ncols, &bytes)
            || !wl_columnar_memory_size_mul(bytes, sizeof(int64_t),
            &bytes)) return EOVERFLOW;
    } else if (!sorted) {
        if (!wl_columnar_memory_size_mul(count, sizeof(uint32_t), &perm)
            || !wl_columnar_memory_size_mul(count, sizeof(int64_t), &lane)
            || !wl_columnar_memory_size_mul(perm, 2u, &bytes)
            || !wl_columnar_memory_size_add(bytes, lane, &bytes)
            || !wl_columnar_memory_size_mul(count,
            count >= 50000u ? 2u : 1u, &buckets)
            || !wl_columnar_memory_size_add(bytes, buckets, &bytes)
            || (count >= 50000u && !wl_columnar_memory_size_add(bytes,
            UINT64_C(65536) * sizeof(uint32_t), &bytes))) return EOVERFLOW;
    }
    if (bytes > SIZE_MAX) return EOVERFLOW;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    rc = wl_columnar_relation_consolidate_reserve(r, bytes, &w->admission,
            &w->admission_active, c);
    if (rc) return rc;
    if (bytes && insertion) {
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->insertion_rows, (size_t)bytes, c);
        if (rc) return rc;
        w->insertion_bytes = (size_t)bytes; w->insertion_capacity = count;
    } else if (bytes) {
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->perm_a,
            (size_t)perm, c); if (rc) return rc;
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->perm_b,
            (size_t)perm, c); if (rc) return rc;
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->temp_column,
            (size_t)lane, c); if (rc) return rc;
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->bucket_values,
            (size_t)buckets, c); if (rc) return rc;
        w->byte_values = w->bucket_values;
        w->short_values = w->bucket_values;
        if (count >= 50000u) {
            rc = wl_columnar_relation_consolidate_allocate_controlled(
                (void **)&w->count16, 65536u * sizeof(uint32_t), c);
            if (rc) return rc;
            w->k16_capacity = count;
        } else w->k8_capacity = count;
    }
    rc = wl_columnar_relation_consolidate_commit_controlled(&w->admission,
            w->admission_active, w, c); if (rc) return rc;
    uint64_t hash;
    rc = wl_columnar_relation_radix_type_fingerprint_controlled(r, &hash, c);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 96); if (rc) return rc;
    w->prepared_relation = r; w->prepared_identity = r->relation_identity;
    w->prepared_view_generation = r->view_generation;
    w->prepared_storage_generation = r->storage_generation;
    w->prepared_type_fingerprint = hash; w->prepared_start = start;
    w->prepared_count = count;
    w->prepared_nrows = r->nrows; w->prepared_ncols = r->ncols;
    w->prepared_capacity = r->capacity;
    w->prepared_has_types = r->column_types != NULL;
    return wl_evaluation_control_charge(c, 0);
}
static int
wl_columnar_relation_segment_sequence_prepare(col_rel_t *r,
    const uint32_t *bounds, uint32_t count,
    wl_columnar_relation_radix_sequence_t *seq,
    wl_columnar_relation_mutation_lease_t *lease, wl_evaluation_control_t *c)
{
    int rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    if (!r || !bounds || !seq || !lease || !c || count < 2u
        || count == UINT32_MAX || r->nrows > r->capacity || r->timestamps
        || r->arena_owned || r->col_shared || seq->identity || seq->prepared
        || seq->consumed || seq->boundaries || seq->needs_sort
        || seq->metadata_admission_active || seq->workspace.admission_active
        || !wl_columnar_relation_consolidate_reservation_empty(
            &seq->metadata_admission)
        || !wl_columnar_relation_consolidate_reservation_empty(
            &seq->workspace.admission)
        || wl_columnar_relation_consolidate_lease_validate(lease,
        r)) return EINVAL;
    const wl_columnar_radix_workspace_t *inert = &seq->workspace;
    if (!(inert && !inert->perm_a && !inert->perm_b && !inert->bucket_values
        && !inert->byte_values && !inert->short_values && !inert->count16
        && !inert->temp_column && !inert->insertion_rows && !inert->timestamps
        && !inert->timestamp_capacity && !inert->k8_capacity &&
        !inert->k16_capacity
        && !inert->insertion_capacity && !inert->insertion_bytes
        && !inert->prepared_relation && !inert->prepared_identity
        && !inert->prepared_view_generation &&
        !inert->prepared_storage_generation
        && !inert->prepared_type_fingerprint && !inert->prepared_start
        && !inert->prepared_count && !inert->prepared_nrows &&
        !inert->prepared_ncols
        && !inert->prepared_capacity && !inert->prepared_timestamp_capacity
        && !inert->prepared_has_timestamps && !inert->prepared_has_types
        && !inert->admission_active
        )) return EINVAL;
    /* Inert accepts initialized original EMPTY tokens as well as zero ones. */
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    bool has_float = false;
    for (uint32_t col = 0; col < r->ncols; col++) {
        rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
        uint32_t type =
            r->column_types ? r->column_types[col] : WIRELOG_TYPE_INT64;
        if (!r->columns || !r->columns[col] || (type != WIRELOG_TYPE_INT64
            && type != WIRELOG_TYPE_FLOAT &&
            type != WIRELOG_TYPE_STRING)) return EINVAL;
        has_float |= type == WIRELOG_TYPE_FLOAT;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    if (bounds[0] || bounds[count] != r->nrows) return EINVAL;
    uint64_t metadata, boundary_bytes;
    if (!wl_columnar_memory_size_mul((uint64_t)count + 1u, 4u, &boundary_bytes)
        || !wl_columnar_memory_size_add(boundary_bytes, count, &metadata)
        || boundary_bytes > SIZE_MAX || metadata > SIZE_MAX) return EOVERFLOW;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    uint32_t unsorted = 0;
    for (uint32_t i = 0; i < count; i++) {
        rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
        if (bounds[i] > bounds[i + 1u] ||
            bounds[i + 1u] > r->nrows) return EINVAL;
        bool sorted;
        rc = wl_columnar_relation_consolidate_sorted(r, bounds[i],
                bounds[i + 1u] - bounds[i], &sorted, c); if (rc) return rc;
        unsorted += !sorted;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    if ((uint64_t)unsorted + 1u >= WL_COLUMNAR_REL_GENERATION_INVALID
        || r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID
        - ((uint64_t)unsorted + 1u)) return EOVERFLOW;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    rc = wl_columnar_relation_consolidate_reserve(r, metadata,
            &seq->metadata_admission, &seq->metadata_admission_active, c);
    if (rc) return rc;
    rc =
        wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&seq->boundaries,
        (size_t)boundary_bytes, c); if (rc) return rc;
    rc =
        wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&seq->needs_sort,
        count, c); if (rc) return rc;
    uint32_t k8 = 0, k16 = 0, insertion = 0;
    for (uint32_t i = 0; i <= count; i++) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        seq->boundaries[i] = bounds[i];
        rc = wl_columnar_relation_consolidate_step(c, 100, (uint64_t)i + 1u, r);
        if (rc) return rc;
    }
    for (uint32_t i = 0; i < count; i++) {
        bool sorted;
        uint32_t n = bounds[i + 1u] - bounds[i];
        rc = wl_columnar_relation_consolidate_sorted(r, bounds[i], n, &sorted,
                c);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
        seq->needs_sort[i] = !sorted;
        if (!sorted && n && r->ncols) {
            if (has_float || n <= 32u) {
                if (n > insertion) insertion = n;
            }else if (n >= 50000u) {
                if (n > k16) k16 = n;
            }else if (n > k8) k8 = n;
        }
        rc = wl_columnar_relation_consolidate_step(c, 101, (uint64_t)i + 1u, r);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
    seq->seg_count = count; seq->needs_sort_count = unsorted;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    rc =
        wl_columnar_relation_consolidate_commit_controlled(
        &seq->metadata_admission,
        seq->metadata_admission_active, seq, c); if (rc) return rc;
    uint64_t p = k8 > k16 ? k8 : k16;
    uint64_t bucket = k8 > (uint64_t)k16 * 2u ? k8 : (uint64_t)k16 * 2u;
    uint64_t bytes, inserted = 0;
    rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
    if (!wl_columnar_memory_size_mul(p, 16u, &bytes)
        || !wl_columnar_memory_size_add(bytes, bucket, &bytes)
        || (k16 && !wl_columnar_memory_size_add(bytes, 262144u, &bytes))
        || (insertion && (!wl_columnar_memory_size_mul((uint64_t)insertion + 1u,
        r->ncols, &inserted) || !wl_columnar_memory_size_mul(inserted, 8u,
        &inserted)))
        || !wl_columnar_memory_size_add(bytes, inserted,
        &bytes) || bytes > SIZE_MAX)
        return EOVERFLOW;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    wl_columnar_radix_workspace_t *w = &seq->workspace;
    rc = wl_columnar_relation_consolidate_reserve(r, bytes, &w->admission,
            &w->admission_active, c); if (rc) return rc;
    if (p) {
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->perm_a,
            (size_t)p * 4u, c); if (rc) return rc;
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->perm_b,
            (size_t)p * 4u, c); if (rc) return rc;
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->temp_column,
            (size_t)p * 8u, c); if (rc) return rc;
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            &w->bucket_values,
            (size_t)bucket, c); if (rc) return rc;
        rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
        w->byte_values = w->bucket_values; w->short_values = w->bucket_values;
        w->k8_capacity = k8; w->k16_capacity = k16;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    }
    if (k16) {
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->count16,
            262144u, c); if (rc) return rc;
    }
    if (inserted) {
        rc =
            wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&w->insertion_rows,
            (size_t)inserted, c); if (rc) return rc;
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        w->insertion_bytes = (size_t)inserted;
        w->insertion_capacity = insertion;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    }
    rc = wl_columnar_relation_consolidate_commit_controlled(&w->admission,
            w->admission_active, w, c); if (rc) return rc;
    uint64_t hash;
    rc = wl_columnar_relation_radix_type_fingerprint_controlled(r, &hash, c);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 192); if (rc) return rc;
    w->prepared_relation = r; w->prepared_identity = r->relation_identity;
    w->prepared_view_generation = r->view_generation;
    w->prepared_storage_generation = r->storage_generation;
    w->prepared_type_fingerprint = hash; w->prepared_start = 0;
    w->prepared_count = r->nrows; w->prepared_nrows = r->nrows;
    w->prepared_ncols = r->ncols; w->prepared_capacity = r->capacity;
    w->prepared_has_types = r->column_types != NULL;
    seq->set = lease->set; seq->lease = lease; seq->relation = r;
    seq->owner = lease->owner;
    seq->acquisition_nonce = lease->acquisition_nonce;
    seq->relation_identity = r->relation_identity;
    seq->relation_generation = r->storage_generation;
    seq->owner_identity = lease->owner_identity;
    seq->owner_generation = lease->owner_generation;
    seq->view_generation = r->view_generation; seq->type_fingerprint = hash;
    seq->columns = (const int64_t *const *)r->columns;
    seq->column_types = r->column_types;
    seq->nrows = r->nrows; seq->ncols = r->ncols; seq->capacity = r->capacity;
    seq->identity = (uintptr_t)seq; seq->prepared = true;
    return wl_evaluation_control_charge(c, 0);
}

int
wl_columnar_relation_radix_sequence_prepare_with_lease_controlled(col_rel_t *r,
    const uint32_t *bounds, uint32_t seg_count,
    wl_columnar_relation_radix_sequence_t *seq,
    wl_columnar_relation_mutation_lease_t *lease, wl_evaluation_control_t *c)
{
    if (seg_count > 1u) return wl_columnar_relation_segment_sequence_prepare(r,
                   bounds, seg_count, seq, lease, c);
    int rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    if (!r || !bounds || !seq || !lease || !c || seg_count != 1
        || bounds[0] || bounds[1] != r->nrows
        || r->nrows > r->capacity || r->timestamps || r->arena_owned ||
        r->col_shared
        || seq->prepared || seq->consumed || seq->identity || seq->boundaries ||
        seq->needs_sort
        || seq->metadata_admission_active ||
        !wl_columnar_relation_consolidate_reservation_empty(
            &seq->metadata_admission)
        || wl_columnar_relation_consolidate_lease_validate(lease,
        r)) return EINVAL;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    for (uint32_t col = 0; col < r->ncols; col++) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        uint32_t type =
            r->column_types ? r->column_types[col] : WIRELOG_TYPE_INT64;
        if (!r->columns || !r->columns[col] || (type != WIRELOG_TYPE_INT64
            && type != WIRELOG_TYPE_FLOAT &&
            type != WIRELOG_TYPE_STRING)) return EINVAL;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    }
    bool sorted;
    rc = wl_columnar_relation_consolidate_sorted(r, 0, r->nrows, &sorted, c);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    unsigned events = sorted ? 1u : 2u;
    if (r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - events
        || COL_MAX_RUNS < 1) return EOVERFLOW;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    rc = wl_columnar_relation_consolidate_reserve(r, 9,
            &seq->metadata_admission, &seq->metadata_admission_active, c);
    if (rc) return rc;
    rc =
        wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&seq->boundaries, 8,
        c); if (rc) return rc;
    rc =
        wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&seq->needs_sort, 1,
        c); if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
    seq->boundaries[0] = 0; seq->boundaries[1] = r->nrows;
    seq->needs_sort[0] = 0; seq->seg_count = 1; seq->needs_sort_count = !sorted;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    rc = wl_columnar_relation_consolidate_sorted(r, 0, r->nrows, &sorted, c);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
    seq->needs_sort[0] = !sorted;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    rc =
        wl_columnar_relation_consolidate_commit_controlled(
        &seq->metadata_admission,
        seq->metadata_admission_active, seq, c);
    if (rc) return rc;
    rc = wl_columnar_relation_radix_workspace_prepare_with_lease_controlled(r,
            0, r->nrows, &seq->workspace, lease, c);
    if (rc) return rc;
    uint64_t hash;
    rc = wl_columnar_relation_radix_type_fingerprint_controlled(r, &hash, c);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    seq->set = lease->set; seq->lease = lease; seq->relation = r;
    seq->owner = lease->owner;
    seq->acquisition_nonce = lease->acquisition_nonce;
    seq->relation_identity = r->relation_identity;
    seq->relation_generation = r->storage_generation;
    seq->owner_identity = lease->owner_identity;
    seq->owner_generation = lease->owner_generation;
    seq->view_generation = r->view_generation;
    seq->type_fingerprint = hash;
    seq->columns = (const int64_t *const *)r->columns;
    seq->column_types = r->column_types; seq->timestamps = r->timestamps;
    seq->nrows = r->nrows; seq->ncols = r->ncols; seq->capacity = r->capacity;
    seq->identity = (uintptr_t)seq; seq->prepared = true;
    return wl_evaluation_control_charge(c, 0);
}

static int
wl_columnar_relation_consolidate_insertion(col_rel_t *r,
    wl_columnar_radix_workspace_t *w, uint32_t start, uint32_t count,
    wl_evaluation_control_t *c)
{
    uint64_t compares = 0, gathers = 0, shifts = 0, writes = 0;
    int64_t *saved = w->insertion_rows;
    for (uint32_t row = start + 1u; row < start + count; row++) {
        int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        for (uint32_t col = 0; col < r->ncols; col++) {
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            saved[col] = r->columns[col][row];
            rc = wl_columnar_relation_consolidate_step(c, 5, ++gathers, r);
            if (rc) return rc;
        }
        uint32_t dst = row;
        while (dst > start) {
            int cmp;
            rc = wl_columnar_relation_consolidate_compare(r, dst - 1u, 0,
                    saved, &cmp, c, &compares); if (rc) return rc;
            if (cmp <= 0) break;
            for (uint32_t col = 0; col < r->ncols; col++) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                r->columns[col][dst] = r->columns[col][dst - 1u];
                rc = wl_columnar_relation_consolidate_step(c, 6, ++shifts, r);
                if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(c, 4); if (rc) return rc;
            dst--;
            rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        }
        for (uint32_t col = 0; col < r->ncols; col++) {
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            r->columns[col][dst] = saved[col];
            rc = wl_columnar_relation_consolidate_step(c, 7, ++writes, r);
            if (rc) return rc;
        }
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    }
    return 0;
}

static int
wl_columnar_relation_consolidate_k8(col_rel_t *r,
    wl_columnar_radix_workspace_t *w, uint32_t start, uint32_t count,
    wl_evaluation_control_t *c)
{
    uint32_t histogram[256];
    uint32_t *current = w->perm_a, *next = w->perm_b;
    uint64_t visits = 0, gathers = 0, writes = 0;
    for (uint32_t row = 0; row < count; row++) {
        int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        current[row] = start + row;
        rc = wl_columnar_relation_consolidate_step(c, 8, row + 1u, r);
        if (rc) return rc;
    }
    for (uint32_t column = r->ncols; column;) {
        int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        column--;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        for (unsigned pass = 0; pass < 8; pass++) {
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            bool uniform = true;
            uint8_t first = 0;
            rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
            for (uint32_t row = 0; row < count; row++) {
                rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
                uint8_t value =
                    (uint8_t)((uint64_t)r->columns[column][current[row]] >>
                    (pass * 8u));
                if (pass == 7) value ^= 0x80u;
                w->byte_values[row] = value;
                if (!row) first = value;
                else uniform &= value == first;
                rc = wl_columnar_relation_consolidate_step(c, 9, ++visits, r);
                if (rc) return rc;
            }
            if (uniform) continue;
            for (unsigned bucket = 0; bucket < 256; bucket++) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                histogram[bucket] = 0;
                rc = wl_columnar_relation_consolidate_step(c, 10, bucket + 1u,
                        r); if (rc) return rc;
            }
            for (uint32_t row = 0; row < count; row++) {
                rc = wl_evaluation_control_charge(c, 12); if (rc) return rc;
                histogram[w->byte_values[row]]++;
                rc = wl_columnar_relation_consolidate_step(c, 11, row + 1u, r);
                if (rc) return rc;
            }
            uint32_t offset = 0;
            for (unsigned bucket = 0; bucket < 256; bucket++) {
                rc = wl_evaluation_control_charge(c, 12); if (rc) return rc;
                uint32_t count = histogram[bucket];
                histogram[bucket] = offset; offset += count;
                rc = wl_columnar_relation_consolidate_step(c, 12, bucket + 1u,
                        r); if (rc) return rc;
            }
            for (uint32_t row = 0; row < count; row++) {
                rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
                next[histogram[w->byte_values[row]]++] = current[row];
                rc = wl_columnar_relation_consolidate_step(c, 13, row + 1u, r);
                if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            uint32_t *swap = current; current = next; next = swap;
            rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        }
    }
    for (uint32_t col = 0; col < r->ncols; col++) {
        for (uint32_t row = 0; row < count; row++) {
            int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            w->temp_column[row] = r->columns[col][current[row]];
            rc = wl_columnar_relation_consolidate_step(c, 14, ++gathers, r);
            if (rc) return rc;
        }
        for (uint32_t row = 0; row < count; row++) {
            int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            r->columns[col][start + row] = w->temp_column[row];
            rc = wl_columnar_relation_consolidate_step(c, 15, ++writes, r);
            if (rc) return rc;
        }
    }
    return 0;
}

static int
wl_columnar_relation_consolidate_k16(col_rel_t *r,
    wl_columnar_radix_workspace_t *w, uint32_t start, uint32_t count,
    wl_evaluation_control_t *c)
{
    uint32_t *histogram = w->count16;
    uint32_t *current = w->perm_a, *next = w->perm_b;
    uint64_t visits = 0, gathers = 0, writes = 0;
    for (uint32_t row = 0; row < count; row++) {
        int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        current[row] = start + row;
        rc = wl_columnar_relation_consolidate_step(c, 60, row + 1u, r);
        if (rc) return rc;
    }
    for (uint32_t column = r->ncols; column;) {
        int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        column--;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        for (unsigned pass = 0; pass < 4; pass++) {
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            bool uniform = true;
            uint16_t first = 0;
            rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
            for (uint32_t row = 0; row < count; row++) {
                rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
                uint16_t value =
                    (uint16_t)((uint64_t)r->columns[column][current[row]] >>
                    (pass * 16u));
                if (pass == 3) value ^= 0x8000u;
                w->short_values[row] = value;
                if (!row) first = value;
                else uniform &= value == first;
                rc = wl_columnar_relation_consolidate_step(c, 61, ++visits, r);
                if (rc) return rc;
            }
            if (uniform) continue;
            rc = wl_columnar_relation_consolidate_step(c, 68, pass + 1u, r);
            if (rc) return rc;
            for (unsigned bucket = 0; bucket < 65536; bucket++) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                histogram[bucket] = 0;
                rc = wl_columnar_relation_consolidate_step(c, 62, bucket + 1u,
                        r); if (rc) return rc;
            }
            for (uint32_t row = 0; row < count; row++) {
                rc = wl_evaluation_control_charge(c, 12); if (rc) return rc;
                histogram[w->short_values[row]]++;
                rc = wl_columnar_relation_consolidate_step(c, 63, row + 1u, r);
                if (rc) return rc;
            }
            uint32_t offset = 0;
            for (unsigned bucket = 0; bucket < 65536; bucket++) {
                rc = wl_evaluation_control_charge(c, 12); if (rc) return rc;
                uint32_t count = histogram[bucket];
                histogram[bucket] = offset; offset += count;
                rc = wl_columnar_relation_consolidate_step(c, 64, bucket + 1u,
                        r); if (rc) return rc;
            }
            for (uint32_t row = 0; row < count; row++) {
                rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
                next[histogram[w->short_values[row]]++] = current[row];
                rc = wl_columnar_relation_consolidate_step(c, 65, row + 1u, r);
                if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            uint32_t *swap = current; current = next; next = swap;
            rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        }
    }
    for (uint32_t col = 0; col < r->ncols; col++) {
        for (uint32_t row = 0; row < count; row++) {
            int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            w->temp_column[row] = r->columns[col][current[row]];
            rc = wl_columnar_relation_consolidate_step(c, 66, ++gathers, r);
            if (rc) return rc;
        }
        for (uint32_t row = 0; row < count; row++) {
            int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            r->columns[col][start + row] = w->temp_column[row];
            rc = wl_columnar_relation_consolidate_step(c, 67, ++writes, r);
            if (rc) return rc;
        }
    }
    return 0;
}

static int
wl_columnar_relation_segment_sequence_execute(
    wl_columnar_relation_radix_sequence_t *seq,
    wl_columnar_relation_mutation_lease_t *lease, wl_evaluation_control_t *c)
{
    int rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    if (!seq || !lease || seq->identity != (uintptr_t)seq || !seq->prepared
        || seq->consumed || seq->lease != lease || seq->set != lease->set
        || seq->acquisition_nonce != lease->acquisition_nonce || !seq->relation
        || seq->relation != lease->relation || seq->seg_count < 2u
        || !seq->boundaries || !seq->needs_sort
        || seq->needs_sort_count > seq->seg_count
        || wl_columnar_relation_consolidate_lease_validate(lease,
        seq->relation))
        return EINVAL;
    col_rel_t *r = seq->relation;
    wl_columnar_radix_workspace_t *w = &seq->workspace;
    if (r->relation_identity != seq->relation_identity
        || r->storage_generation != seq->relation_generation
        || r->view_generation != seq->view_generation ||
        lease->owner != seq->owner
        || lease->owner_identity != seq->owner_identity
        || lease->owner_generation != seq->owner_generation
        || r->columns != (int64_t **)seq->columns ||
        r->column_types != seq->column_types
        || r->timestamps || r->col_shared || r->arena_owned || seq->timestamps
        || r->nrows != seq->nrows || r->ncols != seq->ncols
        || r->capacity != seq->capacity || r->nrows > r->capacity
        || seq->boundaries[0] || seq->boundaries[seq->seg_count] != r->nrows
        || r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID
        - ((uint64_t)seq->needs_sort_count + 1u)) return EINVAL;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    uint64_t hash;
    rc = wl_columnar_relation_radix_type_fingerprint_controlled(r, &hash, c);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 96); if (rc) return rc;
    if (hash != seq->type_fingerprint || hash != w->prepared_type_fingerprint
        || w->prepared_relation != r ||
        w->prepared_identity != r->relation_identity
        || w->prepared_view_generation != r->view_generation
        || w->prepared_storage_generation != r->storage_generation
        || w->prepared_start || w->prepared_count != r->nrows
        || w->prepared_nrows != r->nrows || w->prepared_ncols != r->ncols
        || w->prepared_capacity != r->capacity
        || w->prepared_has_types != (r->column_types != NULL)
        || w->prepared_has_timestamps || w->timestamps
        || w->byte_values != w->bucket_values
        || (void *)w->short_values != w->bucket_values) return EINVAL;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    bool has_float = false;
    for (uint32_t col = 0; col < r->ncols; col++) {
        rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
        if (!r->columns || !r->columns[col]) return EINVAL;
        uint32_t type =
            r->column_types ? r->column_types[col] : WIRELOG_TYPE_INT64;
        if (type != WIRELOG_TYPE_INT64 && type != WIRELOG_TYPE_FLOAT
            && type != WIRELOG_TYPE_STRING) return EINVAL;
        has_float |= type == WIRELOG_TYPE_FLOAT;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    }
    uint32_t sorts = 0;
    for (uint32_t i = 0; i < seq->seg_count; i++) {
        rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
        uint32_t start = seq->boundaries[i], end = seq->boundaries[i + 1u];
        if (start > end || end > r->nrows ||
            seq->needs_sort[i] > 1u) return EINVAL;
        uint32_t count = end - start;
        if (seq->needs_sort[i]) {
            if (count <= 1u) return EINVAL;
            sorts++;
            if (r->ncols && (has_float || count <= 32u)) {
                uint64_t bytes;
                if (!wl_columnar_memory_size_mul((uint64_t)count + 1u, r->ncols,
                    &bytes)
                    || !wl_columnar_memory_size_mul(bytes, 8u, &bytes)
                    || !w->insertion_rows || w->insertion_capacity < count
                    || w->insertion_bytes < bytes) return EINVAL;
            } else if (r->ncols && (!w->perm_a || !w->perm_b || !w->temp_column
                || !w->bucket_values || (count >= 50000u
                    ? (!w->count16 || w->k16_capacity < count)
                    : w->k8_capacity < count))) return EINVAL;
        }
        rc = wl_columnar_relation_consolidate_step(c, 102, (uint64_t)i + 1u, r);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
    if (sorts != seq->needs_sort_count) return EINVAL;
    seq->consumed = true;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    uint64_t generation = seq->view_generation;
    for (uint32_t i = 0; i < seq->seg_count; i++) {
        rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
        if (r->view_generation != generation
            || wl_columnar_relation_consolidate_lease_validate(lease,
            r)) return EINVAL;
        uint32_t start = seq->boundaries[i],
            count = seq->boundaries[i + 1u] - start;
        bool needed = seq->needs_sort[i];
        rc = wl_columnar_relation_consolidate_step(c, 103, (uint64_t)i + 1u, r);
        if (rc) return rc;
        if (!needed) continue;
        if (r->ncols) {
            rc = has_float || count <= 32u
                ? wl_columnar_relation_consolidate_insertion(r, w, start, count,
                    c)
                : count >= 50000u ? wl_columnar_relation_consolidate_k16(r, w,
                    start, count, c)
                : wl_columnar_relation_consolidate_k8(r, w, start, count, c);
            if (rc) return rc;
        }
        rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
        if (r->view_generation != generation
            || wl_columnar_relation_consolidate_lease_validate(lease,
            r)) return EINVAL;
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
        r->view_generation++; generation++;
        lease->set->published = true;
    }
    return 0;
}

int
wl_columnar_relation_radix_sequence_execute_with_lease_controlled(
    wl_columnar_relation_radix_sequence_t *seq,
    wl_columnar_relation_mutation_lease_t *lease, wl_evaluation_control_t *c)
{
    if (seq &&
        seq->seg_count >
        1u) return wl_columnar_relation_segment_sequence_execute(seq, lease, c);
    int rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    if (!seq || !lease || seq->identity != (uintptr_t)seq || !seq->prepared
        || seq->consumed || seq->lease != lease || seq->set != lease->set
        || seq->acquisition_nonce != lease->acquisition_nonce
        || !seq->relation || seq->relation != lease->relation
        || seq->seg_count != 1 || !seq->boundaries || !seq->needs_sort
        || seq->boundaries[0] || seq->boundaries[1] != seq->nrows
        || seq->needs_sort_count > 1 ||
        seq->needs_sort[0] != seq->needs_sort_count
        || wl_columnar_relation_consolidate_lease_validate(lease,
        seq->relation)) return EINVAL;
    col_rel_t *r = seq->relation;
    if (r->relation_identity != seq->relation_identity
        || r->storage_generation != seq->relation_generation
        || r->view_generation != seq->view_generation
        || lease->owner != seq->owner ||
        lease->owner_identity != seq->owner_identity
        || lease->owner_generation != seq->owner_generation
        || r->columns != (int64_t **)seq->columns ||
        r->column_types != seq->column_types
        || r->timestamps != seq->timestamps || r->timestamps || r->col_shared
        || r->nrows != seq->nrows || r->ncols != seq->ncols ||
        r->capacity != seq->capacity
        || r->nrows > r->capacity
        || r->view_generation >=
        WL_COLUMNAR_REL_GENERATION_INVALID -
        (seq->needs_sort_count + 1u)) return EINVAL;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    uint64_t hash;
    rc = wl_columnar_relation_radix_type_fingerprint_controlled(r, &hash, c);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 96); if (rc) return rc;
    wl_columnar_radix_workspace_t *w = &seq->workspace;
    if (hash != seq->type_fingerprint || hash != w->prepared_type_fingerprint
        || w->prepared_relation != r ||
        w->prepared_identity != r->relation_identity
        || w->prepared_view_generation != r->view_generation
        || w->prepared_storage_generation != r->storage_generation
        || w->prepared_start || w->prepared_count != r->nrows
        || w->prepared_nrows != r->nrows || w->prepared_ncols != r->ncols
        || w->prepared_capacity != r->capacity ||
        w->prepared_has_types != (r->column_types != NULL)
        || w->prepared_has_timestamps || w->timestamps
        || w->byte_values != w->bucket_values
        || (void *)w->short_values != w->bucket_values) return EINVAL;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    bool has_float = false;
    for (uint32_t col = 0; col < r->ncols; col++) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        if (!r->columns || !r->columns[col]) return EINVAL;
        if (r->column_types) {
            uint32_t type = r->column_types[col];
            if (type != WIRELOG_TYPE_INT64 && type != WIRELOG_TYPE_FLOAT
                && type != WIRELOG_TYPE_STRING) return EINVAL;
            has_float |= type == WIRELOG_TYPE_FLOAT;
        }
        rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
    bool insertion = has_float || r->nrows <= 32u;
    if (seq->needs_sort_count && r->ncols) {
        uint64_t needed;
        if (insertion) {
            if (!wl_columnar_memory_size_mul((uint64_t)r->nrows + 1u, r->ncols,
                &needed)
                || !wl_columnar_memory_size_mul(needed, sizeof(int64_t),
                &needed)
                || !w->insertion_rows || w->insertion_capacity < r->nrows
                || w->insertion_bytes < needed) return EINVAL;
        } else if (!w->perm_a || !w->perm_b || !w->temp_column
            || !w->bucket_values || (r->nrows >= 50000u
            ? (!w->short_values || !w->count16 || w->k16_capacity < r->nrows)
            : (!w->byte_values || w->k8_capacity < r->nrows))) return EINVAL;
    }
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
    seq->consumed = true;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    if (!seq->needs_sort_count) return 0;
    if (r->ncols) {
        rc = insertion ? wl_columnar_relation_consolidate_insertion(r, w, 0,
                r->nrows, c)
            : r->nrows >= 50000u
            ? wl_columnar_relation_consolidate_k16(r, w, 0, r->nrows, c)
            : wl_columnar_relation_consolidate_k8(r, w, 0, r->nrows, c);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    if (wl_columnar_relation_consolidate_lease_validate(lease,
        r)) return EINVAL;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    if (r->view_generation != seq->view_generation
        || r->view_generation >=
        WL_COLUMNAR_REL_GENERATION_INVALID - 1u) return EINVAL;
    rc = wl_evaluation_control_charge(c, 0); if (rc) return rc;
    r->view_generation++;
    lease->set->published = true;
    return 0;
}
