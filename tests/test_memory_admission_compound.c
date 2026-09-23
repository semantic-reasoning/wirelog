/* test_memory_admission_compound.c - compound storage admission */

#include "../wirelog/arena/compound_arena.h"

#include <errno.h>
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
noop_context_release(void *context)
{
    (void)context;
}

static int
allow_growth_prepare(void *context, uint64_t old_bytes, uint64_t new_bytes,
    wl_columnar_memory_reservation_t *reservation)
{
    (void)context;
    (void)old_bytes;
    (void)new_bytes;
    (void)reservation;
    return 0;
}

static int
fail_growth_publish(void *context,
    wl_columnar_memory_reservation_t *old_reservation,
    wl_columnar_memory_reservation_t *new_reservation, const void *owner)
{
    (void)context;
    (void)old_reservation;
    (void)new_reservation;
    (void)owner;
    return -1;
}

static int
fail_second_growth_publish(void *context,
    wl_columnar_memory_reservation_t *old_reservation,
    wl_columnar_memory_reservation_t *new_reservation, const void *owner)
{
    int *publish_count = context;

    (void)old_reservation;
    (void)new_reservation;
    (void)owner;
    return (*publish_count)++ == 0 ? 0 : -1;
}

static void
noop_growth_abort(void *context,
    wl_columnar_memory_reservation_t *reservation)
{
    (void)context;
    (void)reservation;
}

static int
noop_growth_release(void *context,
    wl_columnar_memory_reservation_t *reservation)
{
    (void)context;
    (void)reservation;
    return 0;
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
fixed_bytes(uint32_t max_epochs)
{
    return (uint64_t)sizeof(wl_compound_arena_t)
           + (uint64_t)max_epochs * sizeof(wl_compound_gen_t);
}

static uint64_t
entry_bytes(uint32_t entry_cap)
{
    return (uint64_t)entry_cap * (sizeof(uint32_t) + sizeof(int64_t));
}

static void
test_managed_lifecycle(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_compound_arena_t *arena;
    const uint64_t fixed = fixed_bytes(2);
    const uint64_t first = fixed + 64 + entry_bytes(16);

    make_resolution(&resolution, first);
    CHECK(wl_columnar_memory_governor_init(&governor, &resolution)
        == WL_COLUMNAR_MEMORY_OK, "governor initialization");
    arena = wl_compound_arena_create_managed(1, 64, 2, &governor);
    CHECK(arena != NULL, "managed compound arena creation");
    CHECK(wl_columnar_memory_reserved(&governor) == fixed,
        "fixed arena and generation table admitted");
    CHECK(wl_compound_arena_alloc(arena, 16) != WL_COMPOUND_HANDLE_NULL,
        "lazy payload allocation remains available");
    CHECK(wl_columnar_memory_reserved(&governor) == first,
        "first payload and entry metadata growth admitted");
    wl_compound_arena_free(arena);
    CHECK(wl_columnar_memory_reserved(&governor) == 0,
        "destroy releases fixed metadata admission");
}

static void
test_growth_peak_and_rollback(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_compound_arena_t *arena;
    const uint64_t fixed = fixed_bytes(2);
    const uint64_t first = fixed + 64 + entry_bytes(16);
    const uint64_t peak = fixed + 64 + 128 + entry_bytes(16);
    uint64_t handle;
    uint32_t size = 0;

    make_resolution(&resolution, first);
    CHECK(wl_columnar_memory_governor_init(&governor, &resolution)
        == WL_COLUMNAR_MEMORY_OK, "growth denial governor initialization");
    arena = wl_compound_arena_create_managed(7, 64, 2, &governor);
    CHECK(arena != NULL, "growth test arena creation");
    handle = wl_compound_arena_alloc(arena, 16);
    CHECK(handle != WL_COMPOUND_HANDLE_NULL, "initial growth-test handle");
    CHECK(wl_compound_arena_alloc(arena, 64) == WL_COMPOUND_HANDLE_NULL,
        "payload growth is denied at old-footprint budget");
    CHECK(wl_columnar_memory_reserved(&governor) == first,
        "denied payload growth leaves reservation unchanged");
    CHECK(wl_compound_arena_lookup(arena, handle, &size) != NULL
        && size == 16, "denied growth preserves the old handle");
    wl_compound_arena_free(arena);

    make_resolution(&resolution, peak);
    CHECK(wl_columnar_memory_governor_init(&governor, &resolution)
        == WL_COLUMNAR_MEMORY_OK, "growth success governor initialization");
    arena = wl_compound_arena_create_managed(7, 64, 2, &governor);
    CHECK(arena != NULL, "growth success arena creation");
    handle = wl_compound_arena_alloc(arena, 16);
    CHECK(handle != WL_COMPOUND_HANDLE_NULL, "growth success initial handle");
    CHECK(wl_compound_arena_alloc(arena, 64) != WL_COMPOUND_HANDLE_NULL,
        "payload growth admitted with peak overlap");
    CHECK(wl_columnar_memory_reserved(&governor)
        == fixed + 128 + entry_bytes(16),
        "payload growth releases the old capacity after publish");
    CHECK(wl_compound_arena_lookup(arena, handle, &size) != NULL
        && size == 16, "payload growth preserves old handle contents");
    wl_compound_arena_free(arena);
    CHECK(wl_columnar_memory_reserved(&governor) == 0,
        "payload growth destroy releases all reservations");
}

static void
test_entry_growth(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_compound_arena_t *arena;
    const uint64_t fixed = fixed_bytes(1);
    const uint64_t peak = fixed + 4096 + entry_bytes(16)
        + entry_bytes(32);
    uint64_t first_handle = 0;

    make_resolution(&resolution, peak);
    CHECK(wl_columnar_memory_governor_init(&governor, &resolution)
        == WL_COLUMNAR_MEMORY_OK, "entry growth governor initialization");
    arena = wl_compound_arena_create_managed(9, 4096, 1, &governor);
    CHECK(arena != NULL, "entry growth arena creation");
    for (uint32_t i = 0; i < 17; i++) {
        uint64_t handle = wl_compound_arena_alloc(arena, 8);
        CHECK(handle != WL_COMPOUND_HANDLE_NULL,
            "entry growth handle allocation");
        if (i == 0)
            first_handle = handle;
    }
    CHECK(wl_columnar_memory_reserved(&governor)
        == fixed + 4096 + entry_bytes(32),
        "paired entry arrays grow under peak admission");
    CHECK(wl_compound_arena_multiplicity(arena, first_handle) == 1,
        "entry growth preserves multiplicity");
    wl_compound_arena_free(arena);
    CHECK(wl_columnar_memory_reserved(&governor) == 0,
        "entry growth destroy releases all reservations");
}

static void
test_publish_failure_rollback(void)
{
    wl_compound_arena_t *arena = wl_compound_arena_create_with_admission(
        11, 64, 1, NULL, noop_context_release, allow_growth_prepare,
        fail_growth_publish, noop_growth_abort, noop_growth_release);

    CHECK(arena != NULL, "publish-failure arena creation");
    CHECK(wl_compound_arena_alloc(arena, 8) == WL_COMPOUND_HANDLE_NULL,
        "publish failure denies the staged allocation");
    CHECK(arena->gens[0].base == NULL && arena->gens[0].capacity == 0
        && arena->gens[0].entry_offsets == NULL
        && arena->gens[0].multiplicity == NULL
        && arena->gens[0].entry_count == 0,
        "publish failure preserves the old generation state");
    wl_compound_arena_free(arena);

    int publish_count = 0;
    arena = wl_compound_arena_create_with_admission(
        11, 64, 1, &publish_count, noop_context_release,
        allow_growth_prepare, fail_second_growth_publish,
        noop_growth_abort, noop_growth_release);
    CHECK(arena != NULL, "second-publish-failure arena creation");
    CHECK(wl_compound_arena_alloc(arena, 8) == WL_COMPOUND_HANDLE_NULL,
        "second publish failure denies the staged allocation");
    CHECK(arena->gens[0].base == NULL && arena->gens[0].capacity == 0
        && arena->gens[0].entry_offsets == NULL
        && arena->gens[0].multiplicity == NULL
        && arena->gens[0].entry_count == 0,
        "second publish failure preserves the old generation state");
    wl_compound_arena_free(arena);
}

static void
test_allocator_failpoints(void)
{
    const wl_compound_alloc_failpoint_t failpoints[] = {
        WL_COMPOUND_FAIL_PAYLOAD,
        WL_COMPOUND_FAIL_ENTRY_OFFSETS,
        WL_COMPOUND_FAIL_MULTIPLICITY,
    };

    for (size_t p = 0; p < sizeof(failpoints) / sizeof(failpoints[0]); p++) {
        wl_columnar_memory_resolution_t resolution;
        wl_columnar_memory_governor_t governor;
        wl_compound_arena_t *arena;
        wl_compound_gen_t *gen;
        uint8_t *old_base;
        uint32_t *old_offsets;
        int64_t *old_multiplicity;
        uint32_t old_capacity;
        uint32_t old_used;
        uint32_t old_entry_count;
        uint32_t old_entry_cap;
        uint64_t old_reserved;

        make_resolution(&resolution, 1u << 20);
        CHECK(wl_columnar_memory_governor_init(&governor, &resolution)
            == WL_COLUMNAR_MEMORY_OK, "failpoint governor initialization");
        arena = wl_compound_arena_create_managed(13,
                p == 0 ? 64 : 256, 1, &governor);
        CHECK(arena != NULL, "failpoint arena creation");
        if (!arena)
            continue;
        if (p == 0) {
            CHECK(wl_compound_arena_alloc(arena, 16) != 0,
                "payload failpoint setup");
        } else {
            for (uint32_t i = 0; i < 16; i++)
                CHECK(wl_compound_arena_alloc(arena, 8) != 0,
                    "entry failpoint setup");
        }
        gen = &arena->gens[0];
        old_base = gen->base;
        old_offsets = gen->entry_offsets;
        old_multiplicity = gen->multiplicity;
        old_capacity = gen->capacity;
        old_used = gen->used;
        old_entry_count = gen->entry_count;
        old_entry_cap = gen->entry_cap;
        old_reserved = wl_columnar_memory_reserved(&governor);
        wl_compound_arena_test_fail_next(arena, failpoints[p]);
        CHECK(wl_compound_arena_alloc(arena, p == 0 ? 64 : 8) == 0,
            "injected allocator failure was not reported");
        CHECK(gen->base == old_base && gen->entry_offsets == old_offsets
            && gen->multiplicity == old_multiplicity
            && gen->capacity == old_capacity && gen->used == old_used
            && gen->entry_count == old_entry_count
            && gen->entry_cap == old_entry_cap,
            "allocator failure changed generation state");
        CHECK(wl_columnar_memory_reserved(&governor) == old_reserved,
            "allocator failure changed reservation accounting");
        wl_compound_arena_free(arena);
        CHECK(wl_columnar_memory_reserved(&governor) == 0,
            "allocator failure leaked a reservation at destroy");
    }
}

static void
test_mutation_window(void)
{
    wl_compound_arena_t *arena = wl_compound_arena_create(17, 64, 2);
    wl_compound_arena_borrow_t borrow = { 0 };
    wl_compound_arena_borrow_t copied;
    wl_compound_arena_mutation_t mutation = { 0 };
    uint64_t handle;
    uint32_t size = 0;

    CHECK(arena != NULL, "mutation-window arena creation");
    if (!arena)
        return;
    handle = wl_compound_arena_alloc(arena, 16);
    CHECK(handle != WL_COMPOUND_HANDLE_NULL, "mutation-window setup handle");
    CHECK(wl_compound_arena_borrow(arena, &borrow),
        "read lease acquisition");
    copied = borrow;
    CHECK(wl_compound_arena_lookup_borrowed(&borrow, handle, &size) != NULL
        && size == 16, "leased lookup remains valid");
    CHECK(wl_compound_arena_mutation_begin(arena, &mutation) == false,
        "mutation window opened while a reader was active");
    CHECK(wl_compound_arena_alloc(arena, 16) == WL_COMPOUND_HANDLE_NULL,
        "allocation bypassed an active read lease");
    CHECK(!wl_compound_arena_borrow_release(&copied),
        "copied read lease released the active lease");
    CHECK(wl_compound_arena_borrow_release(&borrow),
        "read lease release");
    CHECK(wl_compound_arena_borrow(arena, &borrow),
        "read lease can be reacquired after release");
    copied = borrow;
    CHECK(wl_compound_arena_borrow_release(&borrow),
        "second read lease release");
    CHECK(wl_compound_arena_borrow(arena, &borrow),
        "read lease can be reacquired after stale copy");
    CHECK(!wl_compound_arena_borrow_release(&copied),
        "stale copied lease released a reacquired lease");
    CHECK(!wl_compound_arena_mutation_begin(arena, &mutation),
        "mutation window ignored the reacquired lease");
    CHECK(wl_compound_arena_borrow_release(&borrow),
        "reacquired read lease release");
    CHECK(wl_compound_arena_mutation_begin(arena, &mutation),
        "mutation window did not open after reader release");
    CHECK(wl_compound_arena_mutation_end(&mutation),
        "mutation window release");
    CHECK(!wl_compound_arena_mutation_end(&mutation),
        "double mutation release was accepted");
    CHECK(wl_compound_arena_borrow(arena, &borrow),
        "read lease before teardown check");
    CHECK(!wl_compound_arena_free_checked(arena),
        "arena teardown ignored an active lease");
    CHECK(wl_compound_arena_borrow_release(&borrow),
        "teardown-check read lease release");
    CHECK(wl_compound_arena_free_checked(arena),
        "arena teardown after lease release");
}

static void
test_denial_and_legacy_contract(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_compound_arena_t *arena;
    const uint64_t expected = fixed_bytes(2);

    make_resolution(&resolution, expected - 1);
    CHECK(wl_columnar_memory_governor_init(&governor, &resolution)
        == WL_COLUMNAR_MEMORY_OK, "denial governor initialization");
    arena = (wl_compound_arena_t *)1;
    CHECK(wl_compound_arena_create_managed_checked(1, 64, 2, &governor,
        &arena) == ENOSPC && arena == NULL,
        "checked constructor reports exact budget denial");
    arena = wl_compound_arena_create_managed(1, 64, 2, &governor);
    CHECK(arena == NULL, "denied compound metadata returns NULL");
    CHECK(wl_columnar_memory_reserved(&governor) == 0,
        "denial leaves no reservation behind");

    arena = wl_compound_arena_create_managed(1, 64, 0, &governor);
    CHECK(arena == NULL, "default epoch table is also denied honestly");
    CHECK(wl_columnar_memory_reserved(&governor) == 0,
        "default denial leaves no reservation behind");

    make_resolution(&resolution, expected);
    CHECK(wl_columnar_memory_governor_init(&governor, &resolution)
        == WL_COLUMNAR_MEMORY_OK, "exact-fit governor initialization");
    CHECK(wl_compound_arena_create_managed_checked(1, 64, 2, &governor,
        &arena) == 0 && arena != NULL,
        "checked constructor admits exact fixed footprint");
    wl_compound_arena_free(arena);
    CHECK(wl_columnar_memory_reserved(&governor) == 0,
        "checked constructor releases exact reservation");

    arena = wl_compound_arena_create_managed(1, 64, 2, NULL);
    CHECK(arena != NULL, "NULL governor preserves unmanaged mode");
    CHECK(arena->admission_context == NULL
        && arena->admission_release == NULL,
        "NULL governor creates an unmanaged arena");
    wl_compound_arena_free(arena);

    arena = wl_compound_arena_create(1, 64, 2);
    CHECK(arena != NULL, "legacy compound arena creation");
    CHECK(arena->admission_context == NULL
        && arena->admission_release == NULL,
        "legacy arena remains unmanaged");
    CHECK(wl_columnar_memory_reserved(&governor) == 0,
        "legacy arena does not charge a governor");
    wl_compound_arena_free(arena);
}

int
main(void)
{
    test_managed_lifecycle();
    test_growth_peak_and_rollback();
    test_entry_growth();
    test_publish_failure_rollback();
    test_allocator_failpoints();
    test_mutation_window();
    test_denial_and_legacy_contract();
    if (failures != 0)
        return 1;
    puts("memory admission compound arena: PASS");
    return 0;
}
