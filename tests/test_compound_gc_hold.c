/* Compound handle lifetime holds, independent of semantic multiplicity. */
#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/thread.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

static int failures;
#define CHECK(condition) do { \
            if (!(condition)) { \
                fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition); \
                failures++; \
            } \
} while (0)

static void
test_lifetime(void)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    wl_columnar_memory_governor_t governor;
    resolution.budget_bytes = resolution.usable_bytes = 1048576;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    wl_columnar_memory_governor_init(&governor, &resolution);
    wl_compound_arena_t *arena = wl_compound_arena_create_managed(7, 16, 3,
            &governor);
    CHECK(arena != NULL);
    if (!arena)
        return;
    uint64_t handle = wl_compound_arena_alloc(arena, 8);
    CHECK(handle != 0);
    memcpy((void *)wl_compound_arena_lookup(arena, handle, NULL), "sentinel",
        8);
    CHECK(wl_compound_arena_retain(arena, handle, -1) == 0);
    wl_arena_compound_arena_gc_hold_t first = { 0 }, second = { 0 };
    CHECK(wl_arena_compound_arena_gc_hold_acquire(arena, &first) == 0);
    CHECK(wl_arena_compound_arena_gc_hold_acquire(arena, &second) == 0);
    CHECK(wl_compound_arena_multiplicity(arena, handle) == 0);
    CHECK(wl_compound_arena_alloc(arena, 8192) != 0);
    CHECK(arena->gens[0].capacity >= 8192);
    CHECK(wl_compound_arena_retain(arena, handle, 2) == 0);
    CHECK(wl_compound_arena_retain(arena, handle, -2) == 0);
    wl_compound_arena_borrow_t borrow = { 0 };
    CHECK(wl_compound_arena_borrow(arena, &borrow));
    const void *payload = wl_compound_arena_lookup_borrowed(&borrow, handle,
            NULL);
    CHECK(payload && memcmp(payload, "sentinel", 8) == 0);
    CHECK(wl_compound_arena_alloc(arena, 8) == 0);
    CHECK(wl_compound_arena_borrow_release(&borrow));
    uint32_t used = arena->gens[0].used, count = arena->gens[0].entry_count;
    uint32_t reclaimed = 123;
    CHECK(wl_arena_compound_arena_gc_epoch_boundary_checked(arena,
        &reclaimed) == EBUSY);
    CHECK(reclaimed == 123);
    CHECK(wl_compound_arena_gc_epoch_boundary(arena) == 0);
    CHECK(arena->current_epoch == 0 && arena->gens[0].used == used
        && arena->gens[0].entry_count == count);
    CHECK(!wl_compound_arena_free_checked(arena));
    CHECK(wl_arena_compound_arena_gc_hold_release(&first) == 0);
    CHECK(!wl_compound_arena_free_checked(arena));
    CHECK(wl_arena_compound_arena_gc_hold_release(&second) == 0);
    CHECK(arena->current_epoch == 0 && arena->gens[0].used == used);
    CHECK(wl_arena_compound_arena_gc_epoch_boundary_checked(arena,
        &reclaimed) == 0);
    CHECK(reclaimed == 1 && arena->current_epoch == 1);
    CHECK(wl_compound_arena_lookup(arena, handle, NULL) == NULL);
    CHECK(wl_compound_arena_free_checked(arena));
    CHECK(wl_columnar_memory_reserved(&governor) == 0);
}

static void
test_tokens(void)
{
    wl_compound_arena_t *arena = wl_compound_arena_create(9, 16, 1);
    CHECK(arena != NULL);
    if (!arena)
        return;
    wl_arena_compound_arena_gc_hold_t hold = { 0 };
    CHECK(wl_arena_compound_arena_gc_hold_acquire(NULL, &hold) == EINVAL);
    CHECK(wl_arena_compound_arena_gc_hold_acquire(arena, NULL) == EINVAL);
    CHECK(wl_arena_compound_arena_gc_hold_release(&hold) == EINVAL);
    CHECK(wl_arena_compound_arena_gc_hold_acquire(arena, &hold) == 0);
    wl_arena_compound_arena_gc_hold_t copied = hold;
    CHECK(wl_arena_compound_arena_gc_hold_release(&copied) == EINVAL);
    CHECK(wl_arena_compound_arena_gc_hold_acquire(arena, &hold) == EINVAL);
    CHECK(wl_arena_compound_arena_gc_hold_release(&hold) == 0);
    CHECK(wl_arena_compound_arena_gc_hold_release(&hold) == EINVAL);
    /* Sequential corruption probes: no concurrent user observes these counts. */
    atomic_init(&arena->gc_hold_count, UINT64_MAX);
    CHECK(wl_arena_compound_arena_gc_hold_acquire(arena, &hold) == EOVERFLOW);
    CHECK(!hold.arena && !hold.identity);
    atomic_init(&arena->gc_hold_count, 0);
    hold.arena = arena;
    hold.identity = (uintptr_t)&hold;
    CHECK(wl_arena_compound_arena_gc_hold_release(&hold) == EINVAL);
    CHECK(hold.arena == arena && hold.identity == (uintptr_t)&hold);
    memset(&hold, 0, sizeof(hold));
    uint32_t reclaimed = 123;
    CHECK(wl_arena_compound_arena_gc_epoch_boundary_checked(NULL,
        &reclaimed) == EINVAL);
    CHECK(wl_arena_compound_arena_gc_epoch_boundary_checked(arena,
        NULL) == EINVAL);
    CHECK(reclaimed == 123);
    wl_compound_arena_freeze(arena);
    CHECK(wl_arena_compound_arena_gc_epoch_boundary_checked(arena,
        &reclaimed) == EBUSY);
    CHECK(reclaimed == 123 && arena->current_epoch == 0);
    wl_compound_arena_unfreeze(arena);
    CHECK(wl_arena_compound_arena_gc_epoch_boundary_checked(arena,
        &reclaimed) == 0);
    CHECK(reclaimed == 0 && arena->current_epoch == 1);
    CHECK(wl_arena_compound_arena_gc_epoch_boundary_checked(arena,
        &reclaimed) == 0);
    CHECK(reclaimed == 0 && arena->current_epoch == 1);
    CHECK(wl_compound_arena_free_checked(arena));
    /* Copied token is rejected before dereferencing its now-freed arena. */
    CHECK(wl_arena_compound_arena_gc_hold_release(&copied) == EINVAL);
}

typedef struct {
    wl_compound_arena_t *arena;
    wl_arena_compound_arena_gc_hold_t hold;
    int errors;
} thread_context_t;

static void *
release_worker(void *arg)
{
    thread_context_t *context = arg;
    context->errors = wl_arena_compound_arena_gc_hold_release(&context->hold);
    return NULL;
}

static void *
churn_worker(void *arg)
{
    thread_context_t *context = arg;
    for (unsigned i = 0; i < 10000; i++) {
        wl_arena_compound_arena_gc_hold_t hold = { 0 };
        int rc = wl_arena_compound_arena_gc_hold_acquire(context->arena, &hold);
        if (rc == 0) {
            if (wl_arena_compound_arena_gc_hold_release(&hold) != 0)
                context->errors++;
        } else if (rc != EBUSY) {
            context->errors++;
        }
    }
    return NULL;
}

static void
test_concurrency(void)
{
    thread_context_t context = { 0 };
    context.arena = wl_compound_arena_create(11, 16, 3);
    CHECK(context.arena != NULL);
    if (!context.arena)
        return;
    CHECK(wl_arena_compound_arena_gc_hold_acquire(context.arena,
        &context.hold) == 0);
    wl_compound_arena_mutation_t mutation = { 0 };
    CHECK(wl_compound_arena_mutation_begin(context.arena, &mutation));
    wl_arena_compound_arena_gc_hold_t refused = { 0 };
    CHECK(wl_arena_compound_arena_gc_hold_acquire(context.arena,
        &refused) == EBUSY);
    CHECK(!refused.arena && !refused.identity);
    uint32_t reclaimed = 123;
    CHECK(wl_arena_compound_arena_gc_epoch_boundary_checked(context.arena,
        &reclaimed) == EBUSY);
    CHECK(reclaimed == 123);
    wl_thread_t thread;
    int rc = wl_thread_create(&thread, release_worker, &context);
    CHECK(rc == 0);
    if (rc == 0)
        wl_thread_join(&thread);
    else
        CHECK(wl_arena_compound_arena_gc_hold_release(&context.hold) == 0);
    CHECK(context.errors == 0 && !context.hold.arena);
    CHECK(wl_compound_arena_mutation_end(&mutation));
    CHECK(wl_arena_compound_arena_gc_hold_acquire(context.arena,
        &context.hold) == 0);
    rc = wl_thread_create(&thread, churn_worker, &context);
    CHECK(rc == 0);
    for (unsigned i = 0; i < 10000; i++) {
        reclaimed = 123;
        CHECK(wl_arena_compound_arena_gc_epoch_boundary_checked(context.arena,
            &reclaimed) == EBUSY);
        CHECK(reclaimed == 123);
        CHECK(!wl_compound_arena_free_checked(context.arena));
    }
    if (rc == 0)
        wl_thread_join(&thread);
    CHECK(context.errors == 0 && context.arena->current_epoch == 0);
    CHECK(wl_arena_compound_arena_gc_hold_release(&context.hold) == 0);
    CHECK(wl_compound_arena_free_checked(context.arena));
}

int
main(void)
{
    test_lifetime();
    test_tokens();
    test_concurrency();
    return failures ? 1 : 0;
}
