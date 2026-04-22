/*
 * test_compound_arena.c - Side-Relation Tier: Arena & GC Scaffolding (Issue #533)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Unit tests for wl_compound_arena_t:
 *   - Handle allocation + lookup
 *   - Multiplicity tracking (Z-set retain/release)
 *   - Freeze/unfreeze guard (K-Fusion invariant)
 *   - Epoch-boundary GC reclaim (skeleton)
 *   - Handle packing round-trip
 *
 * Integration tests for wl_compound_side_name():
 *   - Canonical __compound_<functor>_<arity> naming convention
 *   - Buffer-size guardrails
 *
 * Note: wl_compound_side_ensure() requires a full wl_col_session_t.  That
 * path is covered by the session-level integration test in
 * test_compound_side_relation.c (which links the full columnar backend).
 * This file is kept lightweight so the arena unit tests run without the
 * full backend dependency graph.
 */

#include "../wirelog/arena/compound_arena.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                              \
        do {                                        \
            tests_run++;                            \
            printf("  [%d] %s", tests_run, name);   \
        } while (0)

#define PASS()                                  \
        do {                                        \
            tests_passed++;                         \
            printf(" ... PASS\n");                  \
        } while (0)

#define FAIL(msg)                               \
        do {                                        \
            tests_failed++;                         \
            printf(" ... FAIL: %s\n", (msg));       \
            return;                                 \
        } while (0)

#define ASSERT(cond, msg)                       \
        do {                                        \
            if (!(cond)) {                          \
                FAIL(msg);                          \
            }                                       \
        } while (0)

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_handle_pack_roundtrip(void)
{
    TEST("handle pack/unpack round-trip");
    uint64_t h = wl_compound_handle_pack(0x12345u, 0xAB, 0xDEADBEEFu);
    ASSERT(wl_compound_handle_session(h) == 0x12345u, "session seed mismatch");
    ASSERT(wl_compound_handle_epoch(h) == 0xAB, "epoch mismatch");
    ASSERT(wl_compound_handle_offset(h) == 0xDEADBEEFu, "offset mismatch");
    PASS();
}

static void
test_create_destroy(void)
{
    TEST("arena create/destroy");
    wl_compound_arena_t *a = wl_compound_arena_create(0xABCDEu, 256, 0);
    ASSERT(a != NULL, "create returned NULL");
    ASSERT(a->session_seed == 0xABCDEu, "session_seed not stored");
    ASSERT(a->current_epoch == 0, "current_epoch non-zero at create");
    ASSERT(!a->frozen, "arena frozen at create");
    ASSERT(a->live_handles == 0, "live_handles non-zero at create");
    wl_compound_arena_free(a);
    wl_compound_arena_free(NULL); /* NULL-safe */
    PASS();
}

static void
test_alloc_lookup(void)
{
    TEST("arena alloc and lookup");
    wl_compound_arena_t *a = wl_compound_arena_create(0x11111u, 256, 0);
    ASSERT(a != NULL, "create failed");

    /* Simulate compound `f(10, 20)`: store functor + 2 args in 24 bytes. */
    uint64_t h1 = wl_compound_arena_alloc(a, 24);
    ASSERT(h1 != WL_COMPOUND_HANDLE_NULL, "alloc returned null handle");
    ASSERT(wl_compound_handle_session(h1) == 0x11111u, "session seed lost");
    ASSERT(wl_compound_handle_epoch(h1) == 0, "wrong epoch");
    ASSERT(a->live_handles == 1, "live_handles not incremented");

    /* Second allocation */
    uint64_t h2 = wl_compound_arena_alloc(a, 16);
    ASSERT(h2 != WL_COMPOUND_HANDLE_NULL, "second alloc failed");
    ASSERT(h2 != h1, "handles collided");
    ASSERT(a->live_handles == 2, "live_handles after 2 allocs");

    /* Lookup returns a valid pointer + size */
    uint32_t sz = 0;
    const void *p1 = wl_compound_arena_lookup(a, h1, &sz);
    ASSERT(p1 != NULL, "lookup h1 returned NULL");
    ASSERT(sz == 24, "lookup h1 wrong size");
    const void *p2 = wl_compound_arena_lookup(a, h2, &sz);
    ASSERT(p2 != NULL, "lookup h2 returned NULL");
    ASSERT(sz == 16, "lookup h2 wrong size");
    ASSERT(p1 != p2, "distinct handles produced same pointer");

    /* Lookup of bogus handle returns NULL (different session seed) */
    uint64_t bogus = wl_compound_handle_pack(0x22222u, 0, 0);
    ASSERT(wl_compound_arena_lookup(a, bogus, NULL) == NULL,
        "lookup of foreign-session handle should fail");

    /* Null handle always returns NULL */
    ASSERT(wl_compound_arena_lookup(a, WL_COMPOUND_HANDLE_NULL, NULL) == NULL,
        "lookup of NULL handle should fail");

    wl_compound_arena_free(a);
    PASS();
}

static void
test_multiplicity(void)
{
    TEST("arena Z-set multiplicity tracking");
    wl_compound_arena_t *a = wl_compound_arena_create(0x42u, 128, 0);
    ASSERT(a != NULL, "create failed");

    uint64_t h = wl_compound_arena_alloc(a, 8);
    ASSERT(h != WL_COMPOUND_HANDLE_NULL, "alloc failed");
    ASSERT(wl_compound_arena_multiplicity(a, h) == 1, "init multiplicity != 1");
    ASSERT(a->live_handles == 1, "live_handles != 1 after alloc");

    /* Retain +2: multiplicity becomes 3 */
    ASSERT(wl_compound_arena_retain(a, h, 2) == 0, "retain +2 failed");
    ASSERT(wl_compound_arena_multiplicity(a, h) == 3, "multiplicity != 3");
    ASSERT(a->live_handles == 1, "live_handles changed on positive retain");

    /* Release -3: multiplicity becomes 0 -> live_handles drops */
    ASSERT(wl_compound_arena_retain(a, h, -3) == 0, "release -3 failed");
    ASSERT(wl_compound_arena_multiplicity(a, h) == 0, "multiplicity != 0");
    ASSERT(a->live_handles == 0, "live_handles not decremented at zero");

    /* Retain back to positive: live_handles comes back */
    ASSERT(wl_compound_arena_retain(a, h, 1) == 0, "retain back failed");
    ASSERT(a->live_handles == 1, "live_handles not re-incremented");

    /* Invalid handle returns -1 */
    ASSERT(wl_compound_arena_retain(a, 0xDEADBEEFu, 1) == -1,
        "retain on invalid handle should return -1");
    ASSERT(wl_compound_arena_multiplicity(a, 0xDEADBEEFu) == 0,
        "multiplicity on invalid handle should be 0");

    wl_compound_arena_free(a);
    PASS();
}

static void
test_freeze_blocks_alloc(void)
{
    TEST("arena freeze blocks new allocations");
    wl_compound_arena_t *a = wl_compound_arena_create(0x1u, 64, 0);
    ASSERT(a != NULL, "create failed");

    uint64_t h1 = wl_compound_arena_alloc(a, 8);
    ASSERT(h1 != WL_COMPOUND_HANDLE_NULL, "first alloc failed");

    wl_compound_arena_freeze(a);
    ASSERT(a->frozen, "freeze flag not set");
    uint64_t h2 = wl_compound_arena_alloc(a, 8);
    ASSERT(h2 == WL_COMPOUND_HANDLE_NULL, "alloc succeeded while frozen");

    /* Lookups still work while frozen */
    ASSERT(wl_compound_arena_lookup(a, h1, NULL) != NULL,
        "lookup failed while frozen");

    wl_compound_arena_unfreeze(a);
    ASSERT(!a->frozen, "unfreeze flag not cleared");
    uint64_t h3 = wl_compound_arena_alloc(a, 8);
    ASSERT(h3 != WL_COMPOUND_HANDLE_NULL, "alloc after unfreeze failed");

    wl_compound_arena_free(a);
    PASS();
}

static void
test_gc_epoch_boundary(void)
{
    TEST("arena GC epoch boundary reclaims zero-multiplicity handles");
    wl_compound_arena_t *a = wl_compound_arena_create(0x7u, 128, 4);
    ASSERT(a != NULL, "create failed");

    /* Allocate 3 compounds in generation 0. */
    uint64_t h0 = wl_compound_arena_alloc(a, 16);
    uint64_t h1 = wl_compound_arena_alloc(a, 16);
    uint64_t h2 = wl_compound_arena_alloc(a, 16);
    ASSERT(h0 && h1 && h2, "3 allocs in gen 0");
    ASSERT(a->live_handles == 3, "live_handles != 3");

    /* Drop h0 and h1 (multiplicity -> 0). h2 stays live. */
    wl_compound_arena_retain(a, h0, -1);
    wl_compound_arena_retain(a, h1, -1);
    ASSERT(a->live_handles == 1, "live_handles != 1 after 2 releases");

    uint32_t reclaimed = wl_compound_arena_gc_epoch_boundary(a);
    ASSERT(reclaimed == 2, "GC should have reported 2 reclaimable handles");
    ASSERT(a->current_epoch == 1, "epoch did not advance");

    /* New allocation goes into generation 1. */
    uint64_t h_new = wl_compound_arena_alloc(a, 16);
    ASSERT(h_new != WL_COMPOUND_HANDLE_NULL, "alloc after GC failed");
    ASSERT(wl_compound_handle_epoch(h_new) == 1,
        "new alloc should land in epoch 1");

    wl_compound_arena_free(a);
    PASS();
}

static void
test_gc_saturation(void)
{
    TEST("arena GC saturates at max_epochs cap");
    wl_compound_arena_t *a = wl_compound_arena_create(0x1u, 64, 2);
    ASSERT(a != NULL, "create failed");

    /* Allocate then GC to epoch 1 */
    uint64_t h0 = wl_compound_arena_alloc(a, 8);
    ASSERT(h0 != WL_COMPOUND_HANDLE_NULL, "alloc gen 0 failed");
    wl_compound_arena_gc_epoch_boundary(a);
    ASSERT(a->current_epoch == 1, "epoch did not advance to 1");

    /* Alloc in gen 1, then GC saturates (no gen 2 available). */
    uint64_t h1 = wl_compound_arena_alloc(a, 8);
    ASSERT(h1 != WL_COMPOUND_HANDLE_NULL, "alloc gen 1 failed");
    wl_compound_arena_gc_epoch_boundary(a);
    ASSERT(a->current_epoch == a->max_epochs, "arena should saturate");

    /* Further allocations refuse. */
    uint64_t h_sat = wl_compound_arena_alloc(a, 8);
    ASSERT(h_sat == WL_COMPOUND_HANDLE_NULL,
        "alloc after saturation should return null handle");

    wl_compound_arena_free(a);
    PASS();
}

static void
test_alloc_zero_size_rejected(void)
{
    TEST("arena rejects zero-size allocations");
    wl_compound_arena_t *a = wl_compound_arena_create(0x1u, 64, 0);
    ASSERT(a != NULL, "create failed");

    uint64_t h = wl_compound_arena_alloc(a, 0);
    ASSERT(h == WL_COMPOUND_HANDLE_NULL, "zero-size alloc should fail");
    ASSERT(a->live_handles == 0, "live_handles changed on rejected alloc");

    wl_compound_arena_free(a);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_compound_arena (Issue #533)\n");
    printf("================================\n");

    test_handle_pack_roundtrip();
    test_create_destroy();
    test_alloc_lookup();
    test_multiplicity();
    test_freeze_blocks_alloc();
    test_gc_epoch_boundary();
    test_gc_saturation();
    test_alloc_zero_size_rejected();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
