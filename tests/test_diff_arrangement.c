/*
 * tests/test_diff_arrangement.c - Unit tests for differential arrangement structures
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Unit tests for the col_diff_arrangement_t delta-aware hash table implementation.
 */

#include <inttypes.h>
#include <stdint.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>

/* Include the headers we need */
#include "wirelog/columnar/diff_arrangement.h"

/* Test framework */
static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
        do {                                                \
            printf("TEST %d: %s ... ", ++test_count, name); \
            fflush(stdout);                                 \
        } while (0)

#define PASS                               \
        do {                                   \
            printf("PASS\n");                  \
            pass_count++;                      \
        } while (0)

#define FAIL(msg)                          \
        do {                                   \
            printf("FAIL: %s\n", msg);         \
            fail_count++;                      \
        } while (0)

/* Test 1: col_diff_arrangement_create */
static void
test_diff_arrangement_create(void)
{
    TEST("col_diff_arrangement_create");

    uint32_t key_cols[] = {0, 1};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 2, 0);

    if (!darr) {
        FAIL("create returned NULL");
        return;
    }

    col_diff_arrangement_destroy(darr);
    PASS;
}

/* Test 2: col_diff_arrangement_has_delta */
static void
test_diff_arrangement_has_delta(void)
{
    TEST("col_diff_arrangement_has_delta");

    uint32_t key_cols[] = {0};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 1, 0);

    if (!darr) {
        FAIL("create failed");
        return;
    }

    if (col_diff_arrangement_has_delta(darr)) {
        FAIL("new arrangement should have no delta");
        col_diff_arrangement_destroy(darr);
        return;
    }

    col_diff_arrangement_destroy(darr);
    PASS;
}

/* Test 3: col_diff_arrangement_get_delta_range */
static void
test_diff_arrangement_get_delta_range(void)
{
    TEST("col_diff_arrangement_get_delta_range");

    uint32_t key_cols[] = {0};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 1, 0);

    if (!darr) {
        FAIL("create failed");
        return;
    }

    uint32_t base, current;
    col_diff_arrangement_get_delta_range(darr, &base, &current);

    if (base != 0 || current != 0) {
        FAIL("new arrangement should have range [0,0)");
        col_diff_arrangement_destroy(darr);
        return;
    }

    col_diff_arrangement_destroy(darr);
    PASS;
}

/* Test 4: col_diff_arrangement_deep_copy */
static void
test_diff_arrangement_deep_copy(void)
{
    TEST("col_diff_arrangement_deep_copy");

    uint32_t key_cols[] = {0, 1};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 2, 0);
    col_diff_arrangement_t *copy = col_diff_arrangement_deep_copy(darr);

    if (!copy) {
        FAIL("deep_copy returned NULL");
        col_diff_arrangement_destroy(darr);
        return;
    }

    if (col_diff_arrangement_has_delta(copy)) {
        FAIL("copy should match original");
        col_diff_arrangement_destroy(darr);
        col_diff_arrangement_destroy(copy);
        return;
    }

    col_diff_arrangement_destroy(darr);
    col_diff_arrangement_destroy(copy);
    PASS;
}

/* Test 5: col_diff_arrangement_reset_delta */
static void
test_diff_arrangement_reset_delta(void)
{
    TEST("col_diff_arrangement_reset_delta");

    uint32_t key_cols[] = {0};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 1, 0);

    if (!darr) {
        FAIL("create failed");
        return;
    }

    col_diff_arrangement_reset_delta(darr);

    uint32_t base, current;
    col_diff_arrangement_get_delta_range(darr, &base, &current);

    if (base != current) {
        FAIL("reset_delta should align base and current");
        col_diff_arrangement_destroy(darr);
        return;
    }

    col_diff_arrangement_destroy(darr);
    PASS;
}

static void
test_diff_arrangement_governor_growth_commit(void)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    wl_columnar_memory_governor_ref_t *ref;
    wl_columnar_memory_governor_t *governor;
    col_diff_arrangement_t *darr;
    col_diff_arrangement_t *probe;
    col_diff_arrangement_t *denied;
    wl_columnar_memory_governor_ref_t *denial_ref;
    wl_columnar_memory_governor_t *denial_governor;
    wl_columnar_memory_governor_ref_t *growth_ref;
    wl_columnar_memory_governor_t *growth_governor;
    col_diff_arrangement_t *growth_arr;
    uint32_t *old_head;
    uint32_t *old_next;
    uint64_t old_reserved;
    uint64_t old_governor_reserved;
    uint32_t key_cols[] = { 0 };
    uint64_t initial_bytes;

    TEST("governed growth commits its reservation");
    resolution.budget_bytes = UINT64_C(1024) * 1024;
    resolution.usable_bytes = resolution.budget_bytes;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    probe = col_diff_arrangement_create(key_cols, 1, 0);
    if (!probe) {
        FAIL("ungoverned probe creation failed");
        return;
    }
    initial_bytes = col_diff_arrangement_bytes(probe);
    col_diff_arrangement_destroy(probe);
    resolution.budget_bytes = initial_bytes - 1u;
    resolution.usable_bytes = resolution.budget_bytes;
    denial_ref = wl_columnar_memory_governor_ref_create(&resolution);
    denial_governor = denial_ref
        ? wl_columnar_memory_governor_ref_get(denial_ref) : NULL;
    denied = denial_ref
        ? col_diff_arrangement_create_with_memory_governor(
        key_cols, 1, 0, denial_ref) : NULL;
    if (!denial_ref
        || denied != NULL
        || wl_columnar_memory_reserved(denial_governor) != 0) {
        FAIL("admission denial changed governed capacity");
        col_diff_arrangement_destroy(denied);
        if (denial_ref)
            wl_columnar_memory_governor_ref_release(denial_ref);
        return;
    }
    wl_columnar_memory_governor_ref_release(denial_ref);
    resolution.budget_bytes = initial_bytes + 1u;
    resolution.usable_bytes = resolution.budget_bytes;
    growth_ref = wl_columnar_memory_governor_ref_create(&resolution);
    growth_governor = growth_ref
        ? wl_columnar_memory_governor_ref_get(growth_ref) : NULL;
    growth_arr = growth_ref
        ? col_diff_arrangement_create_with_memory_governor(
        key_cols, 1, 0, growth_ref) : NULL;
    old_head = growth_arr ? growth_arr->ht_head : NULL;
    old_next = growth_arr ? growth_arr->ht_next : NULL;
    old_reserved = growth_arr ? growth_arr->reserved_bytes : 0;
    old_governor_reserved = growth_governor
        ? wl_columnar_memory_reserved(growth_governor) : 0;
    if (!growth_ref || !growth_arr
        || col_diff_arrangement_ensure_ht_capacity(
            growth_arr, growth_arr->ht_cap + 1u) != ENOMEM
        || growth_arr->ht_head != old_head
        || growth_arr->ht_next != old_next
        || growth_arr->reserved_bytes != old_reserved
        || wl_columnar_memory_reserved(growth_governor)
        != old_governor_reserved) {
        FAIL("denied growth changed the existing arrangement");
        col_diff_arrangement_destroy(growth_arr);
        if (growth_ref)
            wl_columnar_memory_governor_ref_release(growth_ref);
        return;
    }
    col_diff_arrangement_destroy(growth_arr);
    if (wl_columnar_memory_reserved(growth_governor) != 0) {
        FAIL("denied growth leaked governed capacity");
        wl_columnar_memory_governor_ref_release(growth_ref);
        return;
    }
    wl_columnar_memory_governor_ref_release(growth_ref);
    resolution.budget_bytes = UINT64_C(1024) * 1024;
    resolution.usable_bytes = resolution.budget_bytes;
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    if (!ref) {
        FAIL("governor reference creation failed");
        return;
    }
    governor = wl_columnar_memory_governor_ref_get(ref);
    darr = col_diff_arrangement_create_with_memory_governor(
        key_cols, 1, 0, ref);
    if (!darr
        || atomic_load_explicit(&darr->reservation.state,
        memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || atomic_load_explicit(&darr->reservation.owner_bits,
        memory_order_acquire) != (uintptr_t)darr
        || !wl_columnar_memory_reservation_downsize(
            &darr->reservation, darr->reserved_bytes)) {
        FAIL("initial arrangement reservation was not committed");
        col_diff_arrangement_destroy(darr);
        wl_columnar_memory_governor_ref_release(ref);
        return;
    }
    if (col_diff_arrangement_ensure_ht_capacity(darr, darr->ht_cap + 1u)
        != 0
        || atomic_load_explicit(&darr->reservation.state,
        memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || atomic_load_explicit(&darr->reservation.owner_bits,
        memory_order_acquire) != (uintptr_t)darr
        || !wl_columnar_memory_reservation_downsize(
            &darr->reservation, darr->reserved_bytes)) {
        FAIL("growth reservation was not committed");
        col_diff_arrangement_destroy(darr);
        wl_columnar_memory_governor_ref_release(ref);
        return;
    }
    col_diff_arrangement_destroy(darr);
    if (wl_columnar_memory_reserved(governor) != 0) {
        FAIL("arrangement destruction leaked governed capacity");
        wl_columnar_memory_governor_ref_release(ref);
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS;
}

int
main(void)
{
    printf("\nTesting Differential Arrangement Structures\n");
    printf("==========================================\n\n");

    test_diff_arrangement_create();
    test_diff_arrangement_has_delta();
    test_diff_arrangement_get_delta_range();
    test_diff_arrangement_deep_copy();
    test_diff_arrangement_reset_delta();
    test_diff_arrangement_governor_growth_commit();

    printf("\n==========================================\n");
    printf("Results: %d/%d passed\n", pass_count, test_count);

    if (fail_count == 0) {
        printf("All tests passed!\n");
        printf("==========================================\n\n");
        return 0;
    }

    printf("FAILED: %d test(s)\n", fail_count);
    printf("==========================================\n\n");
    return 1;
}
