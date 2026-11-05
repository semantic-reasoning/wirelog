/*
 * test_mat_cache_lifetime.c - materialization-cache lifetime contracts
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/delta_pool.h"
#include "../wirelog/arena/arena.h"

#include <stdio.h>
#include <string.h>

static int tests_run;
static int tests_failed;

#define ASSERT_TRUE(condition, message)                                      \
        do {                                                                      \
            if (!(condition)) {                                                   \
                fprintf(stderr, "FAIL: %s\n", (message));                       \
                tests_failed++;                                                  \
                return;                                                           \
            }                                                                     \
        } while (0)

static col_rel_t *
make_relation(int64_t value)
{
    col_rel_t *rel = col_rel_new_auto("cache-test", 1);
    if (!rel)
        return NULL;
    if (col_rel_append_row(rel, &value) != 0) {
        col_rel_destroy(rel);
        return NULL;
    }
    return rel;
}

static col_mat_cache_pin_t
make_stale_pin(col_mat_cache_t *cache)
{
    col_mat_cache_pin_t pin = {
        .cache = cache,
        .identity = 12345,
        .active = true,
    };
    return pin;
}

static bool
pin_is_inactive(const col_mat_cache_pin_t *pin)
{
    return pin->cache == NULL && pin->identity == 0 && !pin->active
           && pin->self == NULL;
}

/* Existing-API regression shared byte-for-byte with the old-layout witness.
 * Inspect debt before touching result: the old bug already freed it. */
static void
test_copied_pin_cannot_release_native_debt(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left = make_relation(101);
    col_rel_t *right = make_relation(102);
    col_rel_t *result = make_relation(103);
    ASSERT_TRUE(left && right && result, "copy witness relations allocated");
    ASSERT_TRUE(col_mat_cache_insert(&cache, left, right, result) == 0,
        "copy witness inserts a real cache result");
    col_mat_cache_pin_t pin = { 0 };
    ASSERT_TRUE(col_mat_cache_lookup_pin(&cache, left, right, &pin) == result,
        "copy witness obtains a real issued pin");
    col_mat_cache_pin_t copy = pin;
    col_mat_cache_clear(&cache);
    size_t debt = cache.total_bytes;
    ASSERT_TRUE(debt > 0 && cache.count == 1 && cache.active_pins == 1
        && cache.entries[0].pin_count == 1
        && cache.entries[0].eviction_deferred,
        "clear retains original native debt");
    col_mat_cache_pin_release(&copy);
    ASSERT_TRUE(cache.count == 1 && cache.active_pins == 1
        && cache.total_bytes == debt && cache.entries[0].pin_count == 1,
        "copied pin release preserves original deferred debt");
    ASSERT_TRUE(col_rel_get(result, 0, 0) == 103,
        "original issued pin still protects literal result");
    col_mat_cache_pin_release(&copy);
    col_mat_cache_pin_release(&pin);
    ASSERT_TRUE(cache.count == 0 && cache.active_pins == 0
        && cache.total_bytes == 0,
        "original release retires debt exactly once");
    col_mat_cache_pin_release(&pin);
    col_mat_cache_pin_release(&copy);
    col_rel_destroy(left);
    col_rel_destroy(right);
}

/* No snapshot below is ever restored into a live resource handle. */
static void
test_pin_move_checks_and_adjacent_storage(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left = make_relation(201), *right = make_relation(202);
    col_rel_t *result = make_relation(203);
    col_mat_cache_pin_t pair[2] = { 0 };
    ASSERT_TRUE(left && right && result, "move fixture allocated");
    ASSERT_TRUE(col_mat_cache_insert_pin(&cache, left, right, result,
        &pair[0]) == 0, "insert issues real native pin");
    ASSERT_TRUE(pair[0].self == &pair[0], "issuer binds final pin address");
    col_mat_cache_pin_t copy = pair[0], inert = { 0 };
    unsigned char before[sizeof(pair)], copy_before[sizeof(copy)];
    memcpy(before, pair, sizeof(pair));
    memcpy(copy_before, &copy, sizeof(copy));
    ASSERT_TRUE(wl_columnar_cache_pin_move(NULL, &pair[0]) == EINVAL
        && wl_columnar_cache_pin_move(&pair[1], NULL) == EINVAL
        && wl_columnar_cache_pin_move(&pair[0], &pair[0]) == EINVAL,
        "NULL and same-address moves reject");
    ASSERT_TRUE(memcmp(before, pair, sizeof(pair)) == 0,
        "NULL and same-address failures preserve all bytes");
    ASSERT_TRUE(wl_columnar_cache_pin_move(&pair[1], &copy) == EINVAL
        && wl_columnar_cache_pin_move(&pair[1], &inert) == EINVAL,
        "copied and inert sources have no move authority");
    ASSERT_TRUE(memcmp(before, pair, sizeof(pair)) == 0
        && memcmp(copy_before, &copy, sizeof(copy)) == 0
        && pin_is_inactive(&inert),
        "invalid-source failures write neither pin");
    col_mat_cache_pin_release(&copy);
    ASSERT_TRUE(memcmp(copy_before, &copy, sizeof(copy)) == 0
        && cache.active_pins == 1 && cache.entries[0].pin_count == 1,
        "active original copy release is a byte-preserving no-op");

    col_mat_cache_pin_t live = { 0 };
    ASSERT_TRUE(col_mat_cache_lookup_pin(&cache, left, right, &live) == result,
        "second genuine pin is a live move destination");
    unsigned char live_before[sizeof(live)];
    memcpy(live_before, &live, sizeof(live));
    ASSERT_TRUE(wl_columnar_cache_pin_move(&live, &pair[0]) == EBUSY,
        "live destination refuses without losing either resource");
    ASSERT_TRUE(memcmp(live_before, &live, sizeof(live)) == 0
        && memcmp(before, pair, sizeof(pair)) == 0
        && cache.active_pins == 2 && cache.entries[0].pin_count == 2,
        "EBUSY preserves exact bytes and both native debts");
    col_mat_cache_pin_release(&live);

    /* Backed aligned regions are never interpreted as pins: the full range
     * check must refuse both directions before reading resource fields. */
    union {
        col_mat_cache_pin_t alignment;
        unsigned char bytes[2 * sizeof(col_mat_cache_pin_t)];
    } overlap;
    memset(overlap.bytes, 0xa5, sizeof(overlap.bytes));
    unsigned char overlap_before[sizeof(overlap.bytes)];
    memcpy(overlap_before, overlap.bytes, sizeof(overlap.bytes));
    col_mat_cache_pin_t *a = (col_mat_cache_pin_t *)overlap.bytes;
    col_mat_cache_pin_t *b = (col_mat_cache_pin_t *)(overlap.bytes
        + _Alignof(col_mat_cache_pin_t));
    ASSERT_TRUE(wl_columnar_cache_pin_move(a, b) == EINVAL
        && wl_columnar_cache_pin_move(b, a) == EINVAL,
        "both partial-overlap directions reject before any field access");
    ASSERT_TRUE(memcmp(overlap_before, overlap.bytes, sizeof(overlap.bytes))
        == 0, "overlap failures preserve every backed byte");

    size_t debt = cache.total_bytes;
    ASSERT_TRUE(wl_columnar_cache_pin_move(&pair[1], &pair[0]) == 0,
        "exactly adjacent objects permit real authority move");
    ASSERT_TRUE(pin_is_inactive(&pair[0]) && pair[1].self == &pair[1]
        && pair[1].active && pair[1].cache == &cache
        && cache.total_bytes == debt && cache.active_pins == 1
        && cache.entries[0].pin_count == 1,
        "move changes addresses without changing one native debt");
    ASSERT_TRUE(wl_columnar_cache_pin_move(&pair[0], &pair[1]) == 0
        && pin_is_inactive(&pair[1]) && pair[0].self == &pair[0]
        && wl_columnar_cache_pin_move(&pair[1], &pair[0]) == 0,
        "both exactly adjacent address orders transfer native authority");
    col_mat_cache_pin_t moved_copy = pair[1];
    col_mat_cache_clear(&cache);
    col_mat_cache_pin_release(&moved_copy);
    col_mat_cache_pin_release(&copy);
    col_mat_cache_pin_release(&pair[0]);
    ASSERT_TRUE(cache.count == 1 && cache.active_pins == 1,
        "old source and pre-move copy cannot release moved authority");
    ASSERT_TRUE(col_rel_get(result, 0, 0) == 203,
        "moved pin protects literal raw result after clear");
    col_mat_cache_pin_release(&pair[1]);
    ASSERT_TRUE(pin_is_inactive(&pair[1]) && cache.count == 0
        && cache.active_pins == 0 && cache.total_bytes == 0,
        "moved original releases once and clears all four fields");
    col_mat_cache_pin_release(&pair[1]);
    col_mat_cache_pin_release(&copy);
    col_rel_destroy(left);
    col_rel_destroy(right);
}

static void
test_pin_move_clear_and_compaction(void)
{
    tests_run++;
    /* Each identity is first to retire once; surviving identities shift after
     * first/middle removal. Exercise clear on both sides of every move. */
    for (unsigned clear_first = 0; clear_first < 2; clear_first++) {
        for (unsigned first = 0; first < 3; first++) {
            col_mat_cache_t cache = { 0 };
            col_rel_t *left[3], *right[3], *result[3];
            col_mat_cache_pin_t issued[3] = { 0 }, moved[3] = { 0 };
            for (unsigned i = 0; i < 3; i++) {
                left[i] = make_relation(300 + i);
                right[i] = make_relation(310 + i);
                result[i] = make_relation(320 + i);
                ASSERT_TRUE(left[i] && right[i] && result[i],
                    "compaction relations allocated");
                ASSERT_TRUE(col_mat_cache_insert_pin(&cache, left[i],
                    right[i], result[i], &issued[i]) == 0,
                    "three actual issued identities");
            }
            size_t debt = cache.total_bytes;
            if (clear_first)
                col_mat_cache_clear(&cache);
            for (unsigned i = 0; i < 3; i++)
                ASSERT_TRUE(wl_columnar_cache_pin_move(&moved[i],
                    &issued[i]) == 0 && pin_is_inactive(&issued[i]),
                    "each pin moves to its final array address");
            if (!clear_first)
                col_mat_cache_clear(&cache);
            ASSERT_TRUE(cache.count == 3 && cache.active_pins == 3
                && cache.total_bytes == debt, "clear and moves preserve debt");
            for (unsigned step = 0; step < 3; step++) {
                unsigned i = (first + step) % 3;
                ASSERT_TRUE(col_rel_get(result[i], 0, 0) == 320 + i,
                    "stable moved identity protects row after compaction");
                col_mat_cache_pin_release(&issued[i]);
                col_mat_cache_pin_release(&moved[i]);
                ASSERT_TRUE(cache.count == 2 - step
                    && cache.active_pins == 2 - step
                    && pin_is_inactive(&moved[i]),
                    "first middle last release removes exactly one debt");
            }
            ASSERT_TRUE(cache.total_bytes == 0, "compaction refunds all bytes");
            for (unsigned i = 0; i < 3; i++) {
                col_rel_destroy(left[i]);
                col_rel_destroy(right[i]);
            }
        }
    }
}

static void
test_pin_move_preserves_governor_and_ledger_peer(void)
{
    tests_run++;
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    ASSERT_TRUE(g, "governor allocated");
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(g);
    col_rel_t *peer = NULL, *result = NULL;
    ASSERT_TRUE(wl_columnar_relation_new_auto_governed("pin-peer", 1, 1,
        false, g, &peer) == 0, "real governed peer allocated");
    uint64_t peer_bytes = atomic_load_explicit(&governor->reserved_bytes,
            memory_order_acquire);
    ASSERT_TRUE(peer_bytes > 0, "peer has real committed credit");
    ASSERT_TRUE(wl_columnar_relation_new_auto_governed("pin-result", 1, 1,
        false, g, &result) == 0, "real governed result allocated");
    int64_t value = 403;
    ASSERT_TRUE(col_rel_append_row(result, &value) == 0,
        "governed literal result populated");
    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_CACHE, 17);
    col_mat_cache_t cache = { 0 };
    cache.ledger = &ledger;
    ASSERT_TRUE(col_mat_cache_attach_reclaimer(&cache) == 0,
        "real ledger reclaimer attached");
    col_rel_t *left = make_relation(401), *right = make_relation(402);
    col_mat_cache_pin_t source = { 0 }, destination = { 0 };
    ASSERT_TRUE(left && right && col_mat_cache_insert_pin(&cache, left,
        right, result, &source) == 0, "governed result genuinely inserted");
    uint64_t reserved = atomic_load_explicit(&governor->reserved_bytes,
            memory_order_acquire);
    uint64_t accounted = atomic_load_explicit(&ledger.current_bytes,
            memory_order_acquire);
    ASSERT_TRUE(reserved > peer_bytes
        && accounted == col_rel_transport_bytes(result) + 17,
        "real result and unrelated peer separately accounted");
    col_mat_cache_clear(&cache);
    ASSERT_TRUE(wl_columnar_cache_pin_move(&destination, &source) == 0,
        "deferred governed pin moves");
    wl_mem_reclaim_result_t reclaim = wl_mem_ledger_reclaim(&ledger);
    ASSERT_TRUE(reclaim.bytes_released == 0 && reclaim.candidates == 0
        && atomic_load_explicit(&governor->reserved_bytes,
        memory_order_acquire) == reserved
        && atomic_load_explicit(&ledger.current_bytes,
        memory_order_acquire) == accounted,
        "move and reclaimer preserve pinned credit and ledger debt");
    ASSERT_TRUE(col_rel_get(result, 0, 0) == 403,
        "governed result remains readable under moved pin");
    col_mat_cache_pin_release(&destination);
    col_mat_cache_pin_release(&source);
    ASSERT_TRUE(cache.count == 0 && cache.active_pins == 0
        && atomic_load_explicit(&governor->reserved_bytes,
        memory_order_acquire) == peer_bytes
        && atomic_load_explicit(&ledger.current_bytes,
        memory_order_acquire) == 17,
        "actual last release refunds result only and preserves both peers");
    col_mat_cache_detach_reclaimer(&cache);
    wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_CACHE, 17);
    col_rel_destroy(peer);
    ASSERT_TRUE(atomic_load_explicit(&governor->reserved_bytes,
        memory_order_acquire) == 0,
        "peer destruction returns governor baseline");
    wl_columnar_memory_governor_ref_release(g);
    col_rel_destroy(left);
    col_rel_destroy(right);
}

static void
test_pin_output_normalization_order(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left = make_relation(501), *right = make_relation(502);
    col_rel_t *result = make_relation(503);
    ASSERT_TRUE(left && right && result, "normalization fixture allocated");
    /* A nonnative sentinel only tests output normalization. Never release it. */
    col_mat_cache_pin_t pin = make_stale_pin(&cache);
    unsigned char before[sizeof(pin)];
    memcpy(before, &pin, sizeof(pin));
    ASSERT_TRUE(col_mat_cache_lookup_pin(NULL, left, right, &pin) == NULL
        && memcmp(before, &pin, sizeof(pin)) == 0,
        "invalid lookup preserves prior output bytes before normalization");
    ASSERT_TRUE(col_mat_cache_lookup_pin(&cache, left, right, &pin) == NULL
        && pin_is_inactive(&pin), "valid lookup miss clears all four fields");
    /* Fresh output bytes need no incoming active/self initialization. */
    memset(&pin, 0xa5, sizeof(pin));
    ASSERT_TRUE(col_mat_cache_insert_pin(&cache, left, right, result, &pin)
        == 0 && pin.active && pin.self == &pin && pin.cache == &cache,
        "insert normalizes fresh output before publishing native identity");
    col_mat_cache_pin_release(&pin);
    memset(&pin, 0xa5, sizeof(pin));
    ASSERT_TRUE(col_mat_cache_lookup_pin(&cache, left, right, &pin) == result
        && pin.active && pin.self == &pin && cache.active_pins == 1,
        "lookup normalizes fresh output before publishing native identity");
    col_mat_cache_clear(&cache);
    col_mat_cache_pin_release(&pin);
    ASSERT_TRUE(pin_is_inactive(&pin) && cache.count == 0,
        "normalized issued output releases through native path");
    col_rel_destroy(left);
    col_rel_destroy(right);
}

static void
test_rejects_nonexclusive_results(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left = make_relation(80);
    col_rel_t *right = make_relation(81);
    col_rel_t *source = make_relation(82);
    col_rel_t *shared = make_relation(83);
    ASSERT_TRUE(left && right && source && shared,
        "nonexclusive cache relations allocated");
    ASSERT_TRUE(col_rel_install_shared_view(shared, source) == 0,
        "shared result setup succeeds");
    ASSERT_TRUE(col_mat_cache_insert(&cache, left, right, shared) == EINVAL,
        "shared result is rejected");
    ASSERT_TRUE(cache.count == 0 && cache.total_bytes == 0
        && cache.active_pins == 0,
        "shared rejection leaves cache unchanged");
    ASSERT_TRUE(col_rel_get(shared, 0, 0) == 82,
        "shared result remains readable after rejection");
    col_rel_destroy(shared);
    col_rel_destroy(source);

    col_rel_t *same = make_relation(85);
    ASSERT_TRUE(same && col_mat_cache_insert(&cache, same, same, same)
        == EINVAL,
        "input/result pointer alias is rejected");
    ASSERT_TRUE(cache.count == 0 && cache.total_bytes == 0,
        "input/result rejection leaves cache unchanged");
    col_rel_destroy(same);

    delta_pool_t *pool = delta_pool_create(4, sizeof(col_rel_t), 64 * 1024);
    wl_arena_t *arena = wl_arena_create(64 * 1024);
    col_rel_t *arena_result = pool && arena
        ? col_rel_pool_new_auto(pool, arena, "arena-cache-result", 1) : NULL;
    int64_t value = 84;
    ASSERT_TRUE(arena_result && arena_result->arena_owned,
        "arena result setup succeeds");
    ASSERT_TRUE(col_rel_append_row(arena_result, &value) == 0,
        "arena result row append succeeds");
    ASSERT_TRUE(col_mat_cache_insert(&cache, left, right, arena_result)
        == EINVAL,
        "arena result is rejected");
    ASSERT_TRUE(cache.count == 0 && cache.total_bytes == 0,
        "arena rejection leaves cache unchanged");
    ASSERT_TRUE(col_rel_get(arena_result, 0, 0) == value,
        "arena result remains readable after rejection");
    col_rel_destroy(arena_result);
    delta_pool_destroy(pool);
    wl_arena_free(arena);

    delta_pool_t *pool_only = delta_pool_create(4, sizeof(col_rel_t),
            64 * 1024);
    col_rel_t *pool_result = pool_only
        ? col_rel_pool_new_auto(pool_only, NULL, "pool-cache-result", 1)
        : NULL;
    ASSERT_TRUE(pool_result && pool_result->pool_owned
        && !pool_result->arena_owned,
        "pool-only result setup succeeds");
    ASSERT_TRUE(col_rel_append_row(pool_result, &value) == 0,
        "pool-only result row append succeeds");
    ASSERT_TRUE(col_mat_cache_insert(&cache, left, right, pool_result)
        == EINVAL,
        "pool-only result is rejected");
    ASSERT_TRUE(cache.count == 0 && cache.total_bytes == 0,
        "pool rejection leaves cache unchanged");
    col_rel_destroy(pool_result);
    delta_pool_destroy(pool_only);

    col_rel_t *alias_owner = make_relation(86);
    col_rel_t *alias = make_relation(87);
    ASSERT_TRUE(alias_owner && alias
        && col_rel_install_shared_view(alias, alias_owner) == 0,
        "owner alias setup succeeds");
    ASSERT_TRUE(alias_owner->storage_alias_borrows > 0
        && col_mat_cache_insert(&cache, left, right, alias_owner) == EINVAL,
        "owner with a live alias is rejected");
    ASSERT_TRUE(cache.count == 0 && cache.total_bytes == 0,
        "live-alias rejection leaves cache unchanged");
    col_rel_destroy(alias);
    col_rel_destroy(alias_owner);

    col_rel_destroy(left);
    col_rel_destroy(right);
}

static void
test_insert_pin_failure_zeros_stale_pin(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left = make_relation(90);
    col_rel_t *right = make_relation(91);
    col_rel_t *result = make_relation(92);
    ASSERT_TRUE(left && right && result,
        "stale-pin failure relations allocated");

    col_mat_cache_pin_t pin = make_stale_pin(&cache);
    ASSERT_TRUE(col_mat_cache_insert_pin(NULL, left, right, result, &pin)
        == EINVAL,
        "NULL cache insertion reports EINVAL");
    ASSERT_TRUE(pin_is_inactive(&pin),
        "NULL cache insertion clears stale out pin");

    ASSERT_TRUE(col_mat_cache_insert(&cache, left, right, result) == 0,
        "stale-pin duplicate setup insert succeeds");
    pin = make_stale_pin(&cache);
    ASSERT_TRUE(col_mat_cache_insert_pin(&cache, left, right, result, &pin)
        == EEXIST,
        "duplicate result insertion reports EEXIST");
    ASSERT_TRUE(pin_is_inactive(&pin),
        "duplicate result insertion clears stale out pin");

    col_rel_t *alias_owner = make_relation(93);
    col_rel_t *alias = make_relation(94);
    ASSERT_TRUE(alias_owner && alias
        && col_rel_install_shared_view(alias, alias_owner) == 0,
        "stale-pin alias setup succeeds");
    pin = make_stale_pin(&cache);
    ASSERT_TRUE(col_mat_cache_insert_pin(&cache, left, right, alias_owner,
        &pin) == EINVAL,
        "nonexclusive insertion reports EINVAL");
    ASSERT_TRUE(pin_is_inactive(&pin),
        "nonexclusive insertion clears stale out pin");
    col_rel_destroy(alias);
    col_rel_destroy(alias_owner);

    col_mat_cache_clear(&cache);
    col_rel_destroy(left);
    col_rel_destroy(right);
}

static void
test_lookup_clear_and_release(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left = make_relation(1);
    col_rel_t *right = make_relation(2);
    col_rel_t *result = make_relation(3);
    ASSERT_TRUE(left && right && result, "relations allocated");
    ASSERT_TRUE(col_mat_cache_insert(&cache, left, right, result) == 0,
        "cache insert succeeds");
    ASSERT_TRUE(col_mat_cache_insert(&cache, left, right, result) == EEXIST,
        "duplicate result insertion is rejected");
    ASSERT_TRUE(cache.count == 1,
        "duplicate result rejection leaves the entry count unchanged");

    col_mat_cache_pin_t pin = { 0 };
    ASSERT_TRUE(col_mat_cache_lookup_pin(&cache, left, right, &pin) == result,
        "lookup returns cached result");
    ASSERT_TRUE(cache.active_pins == 1,
        "lookup pin increments active-pin accounting");
    col_mat_cache_clear(&cache);
    ASSERT_TRUE(cache.count == 1 && cache.entries[0].result == result,
        "clear defers destruction of pinned entry");
    ASSERT_TRUE(col_mat_cache_lookup(&cache, left, right) == NULL,
        "deferred entry is not visible to legacy lookup");
    ASSERT_TRUE(cache.active_pins == 1,
        "clear preserves active-pin accounting");
    col_mat_cache_pin_release(&pin);
    ASSERT_TRUE(cache.count == 0 && cache.total_bytes == 0,
        "release destroys deferred entry and accounting is zero");
    ASSERT_TRUE(cache.active_pins == 0,
        "release decrements active-pin accounting exactly once");

    col_rel_destroy(left);
    col_rel_destroy(right);
}

static void
test_pin_survives_compaction_and_truncate(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left[3] = { make_relation(10), make_relation(20),
                           make_relation(30) };
    col_rel_t *right[3] = { make_relation(11), make_relation(21),
                            make_relation(31) };
    col_rel_t *result[3] = { make_relation(12), make_relation(22),
                             make_relation(32) };
    for (size_t i = 0; i < 3; i++)
        ASSERT_TRUE(left[i] && right[i] && result[i], "relations allocated");
    for (size_t i = 0; i < 3; i++)
        ASSERT_TRUE(col_mat_cache_insert(&cache, left[i], right[i], result[i])
            == 0,
            "cache insert succeeds");

    col_mat_cache_pin_t pin = { 0 };
    ASSERT_TRUE(col_mat_cache_lookup_pin(&cache, left[1], right[1], &pin)
        == result[1],
        "middle entry is pinned");
    col_mat_cache_truncate(&cache, 0);
    ASSERT_TRUE(cache.count == 1 && cache.entries[0].result == result[1],
        "truncate compacts around pinned entry");
    ASSERT_TRUE(col_mat_cache_lookup(&cache, left[1], right[1]) == NULL,
        "truncated pinned entry is hidden");
    col_mat_cache_pin_release(&pin);
    ASSERT_TRUE(cache.count == 0, "compacted pinned entry is released");
    ASSERT_TRUE(cache.active_pins == 0,
        "compaction pin release leaves no active pins");

    for (size_t i = 0; i < 3; i++) {
        col_rel_destroy(left[i]);
        col_rel_destroy(right[i]);
    }
}

static void
test_lru_preserves_pinned_entry(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left[COL_MAT_CACHE_MAX + 1];
    col_rel_t *right[COL_MAT_CACHE_MAX + 1];
    col_rel_t *result[COL_MAT_CACHE_MAX + 1];
    for (size_t i = 0; i < COL_MAT_CACHE_MAX + 1; i++) {
        left[i] = make_relation((int64_t)(1000 + i * 3));
        right[i] = make_relation((int64_t)(1001 + i * 3));
        result[i] = make_relation((int64_t)(1002 + i * 3));
        ASSERT_TRUE(left[i] && right[i] && result[i], "relations allocated");
    }
    for (size_t i = 0; i < COL_MAT_CACHE_MAX; i++)
        ASSERT_TRUE(col_mat_cache_insert(&cache, left[i], right[i], result[i])
            == 0,
            "cache fills to capacity");

    col_mat_cache_pin_t pin = { 0 };
    ASSERT_TRUE(col_mat_cache_lookup_pin(&cache, left[0], right[0], &pin)
        == result[0],
        "oldest entry is pinned");
    cache.entries[0].lru_clock = 0;
    ASSERT_TRUE(col_mat_cache_insert(&cache, left[COL_MAT_CACHE_MAX],
        right[COL_MAT_CACHE_MAX], result[COL_MAT_CACHE_MAX]) == 0,
        "full cache evicts an unpinned entry");
    ASSERT_TRUE(col_mat_cache_lookup(&cache, left[0], right[0]) == result[0],
        "LRU preserves the pinned entry");
    col_mat_cache_pin_release(&pin);
    /* The legacy lookup above also holds a pin until the epoch ends. */
    col_mat_cache_release_pins(&cache);
    col_mat_cache_clear(&cache);
    ASSERT_TRUE(cache.count == 0, "quiescent cache clear releases all entries");

    /* Entries inserted into the cache are cache-owned after success. */
    for (size_t i = 0; i < COL_MAT_CACHE_MAX + 1; i++) {
        col_rel_destroy(left[i]);
        col_rel_destroy(right[i]);
    }
}

static void
test_all_pinned_insert_retains_callers_ownership(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left[COL_MAT_CACHE_MAX];
    col_rel_t *right[COL_MAT_CACHE_MAX];
    col_rel_t *result[COL_MAT_CACHE_MAX];
    col_mat_cache_pin_t pins[COL_MAT_CACHE_MAX] = { 0 };
    for (size_t i = 0; i < COL_MAT_CACHE_MAX; i++) {
        left[i] = make_relation((int64_t)(3000 + i * 3));
        right[i] = make_relation((int64_t)(3001 + i * 3));
        result[i] = make_relation((int64_t)(3002 + i * 3));
        ASSERT_TRUE(left[i] && right[i] && result[i],
            "all-pinned relations allocated");
        ASSERT_TRUE(col_mat_cache_insert(&cache, left[i], right[i], result[i])
            == 0,
            "all-pinned cache fills");
    }
    for (size_t i = 0; i < COL_MAT_CACHE_MAX; i++)
        ASSERT_TRUE(col_mat_cache_lookup_pin(&cache, left[i], right[i],
            &pins[i]) == result[i],
            "each full-cache entry is pinned");
    ASSERT_TRUE(cache.active_pins == COL_MAT_CACHE_MAX,
        "full cache accounts for every active pin");

    col_rel_t *candidate_left = make_relation(4000);
    col_rel_t *candidate_right = make_relation(4001);
    col_rel_t *candidate = make_relation(4002);
    ASSERT_TRUE(candidate_left && candidate_right && candidate,
        "candidate relations allocated");
    ASSERT_TRUE(col_mat_cache_insert_pin(&cache, candidate_left,
        candidate_right, candidate, NULL) == ENOSPC,
        "all-pinned insertion reports ENOSPC");
    ASSERT_TRUE(candidate->nrows == 1,
        "failed insertion leaves candidate owned by caller");
    col_rel_destroy(candidate);
    col_rel_destroy(candidate_left);
    col_rel_destroy(candidate_right);

    size_t charged_bytes = cache.total_bytes;
    ASSERT_TRUE(charged_bytes > 0, "pinned entries remain charged");
    col_mat_cache_clear(&cache);
    ASSERT_TRUE(cache.count == COL_MAT_CACHE_MAX,
        "clear defers every pinned entry");
    ASSERT_TRUE(cache.total_bytes == charged_bytes,
        "clear preserves deferred-entry accounting");
    for (size_t i = 0; i < COL_MAT_CACHE_MAX; i++)
        ASSERT_TRUE(col_mat_cache_lookup(&cache, left[i], right[i]) == NULL,
            "deferred entries are hidden from lookup");

    for (size_t i = 0; i < COL_MAT_CACHE_MAX; i++)
        col_mat_cache_pin_release(&pins[i]);
    ASSERT_TRUE(cache.count == 0 && cache.total_bytes == 0,
        "all pinned entries release their charge exactly once");
    ASSERT_TRUE(cache.active_pins == 0,
        "all pinned entries release active-pin accounting");
    for (size_t i = 0; i < COL_MAT_CACHE_MAX; i++) {
        col_rel_destroy(left[i]);
        col_rel_destroy(right[i]);
    }
}

static void
test_copy_survives_cache_eviction(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left = make_relation(5000);
    col_rel_t *right = make_relation(5001);
    col_rel_t *result = make_relation(5002);
    ASSERT_TRUE(left && right && result, "copy relations allocated");
    col_mat_cache_pin_t insert_pin = { 0 };
    ASSERT_TRUE(col_mat_cache_insert_pin(&cache, left, right, result,
        &insert_pin) == 0,
        "copy cache insert succeeds");
    ASSERT_TRUE(cache.active_pins == 1,
        "insert pin increments active-pin accounting");
    col_mat_cache_pin_release(&insert_pin);
    ASSERT_TRUE(cache.active_pins == 0,
        "insert pin can be released before the copy operation");

    col_mat_cache_pin_t pin = { 0 };
    col_rel_t *cached = col_mat_cache_lookup_pin(&cache, left, right, &pin);
    ASSERT_TRUE(cached == result, "copy cache hit is pinned");
    col_rel_t *copy = NULL;
    ASSERT_TRUE(col_rel_deep_copy(cached, &copy, NULL) == 0 && copy,
        "cache hit can be copied while pinned");
    col_mat_cache_pin_release(&pin);
    ASSERT_TRUE(cache.active_pins == 0,
        "production-style copy releases its pin before clear");
    col_mat_cache_clear(&cache);
    ASSERT_TRUE(col_rel_get(copy, 0, 0) == 5002,
        "independent copy survives cache eviction");

    col_rel_destroy(copy);
    col_rel_destroy(left);
    col_rel_destroy(right);
}

static void
test_snapshot_invalidates_same_shape_and_poisoned_generations(void)
{
    tests_run++;
    col_mat_cache_t cache = { 0 };
    col_rel_t *left = make_relation(6000);
    col_rel_t *right = make_relation(6001);
    col_rel_t *result = make_relation(6002);
    ASSERT_TRUE(left && right && result, "snapshot relations allocated");
    ASSERT_TRUE(col_mat_cache_insert(&cache, left, right, result) == 0,
        "snapshot cache insert succeeds");
    ASSERT_TRUE(col_mat_cache_lookup(&cache, left, right) == result,
        "matching snapshot is a hit");

    /* Keep nrows and the first row shape unchanged while changing the view. */
    ASSERT_TRUE(col_rel_set(left, 0, 0, 6999) == 0,
        "same-shape mutation succeeds");
    ASSERT_TRUE(col_mat_cache_lookup(&cache, left, right) == NULL,
        "same-nrows mutation is a miss");

    /* A poisoned generation is never evidence of freshness. */
    left->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID;
    ASSERT_TRUE(col_mat_cache_lookup(&cache, left, right) == NULL,
        "invalid generation is always a miss");

    /* Mutating beyond the legacy 100-row hash prefix is still stale. */
    col_mat_cache_t long_cache = { 0 };
    col_rel_t *long_left = col_rel_new_auto("long-left", 1);
    col_rel_t *long_right = make_relation(7001);
    col_rel_t *long_result = make_relation(7002);
    ASSERT_TRUE(long_left && long_right && long_result,
        "long snapshot relations allocated");
    for (int64_t i = 0; i < 101; i++)
        ASSERT_TRUE(col_rel_append_row(long_left, &i) == 0,
            "long relation row append succeeds");
    ASSERT_TRUE(col_mat_cache_insert(&long_cache, long_left, long_right,
        long_result) == 0, "long snapshot cache insert succeeds");
    ASSERT_TRUE(col_mat_cache_lookup(&long_cache, long_left, long_right)
        == long_result, "long snapshot initially hits");
    ASSERT_TRUE(col_rel_set(long_left, 100, 0, 7999) == 0,
        "outside-prefix mutation succeeds");
    ASSERT_TRUE(col_mat_cache_lookup(&long_cache, long_left, long_right)
        == NULL, "outside-prefix mutation is a miss");
    col_mat_cache_release_pins(&long_cache);
    col_mat_cache_clear(&long_cache);
    ASSERT_TRUE(long_cache.count == 0, "long snapshot cache is released");
    col_rel_destroy(long_left);
    col_rel_destroy(long_right);

    col_mat_cache_release_pins(&cache);
    col_mat_cache_clear(&cache);
    ASSERT_TRUE(cache.count == 0, "snapshot cache is released");
    col_rel_destroy(left);
    col_rel_destroy(right);
}

static wl_plan_op_t
make_join_op(const char *const *left_keys, const char *const *right_keys,
    uint32_t key_count)
{
    wl_plan_op_t op;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "a";
    op.left_keys = left_keys;
    op.right_keys = right_keys;
    op.key_count = key_count;
    return op;
}

static void
test_join_signature_separates_operations(void)
{
    tests_run++;
    static const char *const col0[] = { "col0" };
    static const char *const col1[] = { "col1" };
    static const char *const ab[] = { "ab" };
    static const char *const c[] = { "c" };
    static const char *const a[] = { "a" };
    static const char *const bc[] = { "bc" };
    uint8_t filter_one[] = { 1, 2, 3 };
    uint8_t filter_two[] = { 1, 2, 4 };

    wl_plan_op_t join0 = make_join_op(col0, col0, 1);
    wl_plan_op_t join0_again = make_join_op(col0, col0, 1);
    wl_plan_op_t join1 = make_join_op(col1, col1, 1);
    wl_plan_op_t split_ab = make_join_op(ab, c, 1);
    wl_plan_op_t split_bc = make_join_op(a, bc, 1);
    wl_plan_op_t semijoin = join0;
    semijoin.op = WL_PLAN_OP_SEMIJOIN;
    wl_plan_op_t filtered_one = join0;
    filtered_one.right_filter_expr.data = filter_one;
    filtered_one.right_filter_expr.size = sizeof(filter_one);
    wl_plan_op_t filtered_two = join0;
    filtered_two.right_filter_expr.data = filter_two;
    filtered_two.right_filter_expr.size = sizeof(filter_two);

    uint64_t sig0 = col_mat_cache_join_signature(&join0, false);
    ASSERT_TRUE(sig0 != 0, "a join signature is nonzero");
    ASSERT_TRUE(sig0 == col_mat_cache_join_signature(&join0_again, false),
        "equal joins have equal signatures");
    ASSERT_TRUE(sig0 != col_mat_cache_join_signature(&join1, false),
        "joins on different columns have different signatures");
    ASSERT_TRUE(sig0 != col_mat_cache_join_signature(&join0, true),
        "the differential path has its own signature");
    ASSERT_TRUE(sig0 != col_mat_cache_join_signature(&semijoin, false),
        "the operation kind is part of the signature");
    ASSERT_TRUE(col_mat_cache_join_signature(&split_ab, false)
        != col_mat_cache_join_signature(&split_bc, false),
        "key names are separated in the signature");
    ASSERT_TRUE(sig0 != col_mat_cache_join_signature(&filtered_one, false)
        && col_mat_cache_join_signature(&filtered_one, false)
        != col_mat_cache_join_signature(&filtered_two, false),
        "right filter bytes are part of the signature");
}

static void
test_keyed_entries_match_only_their_signature(void)
{
    tests_run++;
    static const char *const col0[] = { "col0" };
    static const char *const col1[] = { "col1" };
    col_mat_cache_t cache = { 0 };
    col_rel_t *left = make_relation(1);
    col_rel_t *right = make_relation(2);
    col_rel_t *result = make_relation(3);
    col_rel_t *legacy_result = make_relation(4);
    ASSERT_TRUE(left && right && result && legacy_result,
        "relations allocated");
    wl_plan_op_t join0 = make_join_op(col0, col0, 1);
    wl_plan_op_t join1 = make_join_op(col1, col1, 1);
    uint64_t sig0 = col_mat_cache_join_signature(&join0, false);
    uint64_t sig1 = col_mat_cache_join_signature(&join1, false);

    ASSERT_TRUE(col_mat_cache_insert_keyed(&cache, left, right, sig0, result)
        == 0, "keyed insert succeeds");
    ASSERT_TRUE(cache.entries[0].op_signature == sig0,
        "the entry records its signature");
    col_mat_cache_pin_t pin = { 0 };
    ASSERT_TRUE(col_mat_cache_lookup_pin_keyed(&cache, left, right, sig1,
        &pin) == NULL && !pin.active,
        "a different join on the same inputs misses");
    ASSERT_TRUE(col_mat_cache_lookup_pin(&cache, left, right, &pin) == NULL,
        "an unkeyed lookup misses a keyed entry");
    ASSERT_TRUE(col_mat_cache_lookup(&cache, left, right) == NULL,
        "a legacy lookup misses a keyed entry");
    ASSERT_TRUE(col_mat_cache_lookup_pin_keyed(&cache, left, right, sig0,
        &pin) == result && pin.active,
        "the same join hits");
    col_mat_cache_pin_release(&pin);

    ASSERT_TRUE(col_mat_cache_insert(&cache, left, right, legacy_result) == 0,
        "legacy insert succeeds");
    ASSERT_TRUE(col_mat_cache_lookup(&cache, left, right) == legacy_result,
        "a legacy lookup hits a legacy entry");
    col_mat_cache_release_pins(&cache);
    col_mat_cache_remove_result(&cache, result);
    ASSERT_TRUE(col_mat_cache_lookup_pin_keyed(&cache, left, right, sig0,
        &pin) == NULL, "a keyed lookup misses a legacy entry");
    col_mat_cache_clear(&cache);
    ASSERT_TRUE(cache.count == 0, "clear empties the cache");
    col_rel_destroy(left);
    col_rel_destroy(right);
}

int
main(void)
{
    test_copied_pin_cannot_release_native_debt();
    test_pin_move_checks_and_adjacent_storage();
    test_pin_move_clear_and_compaction();
    test_pin_move_preserves_governor_and_ledger_peer();
    test_pin_output_normalization_order();
    test_rejects_nonexclusive_results();
    test_insert_pin_failure_zeros_stale_pin();
    test_lookup_clear_and_release();
    test_pin_survives_compaction_and_truncate();
    test_lru_preserves_pinned_entry();
    test_all_pinned_insert_retains_callers_ownership();
    test_copy_survives_cache_eviction();
    test_snapshot_invalidates_same_shape_and_poisoned_generations();
    test_join_signature_separates_operations();
    test_keyed_entries_match_only_their_signature();
    printf("materialization-cache lifetime: %d tests, %d failures\n",
        tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
