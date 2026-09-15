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

int
main(void)
{
    test_rejects_nonexclusive_results();
    test_lookup_clear_and_release();
    test_pin_survives_compaction_and_truncate();
    test_lru_preserves_pinned_entry();
    test_all_pinned_insert_retains_callers_ownership();
    test_copy_survives_cache_eviction();
    test_snapshot_invalidates_same_shape_and_poisoned_generations();
    printf("materialization-cache lifetime: %d tests, %d failures\n",
        tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
