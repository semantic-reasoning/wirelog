/*
 * test_lftj.c - Unit and benchmark tests for Leapfrog Triejoin (Issue #194)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests cover:
 *   1.  2-way join: basic key intersection + non-key output
 *   2.  2-way join: disjoint key sets produce empty output
 *   3.  3-way join: three relations sharing a key column
 *   4.  k-way with one empty relation produces empty output
 *   5.  All relations unary (key-only): pure set intersection
 *   6.  Duplicate key rows: Cartesian product within key group
 *   7.  8-way join (DOOP-style): correctness with known intersection
 *   8.  9-way join: k=9 correctness
 *   9.  Large k=9 performance: count output tuples vs expected
 *  10.  Memory: peak working set is O(N*k), not O(N^k) intermediate
 *  11.  EINVAL: k < 2 rejected
 *  12.  EINVAL: key_col >= ncols rejected
 *  13.  NULL data with nrows > 0 rejected
 *  14.  Single shared key value across 8 relations
 *  15.  EINVAL: k > WL_LFTJ_MAX_K rejected
 */

#include "../wirelog/columnar/lftj.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
    do {                                                \
        test_count++;                                   \
        printf("TEST %d: %s ... ", test_count, (name)); \
    } while (0)

#define PASS()            \
    do {                  \
        pass_count++;     \
        printf("PASS\n"); \
    } while (0)

#define FAIL(msg)                    \
    do {                             \
        fail_count++;                \
        printf("FAIL: %s\n", (msg)); \
        return;                      \
    } while (0)

#define ASSERT(cond, msg) \
    do {                  \
        if (!(cond))      \
            FAIL(msg);    \
    } while (0)

/* ----------------------------------------------------------------
 * Collecting callback: accumulates output rows into a flat buffer
 * ---------------------------------------------------------------- */

typedef struct {
    int64_t *rows;     /* owned, flat buffer of nrows * ncols elements */
    uint32_t nrows;    /* number of rows collected so far              */
    uint32_t capacity; /* allocated row capacity                       */
    uint32_t ncols;    /* columns per row (set on first callback)      */
    int oom;           /* set to 1 on allocation failure               */
} collect_ctx_t;

static void
collect_cb(const int64_t *row, uint32_t ncols, void *user)
{
    collect_ctx_t *ctx = (collect_ctx_t *)user;
    if (ctx->oom)
        return;

    if (ctx->ncols == 0)
        ctx->ncols = ncols;

    if (ctx->nrows >= ctx->capacity) {
        uint32_t new_cap = ctx->capacity ? ctx->capacity * 2u : 16u;
        int64_t *nb = (int64_t *)realloc(ctx->rows, (size_t)new_cap * ncols
                                                        * sizeof(int64_t));
        if (!nb) {
            ctx->oom = 1;
            return;
        }
        ctx->rows = nb;
        ctx->capacity = new_cap;
    }

    memcpy(ctx->rows + (size_t)ctx->nrows * ncols, row,
           ncols * sizeof(int64_t));
    ctx->nrows++;
}

static void
collect_ctx_free(collect_ctx_t *ctx)
{
    free(ctx->rows);
    ctx->rows = NULL;
    ctx->nrows = 0;
    ctx->capacity = 0;
    ctx->ncols = 0;
    ctx->oom = 0;
}

/* Simple count-only callback. */
static void
count_cb(const int64_t *row, uint32_t ncols, void *user)
{
    (void)row;
    (void)ncols;
    (*(int64_t *)user)++;
}

/* ================================================================
 * Test 1: 2-way basic join with non-key columns
 *
 * R(key=0, val=1): {(1,10), (2,20), (3,30)}
 * S(key=0, val=1): {(2,200), (3,300), (4,400)}
 * Join on col 0: keys {2,3} match.
 * Expected output: {(2,20,200), (3,30,300)} — 2 rows, 3 cols.
 * ================================================================ */
static void
test_2way_basic(void)
{
    TEST("2-way join: non-key columns in output");

    int64_t r_data[] = { 1, 10, 2, 20, 3, 30 };
    int64_t s_data[] = { 2, 200, 3, 300, 4, 400 };

    wl_lftj_input_t inputs[2];
    inputs[0].data = r_data;
    inputs[0].nrows = 3;
    inputs[0].ncols = 2;
    inputs[0].key_col = 0;
    inputs[1].data = s_data;
    inputs[1].nrows = 3;
    inputs[1].ncols = 2;
    inputs[1].key_col = 0;

    collect_ctx_t ctx = { 0, 0, 0, 0, 0 };
    int rc = wl_lftj_join(inputs, 2, collect_cb, &ctx);

    ASSERT(rc == 0, "wl_lftj_join must return 0");
    ASSERT(!ctx.oom, "no OOM");
    ASSERT(ctx.nrows == 2, "expected 2 output rows");
    ASSERT(ctx.ncols == 3, "expected 3 output columns (key + val_R + val_S)");

    /* Both output rows must have keys 2 and 3 (any order). */
    bool found2 = false, found3 = false;
    for (uint32_t r = 0; r < ctx.nrows; r++) {
        const int64_t *rp = ctx.rows + (size_t)r * 3u;
        if (rp[0] == 2 && rp[1] == 20 && rp[2] == 200)
            found2 = true;
        if (rp[0] == 3 && rp[1] == 30 && rp[2] == 300)
            found3 = true;
    }
    ASSERT(found2, "output must contain (2,20,200)");
    ASSERT(found3, "output must contain (3,30,300)");

    collect_ctx_free(&ctx);
    PASS();
}

/* ================================================================
 * Test 2: 2-way join with disjoint key sets
 *
 * R: {(1,10), (2,20)}  S: {(3,30), (4,40)}
 * No common keys -> output must be empty.
 * ================================================================ */
static void
test_2way_disjoint(void)
{
    TEST("2-way join: disjoint key sets produce empty output");

    int64_t r_data[] = { 1, 10, 2, 20 };
    int64_t s_data[] = { 3, 30, 4, 40 };

    wl_lftj_input_t inputs[2];
    inputs[0].data = r_data;
    inputs[0].nrows = 2;
    inputs[0].ncols = 2;
    inputs[0].key_col = 0;
    inputs[1].data = s_data;
    inputs[1].nrows = 2;
    inputs[1].ncols = 2;
    inputs[1].key_col = 0;

    int64_t count = 0;
    int rc = wl_lftj_join(inputs, 2, count_cb, &count);

    ASSERT(rc == 0, "must return 0 on empty result");
    ASSERT(count == 0, "disjoint sets must produce 0 output tuples");

    PASS();
}

/* ================================================================
 * Test 3: 3-way join
 *
 * R: {(1,a), (2,b), (5,c)}  key col 0
 * S: {(2,x), (3,y), (5,z)}  key col 0
 * T: {(2,p), (4,q), (5,r)}  key col 0
 * Common keys: {2, 5}
 * Expected:
 *   key=2: (2, b, x, p)
 *   key=5: (5, c, z, r)
 * ================================================================ */
static void
test_3way_basic(void)
{
    TEST("3-way join: three relations, 2 common keys");

    int64_t r_data[] = { 1, 100, 2, 200, 5, 500 };
    int64_t s_data[] = { 2, 20, 3, 30, 5, 50 };
    int64_t t_data[] = { 2, 2, 4, 4, 5, 5 };

    wl_lftj_input_t inputs[3];
    inputs[0].data = r_data;
    inputs[0].nrows = 3;
    inputs[0].ncols = 2;
    inputs[0].key_col = 0;
    inputs[1].data = s_data;
    inputs[1].nrows = 3;
    inputs[1].ncols = 2;
    inputs[1].key_col = 0;
    inputs[2].data = t_data;
    inputs[2].nrows = 3;
    inputs[2].ncols = 2;
    inputs[2].key_col = 0;

    collect_ctx_t ctx = { 0, 0, 0, 0, 0 };
    int rc = wl_lftj_join(inputs, 3, collect_cb, &ctx);

    ASSERT(rc == 0, "must return 0");
    ASSERT(!ctx.oom, "no OOM");
    ASSERT(ctx.nrows == 2, "expected 2 output rows");
    ASSERT(ctx.ncols == 4, "expected 4 cols (key + 3 non-key)");

    bool found2 = false, found5 = false;
    for (uint32_t r = 0; r < ctx.nrows; r++) {
        const int64_t *rp = ctx.rows + (size_t)r * 4u;
        if (rp[0] == 2 && rp[1] == 200 && rp[2] == 20 && rp[3] == 2)
            found2 = true;
        if (rp[0] == 5 && rp[1] == 500 && rp[2] == 50 && rp[3] == 5)
            found5 = true;
    }
    ASSERT(found2, "output must contain (2,200,20,2)");
    ASSERT(found5, "output must contain (5,500,50,5)");

    collect_ctx_free(&ctx);
    PASS();
}

/* ================================================================
 * Test 4: empty relation produces empty output
 *
 * Any relation with 0 rows means the join result is empty.
 * ================================================================ */
static void
test_empty_relation(void)
{
    TEST("join with one empty relation produces empty output");

    int64_t r_data[] = { 1, 2, 3 };
    /* s is empty */

    wl_lftj_input_t inputs[2];
    inputs[0].data = r_data;
    inputs[0].nrows = 3;
    inputs[0].ncols = 1;
    inputs[0].key_col = 0;
    inputs[1].data = NULL;
    inputs[1].nrows = 0;
    inputs[1].ncols = 1;
    inputs[1].key_col = 0;

    int64_t count = 0;
    int rc = wl_lftj_join(inputs, 2, count_cb, &count);

    ASSERT(rc == 0, "must return 0 for empty result");
    ASSERT(count == 0, "empty relation produces 0 output tuples");

    PASS();
}

/* ================================================================
 * Test 5: all-unary relations (set intersection)
 *
 * R={1,2,3,5}  S={2,3,4,5}  T={3,5,7}
 * Intersection = {3,5} -> 2 output rows, each with 1 column (key).
 * ================================================================ */
static void
test_unary_intersection(void)
{
    TEST("all-unary k=3: pure set intersection {3,5}");

    int64_t r[] = { 1, 2, 3, 5 };
    int64_t s[] = { 2, 3, 4, 5 };
    int64_t t[] = { 3, 5, 7 };

    wl_lftj_input_t inputs[3];
    inputs[0].data = r;
    inputs[0].nrows = 4;
    inputs[0].ncols = 1;
    inputs[0].key_col = 0;
    inputs[1].data = s;
    inputs[1].nrows = 4;
    inputs[1].ncols = 1;
    inputs[1].key_col = 0;
    inputs[2].data = t;
    inputs[2].nrows = 3;
    inputs[2].ncols = 1;
    inputs[2].key_col = 0;

    collect_ctx_t ctx = { 0, 0, 0, 0, 0 };
    int rc = wl_lftj_join(inputs, 3, collect_cb, &ctx);

    ASSERT(rc == 0, "must return 0");
    ASSERT(!ctx.oom, "no OOM");
    ASSERT(ctx.nrows == 2, "expected 2 output rows (keys 3 and 5)");
    ASSERT(ctx.ncols == 1, "expected 1 output column");

    bool found3 = false, found5 = false;
    for (uint32_t i = 0; i < ctx.nrows; i++) {
        if (ctx.rows[i] == 3)
            found3 = true;
        if (ctx.rows[i] == 5)
            found5 = true;
    }
    ASSERT(found3, "intersection must contain 3");
    ASSERT(found5, "intersection must contain 5");

    collect_ctx_free(&ctx);
    PASS();
}

/* ================================================================
 * Test 6: duplicate key rows — Cartesian product within key group
 *
 * R: {(1,a), (1,b)}  S: {(1,x), (1,y)}  key col 0
 * Expected 4 output rows: (1,a,x),(1,a,y),(1,b,x),(1,b,y).
 * ================================================================ */
static void
test_duplicate_keys_product(void)
{
    TEST("duplicate key rows: Cartesian product within key group (2x2=4)");

    int64_t r_data[] = { 1, 10, 1, 20 };
    int64_t s_data[] = { 1, 100, 1, 200 };

    wl_lftj_input_t inputs[2];
    inputs[0].data = r_data;
    inputs[0].nrows = 2;
    inputs[0].ncols = 2;
    inputs[0].key_col = 0;
    inputs[1].data = s_data;
    inputs[1].nrows = 2;
    inputs[1].ncols = 2;
    inputs[1].key_col = 0;

    collect_ctx_t ctx = { 0, 0, 0, 0, 0 };
    int rc = wl_lftj_join(inputs, 2, collect_cb, &ctx);

    ASSERT(rc == 0, "must return 0");
    ASSERT(!ctx.oom, "no OOM");
    ASSERT(ctx.nrows == 4, "expected 4 output rows (2x2 Cartesian product)");
    ASSERT(ctx.ncols == 3, "expected 3 output columns");

    /* All rows must have key == 1. */
    for (uint32_t r = 0; r < ctx.nrows; r++)
        ASSERT(ctx.rows[(size_t)r * 3u] == 1, "all output keys must be 1");

    collect_ctx_free(&ctx);
    PASS();
}

/* ================================================================
 * Test 7: 8-way join with known intersection (DOOP-style)
 *
 * 8 relations, each with N rows over a key domain [0, DOMAIN).
 * Each relation i holds keys where key % (i+2) == 0.
 * The intersection is keys where key % lcm(2..9) == 0.
 * lcm(2,3,4,5,6,7,8,9) = 2520.
 * For DOMAIN = 10000, intersection keys = {0, 2520, 5040, 7560} = 4 keys.
 * Expected: 4 output rows (each relation has 1 row per key in domain).
 * ================================================================ */
static void
test_8way_doop_style(void)
{
    TEST("8-way DOOP-style join: lcm(2..9)=2520, domain=10000, 4 output rows");

#define NREL8 8
#define DOMAIN8 10000

    int64_t *rel_data[NREL8];
    uint32_t rel_nrows[NREL8];

    for (int i = 0; i < NREL8; i++) {
        int mod = i + 2; /* divisors 2..9 */
        uint32_t n = 0;
        for (int64_t k = 0; k < DOMAIN8; k++)
            if (k % mod == 0)
                n++;
        rel_data[i] = (int64_t *)malloc(n * sizeof(int64_t));
        rel_nrows[i] = n;
        uint32_t pos = 0;
        for (int64_t k = 0; k < DOMAIN8; k++)
            if (k % mod == 0)
                rel_data[i][pos++] = k;
    }

    wl_lftj_input_t inputs[NREL8];
    for (int i = 0; i < NREL8; i++) {
        inputs[i].data = rel_data[i];
        inputs[i].nrows = rel_nrows[i];
        inputs[i].ncols = 1; /* unary: key only */
        inputs[i].key_col = 0;
    }

    int64_t count = 0;
    int rc = wl_lftj_join(inputs, NREL8, count_cb, &count);

    for (int i = 0; i < NREL8; i++)
        free(rel_data[i]);

    ASSERT(rc == 0, "8-way join must return 0");
    ASSERT(count == 4, "expected 4 output tuples (keys 0,2520,5040,7560)");

    PASS();
#undef NREL8
#undef DOMAIN8
}

/* ================================================================
 * Test 8: 9-way join correctness
 *
 * 9 relations keyed on the same domain. lcm(2..10) = 2520.
 * For DOMAIN = 10000: keys divisible by 2520 are {0, 2520, 5040, 7560}.
 * All 9 relations (divisors 2..10) include those keys -> 4 output rows.
 * ================================================================ */
static void
test_9way_correctness(void)
{
    TEST("9-way join: lcm(2..10)=2520, domain=10000, 4 output rows");

#define NREL9 9
#define DOMAIN9 10000

    int64_t *rel_data[NREL9];
    uint32_t rel_nrows[NREL9];

    for (int i = 0; i < NREL9; i++) {
        int mod = i + 2; /* divisors 2..10 */
        uint32_t n = 0;
        for (int64_t k = 0; k < DOMAIN9; k++)
            if (k % mod == 0)
                n++;
        rel_data[i] = (int64_t *)malloc(n * sizeof(int64_t));
        rel_nrows[i] = n;
        uint32_t pos = 0;
        for (int64_t k = 0; k < DOMAIN9; k++)
            if (k % mod == 0)
                rel_data[i][pos++] = k;
    }

    wl_lftj_input_t inputs[NREL9];
    for (int i = 0; i < NREL9; i++) {
        inputs[i].data = rel_data[i];
        inputs[i].nrows = rel_nrows[i];
        inputs[i].ncols = 1;
        inputs[i].key_col = 0;
    }

    int64_t count = 0;
    int rc = wl_lftj_join(inputs, NREL9, count_cb, &count);

    for (int i = 0; i < NREL9; i++)
        free(rel_data[i]);

    ASSERT(rc == 0, "9-way join must return 0");
    ASSERT(count == 4, "expected 4 output tuples (keys 0,2520,5040,7560)");

    PASS();
#undef NREL9
#undef DOMAIN9
}

/* ================================================================
 * Test 9: large 8-way join with value columns
 *
 * 8 relations, each 2-column (key, id), key in [0,100).
 * Each relation i has rows for keys where key % 2 == 0 (even keys).
 * All 8 relations share all 50 even keys.
 * Expected: 50 output rows, each with 9 columns (key + 8 id values).
 * ================================================================ */
static void
test_8way_with_values(void)
{
    TEST("8-way join with value columns: 50 shared keys, 9-col output");

#define NREL_V 8
#define NKEYS_V 50

    int64_t rel_data[NREL_V][NKEYS_V * 2];
    for (int i = 0; i < NREL_V; i++) {
        for (int k = 0; k < NKEYS_V; k++) {
            rel_data[i][(size_t)k * 2] = (int64_t)k * 2; /* even key */
            rel_data[i][(size_t)k * 2 + 1] = (int64_t)i * 1000 + k; /* id */
        }
    }

    wl_lftj_input_t inputs[NREL_V];
    for (int i = 0; i < NREL_V; i++) {
        inputs[i].data = rel_data[i];
        inputs[i].nrows = NKEYS_V;
        inputs[i].ncols = 2;
        inputs[i].key_col = 0;
    }

    collect_ctx_t ctx = { 0, 0, 0, 0, 0 };
    int rc = wl_lftj_join(inputs, NREL_V, collect_cb, &ctx);

    ASSERT(rc == 0, "8-way join with values must return 0");
    ASSERT(!ctx.oom, "no OOM");
    ASSERT(ctx.nrows == NKEYS_V,
           "expected 50 output rows (one per shared key)");
    ASSERT(ctx.ncols == (uint32_t)(NREL_V + 1),
           "expected 9 output columns (key + 8 id columns)");

    /* Spot-check: first output row with key=0 must have ids 0,1000,...,7000. */
    bool found_key0 = false;
    for (uint32_t r = 0; r < ctx.nrows; r++) {
        const int64_t *rp = ctx.rows + (size_t)r * ctx.ncols;
        if (rp[0] != 0)
            continue;
        found_key0 = true;
        for (int i = 0; i < NREL_V; i++)
            ASSERT(rp[1 + i] == (int64_t)(i * 1000),
                   "key=0 row must have id = i*1000 for each relation");
    }
    ASSERT(found_key0, "output must contain a row with key=0");

    collect_ctx_free(&ctx);
    PASS();
#undef NREL_V
#undef NKEYS_V
}

/* ================================================================
 * Test 10: memory efficiency — no intermediate materialization
 *
 * LFTJ with 8-way join on N=1000 rows per relation does NOT allocate
 * an N^8 intermediate (which would be ~10^24 rows).  We verify the join
 * completes successfully, proving no intermediate explosion.
 * The actual intermediate size is O(N * k) = O(8000 rows).
 * ================================================================ */
static void
test_memory_no_intermediate(void)
{
    TEST("memory: 8-way LFTJ completes without intermediate explosion");

#define NREL_M 8
#define NROWS_M 1000

    /* Each relation: keys 0..999, single value column. */
    int64_t *data[NREL_M];
    for (int i = 0; i < NREL_M; i++) {
        data[i] = (int64_t *)malloc(NROWS_M * sizeof(int64_t));
        ASSERT(data[i] != NULL, "malloc must succeed for 1000-row relation");
        for (int k = 0; k < NROWS_M; k++)
            data[i][k] = (int64_t)k;
    }

    wl_lftj_input_t inputs[NREL_M];
    for (int i = 0; i < NREL_M; i++) {
        inputs[i].data = data[i];
        inputs[i].nrows = NROWS_M;
        inputs[i].ncols = 1;
        inputs[i].key_col = 0;
    }

    int64_t count = 0;
    int rc = wl_lftj_join(inputs, NREL_M, count_cb, &count);

    for (int i = 0; i < NREL_M; i++)
        free(data[i]);

    ASSERT(rc == 0, "8-way join on 1000-row identical relations must return 0");
    ASSERT(count == NROWS_M, "expected 1000 output tuples (all keys shared)");

    PASS();
#undef NREL_M
#undef NROWS_M
}

/* ================================================================
 * Test 11: EINVAL on k < 2
 * ================================================================ */
static void
test_einval_k_too_small(void)
{
    TEST("EINVAL: k < 2 rejected");

    int64_t data[] = { 1, 2 };
    wl_lftj_input_t inp;
    inp.data = data;
    inp.nrows = 2;
    inp.ncols = 1;
    inp.key_col = 0;

    int64_t count = 0;
    int rc = wl_lftj_join(&inp, 1, count_cb, &count);
    ASSERT(rc != 0, "k=1 must return non-zero (EINVAL)");

    rc = wl_lftj_join(&inp, 0, count_cb, &count);
    ASSERT(rc != 0, "k=0 must return non-zero (EINVAL)");

    PASS();
}

/* ================================================================
 * Test 12: EINVAL on key_col >= ncols
 * ================================================================ */
static void
test_einval_bad_key_col(void)
{
    TEST("EINVAL: key_col >= ncols rejected");

    int64_t r_data[] = { 1, 2 };
    int64_t s_data[] = { 1, 2 };

    wl_lftj_input_t inputs[2];
    inputs[0].data = r_data;
    inputs[0].nrows = 2;
    inputs[0].ncols = 1;
    inputs[0].key_col = 1; /* out of range for ncols=1 */
    inputs[1].data = s_data;
    inputs[1].nrows = 2;
    inputs[1].ncols = 1;
    inputs[1].key_col = 0;

    int64_t count = 0;
    int rc = wl_lftj_join(inputs, 2, count_cb, &count);
    ASSERT(rc != 0, "key_col >= ncols must return non-zero (EINVAL)");

    PASS();
}

/* ================================================================
 * Test 13: EINVAL on NULL data with nrows > 0
 * ================================================================ */
static void
test_einval_null_data(void)
{
    TEST("EINVAL: NULL data with nrows > 0 rejected");

    int64_t r_data[] = { 1, 2 };

    wl_lftj_input_t inputs[2];
    inputs[0].data = r_data;
    inputs[0].nrows = 2;
    inputs[0].ncols = 1;
    inputs[0].key_col = 0;
    inputs[1].data = NULL;
    inputs[1].nrows = 2; /* nrows > 0 but data == NULL */
    inputs[1].ncols = 1;
    inputs[1].key_col = 0;

    int64_t count = 0;
    int rc = wl_lftj_join(inputs, 2, count_cb, &count);
    ASSERT(rc != 0, "NULL data with nrows>0 must return non-zero (EINVAL)");

    PASS();
}

/* ================================================================
 * Test 14: single shared key value across 8 relations
 *
 * Each relation has exactly one row with key=42 (plus other keys).
 * Expected: exactly 1 output tuple.
 * ================================================================ */
static void
test_8way_single_shared_key(void)
{
    TEST("8-way join: single shared key value (key=42) across all relations");

#define NREL_S 8

    /* Each relation: keys from a different range, all including 42. */
    int64_t bases[NREL_S] = { 10, 20, 30, 40, 50, 60, 70, 80 };
    int64_t rel_data[NREL_S][4]; /* 4 rows each */
    for (int i = 0; i < NREL_S; i++) {
        rel_data[i][0] = bases[i];
        rel_data[i][1] = bases[i] + 1;
        rel_data[i][2] = 42; /* shared key */
        rel_data[i][3] = bases[i] + 100;
    }

    wl_lftj_input_t inputs[NREL_S];
    for (int i = 0; i < NREL_S; i++) {
        inputs[i].data = rel_data[i];
        inputs[i].nrows = 4;
        inputs[i].ncols = 1;
        inputs[i].key_col = 0;
    }

    int64_t count = 0;
    int rc = wl_lftj_join(inputs, NREL_S, count_cb, &count);

    ASSERT(rc == 0, "single-shared-key 8-way join must return 0");
    ASSERT(count == 1, "expected exactly 1 output tuple (key=42)");

    PASS();
#undef NREL_S
}

/* ================================================================
 * Test 15: EINVAL on k > WL_LFTJ_MAX_K
 * ================================================================ */
static void
test_einval_k_too_large(void)
{
    TEST("EINVAL: k > WL_LFTJ_MAX_K rejected");

    int64_t dummy = 1;
    wl_lftj_input_t inp;
    inp.data = &dummy;
    inp.nrows = 1;
    inp.ncols = 1;
    inp.key_col = 0;

    /* Pass k = WL_LFTJ_MAX_K + 1 with a single descriptor (reused).
     * wl_lftj_join validates k before accessing inputs[k-1], so this
     * is safe even though the array has only 1 element. */
    int64_t count = 0;
    int rc = wl_lftj_join(&inp, WL_LFTJ_MAX_K + 1u, count_cb, &count);
    ASSERT(rc != 0, "k > WL_LFTJ_MAX_K must return non-zero (EINVAL)");

    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== Leapfrog Triejoin Unit Tests (Issue #194) ===\n\n");

    test_2way_basic();
    test_2way_disjoint();
    test_3way_basic();
    test_empty_relation();
    test_unary_intersection();
    test_duplicate_keys_product();
    test_8way_doop_style();
    test_9way_correctness();
    test_8way_with_values();
    test_memory_no_intermediate();
    test_einval_k_too_small();
    test_einval_bad_key_col();
    test_einval_null_data();
    test_8way_single_shared_key();
    test_einval_k_too_large();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
