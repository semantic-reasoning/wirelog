/*
 * test_simd_filter.c - SIMD Fast-Path Filter Tests (Issue #214)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests that the FILTER operator produces correct results for simple
 * comparison predicates via the real session API.  Validates the
 * scalar fast path and, on AVX2 builds, the column-native SIMD scan kernel.
 * There is no NEON body; ARM runs the scalar path.
 *
 * Test cases:
 *   1. Simple EQ filter: x == 1 keeps only matching rows
 *   2. Simple GT filter: x > 5 keeps only rows where x > 5
 *   3. Empty result: filter eliminates all rows
 *   4. All pass: filter keeps all rows (GTE against minimum)
 *   5. Large relation: 1000+ rows with EQ filter
 *   6. NEQ filter: x != 2 keeps all rows except x==2
 *   7. LTE filter: x <= 3 keeps correct rows
 *   8. LT filter: x < 3 keeps correct rows
 *   9. Sparse predicate spanning several scan tiles
 *  10. Dense predicate spanning several tiles (fused fallback)
 *
 * Cases 1-8 all fit inside a single scan tile.  Cases 9 and 10 exist because
 * col_op_filter() scans COL_FILTER_TILE rows at a time and switches to a
 * fused loop when the first tile keeps nearly everything; without them the
 * tile advance, the cross-tile output accumulation and the fallback would
 * have no regression coverage.
 */

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework (matches wirelog convention)
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
 * Tuple collector
 * ---------------------------------------------------------------- */

#define MAX_ROWS 2048
#define MAX_NCOLS 16

typedef struct {
    uint32_t count;
    int64_t rows[MAX_ROWS][MAX_NCOLS];
    uint32_t ncols[MAX_ROWS];
    int oom;
    /* per-relation tracking */
    const char *tracked_rel;
    uint32_t tracked_count;
} collect_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    collect_t *c = (collect_t *)user;
    if (c->tracked_rel && relation && strcmp(relation, c->tracked_rel) == 0) {
        c->tracked_count++;
        if (c->count < MAX_ROWS) {
            uint32_t idx = c->count++;
            c->ncols[idx] = ncols < MAX_NCOLS ? ncols : MAX_NCOLS;
            for (uint32_t i = 0; i < c->ncols[idx]; i++)
                c->rows[idx][i] = row[i];
        } else {
            c->oom = 1;
        }
    }
}

/* ----------------------------------------------------------------
 * Session helper
 * ---------------------------------------------------------------- */

/*
 * run_program_with_facts:
 * Parse src, apply all optimizer passes, create session, load facts,
 * collect tuples from tracked_rel, then tear down.
 *
 * Returns 0 on success.  out->tracked_count holds the tuple count for
 * tracked_rel; out->rows holds up to MAX_ROWS tuples.
 */
static int
run_program_with_facts(const char *src, const char *tracked_rel, collect_t *out)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        fprintf(stderr, "parse error: %d\n", (int)err);
        return -1;
    }

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        fprintf(stderr, "plan generation failed\n");
        return -1;
    }

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        fprintf(stderr, "session create failed\n");
        return -1;
    }

    if (wl_session_load_facts(sess, prog) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        fprintf(stderr, "load facts failed\n");
        return -1;
    }

    memset(out, 0, sizeof(*out));
    out->tracked_rel = tracked_rel;

    int rc = wl_session_snapshot(sess, collect_cb, out);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

/* ----------------------------------------------------------------
 * Source builders (shared by the row-count-driven cases)
 * ---------------------------------------------------------------- */

/*
 * append_fmt:
 * Append to @buf at @*pos, treating truncation as failure.
 *
 * snprintf returns the length it *would* have written, not the length it
 * wrote, so accumulating that return value directly lets @*pos run past
 * @bufsize once anything truncates -- and the next `bufsize - *pos` then
 * underflows into a huge size_t.  Returning false on truncation keeps @*pos
 * bounded by @bufsize at all times.
 */
static bool
append_fmt(char *buf, size_t bufsize, size_t *pos, const char *fmt, ...)
{
    va_list ap;

    if (*pos >= bufsize)
        return false;

    va_start(ap, fmt);
    int n = vsnprintf(buf + *pos, bufsize - *pos, fmt, ap);
    va_end(ap);

    if (n < 0 || (size_t)n >= bufsize - *pos)
        return false;

    *pos += (size_t)n;
    return true;
}

static char *
build_edge_facts(int nrows, const char *rule)
{
    size_t bufsize = (size_t)nrows * 32 + strlen(rule) + 256;
    char *src = (char *)malloc(bufsize);
    if (!src)
        return NULL;

    size_t pos = 0;
    bool ok = append_fmt(src, bufsize, &pos,
            ".decl edge(x: int32, y: int32)\n");

    for (int i = 0; ok && i < nrows; i++)
        ok = append_fmt(src, bufsize, &pos, "edge(%d, %d).\n", i, i * 2);

    if (ok)
        ok = append_fmt(src, bufsize, &pos, "%s", rule);

    if (!ok) {
        free(src);
        return NULL;
    }
    return src;
}

/* ================================================================
 * Test 1: Simple EQ filter -- x == 1 keeps only matching rows.
 *
 * edge(1,10), edge(1,20), edge(2,30), edge(3,40)
 * filtered(x,y) :- edge(x,y), x == 1.
 * Expected: filtered = {(1,10), (1,20)} -- 2 tuples
 * ================================================================ */
static void
test_eq_filter_basic(void)
{
    TEST("EQ filter x==1 keeps correct rows");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 10). edge(1, 20). edge(2, 30). edge(3, 40).\n"
        ".decl filtered(x: int32, y: int32)\n"
        "filtered(x, y) :- edge(x, y), x = 1.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 2, "expected 2 rows (x==1)");

    /* All returned rows must have x==1 */
    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] == 1, "row x value must be 1");

    PASS();
}

/* ================================================================
 * Test 2: Simple GT filter -- x > 5 keeps only rows where x > 5.
 *
 * edge(3,1), edge(5,2), edge(6,3), edge(7,4), edge(10,5)
 * filtered(x,y) :- edge(x,y), x > 5.
 * Expected: filtered = {(6,3),(7,4),(10,5)} -- 3 tuples
 * ================================================================ */
static void
test_gt_filter_basic(void)
{
    TEST("GT filter x>5 keeps correct rows");

    const char *src
        = ".decl edge(x: int32, y: int32)\n"
        "edge(3, 1). edge(5, 2). edge(6, 3). edge(7, 4). edge(10, 5).\n"
        ".decl filtered(x: int32, y: int32)\n"
        "filtered(x, y) :- edge(x, y), x > 5.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 3, "expected 3 rows (x>5)");

    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] > 5, "row x value must be > 5");

    PASS();
}

/* ================================================================
 * Test 3: Empty result -- filter eliminates all rows.
 *
 * edge(1,10), edge(2,20), edge(3,30)
 * filtered(x,y) :- edge(x,y), x == 99.
 * Expected: filtered = {} -- 0 tuples
 * ================================================================ */
static void
test_filter_empty_result(void)
{
    TEST("EQ filter with no matching rows returns empty result");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 10). edge(2, 20). edge(3, 30).\n"
        ".decl filtered(x: int32, y: int32)\n"
        "filtered(x, y) :- edge(x, y), x = 99.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 0, "expected 0 rows when no match");

    PASS();
}

/* ================================================================
 * Test 4: All pass -- GTE filter keeps all rows.
 *
 * edge(1,10), edge(2,20), edge(3,30)
 * filtered(x,y) :- edge(x,y), x >= 1.
 * Expected: filtered = all 3 tuples
 * ================================================================ */
static void
test_filter_all_pass(void)
{
    TEST("GTE filter x>=1 keeps all rows");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 10). edge(2, 20). edge(3, 30).\n"
        ".decl filtered(x: int32, y: int32)\n"
        "filtered(x, y) :- edge(x, y), x >= 1.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 3, "expected all 3 rows to pass");

    PASS();
}

/* ================================================================
 * Test 5: Large relation -- 1000+ rows with EQ filter.
 *
 * edge(i, i*2) for i in [0, 1023].
 * filtered(x,y) :- edge(x,y), x == 512.
 * Expected: filtered = {(512, 1024)} -- exactly 1 tuple
 * ================================================================ */
static void
test_filter_large_relation(void)
{
    TEST("EQ filter on 1024-row relation returns 1 tuple");

    /* Build source with 1024 inline facts.  Shares build_edge_facts with the
     * multi-tile cases, which bounds every write and treats truncation as
     * failure -- the hand-rolled loop this replaced accumulated snprintf
     * return values, which report the length that *would* have been written
     * and so can walk the cursor past the buffer once anything truncates. */
    const int NROWS = 1024;
    char *src = build_edge_facts(NROWS,
            ".decl filtered(x: int32, y: int32)\n"
            "filtered(x, y) :- edge(x, y), x = 512.\n");
    ASSERT(src != NULL, "source construction failed");

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    free(src);

    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 1, "expected exactly 1 row (x==512)");
    ASSERT(out.count >= 1, "collect buffer has row");
    ASSERT(out.rows[0][0] == 512, "x value must be 512");
    ASSERT(out.rows[0][1] == 1024, "y value must be 1024");

    PASS();
}

/* ================================================================
 * Test 6: NEQ filter -- x != 2 keeps all rows except x==2.
 *
 * edge(1,10), edge(2,20), edge(3,30), edge(4,40)
 * filtered(x,y) :- edge(x,y), x != 2.
 * Expected: 3 tuples
 * ================================================================ */
static void
test_neq_filter(void)
{
    TEST("NEQ filter x!=2 keeps all rows except x==2");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 10). edge(2, 20). edge(3, 30). edge(4, 40).\n"
        ".decl filtered(x: int32, y: int32)\n"
        "filtered(x, y) :- edge(x, y), x != 2.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 3, "expected 3 rows (x!=2)");

    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] != 2, "row x value must not be 2");

    PASS();
}

/* ================================================================
 * Test 7: LTE filter -- x <= 3 keeps rows where x <= 3.
 *
 * edge(1..5, ...)
 * filtered(x,y) :- edge(x,y), x <= 3.
 * Expected: 3 tuples
 * ================================================================ */
static void
test_lte_filter(void)
{
    TEST("LTE filter x<=3 keeps correct rows");

    const char *src
        = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 10). edge(2, 20). edge(3, 30). edge(4, 40). edge(5, 50).\n"
        ".decl filtered(x: int32, y: int32)\n"
        "filtered(x, y) :- edge(x, y), x <= 3.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 3, "expected 3 rows (x<=3)");

    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] <= 3, "row x value must be <= 3");

    PASS();
}

/* ================================================================
 * Test 8: LT filter -- x < 3 keeps rows where x < 3.
 * ================================================================ */
static void
test_lt_filter(void)
{
    TEST("LT filter x<3 keeps correct rows");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 10). edge(2, 20). edge(3, 30). edge(4, 40).\n"
        ".decl filtered(x: int32, y: int32)\n"
        "filtered(x, y) :- edge(x, y), x < 3.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 2, "expected 2 rows (x<3)");

    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] < 3, "row x value must be < 3");

    PASS();
}

/* ================================================================
 * Multi-tile coverage.
 *
 * col_op_filter() scans COL_FILTER_TILE rows at a time, and switches to a
 * fused loop when the first tile keeps more than 7/8 of its rows.  Every
 * other test here fits inside a single tile, so without these two the tile
 * advance, the cross-tile output accumulation and the dense fallback all
 * ship with no regression coverage.
 *
 * Both build a relation several tiles wide.  NROWS is derived from
 * COL_FILTER_TILE so the cases keep straddling tile boundaries if the tile
 * size is ever retuned, and is deliberately not a whole multiple of it so
 * the short final tile is exercised too.
 * ================================================================ */
#define MULTI_TILE_NROWS ((int)(COL_FILTER_TILE * 3u + 7u))

/* Sparse predicate across several tiles: exercises the tiled selection path
 * and output accumulation past the first tile. */
static void
test_filter_multi_tile_sparse(void)
{
    TEST("sparse filter spanning multiple scan tiles");

    const int NROWS = MULTI_TILE_NROWS;
    /*
     * Derived from the tile size, not hardcoded, so that retuning
     * COL_FILTER_TILE keeps every match beyond the first tile instead of
     * quietly letting them drift into it.
     *
     * The predicate is a simple comparison so this takes the fast path
     * rather than the bytecode interpreter -- a predicate the fast path
     * rejects (say `x % 512 = 0`) would silently test nothing here.  Matches
     * living past the first tile are what make the test sensitive to the
     * tile base offset: drop it and the reported rows come from tile 0.
     */
    const int THRESHOLD = (int)(COL_FILTER_TILE * 2u);
    char rule[128];
    snprintf(rule, sizeof(rule),
        ".decl filtered(x: int32, y: int32)\n"
        "filtered(x, y) :- edge(x, y), x > %d.\n", THRESHOLD);

    char *src = build_edge_facts(NROWS, rule);
    ASSERT(src != NULL, "malloc failed");

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    free(src);

    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == (uint32_t)(NROWS - 1 - THRESHOLD),
        "expected every row past the threshold, across tiles");

    for (uint32_t i = 0; i < out.count; i++) {
        ASSERT(out.rows[i][0] > THRESHOLD, "row must satisfy predicate");
        ASSERT(out.rows[i][1] == out.rows[i][0] * 2, "row must be intact");
    }

    PASS();
}

/* Dense predicate across several tiles: the first tile keeps everything, so
 * this drives the fallback out of the selection path. */
static void
test_filter_multi_tile_dense(void)
{
    TEST("dense filter spanning multiple tiles takes fused fallback");

    const int NROWS = MULTI_TILE_NROWS;
    /* Rejects exactly one row, far past the first tile, so the fallback is
     * chosen and must still filter the rest correctly. */
    char *src = build_edge_facts(NROWS,
            ".decl filtered(x: int32, y: int32)\n"
            "filtered(x, y) :- edge(x, y), x != 2500.\n");
    ASSERT(src != NULL, "malloc failed");

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    free(src);

    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == (uint32_t)(NROWS - 1),
        "expected every row but one");

    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] != 2500, "rejected row must be absent");

    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== SIMD Filter Fast-Path Tests (Issue #214) ===\n");

    test_eq_filter_basic();
    test_gt_filter_basic();
    test_filter_empty_result();
    test_filter_all_pass();
    test_filter_large_relation();
    test_neq_filter();
    test_lte_filter();
    test_lt_filter();
    test_filter_multi_tile_sparse();
    test_filter_multi_tile_dense();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf(" ---\n");

    return (fail_count > 0) ? 1 : 0;
}
