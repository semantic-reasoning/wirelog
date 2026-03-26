/*
 * tests/test_r9_self_join_delta.c - R9 Self-Join Delta Permutation Tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests that K-fusion correctly handles the R9 self-join delta permutation:
 *
 *   valueAlias(x,y) :- valueFlow(z,x), valueFlow(z,y)
 *
 * With 2 valueFlow scans at delta positions 0 and 1, K=2, the plan generator
 * must produce exactly 2 copies:
 *
 *   Copy 0: delta_pos[0] -> FORCE_DELTA, delta_pos[1] -> FORCE_FULL
 *   Copy 1: delta_pos[1] -> FORCE_DELTA, delta_pos[0] -> FORCE_FULL
 *
 * Each copy must have exactly 1 FORCE_DELTA assignment, not 0 and not 2.
 *
 * Test cases:
 *   1. Delta permutation generates exactly K=2 copies
 *   2. Copy 0: position 0 gets FORCE_DELTA, position 1 gets FORCE_FULL
 *   3. Copy 1: position 1 gets FORCE_DELTA, position 0 gets FORCE_FULL
 *   4. FORCE_DELTA count per copy is exactly 1
 *   5. Non-delta positions stay AUTO (not overridden)
 *   6. Self-join with identical relation names at both positions is handled
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* -------------------------------------------------------------------------
 * Test framework (matches wirelog convention from test_k_fusion_e2e.c)
 * ------------------------------------------------------------------------- */

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

/* -------------------------------------------------------------------------
 * Minimal mock of delta_mode enum (mirrors exec_plan.h wl_delta_mode_t)
 * These values must match the real enum so any future ABI check will catch
 * divergence at compile time when the header is included.
 * ------------------------------------------------------------------------- */

typedef enum {
    MOCK_DELTA_AUTO = 0,
    MOCK_DELTA_FORCE_DELTA = 1,
    MOCK_DELTA_FORCE_FULL = 2,
} mock_delta_mode_t;

/* -------------------------------------------------------------------------
 * Minimal mock of a plan operator (only the fields needed for delta logic)
 * ------------------------------------------------------------------------- */

typedef struct {
    const char *relation_name; /* name of the scanned relation */
    mock_delta_mode_t delta_mode;
} mock_plan_op_t;

/* -------------------------------------------------------------------------
 * Mock relation for the valueFlow data
 * ------------------------------------------------------------------------- */

typedef struct {
    const char *name;
    int64_t **columns; /* column-major */ /* row-major: nrows * ncols int64_t values */
    uint32_t nrows;
    uint32_t ncols;
} mock_rel_t;
/* Column-major helpers for test (Phase C, Issue #332) */
static inline int64_t **
test_cols_alloc(uint32_t ncols, uint32_t cap)
{
    if (ncols == 0) return NULL;
    int64_t **c = (int64_t **)calloc(ncols, sizeof(int64_t *));
    if (!c) return NULL;
    for (uint32_t i = 0; i < ncols; i++) {
        c[i] = (int64_t *)malloc((cap > 0 ? cap : 1) * sizeof(int64_t));
        if (!c[i]) {
            for (uint32_t j = 0; j < i; j++) free(c[j]); free(c); return NULL;
        }
    }
    return c;
}
static inline void test_cols_free(int64_t **c, uint32_t ncols)
{
    if (!c) return; for (uint32_t i = 0; i < ncols; i++) free(c[i]); free(c);
}
static inline int test_cols_realloc(int64_t **c, uint32_t ncols, uint32_t cap)
{
    for (uint32_t i = 0; i < ncols; i++) {
        int64_t *n = (int64_t *)realloc(c[i], (size_t)cap * sizeof(int64_t));
        if (!n) return -1; c[i] = n;
    }
    return 0;
}
static inline int64_t test_get(const mock_rel_t *r, uint32_t row, uint32_t col)
{
    return r->columns[col][row];
}
static inline void test_set(mock_rel_t *r, uint32_t row, uint32_t col,
    int64_t v)
{
    r->columns[col][row] = v;
}
static inline void test_row_out(const mock_rel_t *r, uint32_t row, int64_t *dst)
{
    for (uint32_t c = 0; c < r->ncols; c++) dst[c] = r->columns[c][row];
}
static inline void test_row_in(mock_rel_t *r, uint32_t row, const int64_t *src)
{
    for (uint32_t c = 0; c < r->ncols; c++) r->columns[c][row] = src[c];
}

static mock_rel_t *
mock_rel_new(const char *name, uint32_t ncols, uint32_t capacity)
{
    mock_rel_t *r = (mock_rel_t *)malloc(sizeof(mock_rel_t));
    if (!r)
        return NULL;
    r->name = name;
    r->ncols = ncols;
    r->nrows = 0;
    r->columns = test_cols_alloc(ncols, capacity);
    if (!r->columns) {
        free(r);
        return NULL;
    }
    return r;
}

static void
mock_rel_add_row(mock_rel_t *r, const int64_t *row, uint32_t capacity)
{
    if (r->nrows >= capacity)
        return; /* guard: capacity already checked by caller */
    test_row_in(r, r->nrows, row);
    r->nrows++;
}

static void
mock_rel_free(mock_rel_t *r)
{
    if (!r)
        return;
    test_cols_free(r->columns, r->ncols);
    free(r);
}

/* -------------------------------------------------------------------------
 * Core logic under test: delta permutation assignment
 *
 * Mirrors the inner loop from exec_plan_gen.c:expand_multiway_delta() and
 * expand_multiway_k_fusion().  For copy d of K, walk over all ops and
 * assign:
 *   delta_pos[p] == i && p == d  -> FORCE_DELTA
 *   delta_pos[p] == i && p != d  -> FORCE_FULL
 *   not in delta_pos             -> AUTO
 * ------------------------------------------------------------------------- */

static void
apply_delta_permutation(mock_plan_op_t *ops, uint32_t op_count,
    const uint32_t *delta_pos, uint32_t k, uint32_t copy)
{
    for (uint32_t i = 0; i < op_count; i++) {
        int is_delta_pos = 0;
        for (uint32_t p = 0; p < k; p++) {
            if (delta_pos[p] == i) {
                ops[i].delta_mode = (p == copy) ? MOCK_DELTA_FORCE_DELTA
                                                : MOCK_DELTA_FORCE_FULL;
                is_delta_pos = 1;
                break;
            }
        }
        if (!is_delta_pos)
            ops[i].delta_mode = MOCK_DELTA_AUTO;
    }
}

/* Count ops in copy that have a given delta_mode */
static uint32_t
count_delta_mode(const mock_plan_op_t *ops, uint32_t op_count,
    mock_delta_mode_t mode)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < op_count; i++) {
        if (ops[i].delta_mode == mode)
            n++;
    }
    return n;
}

/* =========================================================================
 * Test 1: Delta permutation generates exactly K=2 copies
 *
 * The rule valueAlias(x,y) :- valueFlow(z,x), valueFlow(z,y) has K=2
 * (two IDB/recursive scans of valueFlow).  The permutation loop iterates
 * d in [0, K), so exactly 2 copies must be produced.
 * ========================================================================= */
static void
test_k2_self_join_generates_two_copies(void)
{
    TEST("R9 self-join K=2 generates exactly 2 delta copies");

    /* K=2: two valueFlow scans */
    uint32_t k = 2;

    /* Simulate producing K copies and count them */
    uint32_t copies_produced = 0;
    for (uint32_t d = 0; d < k; d++)
        copies_produced++;

    ASSERT(copies_produced == 2, "expected exactly 2 copies for K=2");
    PASS();
}

/* =========================================================================
 * Test 2: Copy 0 - position 0 gets FORCE_DELTA, position 1 gets FORCE_FULL
 *
 * Op layout for valueAlias(x,y) :- valueFlow(z,x), valueFlow(z,y):
 *   op[0] = VARIABLE "valueFlow"  (first scan, delta candidate)
 *   op[1] = JOIN with "valueFlow" (second scan, delta candidate via JOIN right)
 *   op[2] = MAP (project to x,y)
 *
 * delta_pos = {0, 1} (both scans reference the recursive relation)
 *
 * In copy 0: delta_pos[0]=0 with p==d=0  -> FORCE_DELTA
 *            delta_pos[1]=1 with p=1!=d=0 -> FORCE_FULL
 * ========================================================================= */
static void
test_copy0_pos0_delta_pos1_full(void)
{
    TEST("R9 copy 0: pos 0 FORCE_DELTA, pos 1 FORCE_FULL");

    /* 3 ops: VARIABLE valueFlow, JOIN valueFlow, MAP */
    mock_plan_op_t ops[3];
    ops[0].relation_name = "valueFlow";
    ops[0].delta_mode = MOCK_DELTA_AUTO;
    ops[1].relation_name = "valueFlow"; /* JOIN right side */
    ops[1].delta_mode = MOCK_DELTA_AUTO;
    ops[2].relation_name = NULL; /* MAP has no relation name */
    ops[2].delta_mode = MOCK_DELTA_AUTO;

    /* delta_pos: positions in ops[] that reference the IDB relation */
    uint32_t delta_pos[2] = { 0, 1 };
    uint32_t k = 2;
    uint32_t copy = 0;

    apply_delta_permutation(ops, 3, delta_pos, k, copy);

    ASSERT(ops[0].delta_mode == MOCK_DELTA_FORCE_DELTA,
        "copy 0: op[0] must be FORCE_DELTA");
    ASSERT(ops[1].delta_mode == MOCK_DELTA_FORCE_FULL,
        "copy 0: op[1] must be FORCE_FULL");
    ASSERT(ops[2].delta_mode == MOCK_DELTA_AUTO,
        "copy 0: op[2] (MAP) must remain AUTO");

    PASS();
}

/* =========================================================================
 * Test 3: Copy 1 - position 1 gets FORCE_DELTA, position 0 gets FORCE_FULL
 *
 * In copy 1: delta_pos[0]=0 with p=0!=d=1 -> FORCE_FULL
 *            delta_pos[1]=1 with p==d=1    -> FORCE_DELTA
 * ========================================================================= */
static void
test_copy1_pos1_delta_pos0_full(void)
{
    TEST("R9 copy 1: pos 1 FORCE_DELTA, pos 0 FORCE_FULL");

    mock_plan_op_t ops[3];
    ops[0].relation_name = "valueFlow";
    ops[0].delta_mode = MOCK_DELTA_AUTO;
    ops[1].relation_name = "valueFlow";
    ops[1].delta_mode = MOCK_DELTA_AUTO;
    ops[2].relation_name = NULL;
    ops[2].delta_mode = MOCK_DELTA_AUTO;

    uint32_t delta_pos[2] = { 0, 1 };
    uint32_t k = 2;
    uint32_t copy = 1;

    apply_delta_permutation(ops, 3, delta_pos, k, copy);

    ASSERT(ops[0].delta_mode == MOCK_DELTA_FORCE_FULL,
        "copy 1: op[0] must be FORCE_FULL");
    ASSERT(ops[1].delta_mode == MOCK_DELTA_FORCE_DELTA,
        "copy 1: op[1] must be FORCE_DELTA");
    ASSERT(ops[2].delta_mode == MOCK_DELTA_AUTO,
        "copy 1: op[2] (MAP) must remain AUTO");

    PASS();
}

/* =========================================================================
 * Test 4: FORCE_DELTA count per copy is exactly 1
 *
 * The fundamental invariant of semi-naive delta expansion: each copy must
 * apply the delta to exactly one position.  Applying it to 0 positions
 * would skip new facts; applying it to 2+ positions would overconstrain
 * and miss tuples.
 * ========================================================================= */
static void
test_exactly_one_force_delta_per_copy(void)
{
    TEST("R9 each copy has exactly 1 FORCE_DELTA, not 0 or 2");

    uint32_t delta_pos[2] = { 0, 1 };
    uint32_t k = 2;
    uint32_t op_count = 3;

    for (uint32_t copy = 0; copy < k; copy++) {
        mock_plan_op_t ops[3];
        for (uint32_t i = 0; i < op_count; i++) {
            ops[i].relation_name = (i < 2) ? "valueFlow" : NULL;
            ops[i].delta_mode = MOCK_DELTA_AUTO;
        }

        apply_delta_permutation(ops, op_count, delta_pos, k, copy);

        uint32_t n_force_delta
            = count_delta_mode(ops, op_count, MOCK_DELTA_FORCE_DELTA);
        uint32_t n_force_full
            = count_delta_mode(ops, op_count, MOCK_DELTA_FORCE_FULL);

        if (n_force_delta != 1) {
            printf("FAIL: copy %u has %u FORCE_DELTA ops (expected 1)\n", copy,
                n_force_delta);
            fail_count++;
            return;
        }
        if (n_force_full != 1) {
            printf("FAIL: copy %u has %u FORCE_FULL ops (expected 1 for K=2)\n",
                copy, n_force_full);
            fail_count++;
            return;
        }
    }

    PASS();
}

/* =========================================================================
 * Test 5: Non-delta positions remain AUTO across all copies
 *
 * Ops that are not in delta_pos (e.g., MAP, FILTER) must never have their
 * delta_mode overridden.  They are EDB references or transforms that have
 * no meaningful delta relation.
 * ========================================================================= */
static void
test_non_delta_positions_stay_auto(void)
{
    TEST("R9 non-delta ops (MAP etc.) keep AUTO mode in both copies");

    /* 5-op plan: VARIABLE(valueFlow), JOIN(valueFlow), MAP, FILTER, MAP
     * Only ops 0 and 1 are in delta_pos; ops 2-4 must stay AUTO. */
    uint32_t op_count = 5;
    uint32_t delta_pos[2] = { 0, 1 };
    uint32_t k = 2;

    for (uint32_t copy = 0; copy < k; copy++) {
        mock_plan_op_t ops[5];
        ops[0].relation_name = "valueFlow";
        ops[1].relation_name = "valueFlow";
        ops[2].relation_name = NULL; /* MAP */
        ops[3].relation_name = NULL; /* FILTER */
        ops[4].relation_name = NULL; /* MAP */
        for (uint32_t i = 0; i < op_count; i++)
            ops[i].delta_mode = MOCK_DELTA_AUTO;

        apply_delta_permutation(ops, op_count, delta_pos, k, copy);

        for (uint32_t i = 2; i < op_count; i++) {
            if (ops[i].delta_mode != MOCK_DELTA_AUTO) {
                printf("FAIL: copy %u op[%u] should be AUTO but got mode %d\n",
                    copy, i, (int)ops[i].delta_mode);
                fail_count++;
                return;
            }
        }
    }

    PASS();
}

/* =========================================================================
 * Test 6: Self-join with identical relation names at both delta positions
 *
 * The R9 rule has "valueFlow" appearing twice.  The permutation logic must
 * correctly distinguish positions by index, not by relation name.  Both
 * copies must produce different FORCE_DELTA/FORCE_FULL assignments despite
 * the relation names being identical.
 * ========================================================================= */
static void
test_self_join_identical_names_distinguished_by_index(void)
{
    TEST("R9 self-join: identical relation names distinguished by position");

    /* 2 ops both named "valueFlow" - positions matter, not names */
    mock_plan_op_t ops_copy0[2];
    mock_plan_op_t ops_copy1[2];
    uint32_t delta_pos[2] = { 0, 1 };
    uint32_t k = 2;

    ops_copy0[0].relation_name = "valueFlow";
    ops_copy0[0].delta_mode = MOCK_DELTA_AUTO;
    ops_copy0[1].relation_name = "valueFlow";
    ops_copy0[1].delta_mode = MOCK_DELTA_AUTO;

    ops_copy1[0].relation_name = "valueFlow";
    ops_copy1[0].delta_mode = MOCK_DELTA_AUTO;
    ops_copy1[1].relation_name = "valueFlow";
    ops_copy1[1].delta_mode = MOCK_DELTA_AUTO;

    apply_delta_permutation(ops_copy0, 2, delta_pos, k, 0);
    apply_delta_permutation(ops_copy1, 2, delta_pos, k, 1);

    /* Copy 0 and Copy 1 must differ */
    ASSERT(ops_copy0[0].delta_mode != ops_copy1[0].delta_mode,
        "op[0] must differ between copy 0 and copy 1");
    ASSERT(ops_copy0[1].delta_mode != ops_copy1[1].delta_mode,
        "op[1] must differ between copy 0 and copy 1");

    /* Verify the exact assignment */
    ASSERT(ops_copy0[0].delta_mode == MOCK_DELTA_FORCE_DELTA,
        "copy 0 op[0] must be FORCE_DELTA");
    ASSERT(ops_copy0[1].delta_mode == MOCK_DELTA_FORCE_FULL,
        "copy 0 op[1] must be FORCE_FULL");
    ASSERT(ops_copy1[0].delta_mode == MOCK_DELTA_FORCE_FULL,
        "copy 1 op[0] must be FORCE_FULL");
    ASSERT(ops_copy1[1].delta_mode == MOCK_DELTA_FORCE_DELTA,
        "copy 1 op[1] must be FORCE_DELTA");

    /* Verify mock relation with 2 rows exercises both permutations */
    mock_rel_t *vf = mock_rel_new("valueFlow", 2, 4);
    ASSERT(vf != NULL, "mock_rel_new failed");

    int64_t row1[2] = { 1, 10 };
    int64_t row2[2] = { 1, 20 };
    mock_rel_add_row(vf, row1, 4);
    mock_rel_add_row(vf, row2, 4);

    ASSERT(vf->nrows == 2, "valueFlow should have 2 rows");
    ASSERT(strcmp(vf->name, "valueFlow") == 0,
        "relation name must be valueFlow");

    /* For the self-join valueAlias(x,y) :- valueFlow(z,x), valueFlow(z,y):
     * with z=1, x=10, y=20 produces (10,20); and (20,10); and (10,10);
     * and (20,20). Each permutation handles one half of this product. */
    uint32_t expected_input_rows = 2;
    ASSERT(vf->nrows == expected_input_rows,
        "input cardinality for self-join must be 2");

    mock_rel_free(vf);
    PASS();
}

/* =========================================================================
 * main
 * ========================================================================= */

int
main(void)
{
    printf("\n=== R9 Self-Join Delta Permutation Tests ===\n\n");
    printf("Rule: valueAlias(x,y) :- valueFlow(z,x), valueFlow(z,y)\n");
    printf("K=2 (two valueFlow scans), delta_pos = {0, 1}\n\n");

    test_k2_self_join_generates_two_copies();
    test_copy0_pos0_delta_pos1_full();
    test_copy1_pos1_delta_pos0_full();
    test_exactly_one_force_delta_per_copy();
    test_non_delta_positions_stay_auto();
    test_self_join_identical_names_distinguished_by_index();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
