/*
 * test_host_insert_width.c - the .decl decides a relation's width (#1038)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A relation's column count used to be whatever its first producer happened
 * to supply.  col_session_insert() lazily set the schema from the caller's
 * ncols when r->ncols was still 0, and nothing at that moment compared it
 * against the `.decl` -- the plan carried only relation names.  Every insert
 * after the first was checked against that accidental width, so the gap was
 * exactly one call wide, and it is the call that matters.
 *
 * The defect has two faces and both are silent:
 *
 *   too narrow.  `.decl p(id: int64, lbl: pair/2 inline)` occupies three
 *   physical slots.  A host inserting at the *logical* width 2 established
 *   the relation at 2, and a rule destructuring it then read a third slot
 *   that was never written:
 *
 *       insert(p, {5, 55}, ncols=2)  ->  outr(5, 55, 0)
 *
 *   The 0 appears in no source data.  This is the shape #985 closed for the
 *   fact and `.input` paths; wirelog_easy_insert() takes ncols from the
 *   caller and kept it open.
 *
 *   too wide.  The same hole is not specific to compounds.  A relation
 *   declared with two columns accepted a first insert of three and silently
 *   dropped the third:
 *
 *       insert(q, {1, 2, 3}, ncols=3)  ->  o2(1, 2)
 *
 * So the assertions here are on *values*, not row counts or exit codes: in
 * both directions the wrong answer has the same cardinality as the right one
 * and a counting test passes on the defect.
 *
 * The positive controls are the point of the exercise.  Rejecting every
 * first insert would satisfy every negative case above, so each is paired
 * with a correctly-shaped insert that must still succeed and must still
 * produce the real values -- including one relation with no compound at all,
 * whose declared and physical widths already agreed and which must be
 * byte-identical to before.
 */

#include "wirelog/wirelog-easy.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)

#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

/* An inline compound: two declared columns, three physical slots. */
static const char *const SRC_INLINE =
    ".decl p(id: int64, lbl: pair/2 inline)\n"
    ".decl outr(id: int64, a: int64, b: int64)\n"
    ".output outr\n"
    "outr(i, a, b) :- p(i, a, b).\n";

/* No compound anywhere: declared width == physical width == 2. */
static const char *const SRC_PLAIN =
    ".decl q(a: int64, b: int64)\n"
    ".decl o2(a: int64, b: int64)\n"
    ".output o2\n"
    "o2(x, y) :- q(x, y).\n";

/* Collected snapshot rows, flattened. */
struct rows {
    int64_t v[64];
    uint32_t n;
    uint32_t ncols;
    uint32_t count;
};

static void
collect(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    struct rows *rs = (struct rows *)user_data;
    rs->ncols = ncols;
    rs->count++;
    for (uint32_t i = 0; i < ncols && rs->n < 64; i++)
        rs->v[rs->n++] = row[i];
}

/* ---------------------------------------------------------------------- */

static void
test_narrow_first_insert_is_rejected(void)
{
    TEST("a first insert at the logical width is rejected, not accepted");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(SRC_INLINE, &s) != WIRELOG_OK) {
        FAIL("could not open the program");
        return;
    }

    int64_t row[2] = { 5, 55 };
    wirelog_error_t err = wirelog_easy_insert(s, "p", row, 2);

    if (err == WIRELOG_OK) {
        /* Show what it fabricated, so a failure here is self-explaining. */
        struct rows rs = { { 0 }, 0, 0, 0 };
        (void)wirelog_easy_step(s);
        (void)wirelog_easy_snapshot(s, "outr", collect, &rs);
        wirelog_easy_close(s);
        if (rs.count > 0)
            printf("\n      (accepted; outr = %lld %lld %lld)",
                (long long)rs.v[0], (long long)rs.v[1],
                (long long)rs.v[2]);
        FAIL("ncols=2 was accepted for a relation occupying 3 slots");
        return;
    }

    wirelog_easy_close(s);
    PASS();
}

static void
test_narrow_first_insert_fabricates_nothing(void)
{
    TEST("a rejected insert leaves no fabricated row behind");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(SRC_INLINE, &s) != WIRELOG_OK) {
        FAIL("could not open the program");
        return;
    }

    int64_t row[2] = { 5, 55 };
    (void)wirelog_easy_insert(s, "p", row, 2);
    (void)wirelog_easy_step(s);

    struct rows rs = { { 0 }, 0, 0, 0 };
    (void)wirelog_easy_snapshot(s, "outr", collect, &rs);
    wirelog_easy_close(s);

    /* The count assertion is the weak half; the value assertion is why this
     * test exists.  Pre-fix outr held exactly one row, (5, 55, 0), so a
     * "no rows derived" check alone would not distinguish a rejected insert
     * from one that fabricated a slot. */
    if (rs.count != 0) {
        if (rs.n >= 3 && rs.v[2] == 0)
            FAIL("derived a row whose third slot was never written");
        else
            FAIL("derived a row from an insert that should have failed");
        return;
    }
    PASS();
}

static void
test_correct_physical_width_still_works(void)
{
    TEST("control: an insert at the physical width succeeds and is exact");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(SRC_INLINE, &s) != WIRELOG_OK) {
        FAIL("could not open the program");
        return;
    }

    int64_t row[3] = { 5, 55, 66 };
    if (wirelog_easy_insert(s, "p", row, 3) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("the correctly-shaped insert was rejected");
        return;
    }
    (void)wirelog_easy_step(s);

    struct rows rs = { { 0 }, 0, 0, 0 };
    (void)wirelog_easy_snapshot(s, "outr", collect, &rs);
    wirelog_easy_close(s);

    if (rs.count != 1) {
        FAIL("expected exactly one derived row");
        return;
    }
    if (rs.n < 3 || rs.v[0] != 5 || rs.v[1] != 55 || rs.v[2] != 66) {
        FAIL("the derived row does not carry the inserted values");
        return;
    }
    PASS();
}

static void
test_wide_first_insert_is_rejected(void)
{
    TEST("a first insert wider than the .decl is rejected, not truncated");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(SRC_PLAIN, &s) != WIRELOG_OK) {
        FAIL("could not open the program");
        return;
    }

    /* Three values into a two-column relation.  Pre-fix this was accepted
     * and the 3 was dropped without a word -- the same defect as the
     * inline case, in the other direction, and with no compound in sight. */
    int64_t row[3] = { 1, 2, 3 };
    wirelog_error_t err = wirelog_easy_insert(s, "q", row, 3);
    wirelog_easy_close(s);

    if (err == WIRELOG_OK) {
        FAIL("ncols=3 was accepted for a relation declared with 2 columns");
        return;
    }
    PASS();
}

static void
test_plain_relation_unchanged(void)
{
    TEST("control: a compound-free relation at its declared width is exact");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(SRC_PLAIN, &s) != WIRELOG_OK) {
        FAIL("could not open the program");
        return;
    }

    int64_t row[2] = { 1, 2 };
    if (wirelog_easy_insert(s, "q", row, 2) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("the correctly-shaped insert was rejected");
        return;
    }
    (void)wirelog_easy_step(s);

    struct rows rs = { { 0 }, 0, 0, 0 };
    (void)wirelog_easy_snapshot(s, "o2", collect, &rs);
    wirelog_easy_close(s);

    if (rs.count != 1 || rs.n < 2 || rs.v[0] != 1 || rs.v[1] != 2) {
        FAIL("the ordinary path changed");
        return;
    }
    PASS();
}

static void
test_established_width_still_enforced(void)
{
    TEST("control: a later mismatched insert is still rejected");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(SRC_INLINE, &s) != WIRELOG_OK) {
        FAIL("could not open the program");
        return;
    }

    int64_t good[3] = { 5, 55, 66 };
    if (wirelog_easy_insert(s, "p", good, 3) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("the correctly-shaped insert was rejected");
        return;
    }

    /* This was already rejected before the fix, by the r->ncols != num_cols
     * arm.  It is here so that a change which replaced that arm with the new
     * declared-width check, rather than adding to it, cannot pass. */
    int64_t bad[2] = { 7, 77 };
    wirelog_error_t err = wirelog_easy_insert(s, "p", bad, 2);
    wirelog_easy_close(s);

    if (err == WIRELOG_OK) {
        FAIL("a mismatched insert after the width was set must still fail");
        return;
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
noop_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)diff;
    (void)user_data;
}

/*
 * col_session_insert() reroutes to col_session_insert_incremental() as soon
 * as a delta callback is installed (issue #662), and that function carries
 * its own copy of the lazy-schema block.  Every case above takes the
 * non-incremental path, so a fix applied to only one of the two passes all
 * of them -- confirmed by mutation: deleting the check from the incremental
 * site alone left the other six cases green.  This case is what covers it.
 */
static void
test_narrow_first_insert_rejected_in_delta_mode(void)
{
    TEST("the incremental insert path enforces the width too");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(SRC_INLINE, &s) != WIRELOG_OK) {
        FAIL("could not open the program");
        return;
    }
    if (wirelog_easy_set_delta_cb(s, noop_delta, NULL) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("could not install the delta callback");
        return;
    }

    int64_t row[2] = { 5, 55 };
    wirelog_error_t err = wirelog_easy_insert(s, "p", row, 2);
    wirelog_easy_close(s);

    if (err == WIRELOG_OK) {
        FAIL("ncols=2 accepted on the incremental path");
        return;
    }
    PASS();
}

static void
test_correct_width_still_works_in_delta_mode(void)
{
    TEST("control: the incremental path still accepts the right width");

    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(SRC_INLINE, &s) != WIRELOG_OK) {
        FAIL("could not open the program");
        return;
    }
    if (wirelog_easy_set_delta_cb(s, noop_delta, NULL) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("could not install the delta callback");
        return;
    }

    int64_t row[3] = { 5, 55, 66 };
    wirelog_error_t err = wirelog_easy_insert(s, "p", row, 3);
    wirelog_easy_close(s);

    if (err != WIRELOG_OK) {
        FAIL("the correctly-shaped incremental insert was rejected");
        return;
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

int
main(void)
{
    printf("=== Host insert width follows the .decl (#1038) ===\n\n");

    test_narrow_first_insert_is_rejected();
    test_narrow_first_insert_fabricates_nothing();
    test_correct_physical_width_still_works();
    test_wide_first_insert_is_rejected();
    test_plain_relation_unchanged();
    test_established_width_still_enforced();
    test_narrow_first_insert_rejected_in_delta_mode();
    test_correct_width_still_works_in_delta_mode();

    printf("\n=== Results: %d run, %d passed, %d failed ===\n",
        tests_run, tests_passed, tests_failed);

    return tests_failed == 0 ? 0 : 1;
}
