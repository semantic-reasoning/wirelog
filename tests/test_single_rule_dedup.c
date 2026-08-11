/*
 * test_single_rule_dedup.c - single-rule relations are sets (Issue #957)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A relation defined by two or more rules is translated through
 * WIRELOG_IR_UNION, whose arm ends its operator sequence with CONSOLIDATE, so
 * it is deduplicated.  A relation defined by a single rule has no UNION
 * wrapper and used to get no CONSOLIDATE, so a rule whose head projects away
 * a body variable derived one row per derivation rather than one per distinct
 * tuple.
 *
 * The duplicates were not confined to that relation's own row count.  Every
 * join reading it multiplied by them:
 *
 *     p(x) :- e(x, y).        3 rows for a 1-tuple relation
 *     q(x) :- p(x), p(x).     9
 *     t(x) :- q(x), p(x).    27
 *
 * so the error compounded with join depth.  Datalog is set-valued, which
 * makes the single-rule path the unsound one; these tests pin the two paths
 * agreeing.
 *
 * Cardinality is the assertion here rather than tuple contents, which is
 * unusual for this suite and is the right way round for once: the values were
 * never wrong, only their multiplicity, so a contents-only check passes on
 * the defect.  The counts are exact, not bounds.
 */

#include "../wirelog/cli/driver.h"

#include "test_tmpdir.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
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

/* Count lines beginning with "<name>(" in the captured output. */
static int
count_rows(const char *text, const char *name)
{
    char needle[128];
    snprintf(needle, sizeof(needle), "%s(", name);

    int n = 0;
    const char *p = text;
    while ((p = strstr(p, needle)) != NULL) {
        if (p == text || p[-1] == '\n')
            n++;
        p += strlen(needle);
    }
    return n;
}

static char *
run_capture(const char *src, const char *outpath)
{
    FILE *f = fopen(outpath, "w");
    if (!f)
        return NULL;
    (void)wl_run_pipeline(src, 1, false, false, 0, f);
    fclose(f);
    return wl_read_file(outpath);
}

/* ---------------------------------------------------------------------- */

static void
test_projection_is_a_set(void)
{
    TEST("a single-rule projection derives one row per distinct tuple");

    char path[512];
    test_tmppath(path, sizeof(path), "wl_957_proj.txt");

    /*
     * Three e rows share x=1, so p's body succeeds three times and its head
     * projects y away.  p is one tuple.  r joins p against a two-row s, so r
     * is two tuples -- pre-fix it was six, p's three copies times s.
     */
    static const char *const src =
        ".decl e(a: int64, b: int64)\n"
        ".decl s(z: int64)\n"
        ".decl p(x: int64)\n"
        ".decl r(x: int64, z: int64)\n"
        ".output p\n"
        ".output r\n"
        "e(1,10).  e(1,20).  e(1,30).\n"
        "s(7).  s(8).\n"
        "p(x)    :- e(x, y).\n"
        "r(x, z) :- p(x), s(z).\n";

    char *text = run_capture(src, path);
    if (!text) {
        remove(path);
        FAIL("pipeline produced no output");
        return;
    }

    int np = count_rows(text, "p");
    int nr = count_rows(text, "r");
    free(text);
    remove(path);

    if (np != 1) {
        FAIL("p must be one tuple, not one row per derivation");
        return;
    }
    if (nr != 2) {
        FAIL("r must be two tuples; p's duplicates multiplied into the join");
        return;
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_duplicates_do_not_compound(void)
{
    TEST("duplicates do not compound with join depth");

    char path[512];
    test_tmppath(path, sizeof(path), "wl_957_compound.txt");

    /* Pre-fix this chain read 3, 9, 27. */
    static const char *const src =
        ".decl e(a: int64, b: int64)\n"
        ".decl p(x: int64)\n"
        ".decl q(x: int64)\n"
        ".decl t(x: int64)\n"
        ".output p\n"
        ".output q\n"
        ".output t\n"
        "e(1,10).  e(1,20).  e(1,30).\n"
        "p(x) :- e(x, y).\n"
        "q(x) :- p(x), p(x).\n"
        "t(x) :- q(x), p(x).\n";

    char *text = run_capture(src, path);
    if (!text) {
        remove(path);
        FAIL("pipeline produced no output");
        return;
    }

    int np = count_rows(text, "p");
    int nq = count_rows(text, "q");
    int nt = count_rows(text, "t");
    free(text);
    remove(path);

    if (np != 1 || nq != 1 || nt != 1) {
        FAIL("each relation must be one tuple at every depth");
        return;
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_multi_rule_path_unchanged(void)
{
    TEST("control: the multi-rule path still deduplicates");

    char path[512];
    test_tmppath(path, sizeof(path), "wl_957_multi.txt");

    /*
     * Two identical rules for one head go through WIRELOG_IR_UNION, which
     * already ended in CONSOLIDATE.  This was correct before the fix and is
     * the control that the fix did not disturb it -- and that the fix is not
     * simply "dedup everywhere twice".
     */
    static const char *const src =
        ".decl e(a: int64, b: int64)\n"
        ".decl m(x: int64)\n"
        ".output m\n"
        "e(1,10).  e(1,20).  e(1,30).\n"
        "m(x) :- e(x, y).\n"
        "m(x) :- e(x, y).\n";

    char *text = run_capture(src, path);
    if (!text) {
        remove(path);
        FAIL("pipeline produced no output");
        return;
    }

    int nm = count_rows(text, "m");
    free(text);
    remove(path);

    if (nm != 1) {
        FAIL("the multi-rule path must still be a set");
        return;
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_distinct_derivations_survive(void)
{
    TEST("control: genuinely distinct tuples are not collapsed");

    char path[512];
    test_tmppath(path, sizeof(path), "wl_957_distinct.txt");

    /*
     * The head keeps both variables, so all three derivations are distinct
     * tuples and must all survive.  Without this, a "fix" that collapsed a
     * relation to its first row would pass every assertion above.
     */
    static const char *const src =
        ".decl e(a: int64, b: int64)\n"
        ".decl keep(x: int64, y: int64)\n"
        ".output keep\n"
        "e(1,10).  e(1,20).  e(1,30).\n"
        "keep(x, y) :- e(x, y).\n";

    char *text = run_capture(src, path);
    if (!text) {
        remove(path);
        FAIL("pipeline produced no output");
        return;
    }

    int nk = count_rows(text, "keep");
    free(text);
    remove(path);

    if (nk != 3) {
        FAIL("three distinct tuples must all survive");
        return;
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

int
main(void)
{
    printf("=== Single-rule relations are sets (#957) ===\n\n");

    test_projection_is_a_set();
    test_duplicates_do_not_compound();
    test_multi_rule_path_unchanged();
    test_distinct_derivations_survive();

    printf("\n=== Results: %d run, %d passed, %d failed ===\n",
        tests_run, tests_passed, tests_failed);

    return tests_failed == 0 ? 0 : 1;
}
