/*
 * test_colname_vars.c - Issue #2231 regression.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Lowering refers to a column by position with a synthesised variable
 * named `col` followed by the column's index: the operand of an aggregate,
 * the filter for a constant in a body atom or a comparison pushed into one,
 * and the filter for a variable repeated in an atom.  Plan generation
 * resolves a variable by name before reading such a name as a position, so
 * a rule variable spelled the same way captured the reference: with
 * n(9, 1, 1) and n(3, 7, 3), `m(a) :- n(col1, a, a).` derived m(7) where
 * `m(a) :- n(z, a, a).` derives m(1).  Each case below pairs such a rule
 * with the rows the same rule derives once the variable is renamed.
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                      \
        do {                               \
            tests_run++;                    \
            printf("  [TEST] %-60s", name); \
            fflush(stdout);                 \
        } while (0)

#define PASS()             \
        do {                   \
            tests_passed++;    \
            printf(" PASS\n"); \
        } while (0)

#define FAIL(msg)                          \
        do {                                   \
            tests_failed++;                     \
            printf(" FAIL: %s\n", (msg));       \
            return;                             \
        } while (0)

#define MAX_ROWS 8
#define MAX_COLS 2

struct rows {
    const char *relation;
    int64_t row[MAX_ROWS][MAX_COLS];
    uint32_t ncols;
    uint32_t count;
    bool overflow;
};

static void
capture(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct rows *out = (struct rows *)user_data;
    if (strcmp(relation, out->relation) != 0)
        return;
    if (out->count >= MAX_ROWS || ncols > MAX_COLS) {
        out->overflow = true;
        return;
    }
    out->ncols = ncols;
    for (uint32_t c = 0; c < ncols; c++)
        out->row[out->count][c] = row[c];
    out->count++;
}

static int
cmp_row(const void *a, const void *b)
{
    const int64_t *x = (const int64_t *)a;
    const int64_t *y = (const int64_t *)b;
    for (uint32_t c = 0; c < MAX_COLS; c++)
        if (x[c] != y[c])
            return x[c] < y[c] ? -1 : 1;
    return 0;
}

/* Evaluates @src and collects the rows of @relation, sorted. */
static int
run(const char *src, const char *relation, struct rows *out)
{
    memset(out, 0, sizeof(*out));
    out->relation = relation;

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;
    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }
    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc == 0)
        rc = wl_session_load_facts(sess, prog);
    if (rc == 0)
        rc = wl_session_snapshot(sess, capture, out);
    if (sess)
        wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    if (rc == 0 && out->overflow)
        rc = -1;
    qsort(out->row, out->count, sizeof(out->row[0]), cmp_row);
    return rc;
}

static bool
same_rows(const struct rows *a, const struct rows *b)
{
    if (a->count != b->count || (a->count && a->ncols != b->ncols))
        return false;
    for (uint32_t r = 0; r < a->count; r++)
        for (uint32_t c = 0; c < a->ncols; c++)
            if (a->row[r][c] != b->row[r][c])
                return false;
    return true;
}

#define N3_DECL ".decl n(a: int64, b: int64, c: int64)\n"
#define M_DECL ".decl m(x: int64)\n"
#define T_DECL ".decl t(g: int64, v: int64)\n"
#define G_ROWS N3_DECL "n(100, 1, 5).\nn(200, 1, 7).\nn(300, 2, 9).\n"

static void
test_colname_vars_match_renamed(void)
{
    static const struct {
        const char *relation;
        const char *src;
        const char *renamed;
        uint32_t rows;
    } cases[] = {
        /* a repeated variable */
        { "m", N3_DECL "n(9, 1, 1).\nn(3, 7, 3).\n" M_DECL
          "m(a) :- n(col1, a, a).\n",
          N3_DECL "n(9, 1, 1).\nn(3, 7, 3).\n" M_DECL
          "m(a) :- n(z, a, a).\n", 1 },
        /* a constant in the atom, and a comparison pushed into it */
        { "m", N3_DECL "n(5, 1, 7).\nn(8, 2, 5).\n" M_DECL
          "m(x) :- n(col2, x, 5).\n",
          N3_DECL "n(5, 1, 7).\nn(8, 2, 5).\n" M_DECL
          "m(x) :- n(z, x, 5).\n", 1 },
        { "m", N3_DECL "n(5, 1, 7).\nn(8, 2, 5).\n" M_DECL
          "m(x) :- n(col2, x, c), c = 5.\n",
          N3_DECL "n(5, 1, 7).\nn(8, 2, 5).\n" M_DECL
          "m(x) :- n(z, x, c), c = 5.\n", 1 },
        /* aggregate operands */
        { "t", ".decl n(a: int64, b: int64)\nn(1, 10).\nn(2, 20).\n"
          ".decl t(s: int64)\nt(sum(x)) :- n(col1, x).\n",
          ".decl n(a: int64, b: int64)\nn(1, 10).\nn(2, 20).\n"
          ".decl t(s: int64)\nt(sum(x)) :- n(a, x).\n", 1 },
        { "t", G_ROWS T_DECL "t(g, max(x)) :- n(g, col2, x).\n",
          G_ROWS T_DECL "t(g, max(x)) :- n(g, z, x).\n", 3 },
        { "t", ".decl n(g: int64, x: int64)\nn(1, 5).\nn(1, 7).\nn(2, 9).\n"
          T_DECL "t(col1, sum(col0)) :- n(col1, col0).\n",
          ".decl n(g: int64, x: int64)\nn(1, 5).\nn(1, 7).\nn(2, 9).\n"
          T_DECL "t(g, sum(x)) :- n(g, x).\n", 2 },
        /* controls that were already right */
        { "t", G_ROWS T_DECL "t(g, max(x)) :- n(col0, g, x).\n",
          G_ROWS T_DECL "t(g, max(x)) :- n(z, g, x).\n", 2 },
        { "m", N3_DECL "n(9, 1, 1).\nn(3, 7, 3).\n" M_DECL
          "m(col1 + 1) :- n(col1, _, _).\n",
          N3_DECL "n(9, 1, 1).\nn(3, 7, 3).\n" M_DECL
          "m(z + 1) :- n(z, _, _).\n", 2 },
        { "t", ".decl n(a: int64, b: int64)\nn(1, 10).\nn(2, 20).\n"
          ".decl k(a: int64, b: int64)\nk(10, 7).\nk(20, 8).\n" T_DECL
          "t(col0, y) :- n(col0, col1), k(col1, y).\n",
          ".decl n(a: int64, b: int64)\nn(1, 10).\nn(2, 20).\n"
          ".decl k(a: int64, b: int64)\nk(10, 7).\nk(20, 8).\n" T_DECL
          "t(a, y) :- n(a, b), k(b, y).\n", 2 },
    };

    TEST("colN variables: rules derive what the renamed rule derives");
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        struct rows got, want;
        if (run(cases[i].renamed, cases[i].relation, &want) != 0
            || want.count != cases[i].rows) {
            printf(" [case %zu]", i);
            FAIL("the renamed control did not run as expected");
        }
        if (run(cases[i].src, cases[i].relation, &got) != 0) {
            printf(" [case %zu]", i);
            FAIL("the colN rule did not run");
        }
        if (!same_rows(&got, &want)) {
            printf(" [case %zu: %u row(s), first (%lld, %lld)]", i,
                got.count, got.count ? (long long)got.row[0][0] : 0,
                got.count && got.ncols > 1 ? (long long)got.row[0][1] : 0);
            FAIL(cases[i].src);
        }
    }
    PASS();
}

static void
test_colname_vars_named_in_errors(void)
{
    TEST("colN variables: errors name the variable as written");
    char errbuf[512];
    wirelog_error_t err = WIRELOG_OK;
    struct wirelog_program *prog = wl_ir_parse_string_err(
        ".decl n(a: int64)\nn(1).\n.decl k(a: int64)\n" M_DECL
        "m(x) :- n(x), !k(col1).\n", &err, errbuf, sizeof(errbuf));
    if (prog) {
        wirelog_program_free(prog);
        FAIL("an unsafe negated variable was accepted");
    }
    if (!strstr(errbuf, "unsafe variable 'col1' appears")) {
        printf(" [%s]", errbuf);
        FAIL("the message does not name 'col1'");
    }
    PASS();
}

int
main(void)
{
    printf("test_colname_vars (#2231)\n");
    test_colname_vars_match_renamed();
    test_colname_vars_named_in_errors();
    printf("=== Results: %d passed, %d failed, %d total ===\n", tests_passed,
        tests_failed, tests_run);
    return tests_failed == 0 ? 0 : 1;
}
