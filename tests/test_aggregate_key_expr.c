/*
 * test_aggregate_key_expr.c - Issue #2231 regression.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A non-aggregate argument of an aggregate head that was not a body
 * variable -- a constant or an expression -- grouped the rows by the body
 * column at its head position and emitted that column's value:
 * `t(g * 10, count(x)) :- n(g, x).` with n(1, 1), n(1, 2) and n(2, 3)
 * derived t(1, 2) and t(2, 1) instead of t(10, 2) and t(20, 1).
 *
 * Each program below holds its expected rows as facts of `want` and
 * joins them with `t` in `hit`, so string and float keys are compared
 * inside one intern table and one lane encoding: with n expected rows,
 * `t` is right when both `t` and `hit` have n rows.  The recursive
 * programs also run with TDD workers and require that a recursive stratum
 * executed on them.
 */

#define _POSIX_C_SOURCE 200809L

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

#ifdef _WIN32
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, (value && *value) ? value : "1");
}

#  define setenv wl_test_setenv_
#endif

extern void
wl_columnar_session_get_tdd_decision_stats(wl_session_t *sess,
    uint32_t *out_recursive_strata, uint32_t *out_executed_strata,
    uint32_t *out_fallback_strata, uint32_t *out_snapshot_ineligible,
    uint32_t *out_no_exchange, uint32_t *out_unsafe_plan,
    uint32_t *out_adaptive_workers, const char **out_last_fallback_reason);

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

struct counts {
    uint32_t t;
    uint32_t hit;
};

static void
count_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)row;
    (void)ncols;
    struct counts *c = (struct counts *)user_data;
    if (strcmp(relation, "t") == 0)
        c->t++;
    else if (strcmp(relation, "hit") == 0)
        c->hit++;
}

/* Evaluates @src with @workers workers.  Returns 0 and fills @out, and
 * @executed with the recursive strata that ran on TDD workers. */
static int
run(const char *src, uint32_t workers, struct counts *out,
    uint32_t *executed)
{
    memset(out, 0, sizeof(*out));
    *executed = 0;
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
    int rc = wl_session_create(wl_backend_columnar(), plan, workers, &sess);
    if (rc == 0)
        rc = wl_session_load_facts(sess, prog);
    if (rc == 0)
        rc = wl_session_snapshot(sess, count_rows, out);
    if (rc == 0) {
        uint32_t recursive = 0, fallback = 0, ineligible = 0, no_exchange = 0;
        uint32_t unsafe = 0, adaptive = 0;
        const char *reason = NULL;
        wl_columnar_session_get_tdd_decision_stats(sess, &recursive,
            executed, &fallback, &ineligible, &no_exchange, &unsafe,
            &adaptive, &reason);
    }
    if (sess)
        wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

#define N_DECL ".decl n(g: int64, x: int64)\n"
#define T2_DECL ".decl t(k: int64, v: int64)\n" \
        ".decl want(k: int64, v: int64)\n.decl hit(k: int64, v: int64)\n" \
        "hit(k, v) :- t(k, v), want(k, v).\n"
#define T3_DECL ".decl t(a: int64, b: int64, c: int64)\n" \
        ".decl want(a: int64, b: int64, c: int64)\n" \
        ".decl hit(a: int64, b: int64, c: int64)\n" \
        "hit(a, b, c) :- t(a, b, c), want(a, b, c).\n"

static const struct {
    const char *name;
    const char *src;
    uint32_t rows;
} key_cases[] = {
    { "expression key",
      N_DECL "n(1, 1).\nn(1, 2).\nn(2, 3).\n" T2_DECL
      "want(10, 2).\nwant(20, 1).\nt(g * 10, count(x)) :- n(g, x).\n", 2 },
    { "expression key merging groups",
      N_DECL "n(1, 1).\nn(2, 3).\nn(3, 4).\nn(4, 7).\n" T2_DECL
      "want(0, 1).\nwant(1, 7).\nwant(2, 7).\n"
      "t(g / 2, sum(x)) :- n(g, x).\n", 3 },
    { "constant key",
      N_DECL "n(1, 1).\nn(1, 2).\nn(2, 3).\nn(3, 4).\n" T2_DECL
      "want(5, 4).\nt(5, count(x)) :- n(g, x).\n", 1 },
    { "variable and constant keys",
      N_DECL "n(1, 1).\nn(1, 2).\nn(2, 5).\n" T3_DECL
      "want(1, 7, 3).\nwant(2, 7, 5).\nt(g, 7, sum(x)) :- n(g, x).\n", 2 },
    { "aggregate before an expression key",
      N_DECL "n(1, 1).\nn(1, 2).\nn(2, 5).\n" T2_DECL
      "want(3, 10).\nwant(5, 20).\nt(sum(x), g * 10) :- n(g, x).\n", 2 },
    { "a wildcard column keeps its rows",
      ".decl m(g: int64, x: int64, y: int64)\n"
      "m(1, 1, 1).\nm(1, 1, 2).\nm(2, 3, 1).\n" T2_DECL
      "want(1, 2).\nwant(2, 1).\nt(g + 0, count(x)) :- m(g, x, _).\n", 2 },
    { "string key",
      ".decl s(g: symbol, x: int64)\ns(\"a\", 1).\ns(\"a\", 2).\n"
      "s(\"b\", 5).\n.decl t(k: symbol, v: int64)\n"
      ".decl want(k: symbol, v: int64)\n.decl hit(k: symbol, v: int64)\n"
      "hit(k, v) :- t(k, v), want(k, v).\n"
      "want(\"a!\", 2).\nwant(\"b!\", 5).\n"
      "t(cat(g, \"!\"), max(x)) :- s(g, x).\n", 2 },
    { "float key",
      ".decl f(g: float, x: int64)\nf(1.5, 1).\nf(1.5, 2).\nf(2.5, 3).\n"
      ".decl t(k: float, v: int64)\n.decl want(k: float, v: int64)\n"
      ".decl hit(k: float, v: int64)\nhit(k, v) :- t(k, v), want(k, v).\n"
      "want(3.0, 2).\nwant(5.0, 1).\nt(g * 2.0, count(x)) :- f(g, x).\n",
      2 },
    { "float operand behind a key of another type",
      ".decl n(g: int64, v: float, w: int64)\nn(1, 1.5, 7).\n"
      "n(1, 2.25, 8).\nn(2, 5.5, 9).\n.decl t(k: int64, s: float)\n"
      ".decl want(k: int64, s: float)\n.decl hit(k: int64, s: float)\n"
      "hit(k, s) :- t(k, s), want(k, s).\nwant(1, 3.75).\nwant(2, 5.5).\n"
      "t(g + 0, sum(v)) :- n(g, v, w).\n", 2 },
    { "symbol min behind an expression key",
      ".decl n(g: int64, v: symbol)\nn(1, \"mm\").\nn(1, \"aa\").\n"
      "n(2, \"zz\").\n.decl t(k: int64, v: symbol)\n"
      ".decl want(k: int64, v: symbol)\n.decl hit(k: int64, v: symbol)\n"
      "hit(k, v) :- t(k, v), want(k, v).\nwant(1, \"aa\").\n"
      "want(2, \"zz\").\nt(g + 0, min(v)) :- n(g, v).\n", 2 },
    { "string constant key",
      N_DECL "n(1, 1).\nn(2, 3).\n.decl t(k: symbol, v: int64)\n"
      ".decl want(k: symbol, v: int64)\n.decl hit(k: symbol, v: int64)\n"
      "hit(k, v) :- t(k, v), want(k, v).\nwant(\"k\", 2).\n"
      "t(\"k\", count(x)) :- n(g, x).\n", 1 },
    { "integer constant key in a float column",
      N_DECL "n(1, 1).\nn(2, 3).\n.decl t(k: float, v: int64)\n"
      ".decl want(k: float, v: int64)\n.decl hit(k: float, v: int64)\n"
      "hit(k, v) :- t(k, v), want(k, v).\nwant(1.0, 2).\n"
      "t(1, count(x)) :- n(g, x).\n", 1 },
};

static void
test_aggregate_keys_group_by_value(void)
{
    TEST("Aggregate keys: constants and expressions group by value");
    for (size_t i = 0; i < sizeof(key_cases) / sizeof(key_cases[0]); i++) {
        for (uint32_t w = 1; w <= 4; w += 3) {
            struct counts c;
            uint32_t executed;
            if (run(key_cases[i].src, w, &c, &executed) != 0) {
                printf(" [%s, W=%u]", key_cases[i].name, w);
                FAIL("the program did not run");
            }
            if (c.t != key_cases[i].rows || c.hit != key_cases[i].rows) {
                printf(" [%s, W=%u: t %u, hit %u]", key_cases[i].name, w,
                    c.t, c.hit);
                FAIL("wrong rows");
            }
        }
    }
    PASS();
}

static const struct {
    const char *name;
    const char *src;
    uint32_t rows;
    uint32_t workers[4];
} recursive_cases[] = {
    { "recursive min with an expression key",
      ".decl e(x: int64, y: int64, w: int64)\n.decl src(x: int64)\n"
      T2_DECL "src(1).\n"
      "e(1, 2, 4).\ne(1, 3, 1).\ne(3, 2, 1).\ne(2, 4, 1).\ne(4, 5, 3).\n"
      "e(3, 5, 9).\ne(5, 6, 1).\ne(6, 7, 2).\ne(2, 7, 8).\n"
      "want(1, 0).\nwant(2, 2).\nwant(3, 1).\nwant(4, 3).\nwant(5, 6).\n"
      "want(6, 7).\nwant(7, 9).\n"
      "t(x, 0) :- src(x).\nt(y + 0, min(d + w)) :- t(x, d), e(x, y, w).\n",
      7, { 1, 4, 0, 0 } },
    { "recursive min over a multiplied key",
      ".decl e(x: int64, y: int64)\n" T2_DECL
      "e(1, 2).\ne(2, 3).\ne(3, 4).\ne(4, 5).\ne(5, 1).\ne(2, 6).\n"
      "e(6, 7).\nwant(1, 1).\nwant(2, 1).\nwant(3, 1).\nwant(4, 1).\n"
      "want(5, 1).\nwant(6, 1).\nwant(7, 1).\n"
      "t(x, x) :- e(x, _).\nt(y * 1, min(d)) :- t(x, d), e(x, y).\n",
      7, { 4, 8, 0, 0 } },
    { "recursive max with a constant key",
      ".decl Edge(x: int32, y: int32, w: int32)\n"
      ".decl t(x: int32, d: int32)\n.decl want(x: int32, d: int32)\n"
      ".decl hit(x: int32, d: int32)\nhit(x, d) :- t(x, d), want(x, d).\n"
      "Edge(1, 2, 5).\nEdge(1, 3, 1).\nEdge(2, 3, 7).\n"
      "want(1, 0).\nwant(2, 5).\nwant(3, 12).\n"
      "t(1, max(0)) :- Edge(1, y, w).\n"
      "t(y, max(d + w)) :- t(x, d), Edge(x, y, w).\n",
      3, { 1, 4, 8, 16 } },
};

static void
test_recursive_aggregate_keys(void)
{
    TEST("Aggregate keys: recursive min/max, serial and on TDD workers");
    for (size_t i = 0;
        i < sizeof(recursive_cases) / sizeof(recursive_cases[0]); i++) {
        for (uint32_t k = 0; k < 4 && recursive_cases[i].workers[k]; k++) {
            uint32_t w = recursive_cases[i].workers[k];
            struct counts c;
            uint32_t executed;
            if (run(recursive_cases[i].src, w, &c, &executed) != 0) {
                printf(" [%s, W=%u]", recursive_cases[i].name, w);
                FAIL("the program did not run");
            }
            if (c.t != recursive_cases[i].rows
                || c.hit != recursive_cases[i].rows) {
                printf(" [%s, W=%u: t %u, hit %u]", recursive_cases[i].name,
                    w, c.t, c.hit);
                FAIL("wrong rows");
            }
            if (w > 1 && executed == 0) {
                printf(" [%s, W=%u]", recursive_cases[i].name, w);
                FAIL("no recursive stratum ran on TDD workers");
            }
        }
    }
    PASS();
}

static void
test_aggregate_key_rejections(void)
{
    static const struct {
        const char *src;
        const char *needle;
    } cases[] = {
        { N_DECL "n(1, 1).\n.decl t(k: int64, v: int64)\n"
          "t(@call(\"test.key\", g), count(x)) :- n(g, x).\n",
          "scalar extension call in a group-by key of rule 't'" },
        { N_DECL "n(1, 1).\n.decl t(k: int64, v: int64)\n"
          "t(2.5, count(x)) :- n(g, x).\n",
          "rule head for relation 't' puts a float constant in a column that"
          " is not float, column 0" },
        { N_DECL "n(1, 1).\n.decl t(k: int64, v: int64)\n"
          "t(\"s\", count(x)) :- n(g, x).\n",
          "constant in column 0 of 't', which is declared int64" },
        { N_DECL "n(1, 1).\n.decl t(k: symbol, v: int64)\n"
          "t(7, count(x)) :- n(g, x).\n",
          "constant in column 0 of 't', which is declared string/symbol" },
    };

    TEST("Aggregate keys: extension calls and mistyped constants rejected");
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char errbuf[512];
        wirelog_error_t err = WIRELOG_OK;
        struct wirelog_program *prog = wl_ir_parse_string_err(cases[i].src,
                &err, errbuf, sizeof(errbuf));
        if (prog) {
            wirelog_program_free(prog);
            FAIL(cases[i].src);
        }
        if (err != WIRELOG_ERR_PARSE || !strstr(errbuf, cases[i].needle)) {
            printf(" [%s]", errbuf);
            FAIL("wrong error");
        }
    }
    PASS();
}

int
main(void)
{
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    printf("test_aggregate_key_expr (#2231)\n");
    test_aggregate_keys_group_by_value();
    test_recursive_aggregate_keys();
    test_aggregate_key_rejections();
    printf("=== Results: %d passed, %d failed, %d total ===\n", tests_passed,
        tests_failed, tests_run);
    return tests_failed == 0 ? 0 : 1;
}
