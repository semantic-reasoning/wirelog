/*
 * test_range_restriction.c - rule safety (Issue #2231)
 *
 * Every variable in a rule head, in a comparison, in a string predicate or
 * in a scalar extension call must be bound by a positive body atom or by a
 * body equality, and a rule needs a positive body atom.  Before, an unbound
 * variable read the first body column: `m(y) :- n(x).` derived m(1), and
 * `z < 4` acted as `x < 4`; a rule with no positive body atom failed at
 * evaluation with "execution failed".
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
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

/* Evaluates @src with @workers workers.  Returns the snapshot's rc (-1 when
 * the program does not parse or plan) and fills @out and @executed, the
 * number of recursive strata that ran on TDD workers. */
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

/* 1 when @src fails to parse with an error containing @needle. */
static int
parse_rejects(const char *src, const char *needle, char *why, size_t cap)
{
    char errbuf[512];
    wirelog_error_t err = WIRELOG_OK;
    struct wirelog_program *prog = wl_ir_parse_string_err(src, &err, errbuf,
            sizeof(errbuf));
    if (prog) {
        wirelog_program_free(prog);
        snprintf(why, cap, "parsed");
        return 0;
    }
    if (err != WIRELOG_ERR_PARSE || !strstr(errbuf, needle)) {
        snprintf(why, cap, "error %d '%s'", (int)err, errbuf);
        return 0;
    }
    return 1;
}

#define N1 ".decl n(x: int64)\nn(1).\nn(5).\n"
#define N2 ".decl n(x: int64, y: int64)\nn(1, 2).\nn(5, 6).\n"
#define M1 ".decl m(x: int64)\n"
#define T1 ".decl t(v: int64)\n.decl want(v: int64)\n.decl hit(v: int64)\n" \
        "hit(v) :- t(v), want(v).\n"

static void
test_unbound_variables_rejected(void)
{
    static const struct {
        const char *src;
        const char *needle;
    } cases[] = {
        { N1 M1 "m(y) :- n(x).\n",
          "unsafe variable 'y' in the head of rule 'm'" },
        { N1 ".decl m(x: int64, y: int64)\nm(x, y + 1) :- n(x).\n",
          "unsafe variable 'y' in the head of rule 'm'" },
        { N2 ".decl m(x: int64, s: int64)\nm(x, sum(z)) :- n(x, y).\n",
          "unsafe variable 'z' in the head of rule 'm'" },
        { N1 M1 "m(col1) :- n(x).\n",
          "unsafe variable 'col1' in the head of rule 'm'" },
        { N1 M1 "m(x) :- n(x), z < 4.\n",
          "unsafe variable 'z' in a comparison in rule 'm'" },
        { N2 M1 "m(x) :- n(x, y), z != y + 1.\n",
          "unsafe variable 'z' in a comparison in rule 'm'" },
        { N1 M1 "m(x) :- n(x), x < z.\n",
          "unsafe variable 'z' in a comparison in rule 'm'" },
        { ".decl s(a: symbol)\ns(\"ab\").\n.decl m(a: symbol)\n"
          "m(a) :- s(a), contains(b, \"a\").\n",
          "unsafe variable 'b' in a string predicate in rule 'm'" },
        { N1 M1 "m(x) :- n(x), @call(\"test.pred\", x, z).\n",
          "unsafe variable 'z' in scalar extension call 'test.pred' in"
          " rule 'm'" },
        { N1 M1 "m(x) :- n(x), !n(z), z < 3.\n",
          "unsafe variable 'z' in a comparison in rule 'm'" },
    };

    TEST("Range restriction: unbound variables are rejected by name");
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char why[600];
        if (!parse_rejects(cases[i].src, cases[i].needle, why,
            sizeof(why))) {
            printf(" [case %zu]", i);
            FAIL(why);
        }
    }
    PASS();
}

static void
test_atomless_bodies_rejected(void)
{
    static const char *const rules[] = {
        "m(y) :- y = 2.\n",
        "m(2) :- 1 < 2.\n",
        "m(2) :- True.\n",
        "m(5) :- !n(3).\n",
        "m(x) :- !n(x).\n",
    };

    TEST("Range restriction: a rule needs a positive body atom");
    for (size_t i = 0; i < sizeof(rules) / sizeof(rules[0]); i++) {
        char src[512];
        char why[600];
        snprintf(src, sizeof(src), N1 M1 "%s", rules[i]);
        if (!parse_rejects(src, "rule for 'm' has no positive body atom",
            why, sizeof(why))) {
            printf(" [%s]", rules[i]);
            FAIL(why);
        }
    }
    PASS();
}

static const struct {
    const char *name;
    const char *src;
    uint32_t rows;
} kept[] = {
    { "a constant head", N1 T1 "want(7).\nt(7) :- n(x).\n", 1 },
    { "a True condition",
      N1 T1 "want(1).\nwant(5).\nt(x) :- n(x), True.\n", 2 },
    { "a nullary head", N1 ".decl ok()\n" T1 "want(3).\n"
      "ok() :- n(5).\nt(3) :- ok().\n", 1 },
    { "a nullary guard", N1 ".decl unit()\nunit().\n" T1 "want(9).\n"
      "t(9) :- unit(), !n(3).\n", 1 },
    { "a wildcard", N2 T1 "want(1).\nwant(5).\nt(x) :- n(x, _).\n", 2 },
    { "a negated atom with a bound variable",
      N1 ".decl b(x: int64)\nb(5).\n" T1 "want(1).\n"
      "t(x) :- n(x), !b(x).\n", 1 },
    { "a body equality",
      N1 T1 "want(2).\nwant(6).\nt(y) :- n(x), y = x + 1.\n", 2 },
    { "a compound pattern",
      ".decl src(i: int64, a: int64, b: int64)\nsrc(1, 3, 4).\n"
      ".decl e(i: int64, p: pair/2 inline)\n"
      "e(i, a, b) :- src(i, a, b).\n" T1
      "want(7).\nt(a + b) :- e(i, pair(a, b)).\n", 1 },
    { "a constant aggregate key",
      ".decl Edge(x: int32, y: int32, w: int32)\nEdge(1, 2, 5).\n"
      ".decl Dist(x: int32, d: int32)\n.output Dist\n"
      ".decl t(v: int64)\n.decl want(v: int64)\n.decl hit(v: int64)\n"
      "hit(v) :- t(v), want(v).\nwant(1).\n"
      "Dist(1, max(0)) :- Edge(1, y, w).\nt(x) :- Dist(x, 0).\n", 1 },
};

static void
test_safe_rules_kept(void)
{
    TEST("Range restriction: safe rules still derive their rows");
    for (size_t i = 0; i < sizeof(kept) / sizeof(kept[0]); i++) {
        struct counts c;
        uint32_t executed;
        if (run(kept[i].src, 1, &c, &executed) != 0) {
            printf(" [%s]", kept[i].name);
            FAIL("the program did not run");
        }
        if (c.t != kept[i].rows || c.hit != kept[i].rows) {
            printf(" [%s: t %u, hit %u]", kept[i].name, c.t, c.hit);
            FAIL("wrong rows");
        }
    }
    PASS();
}

static void
test_recursive_rule_on_workers(void)
{
    static const char *src
        = ".decl e(x: int64, y: int64)\n"
        ".decl t(k: int64, v: int64)\n.decl want(k: int64, v: int64)\n"
        ".decl hit(k: int64, v: int64)\nhit(k, v) :- t(k, v), want(k, v).\n"
        "e(1, 2).\ne(2, 3).\ne(3, 1).\ne(3, 4).\n"
        "want(1, 1).\nwant(1, 2).\nwant(1, 3).\nwant(1, 4).\n"
        "want(2, 1).\nwant(2, 2).\nwant(2, 3).\nwant(2, 4).\n"
        "want(3, 1).\nwant(3, 2).\nwant(3, 3).\nwant(3, 4).\n"
        "t(x, y) :- e(x, y).\nt(x, w + 0) :- t(x, y), e(y, w).\n";

    TEST("Range restriction: a safe recursive rule runs on TDD workers");
    for (uint32_t w = 1; w <= 4; w += 3) {
        struct counts c;
        uint32_t executed;
        if (run(src, w, &c, &executed) != 0)
            FAIL("the program did not run");
        if (c.t != 12 || c.hit != 12) {
            printf(" [W=%u: t %u, hit %u]", w, c.t, c.hit);
            FAIL("wrong rows");
        }
        if (w > 1 && executed == 0)
            FAIL("no recursive stratum ran on TDD workers");
    }
    PASS();
}

int
main(void)
{
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    printf("test_range_restriction (#2231)\n");
    test_unbound_variables_rejected();
    test_atomless_bodies_rejected();
    test_safe_rules_kept();
    test_recursive_rule_on_workers();
    printf("=== Results: %d passed, %d failed, %d total ===\n", tests_passed,
        tests_failed, tests_run);
    return tests_failed == 0 ? 0 : 1;
}
