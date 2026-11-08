/*
 * test_body_eq_binding.c - body equality bindings (Issue #2231)
 *
 * An `=` in a rule body with a lone variable on one side that no positive
 * atom binds, and only bound variables on the other side, binds that
 * variable to the expression.  Before this, the variable was read as the
 * first body column: `t(y) :- n(x), y = x + 1.` derived nothing and
 * `y = 2` kept only the rows whose first column was 2.
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

#define SRC_CAP 8192

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

#define N1 ".decl n(x: int64)\n"
#define N2 ".decl n(g: int64, x: int64)\n"
#define T1 ".decl t(v: int64)\n.decl want(v: int64)\n.decl hit(v: int64)\n" \
        "hit(v) :- t(v), want(v).\n"
#define T2 ".decl t(k: int64, v: int64)\n" \
        ".decl want(k: int64, v: int64)\n.decl hit(k: int64, v: int64)\n" \
        "hit(k, v) :- t(k, v), want(k, v).\n"
#define TS ".decl t(v: symbol)\n.decl want(v: symbol)\n" \
        ".decl hit(v: symbol)\nhit(v) :- t(v), want(v).\n"
#define TF ".decl t(v: float)\n.decl want(v: float)\n" \
        ".decl hit(v: float)\nhit(v) :- t(v), want(v).\n"
#define S1 ".decl s(a: symbol)\ns(\"ab\").\ns(\"cd\").\n"

static const struct {
    const char *name;
    const char *src;
    uint32_t rows;
} bind_cases[] = {
    { "y = x + 1",
      N1 "n(1).\nn(2).\n" T1 "want(2).\nwant(3).\n"
      "t(y) :- n(x), y = x + 1.\n", 2 },
    { "x + 1 = y",
      N1 "n(1).\nn(2).\n" T1 "want(2).\nwant(3).\n"
      "t(y) :- n(x), x + 1 = y.\n", 2 },
    { "a constant",
      N1 "n(1).\nn(3).\n" T1 "want(2).\nt(y) :- n(x), y = 2.\n", 1 },
    { "an alias",
      N1 "n(1).\nn(2).\n" T1 "want(1).\nwant(2).\nt(y) :- n(x), y = x.\n",
      2 },
    { "a chain",
      N1 "n(1).\nn(2).\n" T1 "want(4).\nwant(6).\n"
      "t(z) :- n(x), y = x + 1, z = y * 2.\n", 2 },
    { "a chain written backwards",
      N1 "n(1).\nn(2).\n" T1 "want(4).\nwant(6).\n"
      "t(z) :- n(x), z = y * 2, y = x + 1.\n", 2 },
    { "a bound variable in a comparison",
      N1 "n(1).\nn(2).\n" T1 "want(3).\n"
      "t(y) :- n(x), y = x + 1, y > 2.\n", 1 },
    { "a bound variable used only in a comparison",
      N1 "n(1).\nn(2).\n" T1 "want(2).\n"
      "t(x) :- n(x), y = x * 2, y > 2.\n", 1 },
    { "a bound variable used twice",
      N1 "n(1).\nn(2).\n" T2 "want(2, 2).\nwant(3, 3).\n"
      "t(y, y) :- n(x), y = x + 1.\n", 2 },
    { "an expression over two joined atoms",
      N1 "n(1).\nn(2).\n.decl e(x: int64, y: int64)\ne(1, 10).\n"
      "e(2, 20).\n" T1 "want(11).\nwant(22).\n"
      "t(z) :- n(x), e(x, y), z = x + y.\n", 2 },
    { "string functions",
      S1 T1 "want(3).\nt(k) :- s(a), k = strlen(b), b = cat(a, \"x\").\n",
      1 },
    { "a string head",
      S1 TS "want(\"abx\").\nwant(\"cdx\").\n"
      "t(b) :- s(a), b = cat(a, \"x\").\n", 2 },
    { "a bound variable in a string predicate",
      S1 TS "want(\"ab\").\n"
      "t(a) :- s(a), b = cat(a, \"x\"), contains(b, \"bx\").\n", 1 },
    { "a float expression",
      ".decl f(a: float)\nf(1.5).\n" TF "want(3.0).\n"
      "t(b) :- f(a), b = a * 2.0.\n", 1 },
    { "an integer constant into a float column",
      N1 "n(1).\n" TF "want(2.0).\nt(y) :- n(x), y = 2.\n", 1 },
    { "an aggregate operand",
      N2 "n(1, 1).\nn(1, 2).\nn(2, 5).\n" T2 "want(1, 30).\nwant(2, 50).\n"
      "t(g, sum(y)) :- n(g, x), y = x * 10.\n", 2 },
    { "an alias group-by key from column 0",
      N2 "n(1, 1).\nn(1, 2).\nn(2, 3).\n" T2 "want(1, 2).\nwant(2, 1).\n"
      "t(k, count(x)) :- n(g, x), k = g.\n", 2 },
    { "an alias group-by key from column 1",
      ".decl n(x: int64, g: int64)\nn(1, 1).\nn(2, 1).\nn(3, 2).\n" T2
      "want(1, 2).\nwant(2, 1).\nt(k, count(x)) :- n(x, g), k = g.\n", 2 },
    { "an expression group-by key",
      N2 "n(1, 1).\nn(1, 2).\nn(2, 3).\n" T2 "want(10, 2).\nwant(20, 1).\n"
      "t(k, count(x)) :- n(g, x), k = g * 10.\n", 2 },
    { "a variable spelled like a column",
      N1 "n(1).\nn(2).\n" T1 "want(2).\nwant(3).\n"
      "t(col1) :- n(x), col1 = x + 1.\n", 2 },
};

static void
test_bindings_compute_the_variable(void)
{
    TEST("Body equality: the variable takes the expression's value");
    for (size_t i = 0; i < sizeof(bind_cases) / sizeof(bind_cases[0]); i++) {
        for (uint32_t w = 1; w <= 4; w += 3) {
            struct counts c;
            uint32_t executed;
            if (run(bind_cases[i].src, w, &c, &executed) != 0) {
                printf(" [%s, W=%u]", bind_cases[i].name, w);
                FAIL("the program did not run");
            }
            if (c.t != bind_cases[i].rows || c.hit != bind_cases[i].rows) {
                printf(" [%s, W=%u: t %u, hit %u]", bind_cases[i].name, w,
                    c.t, c.hit);
                FAIL("wrong rows");
            }
        }
    }
    PASS();
}

#define GRAPH ".decl e(x: int64, y: int64)\n" T2 \
        "e(1, 2).\ne(2, 3).\ne(3, 1).\ne(3, 4).\n" \
        "want(1, 1).\nwant(1, 2).\nwant(1, 3).\nwant(1, 4).\n" \
        "want(2, 1).\nwant(2, 2).\nwant(2, 3).\nwant(2, 4).\n" \
        "want(3, 1).\nwant(3, 2).\nwant(3, 3).\nwant(3, 4).\n" \
        "t(x, y) :- e(x, y).\n"

static const struct {
    const char *name;
    const char *src;
    uint32_t rows;
    uint32_t workers[3];
} recursive_cases[] = {
    { "closure through a bound join column",
      GRAPH "t(x, z) :- t(x, y), e(y, w), z = w + 0.\n", 12, { 1, 4, 8 } },
    { "closure through a self-join",
      GRAPH "t(x, z) :- t(x, y), t(y, w), z = w + 0.\n", 12, { 1, 4, 8 } },
    { "recursive min over a bound distance",
      ".decl e(x: int64, y: int64, w: int64)\n.decl src(x: int64)\n"
      T2 "src(1).\n"
      "e(1, 2, 4).\ne(1, 3, 1).\ne(3, 2, 1).\ne(2, 4, 1).\ne(4, 5, 3).\n"
      "e(3, 5, 9).\ne(5, 6, 1).\ne(6, 7, 2).\ne(2, 7, 8).\n"
      "want(1, 0).\nwant(2, 2).\nwant(3, 1).\nwant(4, 3).\nwant(5, 6).\n"
      "want(6, 7).\nwant(7, 9).\n"
      "t(x, 0) :- src(x).\n"
      "t(x, min(d)) :- t(z, dz), e(z, x, w), d = dz + w.\n",
      7, { 1, 4, 8 } },
    { "recursive min over a bound group-by key",
      ".decl e(x: int64, y: int64)\n" T2
      "e(1, 2).\ne(2, 3).\ne(3, 4).\ne(4, 5).\ne(5, 1).\ne(2, 6).\n"
      "e(6, 7).\nwant(1, 1).\nwant(2, 1).\nwant(3, 1).\nwant(4, 1).\n"
      "want(5, 1).\nwant(6, 1).\nwant(7, 1).\n"
      "t(x, x) :- e(x, _).\nt(k, min(d)) :- t(x, d), e(x, y), k = y * 1.\n",
      7, { 1, 4, 8 } },
};

static void
test_recursive_bindings(void)
{
    TEST("Body equality: recursive rules, serial and on TDD workers");
    for (size_t i = 0;
        i < sizeof(recursive_cases) / sizeof(recursive_cases[0]); i++) {
        for (uint32_t k = 0; k < 3; k++) {
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
test_linear_recursion(void)
{
    TEST("Body equality: linear recursion counts up");
    static const char *src = N1 T1 "want(0).\nwant(1).\nwant(2).\n"
        "want(3).\nt(0) :- n(0).\nn(0).\n"
        "t(y) :- t(x), y = x + 1, x < 3.\n";
    for (uint32_t w = 1; w <= 4; w += 3) {
        struct counts c;
        uint32_t executed;
        if (run(src, w, &c, &executed) != 0)
            FAIL("the program did not run");
        if (c.t != 4 || c.hit != 4) {
            printf(" [W=%u: t %u, hit %u]", w, c.t, c.hit);
            FAIL("wrong rows");
        }
    }
    PASS();
}

#define BIG "9223372036854775807"

static void
test_failing_expressions(void)
{
    struct counts c;
    uint32_t executed;

    TEST("Body equality: an overflow in the head is ERANGE");
    int rc = run(N1 "n(" BIG ").\n" T1 "t(y) :- n(x), y = x + 1.\n", 1, &c,
            &executed);
    if (rc != ERANGE) {
        printf(" [rc %d]", rc);
        FAIL("expected ERANGE");
    }
    PASS();

    TEST("Body equality: an overflow in a comparison drops the row");
    rc = run(N1 "n(1).\nn(" BIG ").\n" T1 "want(1).\n"
            "t(x) :- n(x), y = x + 1, y > 0.\n", 1, &c, &executed);
    if (rc != 0 || c.t != 1 || c.hit != 1) {
        printf(" [rc %d, t %u, hit %u]", rc, c.t, c.hit);
        FAIL("expected t(1) only");
    }
    PASS();

    TEST("Body equality: a comparison drops the row before the head");
    rc = run(N1 "n(1).\nn(" BIG ").\n" T1 "want(2).\n"
            "t(y) :- n(x), y = x + 1, y != 0.\n", 1, &c, &executed);
    if (rc != 0 || c.t != 1 || c.hit != 1) {
        printf(" [rc %d, t %u, hit %u]", rc, c.t, c.hit);
        FAIL("expected t(2) only");
    }
    PASS();
}

/* `t(y<levels>) :- n(x), y1 = 0 + x, y2 = 0 + y1, ...`: once substituted,
 * the head nests `0 + ` @levels deep and holds levels + 1 values at once. */
static void
chain_source(char *buf, uint32_t levels)
{
    size_t len = (size_t)snprintf(buf, SRC_CAP, N1 "n(5).\n" T1
            "want(5).\nt(y%u) :- n(x), y1 = 0 + x", levels);
    for (uint32_t i = 2; i <= levels; i++)
        len += (size_t)snprintf(buf + len, SRC_CAP - len,
                ", y%u = 0 + y%u", i, i - 1);
    snprintf(buf + len, SRC_CAP - len, ".\n");
}

/* `t(y<n>) :- n(x), y1 = x + x, y2 = y1 + y1, ...` with @levels doublings
 * and then @bnots bindings `y<k> = bnot(y<k-1>)`: the doublings make
 * 2^(levels+1) - 1 nodes once substituted, and each bnot() adds one. */
static void
doubling_source(char *buf, uint32_t levels, uint32_t bnots)
{
    int64_t want = (int64_t)1 << levels;
    for (uint32_t i = 0; i < bnots; i++)
        want = ~want;
    size_t len = (size_t)snprintf(buf, SRC_CAP, N1 "n(1).\n" T1
            "want(%lld).\nt(y%u) :- n(x), y1 = x + x", (long long)want,
            levels + bnots);
    for (uint32_t i = 2; i <= levels; i++)
        len += (size_t)snprintf(buf + len, SRC_CAP - len,
                ", y%u = y%u + y%u", i, i - 1, i - 1);
    for (uint32_t i = levels + 1; i <= levels + bnots; i++)
        len += (size_t)snprintf(buf + len, SRC_CAP - len,
                ", y%u = bnot(y%u)", i, i - 1);
    snprintf(buf + len, SRC_CAP - len, ".\n");
}

static void
test_substitution_limits(void)
{
    static char src[SRC_CAP];
    struct counts c;
    uint32_t executed;
    char why[600];

    TEST("Body equality: 63 nested bindings fit the evaluator stack");
    chain_source(src, 63);
    if (run(src, 1, &c, &executed) != 0 || c.t != 1 || c.hit != 1)
        FAIL("expected t(5)");
    PASS();

    TEST("Body equality: 64 nested bindings fail to plan");
    chain_source(src, 64);
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        FAIL("parse failed");
    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) == 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("plan accepted");
    }
    const char *msg = wirelog_program_get_plan_error(prog);
    bool named = msg && strstr(msg, "'t'") && strstr(msg, "64");
    wirelog_program_free(prog);
    if (!named)
        FAIL("plan error does not name 't' and the limit");
    PASS();

    TEST("Body equality: a binding of 4096 nodes evaluates");
    doubling_source(src, 11, 1);
    if (run(src, 1, &c, &executed) != 0 || c.t != 1 || c.hit != 1)
        FAIL("expected t(-2049)");
    PASS();

    TEST("Body equality: 4097 nodes, 12 and 40 doublings are rejected");
    doubling_source(src, 11, 2);
    if (!parse_rejects(src, "the expression bound to 'y13' in rule 't'"
        " exceeds 4096 nodes", why, sizeof(why)))
        FAIL(why);
    doubling_source(src, 12, 0);
    if (!parse_rejects(src, "the expression bound to 'y12' in rule 't'"
        " exceeds 4096 nodes", why, sizeof(why)))
        FAIL(why);
    doubling_source(src, 40, 0);
    if (!parse_rejects(src, "the expression bound to 'y12' in rule 't'"
        " exceeds 4096 nodes", why, sizeof(why)))
        FAIL(why);
    PASS();
}

/* `t(<head>) :- n(x), y1 = x + x, ..., y10 = y9 + y9.`: y10 has 2047
 * nodes once substituted. */
static void
use_site_source(char *buf, const char *head)
{
    size_t len = (size_t)snprintf(buf, SRC_CAP, N1 "n(1).\n" T1
            "want(-2049).\nt(%s) :- n(x), y1 = x + x", head);
    for (uint32_t i = 2; i <= 10; i++)
        len += (size_t)snprintf(buf + len, SRC_CAP - len,
                ", y%u = y%u + y%u", i, i - 1, i - 1);
    snprintf(buf + len, SRC_CAP - len, ".\n");
}

static void
test_use_site_limit(void)
{
    static char src[SRC_CAP];
    struct counts c;
    uint32_t executed;
    char why[600];

    TEST("Body equality: a head argument of 4096 nodes evaluates");
    use_site_source(src, "bnot(y10 + y10)");
    if (run(src, 1, &c, &executed) != 0 || c.t != 1 || c.hit != 1)
        FAIL("expected t(-2049)");
    PASS();

    TEST("Body equality: a head argument of 4097 nodes is rejected");
    use_site_source(src, "y10 + y10 + 1");
    if (!parse_rejects(src, "the expressions that use bound variables in"
        " rule 't' exceed 4096 nodes together once they are substituted",
        why, sizeof(why)))
        FAIL(why);
    PASS();

    /* `y10 + y10 > 0` is 4097 nodes; each `y10 > 0` is 2049. */
    static const char *const body[] = {
        ", y10 + y10 > 0.\n", ", y10 > 0, y10 < 5000.\n"
    };
    TEST("Body equality: comparisons over 4096 nodes together rejected");
    for (size_t i = 0; i < sizeof(body) / sizeof(body[0]); i++) {
        use_site_source(src, "x");
        size_t len = strlen(src);
        snprintf(src + len - 2, SRC_CAP - len + 2, "%s", body[i]);
        if (!parse_rejects(src, "the expressions that use bound variables"
            " in rule 't' exceed 4096 nodes", why, sizeof(why)))
            FAIL(why);
    }
    PASS();
}

static void
test_rejections(void)
{
    static const struct {
        const char *src;
        const char *needle;
    } cases[] = {
        { N1 "n(1).\n.decl t(v: int64)\nt(y) :- n(x), y = z + 1.\n",
          "unsafe variable 'z' in equality in rule 't'" },
        { N1 "n(1).\n.decl t(v: int64)\nt(x) :- n(x), y = y + 1.\n",
          "unsafe variable 'y' in equality in rule 't'" },
        { N1 "n(1).\n.decl t(v: int64, u: int64)\n"
          "t(x, u) :- n(x), u = uuid4().\n",
          "variable 'u' is bound to an expression containing uuid4() in"
          " rule 't'" },
        { N1 "n(1).\n.decl t(v: int64)\nt(x) :- n(x), y = x + 1.\n",
          "variable 'y' is bound by an equality but never used in rule 't'" },
        { N1 "n(1).\n.decl t(v: int64)\nt(x) :- n(x), y = x.\n",
          "variable 'y' is bound by an equality but never used in rule 't'" },
        { N1 "n(1).\n.decl t(v: int64)\nt(x) :- n(x), x = y.\n",
          "variable 'y' is bound by an equality but never used in rule 't'" },
        { N1 "n(1).\n.decl t(v: int64)\nt(x) :- n(x), col1 = x + 1.\n",
          "variable 'col1' is bound by an equality but never used" },
        { N1 "n(1).\n.decl t(v: int64)\n"
          "t(x) :- n(x), y = x + 1, @call(\"test.pred\", y).\n",
          "variable 'y' is bound by a body equality in rule 't' and cannot"
          " be passed to scalar extension call 'test.pred' yet" },
        { N1 "n(1).\n.decl t(v: int64)\n"
          "t(@call(\"test.f\", y)) :- n(x), y = x + 1.\n",
          "cannot be passed to scalar extension call 'test.f' yet" },
        { N1 "n(1).\n.decl p(x: int64)\n.decl t(v: int64)\n"
          "t(x) :- n(x), y = x + 1, !p(y).\n",
          "variable 'y' is bound by a body equality in rule 't' and cannot"
          " key negated atom 'p' yet" },
    };

    TEST("Body equality: unsafe and unsupported bindings are rejected");
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

int
main(void)
{
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    printf("test_body_eq_binding (#2231)\n");
    test_bindings_compute_the_variable();
    test_recursive_bindings();
    test_linear_recursion();
    test_failing_expressions();
    test_substitution_limits();
    test_use_site_limit();
    test_rejections();
    printf("=== Results: %d passed, %d failed, %d total ===\n", tests_passed,
        tests_failed, tests_run);
    return tests_failed == 0 ? 0 : 1;
}
