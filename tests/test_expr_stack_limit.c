/*
 * test_expr_stack_limit.c - expression operand stack limit (Issue #2231)
 *
 * The postfix evaluators hold at most 64 values at once.  Plan generation
 * rejects an expression whose evaluation would hold more, instead of letting
 * the evaluator drop a value and return a wrong result.  Each rejected case
 * is paired with the largest accepted one, which must still evaluate right.
 */

#define _GNU_SOURCE

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

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

#define MAX_ROWS 16
#define SRC_CAP  16384

struct result_ctx {
    int64_t vals[MAX_ROWS];
    uint32_t count;
};

static void
capture_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct result_ctx *ctx = (struct result_ctx *)user_data;
    if (strcmp(relation, "n") == 0 || ncols == 0 || ctx->count >= MAX_ROWS)
        return;
    ctx->vals[ctx->count++] = row[0];
}

/* Plan and run @src; 0 with the non-"n" rows in @out, -1 on any failure. */
static int
run_snapshot(const char *src, struct result_ctx *out)
{
    memset(out, 0, sizeof(*out));

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    int rc = wl_session_load_facts(sess, prog);
    if (rc == 0)
        rc = wl_session_snapshot(sess, capture_rows, out);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

/*
 * 1 when @src parses and planning fails with an error that names @rel and
 * the 64-value limit; 0 otherwise.  @why receives the reason for a 0.
 */
static int
plan_rejects(const char *src, const char *rel, char *why, size_t why_cap)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        snprintf(why, why_cap, "parse failed");
        return 0;
    }

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int ok = 0;
    if (wl_plan_from_program(prog, &plan) == 0) {
        snprintf(why, why_cap, "plan accepted");
        wl_plan_free(plan);
    } else {
        const char *msg = wirelog_program_get_plan_error(prog);
        if (msg && strstr(msg, rel) && strstr(msg, "64"))
            ok = 1;
        else
            snprintf(why, why_cap, "plan error '%s'", msg ? msg : "(null)");
    }
    wirelog_program_free(prog);
    return ok;
}

/* Append @s to @buf (capacity SRC_CAP) @times times. */
static void
append_n(char *buf, const char *s, int times)
{
    for (int i = 0; i < times; i++)
        strncat(buf, s, SRC_CAP - strlen(buf) - 1);
}

#define PRELUDE                \
        ".decl n(x: int64)\n"  \
        "n(5).\n"              \
        "n(7).\n"

/*
 * Build "<PRELUDE><decl><head><open^depth><leaf><close^depth><tail>".
 * With open "bor(0, " the leaf stays on the stack beside one held operand
 * per level, so the evaluation holds depth + 1 values.
 */
static void
build(char *buf, const char *decl, const char *head, const char *open,
    int depth, const char *leaf, const char *close, const char *tail)
{
    buf[0] = '\0';
    append_n(buf, PRELUDE, 1);
    append_n(buf, decl, 1);
    append_n(buf, head, 1);
    append_n(buf, open, depth);
    append_n(buf, leaf, 1);
    append_n(buf, close, depth);
    append_n(buf, tail, 1);
}

static int
has_vals(const struct result_ctx *out, const int64_t *want, uint32_t n)
{
    if (out->count != n)
        return 0;
    for (uint32_t i = 0; i < n; i++) {
        int found = 0;
        for (uint32_t j = 0; j < out->count; j++)
            if (out->vals[j] == want[i])
                found = 1;
        if (!found)
            return 0;
    }
    return 1;
}

typedef struct {
    const char *label;
    const char *decl;
    const char *head;
    const char *open;
    int depth;
    const char *leaf;
    const char *close;
    const char *tail;
    const char *rel;     /* relation named by the plan error */
    int64_t want[2];     /* rows of the accepted case */
    uint32_t nwant;
} stack_case_t;

/* Each line: the largest accepted depth, then one more is rejected. */
static const stack_case_t cases[] = {
    { "head, 63 calls nested in the last argument",
      ".decl stackheadrel(x: int64)\n", "stackheadrel(", "bor(0, ", 63,
      "x", ")", ") :- n(x).\n", "stackheadrel", { 5, 7 }, 2 },
    { "filter, nested call = 5",
      ".decl stackfilterrel(x: int64)\n", "stackfilterrel(x) :- n(x), ",
      "bor(0, ", 63, "x", ")", " = 5.\n", "stackfilterrel", { 5 }, 1 },
    { "filter, nested call != 5",
      ".decl stackneqrel(x: int64)\n", "stackneqrel(x) :- n(x), ",
      "bor(0, ", 63, "x", ")", " != 5.\n", "stackneqrel", { 7 }, 1 },
    { "filter, 5 = nested call (one fewer level)",
      ".decl stackrhsrel(x: int64)\n", "stackrhsrel(x) :- n(x), 5 = ",
      "bor(0, ", 62, "x", ")", ".\n", "stackrhsrel", { 5 }, 1 },
    { "aggregate operand",
      ".decl stackaggrel(t: int64)\n", "stackaggrel(sum(", "bor(0, ", 63,
      "x", ")", ")) :- n(x).\n", "stackaggrel", { 12 }, 1 },
};

static void
test_boundary(const stack_case_t *c)
{
    char label[128];
    snprintf(label, sizeof(label), "%s: accepted", c->label);
    TEST(label);

    char *src = (char *)malloc(SRC_CAP);
    if (!src)
        FAIL("malloc");
    build(src, c->decl, c->head, c->open, c->depth, c->leaf, c->close,
        c->tail);
    struct result_ctx out;
    if (run_snapshot(src, &out) != 0) {
        free(src);
        FAIL("largest accepted depth did not plan and run");
    }
    if (!has_vals(&out, c->want, c->nwant)) {
        free(src);
        FAIL("largest accepted depth gave the wrong rows");
    }
    PASS();

    snprintf(label, sizeof(label), "%s: one more level rejected", c->label);
    TEST(label);
    build(src, c->decl, c->head, c->open, c->depth + 1, c->leaf, c->close,
        c->tail);
    char why[640];
    int ok = plan_rejects(src, c->rel, why, sizeof(why));
    free(src);
    if (!ok)
        FAIL(why);
    PASS();
}

/*
 * Nesting in the first argument of a binary operator holds two values at
 * any depth, and nesting a one-argument call holds one, so 200 levels plan
 * and evaluate; the limit is on values held, not on nesting.
 */
static void
test_left_nesting_accepted(void)
{
    TEST("200 nested calls: first argument, and bnot()");

    char *src = (char *)malloc(SRC_CAP);
    if (!src)
        FAIL("malloc");
    build(src, ".decl stackleftrel(x: int64)\n", "stackleftrel(", "bor(",
        200, "x", ", 0)", ") :- n(x).\n");
    struct result_ctx out;
    int64_t both[2] = { 5, 7 };
    if (run_snapshot(src, &out) != 0 || !has_vals(&out, both, 2)) {
        free(src);
        FAIL("left-nested head");
    }

    build(src, ".decl stackleftf(x: int64)\n", "stackleftf(x) :- n(x), ",
        "bor(", 200, "x", ", 0)", " = 5.\n");
    int64_t five[1] = { 5 };
    if (run_snapshot(src, &out) != 0 || !has_vals(&out, five, 1)) {
        free(src);
        FAIL("left-nested filter");
    }

    /* A one-argument call holds one value at any depth. */
    build(src, ".decl stackunary(x: int64)\n", "stackunary(", "bnot(", 200,
        "x", ")", ") :- n(x).\n");
    if (run_snapshot(src, &out) != 0 || !has_vals(&out, both, 2)) {
        free(src);
        FAIL("200 nested bnot() calls");
    }
    free(src);
    PASS();
}

/* count() never evaluates its operand, so a deep one is not rejected. */
static void
test_count_operand_not_checked(void)
{
    TEST("count() over a 64-level operand plans and counts");

    char *src = (char *)malloc(SRC_CAP);
    if (!src)
        FAIL("malloc");
    build(src, ".decl stackcountrel(t: int64)\n", "stackcountrel(count(",
        "bor(0, ", 64, "x", ")", ")) :- n(x).\n");
    struct result_ctx out;
    int64_t two[1] = { 2 };
    int rc = run_snapshot(src, &out);
    free(src);
    if (rc != 0 || !has_vals(&out, two, 1))
        FAIL("expected stackcountrel(2)");
    PASS();
}

/*
 * A three-argument operator nested in its last argument holds two values
 * per level: 31 levels around cat("a", "b") hold 64 values and plan, 32
 * levels hold 66 and are rejected.  Nested in its first argument it holds
 * three values at any depth.
 */
static void
test_ternary_nesting(void)
{
    TEST("str_replace nested in its last argument, 31 accepted, 32 not");

    char *src = (char *)malloc(SRC_CAP);
    if (!src)
        FAIL("malloc");
    const char *decl = ".decl stacktriprel(s: string)\n";
    build(src, decl, "stacktriprel(", "str_replace(\"q\", \"r\", ", 31,
        "cat(\"a\", \"b\")", ")", ") :- n(_).\n");
    struct result_ctx out;
    if (run_snapshot(src, &out) != 0 || out.count != 1) {
        free(src);
        FAIL("31 levels should plan and derive one row");
    }

    build(src, decl, "stacktriprel(", "str_replace(\"q\", \"r\", ", 32,
        "cat(\"a\", \"b\")", ")", ") :- n(_).\n");
    char why[640];
    if (!plan_rejects(src, "stacktriprel", why, sizeof(why))) {
        free(src);
        FAIL(why);
    }

    build(src, ".decl stacktripl(s: string)\n", "stacktripl(", "substr(",
        100, "\"abcdef\"", ", 0, 3)", ") :- n(_).\n");
    if (run_snapshot(src, &out) != 0 || out.count != 1) {
        free(src);
        FAIL("100 levels in the first argument should plan");
    }
    free(src);
    PASS();
}

int
main(void)
{
    printf("Expression stack limit (Issue #2231)\n");
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
        test_boundary(&cases[i]);
    test_left_nesting_accepted();
    test_count_operand_not_checked();
    test_ternary_nesting();

    printf("\n%d tests: %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
