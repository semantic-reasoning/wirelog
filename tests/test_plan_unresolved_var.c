/*
 * test_plan_unresolved_var.c - unresolved names fail to plan (Issue #2231)
 *
 * Plan generation resolved a variable by its name in the operator's input
 * and emitted any other name unchanged, and the interpreted evaluator read
 * an unknown name as column 0.  From program text such a name arises from
 * a variable of a compound term of another functor nested in a matching
 * one (tested below); the other tests change IR from a parsed program the
 * way an internal producer could.  A filter, head (MAP) or aggregate
 * expression naming no input column must fail to plan.
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/ir.h"
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

struct rows {
    int64_t v[8];
    uint32_t n;
};

static void
collect_m(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct rows *r = (struct rows *)user_data;
    if (strcmp(relation, "m") == 0 && ncols > 0 && r->n < 8)
        r->v[r->n++] = row[0];
}

static wirelog_ir_node_t *
relation_root(struct wirelog_program *prog, const char *name)
{
    for (uint32_t i = 0; i < prog->relation_count; i++)
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, name) == 0)
            return prog->relation_irs ? prog->relation_irs[i] : NULL;
    return NULL;
}

static wirelog_ir_node_t *
find_node(wirelog_ir_node_t *node, wirelog_ir_node_type_t type)
{
    if (!node)
        return NULL;
    if (node->type == type)
        return node;
    for (uint32_t i = 0; i < node->child_count; i++) {
        wirelog_ir_node_t *f = find_node(node->children[i], type);
        if (f)
            return f;
    }
    return NULL;
}

/* The first VAR under @e. */
static wl_ir_expr_t *
find_var(wl_ir_expr_t *e)
{
    if (!e)
        return NULL;
    if (e->type == WL_IR_EXPR_VAR)
        return e;
    for (uint32_t i = 0; i < e->child_count; i++) {
        wl_ir_expr_t *f = find_var(e->children[i]);
        if (f)
            return f;
    }
    return NULL;
}

static bool
rename_var(wl_ir_expr_t *v, const char *name)
{
    char *copy = (char *)malloc(strlen(name) + 1);
    if (!v || !copy) {
        free(copy);
        return false;
    }
    strcpy(copy, name);
    free(v->var_name);
    v->var_name = copy;
    return true;
}

enum site { SITE_PROJECT, SITE_FILTER, SITE_AGGREGATE };

/* Parses @src, renames the first variable at @site of relation m to @name,
 * and plans.  Returns the planner's rc; *@err receives its error text and
 * @out the rows of m when the plan runs. */
static int
plan_with_var(const char *src, enum site site, const char *name,
    char *err, size_t err_cap, struct rows *out)
{
    memset(out, 0, sizeof(*out));
    err[0] = '\0';
    wirelog_error_t perr;
    wirelog_program_t *prog = wirelog_parse_string(src, &perr);
    if (!prog) {
        snprintf(err, err_cap, "parse failed");
        return -2;
    }
    wirelog_ir_node_t *root = relation_root(prog, "m");
    wl_ir_expr_t *v = NULL;
    if (site == SITE_PROJECT) {
        wirelog_ir_node_t *p = find_node(root, WIRELOG_IR_PROJECT);
        v = p && p->project_exprs ? find_var(p->project_exprs[0]) : NULL;
    } else if (site == SITE_FILTER) {
        wirelog_ir_node_t *f = find_node(root, WIRELOG_IR_FILTER);
        v = f ? find_var(f->filter_expr) : NULL;
    } else {
        wirelog_ir_node_t *a = find_node(root, WIRELOG_IR_AGGREGATE);
        v = a ? find_var(a->agg_expr) : NULL;
    }
    if (!rename_var(v, name)) {
        wirelog_program_free(prog);
        snprintf(err, err_cap, "no variable at the site");
        return -2;
    }
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        const char *msg = wirelog_program_get_plan_error(prog);
        snprintf(err, err_cap, "%s", msg ? msg : "");
        wirelog_program_free(prog);
        return rc;
    }
    wl_session_t *sess = NULL;
    int run = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (run == 0)
        run = wl_session_load_facts(sess, prog);
    if (run == 0)
        run = wl_session_snapshot(sess, collect_m, out);
    if (sess)
        wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    if (run != 0)
        snprintf(err, err_cap, "run failed %d", run);
    return 0;
}

#define N2 ".decl n(a: int64, b: int64)\nn(1, 10).\nn(2, 20).\n"

static const char *const project_src = N2
    ".decl m(x: int64)\nm(b) :- n(a, b).\n";
static const char *const filter_src = N2
    ".decl m(x: int64)\nm(b) :- n(a, b), b > 15.\n";
static const char *const aggregate_src = N2
    ".decl m(s: int64)\nm(sum(b)) :- n(a, b).\n";

static void
test_unknown_names_fail(void)
{
    static const struct {
        const char *what;
        const char *src;
        enum site site;
        const char *name;
        const char *shown;
    } cases[] = {
        { "head", NULL, SITE_PROJECT, "zz", "'zz'" },
        { "filter", NULL, SITE_FILTER, "zz", "'zz'" },
        { "aggregate", NULL, SITE_AGGREGATE, "zz", "'zz'" },
        { "out of range", NULL, SITE_PROJECT, "col2", "'col2'" },
        { "renamed user col1", NULL, SITE_PROJECT, "$col1", "'col1'" },
    };

    TEST("Plan: a name that is not an input column fails to plan");
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *src = cases[i].site == SITE_PROJECT ? project_src
            : cases[i].site == SITE_FILTER ? filter_src : aggregate_src;
        char err[512];
        struct rows r;
        int rc = plan_with_var(src, cases[i].site, cases[i].name, err,
                sizeof(err), &r);
        if (rc == 0 || !strstr(err, cases[i].shown)
            || !strstr(err, "names no input column")) {
            printf(" [%s: rc %d, rows %u, '%s']", cases[i].what, rc, r.n,
                err);
            FAIL("expected a plan error naming the variable");
        }
    }
    PASS();
}

static void
test_positional_names_still_plan(void)
{
    TEST("Plan: an in-range colN still reads that column");
    char err[512];
    struct rows r;
    int rc = plan_with_var(project_src, SITE_PROJECT, "col1", err,
            sizeof(err), &r);
    if (rc != 0 || r.n != 2 || r.v[0] + r.v[1] != 30) {
        printf(" [rc %d, rows %u, '%s']", rc, r.n, err);
        FAIL("expected m(10), m(20)");
    }
    rc = plan_with_var(project_src, SITE_PROJECT, "col0", err, sizeof(err),
            &r);
    if (rc != 0 || r.n != 2 || r.v[0] + r.v[1] != 3) {
        printf(" [rc %d, rows %u, '%s']", rc, r.n, err);
        FAIL("expected m(1), m(2)");
    }
    PASS();
}

/* A compound term of another functor nested inside a matching one has no
 * column for its variables; c was read as column 0 and derived o(1). */
static void
test_nested_mismatched_term(void)
{
    TEST("Plan: a variable of a nested mismatched term fails to plan");
    wirelog_error_t perr;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl pred(id: int64, payload: f/2 inline)\npred(1, 10, 20).\n"
        ".decl o(c: int64)\no(c) :- pred(a, f(b, g(c, _))).\n", &perr);
    if (!prog)
        FAIL("parse failed");
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    const char *msg = wirelog_program_get_plan_error(prog);
    bool named = msg && strstr(msg, "variable 'c' names no input column");
    if (rc == 0)
        wl_plan_free(plan);
    wirelog_program_free(prog);
    if (rc == 0 || !named)
        FAIL("expected a plan error naming c");
    PASS();

    /* As a group-by key, c took the column at its head position: with
     * pred(1, 10, 20) and pred(2, 10, 30) the rule derived t(1, 1) and
     * t(2, 1). */
    TEST("Load: such a variable as a group-by key is rejected");
    char errbuf[512];
    wirelog_error_t err = WIRELOG_OK;
    struct wirelog_program *p2 = wl_ir_parse_string_err(
        ".decl pred(id: int64, payload: f/2 inline)\npred(1, 10, 20).\n"
        "pred(2, 10, 30).\n.decl t(c: int64, n: int64)\n"
        "t(c, count(a)) :- pred(a, f(b, g(c, _))).\n", &err, errbuf,
        sizeof(errbuf));
    if (p2) {
        wirelog_program_free(p2);
        FAIL("the rule loaded");
    }
    if (!strstr(errbuf, "group-by key 'c' of rule 't' names no column")) {
        printf(" ['%s']", errbuf);
        FAIL("wrong error");
    }
    PASS();
}

int
main(void)
{
    printf("test_plan_unresolved_var (#2231)\n");
    test_unknown_names_fail();
    test_positional_names_still_plan();
    test_nested_mismatched_term();
    printf("=== Results: %d passed, %d failed, %d total ===\n", tests_passed,
        tests_failed, tests_run);
    return tests_failed == 0 ? 0 : 1;
}
