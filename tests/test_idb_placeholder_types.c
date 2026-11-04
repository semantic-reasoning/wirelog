/*
 * test_idb_placeholder_types.c - Recursive IDB schemas across workers
 * (Issue #2334)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Each case evaluates a recursive program through the columnar backend at
 * one or more workers and compares the snapshot of one relation, decoded
 * with the declared column types, against rows written out by hand.  At
 * two or more workers it also checks that the stratum ran through a TDD
 * exchange, so a case cannot pass by falling back to one worker.
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L /* setenv */
#endif

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/intern.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
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
    return _putenv_s(name, value ? value : "");
}

#  define setenv wl_test_setenv_
#endif

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define MAX_ROWS 512
#define ROW_TEXT 128
#define OUT_TEXT (MAX_ROWS * ROW_TEXT)

/* Rows of one relation, rendered with the relation's declared types. */
typedef struct {
    const char *rel;
    const wl_ir_relation_info_t *info;
    const wl_intern_t *intern;
    char rows[MAX_ROWS][ROW_TEXT];
    uint32_t count;
    bool overflow;
} capture_t;

static void
render_value(char *buf, size_t cap, wirelog_column_type_t type, int64_t v,
    const wl_intern_t *intern)
{
    if (type == WIRELOG_TYPE_STRING) {
        const char *s = intern ? wl_intern_reverse(intern, v) : NULL;
        snprintf(buf, cap, "\"%s\"", s ? s : "?");
    } else if (type == WIRELOG_TYPE_FLOAT) {
        double d;
        memcpy(&d, &v, sizeof(d));
        snprintf(buf, cap, "%g", d);
    } else {
        snprintf(buf, cap, "%" PRId64, v);
    }
}

static void
capture_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    capture_t *cap = (capture_t *)user_data;
    if (!relation || strcmp(relation, cap->rel) != 0)
        return;
    if (cap->count >= MAX_ROWS) {
        cap->overflow = true;
        return;
    }
    char *out = cap->rows[cap->count++];
    size_t len = 0;
    out[0] = '\0';
    for (uint32_t c = 0; c < ncols; c++) {
        char value[64];
        wirelog_column_type_t type = WIRELOG_TYPE_INT64;
        if (cap->info && c < cap->info->column_count)
            type = cap->info->columns[c].type;
        render_value(value, sizeof(value), type, row[c], cap->intern);
        int n = snprintf(out + len, ROW_TEXT - len, "%s%s", c ? "," : "",
                value);
        if (n < 0 || (size_t)n >= ROW_TEXT - len) {
            cap->overflow = true;
            return;
        }
        len += (size_t)n;
    }
}

static int
row_cmp(const void *a, const void *b)
{
    return strcmp((const char *)a, (const char *)b);
}

/* The result of one evaluation. */
typedef struct {
    int rc;
    char text[OUT_TEXT];
    const char *strategy;
    uint32_t selected_workers;
    /* Column types of @rel on the session after the snapshot, as many as
     * the relation has columns (0 when it has none). */
    wirelog_column_type_t types[8];
    uint32_t type_count;
    bool typed;
} run_result_t;

static const wl_ir_relation_info_t *
find_info(const wirelog_program_t *prog, const char *name)
{
    for (uint32_t i = 0; i < prog->relation_count; i++)
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, name) == 0)
            return &prog->relations[i];
    return NULL;
}

/* Evaluate @src at @workers and capture @rel.  Returns false when the
 * fixture itself failed (parse, plan or session setup). */
static bool
run_program(const char *src, const char *rel, uint32_t workers,
    run_result_t *res)
{
    static capture_t cap;
    wirelog_error_t err;
    memset(res, 0, sizeof(*res));
    memset(&cap, 0, sizeof(cap));
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return false;
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);
    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0 || !plan) {
        wirelog_program_free(prog);
        return false;
    }
    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, workers, &sess) != 0
        || wl_session_load_facts(sess, prog) != 0) {
        if (sess)
            wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return false;
    }
    cap.rel = rel;
    cap.info = find_info(prog, rel);
    cap.intern = prog->intern;
    res->rc = wl_session_snapshot(sess, capture_cb, &cap);
    if (cap.overflow && res->rc == 0)
        res->rc = -1000;
    qsort(cap.rows, cap.count, sizeof(cap.rows[0]), row_cmp);
    size_t len = 0;
    for (uint32_t i = 0; i < cap.count; i++) {
        int n = snprintf(res->text + len, OUT_TEXT - len, "%s%s",
                i ? " " : "", cap.rows[i]);
        if (n < 0 || (size_t)n >= OUT_TEXT - len)
            break;
        len += (size_t)n;
    }
    wl_col_session_t *cs = COL_SESSION(sess);
    res->strategy = cs->tdd_audit.strategy;
    res->selected_workers = cs->tdd_audit.selected_workers;
    col_rel_t *r = session_find_rel(cs, rel);
    if (r && r->ncols <= 8) {
        res->type_count = r->ncols;
        res->typed = r->column_types != NULL;
        for (uint32_t c = 0; c < r->ncols; c++)
            res->types[c] = r->column_types ? r->column_types[c]
                : WIRELOG_TYPE_INT64;
    }
    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return true;
}

/* Run @src at @workers and require rc 0 and exactly @expected rows of
 * @rel; at two or more workers also require the TDD @strategy. */
static void
check_rows(const char *label, const char *src, const char *rel,
    uint32_t workers, const char *strategy, const char *expected)
{
    static run_result_t res;
    char name[128];
    snprintf(name, sizeof(name), "%s W=%u", label, workers);
    tests_run++;
    printf("  [TEST] %-55s", name);
    fflush(stdout);
    if (!run_program(src, rel, workers, &res)) {
        tests_failed++;
        printf(" FAIL: fixture setup failed\n");
        return;
    }
    if (res.rc != 0) {
        tests_failed++;
        printf(" FAIL: snapshot rc=%d\n", res.rc);
        return;
    }
    if (strcmp(res.text, expected) != 0) {
        tests_failed++;
        printf(" FAIL: rows\n    got:      %s\n    expected: %s\n", res.text,
            expected);
        return;
    }
    if (workers >= 2 && strategy
        && (!res.strategy || strcmp(res.strategy, strategy) != 0
        || res.selected_workers < 2)) {
        tests_failed++;
        printf(" FAIL: route strategy=%s selected=%u (expected %s, >= 2)\n",
            res.strategy ? res.strategy : "(none)", res.selected_workers,
            strategy);
        return;
    }
    tests_passed++;
    printf(" PASS\n");
}

static void
check_rows_at(const char *label, const char *src, const char *rel,
    const uint32_t *workers, uint32_t nworkers, const char *strategy,
    const char *expected)
{
    for (uint32_t i = 0; i < nworkers; i++)
        check_rows(label, src, rel, workers[i], strategy, expected);
}

static const uint32_t W124[] = { 1, 2, 4 };

/* ======================================================================== */
/* Integer-family lanes in the TDD exchange schema check                   */
/* ======================================================================== */

/*
 * A recursive rule listed before the rule that seeds its head reads the
 * head before it has rows.  Its first-round result can label an int32
 * lane INT64 while later rounds label it INT32, and a BOOL-valued builtin
 * labels an int64 lane BOOL.  Integer-family lanes share the int64 storage
 * lane (see col_rel_append_all), so the owner exchange must accept these
 * deltas into the same relation; before #2334 it refused them with EINVAL.
 * UINT32 and UINT64 belong to the same family, but no .dl column type or
 * builtin produces them, so no program here can reach them.
 */
static void
test_int_family(void)
{
    check_rows_at("int32 head shift, string lane in step",
        ".decl s(n: int32, m: int32)\n"
        ".decl step(m: int32, k: int32, t: string)\n"
        ".decl b(n: int32, m: int32)\n"
        "b(m, k) :- b(n, m), step(m, k, t).\n"
        "b(n, m) :- s(n, m).\n"
        "s(0, 1).\n"
        "step(1, 2, \"t\"). step(2, 3, \"t\"). step(3, 4, \"t\").\n",
        "b", W124, 3, "owner", "0,1 1,2 2,3 3,4");
    check_rows_at("int32 chain, string lane first in step",
        ".decl step(t: string, m: int32, k: int32)\n"
        ".decl s(n: int32, m: int32)\n"
        ".decl b(n: int32, m: int32)\n"
        "b(n, k) :- b(n, m), step(t, m, k).\n"
        "b(n, m) :- s(n, m).\n"
        "s(0, 1).\n"
        "step(\"t\", 1, 2). step(\"t\", 2, 3). step(\"t\", 3, 4).\n",
        "b", W124, 3, "owner", "0,1 0,2 0,3 0,4");
    check_rows_at("BOOL builtin into an int64 lane",
        ".decl e(x: string, y: string)\n"
        ".decl b(x: string, y: string, f: int64)\n"
        "b(x, z, contains(z, \"2\")) :- b(x, y, f), e(y, z).\n"
        "b(x, y, 0) :- e(x, y).\n"
        "e(\"n0\", \"n1\"). e(\"n1\", \"n2\"). e(\"n2\", \"n3\").\n",
        "b", W124, 3, "owner",
        "\"n0\",\"n1\",0 \"n0\",\"n2\",1 \"n0\",\"n3\",0 "
        "\"n1\",\"n2\",0 \"n1\",\"n3\",0 \"n2\",\"n3\",0");

    /* Controls: shapes that already passed, kept so the family rule does
     * not change them. */
    check_rows_at("control: string+int32 shift, untyped step",
        ".decl s(x: string, n: int32, m: int32)\n"
        ".decl step(m: int32, k: int32)\n"
        ".decl b(x: string, n: int32, m: int32)\n"
        "b(x, m, k) :- b(x, n, m), step(m, k).\n"
        "b(x, n, m) :- s(x, n, m).\n"
        "s(\"a\", 0, 1).\n"
        "step(1, 2). step(2, 3). step(3, 4).\n",
        "b", W124, 3, "owner", "\"a\",0,1 \"a\",1,2 \"a\",2,3 \"a\",3,4");
    check_rows_at("control: int32 shift, base rule first",
        ".decl s(n: int32, m: int32)\n"
        ".decl step(m: int32, k: int32, t: string)\n"
        ".decl b(n: int32, m: int32)\n"
        "b(n, m) :- s(n, m).\n"
        "b(m, k) :- b(n, m), step(m, k, t).\n"
        "s(0, 1).\n"
        "step(1, 2, \"t\"). step(2, 3, \"t\"). step(3, 4, \"t\").\n",
        "b", W124, 3, "owner", "0,1 1,2 2,3 3,4");
    check_rows_at("control: string+int32 with arithmetic",
        ".decl s(x: string, n: int32)\n"
        ".decl step(n: int32)\n"
        ".decl b(x: string, n: int32)\n"
        "b(x, n) :- s(x, n).\n"
        "b(x, n + 1) :- b(x, n), step(n).\n"
        "s(\"a\", 0). s(\"b\", 2).\n"
        "step(0). step(1). step(2). step(3).\n",
        "b", W124, 3, "owner",
        "\"a\",0 \"a\",1 \"a\",2 \"a\",3 \"a\",4 \"b\",2 \"b\",3 \"b\",4");
}

int
main(void)
{
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0
        || setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    printf("test_idb_placeholder_types (#2334)\n");
    test_int_family();
    printf("\n%d tests: %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
