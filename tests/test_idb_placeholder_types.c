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
        snprintf(buf, cap, "%.17g", d);
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
    /* Recursive strata the snapshot planned, ran through TDD, and sent to
     * the one-worker path. */
    uint32_t recursive_strata;
    uint32_t executed_strata;
    uint32_t fallback_strata;
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
    res->recursive_strata = cs->tdd_recursive_strata;
    res->executed_strata = cs->tdd_executed_strata;
    res->fallback_strata = cs->tdd_fallback_strata;
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

/* At two or more workers, every recursive stratum must have run through
 * TDD on at least two workers, and when @strategy is given, the last
 * stratum must have used that exchange.  Returns NULL or a reason. */
static const char *
route_failure(const run_result_t *res, uint32_t workers, const char *strategy,
    char *buf, size_t cap)
{
    if (workers < 2)
        return NULL;
    if (res->recursive_strata == 0
        || res->executed_strata != res->recursive_strata
        || res->fallback_strata != 0) {
        snprintf(buf, cap, "route: recursive=%u tdd=%u fallback=%u",
            res->recursive_strata, res->executed_strata,
            res->fallback_strata);
        return buf;
    }
    if (strategy
        && (!res->strategy || strcmp(res->strategy, strategy) != 0
        || res->selected_workers < 2)) {
        snprintf(buf, cap, "route: strategy=%s selected=%u (expected %s)",
            res->strategy ? res->strategy : "(none)", res->selected_workers,
            strategy);
        return buf;
    }
    return NULL;
}

/* Run @src at @workers and require rc 0 and exactly @expected rows of
 * @rel; at two or more workers also require the TDD route (see
 * route_failure). */
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
    char why[128];
    if (route_failure(&res, workers, strategy, why, sizeof(why))) {
        tests_failed++;
        printf(" FAIL: %s\n", why);
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

/* ======================================================================== */
/* Declared schemas of empty IDB placeholders                              */
/* ======================================================================== */

#define STR_EDGES \
        "edge(\"n0\", \"n1\"). edge(\"n1\", \"n2\"). edge(\"n2\", \"n3\").\n"
#define STR_CLOSURE \
        "\"n0\",\"n1\" \"n0\",\"n2\" \"n0\",\"n3\" " \
        "\"n1\",\"n2\" \"n1\",\"n3\" \"n2\",\"n3\""

static const uint32_t W148[] = { 1, 4, 8 };

/* Run @src at one worker and at each of @workers, and require every run to
 * succeed with @count rows of @rel, identical to the one-worker rows.  For
 * programs too large to write their rows out by hand. */
static void
check_same_as_one_worker(const char *label, const char *src, const char *rel,
    const uint32_t *workers, uint32_t nworkers, const char *strategy,
    uint32_t count)
{
    static run_result_t one;
    static run_result_t res;
    for (uint32_t i = 0; i < nworkers; i++) {
        char name[128];
        snprintf(name, sizeof(name), "%s W=%u", label, workers[i]);
        tests_run++;
        printf("  [TEST] %-55s", name);
        fflush(stdout);
        if (!run_program(src, rel, 1, &one)
            || !run_program(src, rel, workers[i], &res)) {
            tests_failed++;
            printf(" FAIL: fixture setup failed\n");
            continue;
        }
        uint32_t rows = 0;
        for (const char *c = one.text; *c; c++)
            rows += *c == ' ';
        rows += one.text[0] != '\0';
        char why[128];
        if (one.rc != 0 || res.rc != 0) {
            tests_failed++;
            printf(" FAIL: rc W=1 %d, W=%u %d\n", one.rc, workers[i], res.rc);
        } else if (rows != count) {
            tests_failed++;
            printf(" FAIL: %u rows at W=1 (expected %u)\n", rows, count);
        } else if (strcmp(one.text, res.text) != 0) {
            tests_failed++;
            printf(" FAIL: rows differ from W=1\n");
        } else if (route_failure(&res, workers[i], strategy, why,
            sizeof(why))) {
            tests_failed++;
            printf(" FAIL: %s\n", why);
        } else {
            tests_passed++;
            printf(" PASS\n");
        }
    }
}

/* The issue's three mutually recursive relations over a 24-node graph:
 * edge(i, i+1) for i < 23 and edge(i, i+3) for even i <= 20.  @right_b
 * makes b's recursive rule right-linear. */
static void
build_mutual(char *buf, size_t cap, const char *type, bool right_b)
{
    size_t len = (size_t)snprintf(buf, cap,
            ".decl edge(x: %s, y: %s)\n"
            ".decl a(x: %s, y: %s)\n"
            ".decl b(x: %s, y: %s)\n"
            ".decl c(x: %s, y: %s)\n"
            "a(x, y) :- edge(x, y).\n"
            "a(x, z) :- a(x, y), edge(y, z).\n"
            "%s"
            "c(x, z) :- c(x, y), edge(y, z).\n"
            "b(x, y) :- a(x, y).\n"
            "c(x, y) :- b(x, y).\n"
            "a(x, y) :- c(x, y).\n",
            type, type, type, type, type, type, type, type,
            right_b ? "b(x, z) :- edge(x, y), b(y, z).\n"
            : "b(x, z) :- b(x, y), edge(y, z).\n");
    bool str = strcmp(type, "string") == 0;
    for (int i = 0; i < 23 && len < cap; i++) {
        int to[2] = { i + 1, (i % 2 == 0 && i + 3 < 24) ? i + 3 : -1 };
        for (int k = 0; k < 2 && len < cap; k++) {
            if (to[k] < 0)
                continue;
            int n = str
                ? snprintf(buf + len, cap - len, "edge(\"n%d\", \"n%d\").\n",
                    i, to[k])
                : snprintf(buf + len, cap - len, "edge(%d, %d).\n", i, to[k]);
            if (n > 0)
                len += (size_t)n;
        }
    }
}

/*
 * Before #2334 an empty IDB started as a zero-width, untyped relation.  A
 * recursive rule evaluated before the rule that seeds the head joined that
 * placeholder and typed its result from the wrong input columns, so a
 * string lane could be labelled INT64.  The owner exchange then saw
 * [STRING, INT64] in the first round and [STRING, STRING] later and
 * failed with EINVAL; a FLOAT IDB failed even at one worker, because the
 * untyped placeholder met a FLOAT join key or a FLOAT append.
 */
static void
test_string_heads(void)
{
    static char src[8192];
    check_rows_at("string tc, recursive rule first",
        ".decl edge(x: string, y: string)\n"
        ".decl b(x: string, y: string)\n"
        "b(x, z) :- b(x, y), edge(y, z).\n"
        "b(x, y) :- edge(x, y).\n" STR_EDGES,
        "b", W124, 3, "owner", STR_CLOSURE);
    check_rows_at("string tc, right-linear",
        ".decl edge(x: string, y: string)\n"
        ".decl b(x: string, y: string)\n"
        "b(x, z) :- edge(x, y), b(y, z).\n"
        "b(x, y) :- edge(x, y).\n" STR_EDGES,
        "b", W124, 3, "owner", STR_CLOSURE);
    check_rows_at("string+int32 shift, string lane in step",
        ".decl s(x: string, n: int32, m: int32)\n"
        ".decl step(m: int32, k: int32, t: string)\n"
        ".decl b(x: string, n: int32, m: int32)\n"
        "b(x, m, k) :- b(x, n, m), step(m, k, t).\n"
        "b(x, n, m) :- s(x, n, m).\n"
        "s(\"a\", 0, 1).\n"
        "step(1, 2, \"t\"). step(2, 3, \"t\"). step(3, 4, \"t\").\n",
        "b", W124, 3, "owner", "\"a\",0,1 \"a\",1,2 \"a\",2,3 \"a\",3,4");
    /* Two keys at four and eight workers: most workers own no key, so
     * their local partition stays an empty placeholder while the
     * exchanged delta has rows. */
    check_rows_at("two keys, most partitions empty (owner)",
        ".decl edge(x: string, y: string)\n"
        ".decl b(x: string, y: string)\n"
        "b(x, z) :- b(x, y), edge(y, z).\n"
        "b(x, y) :- edge(x, y).\n"
        "edge(\"k\", \"k1\"). edge(\"k1\", \"k\").\n",
        "b", W148, 3, "owner",
        "\"k\",\"k\" \"k\",\"k1\" \"k1\",\"k\" \"k1\",\"k1\"");
    check_rows_at("control: two keys, self-join (bdx)",
        ".decl edge(x: string, y: string)\n"
        ".decl b(x: string, y: string)\n"
        "b(x, z) :- b(x, y), b(y, z).\n"
        "b(x, y) :- edge(x, y).\n"
        "edge(\"k\", \"k1\"). edge(\"k1\", \"k\").\n",
        "b", W148, 3, "bdx",
        "\"k\",\"k\" \"k\",\"k1\" \"k1\",\"k\" \"k1\",\"k1\"");
    /* A later stratum reads the recursive one; the last stratum is not
     * recursive, so only the general route check applies. */
    check_rows_at("string tc read by a triangle stratum",
        ".decl edge(x: string, y: string)\n"
        ".decl b(x: string, y: string)\n"
        ".decl t(x: string, y: string, z: string)\n"
        "b(x, z) :- b(x, y), edge(y, z).\n"
        "b(x, y) :- edge(x, y).\n"
        "t(x, y, z) :- b(x, y), b(y, z), b(x, z).\n" STR_EDGES,
        "t", W124, 3, NULL,
        "\"n0\",\"n1\",\"n2\" \"n0\",\"n1\",\"n3\" \"n0\",\"n2\",\"n3\" "
        "\"n1\",\"n2\",\"n3\"");

    /* Controls for the canonical INT64 lanes: an int32 lane next to a
     * string lane, filled from string, int32 and arithmetic sources.  A
     * placeholder typed INT64 against INT32 deltas needs the int-family
     * schema rule. */
    check_rows_at("control: string+int32 arithmetic, recursive first",
        ".decl s(x: string, n: int32)\n"
        ".decl step(n: int32)\n"
        ".decl b(x: string, n: int32)\n"
        "b(x, n + 1) :- b(x, n), step(n).\n"
        "b(x, n) :- s(x, n).\n"
        "s(\"a\", 0). s(\"b\", 2).\n"
        "step(0). step(1). step(2). step(3).\n",
        "b", W124, 3, "owner",
        "\"a\",0 \"a\",1 \"a\",2 \"a\",3 \"a\",4 \"b\",2 \"b\",3 \"b\",4");
    check_rows_at("control: string+int32 through an int32 key",
        ".decl edge(x: string, y: int32)\n"
        ".decl nxt(x: int32, y: string)\n"
        ".decl b(x: string, y: int32)\n"
        "b(x, z) :- b(x, y), nxt(y, w), edge(w, z).\n"
        "b(x, y) :- edge(x, y).\n"
        "edge(\"a\", 1). edge(\"b\", 2).\n"
        "nxt(1, \"b\").\n",
        "b", W124, 3, "owner", "\"a\",1 \"a\",2 \"b\",2");
    check_rows_at("control: string+int32 seeded by a cross product",
        ".decl names(x: string)\n"
        ".decl nums(n: int32)\n"
        ".decl link(x: string, y: string)\n"
        ".decl b(x: string, n: int32)\n"
        "b(x, n) :- names(x), nums(n).\n"
        "b(y, n) :- b(x, n), link(x, y).\n"
        "names(\"a\"). nums(7). nums(8).\n"
        "link(\"a\", \"b\"). link(\"b\", \"c\").\n",
        "b", W124, 3, "owner",
        "\"a\",7 \"a\",8 \"b\",7 \"b\",8 \"c\",7 \"c\",8");

    build_mutual(src, sizeof(src), "string", false);
    check_same_as_one_worker("issue program, string", src, "a",
        W124 + 1, 2, "owner", 276);
    build_mutual(src, sizeof(src), "string", true);
    check_same_as_one_worker("issue program, string, b right-linear", src,
        "a", W124 + 1, 2, "owner", 276);
    build_mutual(src, sizeof(src), "int32", false);
    check_same_as_one_worker("control: issue program, int32", src, "a",
        W124 + 1, 2, "owner", 276);
}

static void
test_float_heads(void)
{
    check_rows_at("float reachability, seeded by a rule",
        ".decl e(x: float, y: float)\n"
        ".decl s(x: float)\n"
        ".decl r(x: float)\n"
        "r(y) :- s(y).\n"
        "r(y) :- r(x), e(x, y).\n"
        "s(0.5). e(0.5, 1.5). e(1.5, 2.5).\n",
        "r", W124, 3, "owner", "0.5 1.5 2.5");
    check_rows_at("float tc, right-linear, recursive rule first",
        ".decl e(x: float, y: float)\n"
        ".decl r(x: float, y: float)\n"
        "r(x, z) :- e(x, y), r(y, z).\n"
        "r(x, y) :- e(x, y).\n"
        "e(0.5, 1.5). e(1.5, 2.5).\n",
        "r", W124, 3, "owner", "0.5,1.5 0.5,2.5 1.5,2.5");
    check_rows_at("float tc, right-linear, base rule first",
        ".decl e(x: float, y: float)\n"
        ".decl r(x: float, y: float)\n"
        "r(x, y) :- e(x, y).\n"
        "r(x, z) :- e(x, y), r(y, z).\n"
        "e(0.5, 1.5). e(1.5, 2.5).\n",
        "r", W124, 3, "owner", "0.5,1.5 0.5,2.5 1.5,2.5");
    check_rows_at("int32/float lanes through a float join",
        ".decl edge(x: int32, y: float)\n"
        ".decl e2(x: float, y: int32)\n"
        ".decl b(x: int32, y: float)\n"
        "b(x, w) :- b(x, y), e2(y, z), edge(z, w).\n"
        "b(x, y) :- edge(x, y).\n"
        "edge(1, 2.0). edge(2, 3.0).\n"
        "e2(2.0, 2). e2(3.0, 3).\n",
        "b", W124, 3, "owner", "1,2 1,3 2,3");
    check_rows_at("int32/float lanes, base rule first",
        ".decl edge(x: int32, y: float)\n"
        ".decl e2(x: float, y: int32)\n"
        ".decl b(x: int32, y: float)\n"
        "b(x, y) :- edge(x, y).\n"
        "b(x, w) :- b(x, y), e2(y, z), edge(z, w).\n"
        "edge(1, 2.0). edge(2, 3.0).\n"
        "e2(2.0, 2). e2(3.0, 3).\n",
        "b", W124, 3, "owner", "1,2 1,3 2,3");
    check_rows_at("float value carried along int edges",
        ".decl edge(x: int32, y: int32)\n"
        ".decl val(x: int32, f: float)\n"
        ".decl b(x: int32, f: float)\n"
        "b(x, f) :- b(x0, f), edge(x0, x).\n"
        "b(x, f) :- val(x, f).\n"
        "edge(1, 2). edge(2, 3).\n"
        "val(1, 1.5).\n",
        "b", W124, 3, "owner", "1,1.5 2,1.5 3,1.5");
    check_rows_at("float value carried, base rule first",
        ".decl edge(x: int32, y: int32)\n"
        ".decl val(x: int32, f: float)\n"
        ".decl b(x: int32, f: float)\n"
        "b(x, f) :- val(x, f).\n"
        "b(x, f) :- b(x0, f), edge(x0, x).\n"
        "edge(1, 2). edge(2, 3).\n"
        "val(1, 1.5).\n",
        "b", W124, 3, "owner", "1,1.5 2,1.5 3,1.5");
    /* One row: at two or more workers the adaptive worker count selects
     * one worker for this stratum, so only the one-worker run is checked;
     * it failed there before #2334. */
    check_rows("float head joined with itself and an EDB",
        ".decl s(x: float)\n"
        ".decl r(x: float)\n"
        "r(x) :- r(x), s(x).\n"
        "r(x) :- s(x).\n"
        "s(0.5).\n",
        "r", 1, NULL, "0.5");
    check_rows_at("float same generation (global_read)",
        ".decl par(x: float, y: float)\n"
        ".decl sg(x: float, y: float)\n"
        "sg(x, y) :- par(x, p), sg(p, q), par(y, q).\n"
        "sg(x, y) :- par(x, p), par(y, p), x != y.\n"
        "par(1.5, 9.5). par(2.5, 9.5). par(3.5, 1.5). par(4.5, 2.5).\n"
        "par(5.5, 3.5). par(6.5, 4.5).\n",
        "sg", W124, 3, "global_read",
        "1.5,2.5 2.5,1.5 3.5,4.5 4.5,3.5 5.5,6.5 6.5,5.5");
}

/* The lane types an IDB ends with at one worker, where nothing compares
 * them across rounds: they must be the declared ones in canonical form
 * (STRING and FLOAT as declared, other lanes INT64). */
static void
check_types(const char *label, const char *src, const char *rel,
    const wirelog_column_type_t *expected, uint32_t n)
{
    static run_result_t res;
    tests_run++;
    printf("  [TEST] %-55s", label);
    fflush(stdout);
    if (!run_program(src, rel, 1, &res)) {
        tests_failed++;
        printf(" FAIL: fixture setup failed\n");
        return;
    }
    bool ok = res.rc == 0 && res.typed && res.type_count == n;
    for (uint32_t c = 0; ok && c < n; c++)
        ok = res.types[c] == expected[c];
    if (!ok) {
        tests_failed++;
        printf(" FAIL: rc=%d ncols=%u types=", res.rc, res.type_count);
        for (uint32_t c = 0; c < res.type_count; c++)
            printf("%s%d", c ? "," : "", (int)res.types[c]);
        printf("%s\n", res.typed ? "" : " (untyped)");
        return;
    }
    tests_passed++;
    printf(" PASS\n");
}

static void
test_idb_types(void)
{
    static char src[8192];
    const wirelog_column_type_t ss[] = {
        WIRELOG_TYPE_STRING, WIRELOG_TYPE_STRING
    };
    const wirelog_column_type_t ff[] = {
        WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_FLOAT
    };
    const wirelog_column_type_t ii[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64
    };
    const wirelog_column_type_t sii[] = {
        WIRELOG_TYPE_STRING, WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64
    };
    check_types("types: string tc, recursive rule first",
        ".decl edge(x: string, y: string)\n"
        ".decl b(x: string, y: string)\n"
        "b(x, z) :- b(x, y), edge(y, z).\n"
        "b(x, y) :- edge(x, y).\n" STR_EDGES, "b", ss, 2);
    check_types("types: string tc, right-linear",
        ".decl edge(x: string, y: string)\n"
        ".decl b(x: string, y: string)\n"
        "b(x, z) :- edge(x, y), b(y, z).\n"
        "b(x, y) :- edge(x, y).\n" STR_EDGES, "b", ss, 2);
    build_mutual(src, sizeof(src), "string", false);
    check_types("types: issue program, a", src, "a", ss, 2);
    check_types("types: issue program, b", src, "b", ss, 2);
    check_types("types: issue program, c", src, "c", ss, 2);
    check_types("types: float tc, right-linear",
        ".decl e(x: float, y: float)\n"
        ".decl r(x: float, y: float)\n"
        "r(x, z) :- e(x, y), r(y, z).\n"
        "r(x, y) :- e(x, y).\n"
        "e(0.5, 1.5). e(1.5, 2.5).\n", "r", ff, 2);
    check_types("types: int32 head next to a string lane",
        ".decl step(t: string, m: int32, k: int32)\n"
        ".decl s(n: int32, m: int32)\n"
        ".decl b(n: int32, m: int32)\n"
        "b(n, k) :- b(n, m), step(t, m, k).\n"
        "b(n, m) :- s(n, m).\n"
        "s(0, 1).\n"
        "step(\"t\", 1, 2). step(\"t\", 2, 3).\n", "b", ii, 2);
    check_types("types: string+int32 head",
        ".decl s(x: string, n: int32, m: int32)\n"
        ".decl step(m: int32, k: int32, t: string)\n"
        ".decl b(x: string, n: int32, m: int32)\n"
        "b(x, m, k) :- b(x, n, m), step(m, k, t).\n"
        "b(x, n, m) :- s(x, n, m).\n"
        "s(\"a\", 0, 1).\n"
        "step(1, 2, \"t\"). step(2, 3, \"t\").\n", "b", sii, 3);
    check_types("types: int64 tc",
        ".decl edge(x: int64, y: int64)\n"
        ".decl b(x: int64, y: int64)\n"
        "b(x, z) :- b(x, y), edge(y, z).\n"
        "b(x, y) :- edge(x, y).\n"
        "edge(0, 1). edge(1, 2).\n", "b", ii, 2);
}

/* Rows of "result" captured by count only, for the incremental cases. */
static uint32_t inc_rows;

static void
count_result_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)row;
    (void)ncols;
    if (relation && strcmp(relation, (const char *)user_data) == 0)
        inc_rows++;
}

/* A session that evaluates an empty program first and only then receives
 * its edges: the IDB placeholders exist before any rows do. */
static void
test_incremental_step(uint32_t workers)
{
    char name[96];
    snprintf(name, sizeof(name),
        "incremental: empty step, insert, step W=%u", workers);
    tests_run++;
    printf("  [TEST] %-55s", name);
    fflush(stdout);
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: string, y: string)\n"
        ".decl b(x: string, y: string)\n"
        "b(x, z) :- b(x, y), edge(y, z).\n"
        "b(x, y) :- edge(x, y).\n", &err);
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;
    if (!prog || wl_plan_from_program(prog, &plan) != 0
        || wl_session_create(wl_backend_columnar(), plan, workers, &sess)
        != 0 || wl_session_load_facts(sess, prog) != 0) {
        tests_failed++;
        printf(" FAIL: fixture setup failed\n");
        goto out;
    }
    int step1 = wl_session_step(sess);
    const char *names[] = { "n0", "n1", "n1", "n2", "n2", "n3" };
    int64_t edges[6];
    for (int i = 0; i < 6; i++)
        edges[i] = wl_intern_put(prog->intern, names[i]);
    int ins = wl_session_insert(sess, "edge", edges, 3, 2);
    int step2 = wl_session_step(sess);
    inc_rows = 0;
    int snap = wl_session_snapshot(sess, count_result_cb, (void *)"b");
    if (step1 != 0 || ins != 0 || step2 != 0 || snap != 0 || inc_rows != 6) {
        tests_failed++;
        printf(" FAIL: step=%d insert=%d step=%d snapshot=%d rows=%u\n",
            step1, ins, step2, snap, inc_rows);
        goto out;
    }
    tests_passed++;
    printf(" PASS\n");
out:
    if (sess)
        wl_session_destroy(sess);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
}

/* Insert and remove an edge across three steps of a recursive program
 * whose recursive rule comes first.  The closure has 10, 11 and 10 rows. */
static void
test_incremental_insert_remove(uint32_t workers, bool str)
{
    char name[96];
    snprintf(name, sizeof(name), "incremental: insert/remove, %s W=%u",
        str ? "string" : "int64", workers);
    tests_run++;
    printf("  [TEST] %-55s", name);
    fflush(stdout);
    const char *type = str ? "string" : "int64";
    char src[1024];
    snprintf(src, sizeof(src),
        ".decl links(x: %s, y: %s)\n"
        ".decl seeds(x: %s, y: %s)\n"
        ".decl reach(x: %s, y: %s)\n"
        ".decl result(x: %s, y: %s)\n"
        "reach(x, z) :- reach(x, y), links(y, z), x != z.\n"
        "reach(x, y) :- seeds(x, y).\n"
        "result(x, y) :- reach(x, y).\n",
        type, type, type, type, type, type, type, type);
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;
    if (!prog || wl_plan_from_program(prog, &plan) != 0
        || wl_session_create(wl_backend_columnar(), plan, workers, &sess)
        != 0) {
        tests_failed++;
        printf(" FAIL: fixture setup failed\n");
        goto out;
    }
    int64_t v[8];
    for (int i = 0; i < 8; i++) {
        char buf[8];
        snprintf(buf, sizeof(buf), "n%d", i);
        v[i] = str ? wl_intern_put(prog->intern, buf) : i;
    }
    int64_t links[] = { v[1], v[2], v[2], v[3], v[3], v[4], v[4], v[5],
                        v[5], v[1] };
    int64_t seeds[] = { v[0], v[1], v[6], v[2] };
    int64_t extra[] = { v[4], v[6] };
    const uint32_t want[3] = { 10, 11, 10 };
    if (wl_session_insert(sess, "links", links, 5, 2) != 0
        || wl_session_insert(sess, "seeds", seeds, 2, 2) != 0) {
        tests_failed++;
        printf(" FAIL: initial insert\n");
        goto out;
    }
    for (int cycle = 0; cycle < 3; cycle++) {
        int mut = 0;
        if (cycle == 1)
            mut = wl_session_insert(sess, "links", extra, 1, 2);
        if (cycle == 2)
            mut = wl_session_remove(sess, "links", extra, 1, 2);
        int step = wl_session_step(sess);
        inc_rows = 0;
        int snap = wl_session_snapshot(sess, count_result_cb,
                (void *)"result");
        if (mut != 0 || step != 0 || snap != 0 || inc_rows != want[cycle]) {
            tests_failed++;
            printf(" FAIL: cycle %d mutate=%d step=%d snapshot=%d rows=%u"
                " (expected %u)\n", cycle, mut, step, snap, inc_rows,
                want[cycle]);
            goto out;
        }
    }
    tests_passed++;
    printf(" PASS\n");
out:
    if (sess)
        wl_session_destroy(sess);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
}

static void
test_incremental(void)
{
    for (uint32_t i = 0; i < 3; i++)
        test_incremental_step(W124[i]);
    for (uint32_t i = 0; i < 3; i++) {
        test_incremental_insert_remove(W124[i], true);
        test_incremental_insert_remove(W124[i], false);
    }
}

/* The plan records a declared schema for pure IDBs only: not for an EDB,
 * an IDB with inline facts, or an IDB without a `.decl`. */
static void
test_plan_idb_schemas(void)
{
    tests_run++;
    printf("  [TEST] %-55s", "plan: idb_schemas cover declared pure IDBs");
    fflush(stdout);
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: string, y: int32)\n"
        ".decl b(x: string, y: int32)\n"
        ".decl f(x: float, y: string)\n"
        ".decl seeded(x: string)\n"
        "b(x, y) :- edge(x, y).\n"
        "f(1.5, x) :- edge(x, _).\n"
        "seeded(x) :- edge(x, _).\n"
        "seeded(\"s\").\n"
        "u(x) :- edge(x, _).\n"
        "edge(\"a\", 1).\n", &err);
    wl_plan_t *plan = NULL;
    if (!prog || wl_plan_from_program(prog, &plan) != 0) {
        tests_failed++;
        printf(" FAIL: fixture setup failed\n");
        if (prog)
            wirelog_program_free(prog);
        return;
    }
    const wl_plan_idb_schema_t *b = NULL;
    const wl_plan_idb_schema_t *f = NULL;
    bool unexpected = false;
    for (uint32_t i = 0; i < plan->idb_schema_count; i++) {
        const wl_plan_idb_schema_t *e = &plan->idb_schemas[i];
        if (strcmp(e->name, "b") == 0)
            b = e;
        else if (strcmp(e->name, "f") == 0)
            f = e;
        else
            unexpected = true;
    }
    bool ok = !unexpected && plan->idb_schema_count == 2 && b && f
        && b->width == 2 && b->column_types
        && b->column_types[0] == WIRELOG_TYPE_STRING
        && b->column_types[1] == WIRELOG_TYPE_INT64
        && f->width == 2 && f->column_types
        && f->column_types[0] == WIRELOG_TYPE_FLOAT
        && f->column_types[1] == WIRELOG_TYPE_STRING;
    wl_plan_free(plan);
    wirelog_program_free(prog);
    if (!ok) {
        tests_failed++;
        printf(" FAIL: unexpected idb_schemas\n");
        return;
    }
    tests_passed++;
    printf(" PASS\n");
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
    test_string_heads();
    test_float_heads();
    test_idb_types();
    test_incremental();
    test_plan_idb_schemas();
    printf("\n%d tests: %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
