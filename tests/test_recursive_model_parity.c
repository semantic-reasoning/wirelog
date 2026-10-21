/*
 * test_recursive_model_parity.c - Recursive strata return the same model at
 * every worker count (Issue #2127)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Each fixture is a program from #2127 with a small data set and the model
 * written out by hand.  The model is checked at one, two and four workers,
 * with no optimizer pass, with SIP alone and with fusion, JPP and SIP, after
 * a snapshot and after a step.  One worker is not used as the oracle: for
 * some of these programs it returned a wrong model too.
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

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

static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}

#  define setenv wl_test_setenv_
#  define unsetenv wl_test_unsetenv_
#endif

extern void
wl_columnar_session_get_tdd_worker_width_stats(wl_session_t *sess,
    uint32_t *out_last_active_workers, uint32_t *out_max_active_workers);

/* ======================================================================== */
/* Test Harness                                                             */
/* ======================================================================== */

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

/* ======================================================================== */
/* Environment                                                              */
/* ======================================================================== */

/* Knobs set for every case, and knobs that would change which strategy or
 * how many workers run.  Each is saved before the cases and restored after
 * them. */
static const char *const knob_names[] = {
    "WIRELOG_TDD_MIN_ROWS_PER_WORKER",
    "WIRELOG_TDD_STRATUM_PROFILE",
    "WIRELOG_TDD_MAX_ACTIVE_WORKERS",
    "WIRELOG_TDD_GLOBAL_READ_MAX_ACTIVE_WORKERS",
    "WIRELOG_TDD_REPLICATE_W1",
    "WIRELOG_TDD_GLOBAL_READ",
};
#define KNOB_COUNT (sizeof(knob_names) / sizeof(knob_names[0]))

static char knob_saved[KNOB_COUNT][64];
static int knob_was_set[KNOB_COUNT];

static void
knobs_set(void)
{
    for (size_t k = 0; k < KNOB_COUNT; k++) {
        const char *old = getenv(knob_names[k]);
        knob_was_set[k] = old != NULL;
        if (old)
            snprintf(knob_saved[k], sizeof(knob_saved[k]), "%s", old);
        unsetenv(knob_names[k]);
    }
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
}

static void
knobs_restore(void)
{
    for (size_t k = 0; k < KNOB_COUNT; k++) {
        if (knob_was_set[k])
            setenv(knob_names[k], knob_saved[k], 1);
        else
            unsetenv(knob_names[k]);
    }
}

/* ======================================================================== */
/* Fixture runner                                                           */
/* ======================================================================== */

typedef enum {
    PASSES_NONE,
    PASSES_SIP,
    PASSES_ALL,
} passes_t;

static const char *const passes_names[] = { "none", "sip", "all" };

#define MAX_ROWS 64
#define ROW_TEXT 48
#define MAX_CAPTURES 2

typedef struct {
    char rows[MAX_ROWS][ROW_TEXT];
    uint32_t count;
    uint32_t total; /* rows delivered, including those past MAX_ROWS */
    int overflow;
} model_t;

typedef struct {
    int rc;
    model_t models[MAX_CAPTURES];
    uint32_t captures;
    const char *strategy;
    uint32_t selected_workers;
    uint32_t max_active_workers;
    uint64_t base_case_skips;
    uint64_t mat_cache_hits;
} run_t;

static void
capture_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    model_t *m = (model_t *)user_data;
    m->total++;
    if (m->count >= MAX_ROWS) {
        m->overflow = 1;
        return;
    }
    char *out = m->rows[m->count++];
    int n = snprintf(out, ROW_TEXT, "%s(", relation);
    for (uint32_t c = 0; c < ncols && n > 0 && n < ROW_TEXT; c++)
        n += snprintf(out + n, (size_t)(ROW_TEXT - n), "%s%lld",
                c ? "," : "", (long long)row[c]);
    if (n > 0 && n < ROW_TEXT)
        snprintf(out + n, (size_t)(ROW_TEXT - n), ")");
}

static int
cmp_row(const void *a, const void *b)
{
    return strcmp((const char *)a, (const char *)b);
}

/* Render @m sorted, as "r(1,2) s(3)". */
static void
model_text(model_t *m, char *buf, size_t size)
{
    size_t n = 0;
    qsort(m->rows, m->count, ROW_TEXT, cmp_row);
    buf[0] = '\0';
    for (uint32_t i = 0; i < m->count && n < size; i++) {
        int w = snprintf(buf + n, size - n, "%s%s", i ? " " : "",
                m->rows[i]);
        if (w < 0)
            break;
        n += (size_t)w;
    }
}

/* Parse "rel v v v" lines separated by ';'.  "|" marks a capture point;
 * the end of @data is always one. */
static int
insert_rows(wl_session_t *sess, const char **cursor)
{
    const char *p = *cursor;
    while (*p && *p != '|') {
        char rel[32];
        int64_t row[4];
        uint32_t n = 0;
        int off = 0;
        while (*p == ' ' || *p == ';')
            p++;
        if (!*p || *p == '|')
            break;
        if (sscanf(p, "%31s%n", rel, &off) != 1)
            return -1;
        p += off;
        long long v;
        while (n < 4 && sscanf(p, " %lld%n", &v, &off) == 1) {
            row[n++] = (int64_t)v;
            p += off;
        }
        int rc = wl_session_insert(sess, rel, row, 1, n);
        if (rc != 0)
            return rc;
    }
    if (*p == '|')
        p++;
    *cursor = p;
    return 0;
}

static run_t
run_fixture(const char *src, const char *data, passes_t passes,
    uint32_t workers, bool step)
{
    run_t out;
    wirelog_error_t err;
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;

    memset(&out, 0, sizeof(out));
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        out.rc = -1;
        return out;
    }
    if (passes == PASSES_ALL) {
        wl_fusion_apply(prog, NULL);
        wl_jpp_apply(prog, NULL);
    }
    if (passes != PASSES_NONE)
        wl_sip_apply(prog, NULL);
    out.rc = wl_plan_from_program(prog, &plan);
    plan_fixture_hold(prog);
    if (out.rc == 0)
        out.rc = wl_session_create(wl_backend_columnar(), plan, workers,
                &sess);
    const char *cursor = data;
    while (out.rc == 0 && out.captures < MAX_CAPTURES) {
        out.rc = insert_rows(sess, &cursor);
        if (out.rc == 0 && step)
            out.rc = wl_session_step(sess);
        if (out.rc == 0)
            out.rc = wl_session_snapshot(sess, capture_tuple,
                    &out.models[out.captures]);
        out.captures++;
        if (!*cursor)
            break;
    }
    if (sess) {
        wl_col_session_t *cs = COL_SESSION(sess);
        out.strategy = cs->tdd_audit.strategy;
        out.selected_workers = cs->tdd_audit.selected_workers;
        wl_columnar_session_get_tdd_worker_width_stats(sess, NULL,
            &out.max_active_workers);
        out.base_case_skips = cs->base_case_skips;
        out.mat_cache_hits = cs->mat_cache.hits;
        wl_session_destroy(sess);
    }
    if (plan)
        wl_plan_free(plan);
    return out;
}

/* Sort the space-separated rows of @expected into @buf. */
static void
expected_text(const char *expected, char *buf, size_t size)
{
    model_t m;
    const char *p = expected;
    memset(&m, 0, sizeof(m));
    while (*p && m.count < MAX_ROWS) {
        size_t len = strcspn(p, " ");
        if (len > 0 && len < ROW_TEXT) {
            memcpy(m.rows[m.count], p, len);
            m.rows[m.count][len] = '\0';
            m.count++;
        }
        p += len;
        while (*p == ' ')
            p++;
    }
    model_text(&m, buf, size);
}

static const uint32_t worker_counts[] = { 1, 2, 4 };
#define WORKER_COUNTS (sizeof(worker_counts) / sizeof(worker_counts[0]))

/* Check @src over @data against @expected, which holds one model per
 * capture separated by a vertical bar, at one, two and four workers, each
 * pass setting and both entry points. */
static void
check_fixture_modes(const char *name, const char *src, const char *data,
    const char *expected, bool with_step)
{
    char want[MAX_CAPTURES][2048];
    char got[2048];
    uint32_t want_count = 0;
    int failures = 0;

    TEST(name);
    for (const char *p = expected; want_count < MAX_CAPTURES;) {
        char one[2048];
        size_t len = strcspn(p, "|");
        if (len >= sizeof(one))
            len = sizeof(one) - 1;
        memcpy(one, p, len);
        one[len] = '\0';
        expected_text(one, want[want_count++], sizeof(want[0]));
        if (!p[len])
            break;
        p += len + 1;
    }
    for (int st = 0; st < (with_step ? 2 : 1); st++) {
        for (int ps = PASSES_NONE; ps <= PASSES_ALL; ps++) {
            for (size_t wi = 0; wi < WORKER_COUNTS; wi++) {
                run_t r = run_fixture(src, data, (passes_t)ps,
                        worker_counts[wi], st == 1);
                if (r.rc != 0 || r.captures != want_count) {
                    printf("\n    %s W=%u passes=%s: rc=%d captures=%u",
                        st ? "step" : "snapshot", worker_counts[wi],
                        passes_names[ps], r.rc, r.captures);
                    failures++;
                    continue;
                }
                for (uint32_t c = 0; c < want_count; c++) {
                    if (r.models[c].overflow) {
                        printf("\n    %s W=%u passes=%s capture %u: "
                            "more than %d rows",
                            st ? "step" : "snapshot", worker_counts[wi],
                            passes_names[ps], c, MAX_ROWS);
                        failures++;
                        continue;
                    }
                    model_text(&r.models[c], got, sizeof(got));
                    if (strcmp(got, want[c]) != 0) {
                        printf("\n    %s W=%u passes=%s capture %u:"
                            "\n      got  %s\n      want %s",
                            st ? "step" : "snapshot", worker_counts[wi],
                            passes_names[ps], c, got, want[c]);
                        failures++;
                    }
                }
            }
        }
    }
    if (failures == 0)
        PASS();
    else
        FAIL("model differs from the expected one");
}

static void
check_fixture(const char *name, const char *src, const char *data,
    const char *expected)
{
    check_fixture_modes(name, src, data, expected, true);
}

/* ======================================================================== */
/* Skip scan                                                                */
/* ======================================================================== */

/* Fake delta probe: @ctx is a NULL-terminated list of relations with new
 * rows. */
static bool
fake_has_delta(void *ctx, const char *relation_name)
{
    for (const char *const *n = (const char *const *)ctx; *n; n++) {
        if (strcmp(*n, relation_name) == 0)
            return true;
    }
    return false;
}

/* Build a plan from @spec, one op per space-separated token: "V:rel",
 * "J:rel", "S:rel", "A:rel" (a JOIN, SEMIJOIN or ANTIJOIN reading rel),
 * "JF:rel" (a FORCE_DELTA JOIN), "J:" (a JOIN with no right relation), "M",
 * "F", "C" (CONCAT), "O" (CONSOLIDATE), "X" (EXCHANGE), "R" (REDUCE), "L"
 * (LFTJ) and "K" (K_FUSION).  The VARIABLE under test is op 0. */
#define SCAN_MAX_OPS 12

typedef struct {
    wl_plan_op_t ops[SCAN_MAX_OPS];
    char names[SCAN_MAX_OPS][16];
    wl_plan_relation_t rel;
} scan_plan_t;

static void
scan_plan_build(scan_plan_t *sp, const char *spec)
{
    uint32_t n = 0;
    const char *p = spec;

    memset(sp, 0, sizeof(*sp));
    while (*p && n < SCAN_MAX_OPS) {
        char tok[24];
        size_t len = strcspn(p, " ");
        if (len >= sizeof(tok))
            len = sizeof(tok) - 1;
        memcpy(tok, p, len);
        tok[len] = '\0';
        p += len;
        while (*p == ' ')
            p++;

        wl_plan_op_t *o = &sp->ops[n];
        const char *colon = strchr(tok, ':');
        const char *name = NULL;
        if (colon) {
            snprintf(sp->names[n], sizeof(sp->names[n]), "%s", colon + 1);
            name = sp->names[n][0] ? sp->names[n] : NULL;
        }
        o->delta_mode = WL_DELTA_AUTO;
        switch (tok[0]) {
        case 'V':
            o->op = WL_PLAN_OP_VARIABLE;
            o->relation_name = name;
            break;
        case 'J':
            o->op = WL_PLAN_OP_JOIN;
            o->right_relation = name;
            if (tok[1] == 'F')
                o->delta_mode = WL_DELTA_FORCE_DELTA;
            break;
        case 'S':
            o->op = WL_PLAN_OP_SEMIJOIN;
            o->right_relation = name;
            break;
        case 'A':
            o->op = WL_PLAN_OP_ANTIJOIN;
            o->right_relation = name;
            break;
        case 'M':
            o->op = WL_PLAN_OP_MAP;
            break;
        case 'F':
            o->op = WL_PLAN_OP_FILTER;
            break;
        case 'C':
            o->op = WL_PLAN_OP_CONCAT;
            break;
        case 'O':
            o->op = WL_PLAN_OP_CONSOLIDATE;
            break;
        case 'X':
            o->op = WL_PLAN_OP_EXCHANGE;
            break;
        case 'R':
            o->op = WL_PLAN_OP_REDUCE;
            break;
        case 'L':
            o->op = WL_PLAN_OP_LFTJ;
            break;
        default:
            o->op = WL_PLAN_OP_K_FUSION;
            break;
        }
        n++;
    }
    sp->rel.name = "head";
    sp->rel.ops = sp->ops;
    sp->rel.op_count = n;
}

static void
test_skip_scan(void)
{
    static const char *const none[] = { NULL };
    static const char *const a_new[] = { "a", NULL };
    static const char *const f_new[] = { "f", NULL };
    static const char *const g_new[] = { "g", NULL };
    static const struct {
        const char *spec;
        const char *const *deltas;
        bool want;
    } cases[] = {
        /* The #367 base case: rows reach the head through MAP. */
        { "V:e M C O", none, true },
        /* A static join: no input has new rows. */
        { "V:f F J:g", none, true },
        { "V:f F J:g", g_new, false },
        /* The #2127 shape: the join reads a relation with new rows. */
        { "V:e F J:a", a_new, false },
        { "V:e F S:a", none, true },
        { "V:e F S:a", a_new, false },
        { "V:e F A:a", none, true },
        { "V:e F A:a", a_new, false },
        { "V:e F JF:g", none, false },
        { "V:e F J:", none, false },
        /* A join above the tracked item does not read it. */
        { "V:e M V:e2 J:f C O X", f_new, true },
        /* After CONCAT the merged item is followed. */
        { "V:e V:b C J:a", a_new, false },
        { "V:e V:b C J:a", none, true },
        { "V:e R C", none, false },
        { "V:e M L C", none, false },
        { "V:e M K", none, false },
    };
    int failures = 0;

    TEST("skip scan follows the VARIABLE's rows to the head");
    for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
        scan_plan_t sp;
        scan_plan_build(&sp, cases[c].spec);
        bool got = wl_columnar_eval_plan_var_skip_scan(&sp.rel, 0,
                fake_has_delta, (void *)(uintptr_t)cases[c].deltas);
        if (got != cases[c].want) {
            printf("\n    \"%s\" with deltas %s: got %d, want %d",
                cases[c].spec, cases[c].deltas[0] ? cases[c].deltas[0] : "-",
                got, cases[c].want);
            failures++;
        }
    }
    if (failures == 0)
        PASS();
    else
        FAIL("wrong scan result");
}

/* ======================================================================== */
/* Programs (verbatim from #2127)                                           */
/* ======================================================================== */

static const char program_a[]
    = ".decl e0(c0: int64, c1: int64, c2: int64)\n"
    ".decl e1(c0: int64, c1: int64)\n"
    ".decl e2(c0: int64, c1: int64, c2: int64)\n"
    ".decl e3(c0: int64, c1: int64, c2: int64)\n"
    ".decl l0(c0: int64, c1: int64)\n"
    ".decl a(c0: int64, c1: int64)\n"
    ".decl b(c0: int64, c1: int64)\n"
    "l0(x0,x1) :- e3(x0,x1,x0).\n"
    "a(x0,x0) :- e0(x1,x0,y0), e1(x1,x2), x1 < x0.\n"
    "b(x3,x0) :- e3(x1,x0,0), e1(x2,x1), l0(x2,x3).\n"
    "b(x2,x2) :- e0(x0,x1,x0), a(x1,x2), !e3(x1,x2,x0).\n"
    "a(x0,x3) :- b(x0,x1), e0(x1,x2,1), e2(x0,x3,x2), !e3(x0,x0,x0).\n"
    "b(x0,x1) :- a(x0,x1), e3(x1,x1,x2).\n";

static const char program_c[]
    = ".decl e0(c0: int64, c1: int64, c2: int64)\n"
    ".decl e1(c0: int64, c1: int64)\n"
    ".decl e2(c0: int64, c1: int64)\n"
    ".decl e3(c0: int64, c1: int64)\n"
    ".decl a(c0: int64, c1: int64)\n"
    ".decl b(c0: int64, c1: int64)\n"
    "a(x1,x0) :- e3(x0,x1).\n"
    "a(x1,x0) :- e0(x0,3,2), e2(x1,x2).\n"
    "b(x0,x3) :- e2(x1,x0), e3(x1,x2), e1(4,x3).\n"
    "b(x1,x1) :- e0(1,x1,y0), e2(x1,x2), a(x2,x3).\n"
    "a(x0,x3) :- b(x0,x1), e1(x2,x1), e0(x3,x2,x1).\n"
    "b(x2,x2) :- b(x0,x1), e3(x1,x2).\n";

static const char program_d[]
    = ".decl e0(c0: int64)\n"
    ".decl e1(c0: int64, c1: int64)\n"
    ".decl e2(c0: int64, c1: int64, c2: int64)\n"
    ".decl l0(c0: int64, c1: int64)\n"
    ".decl a(c0: int64, c1: int64)\n"
    ".decl b(c0: int64, c1: int64)\n"
    "l0(x0,x1) :- e1(x0,x1).\n"
    "a(x0,x0) :- e0(x0), !l0(x0,x0).\n"
    "a(x2,x2) :- e1(x0,x1), e1(x2,x1).\n"
    "b(x1,x1) :- l0(x0,x1), !e1(x0,x1).\n"
    "b(x0,x0) :- e1(x0,x1), l0(x1,x2), !l0(x1,x1), x0 < x1.\n"
    "b(x0,x3) :- l0(x0,x1), a(x2,x1), l0(x2,x3), x2 != x1.\n"
    "b(x0,x3) :- e2(x0,x1,2), e1(x1,x2), a(x2,x3).\n"
    "a(x2,x2) :- a(x0,x1), e1(x2,x1), !e2(x1,x1,x1).\n"
    "a(x2,x0) :- e1(x1,x0), b(x2,x1), x1 != x2.\n";

/* A program on which an earlier form of the #2127 fix made a step at two
 * workers return EINVAL. */
static const char program_m63[]
    = ".decl e0(c0: int64, c1: int64, c2: int64)\n"
    ".decl e1(c0: int64, c1: int64)\n"
    ".decl e2(c0: int64, c1: int64, c2: int64)\n"
    ".decl e3(c0: int64, c1: int64)\n"
    ".decl a(c0: int64, c1: int64)\n"
    ".decl b(c0: int64, c1: int64)\n"
    ".decl l0(c0: int64, c1: int64)\n"
    "b(x0,x2) :- e1(x0,x0), e3(x1,x2).\n"
    "b(x0,x0) :- l0(x0,x1), a(x2,x1), !e2(x0,x1,x0).\n"
    "a(x1,x0) :- b(x0,x1), b(x1,x2), a(x3,x4), !l0(x4,x3).\n";

/* Two rules join the same inputs on different columns. */
static const char program_b[]
    = ".decl e0(c0: int64, c1: int64, c2: int64)\n"
    ".decl e1(c0: int64, c1: int64, c2: int64)\n"
    ".decl e2(c0: int64, c1: int64)\n"
    ".decl e3(c0: int64, c1: int64)\n"
    ".decl a(c0: int64, c1: int64)\n"
    ".decl z(c0: int64)\n"
    "a(x3,x3) :- e0(x1,x0,0), e1(x2,x1,x1), e3(x2,x3).\n"
    "a(x2,x0) :- e0(x1,x0,y0), e2(x2,x1).\n"
    "a(x2,x2) :- a(x0,x1), e3(x1,x2).\n"
    "a(x0,x3) :- e2(x0,x1), e3(x1,x2), a(x2,x3).\n"
    "a(x2,x2) :- a(x1,x0), a(x1,x2), x0 != x1.\n"
    "a(x0,x2) :- a(x0,x1), a(x2,x1), !e2(x1,x2).\n"
    "z(x0) :- e2(x0,x0), !a(x0,x0).\n";

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/* A rule whose first atom has a constant or a repeated variable reads an
 * EDB relation through a FILTER before it joins the recursive relation.
 * After the first iteration such a rule was skipped although the relation
 * it joins had new rows, so the rows it derives from them were lost. */
static void
test_rule_joining_new_rows_after_filter(void)
{
    check_fixture("A: repeated variable, then a recursive join",
        program_a, "e0 0 18 0; e1 0 5", "a(18,18) b(18,18)");
    check_fixture("C: constant, then a recursive join", program_c,
        "e0 1 11 19; e2 11 17; e3 19 17", "a(17,19) b(11,11)");
    check_fixture("D: constant, then a recursive join", program_d,
        "e1 7 1; e1 0 7; e2 11 0 2",
        "a(0,0) a(11,1) a(7,7) b(0,0) b(11,7) l0(0,7) l0(7,1)");
    check_fixture("D1: the same loss at one worker", program_d,
        "e1 11 15; e1 15 24; e1 7 1; e1 0 7; e2 0 11 2",
        "a(0,0) a(0,24) a(11,11) a(15,1) a(15,15) a(7,7) "
        "b(0,0) b(0,1) b(0,15) b(11,11) b(15,7) b(7,24) "
        "l0(0,7) l0(11,15) l0(15,24) l0(7,1)");
}

/* Rules 5 and 6 of program B join a delta of a with a on different
 * columns.  A cached join result is reused only for the same join. */
static void
test_cached_join_matches_its_operation(void)
{
    check_fixture("B: two joins of the same inputs on different columns",
        program_b, "e0 4 23 11; e2 21 4",
        "a(21,21) a(21,23) a(23,21) a(23,23)");
    check_fixture("B2: the same rows inserted in two steps", program_b,
        "e0 4 23 11|e2 21 4", "|a(21,21) a(21,23) a(23,21) a(23,23)");
}

/* Two occurrences of the same join still share a cached result.  Only the
 * snapshot path is checked: a step at two and at four workers returns
 * EINVAL on this program on main as well, where p has no columns before its
 * first evaluation. */
static void
test_same_join_still_shares_cache(void)
{
    static const char src[]
        = ".decl e(x: int64, y: int64)\n"
        ".decl p(x: int64, y: int64)\n"
        "p(x,y) :- e(x,y).\n"
        "p(x,w) :- p(x,y), p(y,z), p(z,w).\n";
    static const char data[] = "e 1 2; e 2 3; e 3 4; e 4 5; e 5 6; e 6 7";

    check_fixture_modes("same join: model", src, data,
        "p(1,2) p(1,4) p(1,6) p(2,3) p(2,5) p(2,7) p(3,4) p(3,6) "
        "p(4,5) p(4,7) p(5,6) p(6,7)", false);
    TEST("same join: the cache is hit at one worker");
    run_t r = run_fixture(src, data, PASSES_NONE, 1, false);
    if (r.rc != 0) {
        printf("\n    rc=%d", r.rc);
        FAIL("snapshot failed");
    } else if (r.mat_cache_hits == 0) {
        FAIL("no materialization cache hit");
    } else {
        PASS();
    }
}

/* Joins that filter their right relation differently are not confused. */
static void
test_right_filter_guard(void)
{
    static const char src[]
        = ".decl e(c0: int64, c1: int64)\n"
        ".decl a(c0: int64, c1: int64)\n"
        "a(x,y) :- e(x,y).\n"
        "a(x,x) :- a(x,y), a(y,3), !e(y,x).\n"
        "a(y,x) :- a(x,y), a(y,4), !e(x,x).\n";

    check_fixture("right filter: two filtered joins of a", src,
        "e 3 5; e 6 2; e 3 0; e 1 2; e 3 3; e 6 0; e 7 7; e 7 6; e 7 3; "
        "e 6 1; e 7 3; e 0 4; e 6 7; e 6 1",
        "a(0,4) a(0,6) a(1,2) a(3,0) a(3,3) a(3,5) a(6,0) a(6,1) a(6,2) "
        "a(6,7) a(7,3) a(7,6) a(7,7)");
}

static void
test_step_stays_successful(void)
{
    check_fixture("m63: a step at every worker count succeeds",
        program_m63,
        "e0 4 2 0; e0 0 5 4; e0 5 3 6; e1 0 1; e1 6 6; e2 6 6 5; "
        "e3 6 2; e3 0 2",
        "b(6,2)");
}

/* A rule that reads only relations with no new rows is still skipped after
 * the first iteration. */
static void
test_static_rule_still_skipped(void)
{
    static const char src[]
        = ".decl e(x: int64, y: int64)\n"
        ".decl f(x: int64, c: int64, z: int64)\n"
        ".decl g(z: int64, y: int64)\n"
        ".decl p(x: int64, y: int64)\n"
        "p(x,y) :- e(x,y).\n"
        "p(x,z) :- p(x,y), e(y,z).\n"
        "p(x,y) :- f(x,1,z), g(z,y).\n";
    char data[4096];
    size_t n = 0;

    TEST("static rule in a recursive relation is skipped after iteration 0");
    for (int i = 0; i < 30; i++)
        n += (size_t)snprintf(data + n, sizeof(data) - n, "e %d %d;", i,
                i + 1);
    for (int i = 100; i < 150; i++)
        n += (size_t)snprintf(data + n, sizeof(data) - n,
                "f %d 1 %d; g %d %d;", i, i, i, i + 1000);

    /* One worker: each worker session counts its own skips, so only a
     * one-worker session reports the count for the whole evaluation. */
    run_t r = run_fixture(src, data, PASSES_NONE, 1, false);
    /* 30 * 31 / 2 chain pairs plus one p(i, i + 1000) per f row.  The static
     * rule is skipped once at each iteration after the first; 30 is the
     * count this data gives. */
    if (r.rc != 0)
        FAIL("evaluation failed");
    else if (r.models[0].total != 465 + 50) {
        printf("\n    rows=%u, want %d", r.models[0].total, 465 + 50);
        FAIL("wrong model");
    } else if (r.base_case_skips != 30) {
        printf("\n    skips=%llu, want 30",
            (unsigned long long)r.base_case_skips);
        FAIL("the static rule was not skipped at each later iteration");
    } else {
        PASS();
    }
}

/* The fixtures above are evaluated by the parallel strategy they target:
 * a check that only passed because the stratum ran on one worker would
 * test nothing about #2127. */
static void
test_parallel_strategy_runs(void)
{
    static const struct {
        const char *src;
        const char *data;
    } cases[] = {
        { program_a, "e0 0 18 0; e1 0 5" },
        { program_c, "e0 1 11 19; e2 11 17; e3 19 17" },
        { program_d, "e1 7 1; e1 0 7; e2 11 0 2" },
    };
    int failures = 0;

    TEST("A, C and D run global_read on two or more workers");
    for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
        for (size_t wi = 1; wi < WORKER_COUNTS; wi++) {
            run_t r = run_fixture(cases[c].src, cases[c].data,
                    PASSES_NONE, worker_counts[wi], false);
            if (r.rc != 0 || !r.strategy
                || strcmp(r.strategy, "global_read") != 0
                || r.selected_workers < 2 || r.max_active_workers < 2) {
                printf("\n    case %zu W=%u: rc=%d strategy=%s selected=%u "
                    "max_active=%u",
                    c, worker_counts[wi], r.rc,
                    r.strategy ? r.strategy : "(none)", r.selected_workers,
                    r.max_active_workers);
                failures++;
            }
        }
    }
    if (failures == 0)
        PASS();
    else
        FAIL("a fixture did not run in parallel");
}

int
main(void)
{
    printf("test_recursive_model_parity (Issue #2127)\n");
    knobs_set();
    test_skip_scan();
    test_rule_joining_new_rows_after_filter();
    test_cached_join_matches_its_operation();
    test_same_join_still_shares_cache();
    test_right_filter_guard();
    test_step_stays_successful();
    test_static_rule_still_skipped();
    test_parallel_strategy_runs();
    knobs_restore();
    printf("\n%d tests: %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
