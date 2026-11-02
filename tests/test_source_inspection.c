/*
 * test_source_inspection.c - host source inspection (Issue #2321).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Every count below comes from real public inserts, removes and evaluation;
 * no test assigns a witness count.  The one arithmetic seam (the count bias)
 * only reaches the overflow branch and is not source-semantic evidence.
 */

#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#ifndef _WIN32
#define _POSIX_C_SOURCE 200809L
#endif

#include "wirelog/wirelog-advanced.h"
#include "wirelog/wirelog.h"
#include "test_tmpdir.h"
#include "wirelog/columnar/internal.h"
#include "wirelog/columnar/memory_governor.h"
#include "wirelog/evaluation_control.h"
#include "wirelog/ir/program.h"
#include "wirelog/session.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#else
#include <unistd.h>
#endif

wl_session_t *wirelog_session_testhook_inner(wirelog_session_t *session);
extern uint64_t wl_columnar_source_query_testhook_count_bias;

static int failures;

#define CHECK(cond, ...)                                                   \
        do {                                                               \
            if (!(cond)) {                                                 \
                fprintf(stderr, "%s:%d: CHECK(%s) failed: ", __FILE__,     \
                    __LINE__, #cond);                                      \
                fprintf(stderr, __VA_ARGS__);                              \
                fputc('\n', stderr);                                       \
                failures++;                                                \
            }                                                              \
        } while (0)

#define ALL UINT64_MAX

static wirelog_program_t *
parse(const char *src)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        fprintf(stderr, "parse failed (%d):\n%s\n", err, src);
        exit(2);
    }
    return prog;
}

static wirelog_session_t *
open_session(wirelog_program_t *prog, uint32_t workers)
{
    wirelog_session_t *s = NULL;
    wirelog_error_t err = wirelog_session_create(prog,
            WIRELOG_BACKEND_COLUMNAR, workers, &s);
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "create failed (%d)\n", err);
        exit(2);
    }
    return s;
}

/* Count of @tuple, or UINT64_MAX - status when the query fails. */
static uint64_t
count1(wirelog_session_t *s, const char *rel, int64_t v)
{
    uint64_t n = 99;
    wirelog_source_query_status_t st = wirelog_session_source_count(s, rel,
            &v, 1, ALL, &n);
    return st == WIRELOG_SOURCE_QUERY_OK ? n : UINT64_MAX - (uint64_t)st;
}

static uint64_t
count2(wirelog_session_t *s, const char *rel, int64_t a, int64_t b)
{
    int64_t t[2] = { a, b };
    uint64_t n = 99;
    wirelog_source_query_status_t st = wirelog_session_source_count(s, rel,
            t, 2, ALL, &n);
    return st == WIRELOG_SOURCE_QUERY_OK ? n : UINT64_MAX - (uint64_t)st;
}

static void
ins1(wirelog_session_t *s, const char *rel, int64_t v)
{
    wirelog_error_t err = wirelog_session_insert(s, rel, &v, 1, 1);
    CHECK(err == WIRELOG_OK, "insert %s(%lld) err=%d", rel, (long long)v,
        err);
}

static void
rem1(wirelog_session_t *s, const char *rel, int64_t v)
{
    wirelog_error_t err = wirelog_session_remove(s, rel, &v, 1, 1);
    CHECK(err == WIRELOG_OK, "remove %s(%lld) err=%d", rel, (long long)v,
        err);
}

static void
step(wirelog_session_t *s)
{
    wirelog_error_t err = wirelog_session_step(s);
    CHECK(err == WIRELOG_OK, "step err=%d", err);
}

/* ---- delta recording, to compare an inspected session with a twin ---- */

struct delta_log {
    char buf[4096];
    size_t len;
    wirelog_session_t *reenter; /* query from inside the callback */
    wirelog_source_query_status_t reenter_status;
    uint32_t calls;
};

static void
record_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    struct delta_log *log = (struct delta_log *)user_data;
    log->calls++;
    if (log->len < sizeof(log->buf)) {
        int n = snprintf(log->buf + log->len, sizeof(log->buf) - log->len,
                "%s(%lld%s)%+d;", relation, (long long)row[0],
                ncols > 1 ? ",.." : "", (int)diff);
        if (n > 0)
            log->len += (size_t)n;
    }
    if (log->reenter) {
        uint64_t n = 7;
        int64_t v = row[0];
        log->reenter_status = wirelog_session_source_count(log->reenter,
                "e", &v, 1, ALL, &n);
        if (n != 0)
            log->reenter_status = (wirelog_source_query_status_t)99;
    }
}

static void
record_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    record_delta(relation, row, ncols, 1, user_data);
}

static const char *EDB_HEAD_SRC =
    ".decl e(x: int64)\n"
    ".decl h(x: int64)\n"
    "h(X) :- e(X).\n";

/* ======================================================================== */
/* Exact transitions                                                        */
/* ======================================================================== */

static void
test_edb_transitions(uint32_t workers, bool with_cb)
{
    wirelog_program_t *prog = parse(EDB_HEAD_SRC);
    wirelog_session_t *s = open_session(prog, workers);
    struct delta_log log = { 0 };
    if (with_cb)
        CHECK(wirelog_session_set_delta_cb(s, record_delta, &log)
            == WIRELOG_OK, "set cb");

    CHECK(count1(s, "e", 7) == 0, "w=%u cb=%d start", workers, with_cb);
    ins1(s, "e", 7);
    CHECK(count1(s, "e", 7) == 1, "pending insert counts");
    step(s);
    CHECK(count1(s, "e", 7) == 1, "w=%u cb=%d after 1", workers, with_cb);
    ins1(s, "e", 7);
    step(s);
    CHECK(count1(s, "e", 7) == 2, "w=%u cb=%d duplicate survives step: %llu",
        workers, with_cb, (unsigned long long)count1(s, "e", 7));
    step(s);
    CHECK(count1(s, "e", 7) == 2, "duplicate survives a second step");
    rem1(s, "e", 7);
    CHECK(count1(s, "e", 7) == 1, "pending remove applies");
    step(s);
    CHECK(count1(s, "e", 7) == 1, "w=%u cb=%d after remove", workers,
        with_cb);
    rem1(s, "e", 7);
    step(s);
    CHECK(count1(s, "e", 7) == 0, "w=%u cb=%d back to zero", workers,
        with_cb);
    /* No-match removal is a successful no-op and changes nothing. */
    rem1(s, "e", 7);
    step(s);
    CHECK(count1(s, "e", 7) == 0, "no-match remove");
    /* The model never feeds back into the source. */
    CHECK(count1(s, "h", 7) == 0, "derived-only head tuple counts 0");
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
}

static void
test_head_transitions(uint32_t workers, bool with_cb)
{
    wirelog_program_t *prog = parse(EDB_HEAD_SRC);
    wirelog_session_t *s = open_session(prog, workers);
    struct delta_log log = { 0 };
    if (with_cb)
        CHECK(wirelog_session_set_delta_cb(s, record_delta, &log)
            == WIRELOG_OK, "set cb");
    step(s); /* registers the head */

    CHECK(count1(s, "h", 5) == 0, "head start");
    ins1(s, "h", 5);
    step(s);
    CHECK(count1(s, "h", 5) == 1, "w=%u cb=%d head 1", workers, with_cb);
    ins1(s, "h", 5);
    step(s);
    CHECK(count1(s, "h", 5) == 2, "w=%u cb=%d head 2: %llu", workers,
        with_cb, (unsigned long long)count1(s, "h", 5));
    /* Derive the same tuple: the source still counts only host inserts. */
    ins1(s, "e", 5);
    ins1(s, "e", 6);
    step(s);
    CHECK(count1(s, "h", 5) == 2, "inserted+derived counts inserted only");
    CHECK(count1(s, "h", 6) == 0, "derived-only counts 0");
    rem1(s, "h", 5);
    step(s);
    CHECK(count1(s, "h", 5) == 1, "w=%u cb=%d head back to 1", workers,
        with_cb);
    rem1(s, "h", 5);
    step(s);
    CHECK(count1(s, "h", 5) == 0, "w=%u cb=%d head back to 0", workers,
        with_cb);
    /* Removing a derived-only tuple matches no input row. */
    rem1(s, "h", 6);
    step(s);
    CHECK(count1(s, "h", 6) == 0, "derived-only remove");
    CHECK(count1(s, "e", 5) == 1 && count1(s, "e", 6) == 1, "edb intact");
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
}

static void
test_string_and_wide(void)
{
    wirelog_program_t *prog = parse(
        ".decl e(a: symbol, b: int64)\n"
        ".decl h(a: symbol, b: int64)\n"
        "h(A, B) :- e(A, B).\n");
    wirelog_session_t *s = open_session(prog, 1);
    step(s);
    int64_t id = wl_intern_put(prog->intern, "alpha");
    CHECK(id >= 0, "intern");
    int64_t t[2] = { id, 3 };
    CHECK(wirelog_session_insert(s, "e", t, 1, 2) == WIRELOG_OK, "ins e");
    CHECK(wirelog_session_insert(s, "h", t, 1, 2) == WIRELOG_OK, "ins h");
    CHECK(wirelog_session_insert(s, "h", t, 1, 2) == WIRELOG_OK, "ins h");
    step(s);
    CHECK(count2(s, "e", id, 3) == 1, "string edb");
    CHECK(count2(s, "h", id, 3) == 2, "string head");
    CHECK(count2(s, "h", id, 4) == 0, "other value");
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
}

/* ======================================================================== */
/* Origin                                                                   */
/* ======================================================================== */

static void
test_inline_fact_origin(void)
{
    wirelog_program_t *prog = parse(
        ".decl f(x: int64)\n"
        ".decl g(x: int64)\n"
        ".decl h(x: int64)\n"
        "f(1).\n"
        "h(X) :- f(X).\n"
        "h(X) :- g(X).\n");
    wirelog_session_t *s = open_session(prog, 1);
    uint64_t n = 5;
    int64_t one = 1;
    CHECK(wirelog_session_source_count(s, "f", &one, 1, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNSUPPORTED && n == 0, "fact relation");
    /* Facts on f do not disqualify g or the head h. */
    CHECK(count1(s, "g", 1) == 0, "unrelated relation allowed");
    CHECK(count1(s, "h", 1) == 0, "head of a fact rule allowed");
    /* Identical host insert, then removal of every copy: still refused. */
    ins1(s, "f", 1);
    rem1(s, "f", 1);
    rem1(s, "f", 1);
    step(s);
    CHECK(count1(s, "f", 1)
        == UINT64_MAX - WIRELOG_SOURCE_QUERY_UNSUPPORTED, "after removal");

    /* Editing the borrowed program cannot certify f afterwards. */
    wl_ir_relation_info_t *rel = NULL;
    for (uint32_t i = 0; i < prog->relation_count; i++)
        if (strcmp(prog->relations[i].name, "f") == 0)
            rel = &prog->relations[i];
    CHECK(rel != NULL, "find f");
    if (rel) {
        int64_t *saved_data = rel->fact_data;
        uint32_t saved_count = rel->fact_count;
        rel->fact_data = NULL;
        rel->fact_count = 0;
        CHECK(count1(s, "f", 1)
            == UINT64_MAX - WIRELOG_SOURCE_QUERY_UNSUPPORTED,
            "borrowed metadata edit must not certify");
        rel->fact_data = saved_data;
        rel->fact_count = saved_count;
    }
    /* A fact added to the borrowed program after creation is never
     * attributable to host calls: the live metadata is checked too. */
    wl_ir_relation_info_t *g = NULL;
    for (uint32_t i = 0; i < prog->relation_count; i++)
        if (strcmp(prog->relations[i].name, "g") == 0)
            g = &prog->relations[i];
    CHECK(g != NULL, "find g");
    if (g) {
        int64_t fact = 1;
        g->fact_data = &fact;
        g->fact_count = 1;
        CHECK(count1(s, "g", 1)
            == UINT64_MAX - WIRELOG_SOURCE_QUERY_UNSUPPORTED,
            "fact added after creation");
        g->fact_data = NULL;
        g->fact_count = 0;
        CHECK(count1(s, "g", 1) == 0, "g restored");
    }
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
}

/* ======================================================================== */
/* Arguments, identity and schema                                           */
/* ======================================================================== */

static void
test_errors(void)
{
    wirelog_program_t *prog = parse(
        ".decl e(x: int64)\n"
        ".decl s(x: symbol, y: int64)\n"
        ".decl fl(x: int64, y: float)\n"
        ".decl i32(x: int32)\n"
        ".decl cmp(id: int64, p: pair/2 inline)\n"
        ".decl side(id: int64, p: pair/2)\n"
        ".decl z()\n"
        ".decl h(x: int64)\n"
        "h(X) :- e(X).\n");
    wirelog_session_t *s = open_session(prog, 1);
    int64_t t[3] = { 1, 2, 3 };
    uint64_t n = 5;

#define ST(call) (call)
    CHECK(ST(wirelog_session_source_count(NULL, "e", t, 1, ALL, &n))
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT && n == 0, "null session");
    n = 5;
    CHECK(wirelog_session_source_count(s, NULL, t, 1, ALL, &n)
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT && n == 0, "null rel");
    CHECK(wirelog_session_source_count(s, "e", NULL, 1, ALL, &n)
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT, "null tuple");
    CHECK(wirelog_session_source_count(s, "e", t, 1, ALL, NULL)
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT, "null out");
    CHECK(wirelog_session_source_count(s, "e", t, 0, ALL, &n)
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT, "zero arity");
    CHECK(wirelog_session_source_count(s, "e", t, 2, ALL, &n)
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT, "wrong arity");
    CHECK(wirelog_session_source_count(s, "nope", t, 1, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNKNOWN_RELATION, "unknown");
    CHECK(wirelog_session_source_count(s, "$in$h", t, 1, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNKNOWN_RELATION, "private shadow");
    CHECK(wirelog_session_source_count(s, "__compound_pair_2", t, 1, ALL,
        &n) == WIRELOG_SOURCE_QUERY_UNKNOWN_RELATION, "private compound");
    /* Unknown wins over a wrong arity; schema wins over arity. */
    CHECK(wirelog_session_source_count(s, "nope", t, 3, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNKNOWN_RELATION, "unknown before arity");
    CHECK(wirelog_session_source_count(s, "fl", t, 3, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNSUPPORTED, "float schema before arity");
    CHECK(wirelog_session_source_count(s, "fl", t, 2, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNSUPPORTED, "float lane");
    CHECK(wirelog_session_source_count(s, "i32", t, 1, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNSUPPORTED, "int32 lane");
    CHECK(wirelog_session_source_count(s, "cmp", t, 3, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNSUPPORTED, "inline compound");
    CHECK(wirelog_session_source_count(s, "side", t, 2, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNSUPPORTED, "side compound");
    CHECK(wirelog_session_source_count(s, "z", t, 1, ALL, &n)
        == WIRELOG_SOURCE_QUERY_UNSUPPORTED, "zero width");
    /* STRING lanes must name an intern entry; an integer key does not make
     * the other lanes safe. */
    int64_t bad[2] = { 1000000, 1 };
    CHECK(wirelog_session_source_count(s, "s", bad, 2, ALL, &n)
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT, "bad string id");
    int64_t neg[2] = { -1, 1 };
    CHECK(wirelog_session_source_count(s, "s", neg, 2, ALL, &n)
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT, "negative string id");
#undef ST
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
}

/* ======================================================================== */
/* Work limits and overflow                                                 */
/* ======================================================================== */

static void
test_work_limits(void)
{
    wirelog_program_t *prog = parse(EDB_HEAD_SRC);
    wirelog_session_t *s = open_session(prog, 1);
    int64_t v = 3;
    uint64_t n = 9;

    /* Empty source: zero rows needed. */
    CHECK(wirelog_session_source_count(s, "e", &v, 1, 0, &n)
        == WIRELOG_SOURCE_QUERY_OK && n == 0, "empty with limit 0");
    int64_t rows[4] = { 1, 3, 3, 2 };
    CHECK(wirelog_session_insert(s, "e", rows, 4, 1) == WIRELOG_OK, "batch");
    step(s);
    n = 9;
    CHECK(wirelog_session_source_count(s, "e", &v, 1, 0, &n)
        == WIRELOG_SOURCE_QUERY_WORK_LIMIT && n == 0, "limit 0 nonempty");
    n = 9;
    CHECK(wirelog_session_source_count(s, "e", &v, 1, 3, &n)
        == WIRELOG_SOURCE_QUERY_WORK_LIMIT && n == 0, "one short");
    CHECK(wirelog_session_source_count(s, "e", &v, 1, 4, &n)
        == WIRELOG_SOURCE_QUERY_OK && n == 2, "exact end: %llu",
        (unsigned long long)n);
    CHECK(wirelog_session_source_count(s, "e", &v, 1, 5, &n)
        == WIRELOG_SOURCE_QUERY_OK && n == 2, "ample");

    /* Overflow branch through the arithmetic seam. */
    wl_columnar_source_query_testhook_count_bias = UINT64_MAX - 1;
    n = 9;
    CHECK(wirelog_session_source_count(s, "e", &v, 1, ALL, &n)
        == WIRELOG_SOURCE_QUERY_OVERFLOW && n == 0, "count overflow");
    wl_columnar_source_query_testhook_count_bias = 0;
    wirelog_session_destroy(s);
    wirelog_program_free(prog);

    /* A head name too long for its `$in$` shadow name. */
    char name[300];
    memset(name, 'q', 260);
    name[260] = '\0';
    char src[1024];
    snprintf(src, sizeof(src),
        ".decl e(x: int64)\n.decl %s(x: int64)\n%s(X) :- e(X).\n", name,
        name);
    prog = parse(src);
    s = open_session(prog, 1);
    n = 9;
    CHECK(wirelog_session_source_count(s, name, &v, 1, ALL, &n)
        == WIRELOG_SOURCE_QUERY_OVERFLOW && n == 0, "long head name");
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
}

/* ======================================================================== */
/* Admission                                                                */
/* ======================================================================== */

static void
test_callback_reentry(void)
{
    wirelog_program_t *prog = parse(EDB_HEAD_SRC);
    wirelog_session_t *s = open_session(prog, 1);
    wirelog_program_t *prog2 = parse(EDB_HEAD_SRC);
    wirelog_session_t *twin = open_session(prog2, 1);
    struct delta_log log = { 0 };
    struct delta_log twin_log = { 0 };
    log.reenter = s;
    CHECK(wirelog_session_set_delta_cb(s, record_delta, &log) == WIRELOG_OK,
        "cb");
    CHECK(wirelog_session_set_delta_cb(twin, record_delta, &twin_log)
        == WIRELOG_OK, "twin cb");
    ins1(s, "e", 4);
    ins1(twin, "e", 4);
    step(s);
    step(twin);
    CHECK(log.calls > 0, "callback ran");
    CHECK(log.reenter_status == WIRELOG_SOURCE_QUERY_BUSY,
        "delta callback query is BUSY (%d)", log.reenter_status);
    CHECK(log.len == twin_log.len
        && memcmp(log.buf, twin_log.buf, log.len) == 0,
        "outer step output unchanged: [%.*s] vs [%.*s]", (int)log.len,
        log.buf, (int)twin_log.len, twin_log.buf);
    CHECK(count1(s, "e", 4) == 1, "query after callback");

    /* Snapshot callback. */
    struct delta_log snap = { 0 };
    snap.reenter = s;
    CHECK(wirelog_session_snapshot(s, record_tuple, &snap) == WIRELOG_OK,
        "snapshot");
    CHECK(snap.calls > 0 && snap.reenter_status == WIRELOG_SOURCE_QUERY_BUSY,
        "snapshot callback query is BUSY (%d)", snap.reenter_status);

    /* Callback removal and replacement keep the source answerable. */
    CHECK(wirelog_session_set_delta_cb(s, NULL, NULL) == WIRELOG_OK, "clear");
    CHECK(wirelog_session_set_delta_cb(twin, NULL, NULL) == WIRELOG_OK,
        "twin clear");
    CHECK(count1(s, "e", 4) == 1, "after callback removal");
    ins1(s, "e", 4);
    ins1(twin, "e", 4);
    step(s);
    step(twin);
    CHECK(count1(s, "e", 4) == 2, "plain step after removal");
    log.len = 0;
    twin_log.len = 0;
    log.reenter = NULL;
    CHECK(wirelog_session_set_delta_cb(s, record_delta, &log) == WIRELOG_OK,
        "replace");
    CHECK(wirelog_session_set_delta_cb(twin, record_delta, &twin_log)
        == WIRELOG_OK, "twin replace");
    CHECK(count1(s, "e", 4) == 2, "after replacement");
    rem1(s, "e", 4);
    rem1(twin, "e", 4);
    CHECK(count1(s, "e", 4) == 1, "pending remove under callback");
    step(s);
    step(twin);
    CHECK(log.len == twin_log.len
        && memcmp(log.buf, twin_log.buf, log.len) == 0,
        "replacement deltas match twin: [%.*s] vs [%.*s]", (int)log.len,
        log.buf, (int)twin_log.len, twin_log.buf);
    wirelog_session_destroy(s);
    wirelog_session_destroy(twin);
    wirelog_program_free(prog);
    wirelog_program_free(prog2);
}

static void
test_queries_change_nothing(void)
{
    wirelog_program_t *prog = parse(EDB_HEAD_SRC);
    wirelog_session_t *s = open_session(prog, 1);
    wl_col_session_t *cs = COL_SESSION(wirelog_session_testhook_inner(s));
    step(s);
    ins1(s, "e", 1);
    ins1(s, "e", 1);
    ins1(s, "h", 2);

    col_rel_t *e = session_find_rel(cs, "e");
    CHECK(e != NULL, "e registered");
    uint32_t buckets = cs->rel_hash_nbuckets;
    uint32_t epoch = cs->outer_epoch;
    bool pending = cs->pending_input_change;
    uint64_t gen = e ? e->view_generation : 0;
    uint32_t nrels = cs->nrels;
    uint32_t interned = wl_intern_count(prog->intern);
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(cs->memory_governor));

    for (int i = 0; i < 3; i++) {
        CHECK(count1(s, "e", 1) == 2, "pending edb");
        CHECK(count1(s, "h", 2) == 1, "pending head");
        CHECK(count1(s, "h", 9) == 0, "absent");
    }
    CHECK(cs->rel_hash_nbuckets == buckets, "hash untouched");
    CHECK(cs->outer_epoch == epoch, "epoch untouched");
    CHECK(cs->pending_input_change == pending, "pending flag untouched");
    CHECK(!e || e->view_generation == gen, "generation untouched");
    CHECK(cs->nrels == nrels, "no relation registered");
    CHECK(wl_intern_count(prog->intern) == interned, "intern untouched");
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(cs->memory_governor)) ==
        reserved,
        "no governed reservation left behind");
    step(s);
    CHECK(count1(s, "e", 1) == 2 && count1(s, "h", 2) == 1, "after step");

    /* With no relation hash built, a query must not build one. */
    session_rel_free_hash(cs);
    CHECK(cs->rel_hash_nbuckets == 0, "hash dropped");
    CHECK(count1(s, "e", 1) == 2 && count1(s, "h", 2) == 1,
        "linear lookup answers");
    CHECK(cs->rel_hash_nbuckets == 0, "query built no hash");
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
}

static void
test_sealed_cold_and_controlled(void)
{
    char dir[1024];
#ifdef _WIN32
    snprintf(dir, sizeof(dir), "%s/wirelog-source-inspect-%lu", test_tmpdir(),
        (unsigned long)GetCurrentProcessId());
    CHECK(_mkdir(dir) == 0, "mkdir");
#else
    snprintf(dir, sizeof(dir), "%s/wirelog-source-inspect-XXXXXX",
        test_tmpdir());
    CHECK(mkdtemp(dir) != NULL, "mkdtemp");
#endif
    wirelog_program_t *prog = parse(
        ".decl a(k: int64, v: int64)\n.decl b(k: int64, v: int64)\n"
        ".decl c(k: int64, v: int64)\n"
        ".decl r(k: int64, x: int64, y: int64)\n"
        "r(k, x, y + 0) :- a(k, x), b(k, y).\n");
    wirelog_session_options_v1_t o = { 0 };
    o.struct_size = sizeof(o);
    o.abi_version = 1;
    o.spill_directory = dir;
    o.spill_quota_bytes = 1024 * 1024;
    o.spill_max_row_bytes = 64;
    o.spill_join_batch_bytes = 96;
    o.spill_join_index_bytes = 1024;
    /* Sealing and cold input both seal the whole session's input: every
     * later insert is refused, so every relation is outside v1. */
    for (int variant = 0; variant < 2; variant++) {
        wirelog_session_t *s = NULL;
        CHECK(wirelog_session_create_with_options(prog,
            WIRELOG_BACKEND_DEFAULT, 1, &o, &s) == WIRELOG_OK && s,
            "spill create");
        if (!s)
            continue;
        int64_t row[2] = { 1, 2 };
        CHECK(wirelog_session_insert(s, "b", row, 1, 2) == WIRELOG_OK, "b");
        CHECK(count2(s, "b", 1, 2) == 1, "before sealing");
        if (variant == 0) {
            CHECK(wirelog_session_seal_input(s, "b") == WIRELOG_OK, "seal");
        } else {
            CHECK(wirelog_session_begin_cold_input(s, "a") == WIRELOG_OK,
                "begin cold");
            uint64_t loading = count2(s, "a", 1, 2);
            CHECK(loading == UINT64_MAX - WIRELOG_SOURCE_QUERY_BUSY
                || loading == UINT64_MAX - WIRELOG_SOURCE_QUERY_UNSUPPORTED,
                "loading cold input refused: %llu",
                (unsigned long long)(UINT64_MAX - loading));
            CHECK(wirelog_session_append_cold_input(s, "a", row, 1, 2)
                == WIRELOG_OK, "append cold");
            CHECK(wirelog_session_finish_cold_input(s, "a") == WIRELOG_OK,
                "finish cold");
            CHECK(count2(s, "a", 1, 2)
                == UINT64_MAX - WIRELOG_SOURCE_QUERY_UNSUPPORTED,
                "cold source");
        }
        CHECK(count2(s, "b", 1, 2)
            == UINT64_MAX - WIRELOG_SOURCE_QUERY_UNSUPPORTED,
            "sealed session (%d)", variant);
        CHECK(count2(s, "c", 1, 2)
            == UINT64_MAX - WIRELOG_SOURCE_QUERY_UNSUPPORTED,
            "sealed session, other relation (%d)", variant);
        wirelog_session_destroy(s);
    }
#ifdef _WIN32
    (void)_rmdir(dir);
#else
    (void)rmdir(dir);
#endif
    wirelog_program_free(prog);
    prog = parse(EDB_HEAD_SRC);

    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(100, &control) == 0, "control");
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_testhook_set_default_options(&options);
    wirelog_session_t *s = NULL;
    wirelog_error_t err = wirelog_session_create(prog,
            WIRELOG_BACKEND_COLUMNAR, 1, &s);
    wl_session_testhook_set_default_options(NULL);
    CHECK(err == WIRELOG_OK && s, "controlled create %d", err);
    if (s) {
        uint64_t before = wl_evaluation_control_active_attempt_id(control);
        CHECK(count1(s, "e", 1)
            == UINT64_MAX - WIRELOG_SOURCE_QUERY_UNSUPPORTED,
            "controlled session");
        CHECK(wl_evaluation_control_active_attempt_id(control) == before,
            "attempt id unchanged");
        wirelog_session_destroy(s);
    }
    wl_evaluation_control_release(control);
    wirelog_program_free(prog);
}

/* ======================================================================== */
/* Failures leave no phantom receipts                                       */
/* ======================================================================== */

static void
starve(wl_col_session_t *cs, bool on)
{
    wl_columnar_memory_governor_t *g
        = wl_columnar_memory_governor_ref_get(cs->memory_governor);
    atomic_store_explicit(&g->usable_bytes,
        on ? wl_columnar_memory_reserved(g) : UINT64_MAX / 4u,
        memory_order_release);
}

static wl_columnar_memory_governor_ref_t *
enforcing_governor(void)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = UINT64_MAX / 4u;
    resolution.usable_bytes = UINT64_MAX / 4u;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static wirelog_session_t *
open_governed(wirelog_program_t *prog, wl_columnar_memory_governor_ref_t *ref)
{
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = ref;
    wl_session_testhook_set_default_options(&options);
    wirelog_session_t *s = open_session(prog, 1);
    wl_session_testhook_set_default_options(NULL);
    return s;
}

static void
test_failures_leave_no_receipts(void)
{
    const char *src =
        ".decl e(x: int64)\n"
        ".decl a(x: int64)\n"
        ".decl b(x: int64)\n"
        ".decl h(x: int64)\n"
        "a(X) :- e(X).\n"
        "b(X) :- a(X), e(X).\n";
    wirelog_program_t *prog = parse(src);
    wl_columnar_memory_governor_ref_t *ref = enforcing_governor();
    wirelog_session_t *s = open_governed(prog, ref);
    wl_col_session_t *cs = COL_SESSION(wirelog_session_testhook_inner(s));
    step(s);

    /* A denied insert batch commits nothing. */
    int64_t many[512];
    for (int i = 0; i < 512; i++)
        many[i] = 40;
    starve(cs, true);
    wirelog_error_t err = wirelog_session_insert(s, "e", many, 512, 1);
    starve(cs, false);
    CHECK(err != WIRELOG_OK, "starved insert fails");
    CHECK(count1(s, "e", 40) == 0, "no phantom after failed insert: %llu",
        (unsigned long long)count1(s, "e", 40));
    CHECK(count1(s, "a", 40) == 0, "head source untouched");

    /* A failed, retryable step leaves the source inspectable and exact. */
    for (int i = 0; i < 64; i++)
        ins1(s, "e", 100 + i);
    ins1(s, "e", 100);
    starve(cs, true);
    err = wirelog_session_step(s);
    starve(cs, false);
    CHECK(err != WIRELOG_OK, "starved step fails");
    CHECK(count1(s, "e", 100) == 2, "exact after failed step: %llu",
        (unsigned long long)count1(s, "e", 100));
    CHECK(count1(s, "e", 163) == 1, "last row after failed step");
    step(s);
    CHECK(count1(s, "e", 100) == 2, "same after retry");
    CHECK(count1(s, "e", 163) == 1, "last row after retry");
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    wl_columnar_memory_governor_ref_release(ref);
}

static void
test_typed_rollback_revokes(void)
{
    wirelog_program_t *prog = parse(EDB_HEAD_SRC);
    wl_columnar_memory_governor_ref_t *ref = enforcing_governor();
    wirelog_session_t *s = open_governed(prog, ref);
    wl_col_session_t *cs = COL_SESSION(wirelog_session_testhook_inner(s));
    col_rel_t *e = session_find_rel(cs, "e");
    CHECK(e != NULL, "e registered");
    if (!e)
        goto out;
    /* Fill e to one row short of its capacity, so a two-row typed batch
     * appends its first row in place and needs a governed growth for the
     * second. */
    ins1(s, "e", 1);
    while (e->nrows + 1 < e->capacity)
        ins1(s, "e", 1);
    uint64_t before = count1(s, "e", 1);
    CHECK(before == e->nrows, "filled");

    uint32_t types[1] = { WIRELOG_TYPE_INT64 };
    uint32_t offsets[1] = { 0 };
    uint64_t lanes[2] = { 8, 9 };
    wirelog_typed_row_v1_t rows[2];
    memset(rows, 0, sizeof(rows));
    for (int i = 0; i < 2; i++) {
        rows[i].struct_size = sizeof(rows[i]);
        rows[i].abi_version = 1;
        rows[i].logical_ncols = 1;
        rows[i].physical_nlanes = 1;
        rows[i].physical_stride = 1;
        rows[i].types = types;
        rows[i].lane_offsets = offsets;
        rows[i].physical_types = types;
        rows[i].lanes = &lanes[i];
    }
    starve(cs, true);
    wirelog_error_t err = wirelog_session_insert_typed(s, "e", rows, 2,
            NULL);
    starve(cs, false);
    CHECK(err != WIRELOG_OK, "second typed row denied");
    /* The rollback may or may not have restored e; either way its rows are
     * no longer certified as host requests alone. */
    CHECK(count1(s, "e", 8) == UINT64_MAX - WIRELOG_SOURCE_QUERY_UNSUPPORTED,
        "typed rollback revokes the relation: %llu",
        (unsigned long long)count1(s, "e", 8));
    CHECK(count1(s, "h", 8) == 0, "other relations stay certified");
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    wl_columnar_memory_governor_ref_release(ref);
}

int
main(void)
{
    for (uint32_t w = 1; w <= 2; w++) {
        test_edb_transitions(w, false);
        test_edb_transitions(w, true);
        test_head_transitions(w, false);
        test_head_transitions(w, true);
    }
    test_string_and_wide();
    test_inline_fact_origin();
    test_errors();
    test_work_limits();
    test_callback_reentry();
    test_queries_change_nothing();
    test_sealed_cold_and_controlled();
    test_failures_leave_no_receipts();
    test_typed_rollback_revokes();
    if (failures) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    printf("test_source_inspection: all checks passed\n");
    return 0;
}
