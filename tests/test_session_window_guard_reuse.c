/*
 * Timestamp-window guards across requests (Issue #2324).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Side compounds store intern IDs in int64 lanes. A derived catalogue must
 * still join a declared symbol evidence column with the same ID. Exercise
 * that boundary through the public API, including denied requests, cleanup,
 * repeated snapshots, and equivalence to a newly opened session.
 */
#include "wirelog/wirelog-easy.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expr) do { \
            if (!(expr)) { \
                fprintf(stderr, "line %d: %s\n", __LINE__, #expr); \
                exit(1); \
            } \
} while (0)

static const char program[] =
    ".decl permission(u:symbol,p:symbol,s:symbol)\n"
    ".decl rule(p:symbol,g:int64)\n"
    ".decl window(p:symbol,w:symbol)\n"
    ".decl context(u:symbol,s:symbol,t:int64)\n"
    ".decl evidence(u:symbol,s:symbol,t:int64,w:symbol)\n"
    ".decl guard_result(u:symbol,p:symbol,s:symbol)\n"
    ".decl armed(u:symbol,p:symbol,s:symbol)\n"
    ".decl denied(u:symbol,p:symbol,s:symbol)\n"
    "window(P,W) :- rule(P,G), __compound_window_1(G,W).\n"
    "armed(U,P,S) :- permission(U,P,S), context(U,S,T), "
    "window(P,W), evidence(U,S,T,W), guard_result(U,P,S).\n"
    "denied(U,P,S) :- permission(U,P,S), context(U,S,_), !armed(U,P,S).\n";

typedef struct {
    wirelog_easy_session_t *session;
    int64_t user, permission, scope, window, other;
} fixture_t;

typedef struct {
    const int64_t *tuple;
    uint32_t width, count;
} expected_t;

static void
collect(const char *relation, const int64_t *tuple, uint32_t width, void *data)
{
    (void)relation;
    expected_t *expected = data;
    CHECK(width == expected->width);
    CHECK(memcmp(tuple, expected->tuple, width * sizeof(*tuple)) == 0);
    expected->count++;
}

static void
snapshot(fixture_t *f, const char *name, const int64_t *tuple,
    uint32_t width, uint32_t count)
{
    /* The second call must preserve the committed model exactly. */
    for (unsigned repeat = 0; repeat < 2; repeat++) {
        expected_t expected = { tuple, width, 0 };
        CHECK(wirelog_easy_snapshot(f->session, name, collect, &expected)
            == WIRELOG_OK);
        CHECK(expected.count == count);
    }
}

static void
source_count(fixture_t *f, const char *name, const int64_t *tuple,
    uint32_t width, uint64_t count)
{
    uint64_t actual = UINT64_MAX;
    CHECK(wirelog_easy_source_count(f->session, name, tuple, width, 16,
        &actual) == WIRELOG_SOURCE_QUERY_OK);
    CHECK(actual == count);
}

static fixture_t
open_fixture(uint32_t workers)
{
    fixture_t f = { 0 };
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts.num_workers = workers;
    opts.eager_build = true;
    CHECK(wirelog_easy_open_opts(program, &opts, &f.session) == WIRELOG_OK);
    f.user = wirelog_easy_intern(f.session, "alice");
    f.permission = wirelog_easy_intern(f.session, "write");
    f.scope = wirelog_easy_intern(f.session, "resource");
    f.window = wirelog_easy_intern(f.session, "off_hours");
    f.other = wirelog_easy_intern(f.session, "other");
    CHECK(f.user >= 0 && f.permission >= 0 && f.scope >= 0
        && f.window >= 0 && f.other >= 0);
    wirelog_compound_arg_t arg = { WIRELOG_TYPE_STRING, f.window };
    uint64_t handle = 0;
    CHECK(wirelog_easy_make_compound(f.session, "window", 1, &arg, &handle)
        == WIRELOG_OK);
    int64_t permission[] = { f.user, f.permission, f.scope };
    int64_t rule[] = { f.permission, (int64_t)handle };
    CHECK(wirelog_easy_insert(f.session, "permission", permission, 3)
        == WIRELOG_OK);
    CHECK(wirelog_easy_insert(f.session, "rule", rule, 2) == WIRELOG_OK);
    return f;
}

enum request {
    NO_MATCHER, FALSE_MATCHER, MATCH, WRONG_WINDOW, WRONG_USER,
    WRONG_SCOPE, WRONG_TIME, WRONG_PERMISSION
};

static void
request(fixture_t *f, enum request mode)
{
    int64_t tuple[] = { f->user, f->permission, f->scope };
    int64_t context[] = { f->user, f->scope, 4242 };
    int64_t evidence[] = { f->user, f->scope, 4242, f->window };
    int64_t guard[] = { f->user, f->permission, f->scope };
    int64_t window[] = { f->permission, f->window };
    bool has_evidence = mode >= MATCH;
    if (mode == WRONG_WINDOW) evidence[3] = f->other;
    if (mode == WRONG_USER) evidence[0] = f->other;
    if (mode == WRONG_SCOPE) evidence[1] = f->other;
    if (mode == WRONG_TIME) evidence[2]++;
    if (mode == WRONG_PERMISSION) guard[1] = f->other;
    CHECK(wirelog_easy_insert(f->session, "context", context, 3) == WIRELOG_OK);
    if (has_evidence) {
        CHECK(wirelog_easy_insert(f->session, "evidence", evidence, 4)
            == WIRELOG_OK);
        CHECK(wirelog_easy_insert(f->session, "guard_result", guard, 3)
            == WIRELOG_OK);
    }
    source_count(f, "context", context, 3, 1);
    source_count(f, "evidence", evidence, 4, has_evidence ? 1 : 0);
    snapshot(f, "armed", tuple, 3, mode == MATCH ? 1 : 0);
    snapshot(f, "denied", tuple, 3, mode == MATCH ? 0 : 1);
    snapshot(f, "window", window, 2, 1);
    if (has_evidence) {
        CHECK(wirelog_easy_remove(f->session, "guard_result", guard, 3)
            == WIRELOG_OK);
        CHECK(wirelog_easy_remove(f->session, "evidence", evidence, 4)
            == WIRELOG_OK);
    }
    CHECK(wirelog_easy_remove(f->session, "context", context, 3) == WIRELOG_OK);
    source_count(f, "context", context, 3, 0);
    source_count(f, "evidence", evidence, 4, 0);
    source_count(f, "guard_result", guard, 3, 0);
    snapshot(f, "armed", tuple, 3, 0);
    snapshot(f, "denied", tuple, 3, 0);
    snapshot(f, "window", window, 2, 1);
    source_count(f, "permission", tuple, 3, 1);
}

int
main(void)
{
    const enum request sequence[] = {
        NO_MATCHER, FALSE_MATCHER, MATCH, FALSE_MATCHER,
        WRONG_WINDOW, WRONG_USER, WRONG_SCOPE, WRONG_TIME, WRONG_PERMISSION,
        MATCH, NO_MATCHER
    };
    for (uint32_t workers = 1; workers <= 4; workers += 3) {
        fixture_t reused = open_fixture(workers);
        for (unsigned i = 0; i < sizeof(sequence) / sizeof(sequence[0]); i++) {
            request(&reused, sequence[i]);
            fixture_t fresh = open_fixture(workers);
            request(&fresh, sequence[i]);
            wirelog_easy_close(fresh.session);
        }
        wirelog_easy_close(reused.session);
    }
    puts("window guard reuse: PASS");
    return 0;
}
