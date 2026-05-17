/*
 * test_wirelog_advanced.c - public wirelog_session_* surface tests (#717).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Behavioral parity tests for the advanced session API.  Each test
 * exercises one observable wirelog_easy-equivalent invariant so the two
 * facades cannot drift apart silently.
 */

/* setenv / unsetenv are POSIX (used by the side-compound saturation
 * parity test).  The Windows variants used by test_wirelog_easy.c live
 * under _WIN32 and are out of scope for the v0.40 sweep. */
#define _POSIX_C_SOURCE 200809L

#include "wirelog/wirelog-advanced.h"
#include "wirelog/wirelog.h"
#include "wirelog/intern.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
/* MSVC's CRT lacks POSIX setenv / unsetenv; route through _putenv_s.
 * Mirrors the same shim block in tests/test_wirelog_easy.c so the
 * cross-facade saturation parity test (T16) compiles cleanly under
 * the Windows MSVC CI leg. */
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, (value && *value) ? value : "1");
}

static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}

#  define setenv   wl_test_setenv_
#  define unsetenv wl_test_unsetenv_
#endif

static const char *PROG_SRC =
    ".decl edge(a:symbol,b:symbol)\n"
    ".decl path(a:symbol,b:symbol)\n"
    "path(X,Y) :- edge(X,Y).\n"
    "path(X,Z) :- path(X,Y), edge(Y,Z).\n";

static const char *PROG_RBAC_SRC =
    ".decl role_permission(role:symbol,perm:symbol)\n"
    ".decl member_of(user:symbol,role:symbol,scope:symbol)\n"
    ".decl effective_permission(role:symbol,perm:symbol)\n"
    ".decl has_permission(user:symbol,perm:symbol,scope:symbol)\n"
    "role_permission(\"wr.system_admin\", \"wr.policy.write\").\n"
    "effective_permission(R, P) :- role_permission(R, P).\n"
    "has_permission(U, P, S) :- "
    "  member_of(U, R, S), effective_permission(R, P).\n";

struct count_state {
    uint32_t rows;
};

struct delta_state {
    uint32_t inserts;
    uint32_t removes;
};

static void
count_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    struct count_state *st = (struct count_state *)user_data;
    st->rows++;
}

static void
count_deltas(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    struct delta_state *st = (struct delta_state *)user_data;
    if (diff > 0)
        st->inserts++;
    else if (diff < 0)
        st->removes++;
}

/* Per-relation tuple collector used by inline-compound parity tests
 * (#785).  wirelog_session_snapshot has no relation filter (the easy
 * counterpart does), so we filter inside the callback. */
#define WL_FILTER_MAX_ROWS 8
#define WL_FILTER_MAX_COLS 8

struct tuple_filter {
    const char *target_relation;
    int count;
    uint32_t ncols[WL_FILTER_MAX_ROWS];
    int64_t rows[WL_FILTER_MAX_ROWS][WL_FILTER_MAX_COLS];
};

static void
filter_tuples(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct tuple_filter *f = (struct tuple_filter *)user_data;
    if (strcmp(relation, f->target_relation) != 0)
        return;
    if (f->count >= WL_FILTER_MAX_ROWS)
        return;
    int idx = f->count++;
    f->ncols[idx] = ncols;
    for (uint32_t i = 0; i < ncols && i < WL_FILTER_MAX_COLS; i++)
        f->rows[idx][i] = row[i];
}

static wirelog_program_t *
parse_or_die(const char *src, const char *what)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        fprintf(stderr, "%s: parse failed err=%d\n", what, err);
        return NULL;
    }
    return prog;
}

static wirelog_error_t
make_compound4(wirelog_session_t *s, const char *functor, int64_t a0,
    int64_t a1, int64_t a2, int64_t a3, uint64_t *handle)
{
    wirelog_compound_arg_t args[4] = {
        { WIRELOG_TYPE_INT64, a0 },
        { WIRELOG_TYPE_INT64, a1 },
        { WIRELOG_TYPE_INT64, a2 },
        { WIRELOG_TYPE_INT64, a3 },
    };
    return wirelog_session_make_compound(s, functor, 4, args, handle);
}

static wirelog_error_t
make_compound1(wirelog_session_t *s, const char *functor, int64_t a0,
    uint64_t *handle)
{
    wirelog_compound_arg_t args[1] = {
        { WIRELOG_TYPE_INT64, a0 },
    };
    return wirelog_session_make_compound(s, functor, 1, args, handle);
}

/* T1: create / destroy with default backend; program ownership stays
 *     with the caller (destroy must not free it). */
static int
test_create_destroy_basic(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T1");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    wirelog_error_t err
        = wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s);
    int rc = 0;
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "T1 create err=%d\n", err);
        rc = 1;
    }
    wirelog_session_destroy(s);
    /* Program must still be valid after session destroy. */
    wirelog_program_free(prog);
    return rc;
}

/* T2: explicit columnar selection works the same as DEFAULT. */
static int
test_create_columnar(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T2");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    wirelog_error_t err
        = wirelog_session_create(prog, WIRELOG_BACKEND_COLUMNAR, 1, &s);
    int rc = (err == WIRELOG_OK && s) ? 0 : 1;
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* T3: invalid backend value rejects with WIRELOG_ERR_EXEC and *out is NULL. */
static int
test_create_invalid_backend(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T3");
    if (!prog)
        return 1;

    wirelog_session_t *s = (wirelog_session_t *)0xdeadbeef;
    wirelog_error_t err
        = wirelog_session_create(prog, (wirelog_backend_kind_t)999, 1, &s);
    int rc = 0;
    if (err != WIRELOG_ERR_EXEC || s != NULL) {
        fprintf(stderr,
            "T3: bad backend should reject with ERR_EXEC, got err=%d s=%p\n",
            err, (void *)s);
        rc = 1;
    }
    wirelog_program_free(prog);
    return rc;
}

/* T4: inline `.dl` facts must materialize into snapshot derivations on
 *     the advanced facade just like wirelog_easy (#718 contract carries over). */
static int
test_inline_facts_seeded(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_RBAC_SRC, "T4");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    wirelog_error_t err
        = wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s);
    if (err != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    struct count_state st = { 0 };
    err = wirelog_session_snapshot(s, count_rows, &st);
    int rc = 0;
    if (err != WIRELOG_OK) {
        fprintf(stderr, "T4 snapshot err=%d\n", err);
        rc = 1;
    } else if (st.rows == 0) {
        fprintf(stderr,
            "T4: expected static-fact-derived rows, got 0\n");
        rc = 1;
    }
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* T5: delta callback fires on host insert + step. */
static int
test_insert_step_delta(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T5");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK) {
        wirelog_program_free(prog);
        return 1;
    }

    struct delta_state st = { 0, 0 };
    if (wirelog_session_set_delta_cb(s, count_deltas, &st) != WIRELOG_OK) {
        wirelog_session_destroy(s);
        wirelog_program_free(prog);
        return 1;
    }

    /* Intern via the program's intern table.  A and B as new symbols. */
    wl_intern_t *intern
        = (wl_intern_t *)wirelog_program_get_intern(prog);
    int64_t a = wl_intern_put(intern, "a");
    int64_t b = wl_intern_put(intern, "b");
    if (a < 0 || b < 0) {
        wirelog_session_destroy(s);
        wirelog_program_free(prog);
        return 1;
    }
    int64_t row[2] = { a, b };
    int rc = 0;
    if (wirelog_session_insert(s, "edge", row, 1, 2) != WIRELOG_OK) {
        fprintf(stderr, "T5 insert err\n");
        rc = 1;
    } else if (wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr, "T5 step err\n");
        rc = 1;
    } else if (st.inserts == 0) {
        fprintf(stderr, "T5: expected delta inserts > 0, got 0\n");
        rc = 1;
    }

    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* T6: insert / remove pair leaves the relation back at zero. */
static int
test_insert_remove_roundtrip(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T6");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK) {
        wirelog_program_free(prog);
        return 1;
    }

    wl_intern_t *intern
        = (wl_intern_t *)wirelog_program_get_intern(prog);
    int64_t a = wl_intern_put(intern, "a");
    int64_t b = wl_intern_put(intern, "b");
    int64_t row[2] = { a, b };

    int rc = 0;
    if (wirelog_session_insert(s, "edge", row, 1, 2) != WIRELOG_OK
        || wirelog_session_remove(s, "edge", row, 1, 2) != WIRELOG_OK) {
        fprintf(stderr, "T6 insert/remove failed\n");
        rc = 1;
    }
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* T7: NULL-safe destroy and NULL-guard returns. */
static int
test_null_safety(void)
{
    int rc = 0;
    wirelog_session_destroy(NULL); /* must not crash */

    if (wirelog_session_step(NULL) != WIRELOG_ERR_EXEC)
        rc = 1;
    if (wirelog_session_insert(NULL, "r", NULL, 0, 0) != WIRELOG_ERR_EXEC)
        rc = 1;
    if (wirelog_session_remove(NULL, "r", NULL, 0, 0) != WIRELOG_ERR_EXEC)
        rc = 1;
    if (wirelog_session_set_delta_cb(NULL, NULL, NULL) != WIRELOG_ERR_EXEC)
        rc = 1;
    if (wirelog_session_snapshot(NULL, count_rows, NULL) != WIRELOG_ERR_EXEC)
        rc = 1;
    uint64_t handle = WIRELOG_COMPOUND_HANDLE_NULL + 1;
    if (wirelog_session_make_compound(NULL, "f", 1, NULL, &handle)
        != WIRELOG_ERR_EXEC)
        rc = 1;
    if (handle != WIRELOG_COMPOUND_HANDLE_NULL) {
        fprintf(stderr,
            "T7: make_compound(NULL) must pre-clear handle_out\n");
        rc = 1;
    }

    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T7");
    if (!prog)
        return rc | 1;
    wirelog_session_t *s = NULL;
    wirelog_error_t err
        = wirelog_session_create(NULL, WIRELOG_BACKEND_DEFAULT, 1, &s);
    if (err != WIRELOG_ERR_EXEC || s != NULL) {
        fprintf(stderr, "T7 create(NULL prog) leaked: err=%d s=%p\n",
            err, (void *)s);
        rc = 1;
    }
    err = wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, NULL);
    if (err != WIRELOG_ERR_EXEC) {
        fprintf(stderr, "T7 create(NULL out) wrong err=%d\n", err);
        rc = 1;
    }
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_wirelog_easy.c::test_open_parse_error.
 * On the advanced side, parsing happens explicitly via
 * wirelog_parse_string; the test asserts that malformed Datalog
 * returns NULL and sets the error indicator. */
static int
test_open_parse_error(void)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog
        = wirelog_parse_string("this is not datalog ::: !!!", &err);
    int rc = 0;
    if (prog) {
        fprintf(stderr, "T8: parse should have failed, got prog=%p\n",
            (void *)prog);
        wirelog_program_free(prog);
        rc = 1;
    }
    if (err == WIRELOG_OK) {
        fprintf(stderr, "T8: parse failed but err == WIRELOG_OK\n");
        rc = 1;
    }
    return rc;
}

/* Parity (#785): mirrors test_wirelog_easy.c::test_num_workers_default_is_one.
 * Where the easy facade reads num_workers from an opts struct and
 * defaults to 1 when unspecified, the advanced API takes num_workers
 * as an explicit constructor argument; this test verifies that
 * passing 1 is accepted and the resulting session is usable. */
static int
test_num_workers_default_is_one(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T9");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    wirelog_error_t err
        = wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s);
    int rc = 0;
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "T9 create num_workers=1 err=%d\n", err);
        rc = 1;
    } else if (wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr, "T9 step failed with num_workers=1\n");
        rc = 1;
    }
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_wirelog_easy.c::test_num_workers_explicit_four.
 * Pass num_workers=4 explicitly and verify session_create accepts the
 * value and the resulting session can evaluate a step.  The easy-side
 * test captures the log line to verify the value reached the runtime;
 * on the advanced side num_workers IS the parameter, so behavioural
 * verification (step succeeds) is sufficient. */
static int
test_num_workers_explicit_four(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T10");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    wirelog_error_t err
        = wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 4, &s);
    int rc = 0;
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "T10 create num_workers=4 err=%d\n", err);
        rc = 1;
    } else if (wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr, "T10 step failed with num_workers=4\n");
        rc = 1;
    }
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_wirelog_easy.c::test_inline_compound_body_binding.
 * Asserts the inline-compound body pattern `metadata(L,T,H,R)` binds
 * the four child variables so the head condition `R > 80` filters
 * rows correctly. */
static int
test_inline_compound_body_binding(void)
{
    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl hot(id: int64)\n"
        "hot(ID) :- event(ID, metadata(Level, Ts, Host, Risk)),"
        " Risk > 80.\n";
    wirelog_program_t *prog = parse_or_die(src, "T11");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    int64_t hot_row[5] = { 7, 1, 2, 3, 90 };
    int64_t cold_row[5] = { 8, 1, 2, 3, 40 };
    int rc = 0;
    if (wirelog_session_insert(s, "event", hot_row, 1, 5) != WIRELOG_OK
        || wirelog_session_insert(s, "event", cold_row, 1, 5) != WIRELOG_OK) {
        fprintf(stderr, "T11 insert failed\n");
        rc = 1;
        goto out;
    }
    struct tuple_filter f = { .target_relation = "hot" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T11 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 1 || f.ncols[0] != 1 || f.rows[0][0] != 7) {
        fprintf(stderr,
            "T11: expected only hot(7), got count=%d row0[0]=%lld\n",
            f.count, (long long)f.rows[0][0]);
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_inline_compound_body_join_binding.
 * Verifies that body variables from the inline compound participate
 * in joins (here: Risk joins threshold). */
static int
test_inline_compound_body_join_binding(void)
{
    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl threshold(risk: int64)\n"
        ".decl hot(id: int64)\n"
        "hot(ID) :- event(ID, metadata(Level, Ts, Host, Risk)),"
        " threshold(Risk).\n";
    wirelog_program_t *prog = parse_or_die(src, "T12");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    int64_t matched[5] = { 7, 1, 2, 3, 90 };
    int64_t unmatched[5] = { 8, 1, 2, 3, 40 };
    int64_t threshold_row[1] = { 90 };
    int rc = 0;
    if (wirelog_session_insert(s, "event", matched, 1, 5) != WIRELOG_OK
        || wirelog_session_insert(s, "event", unmatched, 1, 5) != WIRELOG_OK
        || wirelog_session_insert(s, "threshold", threshold_row, 1, 1)
        != WIRELOG_OK) {
        fprintf(stderr, "T12 insert failed\n");
        rc = 1;
        goto out;
    }
    struct tuple_filter f = { .target_relation = "hot" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T12 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 1 || f.rows[0][0] != 7) {
        fprintf(stderr, "T12: expected join to derive only hot(7)\n");
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_inline_compound_functor_mismatch_is_empty.
 * A body using a different functor name does not match. */
static int
test_inline_compound_functor_mismatch_is_empty(void)
{
    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl bad(id: int64)\n"
        "bad(ID) :- event(ID, other(Level, Ts, Host, Risk)).\n";
    wirelog_program_t *prog = parse_or_die(src, "T13");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    int64_t row[5] = { 7, 1, 2, 3, 90 };
    int rc = 0;
    if (wirelog_session_insert(s, "event", row, 1, 5) != WIRELOG_OK) {
        fprintf(stderr, "T13 insert failed\n");
        rc = 1;
        goto out;
    }
    struct tuple_filter f = { .target_relation = "bad" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T13 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 0) {
        fprintf(stderr,
            "T13: mismatched functor should derive no rows, got %d\n",
            f.count);
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_inline_compound_constant_child_filters.
 * A constant in a child slot filters rows that do not match the
 * literal value. */
static int
test_inline_compound_constant_child_filters(void)
{
    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl hot(id: int64)\n"
        "hot(ID) :- event(ID, metadata(_, _, _, 90)).\n";
    wirelog_program_t *prog = parse_or_die(src, "T14");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    int64_t hot_row[5] = { 7, 1, 2, 3, 90 };
    int64_t cold_row[5] = { 8, 1, 2, 3, 40 };
    int rc = 0;
    if (wirelog_session_insert(s, "event", hot_row, 1, 5) != WIRELOG_OK
        || wirelog_session_insert(s, "event", cold_row, 1, 5) != WIRELOG_OK) {
        fprintf(stderr, "T14 insert failed\n");
        rc = 1;
        goto out;
    }
    struct tuple_filter f = { .target_relation = "hot" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T14 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 1 || f.rows[0][0] != 7) {
        fprintf(stderr,
            "T14: expected only row with constant child 90\n");
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_inline_compound_duplicate_child_variables_filter.
 * Repeating a child variable in a compound pattern filters rows where
 * the two slots are unequal. */
static int
test_inline_compound_duplicate_child_variables_filter(void)
{
    const char *src
        = ".decl event(id: int64, payload: metadata/4 inline)\n"
        ".decl same(id: int64)\n"
        "same(ID) :- event(ID, metadata(X, X, _, _)).\n";
    wirelog_program_t *prog = parse_or_die(src, "T15");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    int64_t matched[5] = { 7, 4, 4, 3, 90 };
    int64_t unmatched[5] = { 8, 1, 2, 3, 90 };
    int rc = 0;
    if (wirelog_session_insert(s, "event", matched, 1, 5) != WIRELOG_OK
        || wirelog_session_insert(s, "event", unmatched, 1, 5) != WIRELOG_OK) {
        fprintf(stderr, "T15 insert failed\n");
        rc = 1;
        goto out;
    }
    struct tuple_filter f = { .target_relation = "same" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T15 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 1 || f.rows[0][0] != 7) {
        fprintf(stderr,
            "T15: expected only row with equal duplicate variables\n");
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_side_compound_public_allocation_saturates.
 * With WIRELOG_COMPOUND_MAX_EPOCHS=2, the second compound allocation
 * across two epochs reports WIRELOG_ERR_COMPOUND_SATURATED and the
 * out-handle is cleared to WIRELOG_COMPOUND_HANDLE_NULL. */
static int
test_side_compound_public_allocation_saturates(void)
{
    if (setenv("WIRELOG_COMPOUND_MAX_EPOCHS", "2", 1) != 0)
        return 1;

    const char *src
        = ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl seen(id: int64)\n"
        ".decl edge(x: int64, y: int64)\n"
        ".decl tc(x: int64, y: int64)\n"
        "seen(ID) :- event(ID, _).\n"
        "tc(X, Y) :- edge(X, Y).\n"
        "tc(X, Z) :- edge(X, Y), tc(Y, Z).\n";
    wirelog_program_t *prog = parse_or_die(src, "T16");
    if (!prog) {
        unsetenv("WIRELOG_COMPOUND_MAX_EPOCHS");
        return 1;
    }

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        unsetenv("WIRELOG_COMPOUND_MAX_EPOCHS");
        return 1;
    }

    wirelog_compound_arg_t args[4] = {
        { WIRELOG_TYPE_INT64, 1 },
        { WIRELOG_TYPE_INT64, 2 },
        { WIRELOG_TYPE_INT64, 3 },
        { WIRELOG_TYPE_INT64, 4 },
    };
    uint64_t handle = 0;
    int rc = 0;
    wirelog_error_t mc_rc
        = wirelog_session_make_compound(s, "metadata", 4, args, &handle);
    if (mc_rc != WIRELOG_OK || handle == WIRELOG_COMPOUND_HANDLE_NULL) {
        fprintf(stderr,
            "T16: first make_compound failed rc=%d handle=%llu\n",
            mc_rc, (unsigned long long)handle);
        rc = 1;
        goto out;
    }
    int64_t event_row[2] = { 1, (int64_t)handle };
    int64_t edge_12[2] = { 1, 2 };
    int64_t edge_23[2] = { 2, 3 };
    int64_t edge_34[2] = { 3, 4 };
    if (wirelog_session_insert(s, "event", event_row, 1, 2) != WIRELOG_OK
        || wirelog_session_insert(s, "edge", edge_12, 1, 2) != WIRELOG_OK
        || wirelog_session_insert(s, "edge", edge_23, 1, 2) != WIRELOG_OK
        || wirelog_session_insert(s, "edge", edge_34, 1, 2) != WIRELOG_OK
        || wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr, "T16 first insert/step failed\n");
        rc = 1;
        goto out;
    }
    handle = 123;
    mc_rc = wirelog_session_make_compound(s, "metadata", 4, args, &handle);
    if (mc_rc != WIRELOG_ERR_COMPOUND_SATURATED
        || handle != WIRELOG_COMPOUND_HANDLE_NULL) {
        fprintf(stderr,
            "T16: expected SATURATED + cleared handle, got rc=%d "
            "handle=%llu\n",
            mc_rc, (unsigned long long)handle);
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    unsetenv("WIRELOG_COMPOUND_MAX_EPOCHS");
    return rc;
}

/* Parity (#785): mirrors test_side_compound_body_field_binding. */
static int
test_side_compound_body_field_binding(void)
{
    const char *src
        = ".decl event(id: int64, tenant: symbol, payload: metadata/4 side)\n"
        ".decl host_hit(id: int64, host: symbol)\n"
        "host_hit(ID, Host) :- event(ID, Tenant, "
        "metadata(Level, Ts, Host, Risk)), Risk > 80.\n";
    wirelog_program_t *prog = parse_or_die(src, "T17");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    wl_intern_t *intern = (wl_intern_t *)wirelog_program_get_intern(prog);
    int64_t tenant = wl_intern_put(intern, "acme");
    int64_t host_a = wl_intern_put(intern, "edge-a");
    int64_t host_b = wl_intern_put(intern, "edge-b");
    uint64_t hot_handle = 0;
    uint64_t cold_handle = 0;
    int rc = 0;
    if (tenant < 0 || host_a < 0 || host_b < 0
        || make_compound4(s, "metadata", 1, 100, host_a, 90,
        &hot_handle) != WIRELOG_OK
        || make_compound4(s, "metadata", 1, 101, host_b, 40,
        &cold_handle) != WIRELOG_OK) {
        fprintf(stderr, "T17 compound allocation failed\n");
        rc = 1;
        goto out;
    }

    int64_t hot_row[3] = { 7, tenant, (int64_t)hot_handle };
    int64_t cold_row[3] = { 8, tenant, (int64_t)cold_handle };
    if (wirelog_session_insert(s, "event", hot_row, 1, 3) != WIRELOG_OK
        || wirelog_session_insert(s, "event", cold_row, 1, 3)
        != WIRELOG_OK) {
        fprintf(stderr, "T17 insert failed\n");
        rc = 1;
        goto out;
    }

    struct tuple_filter f = { .target_relation = "host_hit" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T17 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 1 || f.ncols[0] != 2 || f.rows[0][0] != 7
        || f.rows[0][1] != host_a) {
        fprintf(stderr, "T17: expected only host_hit(7, edge-a)\n");
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_side_compound_constant_child_filters. */
static int
test_side_compound_constant_child_filters(void)
{
    const char *src
        = ".decl event(id: int64, tenant: symbol, payload: metadata/4 side)\n"
        ".decl hot(id: int64)\n"
        "hot(ID) :- event(ID, _, metadata(_, _, _, 90)).\n";
    wirelog_program_t *prog = parse_or_die(src, "T18");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    uint64_t hot_handle = 0;
    uint64_t cold_handle = 0;
    int rc = 0;
    if (make_compound4(s, "metadata", 1, 100, 3, 90, &hot_handle)
        != WIRELOG_OK
        || make_compound4(s, "metadata", 1, 101, 3, 40, &cold_handle)
        != WIRELOG_OK) {
        fprintf(stderr, "T18 compound allocation failed\n");
        rc = 1;
        goto out;
    }

    int64_t hot_row[3] = { 7, 1, (int64_t)hot_handle };
    int64_t cold_row[3] = { 8, 1, (int64_t)cold_handle };
    if (wirelog_session_insert(s, "event", hot_row, 1, 3) != WIRELOG_OK
        || wirelog_session_insert(s, "event", cold_row, 1, 3)
        != WIRELOG_OK) {
        fprintf(stderr, "T18 insert failed\n");
        rc = 1;
        goto out;
    }

    struct tuple_filter f = { .target_relation = "hot" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T18 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 1 || f.ncols[0] != 1 || f.rows[0][0] != 7) {
        fprintf(stderr, "T18: expected only hot(7)\n");
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_side_compound_duplicate_child_variables_filter. */
static int
test_side_compound_duplicate_child_variables_filter(void)
{
    const char *src
        = ".decl event(id: int64, tenant: symbol, payload: metadata/4 side)\n"
        ".decl same(id: int64)\n"
        "same(ID) :- event(ID, _, metadata(X, X, _, _)).\n";
    wirelog_program_t *prog = parse_or_die(src, "T19");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    uint64_t matched_handle = 0;
    uint64_t unmatched_handle = 0;
    int rc = 0;
    if (make_compound4(s, "metadata", 4, 4, 3, 90, &matched_handle)
        != WIRELOG_OK
        || make_compound4(s, "metadata", 1, 2, 3, 90,
        &unmatched_handle) != WIRELOG_OK) {
        fprintf(stderr, "T19 compound allocation failed\n");
        rc = 1;
        goto out;
    }

    int64_t matched_row[3] = { 7, 1, (int64_t)matched_handle };
    int64_t unmatched_row[3] = { 8, 1, (int64_t)unmatched_handle };
    if (wirelog_session_insert(s, "event", matched_row, 1, 3)
        != WIRELOG_OK
        || wirelog_session_insert(s, "event", unmatched_row, 1, 3)
        != WIRELOG_OK) {
        fprintf(stderr, "T19 insert failed\n");
        rc = 1;
        goto out;
    }

    struct tuple_filter f = { .target_relation = "same" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T19 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 1 || f.ncols[0] != 1 || f.rows[0][0] != 7) {
        fprintf(stderr, "T19: expected only same(7)\n");
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_side_compound_wrong_functor_handle_no_match. */
static int
test_side_compound_wrong_functor_handle_no_match(void)
{
    const char *src
        = ".decl event(id: int64, tenant: symbol, payload: metadata/4 side)\n"
        ".decl seen(id: int64)\n"
        "seen(ID) :- event(ID, _, metadata(_, _, _, _)).\n";
    wirelog_program_t *prog = parse_or_die(src, "T20");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    uint64_t wrong_handle = 0;
    int rc = 0;
    if (make_compound4(s, "other", 1, 2, 3, 4, &wrong_handle)
        != WIRELOG_OK) {
        fprintf(stderr, "T20 compound allocation failed\n");
        rc = 1;
        goto out;
    }

    int64_t row[3] = { 7, 1, (int64_t)wrong_handle };
    if (wirelog_session_insert(s, "event", row, 1, 3) != WIRELOG_OK) {
        fprintf(stderr, "T20 insert failed\n");
        rc = 1;
        goto out;
    }

    struct tuple_filter f = { .target_relation = "seen" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T20 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 0) {
        fprintf(stderr, "T20: wrong functor handle derived rows\n");
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_side_compound_nested_child_no_match. */
static int
test_side_compound_nested_child_no_match(void)
{
    const char *src
        = ".decl record(id: int64, scope_col: scope/1 side)\n"
        ".decl seen(id: int64)\n"
        "seen(ID) :- record(ID, scope(metadata(Level, Ts, Host, Risk))).\n";
    wirelog_program_t *prog = parse_or_die(src, "T21");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    uint64_t metadata_handle = 0;
    uint64_t scope_handle = 0;
    int rc = 0;
    if (make_compound4(s, "metadata", 1, 100, 3, 90, &metadata_handle)
        != WIRELOG_OK
        || make_compound1(s, "scope", (int64_t)metadata_handle,
        &scope_handle) != WIRELOG_OK) {
        fprintf(stderr, "T21 compound allocation failed\n");
        rc = 1;
        goto out;
    }

    int64_t row[2] = { 7, (int64_t)scope_handle };
    if (wirelog_session_insert(s, "record", row, 1, 2) != WIRELOG_OK) {
        fprintf(stderr, "T21 insert failed\n");
        rc = 1;
        goto out;
    }

    struct tuple_filter f = { .target_relation = "seen" };
    if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
        fprintf(stderr, "T21 snapshot failed\n");
        rc = 1;
        goto out;
    }
    if (f.count != 0) {
        fprintf(stderr, "T21: nested side body destructuring matched\n");
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_negated_side_compound_body_pattern_rejected. */
static int
test_negated_side_compound_body_pattern_rejected(void)
{
    const char *src
        = ".decl all_events(id: int64)\n"
        ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl clean(id: int64)\n"
        "clean(ID) :- all_events(ID), !event(ID, metadata(_, _, _, 90)).\n";
    wirelog_error_t parse_err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &parse_err);
    if (!prog)
        return parse_err == WIRELOG_OK ? 1 : 0;

    wirelog_session_t *s = NULL;
    wirelog_error_t err
        = wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s);
    int rc = 0;
    if (err != WIRELOG_ERR_INVALID_IR || s) {
        fprintf(stderr,
            "T22: expected invalid IR for negated side compound, got %d\n",
            err);
        wirelog_session_destroy(s);
        rc = 1;
    }
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_wirelog_easy.c::test_intern_returns_same_id.
 * Interning the same string twice through wl_intern_put must return
 * the same int64 id; the advanced API exposes the intern table via
 * wirelog_program_get_intern. */
static int
test_intern_returns_same_id(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T17");
    if (!prog)
        return 1;

    wl_intern_t *intern = (wl_intern_t *)wirelog_program_get_intern(prog);
    int64_t a = wl_intern_put(intern, "alice");
    int64_t b = wl_intern_put(intern, "alice");
    int rc = 0;
    if (a < 0 || b < 0 || a != b) {
        fprintf(stderr,
            "T17: intern inconsistent a=%lld b=%lld\n",
            (long long)a, (long long)b);
        rc = 1;
    }
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_wirelog_easy.c::test_snapshot_filter.
 * wirelog_session_snapshot itself does not filter by relation, but
 * the per-tuple callback can.  Insert into two relations (edge and
 * derived path), then verify the filter callback collects only the
 * targeted relation's rows. */
static int
test_snapshot_filter(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T18");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    wl_intern_t *intern = (wl_intern_t *)wirelog_program_get_intern(prog);
    int64_t a = wl_intern_put(intern, "a");
    int64_t b = wl_intern_put(intern, "b");
    int64_t c = wl_intern_put(intern, "c");
    int rc = 0;
    int64_t e_ab[2] = { a, b };
    int64_t e_bc[2] = { b, c };
    if (wirelog_session_insert(s, "edge", e_ab, 1, 2) != WIRELOG_OK
        || wirelog_session_insert(s, "edge", e_bc, 1, 2) != WIRELOG_OK) {
        fprintf(stderr, "T18 insert failed\n");
        rc = 1;
        goto out;
    }

    struct tuple_filter path_filter = { .target_relation = "path" };
    if (wirelog_session_snapshot(s, filter_tuples, &path_filter)
        != WIRELOG_OK) {
        fprintf(stderr, "T18 snapshot failed\n");
        rc = 1;
        goto out;
    }
    /* path is transitive closure over edge: expect path(a,b), path(b,c),
     * path(a,c) -> 3 rows. */
    if (path_filter.count != 3) {
        fprintf(stderr,
            "T18: expected 3 path tuples after filter, got %d\n",
            path_filter.count);
        rc = 1;
    }
    /* All filtered rows must be from the "path" relation. */
    for (int i = 0; i < path_filter.count; i++) {
        if (path_filter.ncols[i] != 2) {
            fprintf(stderr, "T18: path row %d ncols=%u\n",
                i, path_filter.ncols[i]);
            rc = 1;
        }
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_wirelog_easy.c::test_cleanup_order_no_use_after_free.
 * Repeated open/use/close cycles must not leak or use-after-free under
 * ASAN.  Mirrors easy by running two iterations through a tiny insert
 * + step cycle. */
static int
test_cleanup_order_no_use_after_free(void)
{
    for (int iter = 0; iter < 2; iter++) {
        wirelog_program_t *prog = parse_or_die(PROG_SRC, "T19");
        if (!prog)
            return 1;

        wirelog_session_t *s = NULL;
        if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
            != WIRELOG_OK || !s) {
            wirelog_program_free(prog);
            return 1;
        }

        wl_intern_t *intern = (wl_intern_t *)wirelog_program_get_intern(prog);
        int64_t a = wl_intern_put(intern, "a");
        int64_t b = wl_intern_put(intern, "b");
        int64_t row[2] = { a, b };
        int rc = 0;
        if (wirelog_session_insert(s, "edge", row, 1, 2) != WIRELOG_OK
            || wirelog_session_step(s) != WIRELOG_OK) {
            fprintf(stderr, "T19 iter=%d insert/step failed\n", iter);
            rc = 1;
        }
        wirelog_session_destroy(s);
        wirelog_program_free(prog);
        if (rc)
            return rc;
    }
    return 0;
}

/* Parity (#785): mirrors test_wirelog_easy.c::test_intern_after_step_succeeds.
 * Interning a brand new symbol after the plan has been built and
 * stepped must still succeed; the new id must be usable for a
 * subsequent insert+step. */
static int
test_intern_after_step_succeeds(void)
{
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T20");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    wl_intern_t *intern = (wl_intern_t *)wirelog_program_get_intern(prog);
    int64_t a = wl_intern_put(intern, "a");
    int64_t b = wl_intern_put(intern, "b");
    int rc = 0;
    int64_t row[2] = { a, b };
    if (wirelog_session_insert(s, "edge", row, 1, 2) != WIRELOG_OK
        || wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr, "T20 first insert/step failed\n");
        rc = 1;
        goto out;
    }
    /* Intern a brand new symbol after the first step. */
    int64_t late = wl_intern_put(intern, "late_symbol");
    if (late < 0) {
        fprintf(stderr, "T20 late intern failed\n");
        rc = 1;
        goto out;
    }
    int64_t late_row[2] = { late, b };
    if (wirelog_session_insert(s, "edge", late_row, 1, 2) != WIRELOG_OK
        || wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr, "T20 late insert/step failed\n");
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_wirelog_easy.c::test_delta_cb_multi_round_recursive_insert.
 * Issue #662 invariant: a delta callback installed before two
 * insert+step rounds on a recursive stratum must observe deltas in
 * BOTH rounds.  Without the symmetric arrangement-cache invalidation
 * the second round either drops deltas or trips on stale joins. */
static int
test_delta_cb_multi_round_recursive_insert(void)
{
    static const char *RECURSIVE_REACH_SRC
        = ".decl edge(a: int64, b: int64)\n"
        ".decl reach(a: int64, b: int64)\n"
        "reach(A, B) :- edge(A, B).\n"
        "reach(A, C) :- reach(A, B), edge(B, C).\n";

    wirelog_program_t *prog = parse_or_die(RECURSIVE_REACH_SRC, "T21");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK || !s) {
        wirelog_program_free(prog);
        return 1;
    }

    struct delta_state st = { 0, 0 };
    if (wirelog_session_set_delta_cb(s, count_deltas, &st) != WIRELOG_OK) {
        wirelog_session_destroy(s);
        wirelog_program_free(prog);
        return 1;
    }

    int rc = 0;
    int64_t e12[2] = { 1, 2 };
    if (wirelog_session_insert(s, "edge", e12, 1, 2) != WIRELOG_OK
        || wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr, "T21 round 1 insert/step failed\n");
        rc = 1;
        goto out;
    }
    uint32_t first_round_inserts = st.inserts;
    if (first_round_inserts == 0) {
        fprintf(stderr, "T21 round 1: no inserts surfaced\n");
        rc = 1;
        goto out;
    }

    /* Second insert/step round must also surface deltas on the
     * recursive stratum. */
    int64_t e23[2] = { 2, 3 };
    if (wirelog_session_insert(s, "edge", e23, 1, 2) != WIRELOG_OK
        || wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr, "T21 round 2 insert/step failed\n");
        rc = 1;
        goto out;
    }
    if (st.inserts <= first_round_inserts) {
        fprintf(stderr,
            "T21 round 2: expected more inserts than round 1=%u, got %u\n",
            first_round_inserts, st.inserts);
        rc = 1;
    }
out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    return rc;
}

int
main(void)
{
    int failures = 0;
    failures += test_create_destroy_basic();
    failures += test_create_columnar();
    failures += test_create_invalid_backend();
    failures += test_inline_facts_seeded();
    failures += test_insert_step_delta();
    failures += test_insert_remove_roundtrip();
    failures += test_null_safety();
    failures += test_open_parse_error();
    failures += test_num_workers_default_is_one();
    failures += test_num_workers_explicit_four();
    failures += test_inline_compound_body_binding();
    failures += test_inline_compound_body_join_binding();
    failures += test_inline_compound_functor_mismatch_is_empty();
    failures += test_inline_compound_constant_child_filters();
    failures += test_inline_compound_duplicate_child_variables_filter();
    failures += test_side_compound_public_allocation_saturates();
    failures += test_side_compound_body_field_binding();
    failures += test_side_compound_constant_child_filters();
    failures += test_side_compound_duplicate_child_variables_filter();
    failures += test_side_compound_wrong_functor_handle_no_match();
    failures += test_side_compound_nested_child_no_match();
    failures += test_negated_side_compound_body_pattern_rejected();
    failures += test_intern_returns_same_id();
    failures += test_snapshot_filter();
    failures += test_cleanup_order_no_use_after_free();
    failures += test_intern_after_step_succeeds();
    failures += test_delta_cb_multi_round_recursive_insert();
    if (failures == 0)
        printf("test_wirelog_advanced: OK\n");
    else
        printf("test_wirelog_advanced: %d failure(s)\n", failures);
    return failures;
}
