/*
 * test_wirelog_advanced.c - public wirelog_session_* surface tests (#717).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Behavioral parity tests for the advanced session API.  Each test
 * exercises one observable wl_easy-equivalent invariant so the two
 * facades cannot drift apart silently.
 */

#include "wirelog/wirelog-advanced.h"
#include "wirelog/wirelog.h"
#include "wirelog/intern.h"

#include <stdint.h>
#include <stdio.h>

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
    if (err == WIRELOG_OK || s != NULL) {
        fprintf(stderr,
            "T3: bad backend should reject, got err=%d s=%p\n", err,
            (void *)s);
        rc = 1;
    }
    wirelog_program_free(prog);
    return rc;
}

/* T4: inline `.dl` facts must materialize into snapshot derivations on
 *     the advanced facade just like wl_easy (#718 contract carries over). */
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
    if (failures == 0)
        printf("test_wirelog_advanced: OK\n");
    else
        printf("test_wirelog_advanced: %d failure(s)\n", failures);
    return failures;
}
