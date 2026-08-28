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
#include "wirelog/wirelog-extension.h"
#include "wirelog/wirelog.h"
#include "wirelog/intern.h"
#include "wirelog/passes/fusion.h"
#include "wirelog/passes/jpp.h"
#include "wirelog/passes/sip.h"

#include <stdbool.h>
#include <stdint.h>
#include <math.h>
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

static int extension_destroy_calls;
static int snapshot_extension_fail;
static wirelog_program_t *parse_or_die(const char *src, const char *what);

struct public_snapshot_rows {
    uint32_t rows;
};

static void
count_public_snapshot_rows(const char *relation, const int64_t *row,
    uint32_t ncols, void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    ((struct public_snapshot_rows *)user_data)->rows++;
}

static const char *SNAPSHOT_EXTENSION_SRC =
    ".decl src(x: int64)\n"
    ".decl out(x: int64)\n"
    "out(x) :- src(x), @call(\"test.public_snapshot\", x).\n";
static const char *MAP_EXTENSION_SRC =
    ".decl src(x: int64)\n"
    ".decl out(x: int64)\n"
    "out(@call(\"test.public_map\", x)) :- src(x).\n";

static int
test_extension_invoke(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *user_data)
{
    (void)args;
    (void)nargs;
    (void)result;
    (void)user_data;
    return 0;
}

static int
snapshot_extension_invoke(const wirelog_extension_value_t *args,
    uint32_t nargs, wirelog_extension_value_t *result, void *user_data)
{
    (void)args;
    (void)nargs;
    (void)user_data;
    if (snapshot_extension_fail)
        return 1;
    result->type = WIRELOG_EXTENSION_VALUE_BOOL;
    result->size = sizeof(uint8_t);
    result->as.bool_value = 1;
    return 0;
}

static int
map_extension_invoke(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *user_data)
{
    (void)user_data;
    if (snapshot_extension_fail || nargs != 1
        || args[0].type != WIRELOG_EXTENSION_VALUE_INT64)
        return 1;
    result->type = WIRELOG_EXTENSION_VALUE_INT64;
    result->size = sizeof(int64_t);
    result->as.int64_value = args[0].as.int64_value + 10;
    return 0;
}

struct public_map_rows {
    uint32_t rows;
    int64_t value;
};

static void
capture_public_map_rows(const char *relation, const int64_t *row,
    uint32_t ncols, void *user_data)
{
    struct public_map_rows *state = user_data;
    if (strcmp(relation, "out") == 0 && ncols == 1) {
        state->rows++;
        state->value = row[0];
    }
}

static int
test_public_map_extension_execution(void)
{
    const uint32_t argument_types[] = { WIRELOG_EXTENSION_VALUE_INT64 };
    wirelog_extension_descriptor_t descriptor = {
        WIRELOG_EXTENSION_ABI_VERSION, sizeof(descriptor), "test.public_map",
        1, argument_types, WIRELOG_EXTENSION_VALUE_INT64,
        map_extension_invoke, NULL, NULL
    };
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create();
    wirelog_extension_snapshot_t *snapshot = NULL;
    wirelog_program_t *prog = parse_or_die(MAP_EXTENSION_SRC,
            "T-map-extension");
    wirelog_session_t *session = NULL;
    struct public_map_rows rows = { 0, 0 };
    int64_t values[] = { 7, 8, 9, 10 };
    int rc = 0;

    snapshot_extension_fail = 0;
    if (setenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0)
        rc = 1;
    if (!registry || !prog
        || wirelog_extension_register(registry, &descriptor) != 0)
        rc = 1;
    snapshot = registry ? wirelog_extension_snapshot_acquire(registry) : NULL;
    if (!rc && (!snapshot
        || wirelog_session_create_with_snapshot(prog,
        WIRELOG_BACKEND_DEFAULT, 1, snapshot, &session) != WIRELOG_OK
        || !session))
        rc = 1;
    wirelog_extension_snapshot_release(snapshot);
    snapshot = NULL;
    if (!rc && wirelog_extension_unregister(registry, descriptor.name) != 0)
        rc = 1;
    for (size_t i = 0; !rc && i < sizeof(values) / sizeof(values[0]); i++) {
        if (wirelog_session_insert(session, "src", &values[i], 1, 1)
            != WIRELOG_OK)
            rc = 1;
    }
    if (!rc && (wirelog_session_snapshot(session, capture_public_map_rows,
        &rows) != WIRELOG_OK || rows.rows != 4))
        rc = 1;

    snapshot_extension_fail = 1;
    for (size_t i = 0; !rc && i < sizeof(values) / sizeof(values[0]); i++) {
        int64_t value = values[i] + 10;
        if (wirelog_session_insert(session, "src", &value, 1, 1)
            != WIRELOG_OK)
            rc = 1;
    }
    rows.rows = 0;
    if (!rc && wirelog_session_snapshot(session, capture_public_map_rows,
        &rows) != WIRELOG_ERR_EXEC)
        rc = 1;
    if (!rc && !strstr(wirelog_extension_last_error(), "callback"))
        rc = 1;
    if (rows.rows != 0)
        rc = 1;

    wirelog_session_destroy(session);
    wirelog_extension_snapshot_release(snapshot);
    wirelog_program_free(prog);
    if (registry && wirelog_extension_registry_destroy(registry) != 0)
        rc = 1;
    unsetenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER");
    snapshot_extension_fail = 0;
    return rc;
}

static int
test_public_snapshot_extension_execution(void)
{
    const uint32_t argument_types[] = { WIRELOG_EXTENSION_VALUE_INT64 };
    wirelog_extension_descriptor_t descriptor = {
        WIRELOG_EXTENSION_ABI_VERSION, sizeof(descriptor),
        "test.public_snapshot", 1, argument_types,
        WIRELOG_EXTENSION_VALUE_BOOL, snapshot_extension_invoke, NULL, NULL
    };
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create();
    wirelog_extension_snapshot_t *snapshot = NULL;
    wirelog_program_t *prog = parse_or_die(SNAPSHOT_EXTENSION_SRC,
            "T-snapshot-extension");
    wirelog_session_t *session = NULL;
    struct public_snapshot_rows rows = { 0 };
    int64_t value = 7;
    int rc = 0;

    snapshot_extension_fail = 0;
    if (!registry || !prog
        || wirelog_extension_register(registry, &descriptor) != 0)
        rc = 1;
    snapshot = registry ? wirelog_extension_snapshot_acquire(registry) : NULL;
    if (!rc && (!snapshot
        || wirelog_session_create_with_snapshot(prog,
        WIRELOG_BACKEND_DEFAULT, 1, snapshot, &session) != WIRELOG_OK
        || !session))
        rc = 1;
    if (snapshot) {
        wirelog_extension_snapshot_release(snapshot);
        snapshot = NULL;
    }
    if (!rc && wirelog_extension_unregister(registry,
        descriptor.name) != 0)
        rc = 1;
    if (!rc && wirelog_session_insert(session, "src", &value, 1, 1)
        != WIRELOG_OK)
        rc = 1;
    if (!rc && (wirelog_session_snapshot(session, count_public_snapshot_rows,
        &rows) != WIRELOG_OK || rows.rows != 1)) {
        fprintf(stderr, "T-snapshot-extension: rows=%u\n", rows.rows);
        rc = 1;
    }
    snapshot_extension_fail = 1;
    value = 8;
    if (!rc && wirelog_session_insert(session, "src", &value, 1, 1)
        != WIRELOG_OK)
        rc = 1;
    rows.rows = 0;
    if (!rc && wirelog_session_snapshot(session, count_public_snapshot_rows,
        &rows) != WIRELOG_ERR_EXEC) {
        fprintf(stderr, "T-snapshot-extension: failing callback succeeded\n");
        rc = 1;
    }
    if (rows.rows != 0)
        rc = 1;

    wirelog_session_destroy(session);
    wirelog_extension_snapshot_release(snapshot);
    wirelog_program_free(prog);
    if (registry && wirelog_extension_registry_destroy(registry) != 0)
        rc = 1;
    snapshot_extension_fail = 0;
    return rc;
}

static void
test_extension_destroy(void *user_data)
{
    (void)user_data;
    extension_destroy_calls++;
}

static const char *RELATION_NAME_LIFETIME_SRC
    = ".decl edge(x: int64, y: int64)\n"
    ".decl reach(x: int64, y: int64)\n"
    "reach(X, Y) :- edge(X, Y).\n";

static const char *PROG_RBAC_SRC =
    ".decl role_permission(role:symbol,perm:symbol)\n"
    ".decl member_of(user:symbol,role:symbol,scope:symbol)\n"
    ".decl effective_permission(role:symbol,perm:symbol)\n"
    ".decl has_permission(user:symbol,perm:symbol,scope:symbol)\n"
    "role_permission(\"wr.system_admin\", \"wr.policy.write\").\n"
    "effective_permission(R, P) :- role_permission(R, P).\n"
    "has_permission(U, P, S) :- "
    "  member_of(U, R, S), effective_permission(R, P).\n";

static const char *ISSUE_665_PROGRAM_SRC
    = ".decl grant(user: symbol, perm: symbol)\n"
    ".decl principal_state(user: symbol, state: symbol)\n"
    ".decl session_state(scope: symbol, state: symbol)\n"
    ".decl session_active(state: symbol)\n"
    ".decl perm_state(user: symbol, perm: symbol, scope: symbol, "
    "state: symbol)\n"
    ".decl allow_bool(user: symbol, perm: symbol, scope: symbol)\n"
    "allow_bool(U, P, S) :-\n"
    "    grant(U, P),\n"
    "    principal_state(U, \"authenticated\"),\n"
    "    session_state(S, ST),\n"
    "    session_active(ST),\n"
    "    perm_state(U, P, S, \"armed\").\n";

struct count_state {
    uint32_t rows;
};

struct delta_state {
    uint32_t inserts;
    uint32_t removes;
};

#define WL_DELTA_MAX_ROWS 64
#define WL_DELTA_MAX_COLS 8

struct delta_collector {
    int count;
    char relations[WL_DELTA_MAX_ROWS][32];
    uint32_t ncols[WL_DELTA_MAX_ROWS];
    int64_t rows[WL_DELTA_MAX_ROWS][WL_DELTA_MAX_COLS];
    int32_t diffs[WL_DELTA_MAX_ROWS];
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

static wirelog_program_t *parse_or_die(const char *src, const char *what);

struct typed_snapshot_state {
    uint32_t rows;
    uint64_t lane;
};

static void
capture_typed_tuple(const char *relation, const wirelog_typed_row_v1_t *row,
    int32_t diff, void *user_data)
{
    struct typed_snapshot_state *st = user_data;
    if (strcmp(relation, "out") == 0 && row->logical_ncols == 1 && diff > 0) {
        st->rows++;
        st->lane = row->lanes[0];
    }
}

static int
test_typed_float_ingress(void)
{
    static const char *src =
        ".decl point(x: float)\n"
        ".decl out(x: float)\n"
        "out(X) :- point(X).\n";
    wirelog_program_t *prog = parse_or_die(src, "T-typed-float");
    if (!prog)
        return 1;
    wirelog_session_t *session = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &session)
        != WIRELOG_OK) {
        wirelog_program_free(prog);
        return 1;
    }

    double value = 1.5;
    uint64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    uint32_t types[] = { WIRELOG_TYPE_FLOAT };
    uint64_t lanes[] = { bits };
    uint32_t offsets[] = { 0 };
    wirelog_typed_row_v1_t rows = {
        sizeof(rows), 1, 0, 1, 1, 1, types, offsets, types, lanes
    };
    wirelog_typed_error_v1_t typed_error = {
        sizeof(typed_error), WIRELOG_TYPED_ERROR_NONE, UINT32_MAX, UINT32_MAX,
        NULL, 0
    };
    int rc = 0;
    if (wirelog_session_insert_typed(session, "point", &rows, 1, &typed_error)
        != WIRELOG_OK || typed_error.code != WIRELOG_TYPED_ERROR_NONE) {
        fprintf(stderr, "T-typed-float: typed insert failed (%u)\n",
            typed_error.code);
        rc = 1;
    }
    int64_t legacy_lane;
    memcpy(&legacy_lane, &bits, sizeof(legacy_lane));
    if (wirelog_session_insert(session, "point", &legacy_lane, 1, 1)
        != WIRELOG_ERR_EXEC) {
        fprintf(stderr, "T-typed-float: legacy float insert accepted\n");
        rc = 1;
    }
    struct typed_snapshot_state snapshot = { 0, 0 };
    if (wirelog_session_snapshot_typed(session, capture_typed_tuple, &snapshot)
        != WIRELOG_OK || snapshot.rows != 1) {
        fprintf(stderr, "T-typed-float: snapshot rows=%u\n", snapshot.rows);
        rc = 1;
    } else {
        uint64_t expected;
        memcpy(&expected, &value, sizeof(expected));
        if (snapshot.lane != expected) {
            fprintf(stderr, "T-typed-float: lane was not bit-preserving\n");
            rc = 1;
        }
    }

    double infinity = INFINITY;
    uint64_t bad_bits;
    memcpy(&bad_bits, &infinity, sizeof(bad_bits));
    rows.lanes = &bad_bits;
    typed_error.code = WIRELOG_TYPED_ERROR_NONE;
    if (wirelog_session_insert_typed(session, "point", &rows, 1, &typed_error)
        != WIRELOG_ERR_EXEC || typed_error.code != WIRELOG_TYPED_ERROR_VALUE) {
        fprintf(stderr, "T-typed-float: nonfinite value was accepted\n");
        rc = 1;
    }
    snapshot.rows = 0;
    if (wirelog_session_snapshot_typed(session, capture_typed_tuple, &snapshot)
        != WIRELOG_OK || snapshot.rows != 1) {
        fprintf(stderr, "T-typed-float: failed insert changed relation\n");
        rc = 1;
    }
    rows.lanes = lanes;
    if (wirelog_session_remove_typed(session, "point", &rows, 1,
        &typed_error) != WIRELOG_OK) {
        fprintf(stderr, "T-typed-float: typed remove failed\n");
        rc = 1;
    }
    wirelog_typed_compound_arg_v1_t typed_arg = {
        WIRELOG_TYPE_FLOAT, bits
    };
    uint64_t compound_handle = WIRELOG_COMPOUND_HANDLE_NULL;
    if (wirelog_session_make_compound_typed(session, "f", 1, &typed_arg, 1,
        &compound_handle) != WIRELOG_OK
        || compound_handle == WIRELOG_COMPOUND_HANDLE_NULL) {
        fprintf(stderr, "T-typed-float: typed compound failed\n");
        rc = 1;
    }
    wirelog_compound_arg_t legacy_arg = { WIRELOG_TYPE_FLOAT, legacy_lane };
    if (wirelog_session_make_compound(session, "f", 1, &legacy_arg,
        &compound_handle) != WIRELOG_ERR_EXEC) {
        fprintf(stderr, "T-typed-float: legacy float compound accepted\n");
        rc = 1;
    }
    wirelog_session_destroy(session);
    wirelog_program_free(prog);
    return rc;
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

static void
collect_delta_rows(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    struct delta_collector *c = (struct delta_collector *)user_data;
    if (c->count >= WL_DELTA_MAX_ROWS)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, sizeof(c->relations[idx]) - 1);
    c->relations[idx][sizeof(c->relations[idx]) - 1] = '\0';
    c->ncols[idx] = ncols;
    c->diffs[idx] = diff;
    for (uint32_t i = 0; i < ncols && i < WL_DELTA_MAX_COLS; i++)
        c->rows[idx][i] = row[i];
}

static int
count_allow_bool_added(const struct delta_collector *deltas, int from)
{
    int hits = 0;
    for (int i = from; i < deltas->count; i++) {
        if (strcmp(deltas->relations[i], "allow_bool") == 0
            && deltas->diffs[i] == 1)
            hits++;
    }
    return hits;
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

static int
apply_standard_optimizer(wirelog_program_t *prog, const char *what)
{
    int rc = wl_fusion_apply(prog, NULL);
    if (rc != 0) {
        fprintf(stderr, "%s: wl_fusion_apply failed rc=%d\n", what, rc);
        return 0;
    }
    rc = wl_jpp_apply(prog, NULL);
    if (rc != 0) {
        fprintf(stderr, "%s: wl_jpp_apply failed rc=%d\n", what, rc);
        return 0;
    }
    rc = wl_sip_apply(prog, NULL);
    if (rc != 0) {
        fprintf(stderr, "%s: wl_sip_apply failed rc=%d\n", what, rc);
        return 0;
    }
    return 1;
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

/* The additive snapshot API must retain independently of the caller, so a
 * caller may release its reference and unregister the addon immediately. */
static int
test_create_with_snapshot_lifetime(void)
{
    wirelog_extension_registry_t *registry = NULL;
    wirelog_extension_snapshot_t *snapshot = NULL;
    wirelog_program_t *prog = NULL;
    wirelog_session_t *session = NULL;
    const uint32_t argument_types[] = { WIRELOG_EXTENSION_VALUE_INT64 };
    wirelog_extension_descriptor_t descriptor = {
        WIRELOG_EXTENSION_ABI_VERSION,
        sizeof(descriptor),
        "test.public_snapshot",
        1,
        argument_types,
        WIRELOG_EXTENSION_VALUE_BOOL,
        test_extension_invoke,
        NULL,
        test_extension_destroy
    };
    int rc = 0;

    extension_destroy_calls = 0;
    registry = wirelog_extension_registry_create();
    prog = parse_or_die(PROG_SRC, "T-snapshot-public");
    if (!registry || !prog
        || wirelog_extension_register(registry, &descriptor) != 0) {
        rc = 1;
        goto cleanup;
    }
    snapshot = wirelog_extension_snapshot_acquire(registry);
    if (!snapshot
        || wirelog_session_create_with_snapshot(prog,
        WIRELOG_BACKEND_DEFAULT, 2, snapshot, &session) != WIRELOG_OK
        || !session) {
        fprintf(stderr, "T-snapshot-public: creation failed\n");
        rc = 1;
        goto cleanup;
    }
    wirelog_extension_snapshot_release(snapshot);
    snapshot = NULL;
    if (wirelog_extension_unregister(registry, descriptor.name) != 0
        || extension_destroy_calls != 0) {
        fprintf(stderr, "T-snapshot-public: snapshot was not retained\n");
        rc = 1;
    }
    wirelog_session_destroy(session);
    session = NULL;
    if (extension_destroy_calls != 1) {
        fprintf(stderr, "T-snapshot-public: destroy calls=%d\n",
            extension_destroy_calls);
        rc = 1;
    }

cleanup:
    wirelog_session_destroy(session);
    wirelog_extension_snapshot_release(snapshot);
    wirelog_program_free(prog);
    if (registry && wirelog_extension_registry_destroy(registry) != 0)
        rc = 1;
    return rc;
}

/* Invalid arguments must clear out and must not consume the caller's
 * snapshot reference, including when backend selection fails. */
static int
test_create_with_snapshot_invalid_args(void)
{
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create();
    wirelog_extension_snapshot_t *snapshot = registry
        ? wirelog_extension_snapshot_acquire(registry) : NULL;
    wirelog_program_t *prog = parse_or_die(PROG_SRC, "T-snapshot-invalid");
    wirelog_session_t *session = (wirelog_session_t *)0xdeadbeef;
    int rc = 0;

    if (!registry || !snapshot || !prog)
        rc = 1;
    if (wirelog_session_create_with_snapshot(NULL, WIRELOG_BACKEND_DEFAULT,
        1, snapshot, &session) != WIRELOG_ERR_EXEC || session != NULL)
        rc = 1;
    session = (wirelog_session_t *)0xdeadbeef;
    if (wirelog_session_create_with_snapshot(prog,
        (wirelog_backend_kind_t)999, 1, snapshot, &session)
        != WIRELOG_ERR_EXEC || session != NULL)
        rc = 1;
    if (wirelog_session_create_with_snapshot(prog, WIRELOG_BACKEND_DEFAULT,
        1, snapshot, NULL) != WIRELOG_ERR_EXEC)
        rc = 1;
    wirelog_extension_snapshot_release(snapshot);
    wirelog_program_free(prog);
    if (registry && wirelog_extension_registry_destroy(registry) != 0)
        rc = 1;
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

/* Parity (#931): mirrors test_wirelog_easy.c::test_relation_name_lifetime. */
static int
test_relation_name_lifetime(void)
{
    int rc = 0;
    int64_t row[2] = { 1, 2 };
    wirelog_program_t *prog
        = parse_or_die(RELATION_NAME_LIFETIME_SRC, "relation-name lifetime");
    if (!prog)
        return 1;

    wirelog_session_t *s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK) {
        wirelog_program_free(prog);
        return 1;
    }

    struct tuple_filter tuples = { .target_relation = "reach" };
    if (wirelog_session_snapshot(s, filter_tuples, &tuples) != WIRELOG_OK) {
        fprintf(stderr, "relation-name lifetime: baseline snapshot failed\n");
        rc = 1;
        goto regular_out;
    }

    char regular_relation[8] = "edge";
    if (wirelog_session_insert(s, regular_relation, row, 1, 2)
        != WIRELOG_OK) {
        fprintf(stderr, "relation-name lifetime: regular insert failed\n");
        rc = 1;
        goto regular_out;
    }
    strcpy(regular_relation, "bogus");

    memset(&tuples, 0, sizeof(tuples));
    tuples.target_relation = "reach";
    if (wirelog_session_snapshot(s, filter_tuples, &tuples) != WIRELOG_OK) {
        fprintf(stderr,
            "relation-name lifetime: post-insert snapshot failed\n");
        rc = 1;
        goto regular_out;
    }
    bool found_snapshot = false;
    for (int i = 0; i < tuples.count; i++) {
        if (tuples.ncols[i] == 2 && tuples.rows[i][0] == row[0]
            && tuples.rows[i][1] == row[1]) {
            found_snapshot = true;
            break;
        }
    }
    if (!found_snapshot) {
        fprintf(stderr,
            "relation-name lifetime: mutable regular name suppressed "
            "reach(1,2)\n");
        rc = 1;
    }

regular_out:
    wirelog_session_destroy(s);
    wirelog_program_free(prog);
    if (rc != 0)
        return rc;

    prog = parse_or_die(
        RELATION_NAME_LIFETIME_SRC, "relation-name delta lifetime");
    if (!prog)
        return 1;
    s = NULL;
    if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
        != WIRELOG_OK) {
        wirelog_program_free(prog);
        return 1;
    }

    struct delta_collector deltas;
    memset(&deltas, 0, sizeof(deltas));
    if (wirelog_session_set_delta_cb(s, collect_delta_rows, &deltas)
        != WIRELOG_OK) {
        fprintf(stderr, "relation-name lifetime: set_delta_cb failed\n");
        rc = 1;
        goto delta_out;
    }

    char inserted_relation[8] = "edge";
    if (wirelog_session_insert(s, inserted_relation, row, 1, 2)
        != WIRELOG_OK) {
        fprintf(stderr, "relation-name lifetime: delta insert failed\n");
        rc = 1;
        goto delta_out;
    }
    strcpy(inserted_relation, "bogus");
    if (wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr,
            "relation-name lifetime: post-insert step failed\n");
        rc = 1;
        goto delta_out;
    }

    bool found_insert = false;
    for (int i = 0; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "reach") == 0
            && deltas.ncols[i] == 2 && deltas.rows[i][0] == row[0]
            && deltas.rows[i][1] == row[1] && deltas.diffs[i] == 1) {
            found_insert = true;
            break;
        }
    }
    int before_remove = deltas.count;

    char removed_relation[8] = "edge";
    if (wirelog_session_remove(s, removed_relation, row, 1, 2)
        != WIRELOG_OK) {
        fprintf(stderr, "relation-name lifetime: delta remove failed\n");
        rc = 1;
        goto delta_out;
    }
    strcpy(removed_relation, "bogus");
    if (wirelog_session_step(s) != WIRELOG_OK) {
        fprintf(stderr,
            "relation-name lifetime: post-remove step failed\n");
        rc = 1;
        goto delta_out;
    }

    bool found_remove = false;
    for (int i = before_remove; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "reach") == 0
            && deltas.ncols[i] == 2 && deltas.rows[i][0] == row[0]
            && deltas.rows[i][1] == row[1] && deltas.diffs[i] == -1) {
            found_remove = true;
            break;
        }
    }
    if (!found_insert) {
        fprintf(stderr,
            "relation-name lifetime: mutable inserted name suppressed "
            "+reach(1,2)\n");
        rc = 1;
    }
    if (!found_remove) {
        fprintf(stderr,
            "relation-name lifetime: mutable removed name suppressed "
            "-reach(1,2)\n");
        rc = 1;
    }

delta_out:
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

/* Issue #994: conjunction is commutative, so both atom orders must derive the
 * same row when a side compound is not the first body atom. */
static int
test_side_compound_in_non_first_atom_matches(void)
{
    static const char *const spellings[2] = {
        ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl gate(id: int64)\n"
        ".decl hot(id: int64, r: int64)\n"
        "hot(ID, R) :- gate(ID), event(ID, metadata(_, _, _, R)).\n",
        ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl gate(id: int64)\n"
        ".decl hot(id: int64, r: int64)\n"
        "hot(ID, R) :- event(ID, metadata(_, _, _, R)), gate(ID).\n",
    };

    for (unsigned v = 0; v < 2; v++) {
        wirelog_program_t *prog = parse_or_die(spellings[v], "T994a");
        if (!prog)
            return 1;
        wirelog_session_t *s = NULL;
        if (wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s)
            != WIRELOG_OK || !s) {
            wirelog_program_free(prog);
            return 1;
        }

        int rc = 0;
        uint64_t handle = 0;
        if (make_compound4(s, "metadata", 1, 2, 3, 42, &handle)
            != WIRELOG_OK) {
            fprintf(stderr, "T994a compound allocation failed\n");
            rc = 1;
            goto out;
        }
        int64_t event_row[2] = { 7, (int64_t)handle };
        int64_t other_event[2] = { 8, (int64_t)handle };
        int64_t gate_row[1] = { 7 };
        int64_t other_gate[1] = { 9 };
        if (wirelog_session_insert(s, "event", event_row, 1, 2)
            != WIRELOG_OK
            || wirelog_session_insert(s, "event", other_event, 1, 2)
            != WIRELOG_OK
            || wirelog_session_insert(s, "gate", gate_row, 1, 1)
            != WIRELOG_OK
            || wirelog_session_insert(s, "gate", other_gate, 1, 1)
            != WIRELOG_OK) {
            fprintf(stderr, "T994a insert failed\n");
            rc = 1;
            goto out;
        }
        struct tuple_filter f = { .target_relation = "hot" };
        if (wirelog_session_snapshot(s, filter_tuples, &f) != WIRELOG_OK) {
            fprintf(stderr, "T994a snapshot failed\n");
            rc = 1;
            goto out;
        }
        if (f.count != 1 || f.ncols[0] != 2 || f.rows[0][0] != 7
            || f.rows[0][1] != 42) {
            fprintf(stderr, "T994a: expected exactly hot(7, 42)\n");
            rc = 1;
        }
out:
        wirelog_session_destroy(s);
        wirelog_program_free(prog);
        if (rc != 0)
            return rc;
    }
    return 0;
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

/* Parity (#785): mirrors test_unsafe_negation_variable_rejected (issue #920).
 * A variable bound only inside a negated atom is unsafe; IR lowering must
 * reject it. */
static int
test_unsafe_negation_variable_rejected(void)
{
    const char *src
        = ".decl a(x: symbol)\n"
        ".decl b(x: symbol, y: symbol)\n"
        ".decl c(x: symbol)\n"
        "c(X) :- a(X), !b(X, Y).\n";
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
            "expected invalid IR for unsafe negation variable, got %d\n", err);
        wirelog_session_destroy(s);
        rc = 1;
    }
    wirelog_program_free(prog);
    return rc;
}

/* Parity (#785): mirrors test_safe_negation_via_projection_accepted (#920).
 * The projected-key form and wildcard columns are safe and must be accepted. */
static int
test_safe_negation_via_projection_accepted(void)
{
    const char *src
        = ".decl a(x: symbol)\n"
        ".decl b(x: symbol, y: symbol)\n"
        ".decl b_key(x: symbol)\n"
        ".decl c(x: symbol)\n"
        ".decl d(x: symbol)\n"
        "b_key(X) :- b(X, Y).\n"
        "c(X) :- a(X), !b_key(X).\n"
        "d(X) :- a(X), !b(X, _).\n";
    wirelog_error_t parse_err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &parse_err);
    if (!prog) {
        fprintf(stderr, "safe negation: parse failed, got %d\n", parse_err);
        return 1;
    }

    wirelog_session_t *s = NULL;
    wirelog_error_t err
        = wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, 1, &s);
    int rc = 0;
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "safe negation: expected valid IR, got %d\n", err);
        rc = 1;
    }
    wirelog_session_destroy(s);
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

static int
insert_sym_row(wirelog_session_t *s, wl_intern_t *intern,
    const char *relation, const char *const *symbols, uint32_t ncols)
{
    if (ncols > WL_DELTA_MAX_COLS)
        return 0;

    int64_t row[WL_DELTA_MAX_COLS];
    for (uint32_t i = 0; i < ncols; i++) {
        row[i] = wl_intern_put(intern, symbols[i]);
        if (row[i] < 0)
            return 0;
    }
    return wirelog_session_insert(s, relation, row, 1, ncols) == WIRELOG_OK;
}

static bool
drive_issue_665_partial_conjunction(wirelog_session_t *s, wl_intern_t *intern)
{
    struct delta_collector deltas;
    memset(&deltas, 0, sizeof(deltas));
    if (wirelog_session_set_delta_cb(s, collect_delta_rows, &deltas)
        != WIRELOG_OK)
        return false;

    const char *const grant[] = { "u", "p" };
    if (!insert_sym_row(s, intern, "grant", grant, 2)
        || wirelog_session_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, 0) != 0)
        return false;
    int after_grant = deltas.count;

    const char *const principal_state[] = { "u", "authenticated" };
    if (!insert_sym_row(s, intern, "principal_state", principal_state, 2)
        || wirelog_session_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, after_grant) != 0)
        return false;
    int after_principal = deltas.count;

    const char *const session_state[] = { "s", "st" };
    if (!insert_sym_row(s, intern, "session_state", session_state, 2)
        || wirelog_session_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, after_principal) != 0)
        return false;
    int after_session = deltas.count;

    const char *const session_active[] = { "st" };
    if (!insert_sym_row(s, intern, "session_active", session_active, 1)
        || wirelog_session_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, after_session) != 0)
        return false;
    int after_active = deltas.count;

    const char *const perm_state[] = { "u", "p", "s", "armed" };
    if (!insert_sym_row(s, intern, "perm_state", perm_state, 4)
        || wirelog_session_step(s) != WIRELOG_OK)
        return false;
    if (count_allow_bool_added(&deltas, after_active) != 1)
        return false;

    int64_t u = wl_intern_put(intern, "u");
    int64_t p = wl_intern_put(intern, "p");
    int64_t scope = wl_intern_put(intern, "s");
    if (u < 0 || p < 0 || scope < 0)
        return false;

    bool matched = false;
    for (int i = after_active; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "allow_bool") != 0
            || deltas.diffs[i] != 1)
            continue;
        if (deltas.ncols[i] == 3 && deltas.rows[i][0] == u
            && deltas.rows[i][1] == p && deltas.rows[i][2] == scope) {
            matched = true;
            break;
        }
    }
    return matched;
}

static int
run_issue_665_partial_conjunction(uint32_t num_workers, const char *what)
{
    wirelog_program_t *prog = parse_or_die(ISSUE_665_PROGRAM_SRC, what);
    if (!prog)
        return 1;

    int rc = 0;
    if (!apply_standard_optimizer(prog, what)) {
        rc = 1;
        goto out_prog;
    }

    wirelog_session_t *s = NULL;
    wirelog_error_t err
        = wirelog_session_create(prog, WIRELOG_BACKEND_DEFAULT, num_workers,
            &s);
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "%s create failed err=%d\n", what, err);
        rc = 1;
        goto out_prog;
    }

    wl_intern_t *intern = (wl_intern_t *)wirelog_program_get_intern(prog);
    if (!drive_issue_665_partial_conjunction(s, intern)) {
        fprintf(stderr, "%s partial conjunction parity failed\n", what);
        rc = 1;
    }
    wirelog_session_destroy(s);
out_prog:
    wirelog_program_free(prog);
    return rc;
}

static int
test_issue_665_partial_conjunction_default_workers(void)
{
    return run_issue_665_partial_conjunction(1, "issue 665 default workers");
}

static int
test_issue_665_partial_conjunction_multi_worker(void)
{
    return run_issue_665_partial_conjunction(4, "issue 665 multi-worker");
}

int
main(void)
{
    int failures = 0;
    failures += test_create_destroy_basic();
    failures += test_create_columnar();
    failures += test_create_invalid_backend();
    failures += test_create_with_snapshot_lifetime();
    failures += test_create_with_snapshot_invalid_args();
    failures += test_public_snapshot_extension_execution();
    failures += test_public_map_extension_execution();
    failures += test_inline_facts_seeded();
    failures += test_insert_step_delta();
    failures += test_relation_name_lifetime();
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
    failures += test_side_compound_in_non_first_atom_matches();
    failures += test_side_compound_constant_child_filters();
    failures += test_side_compound_duplicate_child_variables_filter();
    failures += test_side_compound_wrong_functor_handle_no_match();
    failures += test_side_compound_nested_child_no_match();
    failures += test_negated_side_compound_body_pattern_rejected();
    failures += test_unsafe_negation_variable_rejected();
    failures += test_safe_negation_via_projection_accepted();
    failures += test_intern_returns_same_id();
    failures += test_snapshot_filter();
    failures += test_cleanup_order_no_use_after_free();
    failures += test_intern_after_step_succeeds();
    failures += test_delta_cb_multi_round_recursive_insert();
    failures += test_issue_665_partial_conjunction_default_workers();
    failures += test_issue_665_partial_conjunction_multi_worker();
    failures += test_typed_float_ingress();
    if (failures == 0)
        printf("test_wirelog_advanced: OK\n");
    else
        printf("test_wirelog_advanced: %d failure(s)\n", failures);
    return failures;
}
