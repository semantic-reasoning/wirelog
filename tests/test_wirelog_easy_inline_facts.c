/*
 * test_wl_easy_inline_facts.c - Issue #718 regression test.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Static facts declared in a `.dl` program must materialize into snapshots
 * and IDB derivations when the host opens the program through wirelog_easy.
 * Pre-fix, wirelog_easy never invoked wl_session_load_facts, so static rows
 * never reached col_rel_t and every downstream observation (snapshot,
 * derived IDB, host-mirrored insert) silently disagreed with the .dl
 * program.
 */

#include "wirelog/wirelog-easy.h"

#include <stdint.h>
#include <stdio.h>

static const char *PROG_SRC =
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

/* T1: a static-only EDB must drive a derived IDB during snapshot
*     evaluation (single-rule case).  This is the simplest shape of
*     #718: without inline-fact seeding the IDB has zero rows. */
static int
test_static_fact_drives_idb_snapshot(void)
{
    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(PROG_SRC, &s);
    if (err != WIRELOG_OK || !s)
        return 1;

    struct count_state st = { 0 };
    err = wirelog_easy_snapshot(s, "effective_permission", count_rows, &st);
    int rc = 0;
    if (err != WIRELOG_OK) {
        fprintf(stderr, "T1 snapshot err=%d\n", err);
        rc = 1;
    } else if (st.rows != 1) {
        fprintf(stderr,
            "T1: expected 1 effective_permission row, got %u\n", st.rows);
        rc = 1;
    }
    wirelog_easy_close(s);
    return rc;
}

/* T2: a host-inserted EDB row must compose with a static .dl fact across
 *     a multi-body rule (the issue's RBAC join shape). */
static int
test_static_fact_joins_with_host_insert(void)
{
    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(PROG_SRC, &s);
    if (err != WIRELOG_OK || !s)
        return 1;

    int64_t alice = wirelog_easy_intern(s, "alice");
    int64_t admin = wirelog_easy_intern(s, "wr.system_admin");
    int64_t global = wirelog_easy_intern(s, "global");
    if (alice < 0 || admin < 0 || global < 0) {
        wirelog_easy_close(s);
        return 1;
    }
    int64_t row[3] = { alice, admin, global };
    err = wirelog_easy_insert(s, "member_of", row, 3);
    if (err != WIRELOG_OK) {
        fprintf(stderr, "T2 insert err=%d\n", err);
        wirelog_easy_close(s);
        return 1;
    }

    struct count_state st = { 0 };
    err = wirelog_easy_snapshot(s, "has_permission", count_rows, &st);
    int rc = 0;
    if (err != WIRELOG_OK) {
        fprintf(stderr, "T2 snapshot err=%d\n", err);
        rc = 1;
    } else if (st.rows != 1) {
        fprintf(stderr,
            "T2: expected 1 has_permission row, got %u\n", st.rows);
        rc = 1;
    }
    wirelog_easy_close(s);
    return rc;
}

int
main(void)
{
    int failures = 0;
    failures += test_static_fact_drives_idb_snapshot();
    failures += test_static_fact_joins_with_host_insert();
    if (failures == 0)
        printf("test_wl_easy_inline_facts: OK\n");
    else
        printf("test_wl_easy_inline_facts: %d failure(s)\n", failures);
    return failures;
}
