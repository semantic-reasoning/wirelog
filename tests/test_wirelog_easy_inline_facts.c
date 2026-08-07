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

/* T3 (issue #977): inline facts whose arity matches the .decl must still
 *     evaluate to exactly the source tuples -- values, not just row counts.
 *
 *     This is the positive control for the fact-arity validation pass added
 *     in wl_ir_program_collect_metadata().  The pass rejects arity
 *     mismatches at parse time; this asserts it did not disturb the
 *     matching case.  Value equality matters here because the pre-fix
 *     wider-fact bug (val(1,5,7). against .decl val/2) evaluated cleanly
 *     with exit 0 while emitting t(7,2) -- a tuple present in no source
 *     fact -- and dropping (6,8).  A row-count-only assertion would not
 *     have caught that: the row count was right. */
struct pair_state {
    uint32_t rows;
    int64_t seen[8][2];
};

static void
collect_pairs(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    struct pair_state *st = (struct pair_state *)user_data;
    if (ncols != 2 || st->rows >= 8) {
        st->rows = 0xFFFFFFFFu;
        return;
    }
    st->seen[st->rows][0] = row[0];
    st->seen[st->rows][1] = row[1];
    st->rows++;
}

static int
has_pair(const struct pair_state *st, int64_t a, int64_t b)
{
    for (uint32_t i = 0; i < st->rows; i++) {
        if (st->seen[i][0] == a && st->seen[i][1] == b)
            return 1;
    }
    return 0;
}

static int
test_matching_arity_facts_evaluate_exactly(void)
{
    static const char *src = ".decl val(g: int32, v: int32)\n"
        ".decl t(x: int32, y: int32)\n"
        "val(1, 5).\n"
        "val(2, 6).\n"
        "t(x, y) :- val(x, y).\n";

    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(src, &s);
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "T3 open err=%d\n", err);
        return 1;
    }

    struct pair_state st = { 0, { { 0, 0 } } };
    err = wirelog_easy_snapshot(s, "t", collect_pairs, &st);
    int rc = 0;
    if (err != WIRELOG_OK) {
        fprintf(stderr, "T3 snapshot err=%d\n", err);
        rc = 1;
    } else if (st.rows != 2) {
        fprintf(stderr, "T3: expected 2 t rows, got %u\n", st.rows);
        rc = 1;
    } else if (!has_pair(&st, 1, 5) || !has_pair(&st, 2, 6)) {
        fprintf(stderr, "T3: expected exactly t(1,5) and t(2,6), got "
            "t(%lld,%lld) and t(%lld,%lld)\n",
            (long long)st.seen[0][0], (long long)st.seen[0][1],
            (long long)st.seen[1][0], (long long)st.seen[1][1]);
        rc = 1;
    }
    wirelog_easy_close(s);
    return rc;
}

/* T4 (issue #977 unit 3): the flattened inline-compound rule head must still
 *     evaluate, and still produce all three physical columns.
 *
 *     This is the positive control for the rule-head arity pass comparing
 *     *physical* width rather than the logical width unit 2 uses for facts.
 *     `pred` is declared with two logical columns (id, payload: f/2 inline)
 *     and three physical ones; `pred(x, y, z)` is three head arguments, and
 *     it is the only spelling the grammar allows, because parse_head_arg()
 *     has no compound-term production and `pred(x, f(y, z))` is a parse
 *     error.  A logical comparison would reject this program (3 != 2).
 *
 *     tests/test_program.c asserts the lowering; that binary links only
 *     parser/ir/io/thread and cannot evaluate.  This one links the full
 *     library, so it can assert the values -- which is what distinguishes
 *     "accepted" from "accepted and correct": the 2-argument spelling is
 *     also accepted by a logical check and evaluates to a fabricated
 *     unwritten slot. */
struct triple_state {
    uint32_t rows;
    int64_t seen[8][3];
};

static void
collect_triples(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    struct triple_state *st = (struct triple_state *)user_data;
    if (ncols != 3 || st->rows >= 8) {
        st->rows = 0xFFFFFFFFu;
        return;
    }
    st->seen[st->rows][0] = row[0];
    st->seen[st->rows][1] = row[1];
    st->seen[st->rows][2] = row[2];
    st->rows++;
}

static int
test_inline_compound_head_evaluates_exactly(void)
{
    static const char *src = ".decl src(a: int64, b: int64, c: int64)\n"
        ".decl pred(id: int64, payload: f/2 inline)\n"
        "src(1, 10, 20).\n"
        "pred(x, y, z) :- src(x, y, z).\n";

    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(src, &s);
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "T4 open err=%d (flattened inline-compound head must "
            "be accepted; a logical arity check rejects it)\n", err);
        return 1;
    }

    struct triple_state st = { 0, { { 0, 0, 0 } } };
    err = wirelog_easy_snapshot(s, "pred", collect_triples, &st);
    int rc = 0;
    if (err != WIRELOG_OK) {
        fprintf(stderr, "T4 snapshot err=%d\n", err);
        rc = 1;
    } else if (st.rows != 1) {
        fprintf(stderr, "T4: expected 1 pred row, got %u\n", st.rows);
        rc = 1;
    } else if (st.seen[0][0] != 1 || st.seen[0][1] != 10
        || st.seen[0][2] != 20) {
        fprintf(stderr,
            "T4: expected pred(1,10,20), got pred(%lld,%lld,%lld)\n",
            (long long)st.seen[0][0], (long long)st.seen[0][1],
            (long long)st.seen[0][2]);
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
    failures += test_matching_arity_facts_evaluate_exactly();
    failures += test_inline_compound_head_evaluates_exactly();
    if (failures == 0)
        printf("test_wl_easy_inline_facts: OK\n");
    else
        printf("test_wl_easy_inline_facts: %d failure(s)\n", failures);
    return failures;
}
