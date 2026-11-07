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
#include "wirelog/backend.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/session.h"
#include "wirelog/session_facts.h"
#include "wirelog/wirelog.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

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

static int
has_triple(const struct triple_state *st, int64_t a, int64_t b, int64_t c)
{
    for (uint32_t i = 0; i < st->rows; i++) {
        if (st->seen[i][0] == a && st->seen[i][1] == b
            && st->seen[i][2] == c)
            return 1;
    }
    return 0;
}

/* T5 (issue #985): a flat inline-compound *fact* must be accepted and must
 *     coexist with the flattened rule-head spelling at the same width.
 *
 *     Pre-fix `pred(7, 98, 99).` was rejected outright (three arguments
 *     against a two-column .decl, compared logically) while the broken
 *     two-argument `pred(7, 99).` was accepted -- so the relation had no
 *     working fact syntax at all.  Worse, the accepted form fixed the
 *     relation's runtime width at 2, so the rule's three columns were then
 *     truncated: the issue's own reproducer emitted pred(7, 99) and
 *     pred(1, 10), dropping the 20.
 *
 *     ncols is asserted inside the callback, which is where the truncation
 *     is visible; collect_triples() poisons st->rows if it is not 3.  Both
 *     rows are asserted by value and order-insensitively, because the row
 *     *count* is 2 either way -- a count-only assertion passes on the bug. */
static int
test_inline_compound_flat_fact_evaluates_exactly(void)
{
    static const char *src = ".decl src(a: int64, b: int64, c: int64)\n"
        ".decl pred(id: int64, payload: f/2 inline)\n"
        "src(1, 10, 20).\n"
        "pred(7, 98, 99).\n"
        "pred(x, y, z) :- src(x, y, z).\n";

    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(src, &s);
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "T5 open err=%d (flat inline-compound fact must be "
            "accepted; a logical arity check rejects it)\n", err);
        return 1;
    }

    struct triple_state st = { 0, { { 0, 0, 0 } } };
    err = wirelog_easy_snapshot(s, "pred", collect_triples, &st);
    int rc = 0;
    if (err != WIRELOG_OK) {
        fprintf(stderr, "T5 snapshot err=%d\n", err);
        rc = 1;
    } else if (st.rows != 2) {
        fprintf(stderr, "T5: expected 2 pred rows at ncols 3, got %u\n",
            st.rows);
        rc = 1;
    } else if (!has_triple(&st, 7, 98, 99) || !has_triple(&st, 1, 10, 20)) {
        fprintf(stderr, "T5: expected pred(7,98,99) and pred(1,10,20), got "
            "pred(%lld,%lld,%lld) and pred(%lld,%lld,%lld)\n",
            (long long)st.seen[0][0], (long long)st.seen[0][1],
            (long long)st.seen[0][2], (long long)st.seen[1][0],
            (long long)st.seen[1][1], (long long)st.seen[1][2]);
        rc = 1;
    }
    wirelog_easy_close(s);
    return rc;
}

/* T6 (issue #985): the handle-form fact must fail to open.
 *
 *     `pred(1, 99).` leaves the second inline slot never written, and
 *     `outr(id, p, q) :- pred(id, f(p, q)).` destructures the column and
 *     reads it regardless -- outr(1, 99, 0) pre-fix, exit 0, ASAN silent.
 *     The 0 is present in no source data and there is no honest default to
 *     put there, so the program is rejected at load.
 *
 *     T7 is this test's positive control and is not optional: without it a
 *     change that rejected *everything* would leave T6 green. */
static int
test_inline_compound_handle_fact_rejected(void)
{
    static const char *src = ".decl pred(id: int64, payload: f/2 inline)\n"
        ".decl outr(id: int64, p: int64, q: int64)\n"
        "pred(1, 99).\n"
        "outr(id, p, q) :- pred(id, f(p, q)).\n";

    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(src, &s);
    if (err == WIRELOG_OK) {
        fprintf(stderr, "T6: pred(1,99). against an f/2 inline .decl must "
            "not open (it fabricates outr(1,99,0))\n");
        wirelog_easy_close(s);
        return 1;
    }
    return 0;
}

/* T7 (issue #985): positive control for T6, and the assertion that the
 *     fabricated 0 is gone.
 *
 *     Same destructuring rule, the fact written flat.  The second inline
 *     slot is now written, so the body pattern reads real data and outr
 *     carries the source values instead of a slot nobody filled in. */
static int
test_inline_compound_flat_fact_destructures_exactly(void)
{
    static const char *src = ".decl pred(id: int64, payload: f/2 inline)\n"
        ".decl outr(id: int64, p: int64, q: int64)\n"
        "pred(1, 98, 99).\n"
        "outr(id, p, q) :- pred(id, f(p, q)).\n";

    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(src, &s);
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "T7 open err=%d\n", err);
        return 1;
    }

    struct triple_state st = { 0, { { 0, 0, 0 } } };
    err = wirelog_easy_snapshot(s, "outr", collect_triples, &st);
    int rc = 0;
    if (err != WIRELOG_OK) {
        fprintf(stderr, "T7 snapshot err=%d\n", err);
        rc = 1;
    } else if (st.rows != 1) {
        fprintf(stderr, "T7: expected 1 outr row at ncols 3, got %u\n",
            st.rows);
        rc = 1;
    } else if (st.seen[0][0] != 1 || st.seen[0][1] != 98
        || st.seen[0][2] != 99) {
        fprintf(stderr,
            "T7: expected outr(1,98,99), got outr(%lld,%lld,%lld)\n",
            (long long)st.seen[0][0], (long long)st.seen[0][1],
            (long long)st.seen[0][2]);
        rc = 1;
    }
    wirelog_easy_close(s);
    return rc;
}

/* T8 (issue #957): a single-rule IDB is set-valued, and duplicate rows must
 * not multiply a downstream join.  The two source facts project to one
 * `projected` tuple; joining that IDB back to source therefore yields the
 * two distinct source pairs, not four raw join products. */
static int
test_single_rule_idb_is_set_valued(void)
{
    static const char *src =
        ".decl source(x: int32, y: int32)\n"
        ".decl projected(x: int32)\n"
        ".decl downstream(x: int32, y: int32)\n"
        "source(1, 10).\n"
        "source(1, 20).\n"
        "projected(X) :- source(X, Y).\n"
        "downstream(X, Y) :- projected(X), source(X, Y).\n";

    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(src, &s);
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "T8 open err=%d\n", err);
        return 1;
    }

    struct count_state projected = { 0 };
    struct count_state downstream = { 0 };
    err = wirelog_easy_snapshot(s, "projected", count_rows, &projected);
    if (err == WIRELOG_OK)
        err = wirelog_easy_snapshot(s, "downstream", count_rows, &downstream);

    int rc = 0;
    if (err != WIRELOG_OK) {
        fprintf(stderr, "T8 snapshot err=%d\n", err);
        rc = 1;
    } else if (projected.rows != 1 || downstream.rows != 2) {
        fprintf(stderr, "T8: expected projected=1 downstream=2, got "
            "%u and %u\n", projected.rows, downstream.rows);
        rc = 1;
    }
    wirelog_easy_close(s);
    return rc;
}

/* T9 (issue #2321): host source inspection refuses a relation that has an
 * inline fact -- before the lazy build loads it and after -- because its
 * stored rows are not host input alone, while a host-only relation beside
 * it is answered.  Through the production library, public API only. */
static int
test_inline_fact_relation_refused_by_source_inspection(void)
{
    static const char *src =
        ".decl seeded(x: int64)\n"
        ".decl hosted(x: int64)\n"
        "seeded(1).\n";

    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(src, &s);
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "T9 open err=%d\n", err);
        return 1;
    }
    int rc = 0;
    int64_t one = 1;
    uint64_t n = 7;
    for (int built = 0; built < 2; built++) {
        n = 7;
        if (wirelog_easy_source_count(s, "seeded", &one, 1, UINT64_MAX, &n)
            != WIRELOG_SOURCE_QUERY_UNSUPPORTED || n != 0) {
            fprintf(stderr, "T9: seeded not refused (built=%d)\n", built);
            rc = 1;
        }
        if (wirelog_easy_source_count(s, "hosted", &one, 1, UINT64_MAX, &n)
            != WIRELOG_SOURCE_QUERY_OK || n != (uint64_t)built) {
            fprintf(stderr, "T9: hosted count %llu (built=%d)\n",
                (unsigned long long)n, built);
            rc = 1;
        }
        if (!built && wirelog_easy_insert(s, "hosted", &one, 1)
            != WIRELOG_OK) {
            fprintf(stderr, "T9: insert failed\n");
            rc = 1;
        }
    }
    wirelog_easy_close(s);
    return rc;
}

/* #2231: a relation with inline facts and no `.decl` was loaded as
 * zero-width rows, so `p(x) :- g(x).` derived p(0).  It now takes its width
 * and column types from its facts. */
#define UNDECL_MAX 8

typedef struct {
    uint32_t count;
    int64_t v[UNDECL_MAX][2];
} undecl_rows_t;

static void
collect_undecl(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    undecl_rows_t *r = (undecl_rows_t *)user_data;

    (void)relation;
    if (r->count < UNDECL_MAX) {
        r->v[r->count][0] = ncols > 0 ? row[0] : -1;
        r->v[r->count][1] = ncols > 1 ? row[1] : -1;
    }
    r->count++;
}

static int
undecl_case(const char *name, const char *src, const char *rel,
    uint32_t want_rows, int64_t first0, int64_t first1)
{
    wirelog_easy_session_t *s = NULL;
    undecl_rows_t got = { 0 };
    int rc = wirelog_easy_open(src, &s);
    if (rc != WIRELOG_OK || !s) {
        printf("FAIL %s: open rc %d\n", name, rc);
        return 1;
    }
    rc = wirelog_easy_snapshot(s, rel, collect_undecl, &got);
    wirelog_easy_close(s);
    if (rc != WIRELOG_OK || got.count != want_rows
        || (want_rows > 0 && (got.v[0][0] != first0
        || (first1 >= 0 && got.v[0][1] != first1)))) {
        printf("FAIL %s: rc %d, %u rows, first (%lld, %lld)\n", name, rc,
            got.count, (long long)got.v[0][0], (long long)got.v[0][1]);
        return 1;
    }
    return 0;
}

static int
test_undeclared_fact_relations(void)
{
    int failures = 0;

    failures += undecl_case("undeclared unary fact",
            "g(5).\n.decl p(x: int64)\np(x) :- g(x).\n", "p", 1, 5, -1);
    failures += undecl_case("undeclared binary fact",
            "g(5, 7).\n.decl p(x: int64, y: int64)\np(x, y) :- g(x, y).\n",
            "p", 1, 5, 7);
    failures += undecl_case("undeclared fact relation with a rule",
            "g(5).\n.decl h(x: int64)\nh(9).\ng(x) :- h(x).\n"
            ".decl p(x: int64)\np(x) :- g(x).\n", "p", 2, 5, -1);
    {
        /* `first` interns "a" ahead of g's "b", so a fabricated 0 cannot
         * pass for the id of "b". */
        wirelog_easy_session_t *s = NULL;
        undecl_rows_t got = { 0 };
        int rc = wirelog_easy_open(".decl first(x: symbol)\nfirst(\"a\").\n"
                "g(\"b\").\n.decl want(x: symbol)\nwant(\"b\").\n"
                ".decl p(x: symbol)\np(x) :- g(x), want(x).\n", &s);
        int64_t b = s ? wirelog_easy_intern(s, "b") : -1;
        if (rc == WIRELOG_OK && s)
            rc = wirelog_easy_snapshot(s, "p", collect_undecl, &got);
        if (s)
            wirelog_easy_close(s);
        if (rc != WIRELOG_OK || got.count != 1 || got.v[0][0] != b) {
            printf("FAIL undeclared string fact: rc %d, %u rows\n", rc,
                got.count);
            failures++;
        }
    }

    {
        /* Disagreeing facts are a parse error, as a `.decl` mismatch is. */
        wirelog_easy_session_t *s = NULL;
        int rc = wirelog_easy_open("q().\nq(1).\n.decl p(x: int64)\n"
                "p(x) :- q(x).\n", &s);
        if (rc != WIRELOG_ERR_PARSE || s) {
            printf("FAIL mixed-width facts: rc %d\n", rc);
            failures++;
            if (s)
                wirelog_easy_close(s);
        }
    }

    {
        /* A host row into the inferred string column is taken, and a
         * printed delta of an id nothing interned does not abort. */
        wirelog_easy_session_t *s = NULL;
        int64_t row[1] = { 999999 };
        int rc = wirelog_easy_open("g(\"a\").\n.decl p(x: symbol)\n"
                "p(x) :- g(x).\n", &s);
        int ins = -1;
        int step = -1;
        if (rc == WIRELOG_OK && s) {
            wirelog_easy_set_delta_cb(s, wirelog_easy_print_delta, s);
            ins = wirelog_easy_insert(s, "g", row, 1);
            step = wirelog_easy_step(s);
        }
        if (s)
            wirelog_easy_close(s);
        if (rc != WIRELOG_OK || ins != WIRELOG_OK || step != WIRELOG_OK) {
            printf("FAIL host row into inferred string column: open %d,"
                " insert %d, step %d\n", rc, ins, step);
            failures++;
        }
    }

    {
        /* hash() of an inferred string column digests the string's bytes,
         * as it does for a declared one. */
        undecl_rows_t undecl = { 0 };
        undecl_rows_t decl = { 0 };
        wirelog_easy_session_t *s = NULL;
        int rc1 = wirelog_easy_open("s(\"abc\").\n.decl p(h: int64)\n"
                "p(hash(x)) :- s(x).\n", &s);
        if (rc1 == WIRELOG_OK && s)
            rc1 = wirelog_easy_snapshot(s, "p", collect_undecl, &undecl);
        if (s)
            wirelog_easy_close(s);
        s = NULL;
        int rc2 = wirelog_easy_open(".decl s(x: symbol)\ns(\"abc\").\n"
                ".decl p(h: int64)\np(hash(x)) :- s(x).\n", &s);
        if (rc2 == WIRELOG_OK && s)
            rc2 = wirelog_easy_snapshot(s, "p", collect_undecl, &decl);
        if (s)
            wirelog_easy_close(s);
        if (rc1 != WIRELOG_OK || rc2 != WIRELOG_OK || undecl.count != 1
            || decl.count != 1 || undecl.v[0][0] != decl.v[0][0]) {
            printf("FAIL hash of inferred string column: rc %d/%d,"
                " %lld vs %lld\n", rc1, rc2, (long long)undecl.v[0][0],
                (long long)decl.v[0][0]);
            failures++;
        }
    }
    return failures;
}

/* The easy facade refuses float relations, so the float cases run through
 * the session API.  Rows of @out are compared as binary64 values. */
typedef struct {
    const char *rel;
    uint32_t count;
    double v[UNDECL_MAX];
} float_rows_t;

static void
collect_float(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    float_rows_t *r = (float_rows_t *)user_data;

    if (strcmp(relation, r->rel) != 0 || ncols != 1)
        return;
    if (r->count < UNDECL_MAX)
        memcpy(&r->v[r->count], &row[0], sizeof(double));
    r->count++;
}

static int
undecl_float_case(const char *name, const char *src, const char *out,
    const double *want, uint32_t nwant)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;
    float_rows_t got = { out, 0, { 0 } };
    const char *why = NULL;

    if (!prog)
        why = "parse";
    else if (!wirelog_optimize(prog, &err))
        why = "optimize";
    else if (wl_plan_from_program(prog, &plan) != 0)
        why = "plan";
    else if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0)
        why = "session";
    else if (wl_session_load_facts(sess, prog) != 0)
        why = "load";
    else if (wl_session_step(sess) != 0)
        why = "step";
    else if (wl_session_snapshot(sess, collect_float, &got) != 0)
        why = "snapshot";
    else if (got.count != nwant)
        why = "row count";
    for (uint32_t i = 0; !why && i < nwant; i++) {
        bool found = false;
        for (uint32_t k = 0; k < got.count && k < UNDECL_MAX; k++)
            if (got.v[k] == want[i])
                found = true;
        if (!found)
            why = "missing value";
    }
    if (sess)
        wl_session_destroy(sess);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
    if (why) {
        printf("FAIL %s: %s (%u rows)\n", name, why, got.count);
        return 1;
    }
    return 0;
}

static int
test_undeclared_float_fact_relations(void)
{
    static const double above_two[] = { 2.5 };
    static const double both[] = { 5.5, 9.5 };
    int failures = 0;

    failures += undecl_float_case("undeclared float facts filtered",
            "g(1.5).\ng(2.5).\n.decl p(x: float)\np(x) :- g(x), x > 2.0.\n",
            "p", above_two, 1);
    failures += undecl_float_case("undeclared float facts with a rule",
            "g(5.5).\n.decl h(x: float)\nh(9.5).\ng(x) :- h(x).\n"
            ".decl p(x: float)\np(x) :- g(x).\n", "p", both, 2);
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_static_fact_drives_idb_snapshot();
    failures += test_static_fact_joins_with_host_insert();
    failures += test_matching_arity_facts_evaluate_exactly();
    failures += test_inline_compound_head_evaluates_exactly();
    failures += test_inline_compound_flat_fact_evaluates_exactly();
    failures += test_inline_compound_handle_fact_rejected();
    failures += test_inline_compound_flat_fact_destructures_exactly();
    failures += test_single_rule_idb_is_set_valued();
    failures += test_inline_fact_relation_refused_by_source_inspection();
    failures += test_undeclared_fact_relations();
    failures += test_undeclared_float_fact_relations();
    if (failures == 0)
        printf("test_wl_easy_inline_facts: OK\n");
    else
        printf("test_wl_easy_inline_facts: %d failure(s)\n", failures);
    return failures;
}
