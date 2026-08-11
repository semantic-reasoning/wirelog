/*
 * tests/test_symbol_aggregates.c - min()/max() over symbol columns (#965)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * `min()` and `max()` reduced over the *interned id* of a `symbol` column
 * rather than over the string the id stands for.  Ids are handed out in
 * first-appearance order, so
 *
 *     .decl S(g: symbol, v: symbol)
 *     S("g", "zz").  S("g", "aa").
 *     T(g, min(v)) :- S(g, v).
 *
 * answered "zz" -- interned first, therefore the smaller id -- and adding
 * one unrelated fact ahead of it that claimed the lower ids flipped the
 * answer to "aa" without touching the data being reduced.  #962 fixed the
 * same class for `<`/`>`/`<=`/`>=`; this is the aggregate half.
 *
 * Every case here is framed as *intern-order independence*: the same rule
 * over the same facts, run once alone and once behind an unrelated relation
 * whose facts are parsed first, must give the same answer -- and the exact
 * tuples are asserted in both directions.  Cardinality cannot see this
 * defect: both orderings return one row per group.
 *
 * Each case is also a *single* aggregate program.  A head carrying two
 * aggregates keeps only the last one, so `T(g, min(v), max(v))` would
 * validate the max half and quietly green-light the min half.
 *
 * Case map:
 *
 *   test_min_lexicographic / test_max_lexicographic
 *       The defect report, non-recursive, both directions.  min must not
 *       return "zz" and max must not return "aa".
 *
 *   test_recursive_min_workers / test_recursive_max_workers
 *       The same property through the recursive fixpoint, at W = 1, 4, 8
 *       and 16.  A second reducer -- the recursive-aggregate
 *       canonicalisation in columnar/eval.c -- runs only on this path and
 *       flipped independently of col_op_reduce().
 *
 *   test_join_chain_aggregate
 *       A body of three atoms joined on one key -- the shortest that
 *       detect_lftj_chain() accepts -- is rewritten into a single LFTJ
 *       operator, and every surviving op -- the REDUCE among them -- is
 *       rebuilt by clone_plan_op().  An operand type that is populated at
 *       translation but not carried across by the clone is lost exactly
 *       here, which is why each case has a single-atom control beside it.
 *
 *   test_min_of_to_upper
 *       min(to_upper(v)).  The operand is a runtime-interned id belonging
 *       to no column, which is why the type has to come from
 *       expr_result_type() and not from a column lookup.
 *
 *   test_integer_aggregates_unchanged
 *       Integer columns must keep the numeric comparison, negatives
 *       included.  This is the failure mode where the string path is
 *       applied unconditionally.
 *
 *   test_mistyped_symbol_column_recursive
 *       A relation declared `symbol` whose values were never interned --
 *       accepted by the parser, and today it evaluates fine.  Following
 *       #963 this is not failed closed: reducing it still gives the numeric
 *       answer for numeric data, and the mistype is reported at plan
 *       generation.  It is recursive because that is where a comparator
 *       parameterised by the (growing) intern table would lose its
 *       termination argument.
 *
 *   test_mixed_interned_and_uninterned_group
 *       A group holding both an interned symbol and a value past the end of
 *       the intern table.  The interned one wins for min() and for max()
 *       alike, which is the step that keeps the comparator monotone while
 *       the intern table grows underneath it.
 *
 *   test_undeclared_relation
 *       An undeclared relation has no column types at all, so min() has
 *       nothing to compare but ids.  Documented in docs/SEMANTICS.md; the
 *       requirement here is only that such a program keeps working.
 *
 *   test_inline_compound_relation
 *       A `symbol` column positioned after an inline compound, which
 *       occupies several physical columns behind one logical one.  If the
 *       type is lost there the aggregate silently returns to ids.
 *
 *   test_count_sum_unchanged
 *       count()/sum() over a symbol column are not ordering aggregates and
 *       must not acquire the string comparison.  average() belongs to the
 *       same non-ordering group but is rejected at lowering (#978), so the
 *       property asserted for it is that it never reaches the reducer.
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/intern.h"
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

static int test_count;
static int pass_count;
static int fail_count;

#define TEST(name)                                      \
        do {                                                \
            test_count++;                                   \
            printf("TEST %d: %s ... ", test_count, (name)); \
        } while (0)

#define PASS()            \
        do {                  \
            pass_count++;     \
            printf("PASS\n"); \
        } while (0)

#define FAIL(msg)                    \
        do {                             \
            fail_count++;                \
            printf("FAIL: %s\n", (msg)); \
            return;                      \
        } while (0)

#define ASSERT(cond, msg) \
        do {                  \
            if (!(cond))      \
            FAIL(msg);    \
        } while (0)

#define MAX_SEEN 32
#define MAX_COLS 8

typedef struct {
    const char *rel;
    uint32_t count;
    uint32_t ncols;
    const wirelog_intern_t *intern;
    /* Rows rendered for assertion, columns joined with '|'.  A value that
     * names an interned string renders as that string, anything else as
     * decimal -- which is also how a mistyped column renders. */
    char seen[MAX_SEEN][256];
    int64_t raw[MAX_SEEN][MAX_COLS];
} collect_t;

/* Rows the caller wants inserted directly (physical layout), used by the
 * compound case whose facts cannot be written in source syntax. */
typedef struct {
    const char *relation;
    const int64_t *rows;
    uint32_t num_rows;
    uint32_t ncols;
} insert_spec_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    collect_t *c = (collect_t *)user;
    if (!c->rel || !relation || strcmp(relation, c->rel) != 0)
        return;
    if (c->count < MAX_SEEN) {
        char *dst = c->seen[c->count];
        size_t pos = 0;
        dst[0] = '\0';
        c->ncols = ncols;
        for (uint32_t i = 0; i < ncols && pos + 1 < sizeof(c->seen[0]); i++) {
            if (i < MAX_COLS)
                c->raw[c->count][i] = row[i];
            const char *v = c->intern
                ? wl_intern_reverse(c->intern, row[i]) : NULL;
            int n;
            if (v)
                n = snprintf(dst + pos, sizeof(c->seen[0]) - pos, "%s%s",
                        i ? "|" : "", v);
            else
                n = snprintf(dst + pos, sizeof(c->seen[0]) - pos,
                        "%s%" PRId64, i ? "|" : "", row[i]);
            if (n < 0)
                break;
            pos += (size_t)n;
        }
    }
    c->count++;
}

/* True if any collected row renders as @want. */
static bool
saw(const collect_t *c, const char *want)
{
    uint32_t n = c->count < MAX_SEEN ? c->count : MAX_SEEN;
    for (uint32_t i = 0; i < n; i++) {
        if (strcmp(c->seen[i], want) == 0)
            return true;
    }
    return false;
}

/* True if any collected row holds @want in column @col. */
static bool
saw_value_at(const collect_t *c, uint32_t col, int64_t want)
{
    uint32_t n = c->count < MAX_SEEN ? c->count : MAX_SEEN;
    for (uint32_t i = 0; i < n; i++) {
        if (col < c->ncols && col < MAX_COLS && c->raw[i][col] == want)
            return true;
    }
    return false;
}

/*
 * Evaluate @src with @workers workers, filling @out with @relation's tuples.
 *
 * @prepare may be NULL.  When present it runs after parsing -- so the intern
 * table already holds the source facts' symbols -- and describes rows to
 * insert directly, which is how the compound case supplies facts that have
 * no source syntax.  Returns 0 on success.
 */
static int
eval_relation_ex(const char *src, const char *relation, uint32_t workers,
    collect_t *out,
    int (*prepare)(const wirelog_program_t *prog, insert_spec_t *ins))
{
    memset(out, 0, sizeof(*out));
    out->rel = relation;

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        fprintf(stderr, "  parse error: %d\n", (int)err);
        return -1;
    }

    insert_spec_t ins;
    memset(&ins, 0, sizeof(ins));
    if (prepare && prepare(prog, &ins) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, workers, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    out->intern = wirelog_program_get_intern(prog);

    int result = -1;
    if (wl_session_load_facts(sess, prog) == 0
        && (ins.relation == NULL
        || wl_session_insert(sess, ins.relation, ins.rows, ins.num_rows,
        ins.ncols) == 0)
        && wl_session_snapshot(sess, collect_cb, out) == 0)
        result = 0;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return result;
}

static int
eval_relation(const char *src, const char *relation, collect_t *out)
{
    return eval_relation_ex(src, relation, 1, out, NULL);
}

/*
 * "zz" is written first and therefore holds the smaller id, while "aa"
 * sorts first as a string: the two orderings disagree on every case below.
 *
 * SHIFT is an unrelated relation whose facts are parsed first and claim the
 * low ids, reversing the id order of "aa" against "zz".  Prepending it must
 * change nothing.
 */
#define SHIFT                          \
        ".decl Unrelated(x: symbol)\n" \
        "Unrelated(\"aa\"). Unrelated(\"mm\"). Unrelated(\"zz\").\n"

#define S_FACTS                          \
        ".decl S(g: symbol, v: symbol)\n" \
        "S(\"g\", \"zz\"). S(\"g\", \"mm\"). S(\"g\", \"aa\").\n"

#define T_DECL ".decl T(g: symbol, m: symbol)\n"

/* The worker counts test_recursive_agg_conformance.c already pins. */
static const uint32_t k_worker_counts[] = { 1, 4, 8, 16 };
#define N_WORKER_COUNTS \
        (sizeof(k_worker_counts) / sizeof(k_worker_counts[0]))

/* ---------------------------------------------------------------------- */

static void
test_min_lexicographic(void)
{
    TEST("min() over a symbol column is lexicographic, not by intern id");

    static const char *cases[] = {
        S_FACTS T_DECL "T(g, min(v)) :- S(g, v).\n",
        SHIFT S_FACTS T_DECL "T(g, min(v)) :- S(g, v).\n",
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        collect_t c;
        ASSERT(eval_relation(cases[i], "T", &c) == 0, "evaluation failed");
        ASSERT(c.count == 1, "expected exactly one group");
        ASSERT(saw(&c, "g|aa"), "min must be \"aa\"");
        ASSERT(!saw(&c, "g|zz"),
            "returned the value the intern-id ordering selects");
    }
    PASS();
}

static void
test_max_lexicographic(void)
{
    TEST("max() over a symbol column is lexicographic, not by intern id");

    static const char *cases[] = {
        S_FACTS T_DECL "T(g, max(v)) :- S(g, v).\n",
        SHIFT S_FACTS T_DECL "T(g, max(v)) :- S(g, v).\n",
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        collect_t c;
        ASSERT(eval_relation(cases[i], "T", &c) == 0, "evaluation failed");
        ASSERT(c.count == 1, "expected exactly one group");
        ASSERT(saw(&c, "g|zz"), "max must be \"zz\"");
        ASSERT(!saw(&c, "g|aa"),
            "returned the value the intern-id ordering selects");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * Connected-component labelling over symbols.  {"zz","aa","mm"} form one
 * component and {"pp","bb"} another, so the answer is the lexicographic
 * minimum (or maximum) of each component -- propagated through the
 * recursive rule, which is what makes the eval.c canonicalisation run.
 */
#define CC_EDGES                                     \
        ".decl E(x: symbol, y: symbol)\n"            \
        ".decl L(x: symbol, l: symbol)\n"            \
        "E(\"zz\",\"aa\"). E(\"aa\",\"zz\"). E(\"aa\",\"mm\").\n" \
        "E(\"mm\",\"aa\"). E(\"pp\",\"bb\"). E(\"bb\",\"pp\").\n"

#define CC_RULES(fn)                        \
        "L(x, " fn "(x)) :- E(x, y).\n"     \
        "L(y, " fn "(y)) :- E(x, y).\n"     \
        "L(x, " fn "(l)) :- L(y, l), E(y, x).\n"

static void
test_recursive_min_workers(void)
{
    TEST("recursive min() over symbols, W = 1/4/8/16");

    static const char *cases[] = {
        CC_EDGES CC_RULES("min"),
        SHIFT CC_EDGES CC_RULES("min"),
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
            collect_t c;
            ASSERT(eval_relation_ex(cases[i], "L", k_worker_counts[w], &c,
                NULL) == 0, "evaluation failed");
            ASSERT(c.count == 5, "expected one label per node");
            ASSERT(saw(&c, "zz|aa") && saw(&c, "aa|aa") && saw(&c, "mm|aa"),
                "component {zz,aa,mm} must label as \"aa\"");
            ASSERT(saw(&c, "pp|bb") && saw(&c, "bb|bb"),
                "component {pp,bb} must label as \"bb\"");
        }
    }
    PASS();
}

static void
test_recursive_max_workers(void)
{
    TEST("recursive max() over symbols, W = 1/4/8/16");

    static const char *cases[] = {
        CC_EDGES CC_RULES("max"),
        SHIFT CC_EDGES CC_RULES("max"),
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
            collect_t c;
            ASSERT(eval_relation_ex(cases[i], "L", k_worker_counts[w], &c,
                NULL) == 0, "evaluation failed");
            ASSERT(c.count == 5, "expected one label per node");
            ASSERT(saw(&c, "zz|zz") && saw(&c, "aa|zz") && saw(&c, "mm|zz"),
                "component {zz,aa,mm} must label as \"zz\"");
            ASSERT(saw(&c, "pp|pp") && saw(&c, "bb|pp"),
                "component {pp,bb} must label as \"pp\"");
        }
    }
    PASS();
}

/*
 * A body of four atoms joined on one key is rewritten into a single LFTJ
 * operator, and every op that is *not* folded into it -- the REDUCE
 * included -- is rebuilt by clone_plan_op().  An operand type that is
 * populated at translation but not carried across by the clone is lost
 * here and nowhere else: the same aggregate over `A` alone, below, is the
 * control that keeps working when the clone drops it.
 */
#define CHAIN_FACTS                              \
        ".decl A(k: symbol, v: symbol)\n"        \
        ".decl B(k: symbol)\n"                   \
        ".decl C(k: symbol)\n"                   \
        "A(\"g\", \"zz\"). A(\"g\", \"mm\"). A(\"g\", \"aa\").\n" \
        "B(\"g\"). C(\"g\").\n"                  \
        ".decl T(k: symbol, m: symbol)\n"

static void
test_join_chain_aggregate(void)
{
    TEST("min()/max() survive the join-chain rewrite (clone_plan_op)");

    static const struct {
        const char *src;
        const char *want;
        const char *reject;
    } cases[] = {
        { CHAIN_FACTS "T(k, min(v)) :- A(k, v), B(k), C(k).\n",
          "g|aa", "g|zz" },
        { SHIFT CHAIN_FACTS "T(k, min(v)) :- A(k, v), B(k), C(k).\n",
          "g|aa", "g|zz" },
        { CHAIN_FACTS "T(k, max(v)) :- A(k, v), B(k), C(k).\n",
          "g|zz", "g|aa" },
        { SHIFT CHAIN_FACTS "T(k, max(v)) :- A(k, v), B(k), C(k).\n",
          "g|zz", "g|aa" },
        /*
         * Controls: the single-atom bodies are not rewritten, so these
         * still answer correctly even when the clone loses the type.
         */
        { CHAIN_FACTS "T(k, min(v)) :- A(k, v).\n", "g|aa", "g|zz" },
        { CHAIN_FACTS "T(k, max(v)) :- A(k, v).\n", "g|zz", "g|aa" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        collect_t c;
        ASSERT(eval_relation(cases[i].src, "T", &c) == 0,
            "evaluation failed");
        ASSERT(c.count == 1, "expected exactly one group");
        ASSERT(saw(&c, cases[i].want), "wrong aggregate value");
        ASSERT(!saw(&c, cases[i].reject),
            "returned the value the intern-id ordering selects");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_min_of_to_upper(void)
{
    TEST("min(to_upper(v)) orders the produced strings, not their ids");

    /* to_upper() interns its result *during* evaluation, so the operand is
     * an id that belongs to no column and whose value depends on the order
     * rows happen to be visited.  "AA" < "MM" < "ZZ" either way. */
    static const char *cases[] = {
        S_FACTS T_DECL "T(g, min(to_upper(v))) :- S(g, v).\n",
        SHIFT S_FACTS T_DECL "T(g, min(to_upper(v))) :- S(g, v).\n",
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        collect_t c;
        ASSERT(eval_relation(cases[i], "T", &c) == 0, "evaluation failed");
        ASSERT(c.count == 1, "expected exactly one group");
        ASSERT(saw(&c, "g|AA"), "min(to_upper(v)) must be \"AA\"");
    }

    /* And the other direction, so a comparator that rejects everything or
     * always keeps the first row cannot pass. */
    collect_t c;
    ASSERT(eval_relation(S_FACTS T_DECL
        "T(g, max(to_upper(v))) :- S(g, v).\n", "T", &c) == 0,
        "evaluation failed");
    ASSERT(c.count == 1 && saw(&c, "g|ZZ"),
        "max(to_upper(v)) must be \"ZZ\"");
    PASS();
}

/* ---------------------------------------------------------------------- */

#define N_FACTS                          \
        ".decl N(g: int32, v: int64)\n"  \
        "N(1, 5). N(1, 30). N(1, 7).\n"  \
        ".decl R(g: int32, m: int64)\n"

/* Interns "zz"=0, "mm"=1, "aa"=2, so the id order and the lexicographic
 * order of the same three values are opposites. */
#define REV_SHIFT                      \
        ".decl Rev(x: symbol)\n"      \
        "Rev(\"zz\"). Rev(\"mm\"). Rev(\"aa\").\n"

#define SMALL_N                          \
        ".decl N(g: int32, v: int64)\n" \
        "N(1, 0). N(1, 1). N(1, 2).\n"  \
        ".decl R(g: int32, m: int64)\n"

static void
test_integer_aggregates_unchanged(void)
{
    TEST("min()/max() over integer columns keep the numeric comparison");

    /* Negative literals have no fact syntax, so the negative operands are
     * produced by the expression instead. */
    static const struct {
        const char *src;
        int64_t expect;
    } cases[] = {
        { N_FACTS "R(g, min(v)) :- N(g, v).\n", 5 },
        { N_FACTS "R(g, max(v)) :- N(g, v).\n", 30 },
        { N_FACTS "R(g, min(0 - v)) :- N(g, v).\n", -30 },
        { N_FACTS "R(g, max(0 - v)) :- N(g, v).\n", -5 },
        { SHIFT N_FACTS "R(g, min(v)) :- N(g, v).\n", 5 },
        /*
         * The discriminating pair.  REV_SHIFT interns "zz"/"mm"/"aa" in
         * that order, so id 0 is "zz" and id 2 is "aa": the numeric order
         * of 0/1/2 and the lexicographic order of the strings those ids
         * hold are exact opposites.  An integer column that reached the
         * string comparison -- which is what mistyping SCALAR as STRING
         * produces -- answers 2 for min and 0 for max here.  Every other
         * integer case above is blind to that mutation, because values as
         * large as 5/7/30 are past the end of the intern table and fall
         * back to the numeric comparison anyway.
         */
        { REV_SHIFT SMALL_N "R(g, min(v)) :- N(g, v).\n", 0 },
        { REV_SHIFT SMALL_N "R(g, max(v)) :- N(g, v).\n", 2 },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        collect_t c;
        ASSERT(eval_relation(cases[i].src, "R", &c) == 0,
            "evaluation failed");
        ASSERT(c.count == 1, "expected exactly one group");
        ASSERT(saw_value_at(&c, 1, cases[i].expect),
            "integer aggregate returned the wrong value");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_mistyped_symbol_column_recursive(void)
{
    TEST("a symbol column whose values were never interned still evaluates");

    /*
     * The parser accepts integer facts for a column declared `symbol`, and
     * the resulting values are not intern ids at all.  #963 settled that
     * this mistype is reported rather than failed closed -- so the answer
     * here is the numeric one, unchanged from before the fix.
     *
     * It matters that this is *recursive*: a comparator that treated
     * "reversible right now" as a property of the value would be
     * parameterised by an intern table growing underneath it, and the
     * fixpoint would have no termination argument.  This case reaching its
     * assertions at all is the observable half of that.
     */
    static const char *src =
        ".decl E(x: symbol, y: symbol)\n"
        ".decl L(x: symbol, l: symbol)\n"
        "E(1,2). E(2,1). E(2,3). E(3,2). E(4,5). E(5,4).\n"
        "L(x, min(x)) :- E(x, y).\n"
        "L(y, min(y)) :- E(x, y).\n"
        "L(x, min(l)) :- L(y, l), E(y, x).\n";

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_ex(src, "L", k_worker_counts[w], &c, NULL) == 0,
            "evaluation failed -- the mistype must not fail the query");
        ASSERT(c.count == 5, "expected one label per node");
        ASSERT(saw(&c, "1|1") && saw(&c, "2|1") && saw(&c, "3|1"),
            "component {1,2,3} must label as 1");
        ASSERT(saw(&c, "4|4") && saw(&c, "5|4"),
            "component {4,5} must label as 4");
    }
    PASS();
}

static void
test_aggregate_preserves_head_position(void)
{
    TEST("aggregate value stays at its declared head position");

    static const char *src =
        ".decl val(g: int64, v: int64)\n"
        ".decl t(a: int64, g: int64)\n"
        ".output t\n"
        "val(7, 100). val(7, 50). val(4, 200).\n"
        "t(min(v), g) :- val(g, v).\n";
    collect_t c;
    ASSERT(eval_relation(src, "t", &c) == 0, "aggregate evaluation failed");
    bool saw_7 = false;
    bool saw_4 = false;
    for (uint32_t i = 0; i < c.count && i < MAX_SEEN; i++) {
        if (c.ncols != 2)
            continue;
        if (c.raw[i][0] == 50 && c.raw[i][1] == 7)
            saw_7 = true;
        if (c.raw[i][0] == 200 && c.raw[i][1] == 4)
            saw_4 = true;
    }
    ASSERT(saw_7 && saw_4,
        "aggregate output was not restored to head order");
    PASS();
}

static void
test_mixed_interned_and_uninterned_group(void)
{
    TEST("a group mixing interned and un-interned values prefers the strings");

    /*
     * The step the termination argument rests on.  9999 is past the end of
     * the intern table and "zz" is not, and which of the two that is stays
     * true only *going forward*: the table grows during evaluation --
     * to_upper(), cat(), trim() and the rest all intern their results --
     * so a value can move from un-interned to interned but never back.
     *
     * Preferring the interned side for min() *and* for max() is what makes
     * that move always an improvement, in both directions, and therefore
     * what makes the reduction strictly monotone in a finite order.  A
     * comparator that preferred the larger raw id for max() would have the
     * fixpoint chasing a verdict that changes underneath it.
     *
     * Both fact orders are covered, so a comparator that simply keeps
     * whichever row it saw first cannot pass.
     */
    static const struct {
        const char *src;
        const char *want;
    } cases[] = {
        { ".decl S(g: symbol, v: symbol)\n"
          "S(\"g\", \"zz\"). S(\"g\", 9999).\n"
          T_DECL "T(g, min(v)) :- S(g, v).\n", "g|zz" },
        { ".decl S(g: symbol, v: symbol)\n"
          "S(\"g\", \"zz\"). S(\"g\", 9999).\n"
          T_DECL "T(g, max(v)) :- S(g, v).\n", "g|zz" },
        { ".decl S(g: symbol, v: symbol)\n"
          "S(\"g\", 9999). S(\"g\", \"zz\").\n"
          T_DECL "T(g, min(v)) :- S(g, v).\n", "g|zz" },
        { ".decl S(g: symbol, v: symbol)\n"
          "S(\"g\", 9999). S(\"g\", \"zz\").\n"
          T_DECL "T(g, max(v)) :- S(g, v).\n", "g|zz" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        collect_t c;
        ASSERT(eval_relation(cases[i].src, "T", &c) == 0,
            "evaluation failed");
        ASSERT(c.count == 1, "expected exactly one group");
        ASSERT(saw(&c, cases[i].want),
            "the interned value must win, whichever aggregate is running");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_undeclared_relation(void)
{
    TEST("min() over an undeclared relation keeps working");

    /*
     * `M` has no .decl, so it has no column types and min() has nothing to
     * compare but ids -- the fallback docs/SEMANTICS.md documents, reported
     * once at plan generation under WL_LOG=EVAL.  What must hold is that
     * the program still evaluates and still produces its group: the failure
     * mode guarded against is a string comparison applied to an untyped
     * column, which drops every row.
     */
    static const char *src =
        S_FACTS
        "M(g, v) :- S(g, v).\n"
        T_DECL
        "T(g, min(v)) :- M(g, v).\n";

    collect_t c;
    ASSERT(eval_relation(src, "T", &c) == 0, "evaluation failed");
    ASSERT(c.count == 1, "expected exactly one group");
    ASSERT(saw(&c, "g|aa") || saw(&c, "g|mm") || saw(&c, "g|zz"),
        "expected one of the three values, not an empty or bogus row");
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * ev(id: int64, lbl: pair/2 inline, v: symbol) occupies four physical
 * columns -- [id][lbl_0][lbl_1][v] -- behind three logical ones.  A type
 * array indexed by physical position gives `v` the compound's type and runs
 * off the end of the declaration, and the aggregate silently returns to
 * ids.  Compound facts have no source syntax, so the rows go in physically.
 */
#define COMPOUND_SRC                                           \
        ".decl seed(s: symbol)\n"                              \
        "seed(\"zz\"). seed(\"mm\"). seed(\"aa\").\n"          \
        ".decl ev(id: int64, lbl: pair/2 inline, v: symbol)\n" \
        ".decl agg(id: int64, m: symbol)\n"                    \
        "agg(i, min(v)) :- ev(i, pair(p, q), v).\n"

static int64_t compound_rows[12];

static int
compound_prepare(const wirelog_program_t *prog, insert_spec_t *ins)
{
    const wirelog_intern_t *in = wirelog_program_get_intern(prog);
    if (!in)
        return -1;
    int64_t zz = wl_intern_get(in, "zz");
    int64_t mm = wl_intern_get(in, "mm");
    int64_t aa = wl_intern_get(in, "aa");
    if (zz < 0 || mm < 0 || aa < 0 || zz >= aa)
        return -1; /* the seed facts must have made id("zz") < id("aa") */

    /* The group id is deliberately past the end of the intern table, so it
     * renders as a number and cannot be confused with a symbol. */
    int64_t r[12] = {
        1000, 7, 8, zz,
        1000, 7, 8, mm,
        1000, 7, 8, aa,
    };
    memcpy(compound_rows, r, sizeof(r));

    ins->relation = "ev";
    ins->rows = compound_rows;
    ins->num_rows = 3;
    ins->ncols = 4;
    return 0;
}

static void
test_inline_compound_relation(void)
{
    TEST("a symbol column after an inline compound keeps its type");

    collect_t c;
    ASSERT(eval_relation_ex(COMPOUND_SRC, "agg", 1, &c, compound_prepare)
        == 0, "evaluation failed");
    ASSERT(c.count == 1, "expected exactly one group");
    ASSERT(saw(&c, "1000|aa"), "min must be \"aa\"");
    ASSERT(!saw(&c, "1000|zz"),
        "returned the value the intern-id ordering selects");
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_count_sum_unchanged(void)
{
    TEST("count()/sum() over a symbol column are unchanged");

    /*
     * These reduce over the id and always did; they are not ordering
     * aggregates.  The guard is against extending the string path to them,
     * which would compare or accumulate strings that have no arithmetic.
     * SHIFT fixes the three ids at 0/1/2, so the sum is a number this test
     * can name.
     */
    static const struct {
        const char *src;
        int64_t expect;
    } cases[] = {
        { SHIFT S_FACTS ".decl C(g: symbol, n: int64)\n"
          "C(g, count(v)) :- S(g, v).\n", 3 },
        /* ids: "aa"=0, "mm"=1, "zz"=2 -- SHIFT interns them in that order */
        { SHIFT S_FACTS ".decl C(g: symbol, n: int64)\n"
          "C(g, sum(v)) :- S(g, v).\n", 0 + 1 + 2 },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        collect_t c;
        ASSERT(eval_relation(cases[i].src, "C", &c) == 0,
            "evaluation failed");
        ASSERT(c.count == 1, "expected exactly one group");
        ASSERT(saw_value_at(&c, 1, cases[i].expect),
            "a non-ordering aggregate changed value");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_average_never_reaches_the_reducer(void)
{
    TEST("average() over a symbol column is rejected, not reduced");

    /*
     * average() belongs with count()/sum() above -- it is not an ordering
     * aggregate and must never acquire the string comparison that lives in
     * the arm beside it.  It cannot be asserted by value, because
     * col_op_reduce() never had an AVG arm: each group kept whichever
     * operand the scan reached first, which under SHIFT was id("zz") = 2.
     * That is not a mean of anything, and #978 rejects average() at
     * lowering rather than freezing an arbitrary number here.
     *
     * Asserting the rejection is still the right guard for *this* issue:
     * the way average() could acquire the ordering path is by being added
     * to the reducer's string branch, and a program that does not lower can
     * never get there.  The rejection's own coverage is
     * tests/test_average_rejected.c.
     */
    collect_t c;
    ASSERT(eval_relation(SHIFT S_FACTS ".decl C(g: symbol, n: int64)\n"
        "C(g, average(v)) :- S(g, v).\n", "C", &c) != 0,
        "average() should not have lowered");
    PASS();
}

/* ---------------------------------------------------------------------- */

int
main(void)
{
    printf("=== min()/max() over symbol columns (Issue #965) ===\n");

    test_min_lexicographic();
    test_max_lexicographic();
    test_recursive_min_workers();
    test_recursive_max_workers();
    test_join_chain_aggregate();
    test_min_of_to_upper();
    test_integer_aggregates_unchanged();
    test_aggregate_preserves_head_position();
    test_mistyped_symbol_column_recursive();
    test_mixed_interned_and_uninterned_group();
    test_undeclared_relation();
    test_inline_compound_relation();
    test_count_sum_unchanged();
    test_average_never_reaches_the_reducer();

    printf("\n%d/%d passed, %d failed\n", pass_count, test_count, fail_count);
    return fail_count == 0 ? 0 : 1;
}
