/*
 * tests/test_symbol_ordering.c - ordering comparisons on symbol columns
 *                                (Issue #962)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * `<`, `>`, `<=` and `>=` used to compare the *intern ids* of symbol
 * columns rather than the strings those ids stand for.  Ids are assigned in
 * first-appearance order, so
 *
 *     S("zz", "aa").
 *     R(a, b) :- S(a, b), a < b.
 *
 * derived a row -- "zz" was interned first and therefore held the smaller
 * id -- and inserting an unrelated earlier fact that shifted the assignment
 * silently changed the answer.  The plan generator now emits the
 * WL_PLAN_EXPR_CMP_STR_* opcodes for ordering comparisons whose operands are
 * both string-typed, so the comparison is lexicographic.
 *
 * Cardinality alone cannot test this.  A wrong plan hits the right count by
 * luck at these sizes, and "zero rows" is also what a string opcode fed a
 * mistyped operand produces -- ops.c returns false whenever wl_intern_reverse
 * yields NULL, which drops every row.  Every case below therefore asserts the
 * exact tuples, and each has a positive counterpart that separates "correct"
 * from "the opcode rejects everything":
 *
 *   test_lt_rejects_reversed_pair / test_lt_accepts_ordered_pair
 *       The two directions of the defect report.
 *
 *   test_intern_order_independence
 *       The property under test.  Same rule, same facts, plus an unrelated
 *       earlier fact that flips which of "aa"/"zz" is interned first.  A
 *       count cannot see this; the output must be identical.
 *
 *   test_all_operators_var_var / test_all_operators_var_literal
 *       All four operators, in both the `a op b` and `a op "literal"` forms.
 *       The literal form resolves through a different serialization path and
 *       was independently broken.  Facts are chosen so the id ordering and
 *       the lexicographic ordering select *different* tuples, never merely
 *       different counts.
 *
 *   test_integer_comparisons_unchanged
 *       Integer columns must keep the integer opcode.  This guards the
 *       failure mode where the emitter converts unconditionally: a blanket
 *       string opcode makes wl_intern_reverse return NULL on integer values
 *       and drops every row.
 *
 *   test_string_function_results_order_lexicographically
 *       to_upper()/to_lower() results are runtime-interned ids, so they are
 *       string-typed too.
 *
 *   test_types_survive_the_column_layout_operators
 *       A column's type travels with its position through the same recursion
 *       that resolves its name -- which is where #955's column-provenance
 *       defect lived.  Joins, antijoins, right atoms that contribute no head
 *       columns, and jpp-inserted projections each get their own case.
 *
 *   test_inline_compound_relation
 *       An inline compound column occupies several *physical* columns while
 *       the declaration lists one *logical* column, so a type array indexed
 *       by physical position assigns every column after the compound the
 *       wrong type and runs off the end of the last one.  Here `name`
 *       follows a pair/2 inline column; if its type is lost the rule silently
 *       returns to comparing ids.
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

typedef struct {
    const char *rel;
    uint32_t count;
    const wirelog_intern_t *intern;
    /* Interned values of each row, joined with '|'.  Integer columns have no
     * reverse mapping, so they are rendered as decimal instead. */
    char seen[MAX_SEEN][256];
} collect_t;

/* Rows the caller wants inserted directly (physical layout), used by the
 * compound case where the fact cannot be written in source syntax. */
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
        for (uint32_t i = 0; i < ncols && pos + 1 < sizeof(c->seen[0]); i++) {
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

/* True if any collected row equals @want. */
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

/*
 * Evaluate @src, filling @out with @relation's tuples.
 *
 * @prepare may be NULL.  When present it runs after parsing -- so the intern
 * table already holds the source facts' symbols -- and describes rows to
 * insert directly, which is how the compound case supplies facts that have
 * no source syntax.  Returns 0 on success.
 */
static int
eval_relation_ex(const char *src, const char *relation, collect_t *out,
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
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
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
    return eval_relation_ex(src, relation, out, NULL);
}

/* Two symbols whose lexicographic order is the reverse of their intern
 * order: "zz" appears first in the source and therefore holds the smaller
 * id, while "aa" sorts first as a string. */
#define S_DECL ".decl S(a: string, b: string)\n"
#define R_DECL ".decl R(a: string, b: string)\n"

/* ---------------------------------------------------------------------- */

static void
test_lt_rejects_reversed_pair(void)
{
    TEST("a < b rejects S(\"zz\", \"aa\") despite the id order");

    const char *src =
        S_DECL
        "S(\"zz\", \"aa\").\n"
        R_DECL
        "R(a, b) :- S(a, b), a < b.\n";

    collect_t c;
    ASSERT(eval_relation(src, "R", &c) == 0, "evaluation failed");
    ASSERT(c.count == 0,
        "\"zz\" < \"aa\" is false; the row must not be derived");
    PASS();
}

static void
test_lt_accepts_ordered_pair(void)
{
    TEST("a < b accepts S(\"aa\", \"zz\")");

    const char *src =
        S_DECL
        "S(\"aa\", \"zz\").\n"
        R_DECL
        "R(a, b) :- S(a, b), a < b.\n";

    /* The positive direction: without it, an opcode that rejects every row
     * (which is what a mistyped operand produces) would pass the test
     * above and look like a fix. */
    collect_t c;
    ASSERT(eval_relation(src, "R", &c) == 0, "evaluation failed");
    ASSERT(c.count == 1, "expected exactly the ordered pair");
    ASSERT(saw(&c, "aa|zz"), "expected (\"aa\", \"zz\")");
    PASS();
}

static void
test_intern_order_independence(void)
{
    TEST("the derived set does not depend on intern order");

    /* Identical rule and identical S facts.  The only difference is an
     * unrelated relation whose facts are parsed first and therefore claim
     * the lower ids, flipping the id order of "aa" against "zz". */
    const char *without =
        S_DECL
        "S(\"zz\", \"aa\").\n"
        "S(\"aa\", \"zz\").\n"
        R_DECL
        "R(a, b) :- S(a, b), a < b.\n";

    const char *with_shift =
        ".decl Z(s: string)\n"
        "Z(\"aa\").\n"
        "Z(\"mm\").\n"
        S_DECL
        "S(\"zz\", \"aa\").\n"
        "S(\"aa\", \"zz\").\n"
        R_DECL
        "R(a, b) :- S(a, b), a < b.\n";

    collect_t a, b;
    ASSERT(eval_relation(without, "R", &a) == 0, "evaluation failed (a)");
    ASSERT(eval_relation(with_shift, "R", &b) == 0, "evaluation failed (b)");

    ASSERT(a.count == 1 && b.count == 1,
        "exactly one of the two pairs is lexicographically ordered");
    ASSERT(saw(&a, "aa|zz") && saw(&b, "aa|zz"),
        "both programs must derive (\"aa\", \"zz\") and nothing else");
    ASSERT(!saw(&a, "zz|aa") && !saw(&b, "zz|aa"),
        "(\"zz\", \"aa\") is not ordered under either intern assignment");
    PASS();
}

/* Facts shared by the operator sweeps.  Interned in source order, so
 * id("zz") < id("aa") < id("mm") while the strings sort aa < mm < zz. */
#define SWEEP_FACTS       \
        S_DECL                \
        "S(\"zz\", \"aa\").\n"    \
        "S(\"aa\", \"zz\").\n"    \
        "S(\"mm\", \"mm\").\n"

static void
test_all_operators_var_var(void)
{
    TEST("all four operators compare two symbol variables lexicographically");

    struct {
        const char *op;
        const char *want[2];
        uint32_t want_count;
        const char *reject; /* what the id ordering would have selected */
    } cases[] = {
        { "<",  { "aa|zz", NULL },    1, "zz|aa" },
        { ">",  { "zz|aa", NULL },    1, "aa|zz" },
        { "<=", { "aa|zz", "mm|mm" }, 2, "zz|aa" },
        { ">=", { "zz|aa", "mm|mm" }, 2, "aa|zz" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char src[512];
        snprintf(src, sizeof(src),
            "%s" R_DECL "R(a, b) :- S(a, b), a %s b.\n",
            SWEEP_FACTS, cases[i].op);

        collect_t c;
        ASSERT(eval_relation(src, "R", &c) == 0, "evaluation failed");
        ASSERT(c.count == cases[i].want_count, "wrong number of rows");
        for (uint32_t w = 0; w < cases[i].want_count; w++)
            ASSERT(saw(&c, cases[i].want[w]), "expected tuple missing");
        ASSERT(!saw(&c, cases[i].reject),
            "derived the tuple the intern-id ordering would have selected");
    }
    PASS();
}

static void
test_all_operators_var_literal(void)
{
    TEST("all four operators compare a symbol variable against a literal");

    /* T holds one column; the literal "mm" is interned last, so under the id
     * ordering it is the largest of the three and every operator selects a
     * different set than the lexicographic answer. */
    struct {
        const char *op;
        const char *want[2];
        uint32_t want_count;
        const char *reject;
    } cases[] = {
        { "<",  { "aa", NULL },  1, "zz" },
        { ">",  { "zz", NULL },  1, "aa" },
        { "<=", { "aa", "mm" },  2, "zz" },
        { ">=", { "zz", "mm" },  2, "aa" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char src[512];
        snprintf(src, sizeof(src),
            ".decl T(a: string)\n"
            "T(\"zz\").\n"
            "T(\"aa\").\n"
            "T(\"mm\").\n"
            ".decl R(a: string)\n"
            "R(a) :- T(a), a %s \"mm\".\n",
            cases[i].op);

        collect_t c;
        ASSERT(eval_relation(src, "R", &c) == 0, "evaluation failed");
        ASSERT(c.count == cases[i].want_count, "wrong number of rows");
        for (uint32_t w = 0; w < cases[i].want_count; w++)
            ASSERT(saw(&c, cases[i].want[w]), "expected tuple missing");
        ASSERT(!saw(&c, cases[i].reject),
            "derived the tuple the intern-id ordering would have selected");
    }
    PASS();
}

static void
test_integer_comparisons_unchanged(void)
{
    TEST("integer columns keep the integer comparison");

    /* Integer values are not intern ids: wl_intern_reverse returns NULL for
     * almost all of them, and the string opcodes drop every row when it
     * does.  A converted-unconditionally emitter therefore shows up here as
     * an empty result, not as a subtly wrong one. */
    struct {
        const char *op;
        const char *want[2];
        uint32_t want_count;
    } cases[] = {
        { "<",  { "1|2", NULL },  1 },
        { ">",  { "2|1", NULL },  1 },
        { "<=", { "1|2", "3|3" }, 2 },
        { ">=", { "2|1", "3|3" }, 2 },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char src[512];
        snprintf(src, sizeof(src),
            ".decl N(x: int64, y: int64)\n"
            "N(1, 2).\n"
            "N(2, 1).\n"
            "N(3, 3).\n"
            ".decl M(x: int64, y: int64)\n"
            "M(x, y) :- N(x, y), x %s y.\n",
            cases[i].op);

        collect_t c;
        ASSERT(eval_relation(src, "M", &c) == 0, "evaluation failed");
        ASSERT(c.count == cases[i].want_count, "wrong number of rows");
        for (uint32_t w = 0; w < cases[i].want_count; w++)
            ASSERT(saw(&c, cases[i].want[w]), "expected tuple missing");
    }
    PASS();
}

static void
test_string_function_results_order_lexicographically(void)
{
    TEST("to_upper() results order lexicographically, not by id");

    /* to_upper() interns its result at evaluation time, so "AA" and "ZZ" get
     * ids in whatever order the rows happen to be visited -- there is no
     * source ordering to appeal to.  Only a lexicographic comparison gives a
     * stable answer. */
    const char *src =
        S_DECL
        "S(\"zz\", \"aa\").\n"
        "S(\"aa\", \"zz\").\n"
        R_DECL
        "R(a, b) :- S(a, b), to_upper(a) < to_upper(b).\n";

    collect_t c;
    ASSERT(eval_relation(src, "R", &c) == 0, "evaluation failed");
    ASSERT(c.count == 1, "expected exactly the ordered pair");
    ASSERT(saw(&c, "aa|zz"), "expected (\"aa\", \"zz\")");
    PASS();
}

static void
test_types_survive_the_column_layout_operators(void)
{
    TEST("types survive joins, antijoins, semijoins and projections");

    /*
     * The type of a column travels with its position through the same
     * recursion that resolves its name, and that recursion is exactly where
     * #955's column-provenance defect lived -- SEMIJOIN and ANTIJOIN widen
     * their layout if the right child is concatenated in.  A type array
     * riding along inherits the same failure, so each shape gets its own
     * case rather than being assumed to follow from the single-scan ones.
     *
     * Every case uses facts whose intern order is the reverse of their
     * lexicographic order, so dropping the types anywhere along the way
     * selects the *other* row instead of merely changing a count.
     */
    struct {
        const char *what;
        const char *src;
    } cases[] = {
        { "two joins and a jpp projection",
          /* y and w die before the head, so jpp inserts PROJECT nodes; x and
           * z are carried across two JOINs from opposite ends of the chain. */
          ".decl A(x: string, y: int64)\n"
          ".decl B(y: int64, w: int64)\n"
          ".decl C(w: int64, z: string)\n"
          "A(\"zz\", 1).\n"
          "A(\"aa\", 2).\n"
          "B(1, 10).\n"
          "B(2, 20).\n"
          "C(10, \"aa\").\n"
          "C(20, \"zz\").\n"
          R_DECL
          "R(x, z) :- A(x, y), B(y, w), C(w, z), x < z.\n" },
        { "antijoin",
          S_DECL
          "S(\"zz\", \"aa\").\n"
          "S(\"aa\", \"zz\").\n"
          ".decl Bad(a: string)\n"
          "Bad(\"mm\").\n"
          R_DECL
          "R(a, b) :- S(a, b), !Bad(a), a < b.\n" },
        { "right atom contributing no head columns",
          S_DECL
          "S(\"zz\", \"aa\").\n"
          "S(\"aa\", \"zz\").\n"
          ".decl T(a: string)\n"
          "T(\"zz\").\n"
          "T(\"aa\").\n"
          R_DECL
          "R(a, b) :- S(a, b), T(a), a < b.\n" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        collect_t c;
        ASSERT(eval_relation(cases[i].src, "R", &c) == 0, "evaluation failed");
        ASSERT(c.count == 1, "expected exactly the lexicographically ordered "
            "pair");
        ASSERT(saw(&c, "aa|zz"), "expected (\"aa\", \"zz\")");
        ASSERT(!saw(&c, "zz|aa"),
            "derived the pair the intern-id ordering would have selected");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * ev(id: int64, lbl: pair/2 inline, lo: string, hi: string) occupies five
 * physical columns -- [id][lbl_0][lbl_1][lo][hi] -- behind four logical
 * ones.  Rows are inserted physically because compound facts have no source
 * syntax.
 */
#define COMPOUND_SRC                                                   \
        ".decl seed(s: string)\n"                                          \
        "seed(\"zz\").\n"                                                  \
        "seed(\"aa\").\n"                                                  \
        ".decl ev(id: int64, lbl: pair/2 inline, lo: string, hi: string)\n" \
        ".decl bad(lo: string, hi: string)\n"                              \
        "bad(lo, hi) :- ev(i, pair(p, q), lo, hi), hi < lo.\n"

static int64_t compound_rows[10];

static int
compound_prepare(const wirelog_program_t *prog, insert_spec_t *ins)
{
    const wirelog_intern_t *in = wirelog_program_get_intern(prog);
    if (!in)
        return -1;
    int64_t zz = wl_intern_get(in, "zz");
    int64_t aa = wl_intern_get(in, "aa");
    if (zz < 0 || aa < 0 || zz >= aa)
        return -1; /* the seed facts must have made id("zz") < id("aa") */

    /* row 0: lo = "aa", hi = "zz"  ->  hi < lo is false as strings,
     *                                  true as ids
     * row 1: lo = "zz", hi = "aa"  ->  hi < lo is true as strings,
     *                                  false as ids */
    int64_t r[10] = {
        1, 7, 8, aa, zz,
        2, 7, 8, zz, aa,
    };
    memcpy(compound_rows, r, sizeof(r));

    ins->relation = "ev";
    ins->rows = compound_rows;
    ins->num_rows = 2;
    ins->ncols = 5;
    return 0;
}

static void
test_inline_compound_relation(void)
{
    TEST("string columns after an inline compound keep their type");

    collect_t c;
    ASSERT(eval_relation_ex(COMPOUND_SRC, "bad", &c, compound_prepare)
        == 0, "evaluation failed");
    ASSERT(c.count == 1, "expected exactly one row");
    /* Under the id ordering the other row is selected instead; a lost type
     * for `lo`/`hi` -- which is what indexing the type array by physical
     * position produces -- shows up here and nowhere else. */
    ASSERT(saw(&c, "zz|aa"),
        "expected the row where hi=\"aa\" sorts before lo=\"zz\"");
    ASSERT(!saw(&c, "aa|zz"),
        "selected the row the intern-id ordering would have selected");
    PASS();
}

int
main(void)
{
    printf("=== symbol ordering comparisons (Issue #962) ===\n");

    test_lt_rejects_reversed_pair();
    test_lt_accepts_ordered_pair();
    test_intern_order_independence();
    test_all_operators_var_var();
    test_all_operators_var_literal();
    test_integer_comparisons_unchanged();
    test_string_function_results_order_lexicographically();
    test_types_survive_the_column_layout_operators();
    test_inline_compound_relation();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf(" ---\n");
    return (fail_count > 0) ? 1 : 0;
}
