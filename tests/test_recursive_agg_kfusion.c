/*
 * tests/test_recursive_agg_kfusion.c - recursive min()/max() under K-fusion
 *                                      (#975)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A recursive relation whose head carries min()/max() is reduced twice: once
 * per rule by col_op_reduce(), and once over the whole relation at fixpoint
 * by the recursive-aggregate canonicalisation in columnar/eval.c.  Only the
 * second one can see across rules, so only it can turn the union of each
 * rule's partial minimum into the relation's minimum.
 *
 * That second reducer used to decide whether to run by scanning the
 * relation's plan operators for a REDUCE.  rewrite_multiway_delta()
 * (exec_plan_gen.c) replaces the operator list with a single K_FUSION
 * operator, moving the originals into its opaque_data, so the scan found
 * nothing and the canonicalisation was skipped entirely -- silently, exit
 * status 0.
 *
 * Note what actually triggers that rewrite: count_delta_positions() >= 2,
 * counting the in-stratum IDB VARIABLE operators and IDB-right JOINs across
 * *all* of the relation's rules.  It is a per-relation count of recursive
 * references, not a per-rule count of body atoms, so two ordinary
 * single-atom recursive rules fuse just as one two-atom rule does.  Both
 * spellings are covered below; the cross-rule one is the shape real
 * programs have.
 *
 * Adding one semantically redundant rule to a working program was enough:
 *
 *     Label(x, min(x)) :- Edge(x, y).
 *     Label(y, min(y)) :- Edge(x, y).
 *     Label(x, min(l)) :- Label(y, l), Edge(y, x).           -> 4 rows
 *     Label(x, min(l)) :- Label(y, l), Label(y, m), Edge(y, x).
 *                                                            -> 10 rows
 *
 * The second rule adds no derivations of its own -- Label(y, m) is satisfied
 * by the same tuple that satisfies Label(y, l) -- but it makes the relation
 * eligible for fusion, and every group then kept all its candidate values.
 *
 * The fix records the aggregate-domination guarantee as relation state at IR
 * lowering, before any rewrite runs, so no rewrite can hide it.  Every case
 * here is therefore framed as *redundant-rule invariance*: the same program
 * with and without a rule that derives nothing must give the same answer.
 *
 * Cardinality alone cannot see this defect in either direction: 10 rows is
 * a plausible answer for a program whose correct answer is 4, and a fix that
 * collapsed groups by the wrong comparator would also return 4.  Every case
 * asserts the exact tuples, and most positive cases additionally assert the
 * absence of each specific row the pre-fix answer contained.  (The two
 * group-by columns case relies on saw_exactly() alone; its twelve pre-fix
 * extras are not enumerated.)
 *
 * Case map:
 *
 *   test_min_redundant_rule_invariance / test_max_redundant_rule_invariance
 *       The defect report over an int64 column, for min and for max.  The
 *       max half exists because a fix that hardcoded MIN passes the min
 *       half.
 *
 *   test_symbol_operand_under_fusion
 *       The same shape over a `symbol` column, run alone and behind an
 *       unrelated relation whose facts claim the low intern ids.  This is
 *       the only case that can see a recorded specification that carries the
 *       aggregate function and the group width but not the operand's domain:
 *       an unset domain reduces by intern id (#965), which agrees with the
 *       lexicographic answer in most orderings and disagrees here.
 *
 *   test_cross_rule_fusion
 *       Two ordinary single-atom recursive rules, forward and backward
 *       along the edges.  No rule carries two IDB body atoms, yet the
 *       relation fuses, because count_delta_positions() sums recursive
 *       references across all of the relation's rules.  This is the shape
 *       real programs have -- the redundant-rule cases above are the
 *       compact reproduction, not the common trigger -- and without it
 *       every positive case here would reach fusion by the same contrived
 *       route.
 *
 *   test_single_idb_atom_control
 *       The same program minus the redundant rule.  It is not fused, so it
 *       answered correctly before the fix as well; it is here to show the
 *       fix did not move the case that already worked, and it is what
 *       catches a fix narrowed to fused relations only.
 *
 *   test_group_by_two_columns
 *       Two grouping columns and an aggregate, fused.  The group comparison
 *       is over group_by_count columns, so a specification that recorded the
 *       aggregate function but defaulted the width to 1 collapses distinct
 *       groups together.
 *
 *   test_count_not_canonicalized
 *       count() is not an ordering aggregate and the canonicalisation
 *       refuses relations whose REDUCE is anything but MIN or MAX.  The
 *       recorded specification must refuse them identically -- both when the
 *       relation is fused and when it is not.
 *
 *   test_mixed_min_max_not_canonicalized
 *       Two rules of one head disagreeing on the aggregate function.  The
 *       canonicalisation refuses these, and the accumulator that records the
 *       specification has to *clear* it on conflict rather than let the last
 *       rule win -- last-writer-wins would newly canonicalise relations that
 *       are correctly skipped today.
 *
 *   test_operand_type_last_rule_wins
 *       Records, rather than endorses, what happens when two rules of one
 *       head agree on the aggregate function and group width but disagree on
 *       the operand's domain: the last one decides.  See the comment on the
 *       case itself.
 *
 * Not covered here, and not silently skipped:
 *
 *   - incremental (--watch) evaluation of a fused aggregate relation.
 *     Affected-strata detection scans the same operator lists and is blinded
 *     by the same K_FUSION hiding, so the insert is dropped before any
 *     aggregate runs.  That is issue #1019.
 *
 *   - a -DENABLE_K_FUSION=0 build as a cross-check oracle.  That build
 *     aborts on this program family; issue #1020.
 *
 *   - a same-stratum consumer of a relation whose labels are dominated away
 *     at fixpoint can retain rows derived from labels that no longer exist,
 *     which is internally contradictory output.  That is issue #1021 and is
 *     a property of canonicalising at fixpoint rather than per iteration.
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

#define MAX_SEEN 64

/* Column rendered as the string its interned id names; -1 for none.  Only
 * the plan knows a column's declared type and the snapshot callback is not
 * told, so the case says which column holds symbols.  Rendering every value
 * that happens to have an intern entry would be ambiguous here: the group
 * keys are small integers and collide with the low intern ids. */
#define NO_SYMBOL_COLUMN (-1)

typedef struct {
    const char *rel;
    uint32_t count;
    uint32_t ncols;
    int sym_col;
    const wirelog_intern_t *intern;
    /* Rows rendered for assertion, columns joined with '|'. */
    char seen[MAX_SEEN][256];
} collect_t;

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
            const char *v = (c->intern && (int)i == c->sym_col)
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

/* True if every row in the NULL-terminated @want list was collected and
 * nothing else was. */
static bool
saw_exactly(const collect_t *c, const char *const *want)
{
    uint32_t n = 0;

    while (want[n])
        n++;
    if (c->count != n)
        return false;
    for (uint32_t i = 0; i < n; i++) {
        if (!saw(c, want[i]))
            return false;
    }
    return true;
}

/*
 * Evaluate @src with @workers workers, filling @out with @relation's tuples.
 * Column @sym_col is rendered as a string, everything else as decimal.
 * Returns 0 on success.
 */
static int
eval_relation_at(const char *src, const char *relation, uint32_t workers,
    int sym_col, collect_t *out)
{
    memset(out, 0, sizeof(*out));
    out->rel = relation;
    out->sym_col = sym_col;

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);

    if (!prog) {
        fprintf(stderr, "  parse error: %d\n", (int)err);
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
        && wl_session_snapshot(sess, collect_cb, out) == 0)
        result = 0;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return result;
}

/* The worker counts test_recursive_agg_conformance.c already pins. */
static const uint32_t k_worker_counts[] = { 1, 4, 8, 16 };
#define N_WORKER_COUNTS \
        (sizeof(k_worker_counts) / sizeof(k_worker_counts[0]))

/* ---------------------------------------------------------------------- */

/*
 * A three-edge path.  Both seeding rules give every node its own id as a
 * label; the recursive rule pushes labels forward along edges.  With the
 * relation reduced as a whole, every node ends up carrying the minimum id
 * reachable backwards from it, which for a path rooted at 1 is 1.
 */
#define PATH_EDGES                                  \
        ".decl Edge(x: int64, y: int64)\n"          \
        ".decl Label(x: int64, l: int64)\n"         \
        "Edge(1,2).  Edge(2,3).  Edge(3,4).\n"

/* Each case appends its own redundant rule.  Label(y, m) is satisfied by the
 * very tuple that satisfies Label(y, l), so the rule derives nothing -- but
 * it takes the relation to two delta positions, which is what
 * rewrite_multiway_delta() fuses on.  Two atoms in one rule is only the
 * most compact way to reach that count; see the forward/backward case for
 * the cross-rule spelling. */

#define MIN_BASE                                             \
        PATH_EDGES                                           \
        "Label(x, min(x)) :- Edge(x, y).\n"                  \
        "Label(y, min(y)) :- Edge(x, y).\n"                  \
        "Label(x, min(l)) :- Label(y, l), Edge(y, x).\n"

#define MAX_BASE                                             \
        PATH_EDGES                                           \
        "Label(x, max(x)) :- Edge(x, y).\n"                  \
        "Label(y, max(y)) :- Edge(x, y).\n"                  \
        "Label(x, max(l)) :- Label(y, l), Edge(y, x).\n"

static void
test_min_redundant_rule_invariance(void)
{
    TEST("min(): a redundant K-fusing rule does not change the answer");

    static const char *src = MIN_BASE
        "Label(x, min(l)) :- Label(y, l), Label(y, m), Edge(y, x).\n";
    static const char *const want[] = {
        "1|1", "2|1", "3|1", "4|1", NULL
    };
    /* The rows the pre-fix answer carried: every group kept each candidate
     * value instead of its minimum. */
    static const char *const unwanted[] = {
        "2|2", "3|2", "3|3", "4|2", "4|3", "4|4", NULL
    };

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_at(src, "Label", k_worker_counts[w],
            NO_SYMBOL_COLUMN, &c) == 0,
            "evaluation failed");
        ASSERT(c.count == 4, "expected one row per node");
        ASSERT(saw_exactly(&c, want), "expected exactly {(1,1),..,(4,1)}");
        for (size_t i = 0; unwanted[i]; i++)
            ASSERT(!saw(&c, unwanted[i]), "a dominated label survived");
    }
    PASS();
}

static void
test_max_redundant_rule_invariance(void)
{
    TEST("max(): a redundant K-fusing rule does not change the answer");

    static const char *src = MAX_BASE
        "Label(x, max(l)) :- Label(y, l), Label(y, m), Edge(y, x).\n";
    /* max propagates forwards, so each node keeps its own id. */
    static const char *const want[] = {
        "1|1", "2|2", "3|3", "4|4", NULL
    };
    static const char *const unwanted[] = {
        "2|1", "3|1", "3|2", "4|1", "4|2", "4|3", NULL
    };

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_at(src, "Label", k_worker_counts[w],
            NO_SYMBOL_COLUMN, &c) == 0,
            "evaluation failed");
        ASSERT(c.count == 4, "expected one row per node");
        ASSERT(saw_exactly(&c, want), "expected exactly {(1,1),..,(4,4)}");
        for (size_t i = 0; unwanted[i]; i++)
            ASSERT(!saw(&c, unwanted[i]), "a dominated label survived");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * "zz" is written first and therefore holds the lower intern id, while "aa"
 * sorts first as a string, so the two orderings disagree on group 2 -- whose
 * candidates are "aa" (its own seed) and "zz" (pushed forward from node 1).
 *
 * SHIFT is an unrelated relation whose facts are parsed first and claim the
 * low ids, reversing the id order of "aa" against "zz".  Prepending it must
 * change nothing.
 */
#define SHIFT                                                    \
        ".decl Unrelated(x: symbol)\n"                           \
        "Unrelated(\"aa\"). Unrelated(\"mm\"). Unrelated(\"zz\").\n"

#define SYM_BASE                                                 \
        ".decl Seed(x: int64, l: symbol)\n"                      \
        ".decl Edge(x: int64, y: int64)\n"                       \
        ".decl Label(x: int64, l: symbol)\n"                     \
        "Seed(1,\"zz\"). Seed(2,\"aa\"). Seed(2,\"mm\").\n"      \
        "Edge(1,2). Edge(2,3).\n"                                \
        "Label(x, min(l)) :- Seed(x, l).\n"                      \
        "Label(x, min(l)) :- Label(y, l), Edge(y, x).\n"

#define SYM_REDUNDANT \
        "Label(x, min(l)) :- Label(y, l), Label(y, m), Edge(y, x).\n"

static void
test_symbol_operand_under_fusion(void)
{
    TEST("min() over a symbol column stays lexicographic under fusion");

    static const char *cases[] = {
        SYM_BASE SYM_REDUNDANT,
        SHIFT SYM_BASE SYM_REDUNDANT,
    };
    static const char *const want[] = {
        "1|zz", "2|aa", "3|aa", NULL
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
            collect_t c;
            ASSERT(eval_relation_at(cases[i], "Label",
                k_worker_counts[w], 1, &c) == 0, "evaluation failed");
            ASSERT(c.count == 3, "expected one row per node");
            ASSERT(saw_exactly(&c, want),
                "expected exactly {(1,zz),(2,aa),(3,aa)}");
            /* The answer an unset operand domain gives: "zz" was interned
             * first, so it is the smaller id (#965). */
            ASSERT(!saw(&c, "2|zz") && !saw(&c, "3|zz"),
                "reduced by intern id, not lexicographically");
        }
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_cross_rule_fusion(void)
{
    TEST("min(): two single-atom recursive rules fuse and still reduce");

    /*
     * The shape real programs have, and the one every other positive case
     * here misses.  No rule carries two IDB body atoms; the relation still
     * fuses, because count_delta_positions() sums recursive references
     * across all of the relation's rules and these two contribute one each.
     *
     * Labels propagate both along and against edges, so all four nodes are
     * one component and every label is the component minimum, 1.  Pre-fix
     * this returned 13 rows -- every node keeping every label that ever
     * reached it.
     */
    static const char *src = PATH_EDGES
        "Label(x, min(x)) :- Edge(x, y).\n"
        "Label(y, min(y)) :- Edge(x, y).\n"
        "Label(x, min(l)) :- Label(y, l), Edge(y, x).\n"
        "Label(y, min(l)) :- Label(x, l), Edge(y, x).\n";
    static const char *const want[] = {
        "1|1", "2|1", "3|1", "4|1", NULL
    };
    /* The nine extra rows the pre-fix answer carried. */
    static const char *const unwanted[] = {
        "1|2", "2|2", "2|3", "3|2", "3|3", "3|4", "4|2", "4|3", "4|4", NULL
    };

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_at(src, "Label", k_worker_counts[w],
            NO_SYMBOL_COLUMN, &c) == 0,
            "evaluation failed");
        ASSERT(c.count == 4, "expected one row per node");
        ASSERT(saw_exactly(&c, want), "expected exactly {(1,1),..,(4,1)}");
        for (size_t i = 0; unwanted[i]; i++)
            ASSERT(!saw(&c, unwanted[i]), "a dominated label survived");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

static void
test_single_idb_atom_control(void)
{
    TEST("min(): the unfused single-IDB-atom shape is unchanged");

    /* MIN_BASE has exactly one recursive rule, so the relation reaches only
     * one delta position, count_delta_positions() returns 1, and
     * rewrite_multiway_delta() does not fire -- the REDUCE stays visible in
     * the operator list.  (One recursive rule, not "no rule with two body
     * atoms": a second single-atom recursive rule would fuse this, which is
     * what the forward/backward case covers.)  This answered correctly
     * before the fix; it is the control that shows the fix moved nothing on
     * that path, and it is what fails if a fix narrows canonicalisation to
     * fused relations only. */
    static const char *src = MIN_BASE;
    static const char *const want[] = {
        "1|1", "2|1", "3|1", "4|1", NULL
    };

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_at(src, "Label", k_worker_counts[w],
            NO_SYMBOL_COLUMN, &c) == 0,
            "evaluation failed");
        ASSERT(saw_exactly(&c, want), "expected exactly {(1,1),..,(4,1)}");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * Two grouping columns.  Tag is an unrelated EDB that widens each label into
 * one label per tag, so the group key is (node, tag) and a canonicalisation
 * that grouped by the first column alone would collapse the two tags into
 * one row per node.
 */
#define GROUP2_BASE                                                     \
        ".decl Edge(x: int64, y: int64)\n"                              \
        ".decl Tag(t: int64)\n"                                         \
        ".decl Label(x: int64, t: int64, l: int64)\n"                   \
        "Edge(1,2). Edge(2,3). Edge(3,4).\n"                            \
        "Tag(7). Tag(8).\n"                                             \
        "Label(x, t, min(x)) :- Edge(x, y), Tag(t).\n"                  \
        "Label(y, t, min(y)) :- Edge(x, y), Tag(t).\n"                  \
        "Label(x, t, min(l)) :- Label(y, t, l), Edge(y, x).\n"

static void
test_group_by_two_columns(void)
{
    TEST("min(): two grouping columns collapse per (node, tag), fused");

    static const char *src = GROUP2_BASE
        "Label(x, t, min(l)) :- Label(y, t, l), Label(y, t, m), Edge(y, x).\n";
    static const char *const want[] = {
        "1|7|1", "1|8|1", "2|7|1", "2|8|1",
        "3|7|1", "3|8|1", "4|7|1", "4|8|1", NULL
    };

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_at(src, "Label", k_worker_counts[w],
            NO_SYMBOL_COLUMN, &c) == 0,
            "evaluation failed");
        ASSERT(c.count == 8, "expected one row per (node, tag)");
        ASSERT(saw_exactly(&c, want), "expected one label per (node, tag)");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * count() is not an ordering aggregate: there is no sense in which one of a
 * group's counts dominates the others, so the canonicalisation refuses any
 * relation whose REDUCE is not MIN or MAX and leaves every row standing.
 * Both spellings of the program are asserted -- fused and not -- because the
 * refusal has to survive the rewrite that hid the REDUCE.
 */
#define COUNT_BASE                                                      \
        ".decl Edge(x: int64, y: int64)\n"                              \
        ".decl Label(x: int64, l: int64)\n"                             \
        "Edge(1,3). Edge(2,3). Edge(3,4).\n"                            \
        "Label(x, count(y)) :- Edge(x, y).\n"                           \
        "Label(y, count(x)) :- Edge(x, y).\n"                           \
        "Label(x, count(l)) :- Label(y, l), Edge(y, x).\n"

static void
test_count_not_canonicalized(void)
{
    TEST("count(): groups keep every value, fused and unfused");

    static const char *unfused = COUNT_BASE;
    static const char *fused = COUNT_BASE
        "Label(x, count(l)) :- Label(y, l), Label(y, m), Edge(y, x).\n";
    /* Groups 3 and 4 hold more than one count.  Canonicalising this relation
     * would keep one row per group, which is what the assertion detects. */
    static const char *const want_unfused[] = {
        "1|1", "2|1", "3|1", "3|2", "4|1", "4|2", NULL
    };
    static const char *const want_fused[] = {
        "1|1", "2|1", "3|1", "3|2", "4|1", "4|2", "4|4", NULL
    };

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_at(unfused, "Label", k_worker_counts[w],
            NO_SYMBOL_COLUMN, &c) == 0,
            "evaluation failed");
        ASSERT(saw_exactly(&c, want_unfused),
            "unfused count() relation was reduced");

        ASSERT(eval_relation_at(fused, "Label", k_worker_counts[w],
            NO_SYMBOL_COLUMN, &c) == 0,
            "evaluation failed");
        ASSERT(saw_exactly(&c, want_fused),
            "fused count() relation was reduced");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * One head, one rule reducing by min and another by max.  There is no single
 * order the relation can be canonicalised under, so it is left alone.
 *
 * This is the case an accumulator that simply overwrites its record per
 * REDUCE gets wrong in the dangerous direction: it would adopt whichever
 * aggregate the last rule named and start collapsing a relation that is
 * correctly left alone today.  The unfused spelling is the one that catches
 * it -- that relation reaches the canonicalisation on both sides of the fix.
 */
#define MIXED_BASE                                                      \
        ".decl Edge(x: int64, y: int64)\n"                              \
        ".decl Label(x: int64, l: int64)\n"                             \
        "Edge(1,2). Edge(2,3). Edge(3,4).\n"                            \
        "Label(x, min(x)) :- Edge(x, y).\n"                             \
        "Label(y, max(y)) :- Edge(x, y).\n"                             \
        "Label(x, min(l)) :- Label(y, l), Edge(y, x).\n"

static void
test_mixed_min_max_not_canonicalized(void)
{
    TEST("min()/max() in one head: the relation is left un-canonicalised");

    static const char *unfused = MIXED_BASE;
    static const char *fused = MIXED_BASE
        "Label(x, max(l)) :- Label(y, l), Label(y, m), Edge(y, x).\n";
    static const char *const want[] = {
        "1|1", "2|1", "2|2", "3|1", "3|2", "3|3",
        "4|1", "4|2", "4|3", "4|4", NULL
    };

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_at(unfused, "Label", k_worker_counts[w],
            NO_SYMBOL_COLUMN, &c) == 0,
            "evaluation failed");
        ASSERT(saw_exactly(&c, want),
            "unfused mixed-aggregate relation was reduced");

        ASSERT(eval_relation_at(fused, "Label", k_worker_counts[w],
            NO_SYMBOL_COLUMN, &c) == 0,
            "evaluation failed");
        ASSERT(saw_exactly(&c, want),
            "fused mixed-aggregate relation was reduced");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * Issue #1024: the operand's domain is reconciled across a head's rules, so
 * rule order no longer decides how min() orders a symbol column.
 *
 * Two rules of one head can agree on the aggregate function and the number
 * of grouping columns and still disagree on the operand's domain -- here a
 * declared `symbol` column against an undeclared relation, which has no
 * column types at all.  The domain used to be the one field agg_spec_observe
 * did not compare, so whichever rule was written *last* established it: with
 * the undeclared rule last the relation reduced by intern id and answered
 * "zz", and with the declared rule last it reduced lexicographically and
 * answered "aa".  Same data, same rules, different order, different answer.
 *
 * That was a latent variant of #965, whose point was that intern ids are
 * assigned in first-appearance order, so ordering symbols by id makes the
 * result depend on which unrelated facts were parsed first.
 *
 * UNKNOWN is not a third domain to negotiate with: it means no producer
 * typed the operand, not that it is numeric.  So the rule that does know
 * wins in either direction and both programs below must now answer "aa".
 * The two orderings are the whole point -- one of them passed before the
 * fix, so a test that ran only one would not have moved.
 */
#define LASTWINS_FACTS                                                  \
        ".decl Edge(x: int64, y: int64)\n"                              \
        ".decl Sym(x: int64, v: symbol)\n"                              \
        ".decl Label(x: int64, l: symbol)\n"                            \
        "Sym(1,\"zz\"). Sym(1,\"aa\"). Sym(2,\"zz\"). Sym(2,\"aa\").\n" \
        "Edge(1,2).\n"                                                  \
        "M(x, v) :- Sym(x, v).\n"                                       \
        "Label(x, min(v)) :- Label(y, v), Edge(y, x).\n"

static void
test_operand_type_is_order_independent(void)
{
    TEST("operand domain: rule order no longer decides the ordering");

    static const char *untyped_last = LASTWINS_FACTS
        "Label(x, min(v)) :- Sym(x, v).\n"
        "Label(x, min(v)) :- M(x, v).\n";
    static const char *typed_last = LASTWINS_FACTS
        "Label(x, min(v)) :- M(x, v).\n"
        "Label(x, min(v)) :- Sym(x, v).\n";
    /* Lexicographic in both orders.  Pre-fix, untyped_last answered zz. */
    static const char *const want[] = { "1|aa", "2|aa", NULL };

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_at(untyped_last, "Label",
            k_worker_counts[w], 1, &c) == 0, "evaluation failed");
        ASSERT(saw_exactly(&c, want),
            "untyped-last must still order lexicographically");

        ASSERT(eval_relation_at(typed_last, "Label",
            k_worker_counts[w], 1, &c) == 0, "evaluation failed");
        ASSERT(saw_exactly(&c, want),
            "typed-last must still order lexicographically");
    }
    PASS();
}

/*
 * The other half of #1024: a domain disagreement that is real.
 *
 * UNKNOWN against a known domain is not a conflict and is reconciled above.
 * SCALAR against STRING is: one rule's `.decl` says the aggregated column
 * holds numbers, another's says it holds symbols, and there is no widening
 * that is right for both -- ordering the numeric rule's values
 * lexicographically would reverse-intern values that are not intern ids.
 * So the relation is vetoed and left un-canonicalised, exactly as it already
 * is when the rules disagree on the aggregate function (TEST 8) or the
 * grouping width.
 *
 * Group 1 keeping two rows is the whole assertion: it is what "not
 * canonicalised" looks like.  Without this case the veto branch is dead
 * code -- confirmed by mutation, deleting it left the other nine cases
 * green.
 *
 * The mixed output itself is not pretty, and is not made prettier here: a
 * column declared `symbol` shows a raw 5 because the numeric rule wrote one.
 * That is the pre-existing consequence of a program whose rules disagree
 * about what the column holds, and diagnosing it is #979's channel, not this
 * change's business.
 */
static void
test_operand_domain_conflict_is_vetoed(void)
{
    TEST("operand domain: a real SCALAR/STRING conflict is not reduced");

    static const char *src =
        ".decl Edge(x: int64, y: int64)\n"
        ".decl Sym(x: int64, v: symbol)\n"
        ".decl Num(x: int64, v: int64)\n"
        ".decl Label(x: int64, l: symbol)\n"
        "Sym(1,\"zz\"). Sym(1,\"aa\"). Num(1, 5). Num(1, 9).\n"
        "Edge(1,2).\n"
        "Label(x, min(v)) :- Sym(x, v).\n"
        "Label(x, min(v)) :- Num(x, v).\n"
        "Label(x, min(v)) :- Label(y, v), Edge(y, x).\n";
    static const char *const want[] = { "1|5", "1|aa", "2|aa", NULL };

    for (size_t w = 0; w < N_WORKER_COUNTS; w++) {
        collect_t c;
        ASSERT(eval_relation_at(src, "Label", k_worker_counts[w], 1, &c) == 0,
            "evaluation failed");
        ASSERT(saw_exactly(&c, want),
            "a conflicting-domain relation must be left un-canonicalised");
    }
    PASS();
}

/* ---------------------------------------------------------------------- */

int
main(void)
{
    printf("=== Recursive aggregates under K-fusion (#975) ===\n\n");

    test_min_redundant_rule_invariance();
    test_max_redundant_rule_invariance();
    test_symbol_operand_under_fusion();
    test_cross_rule_fusion();
    test_single_idb_atom_control();
    test_group_by_two_columns();
    test_count_not_canonicalized();
    test_mixed_min_max_not_canonicalized();
    test_operand_type_is_order_independent();
    test_operand_domain_conflict_is_vetoed();

    printf("\n=== %d/%d passed, %d failed ===\n", pass_count, test_count,
        fail_count);
    return fail_count == 0 ? 0 : 1;
}
