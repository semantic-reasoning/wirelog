/*
 * tests/test_average_rejected.c - average() is rejected at lowering (#978)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * `average()` was never implemented.  col_op_reduce() seeds each group with
 * the group's first operand and its update switch has arms for
 * COUNT/SUM/MIN/MAX only, so WIRELOG_AGG_AVG fell through `default: break;`
 * and the seed was returned unchanged.  The answer therefore followed scan
 * order, not the data:
 *
 *     val(1,9). val(1,5). val(1,2).  ->  t(1, 9)   [mean 5]
 *     val(1,1). val(1,2). val(1,9).  ->  t(1, 1)   [mean 4]
 *
 * The legacy executor still has no float column lane to return a mean in --
 * storage is int64_t throughout -- so the choice was truncating integer
 * division or rejection.  Rejection was taken: widening to float later is a
 * compatible change, whereas replacing truncation with float later would
 * silently change every program's numbers.
 * See docs/SYNTAX.md and the #973 precedent (reject first, support later).
 *
 * Case map:
 *
 *   test_average_rejected_at_lowering
 *       Both accepted spellings (`average` and `AVG`) must PARSE and then
 *       fail to lower.  The distinction matters: the fix must not be a
 *       lexer change, because that would also have to steal the identifiers
 *       `avg`/`AVERAGE` and would break tests/test_parser.c, which asserts
 *       the AST node.  Includes a head-only shape and a recursive shape.
 *
 *   test_other_aggregates_still_evaluate
 *       sum/count/min/max over the same facts must still lower AND still
 *       produce the right numbers.  These are load-bearing: a guard that
 *       rejects every aggregate would pass the negatives above.
 *
 *   test_documented_workaround_evaluates
 *       The sum/count workaround named in the diagnostic must actually
 *       work, and must give the truncated mean of the same facts.
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
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

/* ---------------------------------------------------------------------- */
/* Shared source fragments                                                 */
/* ---------------------------------------------------------------------- */

/*
 * Group 1 holds 9, 5, 2 -- sum 16, count 3, min 2, max 9, mean 5 (truncated
 * from 5.33).  Group 2 holds a single 4, where every aggregate agrees, which
 * is what makes a control that only checks "one row per group" useless here.
 */
#define FACTS                                  \
        ".decl val(g: int64, v: int64)\n"          \
        "val(1, 9).\n"                             \
        "val(1, 5).\n"                             \
        "val(1, 2).\n"                             \
        "val(2, 4).\n"

/* ---------------------------------------------------------------------- */
/* Harness                                                                 */
/* ---------------------------------------------------------------------- */

typedef enum {
    LOWER_OK = 0,        /* parsed and lowered */
    LOWER_PARSE_ERROR,   /* the parser refused it */
    LOWER_REJECTED,      /* parsed, then wl_plan_from_program() refused it */
} lower_status_t;

/*
 * Parse @src and lower it, reporting which of the two stages refused it.
 *
 * Keeping the two apart is the point: "average is rejected" must mean
 * rejected at lowering.  A test that only asserted "this program does not
 * run" would be satisfied by a lexer change, which is the fix this issue
 * explicitly does not want.
 */
static lower_status_t
lower_status(const char *src)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return LOWER_PARSE_ERROR;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return LOWER_REJECTED;
    }
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return LOWER_OK;
}

#define MAX_SEEN 32

typedef struct {
    const char *rel;
    uint32_t count;
    int64_t rows[MAX_SEEN][2];
} collect_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user)
{
    collect_t *c = (collect_t *)user;
    if (!c->rel || !relation || strcmp(relation, c->rel) != 0)
        return;
    if (ncols >= 2 && c->count < MAX_SEEN) {
        c->rows[c->count][0] = row[0];
        c->rows[c->count][1] = row[1];
    }
    c->count++;
}

/* True if @rel holds exactly the row (@g, @v). */
static bool
saw_pair(const collect_t *c, int64_t g, int64_t v)
{
    uint32_t n = c->count < MAX_SEEN ? c->count : MAX_SEEN;
    for (uint32_t i = 0; i < n; i++) {
        if (c->rows[i][0] == g && c->rows[i][1] == v)
            return true;
    }
    return false;
}

/* Evaluate @src and collect @relation's tuples.  Returns 0 on success. */
static int
eval_relation(const char *src, const char *relation, collect_t *out)
{
    memset(out, 0, sizeof(*out));
    out->rel = relation;

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
        fprintf(stderr, "  lowering failed\n");
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    int result = -1;
    if (wl_session_load_facts(sess, prog) == 0
        && wl_session_snapshot(sess, collect_cb, out) == 0)
        result = 0;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return result;
}

/* ---------------------------------------------------------------------- */

static void
test_average_rejected_at_lowering(void)
{
    TEST("average() is rejected at lowering, not at parsing");

    static const struct {
        const char *what;
        const char *src;
    } cases[] = {
        { "average(v) in a head",
          FACTS ".decl t(g: int64, a: int64)\n"
          "t(g, average(v)) :- val(g, v).\n" },
        /* The uppercase spelling is a different lexer token reaching the
         * same WIRELOG_AGG_AVG, so it needs its own case. */
        { "AVG(v) in a head",
          FACTS ".decl t(g: int64, a: int64)\n"
          "t(g, AVG(v)) :- val(g, v).\n" },
        /* No group-by column at all: a global average. */
        { "average over the whole relation",
          FACTS ".decl t(a: int64)\n"
          "t(average(v)) :- val(g, v).\n" },
        /* An expression operand still resolves to the same aggregate. */
        { "average of an arithmetic operand",
          FACTS ".decl t(g: int64, a: int64)\n"
          "t(g, average(v + 1)) :- val(g, v).\n" },
        /* Recursive shape.  The recursive reducer is a second code path and
         * a guard placed only on the non-recursive one would miss it.
         * (Recursive sum/count/average all violate the head relation's
         * functional dependency; that is #991 and is not what this asserts.
         * All this asserts is that the guard is reached here too.) */
        { "average inside a recursive stratum",
          ".decl edge(x: int64, y: int64)\n"
          "edge(1, 2).\n"
          ".decl cc(n: int64, a: int64)\n"
          "cc(1, 5).\n"
          "cc(y, average(c)) :- cc(x, c), edge(x, y).\n" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        lower_status_t st = lower_status(cases[i].src);
        if (st == LOWER_PARSE_ERROR) {
            fprintf(stderr, "  case: %s\n", cases[i].what);
            FAIL("must still parse -- the rejection belongs at lowering, "
                "not in the lexer");
        }
        if (st != LOWER_REJECTED) {
            fprintf(stderr, "  case: %s\n", cases[i].what);
            FAIL("average() lowered instead of being rejected");
        }
    }
    PASS();
}

static void
test_other_aggregates_still_evaluate(void)
{
    TEST("sum/count/min/max still lower and still give the right numbers");

    static const struct {
        const char *fn;
        int64_t g1; /* expected value for group 1 over {9, 5, 2} */
        int64_t g2; /* expected value for group 2 over {4} */
    } cases[] = {
        { "sum", 16, 4 },
        { "count", 3, 1 },
        { "min", 2, 4 },
        { "max", 9, 4 },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char src[512];
        snprintf(src, sizeof(src),
            FACTS ".decl t(g: int64, a: int64)\n"
            "t(g, %s(v)) :- val(g, v).\n", cases[i].fn);

        ASSERT(lower_status(src) == LOWER_OK,
            "a working aggregate stopped lowering");

        collect_t c;
        ASSERT(eval_relation(src, "t", &c) == 0, "evaluation failed");
        ASSERT(c.count == 2, "expected one row per group");
        ASSERT(saw_pair(&c, 1, cases[i].g1), "wrong value for group 1");
        ASSERT(saw_pair(&c, 2, cases[i].g2), "wrong value for group 2");
    }
    PASS();
}

static void
test_documented_workaround_evaluates(void)
{
    TEST("the sum/count workaround gives the truncated mean");

    /* This is the program the rejection diagnostic tells the user to write.
     * If it stops working the diagnostic becomes a lie, so it is asserted
     * here rather than only in prose. */
    static const char *src
        = FACTS ".decl s(g: int64, x: int64)\n"
        ".decl c(g: int64, y: int64)\n"
        ".decl t(g: int64, a: int64)\n"
        "s(g, sum(v)) :- val(g, v).\n"
        "c(g, count(v)) :- val(g, v).\n"
        "t(g, x / y) :- s(g, x), c(g, y).\n";

    collect_t c;
    ASSERT(eval_relation(src, "t", &c) == 0, "evaluation failed");
    ASSERT(c.count == 2, "expected one row per group");
    ASSERT(saw_pair(&c, 1, 5), "16 / 3 should be 5");
    ASSERT(saw_pair(&c, 2, 4), "4 / 1 should be 4");
    PASS();
}

/* ---------------------------------------------------------------------- */

int
main(void)
{
    printf("=== average() rejected at lowering (Issue #978) ===\n");

    test_average_rejected_at_lowering();
    test_other_aggregates_still_evaluate();
    test_documented_workaround_evaluates();

    printf("\n%d/%d passed, %d failed\n", pass_count, test_count, fail_count);
    return fail_count == 0 ? 0 : 1;
}
