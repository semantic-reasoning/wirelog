/*
 * test_base_skip_null_template.c - Issue #1140 regression test
 *
 * col_eval_relation_plan()'s Issue #361/#367 base-case-EDB skip looks the
 * base relation up with session_find_rel() and, before this fix, passed the
 * result straight to col_rel_pool_new_like() behind the expression
 * "full ? full : NULL" -- a no-op that reads like a NULL guard but yields
 * "full" in both arms.  A miss therefore reached a constructor that
 * dereferences its template.
 *
 * The lookup misses whenever the op's relation was never inserted into the
 * session.  This test builds that state directly: a one-op VARIABLE plan on
 * a relation that is not in plan->edb_relations, evaluated at iteration 1.
 * No Datalog program is known to reach it -- the invariant that keeps it
 * unreachable is accidental, resting on col_op_variable() returning ENOENT
 * at iteration 0 in another translation unit -- so the call site is pinned
 * here instead.
 *
 * Expected: the skip is abandoned and the op takes the normal path, which
 * reports ENOENT for the missing relation.  Before the fix this returned
 * ENOMEM (after Issue #1140's constructor hardening) or segfaulted (before
 * it).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan.h"

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                       \
        do {                                 \
            printf("  [TEST] %-60s ", name); \
            fflush(stdout);                  \
        } while (0)
#define PASS              \
        do {                  \
            printf("PASS\n"); \
            tests_passed++;   \
        } while (0)
#define FAIL(msg)                  \
        do {                           \
            printf("FAIL: %s\n", msg); \
            tests_failed++;            \
        } while (0)

/*
 * Evaluate a single-op VARIABLE plan whose relation is absent from the
 * session, at iteration 1, with no delta present.  Those are exactly the
 * conditions that enter the base-case skip and then miss the lookup:
 *
 *   - op is WL_PLAN_OP_VARIABLE with delta_mode WL_DELTA_AUTO
 *   - current_iteration > 0
 *   - the next op is not a JOIN (there is no next op)
 *   - "$d$ghost" is absent, so the delta guard passes
 *   - "ghost" is not any JOIN's right_relation
 *   - "ghost" itself is absent, because plan.edb_count is 0
 */
static void
test_base_skip_missing_relation_reports_enoent(void)
{
    TEST("base-case skip: missing relation reports ENOENT, does not fault");

    wl_plan_op_t op;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_VARIABLE;
    op.relation_name = "ghost";
    op.delta_mode = WL_DELTA_AUTO;

    wl_plan_relation_t rel;
    memset(&rel, 0, sizeof(rel));
    rel.name = "idb";
    rel.ops = &op;
    rel.op_count = 1;

    wl_plan_stratum_t stratum;
    memset(&stratum, 0, sizeof(stratum));
    stratum.stratum_id = 0;
    stratum.is_recursive = false;
    stratum.relations = &rel;
    stratum.relation_count = 1;

    wl_plan_t plan;
    memset(&plan, 0, sizeof(plan));
    plan.strata = &stratum;
    plan.stratum_count = 1;
    /* plan.edb_count stays 0: nothing pre-registers "ghost". */

    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    if (backend->session_create(&plan, 1, &sess) != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    wl_col_session_t *cs = COL_SESSION(sess);
    cs->current_iteration = 1;

    eval_stack_t stack;
    eval_stack_init(&stack);
    int rc = col_eval_relation_plan(&rel, &stack, cs);
    /* The abandoned optimization must not have pushed a $base_skip
     * relation: the op takes the normal path and fails before pushing. */
    bool pushed_nothing = (stack.top == 0);
    eval_stack_drain(&stack);
    backend->session_destroy(sess);

    if (rc == ENOENT && pushed_nothing)
        PASS;
    else if (rc == ENOENT)
        FAIL("ENOENT but something was left on the eval stack");
    else if (rc == ENOMEM)
        FAIL("got ENOMEM: the skip still built an empty relation from a "
            "missing template");
    else {
        char buf[64];
        snprintf(buf, sizeof(buf), "unexpected rc=%d (%s)", rc, strerror(rc));
        FAIL(buf);
    }
}

int
main(void)
{
    printf("\n=== Base-Case Skip NULL Template Tests (Issue #1140) ===\n\n");

    test_base_skip_missing_relation_reports_enoent();

    printf("\n=== Results: %d/%d passed ===\n\n", tests_passed,
        tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
