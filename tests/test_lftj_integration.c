/*
 * test_lftj_integration.c - End-to-end integration tests for LFTJ (Issue #195)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates that:
 *   1. wl_plan_from_program emits WL_PLAN_OP_LFTJ for eligible 3+ EDB chains
 *   2. wl_plan_from_program does NOT rewrite 2-way EDB joins (k < 3)
 *   3. 3-way join via session API produces correct results
 *   4. 4-way join via session API produces correct results
 *   5. No false positive: IDB relations are not rewritten as LFTJ
 */

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/lftj.h"
#include "../wirelog/columnar/memory_governor.h"
#include "../wirelog/exec_plan.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

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

/* ----------------------------------------------------------------
 * Tuple collector
 * ---------------------------------------------------------------- */

#define MAX_ROWS 256
#define MAX_NCOLS 16

typedef struct {
    uint32_t count;
    int64_t rows[MAX_ROWS][MAX_NCOLS];
    uint32_t ncols[MAX_ROWS];
    int oom;
} collect_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    (void)relation;
    collect_t *c = (collect_t *)user;
    if (c->count >= MAX_ROWS) {
        c->oom = 1;
        return;
    }
    uint32_t idx = c->count++;
    c->ncols[idx] = ncols < MAX_NCOLS ? ncols : MAX_NCOLS;
    for (uint32_t i = 0; i < c->ncols[idx]; i++)
        c->rows[idx][i] = row[i];
}

/* ----------------------------------------------------------------
 * Session helpers
 * ---------------------------------------------------------------- */

/*
 * make_plan: parse + apply all optimizer passes + generate plan.
 * Note: JPP and SIP passes interact with SEMIJOIN schema tracking in a way
 * that can produce incorrect project_indices for 4+ way EDB joins.
 * Use make_plan_no_opt for LFTJ-specific tests to avoid that interference.
 */
static int
make_plan(const char *src, wl_plan_t **out_plan, wirelog_program_t **out_prog)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    *out_plan = plan;
    *out_prog = prog;
    return 0;
}

/*
 * make_plan_no_opt: parse + fusion only (no JPP/SIP) + generate plan.
 * Use for LFTJ tests: JPP/SIP insert SEMIJOIN ops that break the
 * consecutive JOIN chain the LFTJ detector looks for.
 */
static int
make_plan_no_opt(const char *src, wl_plan_t **out_plan,
    wirelog_program_t **out_prog)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    *out_plan = plan;
    *out_prog = prog;
    return 0;
}

static int
run_session_no_opt(const char *src, collect_t *out)
{
    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;

    if (make_plan_no_opt(src, &plan, &prog) != 0)
        return -1;

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (wl_session_load_facts(sess, prog) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    memset(out, 0, sizeof(*out));
    int rc = wl_session_snapshot(sess, collect_cb, out);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

/* ----------------------------------------------------------------
 * Plan inspection helpers
 * ---------------------------------------------------------------- */

/* Count WL_PLAN_OP_LFTJ operators across all strata and relations. */
static uint32_t
count_lftj_ops(const wl_plan_t *plan)
{
    uint32_t n = 0;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        for (uint32_t r = 0; r < st->relation_count; r++) {
            const wl_plan_relation_t *rel = &st->relations[r];
            for (uint32_t o = 0; o < rel->op_count; o++) {
                if (rel->ops[o].op == WL_PLAN_OP_LFTJ)
                    n++;
            }
        }
    }
    return n;
}

static wl_columnar_memory_governor_ref_t *
test_governor(uint64_t usable_bytes)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = usable_bytes,
        .headroom_bytes = 0,
        .usable_bytes = usable_bytes,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    return wl_columnar_memory_governor_ref_create(&resolution);
}

/* ----------------------------------------------------------------
 * Tests
 * ---------------------------------------------------------------- */

/* Test 1: 3-way EDB join is rewritten to LFTJ in the generated plan. */
static void
test_plan_has_lftj(void)
{
    TEST("Plan contains WL_PLAN_OP_LFTJ for 3-way EDB join");

    const char *src = ".decl r1(x: int32, a: int32)\n"
        ".decl r2(x: int32, b: int32)\n"
        ".decl r3(x: int32, c: int32)\n"
        ".decl out(x: int32, a: int32, b: int32, c: int32)\n"
        "r1(1, 10). r2(1, 100). r3(1, 1000).\n"
        "out(x, a, b, c) :- r1(x, a), r2(x, b), r3(x, c).\n";

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    ASSERT(make_plan_no_opt(src, &plan, &prog) == 0, "make_plan_no_opt failed");

    uint32_t n = count_lftj_ops(plan);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    ASSERT(n >= 1, "expected >= 1 WL_PLAN_OP_LFTJ, got 0");
    PASS();
}

/* Test 2: 2-way EDB join is NOT rewritten to LFTJ (k < 3). */
static void
test_plan_no_lftj_for_binary(void)
{
    TEST("Plan does NOT contain WL_PLAN_OP_LFTJ for 2-way EDB join");

    const char *src = ".decl r1(x: int32, a: int32)\n"
        ".decl r2(x: int32, b: int32)\n"
        ".decl out(x: int32, a: int32, b: int32)\n"
        "r1(1, 10). r2(1, 100).\n"
        "out(x, a, b) :- r1(x, a), r2(x, b).\n";

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    ASSERT(make_plan(src, &plan, &prog) == 0, "make_plan failed");

    uint32_t n = count_lftj_ops(plan);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    ASSERT(n == 0, "2-way EDB join incorrectly rewritten as LFTJ");
    PASS();
}

/* Test 3: 3-way join produces correct result tuples via session. */
static void
test_3way_join_result(void)
{
    TEST("3-way EDB join produces correct result via session");

    /*
     * r1(x, a): (1,10) (2,20) (3,30)
     * r2(x, b): (1,100) (2,200) (4,400)
     * r3(x, c): (1,1000) (3,3000) (5,5000)
     *
     * out(x, a, b, c) :- r1(x,a), r2(x,b), r3(x,c)
     * Only x=1 is in all three: out(1, 10, 100, 1000)
     */
    const char *src = ".decl r1(x: int32, a: int32)\n"
        ".decl r2(x: int32, b: int32)\n"
        ".decl r3(x: int32, c: int32)\n"
        ".decl out(x: int32, a: int32, b: int32, c: int32)\n"
        "r1(1, 10). r1(2, 20). r1(3, 30).\n"
        "r2(1, 100). r2(2, 200). r2(4, 400).\n"
        "r3(1, 1000). r3(3, 3000). r3(5, 5000).\n"
        "out(x, a, b, c) :- r1(x, a), r2(x, b), r3(x, c).\n";

    collect_t coll;
    ASSERT(run_session_no_opt(src, &coll) == 0, "run_session_no_opt failed");
    ASSERT(!coll.oom, "tuple collector overflowed");

    /* Filter to 4-column result tuples (r1/r2/r3 have 2 cols each). */
    uint32_t out_count = 0;
    bool found_expected = false;
    for (uint32_t i = 0; i < coll.count; i++) {
        if (coll.ncols[i] != 4)
            continue;
        out_count++;
        if (coll.rows[i][0] == 1 && coll.rows[i][1] == 10
            && coll.rows[i][2] == 100 && coll.rows[i][3] == 1000)
            found_expected = true;
    }

    ASSERT(out_count == 1, "expected exactly 1 output tuple");
    ASSERT(found_expected, "expected out(1,10,100,1000) not found");
    PASS();
}

/* Test 4: planned/session LFTJ uses numeric FLOAT key semantics. */
static void
test_float_join_result(void)
{
    TEST("planned 3-way FLOAT join matches -0.0 and +0.0 numerically");

    const char *src = ".decl r1(x: float)\n"
        ".decl r2(x: float)\n"
        ".decl r3(x: float)\n"
        ".decl out(x: float)\n"
        "r1(-1.0). r1(-0.0).\n"
        "r2(-1.0). r2(0.0).\n"
        "r3(-1.0). r3(0.0).\n"
        "out(x) :- r1(x), r2(x), r3(x).\n";

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    ASSERT(make_plan_no_opt(src, &plan, &prog) == 0,
        "float plan construction failed");
    ASSERT(count_lftj_ops(plan) >= 1, "expected FLOAT LFTJ operator");
    wl_plan_free(plan);
    wirelog_program_free(prog);

    collect_t coll;
    ASSERT(run_session_no_opt(src, &coll) == 0,
        "float session evaluation failed");
    ASSERT(!coll.oom, "tuple collector overflowed");
    uint32_t out_count = 0;
    for (uint32_t i = 0; i < coll.count; i++) {
        if (coll.ncols[i] == 1)
            out_count++;
    }
    ASSERT(out_count == 2, "expected -1.0 and one canonicalized zero result");
    PASS();
}

/* Test 4: 4-way join produces correct result via session. */
static void
test_4way_join_result(void)
{
    TEST("4-way EDB join produces correct result via session");

    /*
     * r1(x, a): (1,11) (2,21) (3,31)
     * r2(x, b): (1,12) (2,22) (4,42)
     * r3(x, c): (1,13) (3,33) (5,53)
     * r4(x, d): (1,14) (2,24) (6,64)
     *
     * out(x,a,b,c,d) :- r1(x,a), r2(x,b), r3(x,c), r4(x,d)
     * x=1 is in all four: out(1, 11, 12, 13, 14)
     */
    const char *src
        = ".decl r1(x: int32, a: int32)\n"
        ".decl r2(x: int32, b: int32)\n"
        ".decl r3(x: int32, c: int32)\n"
        ".decl r4(x: int32, d: int32)\n"
        ".decl out(x: int32, a: int32, b: int32, c: int32, d: int32)\n"
        "r1(1, 11). r1(2, 21). r1(3, 31).\n"
        "r2(1, 12). r2(2, 22). r2(4, 42).\n"
        "r3(1, 13). r3(3, 33). r3(5, 53).\n"
        "r4(1, 14). r4(2, 24). r4(6, 64).\n"
        "out(x, a, b, c, d) :- r1(x, a), r2(x, b), r3(x, c), r4(x, d).\n";

    collect_t coll;
    ASSERT(run_session_no_opt(src, &coll) == 0, "run_session_no_opt failed");
    ASSERT(!coll.oom, "tuple collector overflowed");

    uint32_t out_count = 0;
    bool found_expected = false;
    for (uint32_t i = 0; i < coll.count; i++) {
        if (coll.ncols[i] != 5)
            continue;
        out_count++;
        if (coll.rows[i][0] == 1 && coll.rows[i][1] == 11
            && coll.rows[i][2] == 12 && coll.rows[i][3] == 13
            && coll.rows[i][4] == 14)
            found_expected = true;
    }

    ASSERT(out_count == 1, "expected exactly 1 output tuple");
    ASSERT(found_expected, "expected out(1,11,12,13,14) not found");
    PASS();
}

/* Test 5: a full evaluation stack does not leak the LFTJ output relation. */
static void
test_lftj_stack_overflow_releases_output(void)
{
    TEST("LFTJ releases output when the evaluation stack is full");

    const char *src = ".decl r1(x: int32, a: int32)\n"
        ".decl r2(x: int32, b: int32)\n"
        ".decl r3(x: int32, c: int32)\n"
        ".decl out(x: int32, a: int32, b: int32, c: int32)\n"
        "r1(1, 10). r2(1, 100). r3(1, 1000).\n"
        "out(x, a, b, c) :- r1(x, a), r2(x, b), r3(x, c).\n";
    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    ASSERT(make_plan_no_opt(src, &plan, &prog) == 0,
        "make_plan_no_opt failed");

    wl_session_t *sess = NULL;
    ASSERT(wl_session_create(wl_backend_columnar(), plan, 1, &sess) == 0,
        "session_create failed");
    ASSERT(wl_session_load_facts(sess, prog) == 0,
        "session_load_facts failed");

    const wl_plan_op_t *lftj_op = NULL;
    for (uint32_t s = 0; s < plan->stratum_count && !lftj_op; s++) {
        const wl_plan_stratum_t *stratum = &plan->strata[s];
        for (uint32_t r = 0; r < stratum->relation_count && !lftj_op; r++) {
            const wl_plan_relation_t *relation = &stratum->relations[r];
            for (uint32_t o = 0; o < relation->op_count; o++) {
                if (relation->ops[o].op == WL_PLAN_OP_LFTJ) {
                    lftj_op = &relation->ops[o];
                    break;
                }
            }
        }
    }
    ASSERT(lftj_op != NULL, "expected an LFTJ operator");

    eval_stack_t stack;
    eval_stack_init(&stack);
    for (uint32_t i = 0; i < COL_STACK_MAX; i++) {
        col_rel_t *filler = col_rel_new_auto("$lftj_stack_filler", 0);
        ASSERT(filler != NULL, "filler relation allocation failed");
        ASSERT(eval_stack_push(&stack, filler, true) == 0,
            "filler push failed");
    }

    int rc = col_op_lftj(lftj_op, &stack, COL_SESSION(sess));
    ASSERT(rc == ENOBUFS, "full stack must reject the LFTJ output");
    ASSERT(stack.top == COL_STACK_MAX,
        "failed LFTJ push must not change the stack depth");
    wl_col_session_t *columnar = COL_SESSION(sess);
    ASSERT(columnar->sarr_active_pins == 0,
        "failed LFTJ push leaked sorted-arrangement probes");
    for (uint32_t i = 0; i < columnar->sarr_count; i++)
        ASSERT(columnar->sarr_entries[i].sarr.pin_count == 0,
            "failed LFTJ push left a sorted arrangement pinned");

    eval_stack_drain(&stack);
    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_lftj_inner_denial_propagates_and_retries(void)
{
    TEST("inner LFTJ denial restores state and permits retry");
    const char *src = ".decl r1(x: int32, a: int32)\n"
        ".decl r2(x: int32, b: int32)\n"
        ".decl r3(x: int32, c: int32)\n"
        ".decl out(x: int32, a: int32, b: int32, c: int32)\n"
        "r1(1, 10). r2(1, 100). r3(1, 1000).\n"
        "out(x, a, b, c) :- r1(x, a), r2(x, b), r3(x, c).\n";
    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    wl_columnar_memory_governor_ref_t *ref
        = test_governor(UINT64_C(1) << 30);
    wl_session_options_t options;
    eval_stack_t stack;
    bool stack_initialized = false;
    wl_columnar_lftj_test_hook_state_t hook_state = {0};
    wl_col_session_t *columnar = NULL;
    const wl_plan_op_t *lftj_op = NULL;
    const char *failure = NULL;
    uint64_t baseline_reserved = 0;
    uint64_t original_usable = UINT64_C(1) << 30;
    int64_t expected_row[16] = { 0 };
    uint32_t expected_ncols = 0;
    int rc;

    wl_columnar_lftj_test_clear_hooks();
    if (!ref) {
        failure = "governor creation failed";
        goto cleanup;
    }
    if (make_plan_no_opt(src, &plan, &prog) != 0) {
        failure = "plan creation failed";
        goto cleanup;
    }
    wl_session_options_init(&options);
    options.memory_governor = ref;
    if (wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) != 0 || !session) {
        failure = "session creation failed";
        goto cleanup;
    }
    if (wl_session_load_facts(session, prog) != 0) {
        failure = "loading EDB facts failed";
        goto cleanup;
    }
    for (uint32_t s = 0; s < plan->stratum_count && !lftj_op; s++) {
        const wl_plan_stratum_t *stratum = &plan->strata[s];
        for (uint32_t r = 0; r < stratum->relation_count && !lftj_op; r++) {
            const wl_plan_relation_t *relation = &stratum->relations[r];
            for (uint32_t o = 0; o < relation->op_count; o++) {
                if (relation->ops[o].op == WL_PLAN_OP_LFTJ) {
                    lftj_op = &relation->ops[o];
                    break;
                }
            }
        }
    }
    if (!lftj_op) {
        failure = "plan has no LFTJ operation";
        goto cleanup;
    }
    columnar = COL_SESSION(session);
    original_usable = atomic_load_explicit(
        &wl_columnar_memory_governor_ref_get(ref)->usable_bytes,
        memory_order_relaxed);
    eval_stack_init(&stack);
    stack_initialized = true;

    /* First execution creates and retains sorted arrangements. */
    rc = col_op_lftj(lftj_op, &stack, columnar);
    if (rc != 0 || stack.top != 1 || !stack.items[0].rel
        || stack.items[0].rel->nrows != 1) {
        failure = "LFTJ warmup did not produce the expected row";
        goto cleanup;
    }
    expected_ncols = stack.items[0].rel->ncols;
    if (expected_ncols > sizeof(expected_row) / sizeof(expected_row[0])) {
        failure = "LFTJ warmup produced an unexpected schema width";
        goto cleanup;
    }
    for (uint32_t col = 0; col < expected_ncols; col++)
        expected_row[col] = stack.items[0].rel->columns[col][0];
    eval_stack_drain(&stack);
    baseline_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    if (columnar->sarr_active_pins != 0 || baseline_reserved == 0
        || columnar->sarr_count < 3) {
        failure = "warm sorted arrangements did not reach a stable baseline";
        goto cleanup;
    }

    columnar->memory_budget_denied = false;
    wl_columnar_lftj_test_deny_next_inner_admission();
    rc = col_op_lftj(lftj_op, &stack, columnar);
    wl_columnar_lftj_test_get_hook_state(&hook_state);
    if (hook_state.deny_inner_consumed) {
        atomic_store_explicit(
            &wl_columnar_memory_governor_ref_get(ref)->usable_bytes,
            hook_state.previous_usable_bytes, memory_order_relaxed);
    }
    if (!hook_state.deny_inner_consumed
        || hook_state.inner_status != WL_COLUMNAR_MEMORY_ADMISSION_DENIED
        || hook_state.reserved_before_inner <= baseline_reserved
        || rc != ENOSPC || !columnar->memory_budget_denied || stack.top != 0) {
        failure = "operator did not propagate a real inner-admission denial";
        goto cleanup;
    }
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != baseline_reserved) {
        failure = "inner denial leaked temporary reservation bytes";
        goto cleanup;
    }
    if (columnar->sarr_active_pins != 0) {
        failure = "inner denial leaked an arrangement probe";
        goto cleanup;
    }
    for (uint32_t i = 0; i < columnar->sarr_count; i++) {
        if (columnar->sarr_entries[i].sarr.pin_count != 0) {
            failure = "inner denial left a sorted arrangement pinned";
            goto cleanup;
        }
    }
    for (uint32_t i = 0; i < 3; i++) {
        static const char *const names[] = { "r1", "r2", "r3" };
        col_rel_t *rel = session_find_rel(columnar, names[i]);
        if (!rel || wl_columnar_source_access_gate_busy(&rel->source_access)) {
            failure = "inner denial leaked an input source reader";
            goto cleanup;
        }
    }

    wl_columnar_lftj_test_clear_hooks();
    columnar->memory_budget_denied = false;
    rc = col_op_lftj(lftj_op, &stack, columnar);
    if (rc != 0 || columnar->memory_budget_denied || stack.top != 1
        || !stack.items[0].rel || stack.items[0].rel->nrows != 1
        || stack.items[0].rel->ncols != expected_ncols) {
        failure = "LFTJ retry did not publish the expected row";
        goto cleanup;
    }
    for (uint32_t col = 0; col < expected_ncols; col++) {
        if (stack.items[0].rel->columns[col][0] != expected_row[col]) {
            failure = "LFTJ retry row differs from successful warmup";
            goto cleanup;
        }
    }
    eval_stack_drain(&stack);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != baseline_reserved) {
        failure = "successful retry did not release output reservation";
        goto cleanup;
    }

cleanup:
    wl_columnar_lftj_test_clear_hooks();
    if (columnar && ref) {
        atomic_store_explicit(
            &wl_columnar_memory_governor_ref_get(ref)->usable_bytes,
            original_usable,
            memory_order_relaxed);
        columnar->memory_budget_denied = false;
    }
    if (stack_initialized)
        eval_stack_drain(&stack);
    if (session)
        wl_session_destroy(session);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
    if (failure)
        FAIL(failure);
    PASS();
}

static void
test_lftj_output_growth_denial_propagates_and_retries(void)
{
    TEST("LFTJ output growth denial and allocator failure restore state");
    char source[8192];
    size_t used = 0;
    const char *prefix = ".decl r1(x: int32, a: int32)\n"
        ".decl r2(x: int32, b: int32)\n"
        ".decl r3(x: int32, c: int32)\n"
        ".decl out(x: int32, a: int32, b: int32, c: int32)\n";
    int written = snprintf(source, sizeof(source), "%s", prefix);
    if (written < 0 || (size_t)written >= sizeof(source))
        FAIL("query prefix did not fit the test buffer");
    used = (size_t)written;
    for (uint32_t i = 1; i <= 100; i++) {
        written = snprintf(source + used, sizeof(source) - used,
                "r1(%u, %u). r2(%u, %u). r3(%u, %u).\n",
                i, i + 100u, i, i + 200u, i, i + 300u);
        if (written < 0 || (size_t)written >= sizeof(source) - used)
            FAIL("generated input facts did not fit the test buffer");
        used += (size_t)written;
    }
    written = snprintf(source + used, sizeof(source) - used,
            "out(x, a, b, c) :- r1(x, a), r2(x, b), r3(x, c).\n");
    if (written < 0 || (size_t)written >= sizeof(source) - used)
        FAIL("query rule did not fit the test buffer");

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    wl_columnar_memory_governor_ref_t *ref
        = test_governor(UINT64_C(1) << 30);
    wl_session_options_t options;
    eval_stack_t stack;
    bool stack_initialized = false;
    wl_columnar_lftj_output_test_hook_state_t hook_state = {0};
    wl_col_session_t *columnar = NULL;
    const wl_plan_op_t *lftj_op = NULL;
    const char *failure = NULL;
    uint64_t baseline_reserved = 0;
    uint64_t original_usable = UINT64_C(1) << 30;
    int rc;

    wl_columnar_lftj_test_clear_hooks();
    wl_columnar_lftj_test_clear_output_hooks();
    if (!ref) {
        failure = "governor creation failed";
        goto cleanup;
    }
    if (make_plan_no_opt(source, &plan, &prog) != 0) {
        failure = "plan creation failed";
        goto cleanup;
    }
    wl_session_options_init(&options);
    options.memory_governor = ref;
    if (wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) != 0 || !session) {
        failure = "session creation failed";
        goto cleanup;
    }
    if (wl_session_load_facts(session, prog) != 0) {
        failure = "loading EDB facts failed";
        goto cleanup;
    }
    for (uint32_t s = 0; s < plan->stratum_count && !lftj_op; s++) {
        const wl_plan_stratum_t *stratum = &plan->strata[s];
        for (uint32_t r = 0; r < stratum->relation_count && !lftj_op; r++) {
            const wl_plan_relation_t *relation = &stratum->relations[r];
            for (uint32_t o = 0; o < relation->op_count; o++) {
                if (relation->ops[o].op == WL_PLAN_OP_LFTJ) {
                    lftj_op = &relation->ops[o];
                    break;
                }
            }
        }
    }
    if (!lftj_op) {
        failure = "plan has no LFTJ operation";
        goto cleanup;
    }
    columnar = COL_SESSION(session);
    original_usable = atomic_load_explicit(
        &wl_columnar_memory_governor_ref_get(ref)->usable_bytes,
        memory_order_relaxed);
    eval_stack_init(&stack);
    stack_initialized = true;

    rc = col_op_lftj(lftj_op, &stack, columnar);
    if (rc != 0 || stack.top != 1 || !stack.items[0].rel
        || stack.items[0].rel->nrows != 100) {
        failure = "warmup did not publish all 100 expected rows";
        goto cleanup;
    }
    eval_stack_drain(&stack);
    baseline_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
    if (baseline_reserved == 0 || columnar->sarr_active_pins != 0) {
        failure = "warm arrangements did not reach a stable baseline";
        goto cleanup;
    }

    columnar->memory_budget_denied = false;
    wl_columnar_lftj_test_deny_next_output_growth();
    rc = col_op_lftj(lftj_op, &stack, columnar);
    wl_columnar_lftj_test_get_output_hook_state(&hook_state);
    if (hook_state.deny_output_growth_consumed) {
        atomic_store_explicit(
            &wl_columnar_memory_governor_ref_get(ref)->usable_bytes,
            hook_state.previous_usable_bytes, memory_order_relaxed);
    }
    if (!hook_state.deny_output_growth_consumed
        || hook_state.output_rows_at_growth == 0
        || hook_state.output_rows_at_growth
        < hook_state.output_capacity_at_growth
        || hook_state.reserved_before_growth < baseline_reserved
        || rc != ENOSPC || !columnar->memory_budget_denied || stack.top != 0) {
        failure = "output growth denial was not propagated as ENOSPC";
        goto cleanup;
    }
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != baseline_reserved) {
        failure = "output growth denial leaked reservation bytes";
        goto cleanup;
    }
    if (columnar->sarr_active_pins != 0) {
        failure = "output growth denial leaked an arrangement probe";
        goto cleanup;
    }
    for (uint32_t i = 0; i < columnar->sarr_count; i++) {
        if (columnar->sarr_entries[i].sarr.pin_count != 0) {
            failure = "output growth denial left an arrangement pinned";
            goto cleanup;
        }
    }
    for (uint32_t i = 0; i < 3; i++) {
        static const char *const names[] = { "r1", "r2", "r3" };
        col_rel_t *rel = session_find_rel(columnar, names[i]);
        if (!rel || wl_columnar_source_access_gate_busy(&rel->source_access)) {
            failure = "output growth denial leaked a source reader";
            goto cleanup;
        }
    }

    wl_columnar_lftj_test_clear_output_hooks();
    columnar->memory_budget_denied = false;
    rc = col_op_lftj(lftj_op, &stack, columnar);
    if (rc != 0 || columnar->memory_budget_denied || stack.top != 1
        || !stack.items[0].rel || stack.items[0].rel->nrows != 100) {
        failure = "retry after output growth denial failed";
        goto cleanup;
    }
    eval_stack_drain(&stack);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != baseline_reserved) {
        failure = "successful retry leaked output reservation bytes";
        goto cleanup;
    }

    columnar->memory_budget_denied = false;
    wl_columnar_lftj_test_fail_next_output_growth();
    rc = col_op_lftj(lftj_op, &stack, columnar);
    wl_columnar_lftj_test_get_output_hook_state(&hook_state);
    if (!hook_state.fail_output_growth_consumed
        || hook_state.output_rows_at_growth == 0
        || hook_state.output_rows_at_growth
        < hook_state.output_capacity_at_growth
        || rc != ENOMEM
        || hook_state.failure_pending_flag || columnar->memory_budget_denied
        || stack.top != 0) {
        failure = "ordinary output allocator failure was misclassified";
        goto cleanup;
    }
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != baseline_reserved) {
        failure = "ordinary output failure leaked reservations";
        goto cleanup;
    }
    if (columnar->sarr_active_pins != 0) {
        failure = "ordinary output failure leaked an arrangement probe";
        goto cleanup;
    }
    for (uint32_t i = 0; i < columnar->sarr_count; i++) {
        if (columnar->sarr_entries[i].sarr.pin_count != 0) {
            failure = "ordinary output failure left an arrangement pinned";
            goto cleanup;
        }
    }
    for (uint32_t i = 0; i < 3; i++) {
        static const char *const names[] = { "r1", "r2", "r3" };
        col_rel_t *rel = session_find_rel(columnar, names[i]);
        if (!rel || wl_columnar_source_access_gate_busy(&rel->source_access)) {
            failure = "ordinary output failure leaked a source reader";
            goto cleanup;
        }
    }
    wl_columnar_lftj_test_clear_output_hooks();
    rc = col_op_lftj(lftj_op, &stack, columnar);
    if (rc != 0 || stack.top != 1 || !stack.items[0].rel
        || stack.items[0].rel->nrows != 100) {
        failure = "retry after ordinary output failure failed";
        goto cleanup;
    }
    eval_stack_drain(&stack);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != baseline_reserved) {
        failure = "ordinary output retry leaked reservation bytes";
        goto cleanup;
    }

cleanup:
    wl_columnar_lftj_test_clear_hooks();
    wl_columnar_lftj_test_clear_output_hooks();
    if (columnar && ref) {
        atomic_store_explicit(
            &wl_columnar_memory_governor_ref_get(ref)->usable_bytes,
            original_usable, memory_order_relaxed);
        columnar->memory_budget_denied = false;
    }
    if (stack_initialized)
        eval_stack_drain(&stack);
    if (session)
        wl_session_destroy(session);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
    if (failure)
        FAIL(failure);
    PASS();
}

/* Test 5: IDB relation in chain prevents LFTJ rewrite. */
static void
test_idb_not_rewritten(void)
{
    TEST("IDB right_relation in chain is not rewritten as LFTJ");

    /*
     * tc is IDB (derived from edge), so the join chain
     * r1 ⋈ tc ⋈ r2 is NOT eligible for LFTJ.
     */
    const char *src = ".decl edge(x: int32, y: int32)\n"
        ".decl r1(x: int32, a: int32)\n"
        ".decl r2(x: int32, b: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        ".decl out(x: int32)\n"
        "edge(1, 2). edge(2, 3).\n"
        "r1(1, 10). r2(1, 100).\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n"
        "out(x) :- r1(x, a), tc(x, y), r2(x, b).\n";

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    ASSERT(make_plan(src, &plan, &prog) == 0, "make_plan failed");

    uint32_t n = count_lftj_ops(plan);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    /* The out-relation plan contains tc (IDB) -> no LFTJ rewrite for out */
    ASSERT(n == 0, "IDB-containing chain incorrectly rewritten as LFTJ");
    PASS();
}

/* Test 6: TDD classifiers reject unsupported or malformed LFTJ metadata. */
static void
test_tdd_lftj_metadata_is_conservative(void)
{
    TEST("TDD rejects IDB and malformed LFTJ metadata conservatively");

    const char *src = ".decl r1(x: int32, a: int32)\n"
        ".decl r2(x: int32, b: int32)\n"
        ".decl r3(x: int32, c: int32)\n"
        ".decl out(x: int32, a: int32, b: int32, c: int32)\n"
        "out(x, a, b, c) :- r1(x, a), r2(x, b), r3(x, c).\n";
    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    ASSERT(make_plan_no_opt(src, &plan, &prog) == 0,
        "make_plan_no_opt failed");

    const wl_plan_stratum_t *stratum = NULL;
    const wl_plan_op_t *found_op = NULL;
    for (uint32_t si = 0; si < plan->stratum_count && !found_op; si++) {
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ri++) {
            const wl_plan_relation_t *rel = &plan->strata[si].relations[ri];
            for (uint32_t oi = 0; oi < rel->op_count; oi++) {
                if (rel->ops[oi].op == WL_PLAN_OP_LFTJ) {
                    stratum = &plan->strata[si];
                    found_op = &rel->ops[oi];
                    break;
                }
            }
        }
    }
    ASSERT(found_op && stratum, "real EDB-only LFTJ premise missing");
    wl_plan_op_t *lftj_op = (wl_plan_op_t *)(uintptr_t)found_op;

    wl_plan_op_lftj_t *original =
        (wl_plan_op_lftj_t *)lftj_op->opaque_data;
    ASSERT(original && original->k >= 3, "invalid generated LFTJ metadata");
    ASSERT(!tdd_stratum_has_unsupported_lftj(stratum),
        "valid EDB-only LFTJ incorrectly rejected");

    wl_plan_op_lftj_t clone = *original;
    clone.rel_names = calloc(clone.k, sizeof(*clone.rel_names));
    clone.key_cols = calloc(clone.k, sizeof(*clone.key_cols));
    ASSERT(clone.rel_names && clone.key_cols, "LFTJ metadata clone failed");
    for (uint32_t i = 0; i < clone.k; i++) {
        clone.rel_names[i] = original->rel_names[i];
        clone.key_cols[i] = original->key_cols[i];
    }

    lftj_op->opaque_data = &clone;
    clone.rel_names[0] = "out";
    ASSERT(tdd_stratum_has_unsupported_lftj(stratum),
        "IDB-bearing LFTJ was accepted");
    ASSERT(stratum_max_idb_body_atoms(stratum) == 1,
        "IDB operand count was not recorded");
    ASSERT(!tdd_stratum_global_read_candidate(stratum),
        "IDB-bearing LFTJ reached global-read candidate");
    ASSERT(!tdd_stratum_mixed_slice_candidate(stratum),
        "IDB-bearing LFTJ reached mixed-slice candidate");

    clone.rel_names[1] = "out";
    ASSERT(tdd_stratum_has_idb_self_join(stratum),
        "two IDB LFTJ operands were not treated as self-join");

    clone.rel_names[0] = NULL;
    ASSERT(tdd_stratum_has_unsupported_lftj(stratum),
        "NULL LFTJ relation name was accepted");
    clone.rel_names[0] = original->rel_names[0];

    free(clone.key_cols);
    clone.key_cols = NULL;
    ASSERT(tdd_stratum_has_unsupported_lftj(stratum),
        "NULL LFTJ key array was accepted");
    clone.key_cols = calloc(original->k, sizeof(*clone.key_cols));
    ASSERT(clone.key_cols, "LFTJ key array restore failed");
    for (uint32_t i = 0; i < clone.k; i++)
        clone.key_cols[i] = original->key_cols[i];

    clone.rel_names[0] = original->rel_names[0];
    clone.rel_names[1] = original->rel_names[1];
    free(clone.rel_names);
    clone.rel_names = NULL;
    ASSERT(tdd_stratum_has_unsupported_lftj(stratum),
        "missing LFTJ relation array was accepted");

    lftj_op->opaque_data = original;
    free(clone.key_cols);
    free(clone.rel_names);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    const char *recursive_src =
        ".decl edge(x: int32, y: int32)\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n"
        "path(x, z) :- path(x, y), path(y, z).\n";
    prog = NULL;
    plan = NULL;
    ASSERT(make_plan(recursive_src, &plan, &prog) == 0,
        "recursive plan build failed");
    const wl_plan_op_t *fusion_found = NULL;
    const wl_plan_stratum_t *fusion_stratum = NULL;
    for (uint32_t si = 0; si < plan->stratum_count && !fusion_found; si++) {
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ri++) {
            const wl_plan_relation_t *rel = &plan->strata[si].relations[ri];
            for (uint32_t oi = 0; oi < rel->op_count; oi++) {
                if (rel->ops[oi].op == WL_PLAN_OP_K_FUSION) {
                    fusion_found = &rel->ops[oi];
                    fusion_stratum = &plan->strata[si];
                    break;
                }
            }
        }
    }
    ASSERT(fusion_found && fusion_stratum, "real K_FUSION premise missing");
    wl_plan_op_t *fusion_op = (wl_plan_op_t *)(uintptr_t)fusion_found;
    wl_plan_op_k_fusion_t *fusion =
        (wl_plan_op_k_fusion_t *)fusion_op->opaque_data;
    ASSERT(fusion && fusion->k > 0 && fusion->k_ops && fusion->k_op_counts,
        "invalid generated K_FUSION metadata");
    wl_plan_op_k_fusion_t fusion_clone = *fusion;
    fusion_clone.k_ops = calloc(fusion->k, sizeof(*fusion_clone.k_ops));
    fusion_clone.k_op_counts = calloc(fusion->k,
            sizeof(*fusion_clone.k_op_counts));
    ASSERT(fusion_clone.k_ops && fusion_clone.k_op_counts,
        "K_FUSION metadata clone failed");
    for (uint32_t i = 0; i < fusion->k; i++) {
        fusion_clone.k_ops[i] = fusion->k_ops[i];
        fusion_clone.k_op_counts[i] = fusion->k_op_counts[i];
    }
    fusion_op->opaque_data = NULL;
    ASSERT(tdd_stratum_has_unsupported_lftj(fusion_stratum),
        "NULL K_FUSION metadata was accepted");
    fusion_op->opaque_data = &fusion_clone;
    fusion_clone.k = 0;
    ASSERT(tdd_stratum_has_unsupported_lftj(fusion_stratum),
        "zero-length K_FUSION metadata was accepted");
    fusion_clone.k = fusion->k;
    uint32_t bad_child = UINT32_MAX;
    for (uint32_t i = 0; i < fusion->k; i++) {
        if (fusion_clone.k_op_counts[i] > 0) {
            bad_child = i;
            break;
        }
    }
    ASSERT(bad_child != UINT32_MAX, "K_FUSION has no non-empty child");
    fusion_clone.k_ops[bad_child] = NULL;
    fusion_op->opaque_data = &fusion_clone;
    ASSERT(tdd_stratum_has_unsupported_lftj(fusion_stratum),
        "NULL K_FUSION child was accepted");
    ASSERT(tdd_stratum_has_idb_self_join(fusion_stratum),
        "NULL K_FUSION child broke self-join diagnostics");
    fusion_op->opaque_data = fusion;
    free(fusion_clone.k_ops);
    free(fusion_clone.k_op_counts);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== LFTJ Integration Tests (Issue #195) ===\n");

    test_plan_has_lftj();
    test_plan_no_lftj_for_binary();
    test_3way_join_result();
    test_float_join_result();
    test_4way_join_result();
    test_idb_not_rewritten();
    test_lftj_stack_overflow_releases_output();
    test_lftj_inner_denial_propagates_and_retries();
    test_lftj_output_growth_denial_propagates_and_retries();
    test_tdd_lftj_metadata_is_conservative();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n");

    return fail_count > 0 ? 1 : 0;
}
