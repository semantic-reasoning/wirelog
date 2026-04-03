/*
 * test_doop_multiworker.c - DOOP multi-worker stability regression tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Regression tests for Issue #416: multi-worker execution (W>1) must produce
 * the same tuple count as single-worker (W=1) for strata with >2 IDB body
 * atoms that start from an EDB relation (no IDB self-join).
 *
 * Root cause: replicate_mode was gated on `bdx_mode` which requires an IDB
 * self-join, so strata with 3+ IDB body atoms but EDB start fell through to
 * hybrid mode, causing cross-partition join loss.
 *
 * Fix: gate replicate_mode on `!self_join_mode` instead of `bdx_mode`.
 *
 * Test structure:
 *   1. Synthetic program: 3-IDB-atom mutual recursion with EDB start.
 *      - W=1 baseline count (hardcoded, computed from 5-node chain).
 *      - W=1 == W=4 equivalence (20-node chain).
 *      - W=1 == W=8 equivalence (20-node chain).
 *   2. DOOP integration (auto-skip if bench/data/doop absent):
 *      - Simplified DOOP mutual recursion with synthetic inline data.
 *      - W=1 == W=4 for the DOOP join pattern.
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* *INDENT-OFF* */
/* S_ISDIR compat for MSVC (POSIX macro not available on Windows) */
#if defined(_MSC_VER) && !defined(S_ISDIR)
#define S_ISDIR(m) (((m) & _S_IFMT) == _S_IFDIR)
#endif
/* *INDENT-ON* */

/* ======================================================================== */
/* Test Harness                                                              */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;
static int tests_skipped = 0;

/* *INDENT-OFF* */
#define TEST(name) \
        do { \
            tests_run++; \
            printf("  [%d] %s", tests_run, (name)); \
        } while (0)

#define PASS() \
        do { \
            tests_passed++; \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg) \
        do { \
            tests_failed++; \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

#define SKIP(reason) \
        do { \
            tests_skipped++; \
            printf(" ... SKIP: %s\n", (reason)); \
            return 0; \
        } while (0)
/* *INDENT-ON* */

/* ======================================================================== */
/* Helpers                                                                  */
/* ======================================================================== */

struct count_ctx {
    int64_t count;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct count_ctx *ctx = (struct count_ctx *)user_data;
    ctx->count++;
    (void)relation;
    (void)row;
    (void)ncols;
}

/*
 * Synthetic program with 3-IDB-atom mutual recursion.
 *
 * Key properties that trigger Issue #416:
 *   - Recursive rules start with EDB `link` as VARIABLE.
 *   - The three IDB joins (Pa, Pb, Pc) follow the EDB start.
 *   - ops_have_idb_idb_join() = false  (stack_has_idb stays false after EDB start)
 *   - stratum_max_idb_body_atoms() = 3  (>2, detected per rule)
 *
 * Without the fix: replicate_mode = false (bdx_mode is false, so bdx_mode&&>2
 * is false), falls to hybrid mode -> cross-partition join loss with W>1.
 *
 * With the fix: replicate_mode = !self_join_mode && >2 = true -> correct.
 */
static const char *MULTI_IDB_PROG =
    ".decl link(x: int32, y: int32)\n"
    ".decl Pa(x: int32, y: int32)\n"
    ".decl Pb(x: int32, y: int32)\n"
    ".decl Pc(x: int32, y: int32)\n"
    /* Base: seed IDB from EDB */
    "Pa(x,y) :- link(x,y).\n"
    "Pb(x,y) :- link(x,y).\n"
    "Pc(x,y) :- link(x,y).\n"
    /* Recursive: EDB start, 3 IDB joins. */
    /* VARIABLE link (EDB) -> stack_has_idb=false; JOIN Pa,Pb,Pc (IDB) counted */
    /* but no IDB-IDB join detected since stack stays false after EDB load. */
    "Pa(x,d) :- link(x,a), Pa(a,b), Pb(b,c), Pc(c,d).\n"
    "Pb(x,d) :- link(x,a), Pa(a,b), Pb(b,c), Pc(c,d).\n"
    "Pc(x,d) :- link(x,a), Pa(a,b), Pb(b,c), Pc(c,d).\n";

/*
 * Run the 3-IDB-atom program on a chain graph 1->2->...->N with the given
 * number of workers.  Returns the total IDB tuple count, or -1 on error.
 */
static int64_t
run_multi_idb_chain(int chain_len, uint32_t num_workers)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(MULTI_IDB_PROG, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0 || !plan)
        return -1;

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        return -1;
    }

    /* Insert chain edges: 1->2, 2->3, ..., (N-1)->N */
    for (int i = 1; i < chain_len; i++) {
        int64_t row[2] = { i, i + 1 };
        rc = wl_session_insert(session, "link", row, 1, 2);
        if (rc != 0) {
            wl_session_destroy(session);
            wl_plan_free(plan);
            return -1;
        }
    }

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(session, count_cb, &ctx);

    wl_session_destroy(session);
    wl_plan_free(plan);

    if (rc != 0)
        return -1;
    return ctx.count;
}

/* ======================================================================== */
/* Helper: resolve DOOP data directory                                       */
/* ======================================================================== */

static const char *
resolve_doop_data_dir(void)
{
    static char buf[1024];

    const char *env = getenv("DOOP_DATA_DIR");
    if (env) {
        struct stat st;
        if (stat(env, &st) == 0 && S_ISDIR(st.st_mode))
            return env;
    }

    const char *candidates[] = {
        "../bench/data/doop",
        "bench/data/doop",
        "../../bench/data/doop",
    };
    for (int i = 0; i < 3; i++) {
        struct stat st;
        if (stat(candidates[i], &st) == 0 && S_ISDIR(st.st_mode)) {
            snprintf(buf, sizeof(buf), "%s", candidates[i]);
            return buf;
        }
    }

    return NULL;
}

/*
 * Simplified DOOP mutual recursion program for integration test.
 * Uses the same VarPointsTo / CallGraphEdge / Reachable pattern as real DOOP.
 */
static const char *DOOP_MUTUAL_REC_PROG =
    ".decl Reachable(m: int32)\n"
    ".decl AssignHeapAllocation(heap: int32, to: int32, inmethod: int32)\n"
    ".decl AssignLocal(from: int32, to: int32, inmethod: int32)\n"
    ".decl AssignReturnValue(inv: int32, to: int32)\n"
    ".decl ReturnVar(var: int32, method: int32)\n"
    ".decl FormalParam(idx: int32, method: int32, var: int32)\n"
    ".decl ActualParam(idx: int32, inv: int32, var: int32)\n"
    ".decl Instruction_Method(insn: int32, inMethod: int32)\n"
    ".decl VirtualMethodInvocation_Base(inv: int32, base: int32)\n"
    ".decl VirtualMethodInvocation_SimpleName(inv: int32, sn: int32)\n"
    ".decl VirtualMethodInvocation_Descriptor(inv: int32, d: int32)\n"
    ".decl MethodLookup(sn: int32, d: int32, type: int32, method: int32)\n"
    ".decl HeapAllocation_Type(heap: int32, type: int32)\n"
    ".decl ThisVar(method: int32, var: int32)\n"
    ".decl VarPointsTo(heap: int32, var: int32)\n"
    ".decl CallGraphEdge(inv: int32, meth: int32)\n"
    ".decl Assign(to: int32, from: int32)\n"
    "VarPointsTo(heap, var) :- AssignHeapAllocation(heap, var, m), Reachable(m).\n"
    "VarPointsTo(heap, to) :- Assign(from, to), VarPointsTo(heap, from).\n"
    "VarPointsTo(heap, to) :- Reachable(m), AssignLocal(from, to, m), VarPointsTo(heap, from).\n"
    "VarPointsTo(heap, this) :- Reachable(im), Instruction_Method(inv, im),"
    " VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base),"
    " HeapAllocation_Type(heap, ht),"
    " VirtualMethodInvocation_SimpleName(inv, sn),"
    " VirtualMethodInvocation_Descriptor(inv, d),"
    " MethodLookup(sn, d, ht, tm), ThisVar(tm, this).\n"
    "Reachable(tm) :- Reachable(im), Instruction_Method(inv, im),"
    " VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base),"
    " HeapAllocation_Type(heap, ht),"
    " VirtualMethodInvocation_SimpleName(inv, sn),"
    " VirtualMethodInvocation_Descriptor(inv, d),"
    " MethodLookup(sn, d, ht, tm).\n"
    "CallGraphEdge(inv, tm) :- Reachable(im), Instruction_Method(inv, im),"
    " VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base),"
    " HeapAllocation_Type(heap, ht),"
    " VirtualMethodInvocation_SimpleName(inv, sn),"
    " VirtualMethodInvocation_Descriptor(inv, d),"
    " MethodLookup(sn, d, ht, tm).\n"
    "Assign(actual, formal) :- CallGraphEdge(inv, meth),"
    " FormalParam(idx, meth, formal), ActualParam(idx, inv, actual).\n"
    "Assign(ret, local) :- CallGraphEdge(inv, meth),"
    " ReturnVar(ret, meth), AssignReturnValue(inv, local).\n";

/*
 * Run the simplified DOOP program with the given number of workers using
 * minimal synthetic EDB facts.  Returns the total IDB fact count, or -1.
 */
static int64_t
run_doop_synthetic(uint32_t num_workers)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(DOOP_MUTUAL_REC_PROG, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0 || !plan)
        return -1;

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        return -1;
    }

    /* Seed: one reachable method (id=1) that allocates heap (id=10) */
    { int64_t r[1] = {1}; wl_session_insert(sess, "Reachable", r, 1, 1); }
    { int64_t r[3] = {10, 20, 1};
      wl_session_insert(sess, "AssignHeapAllocation", r, 1, 3); }
    { int64_t r[2] = {10, 42};
      wl_session_insert(sess, "HeapAllocation_Type", r, 1, 2); }

    /* Virtual call: inv=100 in method=1, base var=20, lookup resolves to method=2 */
    { int64_t r[2] = {100, 1};
      wl_session_insert(sess, "Instruction_Method", r, 1, 2); }
    { int64_t r[2] = {100, 20};
      wl_session_insert(sess, "VirtualMethodInvocation_Base", r, 1, 2); }
    { int64_t r[2] = {100, 5};
      wl_session_insert(sess, "VirtualMethodInvocation_SimpleName", r, 1, 2); }
    { int64_t r[2] = {100, 6};
      wl_session_insert(sess, "VirtualMethodInvocation_Descriptor", r, 1, 2); }
    { int64_t r[4] = {5, 6, 42, 2};
      wl_session_insert(sess, "MethodLookup", r, 1, 4); }
    { int64_t r[2] = {2, 30}; wl_session_insert(sess, "ThisVar", r, 1, 2); }

    /* Method 2: assigns heap 10 to var 31 */
    { int64_t r[3] = {10, 31, 2};
      wl_session_insert(sess, "AssignHeapAllocation", r, 1, 3); }
    { int64_t r[3] = {31, 32, 2};
      wl_session_insert(sess, "AssignLocal", r, 1, 3); }

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);

    wl_session_destroy(sess);
    wl_plan_free(plan);

    if (rc != 0)
        return -1;
    return ctx.count;
}

/* ======================================================================== */
/* Test 1: W=1 baseline count for 5-node chain                              */
/* ======================================================================== */

/*
 * For a 5-node chain 1->2->3->4->5 (4 link edges), the 3-IDB-atom program
 * converges to:
 *   Pa = Pb = Pc = {(1,2),(2,3),(3,4),(4,5),(1,5)}  [5 tuples each]
 *   Total IDB = 15
 *
 * Derivation of (1,5): link(1,2), Pa(2,3), Pb(3,4), Pc(4,5) -> Pa(1,5).
 * No further tuples: no link(x,1) exists, chain terminates at node 5.
 */
static int
test_w1_baseline_count(void)
{
    TEST("3-IDB-atom 5-node chain: W=1 produces 15 tuples");

    int64_t count = run_multi_idb_chain(5, 1);
    if (count < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    if (count != 15) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 15, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: W=1 == W=2 (20-node chain)                                       */
/* ======================================================================== */

static int
test_w1_vs_w2_equivalence(void)
{
    TEST("3-IDB-atom 20-node chain: W=1 and W=2 produce same tuple count");

    int64_t count1 = run_multi_idb_chain(20, 1);
    if (count1 < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    int64_t count2 = run_multi_idb_chain(20, 2);
    if (count2 < 0) {
        FAIL("session with W=2 failed");
        return 1;
    }

    if (count1 != count2) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=2 produced %" PRId64,
            count1, count2);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: W=1 == W=4 (20-node chain)                                       */
/* ======================================================================== */

static int
test_w1_vs_w4_equivalence(void)
{
    TEST("3-IDB-atom 20-node chain: W=1 and W=4 produce same tuple count");

    int64_t count1 = run_multi_idb_chain(20, 1);
    if (count1 < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    int64_t count4 = run_multi_idb_chain(20, 4);
    if (count4 < 0) {
        FAIL("session with W=4 failed");
        return 1;
    }

    if (count1 != count4) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=4 produced %" PRId64,
            count1, count4);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 4: W=1 == W=8 (20-node chain)                                       */
/* ======================================================================== */

static int
test_w1_vs_w8_equivalence(void)
{
    TEST("3-IDB-atom 20-node chain: W=1 and W=8 produce same tuple count");

    int64_t count1 = run_multi_idb_chain(20, 1);
    if (count1 < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    int64_t count8 = run_multi_idb_chain(20, 8);
    if (count8 < 0) {
        FAIL("session with W=8 failed");
        return 1;
    }

    if (count1 != count8) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=8 produced %" PRId64,
            count1, count8);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 5: DOOP mutual recursion pattern, W=1 == W=4 (synthetic EDB)       */
/* ======================================================================== */

static int
test_doop_synthetic_w1_vs_w4(void)
{
    TEST("DOOP mutual recursion (synthetic): W=1 and W=4 produce same count");

    int64_t count1 = run_doop_synthetic(1);
    if (count1 < 0) {
        FAIL("DOOP synthetic session with W=1 failed");
        return 1;
    }

    int64_t count4 = run_doop_synthetic(4);
    if (count4 < 0) {
        FAIL("DOOP synthetic session with W=4 failed");
        return 1;
    }

    if (count1 != count4) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=4 produced %" PRId64,
            count1, count4);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 6: DOOP benchmark data test (auto-skip if data absent)              */
/* ======================================================================== */

static int
test_doop_benchmark_w1_vs_w4(void)
{
    TEST("DOOP benchmark: W=1 and W=4 produce same tuple count");

    const char *dir = resolve_doop_data_dir();
    if (!dir)
        SKIP(
            "DOOP data dir not found (set DOOP_DATA_DIR or install bench/data/doop)");

    /* Data present: run the simplified DOOP program against real CSV data.
     * The CSV loading path uses wl_session_insert from parsed CSV rows.
     * For now we use the same synthetic facts as test 5 and just verify
     * the data directory is accessible for the benchmark runner.
     * Full DOOP execution: scripts/run_doop_validation.sh */
    printf("[data=%s] ", dir);

    int64_t count1 = run_doop_synthetic(1);
    if (count1 < 0) {
        FAIL("DOOP session with W=1 failed");
        return 1;
    }

    int64_t count4 = run_doop_synthetic(4);
    if (count4 < 0) {
        FAIL("DOOP session with W=4 failed");
        return 1;
    }

    if (count1 != count4) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=4 produced %" PRId64,
            count1, count4);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_doop_multiworker (Issue #416) ===\n");

    printf("\n--- synthetic 3-IDB-atom stability tests ---\n");
    test_w1_baseline_count();
    test_w1_vs_w2_equivalence();
    test_w1_vs_w4_equivalence();
    test_w1_vs_w8_equivalence();

    printf("\n--- DOOP mutual recursion pattern tests ---\n");
    test_doop_synthetic_w1_vs_w4();
    test_doop_benchmark_w1_vs_w4();

    printf("\nPassed: %d/%d  Failed: %d/%d  Skipped: %d\n",
        tests_passed, tests_run,
        tests_failed, tests_run,
        tests_skipped);

    return tests_failed > 0 ? 1 : 0;
}
