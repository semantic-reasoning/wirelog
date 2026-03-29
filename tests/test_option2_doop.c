/*
 * test_option2_doop.c - DOOP Validation Test Harness
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * ========================================================================
 * PURPOSE
 * ========================================================================
 *
 * Validates DOOP (Java points-to analysis) infrastructure for Option 2.
 * This file covers US-009 preparation tasks:
 *
 *   1. Verify DOOP data directory has all 34 expected CSV files.
 *   2. Verify DOOP declarations parse without error (syntax check).
 *   3. Verify 8-way virtual-dispatch join produces correct results
 *      using synthetic inline data (avoids loading 83MB dataset).
 *
 * Full DOOP benchmark execution (US-010) uses the shell script:
 *   scripts/run_doop_validation.sh
 *
 * ========================================================================
 * DATA DIRECTORY
 * ========================================================================
 *
 * The DOOP zxing CSV dataset lives at bench/data/doop/ relative to the
 * project root.  When running under meson test the CWD is the build
 * directory, so we try "../bench/data/doop" first and fall back to the
 * DOOP_DATA_DIR environment variable.
 *
 * If neither path resolves, data-dependent tests are skipped (exit 0).
 *
 * ========================================================================
 * EXPECTED STATE
 * ========================================================================
 *
 * Infrastructure tests (file checks, parse) PASS immediately.
 * 8-way join test PASSES when evaluator correctly handles multi-way joins.
 * Full benchmark is NOT run here — see scripts/run_doop_validation.sh.
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

/* MSVC: S_ISDIR macro not available, define it */
#ifdef _MSC_VER
#ifndef S_ISDIR
#define S_ISDIR(m) (((m)&_S_IFMT) == _S_IFDIR)
#endif
#endif

/* ========================================================================
 * Test Framework
 * ======================================================================== */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;
static int skip_count = 0;

/* *INDENT-OFF* */
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
    } while (0)

#define SKIP(reason)                    \
    do {                                \
        skip_count++;                   \
        printf("SKIP: %s\n", (reason)); \
        return;                         \
    } while (0)

#define ASSERT(cond, msg) \
    do {                  \
        if (!(cond)) {    \
            FAIL(msg);    \
            return;       \
        }                 \
    } while (0)
/* *INDENT-ON* */

/* ========================================================================
 * DOOP EDB catalogue: the 34 CSV files expected in the data directory.
 * ======================================================================== */

static const char *doop_csv_files[] = {
    "ActualParam.csv",
    "ApplicationClass.csv",
    "ArrayType.csv",
    "AssignCast.csv",
    "AssignHeapAllocation.csv",
    "AssignLocal.csv",
    "AssignReturnValue.csv",
    "ClassType.csv",
    "ComponentType.csv",
    "DirectSuperclass.csv",
    "DirectSuperinterface.csv",
    "Field.csv",
    "FormalParam.csv",
    "HeapAllocation_Type.csv",
    "InterfaceType.csv",
    "LoadArrayIndex.csv",
    "LoadInstanceField.csv",
    "LoadStaticField.csv",
    "MainClass.csv",
    "Method.csv",
    "Method_Descriptor.csv",
    "Method_Modifier.csv",
    "NormalHeap.csv",
    "Return.csv",
    "SpecialMethodInvocation.csv",
    "StaticMethodInvocation.csv",
    "StoreArrayIndex.csv",
    "StoreInstanceField.csv",
    "StoreStaticField.csv",
    "StringConstant.csv",
    "ThisVar.csv",
    "Var_DeclaringMethod.csv",
    "Var_Type.csv",
    "VirtualMethodInvocation.csv",
};

#define DOOP_NCSV ((int)(sizeof(doop_csv_files) / sizeof(doop_csv_files[0])))

/* ========================================================================
 * Helper: resolve DOOP data directory
 *
 * Returns a pointer to a static buffer with the resolved path, or NULL
 * if no valid directory is found.
 * ======================================================================== */

static const char *
resolve_doop_data_dir(void)
{
    static char buf[1024];

    /* 1. Environment variable override */
    const char *env = getenv("DOOP_DATA_DIR");
    if (env) {
        struct stat st;
        if (stat(env, &st) == 0 && S_ISDIR(st.st_mode))
            return env;
    }

    /* 2. Relative from build directory (meson test CWD = build/) */
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

/* ========================================================================
 * Helper: count total and by relation name (single snapshot)
 * ======================================================================== */

struct combined_count_ctx {
    const char *target;
    int64_t total;
    int64_t rel_count;
};

static void
combined_count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct combined_count_ctx *ctx = (struct combined_count_ctx *)user_data;
    ctx->total++;
    if (ctx->target && strcmp(relation, ctx->target) == 0)
        ctx->rel_count++;
    (void)row;
    (void)ncols;
}

/* ========================================================================
 * Helper: parse + optimise + plan + run, return total IDB fact count.
 * Also returns count for a specific named relation.
 * Returns -1 on any error.
 * ======================================================================== */

static int64_t
run_program_count_rel(const char *src, uint32_t num_workers,
    const char *rel_name, int64_t *rel_count_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    /* JPP and SIP disabled: insert_projections in JPP incorrectly prunes
     * intermediate columns needed by K-fusion delta copies in recursive
     * strata with 6+ body atoms.  See issue #382. */

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct combined_count_ctx ctx = { rel_name, 0, 0 };

    rc = wl_session_snapshot(sess, combined_count_cb, &ctx);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    if (rc != 0)
        return -1;
    if (rel_count_out)
        *rel_count_out = ctx.rel_count;
    return ctx.total;
}

/* ========================================================================
 * INFRASTRUCTURE TESTS: Data files
 * ======================================================================== */

/*
 * Test 1: DOOP data directory is accessible.
 *
 * Resolves the DOOP CSV directory via env var or relative path.
 * SKIP if not found (no DOOP data installed — CI without large assets).
 */
static void
test_doop_data_dir_accessible(void)
{
    TEST("doop_infra: data directory is accessible");

    const char *dir = resolve_doop_data_dir();
    if (!dir)
        SKIP("DOOP data dir not found (set DOOP_DATA_DIR or install "
            "bench/data/doop)");

    printf("[dir=%s] ", dir);
    PASS();
}

/*
 * Test 2: All 34 DOOP CSV files exist and are non-empty.
 *
 * Spot-checks 5 representative files from the 34-file EDB.
 * SKIP if data dir not found.
 */
static void
test_doop_all_csv_files_exist(void)
{
    TEST("doop_infra: all 34 CSV files exist and are non-empty");

    const char *dir = resolve_doop_data_dir();
    if (!dir)
        SKIP("DOOP data dir not found");

    char path[1024];
    int missing = 0;
    int empty = 0;

    for (int i = 0; i < DOOP_NCSV; i++) {
        snprintf(path, sizeof(path), "%s/%s", dir, doop_csv_files[i]);
        struct stat st;
        if (stat(path, &st) != 0) {
            fprintf(stderr, "\n  missing: %s", doop_csv_files[i]);
            missing++;
        } else if (st.st_size == 0) {
            fprintf(stderr, "\n  empty: %s", doop_csv_files[i]);
            empty++;
        }
    }

    char msg[128];
    if (missing > 0) {
        snprintf(msg, sizeof(msg), "%d of %d CSV files missing", missing,
            DOOP_NCSV);
        FAIL(msg);
        return;
    }
    if (empty > 0) {
        snprintf(msg, sizeof(msg), "%d CSV files are empty", empty);
        FAIL(msg);
        return;
    }

    printf("[%d files OK] ", DOOP_NCSV);
    PASS();
}

/*
 * Test 3: Total dataset size is in the expected range (70–100 MB).
 *
 * The zxing DOOP dataset is documented as ~83MB.  A significant
 * deviation indicates a corrupted or wrong dataset.
 */
static void
test_doop_dataset_size_in_range(void)
{
    TEST("doop_infra: total dataset size is 70–100 MB");

    const char *dir = resolve_doop_data_dir();
    if (!dir)
        SKIP("DOOP data dir not found");

    char path[1024];
    off_t total = 0;

    for (int i = 0; i < DOOP_NCSV; i++) {
        snprintf(path, sizeof(path), "%s/%s", dir, doop_csv_files[i]);
        struct stat st;
        if (stat(path, &st) == 0)
            total += st.st_size;
    }

    long long mb = (long long)total / (1024LL * 1024LL);
    printf("[%lld MB] ", mb);

    char msg[128];
    snprintf(msg, sizeof(msg), "expected 70–100 MB, got %lld MB", mb);
    ASSERT(mb >= 70 && mb <= 100, msg);

    PASS();
}

/* ========================================================================
 * INFRASTRUCTURE TESTS: Parser
 * ======================================================================== */

/*
 * Test 4: DOOP EDB declarations parse without error.
 *
 * Parses only the EDB relation declarations (no .input directives, no rules)
 * to verify the Datalog schema is syntactically valid.
 */
static void
test_doop_edb_declarations_parse(void)
{
    TEST("doop_parse: EDB declarations parse without error");

    /* Minimal EDB schema — the 10 direct-input relations from doop.dl */
    const char *src
        = ".decl DirectSuperclass(class: int32, superclass: int32)\n"
        ".decl DirectSuperinterface(ref: int32, interface: int32)\n"
        ".decl MainClass(class: int32)\n"
        ".decl FormalParam(index: int32, method: int32, var: int32)\n"
        ".decl ComponentType(arrayType: int32, componentType: int32)\n"
        ".decl AssignReturnValue(invocation: int32, to: int32)\n"
        ".decl ActualParam(index: int32, invocation: int32, var: int32)\n"
        ".decl Method_Modifier(mod: int32, method: int32)\n"
        ".decl Var_Type(var: int32, type: int32)\n"
        ".decl HeapAllocation_Type(heap: int32, type: int32)\n"
        /* staging inputs */
        ".decl _ClassType(class: int32)\n"
        ".decl _ArrayType(arrayType: int32)\n"
        ".decl _InterfaceType(interface: int32)\n"
        ".decl _Var_DeclaringMethod(var: int32, method: int32)\n"
        ".decl _ApplicationClass(type: int32)\n"
        ".decl _ThisVar(method: int32, var: int32)\n"
        ".decl _NormalHeap(id: int32, type: int32)\n"
        ".decl _StringConstant(id: int32)\n"
        ".decl _AssignHeapAllocation(instruction: int32, idx: int32, "
        "heap: int32, to: int32, inmethod: int32, linenumber: int32)\n"
        ".decl _AssignLocal(instruction: int32, idx: int32, from: int32, "
        "to: int32, inmethod: int32)\n"
        ".decl _AssignCast(instruction: int32, idx: int32, from: int32, "
        "to: int32, type: int32, inmethod: int32)\n"
        ".decl _Field(signature: int32, declaringClass: int32, "
        "simplename: int32, type: int32)\n"
        ".decl _StaticMethodInvocation(instruction: int32, idx: int32, "
        "signature: int32, method: int32)\n"
        ".decl _SpecialMethodInvocation(instruction: int32, idx: int32, "
        "signature: int32, base: int32, method: int32)\n"
        ".decl _VirtualMethodInvocation(instruction: int32, idx: int32, "
        "signature: int32, base: int32, method: int32)\n"
        ".decl _Method(method: int32, simplename: int32, params: int32, "
        "declaringType: int32, returnType: int32, jvmDescriptor: int32, "
        "arity: int32)\n"
        ".decl Method_Descriptor(method: int32, descriptor: int32)\n"
        ".decl _StoreInstanceField(instruction: int32, idx: int32, "
        "from: int32, base: int32, signature: int32, method: int32)\n"
        ".decl _LoadInstanceField(instruction: int32, idx: int32, "
        "to: int32, base: int32, signature: int32, method: int32)\n"
        ".decl _StoreStaticField(instruction: int32, idx: int32, "
        "from: int32, signature: int32, method: int32)\n"
        ".decl _LoadStaticField(instruction: int32, idx: int32, "
        "to: int32, signature: int32, method: int32)\n"
        ".decl _StoreArrayIndex(instruction: int32, idx: int32, "
        "from: int32, base: int32, method: int32)\n"
        ".decl _LoadArrayIndex(instruction: int32, idx: int32, "
        "to: int32, base: int32, method: int32)\n"
        ".decl _Return(instruction: int32, idx: int32, "
        "var: int32, method: int32)\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "EDB declarations failed to parse");
    wirelog_program_free(prog);
    PASS();
}

/*
 * Test 5: Key IDB declarations with recursive rules parse without error.
 *
 * Tests that the parser accepts the mutual recursion pattern that DOOP
 * uses: VarPointsTo <-> CallGraphEdge <-> Reachable.
 */
static void
test_doop_mutual_recursion_parses(void)
{
    TEST("doop_parse: mutual recursion (VarPointsTo/CallGraphEdge/Reachable) "
        "parses");

    const char *src =
        /* minimal EDB stubs */
        ".decl Reachable(m: int32)\n"
        ".decl HeapAllocation_Type(heap: int32, type: int32)\n"
        ".decl Instruction_Method(insn: int32, inMethod: int32)\n"
        ".decl VirtualMethodInvocation_Base(inv: int32, base: int32)\n"
        ".decl VirtualMethodInvocation_SimpleName(inv: int32, sn: int32)\n"
        ".decl VirtualMethodInvocation_Descriptor(inv: int32, d: int32)\n"
        ".decl MethodLookup(sn: int32, d: int32, type: int32, method: int32)\n"
        ".decl AssignHeapAllocation(heap: int32, to: int32, inmethod: int32)\n"
        ".decl AssignLocal(from: int32, to: int32, inmethod: int32)\n"
        ".decl ThisVar(method: int32, var: int32)\n"
        ".decl FormalParam(idx: int32, method: int32, var: int32)\n"
        ".decl ActualParam(idx: int32, inv: int32, var: int32)\n"
        ".decl AssignReturnValue(inv: int32, to: int32)\n"
        ".decl ReturnVar(var: int32, method: int32)\n"
        ".decl Assign(to: int32, from: int32)\n"
        /* IDB */
        ".decl VarPointsTo(heap: int32, var: int32)\n"
        ".decl CallGraphEdge(inv: int32, meth: int32)\n"
        /* mutual recursion rules */
        "VarPointsTo(heap, var) :- AssignHeapAllocation(heap, var, m), "
        "Reachable(m).\n"
        "VarPointsTo(heap, to) :- Assign(from, to), VarPointsTo(heap, from).\n"
        "VarPointsTo(heap, to) :- Reachable(m), AssignLocal(from, to, m), "
        "VarPointsTo(heap, from).\n"
        "VarPointsTo(heap, this) :- Reachable(im), "
        "Instruction_Method(inv, im), "
        "VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base), "
        "HeapAllocation_Type(heap, ht), "
        "VirtualMethodInvocation_SimpleName(inv, sn), "
        "VirtualMethodInvocation_Descriptor(inv, d), "
        "MethodLookup(sn, d, ht, tm), ThisVar(tm, this).\n"
        "Reachable(tm) :- Reachable(im), Instruction_Method(inv, im), "
        "VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base), "
        "HeapAllocation_Type(heap, ht), "
        "VirtualMethodInvocation_SimpleName(inv, sn), "
        "VirtualMethodInvocation_Descriptor(inv, d), "
        "MethodLookup(sn, d, ht, tm).\n"
        "CallGraphEdge(inv, tm) :- Reachable(im), "
        "Instruction_Method(inv, im), "
        "VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base), "
        "HeapAllocation_Type(heap, ht), "
        "VirtualMethodInvocation_SimpleName(inv, sn), "
        "VirtualMethodInvocation_Descriptor(inv, d), "
        "MethodLookup(sn, d, ht, tm).\n"
        "Assign(actual, formal) :- CallGraphEdge(inv, meth), "
        "FormalParam(idx, meth, formal), ActualParam(idx, inv, actual).\n"
        "Assign(ret, local) :- CallGraphEdge(inv, meth), "
        "ReturnVar(ret, meth), AssignReturnValue(inv, local).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "mutual recursion rules failed to parse");
    wirelog_program_free(prog);
    PASS();
}

/* ========================================================================
 * INTEGRATION TESTS: Synthetic join correctness
 * ======================================================================== */

/*
 * Test 6: 8-way virtual dispatch join produces CallGraphEdge.
 *
 * Minimal DOOP-style program with synthetic inline data.  The 8-way join:
 *
 *   CallGraphEdge(inv, toMethod) :-
 *     Reachable(inMethod),                           [1]
 *     Instruction_Method(inv, inMethod),             [2]
 *     VirtualMethodInvocation_Base(inv, base),       [3]
 *     VarPointsTo(heap, base),                       [4]
 *     HeapAllocation_Type(heap, heaptype),           [5]
 *     VirtualMethodInvocation_SimpleName(inv, sn),   [6]
 *     VirtualMethodInvocation_Descriptor(inv, d),    [7]
 *     MethodLookup(sn, d, heaptype, toMethod).       [8]
 *
 * Facts (all using simple integers):
 *   Reachable(0)               — main method id=0
 *   Instruction_Method(200, 0) — call site 200 is in main=0
 *   VirtualMethodInvocation_Base(200, 500)     — base var=500
 *   AssignHeapAllocation(300, 500, 0)          — alloc heap=300 into var=500 in main
 *   HeapAllocation_Type(300, 400)              — heap 300 has type TypeA=400
 *   VirtualMethodInvocation_SimpleName(200, 11)
 *   VirtualMethodInvocation_Descriptor(200, 22)
 *   Method_SimpleName(1, 11)   — method 1 has simplename 11
 *   Method_Descriptor(1, 22)   — method 1 has descriptor 22
 *   Method_DeclaringType(1, 400) — method 1 declared in TypeA=400
 *   (no Method_Modifier for 1928492 -> method 1 is concrete)
 *   ThisVar(1, 600)            — method 1's this-var is 600
 *
 * Expected derivations:
 *   VarPointsTo(300, 500)      — from AssignHeapAllocation + Reachable(0)
 *   MethodImplemented(11,22,400,1) — method 1 is concrete in TypeA
 *   MethodLookup(11,22,400,1)  — lookup resolves
 *   CallGraphEdge(200, 1)      — the 8-way join fires
 *   Reachable(1)               — method 1 becomes reachable
 *   VarPointsTo(300, 600)      — this-var binding for method 1
 *
 * Correctness oracle: CallGraphEdge has exactly 1 tuple.
 *
 * Expected current state: PASSES if evaluator handles 8-way joins correctly.
 */
static void
test_doop_8way_virtual_dispatch_join(void)
{
    TEST("doop_8way: virtual dispatch 8-way join produces CallGraphEdge");

    const char *src =
        /* EDB: entry point */
        ".decl Reachable(m: int32)\n"
        "Reachable(0).\n"

        /* EDB: type info */
        ".decl HeapAllocation_Type(heap: int32, type: int32)\n"
        "HeapAllocation_Type(300, 400).\n"

        /* EDB: narrow schema — instruction membership */
        ".decl Instruction_Method(insn: int32, inMethod: int32)\n"
        "Instruction_Method(200, 0).\n"
        "Instruction_Method(100, 0).\n"

        /* EDB: virtual call site info */
        ".decl VirtualMethodInvocation_Base(inv: int32, base: int32)\n"
        "VirtualMethodInvocation_Base(200, 500).\n"
        ".decl VirtualMethodInvocation_SimpleName(inv: int32, sn: int32)\n"
        "VirtualMethodInvocation_SimpleName(200, 11).\n"
        ".decl VirtualMethodInvocation_Descriptor(inv: int32, d: int32)\n"
        "VirtualMethodInvocation_Descriptor(200, 22).\n"

        /* EDB: method metadata */
        ".decl Method_SimpleName(method: int32, sn: int32)\n"
        "Method_SimpleName(1, 11).\n"
        ".decl Method_Descriptor(method: int32, d: int32)\n"
        "Method_Descriptor(1, 22).\n"
        ".decl Method_DeclaringType(method: int32, type: int32)\n"
        "Method_DeclaringType(1, 400).\n"
        /* No Method_Modifier(1928492, 1) => method 1 is concrete */
        ".decl Method_Modifier(mod: int32, method: int32)\n"

        /* EDB: heap allocation seeds VarPointsTo */
        ".decl AssignHeapAllocation(heap: int32, to: int32, inmethod: int32)\n"
        "AssignHeapAllocation(300, 500, 0).\n"

        /* EDB: this-variable for callee */
        ".decl ThisVar(method: int32, var: int32)\n"
        "ThisVar(1, 600).\n"

        /* IDB: MethodImplemented (concrete methods only) */
        ".decl MethodImplemented(sn: int32, d: int32, type: int32, method: "
        "int32)\n"
        "MethodImplemented(sn, d, type, method) :- "
        "Method_SimpleName(method, sn), Method_Descriptor(method, d), "
        "Method_DeclaringType(method, type), !Method_Modifier(1928492, "
        "method).\n"

        /* IDB: MethodLookup (base case: implemented methods) */
        ".decl MethodLookup(sn: int32, d: int32, type: int32, method: int32)\n"
        "MethodLookup(sn, d, type, method) :- "
        "MethodImplemented(sn, d, type, method).\n"

        /* IDB: VarPointsTo — heap allocation */
        ".decl VarPointsTo(heap: int32, var: int32)\n"
        "VarPointsTo(heap, var) :- "
        "AssignHeapAllocation(heap, var, inMethod), Reachable(inMethod).\n"

        /* IDB: CallGraphEdge — 8-way virtual dispatch join */
        ".decl CallGraphEdge(inv: int32, method: int32)\n"
        "CallGraphEdge(inv, toMethod) :- "
        "Reachable(inMethod), Instruction_Method(inv, inMethod), "
        "VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base), "
        "HeapAllocation_Type(heap, heaptype), "
        "VirtualMethodInvocation_SimpleName(inv, sn), "
        "VirtualMethodInvocation_Descriptor(inv, d), "
        "MethodLookup(sn, d, heaptype, toMethod).\n"

        /* IDB: Reachable propagation via virtual dispatch — 8-way join */
        "Reachable(toMethod) :- "
        "Reachable(inMethod), Instruction_Method(inv, inMethod), "
        "VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base), "
        "HeapAllocation_Type(heap, heaptype), "
        "VirtualMethodInvocation_SimpleName(inv, sn), "
        "VirtualMethodInvocation_Descriptor(inv, d), "
        "MethodLookup(sn, d, heaptype, toMethod).\n"

        /* IDB: VarPointsTo — this-variable binding via virtual dispatch */
        "VarPointsTo(heap, this) :- "
        "Reachable(inMethod), Instruction_Method(inv, inMethod), "
        "VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base), "
        "HeapAllocation_Type(heap, heaptype), "
        "VirtualMethodInvocation_SimpleName(inv, sn), "
        "VirtualMethodInvocation_Descriptor(inv, d), "
        "MethodLookup(sn, d, heaptype, toMethod), ThisVar(toMethod, this).\n";

    int64_t cge_count = 0;
    int64_t total = run_program_count_rel(src, 1, "CallGraphEdge", &cge_count);

    ASSERT(total >= 0, "program evaluation failed");

    /*
     * Correctness oracle: CallGraphEdge(200, 1) must be derived.
     * The 8-way join must fire exactly once for this dataset.
     */
    char msg[128];
    snprintf(msg, sizeof(msg),
        "expected CallGraphEdge count == 1, got %" PRId64, cge_count);
    ASSERT(cge_count == 1, msg);

    PASS();
}

/*
 * Test 7: Reachability propagates through virtual dispatch chain.
 *
 * Extends test 6 with a second virtual call site in method 1
 * targeting method 2.  Verifies transitive reachability:
 *   main(0) -> method1(1) -> method2(2).
 *
 * This tests that the evaluator correctly re-fires the 8-way join
 * when Reachable gains new facts (fixpoint behavior).
 */
static void
test_doop_reachable_propagates_through_virtual_chain(void)
{
    TEST("doop_8way: Reachable propagates through 2-hop virtual dispatch");

    const char *src =
        /* Entry point */
        ".decl Reachable(m: int32)\n"
        "Reachable(0).\n"

        /* Type info for 2 heap objects */
        ".decl HeapAllocation_Type(heap: int32, type: int32)\n"
        "HeapAllocation_Type(300, 400).\n" /* heap h1=300: TypeA=400 */
        "HeapAllocation_Type(310, 410).\n" /* heap h2=310: TypeB=410 */

        /* Instruction_Method */
        ".decl Instruction_Method(insn: int32, inMethod: int32)\n"
        "Instruction_Method(200, 0).\n" /* call1 in main */
        "Instruction_Method(100, 0).\n" /* alloc1 in main */
        "Instruction_Method(201, 1).\n" /* call2 in method1 */
        "Instruction_Method(101, 1).\n" /* alloc2 in method1 */

        /* Virtual call sites */
        ".decl VirtualMethodInvocation_Base(inv: int32, base: int32)\n"
        "VirtualMethodInvocation_Base(200, 500).\n" /* call1 base=v1 */
        "VirtualMethodInvocation_Base(201, 510).\n" /* call2 base=v2 */
        ".decl VirtualMethodInvocation_SimpleName(inv: int32, sn: int32)\n"
        "VirtualMethodInvocation_SimpleName(200, 11).\n"
        "VirtualMethodInvocation_SimpleName(201, 12).\n"
        ".decl VirtualMethodInvocation_Descriptor(inv: int32, d: int32)\n"
        "VirtualMethodInvocation_Descriptor(200, 22).\n"
        "VirtualMethodInvocation_Descriptor(201, 23).\n"

        /* Method metadata */
        ".decl Method_SimpleName(method: int32, sn: int32)\n"
        "Method_SimpleName(1, 11).\n"
        "Method_SimpleName(2, 12).\n"
        ".decl Method_Descriptor(method: int32, d: int32)\n"
        "Method_Descriptor(1, 22).\n"
        "Method_Descriptor(2, 23).\n"
        ".decl Method_DeclaringType(method: int32, type: int32)\n"
        "Method_DeclaringType(1, 400).\n" /* method1 in TypeA */
        "Method_DeclaringType(2, 410).\n" /* method2 in TypeB */
        ".decl Method_Modifier(mod: int32, method: int32)\n"
        /* no abstract modifiers */

        /* Heap allocations */
        ".decl AssignHeapAllocation(heap: int32, to: int32, inmethod: int32)\n"
        "AssignHeapAllocation(300, 500, 0).\n" /* main allocs h1 into v1 */
        "AssignHeapAllocation(310, 510, 1).\n" /* method1 allocs h2 into v2 */

        /* This-vars */
        ".decl ThisVar(method: int32, var: int32)\n"
        "ThisVar(1, 600).\n"
        "ThisVar(2, 610).\n"

        /* IDB */
        ".decl MethodImplemented(sn: int32, d: int32, type: int32, method: "
        "int32)\n"
        "MethodImplemented(sn, d, type, method) :- "
        "Method_SimpleName(method, sn), Method_Descriptor(method, d), "
        "Method_DeclaringType(method, type), !Method_Modifier(1928492, "
        "method).\n"

        ".decl MethodLookup(sn: int32, d: int32, type: int32, method: int32)\n"
        "MethodLookup(sn, d, type, method) :- "
        "MethodImplemented(sn, d, type, method).\n"

        ".decl VarPointsTo(heap: int32, var: int32)\n"
        "VarPointsTo(heap, var) :- "
        "AssignHeapAllocation(heap, var, inMethod), Reachable(inMethod).\n"
        "VarPointsTo(heap, this) :- "
        "Reachable(inMethod), Instruction_Method(inv, inMethod), "
        "VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base), "
        "HeapAllocation_Type(heap, heaptype), "
        "VirtualMethodInvocation_SimpleName(inv, sn), "
        "VirtualMethodInvocation_Descriptor(inv, d), "
        "MethodLookup(sn, d, heaptype, toMethod), ThisVar(toMethod, this).\n"

        ".decl CallGraphEdge(inv: int32, method: int32)\n"
        "CallGraphEdge(inv, toMethod) :- "
        "Reachable(inMethod), Instruction_Method(inv, inMethod), "
        "VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base), "
        "HeapAllocation_Type(heap, heaptype), "
        "VirtualMethodInvocation_SimpleName(inv, sn), "
        "VirtualMethodInvocation_Descriptor(inv, d), "
        "MethodLookup(sn, d, heaptype, toMethod).\n"

        "Reachable(toMethod) :- "
        "Reachable(inMethod), Instruction_Method(inv, inMethod), "
        "VirtualMethodInvocation_Base(inv, base), VarPointsTo(heap, base), "
        "HeapAllocation_Type(heap, heaptype), "
        "VirtualMethodInvocation_SimpleName(inv, sn), "
        "VirtualMethodInvocation_Descriptor(inv, d), "
        "MethodLookup(sn, d, heaptype, toMethod).\n";

    int64_t reach_count = 0;
    int64_t total = run_program_count_rel(src, 1, "Reachable", &reach_count);

    ASSERT(total >= 0, "program evaluation failed");

    /*
     * Expected Reachable: {0, 1, 2} = 3 facts.
     * main=0 is seeded; method1=1 via call from main; method2=2 via call
     * from method1 (requires fixpoint re-evaluation after Reachable(1) added).
     */
    char msg[128];
    snprintf(msg, sizeof(msg),
        "expected Reachable count == 3 (main,m1,m2), got %" PRId64,
        reach_count);
    ASSERT(reach_count == 3, msg);

    PASS();
}

/*
 * Test 8: MethodLookup with stratified negation (abstract methods excluded).
 *
 * MethodImplemented uses !Method_Modifier(1928492, method) to exclude abstract
 * methods.  This test verifies stratified negation is evaluated correctly:
 * an abstract method must NOT appear in MethodImplemented.
 *
 * Facts:
 *   Method 1: concrete (no modifier 1928492)  -> MethodImplemented
 *   Method 2: abstract (modifier 1928492)     -> NOT in MethodImplemented
 */
static void
test_doop_stratified_negation_excludes_abstract(void)
{
    TEST("doop_negation: abstract method excluded from MethodImplemented");

    const char *src
        = ".decl Method_SimpleName(method: int32, sn: int32)\n"
        "Method_SimpleName(1, 10).\n"
        "Method_SimpleName(2, 20).\n"
        ".decl Method_Descriptor(method: int32, d: int32)\n"
        "Method_Descriptor(1, 30).\n"
        "Method_Descriptor(2, 40).\n"
        ".decl Method_DeclaringType(method: int32, type: int32)\n"
        "Method_DeclaringType(1, 100).\n"
        "Method_DeclaringType(2, 200).\n"
        ".decl Method_Modifier(mod: int32, method: int32)\n"
        "Method_Modifier(1928492, 2).\n"   /* method 2 is abstract */
        ".decl MethodImplemented(sn: int32, d: int32, type: int32, method: "
        "int32)\n"
        "MethodImplemented(sn, d, type, method) :- "
        "Method_SimpleName(method, sn), Method_Descriptor(method, d), "
        "Method_DeclaringType(method, type), !Method_Modifier(1928492, "
        "method).\n";

    int64_t impl_count = 0;
    int64_t total
        = run_program_count_rel(src, 1, "MethodImplemented", &impl_count);

    ASSERT(total >= 0, "evaluation failed");

    /*
     * Only method 1 is concrete.  MethodImplemented must have exactly 1 tuple.
     */
    char msg[128];
    snprintf(msg, sizeof(msg),
        "expected MethodImplemented == 1 (concrete only), got %" PRId64,
        impl_count);
    ASSERT(impl_count == 1, msg);

    PASS();
}

/* ========================================================================
 * main
 * ======================================================================== */

int
main(void)
{
    printf("=== DOOP Validation Tests (US-009/010) ===\n");
    printf("NOTE: Infrastructure tests (1-5) PASS immediately.\n");
    printf(
        "      Synthetic join tests (6-8) FAIL until K-way delta expansion\n");
    printf(
        "      (Option 2 + CSE) is implemented — this is the TDD RED state.\n");
    printf("      Full DOOP benchmark -> scripts/run_doop_validation.sh\n\n");

    /* --- Infrastructure: data files --- */
    printf("--- Infrastructure: Data Files ---\n");
    test_doop_data_dir_accessible();
    test_doop_all_csv_files_exist();
    test_doop_dataset_size_in_range();

    /* --- Infrastructure: parser --- */
    printf("\n--- Infrastructure: Parser ---\n");
    test_doop_edb_declarations_parse();
    test_doop_mutual_recursion_parses();

    /* --- Integration: synthetic join correctness --- */
    printf("\n--- Integration: Synthetic Join Correctness ---\n");
    test_doop_8way_virtual_dispatch_join();
    test_doop_reachable_propagates_through_virtual_chain();
    test_doop_stratified_negation_excludes_abstract();

    printf("\n=== Results: %d passed, %d failed, %d skipped (of %d) ===\n",
        pass_count, fail_count, skip_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
