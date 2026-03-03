/*
 * test_dd_ffi.c - Tests for DD FFI Marshalling Layer
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * TDD RED phase: these tests are written BEFORE dd_marshal.c exists.
 * The file compiles (references only declared functions) but will not
 * link until the implementation is provided.
 */

#include "../wirelog/ffi/dd_ffi.h"
#include "../wirelog/ir/ir.h"
#include "../wirelog/wirelog-parser.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
    do {                                      \
        tests_run++;                          \
        printf("  [%d] %s", tests_run, name); \
    } while (0)

#define PASS()                 \
    do {                       \
        tests_passed++;        \
        printf(" ... PASS\n"); \
    } while (0)

#define FAIL(msg)                         \
    do {                                  \
        tests_failed++;                   \
        printf(" ... FAIL: %s\n", (msg)); \
    } while (0)

/* ======================================================================== */
/* Helper: generate DD plan from Datalog source                             */
/* ======================================================================== */

static wl_ffi_dd_plan_t *
plan_from_source(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_ffi_dd_plan_t *plan = NULL;
    int rc = wl_ffi_dd_plan_generate(prog, &plan);
    wirelog_program_free(prog);

    if (rc != 0)
        return NULL;
    return plan;
}

/* ======================================================================== */
/* Helper: find FFI relation plan by name                                   */
/* ======================================================================== */

static const wl_ffi_relation_plan_t *
find_ffi_relation(const wl_ffi_plan_t *plan, const char *name)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            if (strcmp(plan->strata[s].relations[r].name, name) == 0)
                return &plan->strata[s].relations[r];
        }
    }
    return NULL;
}

/* ======================================================================== */
/* Expression Serialization Tests                                           */
/* ======================================================================== */

static void
test_expr_serialize_null(void)
{
    TEST("expr serialize: NULL expr -> data=NULL, size=0, rc=0");

    wl_ffi_expr_buffer_t buf;
    memset(&buf, 0xFF, sizeof(buf)); /* poison to detect writes */
    int rc = wl_ffi_expr_serialize(NULL, &buf);

    if (rc != 0) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected rc=0, got %d", rc);
        FAIL(msg);
        return;
    }

    if (buf.data != NULL) {
        FAIL("data should be NULL for NULL expr");
        return;
    }

    if (buf.size != 0) {
        FAIL("size should be 0 for NULL expr");
        return;
    }

    PASS();
}

static void
test_expr_serialize_var(void)
{
    TEST("expr serialize: EXPR_VAR(x) -> tag + name");

    wl_ir_expr_t *expr = wl_ir_expr_create(WL_IR_EXPR_VAR);
    expr->var_name = strdup_safe("x");

    wl_ffi_expr_buffer_t buf;
    int rc = wl_ffi_expr_serialize(expr, &buf);

    if (rc != 0) {
        wl_ir_expr_free(expr);
        FAIL("serialize failed");
        return;
    }

    /* Expected: [EXPR_VAR=0x01] [name_len:u16=1,0] [name:'x'] = 4 bytes */
    if (buf.size < 4) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected >= 4 bytes, got %u", buf.size);
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL(msg);
        return;
    }

    if (buf.data[0] != WL_FFI_EXPR_VAR) {
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL("first byte should be EXPR_VAR tag");
        return;
    }

    /* name_len as u16 little-endian */
    uint16_t name_len = (uint16_t)(buf.data[1] | (buf.data[2] << 8));
    if (name_len != 1) {
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL("name_len should be 1");
        return;
    }

    if (buf.data[3] != 'x') {
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL("name byte should be 'x'");
        return;
    }

    free(buf.data);
    wl_ir_expr_free(expr);
    PASS();
}

static void
test_expr_serialize_const_int(void)
{
    TEST("expr serialize: EXPR_CONST_INT(42) -> tag + i64");

    wl_ir_expr_t *expr = wl_ir_expr_create(WL_IR_EXPR_CONST_INT);
    expr->int_value = 42;

    wl_ffi_expr_buffer_t buf;
    int rc = wl_ffi_expr_serialize(expr, &buf);

    if (rc != 0) {
        wl_ir_expr_free(expr);
        FAIL("serialize failed");
        return;
    }

    /* Expected: [EXPR_CONST_INT=0x02] [value:i64] = 9 bytes */
    if (buf.size != 9) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected 9 bytes, got %u", buf.size);
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL(msg);
        return;
    }

    if (buf.data[0] != WL_FFI_EXPR_CONST_INT) {
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL("first byte should be EXPR_CONST_INT tag");
        return;
    }

    /* Read i64 little-endian */
    int64_t val = 0;
    memcpy(&val, &buf.data[1], sizeof(int64_t));
    if (val != 42) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected value=42, got %lld",
                 (long long)val);
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL(msg);
        return;
    }

    free(buf.data);
    wl_ir_expr_free(expr);
    PASS();
}

static void
test_expr_serialize_const_str(void)
{
    TEST("expr serialize: EXPR_CONST_STR(\"hi\") -> tag + len + data");

    wl_ir_expr_t *expr = wl_ir_expr_create(WL_IR_EXPR_CONST_STR);
    expr->str_value = strdup_safe("hi");

    wl_ffi_expr_buffer_t buf;
    int rc = wl_ffi_expr_serialize(expr, &buf);

    if (rc != 0) {
        wl_ir_expr_free(expr);
        FAIL("serialize failed");
        return;
    }

    /* Expected: [EXPR_CONST_STR=0x03] [len:u16=2,0] ['h','i'] = 5 bytes */
    if (buf.size != 5) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected 5 bytes, got %u", buf.size);
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL(msg);
        return;
    }

    if (buf.data[0] != WL_FFI_EXPR_CONST_STR) {
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL("first byte should be EXPR_CONST_STR tag");
        return;
    }

    uint16_t str_len = (uint16_t)(buf.data[1] | (buf.data[2] << 8));
    if (str_len != 2) {
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL("str_len should be 2");
        return;
    }

    if (buf.data[3] != 'h' || buf.data[4] != 'i') {
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL("string data should be 'hi'");
        return;
    }

    free(buf.data);
    wl_ir_expr_free(expr);
    PASS();
}

static void
test_expr_serialize_bool(void)
{
    TEST("expr serialize: EXPR_BOOL(true) -> tag + u8(1)");

    wl_ir_expr_t *expr = wl_ir_expr_create(WL_IR_EXPR_BOOL);
    expr->bool_value = true;

    wl_ffi_expr_buffer_t buf;
    int rc = wl_ffi_expr_serialize(expr, &buf);

    if (rc != 0) {
        wl_ir_expr_free(expr);
        FAIL("serialize failed");
        return;
    }

    /* Expected: [EXPR_BOOL=0x04] [value:u8=1] = 2 bytes */
    if (buf.size != 2) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected 2 bytes, got %u", buf.size);
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL(msg);
        return;
    }

    if (buf.data[0] != WL_FFI_EXPR_BOOL) {
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL("first byte should be EXPR_BOOL tag");
        return;
    }

    if (buf.data[1] != 1) {
        free(buf.data);
        wl_ir_expr_free(expr);
        FAIL("value byte should be 1 (true)");
        return;
    }

    free(buf.data);
    wl_ir_expr_free(expr);
    PASS();
}

static void
test_expr_serialize_cmp_gt(void)
{
    TEST("expr serialize: X > 5 -> [VAR(X)][CONST_INT(5)][CMP_GT]");

    /* Build: CMP(GT, VAR("X"), CONST_INT(5)) */
    wl_ir_expr_t *var_x = wl_ir_expr_create(WL_IR_EXPR_VAR);
    var_x->var_name = strdup_safe("X");

    wl_ir_expr_t *const5 = wl_ir_expr_create(WL_IR_EXPR_CONST_INT);
    const5->int_value = 5;

    wl_ir_expr_t *cmp = wl_ir_expr_create(WL_IR_EXPR_CMP);
    cmp->cmp_op = WIRELOG_CMP_GT;
    wl_ir_expr_add_child(cmp, var_x);
    wl_ir_expr_add_child(cmp, const5);

    wl_ffi_expr_buffer_t buf;
    int rc = wl_ffi_expr_serialize(cmp, &buf);

    if (rc != 0) {
        wl_ir_expr_free(cmp);
        FAIL("serialize failed");
        return;
    }

    if (buf.data == NULL || buf.size == 0) {
        wl_ir_expr_free(cmp);
        FAIL("buffer should not be empty");
        return;
    }

    /*
     * Expected postfix encoding:
     * [EXPR_VAR][1,0]['X']  = 4 bytes
     * [EXPR_CONST_INT][5,0,0,0,0,0,0,0] = 9 bytes
     * [EXPR_CMP_GT] = 1 byte
     * Total = 14 bytes
     */
    if (buf.size != 14) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected 14 bytes, got %u", buf.size);
        free(buf.data);
        wl_ir_expr_free(cmp);
        FAIL(msg);
        return;
    }

    /* First instruction: VAR("X") */
    if (buf.data[0] != WL_FFI_EXPR_VAR) {
        free(buf.data);
        wl_ir_expr_free(cmp);
        FAIL("byte[0] should be EXPR_VAR");
        return;
    }

    /* Second instruction: CONST_INT(5) at offset 4 */
    if (buf.data[4] != WL_FFI_EXPR_CONST_INT) {
        free(buf.data);
        wl_ir_expr_free(cmp);
        FAIL("byte[4] should be EXPR_CONST_INT");
        return;
    }

    /* Third instruction: CMP_GT at offset 13 */
    if (buf.data[13] != WL_FFI_EXPR_CMP_GT) {
        free(buf.data);
        wl_ir_expr_free(cmp);
        FAIL("last byte should be EXPR_CMP_GT");
        return;
    }

    free(buf.data);
    wl_ir_expr_free(cmp);
    PASS();
}

static void
test_expr_serialize_arith_add(void)
{
    TEST("expr serialize: A + B -> [VAR(A)][VAR(B)][ARITH_ADD]");

    wl_ir_expr_t *var_a = wl_ir_expr_create(WL_IR_EXPR_VAR);
    var_a->var_name = strdup_safe("A");

    wl_ir_expr_t *var_b = wl_ir_expr_create(WL_IR_EXPR_VAR);
    var_b->var_name = strdup_safe("B");

    wl_ir_expr_t *arith = wl_ir_expr_create(WL_IR_EXPR_ARITH);
    arith->arith_op = WIRELOG_ARITH_ADD;
    wl_ir_expr_add_child(arith, var_a);
    wl_ir_expr_add_child(arith, var_b);

    wl_ffi_expr_buffer_t buf;
    int rc = wl_ffi_expr_serialize(arith, &buf);

    if (rc != 0) {
        wl_ir_expr_free(arith);
        FAIL("serialize failed");
        return;
    }

    /*
     * Expected postfix:
     * [EXPR_VAR][1,0]['A']  = 4 bytes
     * [EXPR_VAR][1,0]['B']  = 4 bytes
     * [EXPR_ARITH_ADD]      = 1 byte
     * Total = 9 bytes
     */
    if (buf.size != 9) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected 9 bytes, got %u", buf.size);
        free(buf.data);
        wl_ir_expr_free(arith);
        FAIL(msg);
        return;
    }

    /* First: VAR(A) */
    if (buf.data[0] != WL_FFI_EXPR_VAR) {
        free(buf.data);
        wl_ir_expr_free(arith);
        FAIL("byte[0] should be EXPR_VAR");
        return;
    }

    /* Second: VAR(B) at offset 4 */
    if (buf.data[4] != WL_FFI_EXPR_VAR) {
        free(buf.data);
        wl_ir_expr_free(arith);
        FAIL("byte[4] should be EXPR_VAR");
        return;
    }

    /* Last: ARITH_ADD */
    if (buf.data[8] != WL_FFI_EXPR_ARITH_ADD) {
        free(buf.data);
        wl_ir_expr_free(arith);
        FAIL("byte[8] should be EXPR_ARITH_ADD");
        return;
    }

    free(buf.data);
    wl_ir_expr_free(arith);
    PASS();
}

static void
test_expr_serialize_nested(void)
{
    TEST("expr serialize: (X + 1) > 5 -> nested postfix");

    /* Build: CMP(GT, ARITH(ADD, VAR("X"), CONST_INT(1)), CONST_INT(5)) */
    wl_ir_expr_t *var_x = wl_ir_expr_create(WL_IR_EXPR_VAR);
    var_x->var_name = strdup_safe("X");

    wl_ir_expr_t *const1 = wl_ir_expr_create(WL_IR_EXPR_CONST_INT);
    const1->int_value = 1;

    wl_ir_expr_t *add = wl_ir_expr_create(WL_IR_EXPR_ARITH);
    add->arith_op = WIRELOG_ARITH_ADD;
    wl_ir_expr_add_child(add, var_x);
    wl_ir_expr_add_child(add, const1);

    wl_ir_expr_t *const5 = wl_ir_expr_create(WL_IR_EXPR_CONST_INT);
    const5->int_value = 5;

    wl_ir_expr_t *gt = wl_ir_expr_create(WL_IR_EXPR_CMP);
    gt->cmp_op = WIRELOG_CMP_GT;
    wl_ir_expr_add_child(gt, add);
    wl_ir_expr_add_child(gt, const5);

    wl_ffi_expr_buffer_t buf;
    int rc = wl_ffi_expr_serialize(gt, &buf);

    if (rc != 0) {
        wl_ir_expr_free(gt);
        FAIL("serialize failed");
        return;
    }

    /*
     * Expected postfix (left-to-right, depth-first):
     * [VAR("X")]        = 4 bytes   (offset 0)
     * [CONST_INT(1)]    = 9 bytes   (offset 4)
     * [ARITH_ADD]       = 1 byte    (offset 13)
     * [CONST_INT(5)]    = 9 bytes   (offset 14)
     * [CMP_GT]          = 1 byte    (offset 23)
     * Total = 24 bytes
     */
    if (buf.size != 24) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected 24 bytes, got %u", buf.size);
        free(buf.data);
        wl_ir_expr_free(gt);
        FAIL(msg);
        return;
    }

    /* Verify ordering: VAR first, then CONST_INT, ADD, CONST_INT, CMP_GT */
    if (buf.data[0] != WL_FFI_EXPR_VAR) {
        free(buf.data);
        wl_ir_expr_free(gt);
        FAIL("byte[0] should be EXPR_VAR");
        return;
    }

    if (buf.data[4] != WL_FFI_EXPR_CONST_INT) {
        free(buf.data);
        wl_ir_expr_free(gt);
        FAIL("byte[4] should be EXPR_CONST_INT");
        return;
    }

    if (buf.data[13] != WL_FFI_EXPR_ARITH_ADD) {
        free(buf.data);
        wl_ir_expr_free(gt);
        FAIL("byte[13] should be EXPR_ARITH_ADD");
        return;
    }

    if (buf.data[14] != WL_FFI_EXPR_CONST_INT) {
        free(buf.data);
        wl_ir_expr_free(gt);
        FAIL("byte[14] should be EXPR_CONST_INT");
        return;
    }

    if (buf.data[23] != WL_FFI_EXPR_CMP_GT) {
        free(buf.data);
        wl_ir_expr_free(gt);
        FAIL("byte[23] should be EXPR_CMP_GT");
        return;
    }

    free(buf.data);
    wl_ir_expr_free(gt);
    PASS();
}

/* ======================================================================== */
/* Marshal Plan: NULL / Invalid Input Tests                                 */
/* ======================================================================== */

static void
test_marshal_null_plan(void)
{
    TEST("marshal plan: NULL plan -> returns -2");

    wl_ffi_plan_t *out = NULL;
    int rc = wl_dd_marshal_plan(NULL, &out);

    if (rc != -2) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected rc=-2, got %d", rc);
        FAIL(msg);
        return;
    }

    if (out != NULL) {
        wl_ffi_plan_free(out);
        FAIL("out should remain NULL on error");
        return;
    }

    PASS();
}

static void
test_marshal_null_out(void)
{
    TEST("marshal plan: NULL out -> returns -2");

    /* Create a minimal valid plan to test the out parameter check */
    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    int rc = wl_dd_marshal_plan(plan, NULL);

    if (rc != -2) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected rc=-2, got %d", rc);
        wl_ffi_dd_plan_free(plan);
        FAIL(msg);
        return;
    }

    wl_ffi_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Marshal Plan: Empty Plan Tests                                           */
/* ======================================================================== */

static void
test_marshal_empty_plan(void)
{
    TEST("marshal plan: EDB-only -> valid empty FFI plan");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected rc=0, got %d", rc);
        wl_ffi_dd_plan_free(plan);
        FAIL(msg);
        return;
    }

    if (!ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("ffi plan should not be NULL");
        return;
    }

    if (ffi->edb_count != 1) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected 1 EDB, got %u", ffi->edb_count);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL(msg);
        return;
    }

    if (strcmp(ffi->edb_relations[0], "a") != 0) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("EDB relation should be 'a'");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Marshal Plan: EDB Relations Tests                                        */
/* ======================================================================== */

static void
test_marshal_edb_relations(void)
{
    TEST("marshal plan: multiple EDB -> edb_relations array correct");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl b(x: int32)\n"
                                          ".decl c(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    /* a, b, c are EDB (no rules) */
    if (ffi->edb_count != 3) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected 3 EDB, got %u", ffi->edb_count);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL(msg);
        return;
    }

    /* Verify all EDB names are non-NULL strings */
    for (uint32_t i = 0; i < ffi->edb_count; i++) {
        if (!ffi->edb_relations[i] || strlen(ffi->edb_relations[i]) == 0) {
            wl_ffi_plan_free(ffi);
            wl_ffi_dd_plan_free(plan);
            FAIL("EDB relation name is NULL or empty");
            return;
        }
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Marshal Plan: Operator Translation Tests                                 */
/* ======================================================================== */

static void
test_marshal_scan_op(void)
{
    TEST("marshal plan: SCAN -> FFI VARIABLE op");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "r");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'r' not found in FFI plan");
        return;
    }

    /* Should have a VARIABLE op referencing "a" */
    bool found_var = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_FFI_OP_VARIABLE) {
            if (rp->ops[i].relation_name
                && strcmp(rp->ops[i].relation_name, "a") == 0) {
                found_var = true;
            }
        }
    }

    if (!found_var) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected FFI VARIABLE op with relation_name='a'");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_filter_op(void)
{
    TEST("marshal plan: FILTER -> FFI FILTER with serialized expr");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x), x > 5.\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "r");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'r' not found in FFI plan");
        return;
    }

    bool found_filter = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_FFI_OP_FILTER) {
            if (rp->ops[i].filter_expr.data != NULL
                && rp->ops[i].filter_expr.size > 0) {
                found_filter = true;
            } else {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("FILTER op has NULL/empty filter_expr");
                return;
            }
        }
    }

    if (!found_filter) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected FFI FILTER op");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_map_op(void)
{
    TEST("marshal plan: PROJECT -> FFI MAP with project_indices");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32, y: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x, y).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "r");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'r' not found in FFI plan");
        return;
    }

    bool found_map = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_FFI_OP_MAP) {
            if (rp->ops[i].project_count > 0) {
                found_map = true;
            } else {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("MAP op has project_count == 0");
                return;
            }
        }
    }

    if (!found_map) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected FFI MAP op");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_map_exprs(void)
{
    TEST("marshal plan: b(x, y+1) :- a(x,y). -> MAP with map_exprs");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32, y: int32)\n"
                                          ".decl b(x: int32, y: int32)\n"
                                          "b(x, y + 1) :- a(x, y).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "b");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'b' not found in FFI plan");
        return;
    }

    /* Find MAP op and verify it has map_exprs with serialized data
     * for column 1 (y+1) and NULL data for column 0 (simple x). */
    bool found = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_FFI_OP_MAP && rp->ops[i].map_exprs != NULL
            && rp->ops[i].map_expr_count == 2) {
            /* Column 0 (x) should have no expression (data==NULL) */
            if (rp->ops[i].map_exprs[0].data != NULL) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("map_exprs[0] should be NULL for simple column x");
                return;
            }
            /* Column 1 (y+1) should have serialized expression data */
            if (rp->ops[i].map_exprs[1].data == NULL
                || rp->ops[i].map_exprs[1].size == 0) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("map_exprs[1] should have serialized data for y+1");
                return;
            }
            found = true;
        }
    }

    if (!found) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected MAP op with map_exprs");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_join_op(void)
{
    TEST("marshal plan: JOIN -> FFI JOIN with keys and right_relation");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32, y: int32)\n"
                                          ".decl b(y: int32, z: int32)\n"
                                          ".decl r(x: int32, z: int32)\n"
                                          "r(x, z) :- a(x, y), b(y, z).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "r");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'r' not found in FFI plan");
        return;
    }

    bool found_join = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_FFI_OP_JOIN) {
            if (rp->ops[i].key_count < 1) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("JOIN key_count should be >= 1");
                return;
            }
            if (!rp->ops[i].right_relation) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("JOIN right_relation should not be NULL");
                return;
            }
            if (strcmp(rp->ops[i].right_relation, "b") != 0) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("JOIN right_relation should be 'b'");
                return;
            }
            if (!rp->ops[i].left_keys || !rp->ops[i].right_keys) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("JOIN left_keys/right_keys should not be NULL");
                return;
            }
            found_join = true;
        }
    }

    if (!found_join) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected FFI JOIN op");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_antijoin_op(void)
{
    TEST("marshal plan: ANTIJOIN -> FFI ANTIJOIN with right_relation");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl b(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x), !b(x).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "r");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'r' not found in FFI plan");
        return;
    }

    bool found_antijoin = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_FFI_OP_ANTIJOIN) {
            if (!rp->ops[i].right_relation) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("ANTIJOIN right_relation should not be NULL");
                return;
            }
            if (strcmp(rp->ops[i].right_relation, "b") != 0) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("ANTIJOIN right_relation should be 'b'");
                return;
            }
            found_antijoin = true;
        }
    }

    if (!found_antijoin) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected FFI ANTIJOIN op");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_reduce_op(void)
{
    TEST("marshal plan: AGGREGATE -> FFI REDUCE with agg_fn + group_by");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32, y: int32)\n"
                                          ".decl r(x: int32, c: int32)\n"
                                          "r(x, count(y)) :- a(x, y).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "r");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'r' not found in FFI plan");
        return;
    }

    bool found_reduce = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_FFI_OP_REDUCE) {
            if (rp->ops[i].group_by_count == 0) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("REDUCE group_by_count should be > 0");
                return;
            }
            if (!rp->ops[i].group_by_indices) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("REDUCE group_by_indices should not be NULL");
                return;
            }
            found_reduce = true;
        }
    }

    if (!found_reduce) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected FFI REDUCE op");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_union_ops(void)
{
    TEST("marshal plan: UNION -> FFI CONCAT + CONSOLIDATE");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl b(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x).\n"
                                          "r(x) :- b(x).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "r");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'r' not found in FFI plan");
        return;
    }

    bool found_concat = false;
    bool found_consolidate = false;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_FFI_OP_CONCAT)
            found_concat = true;
        if (rp->ops[i].op == WL_FFI_OP_CONSOLIDATE)
            found_consolidate = true;
    }

    if (!found_concat) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected FFI CONCAT op for union");
        return;
    }

    if (!found_consolidate) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected FFI CONSOLIDATE op for union");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Marshal Plan: Stratum Structure Tests                                    */
/* ======================================================================== */

static void
test_marshal_recursive_stratum(void)
{
    TEST("marshal plan: recursive tc -> is_recursive preserved");

    wl_ffi_dd_plan_t *plan
        = plan_from_source(".decl edge(x: int32, y: int32)\n"
                           ".decl tc(x: int32, y: int32)\n"
                           "tc(x, y) :- edge(x, y).\n"
                           "tc(x, y) :- tc(x, z), edge(z, y).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    bool found_recursive = false;
    for (uint32_t s = 0; s < ffi->stratum_count; s++) {
        if (ffi->strata[s].is_recursive) {
            for (uint32_t r = 0; r < ffi->strata[s].relation_count; r++) {
                if (strcmp(ffi->strata[s].relations[r].name, "tc") == 0)
                    found_recursive = true;
            }
        }
    }

    if (!found_recursive) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("expected recursive stratum containing 'tc'");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_multi_stratum_ordering(void)
{
    TEST("marshal plan: chain a->b->c -> strata ordered by ID");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl b(x: int32)\n"
                                          ".decl c(x: int32)\n"
                                          "b(x) :- a(x).\n"
                                          "c(x) :- b(x).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    if (ffi->stratum_count < 2) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected >= 2 strata, got %u",
                 ffi->stratum_count);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL(msg);
        return;
    }

    /* Strata should be ordered by stratum_id */
    for (uint32_t s = 1; s < ffi->stratum_count; s++) {
        if (ffi->strata[s].stratum_id <= ffi->strata[s - 1].stratum_id) {
            wl_ffi_plan_free(ffi);
            wl_ffi_dd_plan_free(plan);
            FAIL("strata should be ordered by ascending stratum_id");
            return;
        }
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_stratum_count_matches(void)
{
    TEST("marshal plan: FFI stratum_count matches DD plan");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl b(x: int32)\n"
                                          ".decl c(x: int32)\n"
                                          "b(x) :- a(x).\n"
                                          "c(x) :- b(x).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    uint32_t expected_count = plan->stratum_count;

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    if (ffi->stratum_count != expected_count) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected %u strata, got %u", expected_count,
                 ffi->stratum_count);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL(msg);
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Marshal Plan: Relation Name and Op Count Fidelity                        */
/* ======================================================================== */

static void
test_marshal_relation_name_copied(void)
{
    TEST("marshal plan: relation name is a deep copy");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "r");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'r' not found in FFI plan");
        return;
    }

    if (strcmp(rp->name, "r") != 0) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("FFI relation name should be 'r'");
        return;
    }

    if (rp->op_count < 1) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("FFI relation should have >= 1 op");
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

static void
test_marshal_edb_count_matches(void)
{
    TEST("marshal plan: FFI edb_count matches DD plan");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl b(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    uint32_t expected_edb = plan->edb_count;

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    if (ffi->edb_count != expected_edb) {
        char msg[100];
        snprintf(msg, sizeof(msg), "expected %u EDB, got %u", expected_edb,
                 ffi->edb_count);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL(msg);
        return;
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Marshal Plan: JOIN Key Fidelity                                          */
/* ======================================================================== */

static void
test_marshal_join_keys_copied(void)
{
    TEST("marshal plan: JOIN left_keys/right_keys are deep copies");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32, y: int32)\n"
                                          ".decl b(y: int32, z: int32)\n"
                                          ".decl r(x: int32, z: int32)\n"
                                          "r(x, z) :- a(x, y), b(y, z).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    const wl_ffi_relation_plan_t *rp = find_ffi_relation(ffi, "r");
    if (!rp) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(plan);
        FAIL("relation 'r' not found in FFI plan");
        return;
    }

    /* Find JOIN op and verify key strings */
    for (uint32_t i = 0; i < rp->op_count; i++) {
        if (rp->ops[i].op == WL_FFI_OP_JOIN) {
            if (rp->ops[i].key_count != 1) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("JOIN should have 1 key pair");
                return;
            }
            if (strcmp(rp->ops[i].left_keys[0], "y") != 0) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("JOIN left_keys[0] should be 'y'");
                return;
            }
            if (strcmp(rp->ops[i].right_keys[0], "y") != 0) {
                wl_ffi_plan_free(ffi);
                wl_ffi_dd_plan_free(plan);
                FAIL("JOIN right_keys[0] should be 'y'");
                return;
            }

            wl_ffi_plan_free(ffi);
            wl_ffi_dd_plan_free(plan);
            PASS();
            return;
        }
    }

    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    FAIL("JOIN op not found");
}

/* ======================================================================== */
/* FFI Plan Free Tests                                                      */
/* ======================================================================== */

static void
test_ffi_plan_free_null(void)
{
    TEST("ffi plan free: NULL does not crash");

    wl_ffi_plan_free(NULL);

    PASS();
}

static void
test_ffi_plan_free_after_marshal(void)
{
    TEST("ffi plan free: fields accessible before free");

    wl_ffi_dd_plan_t *plan = plan_from_source(".decl a(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not create test plan");
        return;
    }

    wl_ffi_plan_t *ffi = NULL;
    int rc = wl_dd_marshal_plan(plan, &ffi);

    if (rc != 0 || !ffi) {
        wl_ffi_dd_plan_free(plan);
        FAIL("marshal failed");
        return;
    }

    /* Access all fields before free to verify structure integrity */
    uint32_t sc = ffi->stratum_count;
    uint32_t ec = ffi->edb_count;
    (void)sc;
    (void)ec;

    for (uint32_t s = 0; s < ffi->stratum_count; s++) {
        (void)ffi->strata[s].stratum_id;
        (void)ffi->strata[s].is_recursive;
        for (uint32_t r = 0; r < ffi->strata[s].relation_count; r++) {
            const wl_ffi_relation_plan_t *rp = &ffi->strata[s].relations[r];
            (void)rp->name;
            for (uint32_t o = 0; o < rp->op_count; o++) {
                (void)rp->ops[o].op;
            }
        }
    }

    for (uint32_t i = 0; i < ffi->edb_count; i++) {
        (void)ffi->edb_relations[i];
    }

    /* Free should not crash */
    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* FFI Type Size Tests                                                      */
/* ======================================================================== */

static void
test_ffi_types_exist(void)
{
    TEST("FFI types: structs have non-zero size");

    if (sizeof(wl_ffi_op_t) == 0) {
        FAIL("wl_ffi_op_t has zero size");
        return;
    }

    if (sizeof(wl_ffi_relation_plan_t) == 0) {
        FAIL("wl_ffi_relation_plan_t has zero size");
        return;
    }

    if (sizeof(wl_ffi_stratum_plan_t) == 0) {
        FAIL("wl_ffi_stratum_plan_t has zero size");
        return;
    }

    if (sizeof(wl_ffi_plan_t) == 0) {
        FAIL("wl_ffi_plan_t has zero size");
        return;
    }

    if (sizeof(wl_ffi_expr_buffer_t) == 0) {
        FAIL("wl_ffi_expr_buffer_t has zero size");
        return;
    }

    PASS();
}

static void
test_ffi_op_type_values(void)
{
    TEST("FFI op types: explicit integer values match spec");

    if (WL_FFI_OP_VARIABLE != 0) {
        FAIL("WL_FFI_OP_VARIABLE should be 0");
        return;
    }
    if (WL_FFI_OP_MAP != 1) {
        FAIL("WL_FFI_OP_MAP should be 1");
        return;
    }
    if (WL_FFI_OP_FILTER != 2) {
        FAIL("WL_FFI_OP_FILTER should be 2");
        return;
    }
    if (WL_FFI_OP_JOIN != 3) {
        FAIL("WL_FFI_OP_JOIN should be 3");
        return;
    }
    if (WL_FFI_OP_ANTIJOIN != 4) {
        FAIL("WL_FFI_OP_ANTIJOIN should be 4");
        return;
    }
    if (WL_FFI_OP_REDUCE != 5) {
        FAIL("WL_FFI_OP_REDUCE should be 5");
        return;
    }
    if (WL_FFI_OP_CONCAT != 6) {
        FAIL("WL_FFI_OP_CONCAT should be 6");
        return;
    }
    if (WL_FFI_OP_CONSOLIDATE != 7) {
        FAIL("WL_FFI_OP_CONSOLIDATE should be 7");
        return;
    }

    PASS();
}

static void
test_ffi_expr_tag_values(void)
{
    TEST("FFI expr tags: values match spec");

    if (WL_FFI_EXPR_VAR != 0x01) {
        FAIL("WL_FFI_EXPR_VAR should be 0x01");
        return;
    }
    if (WL_FFI_EXPR_CONST_INT != 0x02) {
        FAIL("WL_FFI_EXPR_CONST_INT should be 0x02");
        return;
    }
    if (WL_FFI_EXPR_CONST_STR != 0x03) {
        FAIL("WL_FFI_EXPR_CONST_STR should be 0x03");
        return;
    }
    if (WL_FFI_EXPR_BOOL != 0x04) {
        FAIL("WL_FFI_EXPR_BOOL should be 0x04");
        return;
    }
    if (WL_FFI_EXPR_ARITH_ADD != 0x10) {
        FAIL("WL_FFI_EXPR_ARITH_ADD should be 0x10");
        return;
    }
    if (WL_FFI_EXPR_CMP_EQ != 0x20) {
        FAIL("WL_FFI_EXPR_CMP_EQ should be 0x20");
        return;
    }
    if (WL_FFI_EXPR_AGG_COUNT != 0x30) {
        FAIL("WL_FFI_EXPR_AGG_COUNT should be 0x30");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== wirelog DD FFI Marshalling Tests ===\n\n");

    /* FFI type and enum value tests */
    printf("--- FFI Types ---\n");
    test_ffi_types_exist();
    test_ffi_op_type_values();
    test_ffi_expr_tag_values();

    /* Expression serialization tests */
    printf("\n--- Expression Serialization ---\n");
    test_expr_serialize_null();
    test_expr_serialize_var();
    test_expr_serialize_const_int();
    test_expr_serialize_const_str();
    test_expr_serialize_bool();
    test_expr_serialize_cmp_gt();
    test_expr_serialize_arith_add();
    test_expr_serialize_nested();

    /* Marshal plan: null/invalid input */
    printf("\n--- Marshal Plan: Input Validation ---\n");
    test_marshal_null_plan();
    test_marshal_null_out();

    /* Marshal plan: empty and EDB-only */
    printf("\n--- Marshal Plan: EDB & Empty ---\n");
    test_marshal_empty_plan();
    test_marshal_edb_relations();
    test_marshal_edb_count_matches();

    /* Marshal plan: operator translation */
    printf("\n--- Marshal Plan: Operator Translation ---\n");
    test_marshal_scan_op();
    test_marshal_filter_op();
    test_marshal_map_op();
    test_marshal_map_exprs();
    test_marshal_join_op();
    test_marshal_antijoin_op();
    test_marshal_reduce_op();
    test_marshal_union_ops();

    /* Marshal plan: JOIN key fidelity */
    printf("\n--- Marshal Plan: Key Fidelity ---\n");
    test_marshal_join_keys_copied();
    test_marshal_relation_name_copied();

    /* Marshal plan: stratum structure */
    printf("\n--- Marshal Plan: Stratum Structure ---\n");
    test_marshal_recursive_stratum();
    test_marshal_multi_stratum_ordering();
    test_marshal_stratum_count_matches();

    /* FFI plan free */
    printf("\n--- FFI Plan Free ---\n");
    test_ffi_plan_free_null();
    test_ffi_plan_free_after_marshal();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
