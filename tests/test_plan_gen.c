/*
 * test_plan_gen.c - Unit tests for wl_plan_from_program() and wl_plan_free()
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/backend.h"
#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/intern.h"
#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char *wl_test_make_delta_name(const char *name);

static const char *k_semijoin_chain_src;
static wirelog_ir_node_t *find_composite_left_joinlike(
    wirelog_ir_node_t *node, wirelog_ir_node_type_t type);

static int
test_extension_invoke(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *user_data)
{
    (void)args;
    (void)nargs;
    (void)result;
    (void)user_data;
    return 0;
}

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                    \
        do {                                              \
            test_count++;                                 \
            printf("TEST %d: %s ... ", test_count, name); \
        } while (0)

#define PASS()            \
        do {                  \
            pass_count++;     \
            printf("PASS\n"); \
        } while (0)

#define FAIL(msg)                  \
        do {                           \
            fail_count++;              \
            printf("FAIL: %s\n", msg); \
        } while (0)

#define ASSERT(cond, msg) \
        do {                  \
            if (!(cond)) {    \
                FAIL(msg);    \
                return;       \
            }                 \
        } while (0)

static bool
expr_contains_float_constant(const wl_ir_expr_t *expr)
{
    if (!expr)
        return false;
    if (expr->type == WL_IR_EXPR_CONST_FLOAT)
        return true;
    for (uint32_t i = 0; i < expr->child_count; i++) {
        if (expr_contains_float_constant(expr->children[i]))
            return true;
    }
    return false;
}

static bool
node_contains_float_constant(const wirelog_ir_node_t *node)
{
    if (!node)
        return false;
    if (expr_contains_float_constant(node->filter_expr)
        || expr_contains_float_constant(node->agg_expr))
        return true;
    for (uint32_t i = 0; node->project_exprs
        && i < node->project_count; i++) {
        if (expr_contains_float_constant(node->project_exprs[i]))
            return true;
    }
    for (uint32_t i = 0; i < node->child_count; i++) {
        if (node_contains_float_constant(node->children[i]))
            return true;
    }
    return false;
}

static wl_ir_expr_t *
find_expr_type(const wirelog_ir_node_t *node, wl_ir_expr_type_t type)
{
    if (!node)
        return NULL;
    const wl_ir_expr_t *exprs[] = {
        node->filter_expr, node->agg_expr
    };
    for (size_t i = 0; i < sizeof(exprs) / sizeof(exprs[0]); i++) {
        if (exprs[i] && exprs[i]->type == type)
            return (wl_ir_expr_t *)exprs[i];
        if (exprs[i]) {
            for (uint32_t c = 0; c < exprs[i]->child_count; c++) {
                if (exprs[i]->children[c]->type == type)
                    return exprs[i]->children[c];
            }
        }
    }
    for (uint32_t i = 0; node->project_exprs
        && i < node->project_count; i++) {
        if (node->project_exprs[i]
            && node->project_exprs[i]->type == type)
            return node->project_exprs[i];
    }
    for (uint32_t i = 0; i < node->child_count; i++) {
        wl_ir_expr_t *found = find_expr_type(node->children[i], type);
        if (found)
            return found;
    }
    return NULL;
}

static const wl_plan_op_t *
find_plan_op(const wl_plan_t *plan, wl_plan_op_type_t type)
{
    if (!plan)
        return NULL;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *rel = &plan->strata[s].relations[r];
            for (uint32_t o = 0; o < rel->op_count; o++) {
                if (rel->ops[o].op == type)
                    return &rel->ops[o];
            }
        }
    }
    return NULL;
}

static void
test_extension_call_ir_and_plan(void)
{
    TEST("scalar extension call lowers to IR and postfix plan");
    const char *src = ".decl src(x: int64)\n"
        ".decl out(x: int64)\n"
        "out(x) :- src(x), @call(\"math.is_even\", x).\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "extension filter failed to parse and lower");
    wl_ir_expr_t *call = find_expr_type(prog->rules[0].ir_root,
            WL_IR_EXPR_EXTENSION_CALL);
    ASSERT(call != NULL, "extension call missing from IR");
    ASSERT(strcmp(call->extension_name, "math.is_even") == 0,
        "extension name missing from IR");
    ASSERT(call->child_count == 1 && call->children[0]->type == WL_IR_EXPR_VAR,
        "extension arguments missing from IR");

    wl_ir_expr_t *clone = wl_ir_expr_clone(call);
    ASSERT(clone != NULL && clone != call, "extension IR clone failed");
    ASSERT(clone->extension_name != call->extension_name
        && strcmp(clone->extension_name, call->extension_name) == 0,
        "extension name was not deep-cloned");
    wl_ir_expr_free(clone);

    wl_plan_t *plan = NULL;
    ASSERT(wl_plan_from_program(prog, &plan) == 0 && plan != NULL,
        "direct extension filter failed to generate a plan");
    ASSERT(find_plan_op(plan, WL_PLAN_OP_FILTER) != NULL,
        "direct extension filter plan is missing");
    wl_plan_free(plan);

    wl_plan_expr_buffer_t serialized = { 0 };
    ASSERT(wl_exec_plan_gen_serialize_expr_for_test(call, &serialized) == 0,
        "extension serialization helper failed");
    const uint8_t expected[] = {
        WL_PLAN_EXPR_VAR, 1, 0, 'x',
        WL_PLAN_EXPR_EXTENSION_CALL, 12, 0,
        'm', 'a', 't', 'h', '.', 'i', 's', '_', 'e', 'v', 'e', 'n',
        1, 0, 0, 0
    };
    ASSERT(serialized.size == sizeof(expected),
        "extension postfix plan has unexpected size");
    ASSERT(memcmp(serialized.data, expected, sizeof(expected)) == 0,
        "extension postfix bytes are not stable");
    free(serialized.data);

    {
        static const uint32_t argument_types[] = {
            WIRELOG_EXTENSION_VALUE_INT64
        };
        wirelog_extension_registry_t *registry
            = wirelog_extension_registry_create();
        wirelog_extension_descriptor_t descriptor = {
            WIRELOG_EXTENSION_ABI_VERSION,
            sizeof(descriptor),
            "math.is_even",
            1,
            argument_types,
            WIRELOG_EXTENSION_VALUE_BOOL,
            test_extension_invoke,
            NULL,
            NULL,
            UINT64_C(0x0102030405060708),
            7
        };
        wirelog_extension_snapshot_t *snapshot = NULL;
        ASSERT(registry != NULL
            && wirelog_extension_register(registry, &descriptor) == 0,
            "metadata descriptor registration failed");
        snapshot = wirelog_extension_snapshot_acquire(registry);
        ASSERT(snapshot != NULL, "metadata snapshot acquire failed");
        wl_plan_expr_buffer_t versioned = { 0 };
        ASSERT(wl_exec_plan_gen_serialize_expr_with_snapshot_for_test(
                call, snapshot, &versioned) == 0,
            "versioned extension serialization failed");
        const uint8_t expected_versioned[] = {
            WL_PLAN_EXPR_VAR, 1, 0, 'x',
            WL_PLAN_EXPR_EXTENSION_CALL_ABI, 12, 0,
            'm', 'a', 't', 'h', '.', 'i', 's', '_', 'e', 'v', 'e', 'n',
            1, 0, 0, 0,
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
            7, 0, 0, 0
        };
        ASSERT(versioned.size == sizeof(expected_versioned)
            && memcmp(versioned.data, expected_versioned,
            sizeof(expected_versioned)) == 0,
            "versioned extension postfix bytes are not stable");
        free(versioned.data);
        wirelog_extension_snapshot_release(snapshot);
        ASSERT(wirelog_extension_unregister(registry, "math.is_even") == 0
            && wirelog_extension_registry_destroy(registry) == 0,
            "metadata registry destroy failed");
    }

    {
        wirelog_extension_registry_t *empty_registry
            = wirelog_extension_registry_create();
        wirelog_extension_snapshot_t *empty_snapshot
            = empty_registry
            ? wirelog_extension_snapshot_acquire(empty_registry) : NULL;
        wl_plan_t *missing_plan = NULL;
        ASSERT(empty_registry != NULL && empty_snapshot != NULL,
            "empty snapshot setup failed");
        ASSERT(wl_plan_from_program_with_snapshot(prog, empty_snapshot,
            &missing_plan) != 0 && missing_plan == NULL
            && strstr(wirelog_program_get_plan_error(prog),
            "absent from the pinned snapshot") != NULL,
            "missing snapshot descriptor did not fail with a diagnostic");
        wirelog_extension_snapshot_release(empty_snapshot);
        ASSERT(wirelog_extension_registry_destroy(empty_registry) == 0,
            "empty snapshot registry destroy failed");
    }

    {
        const char *string_src = ".decl src(x: string)\n"
            ".decl out(x: string)\n"
            "out(@call(\"text.keep\", x)) :- src(x).\n";
        wirelog_program_t *string_prog = wirelog_parse_string(string_src,
                &err);
        ASSERT(string_prog != NULL, "string MAP extension failed to parse");
        wl_plan_t *string_plan = NULL;
        ASSERT(wl_plan_from_program(string_prog, &string_plan) == 0
            && string_plan != NULL, "string MAP extension plan failed");
        const wl_plan_op_t *map = find_plan_op(string_plan, WL_PLAN_OP_MAP);
        const uint8_t string_var[] = {
            WL_PLAN_EXPR_VAR_STRING, 4, 0, 'c', 'o', 'l', '0',
            WL_PLAN_EXPR_EXTENSION_CALL, 9, 0,
            't', 'e', 'x', 't', '.', 'k', 'e', 'e', 'p',
            1, 0, 0, 0
        };
        ASSERT(map != NULL && map->map_exprs != NULL
            && map->map_exprs[0].size == sizeof(string_var)
            && memcmp(map->map_exprs[0].data, string_var,
            sizeof(string_var)) == 0,
            "string variable postfix encoding is not stable");
        wl_plan_free(string_plan);
        wirelog_program_free(string_prog);
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_existing_expression_plan_encoding_unchanged(void)
{
    TEST("ordinary expression plan encoding remains unchanged");
    const char *src = ".decl src(x: int64)\n"
        ".decl out(x: int64)\n"
        "out(x) :- src(x), x = 7.\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "ordinary expression failed to parse");
    wl_plan_t *plan = NULL;
    ASSERT(wl_plan_from_program(prog, &plan) == 0 && plan != NULL,
        "ordinary expression failed to generate a plan");
    const wl_plan_op_t *filter = find_plan_op(plan, WL_PLAN_OP_FILTER);
    const uint8_t expected[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0',
        WL_PLAN_EXPR_CONST_INT, 7, 0, 0, 0, 0, 0, 0, 0,
        WL_PLAN_EXPR_CMP_EQ
    };
    ASSERT(filter != NULL && filter->filter_expr.size == sizeof(expected),
        "ordinary filter encoding changed size");
    ASSERT(memcmp(filter->filter_expr.data, expected, sizeof(expected)) == 0,
        "ordinary filter encoding changed");
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_extension_call_unsupported_contexts_rejected(void)
{
    TEST("extension calls reject unsupported contexts deterministically");
    const char *sources[] = {
        ".decl src(x: int64)\n.decl out(x: int64)\n"
        "out(x) :- @call(\"math.answer\", x), src(x).\n",
        ".decl src(x: int64)\n.decl out(x: int64)\n"
        "out(@call(\"math.answer\", x)) :- src(x).\n",
        ".decl src(x: int64)\n.decl out(x: int64)\n"
        "out(x) :- src(x), !@call(\"math.answer\", x).\n",
        ".decl src(x: int64)\n.decl out(x: int64)\n"
        "out(x) :- src(x), @call(\"math.outer\", "
        "@call(\"math.inner\", x)).\n"
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        wirelog_error_t err;
        char diagnostic[512] = { 0 };
        wirelog_program_t *prog = wl_ir_parse_string_err(sources[i], &err,
                diagnostic, sizeof(diagnostic));
        if (i == 0) {
            ASSERT(prog == NULL, "leading extension call was silently kept");
            ASSERT(strstr(diagnostic, "before any relation scan") != NULL,
                "leading extension call lacks an IR diagnostic");
            continue;
        }
        if (i == 1) {
            wl_plan_t *plan = NULL;
            ASSERT(prog != NULL, "direct MAP extension call failed to parse");
            ASSERT(wl_plan_from_program(prog, &plan) == 0 && plan != NULL,
                "direct MAP extension call failed to generate a plan");
            ASSERT(find_plan_op(plan, WL_PLAN_OP_MAP) != NULL,
                "direct MAP extension call has no MAP operator");
            wl_plan_free(plan);
            wirelog_program_free(prog);
            continue;
        }
        if (prog) {
            wl_plan_t *plan = NULL;
            ASSERT(wl_plan_from_program(prog, &plan) != 0 && plan == NULL,
                "unsupported extension context generated a plan");
            ASSERT(wirelog_program_get_plan_error(prog)[0] != '\0',
                "unsupported extension context lacks a diagnostic");
            wirelog_program_free(prog);
        } else {
            ASSERT(err == WIRELOG_ERR_PARSE,
                "unsupported extension context returned wrong parse error");
        }
    }

    {
        const char *src = ".decl src(x: int64)\n"
            ".decl out(x: int64)\n"
            "out(x) :- src(x), @call(\"math.answer\", x).\n";
        wirelog_error_t err;
        wirelog_program_t *prog = wirelog_parse_string(src, &err);
        ASSERT(prog != NULL, "length-validation fixture failed to parse");
        wl_ir_expr_t *call = find_expr_type(prog->rules[0].ir_root,
                WL_IR_EXPR_EXTENSION_CALL);
        ASSERT(call != NULL, "length-validation call missing from IR");
        char *long_name = (char *)malloc(UINT16_MAX + 2u);
        ASSERT(long_name != NULL, "length-validation allocation failed");
        memset(long_name, 'a', UINT16_MAX + 1u);
        long_name[UINT16_MAX + 1u] = '\0';
        free(call->extension_name);
        call->extension_name = long_name;
        wl_plan_t *plan = NULL;
        ASSERT(wl_plan_from_program(prog, &plan) != 0 && plan == NULL,
            "overlong extension name was serialized");
        wirelog_program_free(prog);
    }
    PASS();
}

static void
test_optimized_flatmap_extension_filter_rejected(void)
{
    TEST("optimized FLATMAP extension filter is rejected");
    const char *src = ".decl src(x: int64)\n"
        ".decl out(x: int64)\n"
        "out(x) :- src(x), @call(\"math.answer\", x).\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "optimized FLATMAP fixture failed to parse");
    ASSERT(wirelog_optimize(prog, &err),
        "optimized FLATMAP fixture failed to optimize");
    ASSERT(prog->rules[0].ir_root != NULL
        && prog->rules[0].ir_root->type == WIRELOG_IR_FLATMAP,
        "optimizer did not produce the FLATMAP regression shape");
    wl_plan_t *plan = NULL;
    ASSERT(wl_plan_from_program(prog, &plan) != 0 && plan == NULL,
        "optimized FLATMAP extension filter generated a plan");
    ASSERT(strstr(wirelog_program_get_plan_error(prog),
        "MAP/FLATMAP") != NULL,
        "optimized FLATMAP rejection lacks a deterministic diagnostic");
    wirelog_program_free(prog);
    PASS();
}

static char *
test_copy_string(const char *value)
{
    size_t length = strlen(value) + 1;
    char *copy = (char *)malloc(length);
    if (copy)
        memcpy(copy, value, length);
    return copy;
}

static wirelog_ir_node_t *
test_wrap_right_filter(wirelog_ir_node_t *right)
{
    wirelog_ir_node_t *filter = wl_ir_node_create(WIRELOG_IR_FILTER);
    wl_ir_expr_t *call = wl_ir_expr_create(WL_IR_EXPR_EXTENSION_CALL);
    if (!filter || !call) {
        wl_ir_node_free(filter);
        wl_ir_expr_free(call);
        return NULL;
    }
    filter->filter_expr = call;
    call->extension_name = test_copy_string("test.right");
    if (!call->extension_name || wl_ir_node_add_child(filter, right) != 0) {
        wl_ir_node_free(filter);
        return NULL;
    }
    return filter;
}

static void
test_extension_right_filters_rejected(void)
{
    TEST("extension right-filters reject deterministically");
    const char *sources[] = {
        ".decl a(x: int64, y: int64)\n.decl b(y: int64, z: int64)\n"
        ".decl blocked(z: int64)\n.decl out(x: int64)\n"
        "out(x) :- a(x, y), b(y, z), !blocked(z).\n",
        k_semijoin_chain_src
    };
    const wirelog_ir_node_type_t types[] = {
        WIRELOG_IR_ANTIJOIN, WIRELOG_IR_SEMIJOIN
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        wirelog_error_t err;
        wirelog_program_t *prog = wirelog_parse_string(
            i == 1 ? k_semijoin_chain_src : sources[i], &err);
        ASSERT(prog != NULL, "right-filter fixture failed to parse");
        if (i == 1) {
            wl_fusion_apply(prog, NULL);
            wl_jpp_apply(prog, NULL);
            wl_sip_apply(prog, NULL);
        }
        wirelog_ir_node_t *joinlike = NULL;
        for (uint32_t r = 0; r < prog->rule_count && !joinlike; r++)
            joinlike = find_composite_left_joinlike(prog->rules[r].ir_root,
                    types[i]);
        ASSERT(joinlike != NULL, "right-filter joinlike node missing");
        wirelog_ir_node_t *right = joinlike->children[1];
        joinlike->children[1] = test_wrap_right_filter(right);
        ASSERT(joinlike->children[1] != NULL, "right-filter wrapper failed");
        ASSERT(wl_ir_program_rebuild_relation_irs(prog) == 0,
            "right-filter IR rebuild failed");
        wl_plan_t *plan = NULL;
        ASSERT(wl_plan_from_program(prog, &plan) != 0 && plan == NULL,
            "extension right-filter generated a plan");
        ASSERT(strstr(wirelog_program_get_plan_error(prog), "right-filter")
            != NULL, "right-filter rejection lacks a deterministic diagnostic");
        wirelog_program_free(prog);
    }
    PASS();
}

/* ----------------------------------------------------------------
 * Snapshot counting callback
 * ---------------------------------------------------------------- */

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

/* ----------------------------------------------------------------
 * Test: Simple TC program generates valid plan
 * ---------------------------------------------------------------- */

static void
test_tc_plan_generation(void)
{
    TEST("TC plan generation");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "wl_plan_from_program failed");
    ASSERT(plan != NULL, "plan is NULL");

    /* Should have at least 1 stratum */
    ASSERT(plan->stratum_count >= 1, "no strata");

    /* Should have at least 1 relation (tc) in some stratum */
    int found_tc = 0;
    int checked_delta_names = 0;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *relation
                = &plan->strata[s].relations[r];
            if (relation->name) {
                ASSERT(relation->delta_name != NULL,
                    "plan relation delta name is missing");
                char expected[256];
                int written = snprintf(expected, sizeof(expected), "$d$%s",
                        relation->name);
                ASSERT(written >= 0 && (size_t)written < sizeof(expected),
                    "delta name fixture unexpectedly too long");
                ASSERT(strcmp(relation->delta_name, expected) == 0,
                    "plan relation delta name mismatch");
                ASSERT(relation->delta_name[written] == '\0',
                    "plan relation delta name not terminated");
                checked_delta_names++;
            }
            if (relation->name && strcmp(relation->name, "tc") == 0) {
                found_tc = 1;
                ASSERT(relation->op_count > 0,
                    "tc has no ops");
            }
        }
    }
    ASSERT(found_tc, "tc relation not found in plan");
    ASSERT(checked_delta_names > 0, "no plan delta names were checked");

    /* edge should be an EDB */
    int found_edge_edb = 0;
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        if (plan->edb_relations[i]
            && strcmp(plan->edb_relations[i], "edge") == 0)
            found_edge_edb = 1;
    }
    ASSERT(found_edge_edb, "edge not in EDB list");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_delta_name_invariant(void)
{
    TEST("delta name prefix and termination invariant");
    char *name = wl_test_make_delta_name("edge");
    ASSERT(name != NULL, "non-empty delta name allocation failed");
    ASSERT(strcmp(name, "$d$edge") == 0, "delta name content mismatch");
    ASSERT(name[strlen("$d$edge")] == '\0', "delta name not terminated");
    free(name);

    char *empty = wl_test_make_delta_name("");
    ASSERT(empty != NULL, "empty delta name allocation failed");
    ASSERT(strcmp(empty, "$d$") == 0, "empty delta name mismatch");
    ASSERT(empty[3] == '\0', "empty delta name not terminated");
    free(empty);
    PASS();
}

static void
test_plan_generation_preinterns_static_string_literals(void)
{
    TEST("plan generation pre-interns static string literals");

    const char *src = ".decl trigger(value: symbol)\n"
        ".decl unused(value: symbol, plane: symbol)\n"
        "unused(X, \"data\") :- trigger(X).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");
    ASSERT(wl_intern_get(prog->intern, "data") < 0,
        "parser unexpectedly interned projection literal");

    ASSERT(wl_fusion_apply(prog, NULL) == 0, "fusion failed");
    ASSERT(wl_jpp_apply(prog, NULL) == 0, "JPP failed");
    ASSERT(wl_sip_apply(prog, NULL) == 0, "SIP failed");

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");
    ASSERT(wl_intern_get(prog->intern, "data") >= 0,
        "plan generation did not intern projection literal");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: TC end-to-end via columnar session
 * ---------------------------------------------------------------- */

static void
test_tc_end_to_end(void)
{
    TEST("TC end-to-end columnar session");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");
    ASSERT(ctx.count > 0, "no tuples produced");

    /* TC on 1->2->3->4 should produce 6 tuples:
     * tc(1,2), tc(2,3), tc(3,4), tc(1,3), tc(2,4), tc(1,4) */
    ASSERT(ctx.count == 6, "expected 6 TC tuples");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_join_project_fusion_plan_shape(void)
{
    TEST("JOIN project fusion plan shape");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl r(x: int32, z: int32)\n"
        "r(x, z) :- a(x, y), b(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    const wl_plan_relation_t *rp = NULL;
    for (uint32_t s = 0; s < plan->stratum_count && !rp; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *cand = &plan->strata[s].relations[r];
            if (cand->name && strcmp(cand->name, "r") == 0) {
                rp = cand;
                break;
            }
        }
    }
    ASSERT(rp != NULL, "relation r not found");

    const wl_plan_op_t *join = NULL;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        ASSERT(rp->ops[i].op != WL_PLAN_OP_MAP,
            "pure join projection should not leave MAP");
        if (rp->ops[i].op == WL_PLAN_OP_JOIN)
            join = &rp->ops[i];
    }
    ASSERT(join != NULL, "JOIN not found");
    ASSERT(join->project_count == 2, "JOIN should carry projection width");
    ASSERT(join->project_indices != NULL, "JOIN projection indices missing");
    ASSERT(join->project_indices[0] == 0, "first projected column should be x");
    ASSERT(join->project_indices[1] == 3,
        "second projected column should be z");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_join_project_fusion_keeps_computed_map(void)
{
    TEST("JOIN project fusion keeps computed MAP");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl r(x: int32, z: int32)\n"
        "r(x + 1, z) :- a(x, y), b(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    bool found_map = false;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *rp = &plan->strata[s].relations[r];
            if (!rp->name || strcmp(rp->name, "r") != 0)
                continue;
            for (uint32_t i = 0; i < rp->op_count; i++)
                if (rp->ops[i].op == WL_PLAN_OP_MAP)
                    found_map = true;
        }
    }
    ASSERT(found_map, "computed projection should keep MAP");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_k_fusion_sequences_fuse_join_project(void)
{
    TEST("K_FUSION sequences fuse JOIN project");

    const char *src = ".decl p(x: int32, y: int32)\n"
        "p(1, 2).\n"
        "p(x, z) :- p(x, y), p(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    const wl_plan_op_k_fusion_t *kf = NULL;
    for (uint32_t s = 0; s < plan->stratum_count && !kf; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count && !kf; r++) {
            const wl_plan_relation_t *rel = &plan->strata[s].relations[r];
            if (!rel->name || strcmp(rel->name, "p") != 0)
                continue;
            for (uint32_t i = 0; i < rel->op_count; i++) {
                if (rel->ops[i].op == WL_PLAN_OP_K_FUSION) {
                    kf = (const wl_plan_op_k_fusion_t *)
                        rel->ops[i].opaque_data;
                    break;
                }
            }
        }
    }
    ASSERT(kf != NULL, "K_FUSION not found");
    ASSERT(kf->k >= 2, "K_FUSION should contain delta copies");

    for (uint32_t d = 0; d < kf->k; d++) {
        bool projected_join = false;
        for (uint32_t i = 0; i < kf->k_op_counts[d]; i++) {
            const wl_plan_op_t *op = &kf->k_ops[d][i];
            ASSERT(op->op != WL_PLAN_OP_MAP,
                "pure projection MAP should be fused inside K_FUSION");
            if (op->op == WL_PLAN_OP_JOIN && op->project_count == 2
                && op->project_indices)
                projected_join = true;
        }
        ASSERT(projected_join, "projected JOIN not found in K_FUSION copy");
    }

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: SIP-inserted SEMIJOINs must not shift column indices (#955)
 *
 * A SEMIJOIN filters its left input and emits the left columns only.
 * When the plan generator resolved join keys and projections against a
 * column layout that also counted the SEMIJOIN's right side, every index
 * past the first SEMIJOIN was shifted; out-of-range "colN" names then fell
 * back to column 0 in the evaluator, silently dropping derivations.
 *
 * The chain below is the smallest shape that exposes it: five atoms, all
 * variables live in the head (so no projection re-bases the layout), each
 * atom introducing a fresh variable.
 * ---------------------------------------------------------------- */

static const char *k_semijoin_chain_src
    = ".decl a(p1: int64, p2: int64)\n"
    "a(1, 2).\n"
    ".decl b(q1: int64, q2: int64)\n"
    "b(2, 3).\n"
    ".decl c(r1: int64, r2: int64)\n"
    "c(3, 4).\n"
    ".decl d(s1: int64, s2: int64)\n"
    "d(4, 5).\n"
    ".decl e(t1: int64, t2: int64)\n"
    "e(5, 6).\n"
    ".decl chain(c1: int64, c2: int64, c3: int64, c4: int64, c5: int64,"
    " c6: int64)\n"
    "chain(v1, v2, v3, v4, v5, v6) :- a(v1, v2), b(v2, v3), c(v3, v4),"
    " d(v4, v5), e(v5, v6).\n";

/* Declared arity of the relations in k_semijoin_chain_src. */
static uint32_t
chain_rel_width(const char *name)
{
    if (!name)
        return 0;
    if (strcmp(name, "chain") == 0)
        return 6;
    return 2; /* a, b, c, d, e are all binary */
}

static void
test_semijoin_does_not_shift_column_indices(void)
{
    TEST("SEMIJOIN does not shift join key/projection indices");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(k_semijoin_chain_src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    const wl_plan_relation_t *rp = NULL;
    for (uint32_t s = 0; s < plan->stratum_count && !rp; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *cand = &plan->strata[s].relations[r];
            if (cand->name && strcmp(cand->name, "chain") == 0) {
                rp = cand;
                break;
            }
        }
    }
    ASSERT(rp != NULL, "relation chain not found");

    /* Replay the operator sequence, tracking the width of the relation on
     * top of the evaluator stack, and check every resolved index against
     * the operand widths it will actually see at run time. */
    uint32_t width = 0;
    int saw_semijoin = 0;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        const wl_plan_op_t *op = &rp->ops[i];
        uint32_t rw = op->right_relation ? chain_rel_width(op->right_relation)
                                         : 0;
        if (op->op == WL_PLAN_OP_SEMIJOIN)
            saw_semijoin = 1;
        if (op->op == WL_PLAN_OP_JOIN || op->op == WL_PLAN_OP_SEMIJOIN
            || op->op == WL_PLAN_OP_ANTIJOIN) {
            for (uint32_t k = 0; k < op->key_count; k++) {
                const char *lk = op->left_keys ? op->left_keys[k] : NULL;
                ASSERT(lk != NULL, "missing left key");
                ASSERT(strncmp(lk, "col", 3) == 0, "left key is not colN");
                ASSERT((uint32_t)atoi(lk + 3) < width,
                    "left join key column index out of range");
            }
            uint32_t concat = (op->op == WL_PLAN_OP_JOIN) ? width + rw : width;
            for (uint32_t p = 0; p < op->project_count; p++) {
                if (!op->project_indices)
                    break;
                ASSERT(op->project_indices[p] < concat,
                    "projection index out of range");
            }
        }
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
            width = chain_rel_width(op->relation_name);
            break;
        case WL_PLAN_OP_JOIN:
            width = op->project_count ? op->project_count : width + rw;
            break;
        case WL_PLAN_OP_MAP:
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
            if (op->project_count)
                width = op->project_count;
            break;
        default:
            break;
        }
    }
    ASSERT(saw_semijoin, "SIP inserted no SEMIJOIN -- test no longer covers "
        "the regression");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

struct chain_ctx {
    int64_t count;
    int64_t row[8];
    uint32_t ncols;
};

static void
chain_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct chain_ctx *ctx = (struct chain_ctx *)user_data;
    if (!relation || strcmp(relation, "chain") != 0)
        return;
    ctx->count++;
    if (ctx->count == 1) {
        ctx->ncols = ncols < 8 ? ncols : 8;
        for (uint32_t i = 0; i < ctx->ncols; i++)
            ctx->row[i] = row[i];
    }
}

static void
test_semijoin_chain_end_to_end(void)
{
    TEST("five-atom chain with SEMIJOINs derives its one tuple");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(k_semijoin_chain_src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    struct chain_ctx ctx = { 0, { 0 }, 0 };
    rc = wl_session_snapshot(sess, chain_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");

    ASSERT(ctx.count == 1, "expected exactly one chain tuple");
    ASSERT(ctx.ncols == 6, "expected six columns");
    for (uint32_t i = 0; i < 6; i++)
        ASSERT(ctx.row[i] == (int64_t)(i + 1), "unexpected chain tuple value");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* Same chain shape, but with a negated atom so the head projection stays a
 * separate MAP instead of being fused into the last JOIN.  The shifted
 * layout corrupted that projection too: the tuple was still derived, with
 * an out-of-range index silently yielding 0 in the last column. */
static const char *k_semijoin_negation_src
    = ".decl a(p1: int64, p2: int64)\n"
    "a(1, 2).\n"
    ".decl b(q1: int64, q2: int64)\n"
    "b(2, 3).\n"
    ".decl n(z1: int64)\n"
    "n(99).\n"
    ".decl c(r1: int64, r2: int64)\n"
    "c(3, 4).\n"
    ".decl d(s1: int64, s2: int64)\n"
    "d(4, 5).\n"
    ".decl chain(c1: int64, c2: int64, c3: int64, c4: int64, c5: int64)\n"
    "chain(v1, v2, v3, v4, v5) :- a(v1, v2), b(v2, v3), !n(v3),"
    " c(v3, v4), d(v4, v5).\n";

static void
test_semijoin_head_projection_values(void)
{
    TEST("SEMIJOIN chain keeps head projection values intact");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(k_semijoin_negation_src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    struct chain_ctx ctx = { 0, { 0 }, 0 };
    rc = wl_session_snapshot(sess, chain_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");

    ASSERT(ctx.count == 1, "expected exactly one chain tuple");
    ASSERT(ctx.ncols == 5, "expected five columns");
    for (uint32_t i = 0; i < 5; i++)
        ASSERT(ctx.row[i] == (int64_t)(i + 1), "unexpected chain tuple value");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: a JOIN whose right child is not a relation is rejected
 *
 * wl_plan_op_t.right_relation is a relation name, not a subtree.  A JOIN
 * whose right child is itself a composite node cannot be represented; if
 * plan generation emits it anyway the operator silently matches nothing
 * (Issue #989).
 * ---------------------------------------------------------------- */

static wirelog_ir_node_t *
find_right_deep_candidate(wirelog_ir_node_t *node)
{
    if (!node)
        return NULL;
    if (node->type == WIRELOG_IR_JOIN && node->child_count == 2
        && node->children[0]
        && node->children[0]->type == WIRELOG_IR_JOIN)
        return node;
    for (uint32_t i = 0; i < node->child_count; i++) {
        wirelog_ir_node_t *hit = find_right_deep_candidate(node->children[i]);
        if (hit)
            return hit;
    }
    return NULL;
}

static wirelog_ir_node_t *
find_composite_left_joinlike(wirelog_ir_node_t *node,
    wirelog_ir_node_type_t type)
{
    if (!node)
        return NULL;
    if (node->type == type && node->child_count == 2
        && node->children[0]
        && node->children[0]->type == WIRELOG_IR_JOIN
        && node->children[1] && node->children[1]->relation_name)
        return node;
    for (uint32_t i = 0; i < node->child_count; i++) {
        wirelog_ir_node_t *hit
            = find_composite_left_joinlike(node->children[i], type);
        if (hit)
            return hit;
    }
    return NULL;
}

static void
test_join_with_composite_right_child_rejected(void)
{
    TEST("JOIN with composite right child is rejected");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl c(z: int32, w: int32)\n"
        ".decl out(x: int32, w: int32)\n"
        "a(1, 2). b(2, 3). c(3, 4).\n"
        "out(x, w) :- a(x, y), b(y, z), c(z, w).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    /* The parser builds JOIN(JOIN(a, b), c).  Swap the outer JOIN's
     * children to obtain the right-deep JOIN(c, JOIN(a, b)). */
    wirelog_ir_node_t *outer = NULL;
    for (uint32_t r = 0; r < prog->rule_count && !outer; r++) {
        if (prog->rules[r].head_relation
            && strcmp(prog->rules[r].head_relation, "out") == 0)
            outer = find_right_deep_candidate(prog->rules[r].ir_root);
    }
    if (!outer) {
        wirelog_program_free(prog);
        FAIL("no left-deep JOIN chain to invert");
        return;
    }

    wirelog_ir_node_t *tmp = outer->children[0];
    outer->children[0] = outer->children[1];
    outer->children[1] = tmp;

    if (wl_ir_program_rebuild_relation_irs(prog) != 0) {
        wirelog_program_free(prog);
        FAIL("rebuild_relation_irs failed");
        return;
    }

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc == 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("plan generation accepted an unrepresentable JOIN right child");
        return;
    }
    ASSERT(plan == NULL, "plan must not be returned on error");

    wirelog_program_free(prog);
    PASS();
}

static void
test_antijoin_with_composite_right_child_rejected(void)
{
    TEST("ANTIJOIN with composite right child is rejected (#993)");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl blocked(z: int32)\n"
        ".decl out(x: int32)\n"
        "out(x) :- a(x, y), b(y, z), !blocked(z).\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_plan_t *control = NULL;
    int rc = wl_plan_from_program(prog, &control);
    ASSERT(rc == 0 && control != NULL,
        "unmodified ANTIJOIN plan should be valid");
    wl_plan_free(control);

    wirelog_ir_node_t *anti = NULL;
    for (uint32_t r = 0; r < prog->rule_count && !anti; r++) {
        if (prog->rules[r].head_relation
            && strcmp(prog->rules[r].head_relation, "out") == 0)
            anti = find_composite_left_joinlike(prog->rules[r].ir_root,
                    WIRELOG_IR_ANTIJOIN);
    }
    ASSERT(anti != NULL, "no composite-left ANTIJOIN candidate found");

    uint32_t saved_child_count = anti->child_count;
    anti->child_count = 1;
    ASSERT(wl_ir_program_rebuild_relation_irs(prog) == 0,
        "rebuild_relation_irs failed for missing child");
    wl_plan_t *missing_plan = NULL;
    rc = wl_plan_from_program(prog, &missing_plan);
    ASSERT(rc != 0 && missing_plan == NULL,
        "plan generation accepted a missing ANTIJOIN child");
    anti->child_count = saved_child_count;

    wirelog_ir_node_t *tmp = anti->children[0];
    anti->children[0] = anti->children[1];
    anti->children[1] = tmp;
    ASSERT(wl_ir_program_rebuild_relation_irs(prog) == 0,
        "rebuild_relation_irs failed");

    wl_plan_t *plan = NULL;
    rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc != 0 && plan == NULL,
        "plan generation accepted an unrepresentable ANTIJOIN");
    wirelog_program_free(prog);
    PASS();
}

static void
test_semijoin_with_composite_right_child_rejected(void)
{
    TEST("SEMIJOIN with composite right child is rejected (#993)");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(k_semijoin_chain_src, &err);
    ASSERT(prog != NULL, "parse failed");
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *control = NULL;
    int rc = wl_plan_from_program(prog, &control);
    ASSERT(rc == 0 && control != NULL,
        "unmodified SEMIJOIN plan should be valid");
    wl_plan_free(control);

    wirelog_ir_node_t *semi = NULL;
    for (uint32_t r = 0; r < prog->rule_count && !semi; r++) {
        if (prog->rules[r].head_relation
            && strcmp(prog->rules[r].head_relation, "chain") == 0)
            semi = find_composite_left_joinlike(prog->rules[r].ir_root,
                    WIRELOG_IR_SEMIJOIN);
    }
    ASSERT(semi != NULL, "no composite-left SEMIJOIN candidate found");

    wirelog_ir_node_t *tmp = semi->children[0];
    semi->children[0] = semi->children[1];
    semi->children[1] = tmp;
    ASSERT(wl_ir_program_rebuild_relation_irs(prog) == 0,
        "rebuild_relation_irs failed");

    wl_plan_t *plan = NULL;
    rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc != 0 && plan == NULL,
        "plan generation accepted an unrepresentable SEMIJOIN");
    wirelog_program_free(prog);
    PASS();
}

static const wl_plan_relation_t *
find_plan_relation(const wl_plan_t *plan, const char *name)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *rel = &plan->strata[s].relations[r];
            if (rel->name && strcmp(rel->name, name) == 0)
                return rel;
        }
    }
    return NULL;
}

/* Issue #994: side-compound joins must lower from either atom position. */
static void
test_side_compound_lowers_in_either_atom_position(void)
{
    TEST("side compound lowers from any body-atom position (#994)");

    const char *src = ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl gate(id: int64)\n"
        ".decl hot(id: int64, r: int64)\n"
        "hot(ID, R) :- gate(ID), event(ID, metadata(_, _, _, R)).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0 || !plan) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("side compound in a non-first atom must lower");
        return;
    }
    const wl_plan_relation_t *hot = find_plan_relation(plan, "hot");
    ASSERT(hot != NULL, "hot relation plan not found");

    const wl_plan_op_t *chain = NULL;
    const wl_plan_op_t *side = NULL;
    for (uint32_t i = 0; i < hot->op_count; i++) {
        const wl_plan_op_t *op = &hot->ops[i];
        if (op->op != WL_PLAN_OP_JOIN || !op->right_relation)
            continue;
        if (strcmp(op->right_relation, "event") == 0)
            chain = op;
        if (strcmp(op->right_relation, "__compound_metadata_4") == 0)
            side = op;
    }
    ASSERT(chain != NULL, "event join not found");
    ASSERT(side != NULL, "side-compound join not found");
    ASSERT(side > chain, "side join must follow the chain join");
    ASSERT(side->key_count == 1, "side join must have one key");
    ASSERT(strcmp(side->right_keys[0], "col0") == 0,
        "side join must use side handle column");
    wirelog_program_free(prog);
    wl_plan_free(plan);

    /* Control: the same rule with the compound atom first must also lower;
     * both spellings are accepted. */
    const char *ok_src = ".decl event(id: int64, payload: metadata/4 side)\n"
        ".decl gate(id: int64)\n"
        ".decl hot(id: int64, r: int64)\n"
        "hot(ID, R) :- event(ID, metadata(_, _, _, R)), gate(ID).\n";

    wirelog_program_t *ok = wirelog_parse_string(ok_src, &err);
    ASSERT(ok != NULL, "control parse failed");

    wl_plan_t *ok_plan = NULL;
    if (wl_plan_from_program(ok, &ok_plan) != 0) {
        wirelog_program_free(ok);
        FAIL("control: compound-first rule must still lower");
        return;
    }
    wl_plan_free(ok_plan);
    wirelog_program_free(ok);

    PASS();
}

/* ----------------------------------------------------------------
 * Test: wl_plan_free NULL-safe
 * ---------------------------------------------------------------- */

static void
test_plan_free_null(void)
{
    TEST("wl_plan_free NULL-safe");
    wl_plan_free(NULL); /* should not crash */
    PASS();
}

static void
test_plan_generation_diagnostics(void)
{
    TEST("plan-generation diagnostics are retained on the public program");

    ASSERT(wirelog_program_get_plan_error(NULL)[0] == '\0',
        "NULL program must have an empty plan diagnostic");

    const char *src =
        ".decl plan_edge(x: int64, y: int64)\n"
        "plan_edge(1, 2).\n"
        ".decl plan_count(x: int64, n: int64)\n"
        "plan_count(1, 1).\n"
        "plan_count(y, count(n)) :- plan_count(x, n), plan_edge(x, y).\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "diagnostic fixture failed to parse");

    wl_plan_t *plan = NULL;
    ASSERT(wl_plan_from_program(prog, &plan) != 0,
        "recursive count fixture unexpectedly generated a plan");
    ASSERT(plan == NULL, "failed plan generation must leave plan NULL");
    const char *detail = wirelog_program_get_plan_error(prog);
    ASSERT(detail && strstr(detail, "recursive aggregate") != NULL,
        "plan diagnostic must identify recursive aggregate rejection");
    ASSERT(strstr(detail, "plan_count") != NULL,
        "plan diagnostic must identify the rejected relation");

    ASSERT(wl_plan_from_program(prog, NULL) != 0,
        "NULL output must be rejected");
    detail = wirelog_program_get_plan_error(prog);
    ASSERT(strcmp(detail, "execution plan generation failed") == 0,
        "NULL output must retain the generic plan diagnostic");

    wirelog_program_free(prog);

    wirelog_program_t *ok = wirelog_parse_string(
        ".decl plan_ok(x: int64)\nplan_ok(1).\n", &err);
    ASSERT(ok != NULL, "control fixture failed to parse");
    wl_plan_t *ok_plan = NULL;
    ASSERT(wl_plan_from_program(ok, &ok_plan) == 0 && ok_plan != NULL,
        "valid control unexpectedly failed plan generation");
    ASSERT(wirelog_program_get_plan_error(ok)[0] == '\0',
        "successful plan generation must clear its previous diagnostic");
    wl_plan_free(ok_plan);
    wirelog_program_free(ok);
    PASS();
}

static void
test_float_plan_lowering(void)
{
    TEST("float plans lower with typed columnar support");
    wirelog_error_t err;
    const char *sources[] = {
        ".decl value(x: float)\nvalue(1.5).\n",
        ".decl value(x: int64)\nvalue(1.5).\n",
        ".decl value(x: box(float))\nvalue(1.5).\n",
        ".decl value(x: box(float))\n",
        ".decl value(x: box(float) inline)\n",
        ".decl src(x: int64)\nsrc(1).\n"
        ".decl out(x: int64)\nout(1.5) :- src(x).\n",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        wirelog_program_t *prog = wirelog_parse_string(sources[i], &err);
        if (i == 1) {
            ASSERT(prog == NULL,
                "decimal float fact must not enter integer column");
            continue;
        }
        ASSERT(prog != NULL, "float fixture failed to parse");
        if (i == 5) {
            ASSERT(prog->rule_count == 1 && prog->rules[0].ir_root != NULL,
                "float rule was not converted to IR");
            ASSERT(node_contains_float_constant(prog->rules[0].ir_root),
                "float rule literal was not preserved in the IR");
        }
        wl_plan_t *plan = NULL;
        int plan_rc = wl_plan_from_program(prog, &plan);
        if (i == 0) {
            ASSERT(plan_rc == 0 && plan != NULL,
                "declared float relation should lower");
            wl_plan_free(plan);
            wirelog_program_free(prog);
            continue;
        }
        if (plan_rc == 0) {
            wl_plan_free(plan);
            wirelog_program_free(prog);
            continue;
        }
        ASSERT(plan == NULL, "rejected float plan must remain NULL");
        const char *detail = wirelog_program_get_plan_error(prog);
        if (i < 3) {
            ASSERT(detail && strstr(detail, "float") != NULL,
                "float rejection must explain the unsupported execution path");
        }
        wirelog_program_free(prog);
    }
    PASS();
}

static void
test_float_compound_metadata_resets_on_redeclaration(void)
{
    TEST("float compound metadata resets on redeclaration");
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl redecl(x: box(float))\n"
        ".decl redecl(x: int64)\n"
        "redecl(1).\n", &err);
    ASSERT(prog != NULL, "redeclaration fixture failed to parse");
    wl_plan_t *plan = NULL;
    ASSERT(wl_plan_from_program(prog, &plan) == 0 && plan != NULL,
        "integer redeclaration retained stale float metadata");
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: wl_session_load_facts NULL-safe
 * ---------------------------------------------------------------- */

static void
test_load_facts_null_safe(void)
{
    TEST("wl_session_load_facts NULL-safe");
    int rc = wl_session_load_facts(NULL, NULL);
    ASSERT(rc == -1, "expected -1 for NULL args");
    PASS();
}

/* ----------------------------------------------------------------
 * Main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Plan Generator Tests ===\n");

    test_delta_name_invariant();
    test_tc_plan_generation();
    test_plan_generation_preinterns_static_string_literals();
    test_tc_end_to_end();
    test_join_project_fusion_plan_shape();
    test_join_project_fusion_keeps_computed_map();
    test_k_fusion_sequences_fuse_join_project();
    test_semijoin_does_not_shift_column_indices();
    test_semijoin_chain_end_to_end();
    test_semijoin_head_projection_values();
    test_join_with_composite_right_child_rejected();
    test_antijoin_with_composite_right_child_rejected();
    test_semijoin_with_composite_right_child_rejected();
    test_side_compound_lowers_in_either_atom_position();
    test_plan_generation_diagnostics();
    test_float_plan_lowering();
    test_float_compound_metadata_resets_on_redeclaration();
    test_extension_call_ir_and_plan();
    test_existing_expression_plan_encoding_unchanged();
    test_extension_call_unsupported_contexts_rejected();
    test_optimized_flatmap_extension_filter_rejected();
    test_extension_right_filters_rejected();
    test_plan_free_null();
    test_load_facts_null_safe();

    printf("\n%d tests: %d passed, %d failed\n", test_count, pass_count,
        fail_count);
    return fail_count > 0 ? 1 : 0;
}
