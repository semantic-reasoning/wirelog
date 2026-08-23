/* Regression coverage for issues #1143 and #1165. */

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/parser/parser.h"

void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
void *__real_realloc(void *ptr, size_t size);

static long fail_at = -1;
static long allocation_calls;

void *
__wrap_malloc(size_t size)
{
    if (fail_at >= 0 && allocation_calls++ == fail_at)
        return NULL;
    return __real_malloc(size);
}

void *
__wrap_calloc(size_t count, size_t size)
{
    if (fail_at >= 0 && allocation_calls++ == fail_at)
        return NULL;
    return __real_calloc(count, size);
}

void *
__wrap_realloc(void *ptr, size_t size)
{
    if (fail_at >= 0 && allocation_calls++ == fail_at)
        return NULL;
    return __real_realloc(ptr, size);
}

static struct wirelog_program *
make_program(const char *source, wl_parser_ast_node_t **out_ast)
{
    char error[512] = {0};
    wl_parser_ast_node_t *ast
        = wl_parser_parse_string(source, error, sizeof(error));
    if (!ast)
        return NULL;

    struct wirelog_program *program = wl_ir_program_create();
    if (!program) {
        wl_parser_ast_node_free(ast);
        return NULL;
    }
    program->ast = ast;
    if (wl_ir_program_collect_metadata(program, ast) != 0) {
        wl_ir_program_free(program);
        return NULL;
    }
    *out_ast = ast;
    return program;
}

static bool
contains_type(const wirelog_ir_node_t *node, wirelog_ir_node_type_t type)
{
    if (!node)
        return false;
    if (node->type == type)
        return true;
    for (uint32_t i = 0; i < node->child_count; i++) {
        if (contains_type(node->children[i], type))
            return true;
    }
    return false;
}

static bool
valid_expr(const wl_ir_expr_t *expr)
{
    if (!expr)
        return false;
    if (expr->type == WL_IR_EXPR_VAR && !expr->var_name)
        return false;
    if (expr->type == WL_IR_EXPR_CONST_STR && !expr->str_value)
        return false;
    if (expr->type == WL_IR_EXPR_CMP && expr->child_count != 2)
        return false;
    for (uint32_t i = 0; i < expr->child_count; i++) {
        if (!valid_expr(expr->children[i]))
            return false;
    }
    return true;
}

static bool
valid_ir(const wirelog_ir_node_t *node)
{
    if (!node)
        return false;
    if (node->type == WIRELOG_IR_FILTER
        && !valid_expr(node->filter_expr))
        return false;
    if (node->type == WIRELOG_IR_PROJECT) {
        if (node->project_count > 0 && !node->project_exprs)
            return false;
        for (uint32_t i = 0; i < node->project_count; i++) {
            if (!valid_expr(node->project_exprs[i]))
                return false;
        }
    }
    if (node->type == WIRELOG_IR_AGGREGATE) {
        if (!valid_expr(node->agg_expr))
            return false;
        if (node->group_by_count > 0 && !node->group_by_indices)
            return false;
    }
    for (uint32_t i = 0; i < node->child_count; i++) {
        if (!valid_ir(node->children[i]))
            return false;
    }
    return true;
}

static bool
contains_expr_type(const wirelog_ir_node_t *node, wl_ir_expr_type_t type)
{
    if (!node)
        return false;
    if (node->filter_expr && node->filter_expr->type == type)
        return true;
    if (node->project_exprs) {
        for (uint32_t i = 0; i < node->project_count; i++) {
            if (node->project_exprs[i] && node->project_exprs[i]->type == type)
                return true;
        }
    }
    if (node->agg_expr && node->agg_expr->type == type)
        return true;
    for (uint32_t i = 0; i < node->child_count; i++) {
        if (contains_expr_type(node->children[i], type))
            return true;
    }
    return false;
}

static int
sweep_conversion(const char *source, wirelog_ir_node_type_t expected_a,
    wirelog_ir_node_type_t expected_b, int expected_expr_type)
{
    bool saw_success = false;
    bool saw_injected_failure = false;
    for (long attempt = 0; attempt < 512; attempt++) {
        wl_parser_ast_node_t *ast = NULL;
        struct wirelog_program *program = make_program(source, &ast);
        if (!program)
            return 1;

        fail_at = attempt;
        allocation_calls = 0;
        int rc = wl_ir_program_convert_rules(program, ast);
        bool injected = allocation_calls > attempt;
        if (injected && rc == 0) {
            fprintf(stderr,
                "issue #1165: allocation failure was accepted at %ld\n",
                attempt);
            wl_ir_program_free(program);
            return 1;
        }
        if (injected && program->rules[0].ir_root) {
            fprintf(stderr,
                "issue #1165: rejected conversion retained an IR root at "
                "allocation %ld\n", attempt);
            wl_ir_program_free(program);
            return 1;
        }
        if (injected)
            saw_injected_failure = true;
        fail_at = -1;
        if (rc == 0) {
            wirelog_ir_node_t *root = program->rules[0].ir_root;
            if (!root || !valid_ir(root) || !contains_type(root, expected_a)
                || !contains_type(root, expected_b)
                || (expected_expr_type >= 0
                && !contains_expr_type(root,
                (wl_ir_expr_type_t)expected_expr_type))) {
                fprintf(stderr,
                    "issue #1165: incomplete IR accepted at allocation %ld\n",
                    attempt);
                wl_ir_program_free(program);
                return 1;
            }
            saw_success = true;
        }
        wl_ir_program_free(program);
    }
    return saw_success && saw_injected_failure ? 0 : 1;
}

static int
sweep_compound_metadata(void)
{
    static const char source[]
        = ".decl item(key: int64, payload: pair/2 inline)\n";
    bool saw_success = false;
    bool saw_injected_failure = false;
    for (long attempt = 0; attempt < 512; attempt++) {
        char error[512] = {0};
        wl_parser_ast_node_t *ast
            = wl_parser_parse_string(source, error, sizeof(error));
        if (!ast)
            return 1;
        struct wirelog_program *program = wl_ir_program_create();
        if (!program) {
            wl_parser_ast_node_free(ast);
            return 1;
        }
        program->ast = ast;
        fail_at = attempt;
        allocation_calls = 0;
        int rc = wl_ir_program_collect_metadata(program, ast);
        if (allocation_calls > attempt)
            saw_injected_failure = true;
        fail_at = -1;
        if (rc == 0) {
            if (program->relation_count != 1
                || program->relations[0].columns[1].compound_kind
                == WIRELOG_COMPOUND_KIND_NONE) {
                fprintf(stderr,
                    "issue #1143: compound metadata was weakened at %ld\n",
                    attempt);
                wl_ir_program_free(program);
                return 1;
            }
            saw_success = true;
        }
        wl_ir_program_free(program);
    }
    return saw_success && saw_injected_failure ? 0 : 1;
}

int
main(void)
{
    static const char constant_source[]
        = ".decl item(key: int64)\n"
        ".decl out(key: int64)\n"
        "out(key) :- item(key), key = 7.\n";
    static const char join_source[]
        = ".decl left(key: int64)\n"
        ".decl right(key: int64)\n"
        ".decl out(key: int64)\n"
        "out(key) :- left(key), right(key).\n";
    static const char anti_join_source[]
        = ".decl item(x: int64)\n"
        ".decl blocked(x: int64)\n"
        ".decl out(x: int64)\n"
        "out(x) :- item(x), !blocked(x).\n";
    static const char false_filter_source[]
        = ".decl item(key: int64, payload: pair/2 inline)\n"
        ".decl out(key: int64)\n"
        "out(key) :- item(key, wrong(a, b)).\n";
    static const char explicit_filter_source[]
        = ".decl item(key: int64)\n"
        ".decl out(key: int64)\n"
        "out(key) :- item(key), key > 0.\n";
    static const char boolean_filter_source[]
        = ".decl item(value: int64)\n"
        ".decl out(value: int64)\n"
        "out(value) :- item(value), False.\n";
    static const char string_filter_source[]
        = ".decl item(value: string)\n"
        ".decl out(value: string)\n"
        "out(value) :- item(value), contains(value, \"x\").\n";
    static const char aggregate_source[]
        = ".decl item(group: int64, value: int64)\n"
        ".decl out(group: int64, value: int64)\n"
        "out(group, min(value + 1)) :- item(group, value).\n";
    static const char projection_source[]
        = ".decl item(key: int64)\n"
        ".decl out(key: int64, next: int64)\n"
        "out(key, key + 1) :- item(key).\n";

    if (sweep_conversion(constant_source, WIRELOG_IR_FILTER,
        WIRELOG_IR_SCAN, WL_IR_EXPR_CMP) != 0)
        return 1;
    if (sweep_conversion(join_source, WIRELOG_IR_JOIN,
        WIRELOG_IR_SCAN, -1) != 0)
        return 1;
    if (sweep_conversion(anti_join_source, WIRELOG_IR_ANTIJOIN,
        WIRELOG_IR_SCAN, -1) != 0)
        return 1;
    if (sweep_conversion(false_filter_source, WIRELOG_IR_FILTER,
        WIRELOG_IR_SCAN, WL_IR_EXPR_BOOL) != 0)
        return 1;
    if (sweep_conversion(explicit_filter_source, WIRELOG_IR_FILTER,
        WIRELOG_IR_SCAN, WL_IR_EXPR_CMP) != 0)
        return 1;
    if (sweep_conversion(boolean_filter_source, WIRELOG_IR_FILTER,
        WIRELOG_IR_SCAN, WL_IR_EXPR_BOOL) != 0)
        return 1;
    if (sweep_conversion(string_filter_source, WIRELOG_IR_FILTER,
        WIRELOG_IR_SCAN, WL_IR_EXPR_STR_FN) != 0)
        return 1;
    if (sweep_conversion(aggregate_source, WIRELOG_IR_AGGREGATE,
        WIRELOG_IR_SCAN, WL_IR_EXPR_ARITH) != 0)
        return 1;
    if (sweep_conversion(projection_source, WIRELOG_IR_PROJECT,
        WIRELOG_IR_SCAN, WL_IR_EXPR_ARITH) != 0)
        return 1;
    return sweep_compound_metadata();
}
