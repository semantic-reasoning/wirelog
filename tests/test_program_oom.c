/* Regression coverage for issue #1143. */

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

static int
sweep_conversion(const char *source, wirelog_ir_node_type_t expected_a,
    wirelog_ir_node_type_t expected_b)
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
        if (allocation_calls > attempt)
            saw_injected_failure = true;
        fail_at = -1;
        if (rc == 0) {
            wirelog_ir_node_t *root = program->rules[0].ir_root;
            if (!root || !contains_type(root, expected_a)
                || !contains_type(root, expected_b)) {
                fprintf(stderr,
                    "issue #1143: incomplete IR accepted at allocation %ld\n",
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

    if (sweep_conversion(constant_source, WIRELOG_IR_FILTER,
        WIRELOG_IR_SCAN) != 0)
        return 1;
    if (sweep_conversion(join_source, WIRELOG_IR_JOIN,
        WIRELOG_IR_SCAN) != 0)
        return 1;
    return sweep_compound_metadata();
}
