/* Arithmetic AST ownership regression for issue #1353. */

#include "../wirelog/parser/parser.h"

#include <stdbool.h>
#include <stdio.h>

wl_parser_ast_node_t *__real_wl_parser_ast_node_create(
    wl_parser_ast_node_type_t type, uint32_t line, uint32_t col);
int __real_wl_parser_ast_node_add_child(wl_parser_ast_node_t *parent,
    wl_parser_ast_node_t *child);

static bool fail_creation;
static long fail_at = -1;
static long calls;
static bool injected;

wl_parser_ast_node_t *
__wrap_wl_parser_ast_node_create(wl_parser_ast_node_type_t type,
    uint32_t line, uint32_t col)
{
    if (fail_creation && type == WL_PARSER_AST_NODE_BINARY_EXPR
        && calls++ == fail_at) {
        injected = true;
        return NULL;
    }
    return __real_wl_parser_ast_node_create(type, line, col);
}

int
__wrap_wl_parser_ast_node_add_child(wl_parser_ast_node_t *parent,
    wl_parser_ast_node_t *child)
{
    if (!fail_creation && parent
        && parent->type == WL_PARSER_AST_NODE_BINARY_EXPR
        && calls++ == fail_at) {
        injected = true;
        return -1;
    }
    return __real_wl_parser_ast_node_add_child(parent, child);
}

static int
sweep_arithmetic(const char *source, bool creation)
{
    fail_creation = creation;
    fail_at = -1;
    calls = 0;
    injected = false;
    char error[512] = {0};
    wl_parser_ast_node_t *ast
        = wl_parser_parse_string(source, error, sizeof(error));
    long count = calls;
    bool valid = ast && error[0] == '\0' && !injected && count > 0;
    wl_parser_ast_node_free(ast);
    if (!valid) {
        fprintf(stderr, "uninjected arithmetic parse failed: %s\n", error);
        return 1;
    }

    /* Include one final uninjected run to prove the sweep terminates normally. */
    for (long attempt = 0; attempt <= count; attempt++) {
        calls = 0;
        fail_at = attempt;
        injected = false;
        error[0] = '\0';
        ast = wl_parser_parse_string(source, error, sizeof(error));
        fail_at = -1;
        valid = attempt < count
            ? injected && !ast && error[0] != '\0'
            : !injected && ast && error[0] == '\0' && calls == count;
        wl_parser_ast_node_free(ast);
        if (!valid) {
            fprintf(stderr, "arithmetic %s failure %ld/%ld was mishandled\n",
                creation ? "creation" : "attachment", attempt, count);
            return 1;
        }
    }
    printf("arithmetic %s: %ld injected failures rejected and control passed\n",
        creation ? "creation" : "attachment", count);
    return 0;
}

int
main(void)
{
    static const char *sources[] = {
        "r(x + y * z - u / v % w) :- a(x, y, z, u, v, w).",
        "r(x) :- a(x, y, z), x + y * z = x - y / z.",
        "r(sum(x + y * z)) :- a(x, y, z).",
        "r(x) :- a(x, y, z), 8 + y * z = x + y % z.",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        if (sweep_arithmetic(sources[i], true) != 0
            || sweep_arithmetic(sources[i], false) != 0)
            return 1;
    }
    return 0;
}
