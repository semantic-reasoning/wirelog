/* Regression test for issue #1129. */

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/ir/stratify.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/parser/parser.h"
#include "../wirelog/wirelog-parser.h"

void *__real_malloc(size_t size);

static long fail_at = -1;
static long malloc_calls;

void *
__wrap_malloc(size_t size)
{
    if (fail_at >= 0 && malloc_calls++ == fail_at)
        return NULL;
    return __real_malloc(size);
}

static wirelog_ir_node_t *
find_relation(wirelog_ir_node_t *node, const char *name)
{
    if (!node)
        return NULL;
    if (node->type == WIRELOG_IR_SCAN && node->relation_name
        && strcmp(node->relation_name, name) == 0)
        return node;
    for (uint32_t i = 0; i < node->child_count; i++) {
        wirelog_ir_node_t *found = find_relation(node->children[i], name);
        if (found)
            return found;
    }
    return NULL;
}

static wirelog_ir_node_t *
find_join(wirelog_ir_node_t *node)
{
    if (!node)
        return NULL;
    if (node->type == WIRELOG_IR_JOIN && node->join_key_count > 0)
        return node;
    for (uint32_t i = 0; i < node->child_count; i++) {
        wirelog_ir_node_t *found = find_join(node->children[i]);
        if (found)
            return found;
    }
    return NULL;
}

static bool
has_name(const wirelog_ir_node_t *scan, const char *name)
{
    if (!scan || !scan->column_names)
        return false;
    for (uint32_t i = 0; i < scan->column_count; i++) {
        if (scan->column_names[i] && strcmp(scan->column_names[i], name) == 0)
            return true;
    }
    return false;
}

static bool
plan_has_expected_join(const wl_plan_t *plan)
{
    if (!plan)
        return false;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *stratum = &plan->strata[s];
        for (uint32_t r = 0; r < stratum->relation_count; r++) {
            const wl_plan_relation_t *relation = &stratum->relations[r];
            for (uint32_t i = 0; i < relation->op_count; i++) {
                const wl_plan_op_t *op = &relation->ops[i];
                if (op->op != WL_PLAN_OP_JOIN || op->key_count != 1
                    || !op->left_keys || !op->right_keys)
                    continue;
                if (strcmp(op->left_keys[0], "col1") == 0
                    && strcmp(op->right_keys[0], "col0") == 0)
                    return true;
            }
        }
    }
    return false;
}

int
main(void)
{
    static const char source[] =
        ".decl left(a: int64, y: int64)\n"
        ".decl right(y: int64, z: int64)\n"
        ".decl out(a: int64, z: int64)\n"
        "out(a, z) :- left(a, y), right(y, z).\n";
    bool saw_success = false;

    /* The fixture is small, but allow room for parser and IR allocations. */
    for (long attempt = 0; attempt < 512; attempt++) {
        char parse_error[512] = { 0 };
        wl_parser_ast_node_t *ast = wl_parser_parse_string(source,
                parse_error, sizeof(parse_error));
        if (!ast)
            return 1;
        struct wirelog_program *program = wl_ir_program_create();
        if (!program)
            return 1;
        program->ast = ast;
        if (wl_ir_program_collect_metadata(program, ast) != 0) {
            wirelog_program_free(program);
            return 1;
        }

        fail_at = attempt;
        malloc_calls = 0;
        int convert_rc = wl_ir_program_convert_rules(program, ast);
        fail_at = -1;
        if (convert_rc != 0) {
            wirelog_program_free(program);
            continue; /* A required allocation failed: rejection is valid. */
        }
        if (wl_ir_program_merge_unions(program) != 0) {
            wirelog_program_free(program);
            return 1;
        }
        wl_ir_program_build_schemas(program);
        if (wl_ir_stratify_program(program) != 0) {
            wirelog_program_free(program);
            return 1;
        }
        wl_plan_t *plan = NULL;
        if (wl_plan_from_program(program, &plan) != 0) {
            wl_plan_free(plan);
            wirelog_program_free(program);
            continue; /* unresolved layouts must reject plan generation. */
        }
        if (!plan_has_expected_join(plan)) {
            wl_plan_free(plan);
            wirelog_program_free(program);
            fprintf(stderr,
                "issue #1129: plan accepted an unexpected join layout at %ld\n",
                attempt);
            return 1;
        }
        wl_plan_free(plan);
        saw_success = true;
        wirelog_ir_node_t *out = (wirelog_ir_node_t *)
            wirelog_program_get_relation_ir(program, "out");
        wirelog_ir_node_t *join = find_join(out);
        wirelog_ir_node_t *left = find_relation(out, "left");
        wirelog_ir_node_t *right = find_relation(out, "right");
        bool valid = join && join->join_key_count == 1
            && join->join_left_keys && join->join_right_keys
            && join->join_left_keys[0]
            && join->join_right_keys[0]
            && strcmp(join->join_left_keys[0], "y") == 0
            && strcmp(join->join_right_keys[0], "y") == 0
            && has_name(left, "y") && has_name(right, "y");
        if (!valid) {
            fprintf(stderr,
                "issue #1129: partial name allocation was accepted at %ld\n",
                attempt);
            wirelog_program_free(program);
            return 1;
        }
        wirelog_program_free(program);
    }

    fail_at = -1;
    if (!saw_success) {
        fprintf(stderr, "issue #1129: OOM sweep never reached a valid parse\n");
        return 1;
    }
    return 0;
}
