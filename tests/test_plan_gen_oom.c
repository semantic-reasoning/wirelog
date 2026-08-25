/* Regression coverage for issue #1102: planner clone failures are atomic. */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

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

int
main(void)
{
    static const char source[]
        = ".decl a(x: int64, value: int64)\n"
        ".decl b(x: int64, value: int64)\n"
        ".decl c(x: int64, value: int64)\n"
        ".decl guard(q: int64)\n"
        ".decl out(x: int64, av: int64, bv: int64, cv: int64)\n"
        "out(x, av, bv, cv) :- a(x, av), b(x, bv), c(x, cv), guard(7).\n";
    bool saw_success = false;
    bool saw_injected_failure = false;
    bool saw_lftj = false;

    for (long attempt = 0; attempt < 512; attempt++) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        if (!program) {
            fprintf(stderr, "parse failed before allocation sweep\n");
            return 1;
        }
        /* Keep the consecutive EDB JOIN chain intact so the planner's LFTJ
         * rewrite and subsequent clone path are exercised. */
        if (wl_fusion_apply(program, NULL) != 0) {
            wirelog_program_free(program);
            return 1;
        }

        fail_at = attempt;
        allocation_calls = 0;
        wl_plan_t *plan = NULL;
        int rc = wl_plan_from_program(program, &plan);
        bool injected = allocation_calls > attempt;
        if (injected)
            saw_injected_failure = true;
        if (rc == 0) {
            if (!plan) {
                fprintf(stderr, "successful generation returned no plan at "
                    "%ld\n", attempt);
                wirelog_program_free(program);
                return 1;
            }
            for (uint32_t s = 0; s < plan->stratum_count; s++) {
                for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
                    const wl_plan_relation_t *relation
                        = &plan->strata[s].relations[r];
                    for (uint32_t i = 0; i < relation->op_count; i++) {
                        if (relation->ops[i].op == WL_PLAN_OP_LFTJ)
                            saw_lftj = true;
                    }
                }
            }
            saw_success = true;
            wl_plan_free(plan);
        } else if (plan) {
            fprintf(stderr, "failed clone returned a partial plan at %ld\n",
                attempt);
            wl_plan_free(plan);
            wirelog_program_free(program);
            return 1;
        }
        fail_at = -1;
        wirelog_program_free(program);
    }

    return saw_success && saw_injected_failure && saw_lftj ? 0 : 1;
}
