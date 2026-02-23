/*
 * dd_plan.c - wirelog DD Execution Plan
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Translates stratified IR trees into DD execution plans.
 */

#include "dd_plan.h"
#include "program.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Plan Generation (stub)                                                   */
/* ======================================================================== */

int
wl_dd_plan_generate(const struct wirelog_program *prog, wl_dd_plan_t **out)
{
    if (!prog || !out)
        return -2;

    if (!prog->is_stratified)
        return -2;

    /* TODO: implement translation */
    return -1;
}

/* ======================================================================== */
/* Cleanup                                                                  */
/* ======================================================================== */

void
wl_dd_plan_free(wl_dd_plan_t *plan)
{
    if (!plan)
        return;

    /* TODO: implement full cleanup */
    free(plan);
}

/* ======================================================================== */
/* Debug Utilities                                                          */
/* ======================================================================== */

const char *
wl_dd_op_type_str(wl_dd_op_type_t type)
{
    switch (type) {
    case WL_DD_VARIABLE:
        return "VARIABLE";
    case WL_DD_MAP:
        return "MAP";
    case WL_DD_FILTER:
        return "FILTER";
    case WL_DD_JOIN:
        return "JOIN";
    case WL_DD_ANTIJOIN:
        return "ANTIJOIN";
    case WL_DD_REDUCE:
        return "REDUCE";
    case WL_DD_CONCAT:
        return "CONCAT";
    case WL_DD_CONSOLIDATE:
        return "CONSOLIDATE";
    }
    return "UNKNOWN";
}

void
wl_dd_plan_print(const wl_dd_plan_t *plan)
{
    if (!plan) {
        printf("DD Plan: (null)\n");
        return;
    }

    printf("DD Plan: %u strata, %u EDB relations\n", plan->stratum_count,
           plan->edb_count);

    for (uint32_t i = 0; i < plan->edb_count; i++)
        printf("  EDB: %s\n", plan->edb_relations[i]);

    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        wl_dd_stratum_plan_t *sp = &plan->strata[s];
        printf("  Stratum %u [%s] (%u relations):\n", sp->stratum_id,
               sp->is_recursive ? "recursive" : "non-recursive",
               sp->relation_count);

        for (uint32_t r = 0; r < sp->relation_count; r++) {
            wl_dd_relation_plan_t *rp = &sp->relations[r];
            printf("    %s (%u ops):\n", rp->name, rp->op_count);

            for (uint32_t o = 0; o < rp->op_count; o++) {
                wl_dd_op_t *op = &rp->ops[o];
                printf("      [%u] %s", o, wl_dd_op_type_str(op->op));
                if (op->relation_name)
                    printf("(%s)", op->relation_name);
                if (op->right_relation)
                    printf(" right=%s", op->right_relation);
                printf("\n");
            }
        }
    }
}
