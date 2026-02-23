/*
 * stratify.c - wirelog Stratification & SCC Detection
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements dependency graph construction, Tarjan's SCC detection,
 * and stratification for Datalog programs with negation.
 */

#include "stratify.h"
#include "program.h"

#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Dependency Graph                                                         */
/* ======================================================================== */

wl_dep_graph_t *
wl_dep_graph_build(const struct wirelog_program *prog)
{
    (void)prog;
    return NULL; /* STUB: to be implemented */
}

void
wl_dep_graph_free(wl_dep_graph_t *graph)
{
    if (!graph)
        return;
    free(graph->relation_map);
    free(graph->edges);
    free(graph);
}

/* ======================================================================== */
/* SCC Detection                                                            */
/* ======================================================================== */

wl_scc_result_t *
wl_scc_detect(const wl_dep_graph_t *graph)
{
    (void)graph;
    return NULL; /* STUB: to be implemented */
}

void
wl_scc_free(wl_scc_result_t *result)
{
    if (!result)
        return;
    free(result->scc_id);
    free(result);
}

/* ======================================================================== */
/* Stratification Pipeline                                                  */
/* ======================================================================== */

int
wl_program_stratify(struct wirelog_program *program)
{
    (void)program;
    return -1; /* STUB: to be implemented */
}
