/*
 * exec_plan_gen.h - wirelog Plan Generator from Parsed Program
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Converts a parsed+stratified wirelog_program_t into a wl_plan_t
 * execution plan for any backend (columnar, etc.).
 *
 * Replaces the deleted DD plan generation path
 * (wl_dd_plan_generate + wl_dd_marshal_plan).
 */

#ifndef WL_EXEC_PLAN_GEN_H
#define WL_EXEC_PLAN_GEN_H

#include "exec_plan.h"

/* Forward declaration (opaque in public header) */
struct wirelog_program;

/**
 * wl_plan_from_program:
 * @prog: Parsed, stratified program (must not be NULL).
 * @out:  (out) Pointer to the newly created execution plan on success.
 *
 * Convert a parsed+stratified wirelog_program_t into a wl_plan_t
 * execution plan.  The plan is fully owned by the caller and must
 * be freed with wl_plan_free().
 *
 * IR-to-plan translation rules:
 *   SCAN      -> WL_PLAN_OP_VARIABLE
 *   PROJECT   -> WL_PLAN_OP_MAP
 *   FILTER    -> WL_PLAN_OP_FILTER
 *   JOIN      -> WL_PLAN_OP_JOIN
 *   ANTIJOIN  -> WL_PLAN_OP_ANTIJOIN
 *   SEMIJOIN  -> WL_PLAN_OP_SEMIJOIN
 *   AGGREGATE -> WL_PLAN_OP_REDUCE
 *   UNION     -> WL_PLAN_OP_CONCAT + WL_PLAN_OP_CONSOLIDATE
 *   FLATMAP   -> WL_PLAN_OP_FILTER + WL_PLAN_OP_MAP
 *
 * Returns:
 *    0 on success.
 *   -1 on error (NULL args, memory allocation failure, or
 *      unrecognized IR node type).
 */
int
wl_plan_from_program(const struct wirelog_program *prog, wl_plan_t **out);

/**
 * wl_plan_free:
 * @plan: (transfer full): Execution plan to free (NULL-safe).
 *
 * Free a wl_plan_t and all nested allocations (strata, relations,
 * operators, expression buffers, string copies, key arrays).
 */
void
wl_plan_free(wl_plan_t *plan);

/* ======================================================================== */
/* Plan Segment Split (Issue #316)                                          */
/* ======================================================================== */

/**
 * wl_plan_segment_array_t:
 *
 * Array of plan segments produced by col_plan_split_at_exchange().
 * Each segment is a non-owning view into the original relation plan's
 * op array (ops and name are NOT copies; do not free them).
 *
 * @num_segments: Number of segments (0 on allocation failure).
 * @segments:     Owned array of wl_plan_relation_t views (free with
 *                wl_plan_segment_array_free()).
 */
typedef struct {
    uint32_t num_segments;
    wl_plan_relation_t *segments; /* owned array; ops/name point into original */
} wl_plan_segment_array_t;

/**
 * col_plan_split_at_exchange:
 * Split @rplan at each WL_PLAN_OP_EXCHANGE boundary.
 * N exchanges → N+1 segments; each EXCHANGE op is consumed (not included
 * in any segment).  Returns {0, NULL} on allocation failure.
 */
wl_plan_segment_array_t
col_plan_split_at_exchange(const wl_plan_relation_t *rplan);

/**
 * wl_plan_segment_array_free:
 * Free the segment array allocated by col_plan_split_at_exchange().
 * NULL-safe.
 */
void
wl_plan_segment_array_free(wl_plan_segment_array_t *sa);

#endif /* WL_EXEC_PLAN_GEN_H */
