/* Internal JOIN pipeline: a JOIN followed by its longest consumable prefix
 * of row-local FILTERs and SEMIJOIN/ANTIJOIN probe stages, optionally ended
 * by one row-local MAP (Issues #1475, #1370). */
#ifndef WL_COLUMNAR_JOIN_PIPELINE_H
#define WL_COLUMNAR_JOIN_PIPELINE_H

#include "columnar/internal.h"

typedef enum {
    WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE = 0,
    WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_OFF,
    WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION,
    WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN,
    WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE,
    WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER,
    WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP,
} wl_columnar_join_pipeline_eligibility_t;

/* Conservative preflight for the continuation consumer.  Projection MAPs
 * and numeric expression MAPs (Issue #1777) are row-local without
 * consulting intern or extension/session state; string, digest, UUID,
 * aggregate and extension expressions remain excluded until their ownership
 * and retry contracts are defined.  A SEMIJOIN or ANTIJOIN stage passes
 * this static check when it is keyed, unfiltered, not materialized and
 * reads a relation whose name does not start with '$'; the run-time checks
 * follow in wl_columnar_join_pipeline_try_eval.  Returns ELIGIBLE and
 * *@tail_end (the last consumed operator) when at least one operator after
 * the JOIN is consumable; a MAP ends the prefix.  Otherwise it returns why
 * the first operator after the JOIN is not consumed and sets *@tail_end to
 * UINT32_MAX. */
wl_columnar_join_pipeline_eligibility_t
wl_columnar_join_pipeline_preflight(const wl_plan_relation_t *plan,
    uint32_t join_index, const wl_col_session_t *sess, uint32_t *tail_end);

const char *
wl_columnar_join_pipeline_eligibility_name(
    wl_columnar_join_pipeline_eligibility_t reason);

/* Why wl_columnar_join_pipeline_try_eval returned 0. */
typedef enum {
    /* An errno or 1 was returned. */
    WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NONE = 0,
    /* The session, the JOIN or the shape of its tail is not consumable;
     * what consumes the JOIN's output decides what that costs. */
    WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NOT_ELIGIBLE,
    /* A missing right relation, a forced-empty right operand or a key
     * outside a zero-column input: the ordinary JOIN's result stands. */
    WL_COLUMNAR_JOIN_PIPELINE_DECLINE_ORDINARY,
    /* The JOIN reads a $d$ or $r$ right operand. */
    WL_COLUMNAR_JOIN_PIPELINE_DECLINE_DELTA_RIGHT,
} wl_columnar_join_pipeline_decline_t;

/* What consumes the stack entry a JOIN (or its consumed tail) publishes. */
typedef enum {
    /* The entry reaches the end of the plan through CONCAT, CONSOLIDATE or
     * an EXCHANGE that partitions across more than one worker. */
    WL_COLUMNAR_JOIN_SUCCESSOR_RELATION_SINK = 0,
    /* Another operator reads it, or an operator the classifier does not
     * model is reached. */
    WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING,
} wl_columnar_join_successor_t;

/* Follow the entry published just before @first_unconsumed through the
 * rest of @plan.  Pure: reads only the plan, including the worker count of
 * an EXCHANGE, which is a no-op for one worker and consumes the entry for
 * more than one. */
wl_columnar_join_successor_t
wl_columnar_join_pipeline_successor(const wl_plan_relation_t *plan,
    uint32_t first_unconsumed);

/* Consume an eligible JOIN and the tail wl_columnar_join_pipeline_preflight
 * accepts, directly from a bounded join continuation.  Returns 0 when the
 * shape is not eligible, 1 when the pipeline was evaluated and pushed, or an
 * errno value when an eligible pipeline could not be completed.  The caller
 * may then restore the original stack entry and use the ordinary join path.
 * The JOIN is consumed only when its right-operand selection resolves to the
 * persistent relation and its key columns resolve in range on both sides;
 * otherwise 0 is returned with the stack unchanged.  Incompatible key types
 * return EINVAL before the left input is popped.  When 0 is returned and
 * @decline is non-NULL, *@decline says why; it is NONE whenever an errno or
 * 1 is returned. */
int
wl_columnar_join_pipeline_try_eval(const wl_plan_relation_t *plan,
    uint32_t join_index, eval_stack_t *stack, wl_col_session_t *sess,
    uint32_t *next_index, wl_columnar_join_pipeline_decline_t *decline);

#endif /* WL_COLUMNAR_JOIN_PIPELINE_H */
