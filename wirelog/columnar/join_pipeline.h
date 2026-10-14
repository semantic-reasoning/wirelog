/* Internal JOIN -> FILTER* -> MAP pipeline preflight (Issue #1475). */
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
 * and numeric expression MAPs (Issue #1777) are row-local without consulting
 * intern or extension/session state; string, digest, UUID, aggregate and
 * extension expressions remain excluded until their ownership and retry
 * contracts are defined. */
wl_columnar_join_pipeline_eligibility_t
wl_columnar_join_pipeline_preflight(const wl_plan_relation_t *plan,
    uint32_t join_index, const wl_col_session_t *sess, uint32_t *map_index);

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

/* Consume an eligible JOIN -> FILTER* -> MAP pipeline directly
 * from a bounded join continuation.  Returns 0 when the shape is not
 * eligible, 1 when the pipeline was evaluated and pushed, or an errno value
 * when an eligible pipeline could not be completed.  The caller may then
 * restore the original stack entry and use the ordinary join path.
 * The JOIN is consumed only when its right-operand selection resolves to
 * the persistent relation and its key columns resolve in range on both
 * sides; otherwise 0 is returned with the stack unchanged.  Incompatible
 * key types return EINVAL before the left input is popped.
 * When 0 is returned and @decline is non-NULL, *@decline says why; it is
 * NONE whenever an errno or 1 is returned. */
int
wl_columnar_join_pipeline_try_eval(const wl_plan_relation_t *plan,
    uint32_t join_index, eval_stack_t *stack, wl_col_session_t *sess,
    uint32_t *next_index, wl_columnar_join_pipeline_decline_t *decline);

#endif /* WL_COLUMNAR_JOIN_PIPELINE_H */
