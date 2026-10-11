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
int wl_columnar_join_pipeline_plan_validate_controlled(
    const wl_plan_relation_t *, const wl_col_session_t *,
    wl_evaluation_control_t *, bool *);
int wl_columnar_join_pipeline_preflight_controlled(const wl_plan_relation_t *,
    uint32_t, const wl_col_session_t *, uint32_t *,
    wl_columnar_join_pipeline_eligibility_t *, wl_evaluation_control_t *);

const char *
wl_columnar_join_pipeline_eligibility_name(
    wl_columnar_join_pipeline_eligibility_t reason);

/* Consume an eligible JOIN -> FILTER* -> MAP pipeline directly
 * from a bounded join continuation.  Returns 0 when the shape is not
 * eligible, 1 when the pipeline was evaluated and pushed, or an errno value
 * when an eligible pipeline could not be completed.  The caller may then
 * restore the original stack entry and use the ordinary join path. */
int
wl_columnar_join_pipeline_try_eval(const wl_plan_relation_t *plan,
    uint32_t join_index, eval_stack_t *stack, wl_col_session_t *sess,
    uint32_t *next_index);
int wl_columnar_join_pipeline_try_eval_controlled(const wl_plan_relation_t *,
    uint32_t, eval_stack_t *, wl_col_session_t *, uint32_t *,
    wl_evaluation_control_t *);
int wl_columnar_join_pipeline_publish_controlled(
    wl_columnar_join_batch_record_t *);

#endif /* WL_COLUMNAR_JOIN_PIPELINE_H */
