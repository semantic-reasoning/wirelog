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

/* Conservative preflight for the first continuation consumer.  Projection
 * MAPs are row-local without consulting intern or extension/session state;
 * expression MAPs remain excluded until their transactional sink exists. */
wl_columnar_join_pipeline_eligibility_t
wl_columnar_join_pipeline_preflight(const wl_plan_relation_t *plan,
    uint32_t join_index, const wl_col_session_t *sess, uint32_t *map_index);

const char *
wl_columnar_join_pipeline_eligibility_name(
    wl_columnar_join_pipeline_eligibility_t reason);

#endif /* WL_COLUMNAR_JOIN_PIPELINE_H */
