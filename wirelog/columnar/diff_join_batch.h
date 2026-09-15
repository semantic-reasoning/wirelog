/*
 * columnar/diff_join_batch.h - bounded differential keyed-join producer
 *
 * INTERNAL HEADER - not installed.
 */
#ifndef WL_COLUMNAR_DIFF_JOIN_BATCH_H
#define WL_COLUMNAR_DIFF_JOIN_BATCH_H

#include "columnar/join_batch.h"

/* Create a resumable producer over a fully indexed persistent differential
 * arrangement.  The arrangement is incrementally indexed and committed
 * before the producer is returned; the producer then holds a generation pin
 * for its entire continuation lifetime. */
int
col_diff_join_batch_producer_create(wl_col_session_t *sess,
    const wl_plan_op_t *op, const col_rel_t *left, bool left_is_delta,
    const uint32_t *lk, const uint32_t *rk, uint32_t kc,
    uint64_t batch_bytes, wl_columnar_continuation_t **out);

#endif /* WL_COLUMNAR_DIFF_JOIN_BATCH_H */
