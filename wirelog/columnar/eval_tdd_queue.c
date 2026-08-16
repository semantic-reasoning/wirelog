/*
 * columnar/eval_tdd_queue.c - TDD delta transport helpers
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#include "columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>

void
wl_columnar_eval_tdd_queue_reconstruct_delta_matrix(
    col_eval_tdd_worker_ctx_t *ctxs, const wl_delta_msg_t *msgs,
    uint32_t count, uint32_t num_workers, uint32_t nrels)
{
    for (uint32_t i = 0; i < count; i++) {
        uint32_t w = msgs[i].worker_id;
        uint32_t ri = msgs[i].rel_idx;

        if (w >= num_workers || ri >= nrels) {
            col_rel_destroy((col_rel_t *)msgs[i].delta);
            continue;
        }

        ctxs[w].delta_rels[ri] = (col_rel_t *)msgs[i].delta;
    }
}

void
wl_columnar_eval_tdd_queue_discard_delta_queue(wl_mpsc_queue_t *queue,
    uint32_t W, uint32_t nrels)
{
    if (!queue)
        return;
    uint32_t max_msgs = W * nrels;
    if (max_msgs == 0)
        max_msgs = 1;
    wl_delta_msg_t *msgs = (wl_delta_msg_t *)calloc(max_msgs,
            sizeof(wl_delta_msg_t));
    if (!msgs)
        return;
    uint32_t msg_count = wl_mpsc_dequeue_all(queue, msgs, max_msgs);
    for (uint32_t i = 0; i < msg_count; i++)
        col_rel_destroy((col_rel_t *)msgs[i].delta);
    free(msgs);
}

int
wl_columnar_eval_tdd_queue_publish_delta(col_eval_tdd_worker_ctx_t *ctx,
    wl_col_session_t *sess, col_rel_t *delta, uint32_t rel_idx,
    uint32_t eff_iter)
{
    if (delta->nrows == 0) {
        col_rel_destroy(delta);
        return 0;
    }

    delta->timestamps = (col_delta_timestamp_t *)calloc(
        delta->nrows, sizeof(col_delta_timestamp_t));
    if (!delta->timestamps) {
        col_rel_destroy(delta);
        return ENOMEM;
    }
    for (uint32_t ti = 0; ti < delta->nrows; ti++) {
        delta->timestamps[ti].iteration = eff_iter;
        delta->timestamps[ti].stratum = ctx->stratum_idx;
        delta->timestamps[ti].worker = (uint16_t)sess->worker_id;
        delta->timestamps[ti].multiplicity = 1;
    }

    if (sess->coordinator && sess->coordinator->delta_queue) {
        int rc = wl_mpsc_enqueue(sess->coordinator->delta_queue,
                sess->worker_id, delta, ctx->stratum_idx, rel_idx);
        if (rc != 0) {
            col_rel_destroy(delta);
            return ENOMEM;
        }
    } else {
        ctx->delta_rels[rel_idx] = delta;
    }
    return 0;
}
