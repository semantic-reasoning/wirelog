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

static void
tdd_destroy_delta_payload(void *payload)
{
    col_rel_destroy((col_rel_t *)payload);
}

static bool
tdd_matrix_contains_payload(const col_eval_tdd_worker_ctx_t *ctxs,
    uint32_t num_workers, uint32_t nrels, const void *payload)
{
    for (uint32_t w = 0; w < num_workers; w++)
        for (uint32_t ri = 0; ri < nrels; ri++)
            if (ctxs[w].delta_rels[ri] == payload)
                return true;
    return false;
}

static void
tdd_reconstruct_delta_matrix_with_destroyer(
    col_eval_tdd_worker_ctx_t *ctxs, const wl_delta_msg_t *msgs,
    uint32_t count, uint32_t num_workers, uint32_t nrels,
    wl_mpsc_payload_destroy_fn destroy_payload)
{
    for (uint32_t i = 0; i < count; i++) {
        uint32_t w = msgs[i].worker_id;
        uint32_t ri = msgs[i].rel_idx;
        void *payload = msgs[i].delta;

        if (w >= num_workers || ri >= nrels || !payload)
            continue;

        /* A pointer can be mentioned by more than one malformed message.
         * Keep its first matrix owner and reject later aliases. */
        if (tdd_matrix_contains_payload(ctxs, num_workers, nrels, payload))
            continue;
        ctxs[w].delta_rels[ri] = (col_rel_t *)payload;
    }

    /* Destroy every rejected/replaced payload exactly once.  Deferring this
     * pass handles invalid-before-valid aliases without dangling a slot. */
    for (uint32_t i = 0; i < count; i++) {
        void *payload = msgs[i].delta;
        if (!payload || tdd_matrix_contains_payload(ctxs, num_workers, nrels,
            payload))
            continue;
        bool seen = false;
        for (uint32_t j = 0; j < i; j++) {
            if (msgs[j].delta == payload) {
                seen = true;
                break;
            }
        }
        if (!seen)
            destroy_payload(payload);
    }
}

void
wl_columnar_eval_tdd_queue_reconstruct_delta_matrix(
    col_eval_tdd_worker_ctx_t *ctxs, const wl_delta_msg_t *msgs,
    uint32_t count, uint32_t num_workers, uint32_t nrels)
{
    if (!ctxs || (!msgs && count != 0))
        return;
    tdd_reconstruct_delta_matrix_with_destroyer(ctxs, msgs, count,
        num_workers, nrels, tdd_destroy_delta_payload);
}

void
wl_columnar_eval_tdd_queue_reconstruct_delta_matrix_with_destroyer(
    col_eval_tdd_worker_ctx_t *ctxs, const wl_delta_msg_t *msgs,
    uint32_t count, uint32_t num_workers, uint32_t nrels,
    wl_mpsc_payload_destroy_fn destroy_payload)
{
    if (!ctxs || (!msgs && count != 0) || !destroy_payload)
        return;
    tdd_reconstruct_delta_matrix_with_destroyer(ctxs, msgs, count,
        num_workers, nrels, destroy_payload);
}

void
wl_columnar_eval_tdd_queue_discard_delta_queue_with_destroyer(
    wl_mpsc_queue_t *queue, wl_mpsc_payload_destroy_fn destroy_payload)
{
    if (!queue || !destroy_payload)
        return;
    wl_delta_msg_t msg;
    while (wl_mpsc_dequeue(queue, &msg))
        if (msg.delta)
            destroy_payload(msg.delta);
}

void
wl_columnar_eval_tdd_queue_discard_delta_queue(wl_mpsc_queue_t *queue,
    uint32_t W, uint32_t nrels)
{
    if (!queue)
        return;
    /* This error-path drain is intentionally allocation-free and ignores the
    * historical sizing hints: every live queue message must be reclaimed. */
    (void)W;
    (void)nrels;
    wl_columnar_eval_tdd_queue_discard_delta_queue_with_destroyer(queue,
        tdd_destroy_delta_payload);
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
