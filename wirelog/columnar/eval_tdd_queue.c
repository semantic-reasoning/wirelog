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
wl_columnar_eval_tdd_queue_discard_delta_queue_ledger(wl_mpsc_queue_t *queue,
    wl_mem_ledger_t *ledger)
{
    if (!queue)
        return;
    wl_delta_msg_t msg;
    while (wl_mpsc_dequeue(queue, &msg)) {
        if (!msg.delta)
            continue;
        /* Issue #1380: the payload leaves the channel here. */
        wl_mem_ledger_free(ledger, WL_MEM_SUBSYS_CHANNEL,
            col_rel_transport_bytes((const col_rel_t *)msg.delta));
        tdd_destroy_delta_payload(msg.delta);
    }
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
    wl_col_session_t *sess, col_rel_t **candidate, uint32_t rel_idx,
    uint32_t eff_iter)
{
    if (!ctx || !sess || !candidate || !*candidate)
        return EINVAL;
    col_rel_t *delta = *candidate;
    if (delta->nrows == 0) {
        int rc = col_rel_destroy_checked(delta);
        if (rc == 0)
            *candidate = NULL;
        return rc;
    }

    /* Reuse any timestamp allocation inherited from the governed copy. */
    int rc = col_rel_enable_timestamps(delta);
    if (rc != 0)
        return rc;
    wl_columnar_source_access_writer_t writer = { 0 };
    rc = col_rel_source_writer_acquire(delta, &writer);
    if (rc != 0)
        return rc;
    if (col_rel_storage_alias_borrow_count(delta) != 0) {
        rc = EBUSY;
        goto done;
    }
    for (uint32_t ti = 0; ti < delta->nrows; ti++) {
        delta->timestamps[ti].iteration = eff_iter;
        delta->timestamps[ti].stratum = ctx->stratum_idx;
        delta->timestamps[ti].worker = (uint16_t)sess->worker_id;
        delta->timestamps[ti].multiplicity = 1;
    }
    wl_columnar_relation_touch_storage(delta);

    if (sess->coordinator && sess->coordinator->delta_queue) {
        uint64_t transport_bytes = col_rel_transport_bytes(delta);
        rc = wl_mpsc_enqueue(sess->coordinator->delta_queue,
                sess->worker_id, delta, ctx->stratum_idx, rel_idx);
        if (rc != 0) {
            rc = ENOMEM;
            goto done;
        }
        *candidate = NULL;
        wl_mem_ledger_alloc(&sess->coordinator->mem_ledger,
            WL_MEM_SUBSYS_CHANNEL, transport_bytes);
    } else {
        if (ctx->delta_rels[rel_idx]) {
            rc = EBUSY;
            goto done;
        }
        ctx->delta_rels[rel_idx] = delta;
        *candidate = NULL;
    }
done:;
    /* Consumers join all workers before reading or destroying any payload.
     * Once transferred, caller ownership stays cleared even if release fails. */
    int release_rc = wl_columnar_source_access_writer_release(&writer);
    return rc != 0 ? rc : release_rc;
}
