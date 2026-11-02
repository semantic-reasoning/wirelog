/*
 * columnar/eval_tdd_channel.c - relation adapters for the TDD channel
 * (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * See eval_tdd_channel.h.  Everything here runs on the coordinator thread
 * and touches only CONSUMER descriptors, so no channel lock is taken except
 * inside wl_tdd_channel_retire().
 */
#include "columnar/eval_tdd_channel.h"

#include "columnar/internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

int
wl_columnar_eval_tdd_spill_errno(wl_columnar_spill_status_t status)
{
    switch (status) {
    case WL_COLUMNAR_SPILL_OK:
        return 0;
    case WL_COLUMNAR_SPILL_MEMORY:
        return ENOSPC;
    case WL_COLUMNAR_SPILL_ALLOCATION:
        return ENOMEM;
    case WL_COLUMNAR_SPILL_IO:
    case WL_COLUMNAR_SPILL_QUOTA:
    case WL_COLUMNAR_SPILL_CAPACITY:
        return EIO;
    case WL_COLUMNAR_SPILL_OVERFLOW:
        return EOVERFLOW;
    case WL_COLUMNAR_SPILL_BUSY:
        return EBUSY;
    default:
        return ENOTSUP;
    }
}

/* A missing type array reads as INT64, as a payload schema's does. */
static wirelog_column_type_t
tdd_rel_type(const col_rel_t *rel, uint32_t col)
{
    return rel->column_types ? rel->column_types[col] : WIRELOG_TYPE_INT64;
}

int
wl_columnar_eval_tdd_batch_spill(wl_tdd_batch_t *batch,
    wl_columnar_memory_governor_ref_t *gov, wl_columnar_spill_store_t *store,
    wl_columnar_spill_status_t *status)
{
    if (status)
        *status = WL_COLUMNAR_SPILL_INVALID;
    if (!batch || !gov || !store || batch->state != WL_TDD_BATCH_CONSUMER
        || batch->kind != WL_TDD_BATCH_RESIDENT || !batch->payload)
        return EINVAL;
    col_rel_t *rel = batch->payload;
    /* take() would stamp untimestamped rows {0,0,0,+1}; a TDD delta always
     * carries its own timestamps, so refuse instead of inventing them. */
    if (!rel->timestamps)
        return EINVAL;
    uint64_t floor_bytes = 0;
    uint64_t reader_bytes = 0;
    wl_columnar_spill_status_t st = wl_columnar_batch_payload_floor(rel->ncols,
            rel->nrows, WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, &floor_bytes,
            &reader_bytes);
    wl_columnar_batch_payload_t *payload = NULL;
    /* take() frees the relation and clears the local slot on success, and
     * leaves both unchanged on every failure. */
    if (st == WL_COLUMNAR_SPILL_OK)
        st = wl_columnar_batch_payload_take(gov, store, &rel, NULL, NULL,
                &payload);
    if (status)
        *status = st;
    if (st != WL_COLUMNAR_SPILL_OK) {
        batch->flags |= WL_TDD_BATCH_FLAG_UNSPILLABLE;
        return wl_columnar_eval_tdd_spill_errno(st);
    }
    batch->payload = payload;
    batch->kind = WL_TDD_BATCH_SPILLED;
    batch->charge = floor_bytes;
    return 0;
}

int
wl_columnar_eval_tdd_batch_rehydrate(const wl_tdd_batch_t *batch,
    col_rel_t *dst, uint32_t nworkers)
{
    if (!batch || !dst || batch->kind != WL_TDD_BATCH_SPILLED
        || !batch->payload)
        return EINVAL;
    wl_columnar_batch_payload_t *payload = batch->payload;
    uint32_t ncols = 0;
    uint32_t nrows = 0;
    const wirelog_column_type_t *types =
        wl_columnar_batch_payload_schema(payload, &ncols, &nrows);
    if (!dst->timestamps || dst->ncols != ncols || (ncols && !types))
        return EINVAL;
    for (uint32_t c = 0; c < ncols; c++)
        if (tdd_rel_type(dst, c) != types[c])
            return EINVAL;
    if (nrows == 0)
        return 0;

    wl_columnar_batch_payload_reader_t *reader = NULL;
    wl_columnar_spill_status_t st =
        wl_columnar_batch_payload_acquire(payload, &reader);
    if (st != WL_COLUMNAR_SPILL_OK)
        return wl_columnar_eval_tdd_spill_errno(st);
    col_rel_append_batch_t append;
    memset(&append, 0, sizeof(append));
    int rc = col_rel_append_batch_begin(&append, dst);
    bool appending = rc == 0;
    for (uint32_t row = 0; rc == 0 && row < nrows; row++) {
        const uint64_t *lanes = NULL;
        wl_columnar_spill_metadata_t metadata;
        st = wl_columnar_batch_payload_read(reader, row, NULL, NULL, &lanes,
                &metadata);
        if (st != WL_COLUMNAR_SPILL_OK) {
            rc = wl_columnar_eval_tdd_spill_errno(st);
            break;
        }
        if (metadata.worker >= nworkers) {
            rc = EPROTO;
            break;
        }
        /* int64_t and uint64_t lanes share one object representation, and
         * C11 6.5p7 allows reading one through the other. */
        rc = col_rel_append_batch_row(&append, (const int64_t *)lanes);
        if (rc != 0)
            break;
        /* The append zeroed this row's timestamp; this thread holds the
         * relation's writer for the whole batch. */
        col_delta_timestamp_t *stamp = &dst->timestamps[dst->nrows - 1];
        stamp->iteration = metadata.iteration;
        stamp->stratum = metadata.stratum;
        stamp->worker = metadata.worker;
        stamp->multiplicity = metadata.multiplicity;
    }
    if (appending) {
        int ended = col_rel_append_batch_end(&append, rc == 0);
        if (ended != 0 && rc == 0)
            rc = ended;
    }
    wl_columnar_batch_payload_reader_release(reader);
    return rc;
}

/* Retire @batch's payload and then the descriptor.  A refused retirement
 * hands both to @sink instead; without a sink that is a contract violation. */
static void
tdd_batch_dispose(wl_tdd_channel_t *ch, wl_tdd_batch_t *batch,
    const wl_tdd_refusal_sink_t *sink)
{
    int rc = 0;
    if (batch->payload) {
        if (batch->kind == WL_TDD_BATCH_RESIDENT) {
            rc = col_rel_destroy_checked(batch->payload);
        } else {
            rc = wl_columnar_eval_tdd_spill_errno(
                wl_columnar_batch_payload_release(batch->payload));
        }
    }
    if (rc != 0) {
        if (!sink || !sink->fn)
            abort();
        sink->fn(sink->ctx, batch, rc);
        return;
    }
    batch->payload = NULL;
    wl_tdd_channel_retire(ch, batch);
}

void
wl_columnar_eval_tdd_batch_discard(wl_tdd_channel_t *ch,
    wl_tdd_batch_t *batch, const wl_tdd_refusal_sink_t *sink)
{
    if (!ch || !batch)
        return;
    batch->next = NULL;
    tdd_batch_dispose(ch, batch, sink);
}

void
wl_columnar_eval_tdd_discard_list(wl_tdd_channel_t *ch, wl_tdd_batch_t **list,
    const wl_tdd_refusal_sink_t *sink)
{
    if (!ch || !list)
        return;
    wl_tdd_batch_t *batch = *list;
    *list = NULL;
    while (batch) {
        wl_tdd_batch_t *next = batch->next;
        batch->next = NULL;
        tdd_batch_dispose(ch, batch, sink);
        batch = next;
    }
}

void
wl_columnar_eval_tdd_discard_sink(void *ctx, wl_tdd_batch_t *batch)
{
    wl_tdd_discard_ctx_t *discard = ctx;
    if (!batch)
        return;
    if (!discard)
        abort();
    batch->next = NULL;
    tdd_batch_dispose(discard->channel, batch, &discard->refusal);
}

/* Whether every row of @rel carries the same iteration, stratum and worker
 * and a multiplicity of +1. */
static bool
tdd_timestamps_uniform_insert(const col_rel_t *rel)
{
    if (rel->nrows && rel->timestamps[0].multiplicity != 1)
        return false;
    for (uint32_t row = 1; row < rel->nrows; row++) {
        const col_delta_timestamp_t *a = &rel->timestamps[0];
        const col_delta_timestamp_t *b = &rel->timestamps[row];
        if (a->iteration != b->iteration || a->stratum != b->stratum
            || a->worker != b->worker || a->multiplicity != b->multiplicity)
            return false;
    }
    return true;
}

/* Whether a batch's schema matches the slot's column count and types. */
static bool
tdd_batch_schema_matches(const wl_tdd_batch_t *batch, const col_rel_t *slot)
{
    if (batch->kind == WL_TDD_BATCH_RESIDENT) {
        const col_rel_t *rel = batch->payload;
        if (rel->ncols != slot->ncols)
            return false;
        for (uint32_t c = 0; c < slot->ncols; c++)
            if (tdd_rel_type(rel, c) != tdd_rel_type(slot, c))
                return false;
        return true;
    }
    uint32_t ncols = 0;
    uint32_t nrows = 0;
    const wirelog_column_type_t *types =
        wl_columnar_batch_payload_schema(batch->payload, &ncols, &nrows);
    if (ncols != slot->ncols || (ncols && !types))
        return false;
    for (uint32_t c = 0; c < ncols; c++)
        if (types[c] != tdd_rel_type(slot, c))
            return false;
    return true;
}

/* Create the empty slot under the coordinator's governor with timestamps
 * and room for @rows rows.  Its schema is @model's when a RESIDENT batch
 * exists, else the first payload's. */
static int
tdd_slot_create(wl_col_session_t *coord, const char *name,
    const col_rel_t *model, const wl_tdd_batch_t *head, uint32_t rows,
    col_rel_t **out)
{
    wl_columnar_memory_governor_ref_t *gov = coord->memory_governor;
    col_rel_t *slot = NULL;
    int rc;
    if (model) {
        rc = wl_columnar_relation_new_like_governed_checked_mode(
            name ? name : model->name, model, gov, true, &slot);
    } else {
        uint32_t ncols = 0;
        uint32_t nrows = 0;
        const wirelog_column_type_t *types =
            wl_columnar_batch_payload_schema(head->payload, &ncols, &nrows);
        rc = wl_columnar_relation_new_auto_governed(name ? name : "$tdd_slot",
                ncols, rows ? rows : 1, true, gov, &slot);
        if (rc == 0 && ncols)
            rc = col_rel_set_column_types(slot, types, ncols);
    }
    if (rc == 0 && !slot->timestamps)
        rc = col_rel_enable_timestamps(slot);
    if (rc == 0) {
        bool denied = false;
        rc = col_rel_reserve_capacity_admitted(slot, rows ? rows : 1, &denied);
        if (rc == ENOMEM && denied)
            rc = ENOSPC;
    }
    if (rc == ENOMEM && slot && slot->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc != 0) {
        col_rel_destroy(slot);
        return rc;
    }
    *out = slot;
    return 0;
}

int
wl_columnar_eval_tdd_coalesce_slot(wl_col_session_t *coord,
    wl_tdd_channel_t *ch, wl_tdd_batch_t **list, uint32_t nworkers,
    const char *name, const wl_tdd_refusal_sink_t *sink, col_rel_t **out_slot)
{
    if (!coord || !ch || !list || !out_slot || *out_slot || !sink
        || !sink->fn)
        return EINVAL;
    wl_tdd_batch_t *head = *list;
    if (!head)
        return 0;

    /* Validate the whole list before touching any payload. */
    const col_rel_t *model = NULL;
    uint64_t total = 0;
    uint32_t count = 0;
    uint64_t prev_seq = 0;
    for (const wl_tdd_batch_t *b = head; b; b = b->next) {
        if (b->state != WL_TDD_BATCH_CONSUMER || !b->payload
            || b->worker != head->worker || b->rel_idx != head->rel_idx
            || (b != head && b->seq <= prev_seq))
            return EINVAL;
        prev_seq = b->seq;
        uint32_t rows = 0;
        if (b->kind == WL_TDD_BATCH_RESIDENT) {
            const col_rel_t *rel = b->payload;
            if (!rel->timestamps)
                return EPROTO;
            if (!model)
                model = rel;
            rows = rel->nrows;
        } else if (b->kind == WL_TDD_BATCH_SPILLED) {
            uint32_t ncols = 0;
            (void)wl_columnar_batch_payload_schema(b->payload, &ncols, &rows);
        } else {
            return EINVAL;
        }
        total += rows;
        count++;
    }
    if (total > UINT32_MAX)
        return EOVERFLOW;

    if (count == 1 && head->kind == WL_TDD_BATCH_RESIDENT) {
        *out_slot = head->payload;
        head->payload = NULL;
        head->next = NULL;
        *list = NULL;
        wl_tdd_channel_retire(ch, head);
        return 0;
    }

    col_rel_t *slot = NULL;
    int rc = tdd_slot_create(coord, name, model, head, (uint32_t)total,
            &slot);
    for (const wl_tdd_batch_t *b = head; rc == 0 && b; b = b->next) {
        if (!tdd_batch_schema_matches(b, slot)) {
            rc = EPROTO;
        } else if (b->kind == WL_TDD_BATCH_RESIDENT) {
            rc = col_rel_append_all(slot, b->payload, NULL);
            if (rc == ENOMEM && slot->memory_budget_denial_pending)
                rc = ENOSPC;
        } else {
            rc = wl_columnar_eval_tdd_batch_rehydrate(b, slot, nworkers);
        }
    }
    /* TDD publishers stamp every batch of one (worker, relation, round)
     * slot alike with multiplicity +1, which makes a set union of them
     * sound.  Any other slot keeps every row with its own timestamp: a
     * union would merge equal signed rows and change their weight. */
    if (rc == 0 && count >= 2 && tdd_timestamps_uniform_insert(slot))
        rc = wl_columnar_eval_delta_consolidate(slot, coord);
    if (rc != 0) {
        if (rc == ENOSPC)
            coord->memory_budget_denied = true;
        col_rel_destroy(slot);
        return rc;
    }

    wl_tdd_batch_t *batch = head;
    *list = NULL;
    while (batch) {
        wl_tdd_batch_t *next = batch->next;
        batch->next = NULL;
        tdd_batch_dispose(ch, batch, sink);
        batch = next;
    }
    *out_slot = slot;
    return 0;
}
