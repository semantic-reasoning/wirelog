/*
 * columnar/eval_tdd_channel.c - relation adapters for the TDD channel
 * (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * See eval_tdd_channel.h.  The producer helpers run on a worker's thread
 * and touch only that lane's PRODUCER descriptors; everything else runs on
 * the coordinator thread and touches only CONSUMER descriptors.  Channel
 * state is reached only through the tdd_channel.h calls, which lock.
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
    if (discard->ledger)
        wl_mem_ledger_free(discard->ledger, WL_MEM_SUBSYS_CHANNEL,
            batch->charge);
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

/* Whether @schema has the column count and types of @payload. */
static bool
tdd_schema_matches_payload(const col_rel_t *schema,
    const wl_columnar_batch_payload_t *payload)
{
    uint32_t ncols = 0;
    uint32_t nrows = 0;
    const wirelog_column_type_t *types =
        wl_columnar_batch_payload_schema(payload, &ncols, &nrows);
    if (schema->ncols != ncols || (ncols && !types))
        return false;
    for (uint32_t c = 0; c < ncols; c++)
        if (tdd_rel_type(schema, c) != types[c])
            return false;
    return true;
}

/* Create the empty slot under the coordinator's governor with timestamps
 * and room for @rows rows.  Its schema is @model's when a RESIDENT batch
 * exists, else @schema's when it matches the first payload, else the first
 * payload's column types alone. */
static int
tdd_slot_create(wl_col_session_t *coord, const char *name,
    const col_rel_t *model, const col_rel_t *schema,
    const wl_tdd_batch_t *head, uint32_t rows, col_rel_t **out)
{
    wl_columnar_memory_governor_ref_t *gov = coord->memory_governor;
    col_rel_t *slot = NULL;
    int rc;
    if (!model && schema && tdd_schema_matches_payload(schema, head->payload))
        model = schema;
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
    const char *name, const col_rel_t *schema,
    const wl_tdd_refusal_sink_t *sink, col_rel_t **out_slot)
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
    int rc = tdd_slot_create(coord, name, model, schema, head,
            (uint32_t)total, &slot);
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

#ifdef WL_SESSION_TEST_HOOKS
int (*wl_columnar_eval_test_channel_reserve)(wl_col_session_t *worker,
    uint32_t rel_idx, uint32_t rows);
#endif

/* The bytes a delta charges against the channel: its transport bytes,
 * which a stamped non-empty delta keeps above zero.  EOVERFLOW when they
 * cannot be represented. */
static int
tdd_delta_charge(const col_rel_t *delta, uint64_t *charge)
{
    *charge = col_rel_transport_bytes(delta);
    return *charge == UINT64_MAX ? EOVERFLOW : 0;
}

int
wl_columnar_eval_tdd_channel_reserve_delta(wl_col_session_t *sess,
    uint32_t rel_idx, const col_rel_t *delta, wl_tdd_batch_t **out)
{
    if (!out)
        return EINVAL;
    *out = NULL;
    if (!sess || !delta || !sess->coordinator
        || !sess->coordinator->tdd_channel)
        return EINVAL;
    wl_tdd_channel_t *ch = sess->coordinator->tdd_channel;
    wl_tdd_batch_t *batch = NULL;
    uint64_t charge = 0;
    int rc = 0;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_test_channel_reserve)
        rc = wl_columnar_eval_test_channel_reserve(sess, rel_idx,
                delta->nrows);
#endif
    if (rc == 0)
        rc = tdd_delta_charge(delta, &charge);
    if (rc == 0)
        rc = wl_tdd_channel_batch_new(ch, sess->worker_id, rel_idx, sess,
                &batch);
    if (rc == 0) {
        rc = wl_tdd_channel_reserve(ch, batch, charge);
        if (rc != 0)
            wl_tdd_channel_batch_abandon(ch, batch);
    }
    if (rc == ENOSPC)
        sess->memory_budget_denied = true;
    if (rc == 0)
        *out = batch;
    return rc;
}

/* Charge the CHANNEL ledger with @batch's reserved bytes, then queue
 * @delta on it.  The credit comes first so the consumer's free, which
 * follows its take, never runs ahead of it. */
static void
tdd_channel_publish_one(wl_col_session_t *coord, wl_tdd_batch_t *batch,
    col_rel_t *delta)
{
    wl_mem_ledger_alloc(&coord->mem_ledger, WL_MEM_SUBSYS_CHANNEL,
        batch->charge);
    wl_tdd_channel_publish(coord->tdd_channel, batch, delta, delta->nrows);
}

#ifdef WL_SESSION_TEST_HOOKS
uint32_t (*wl_columnar_eval_test_channel_split)(wl_col_session_t *worker,
    uint32_t rel_idx, uint32_t rows, bool *overlap);

/* Copy rows [@from, @to) of @src, with their timestamps, into a new
 * relation governed like the worker's deltas. */
static int
tdd_test_split_part(wl_col_session_t *sess, const col_rel_t *src,
    uint32_t from, uint32_t to, col_rel_t **out)
{
    col_rel_t *part = NULL;
    int64_t *row = NULL;
    col_rel_append_batch_t append;
    bool appending = false;
    memset(&append, 0, sizeof(append));
    int rc = wl_columnar_relation_new_like_governed_checked(&part, src->name,
            src, sess->memory_governor);
    if (rc == 0)
        rc = col_rel_enable_timestamps(part);
    if (rc == 0) {
        row = calloc(src->ncols ? src->ncols : 1u, sizeof(*row));
        if (!row)
            rc = ENOMEM;
    }
    if (rc == 0) {
        rc = col_rel_append_batch_begin(&append, part);
        appending = rc == 0;
    }
    for (uint32_t r = from; rc == 0 && r < to; r++) {
        for (uint32_t c = 0; c < src->ncols; c++)
            row[c] = col_rel_get(src, r, c);
        rc = col_rel_append_batch_row(&append, row);
        /* The append zeroed this row's timestamp; this thread holds the
         * new relation's writer for the whole batch. */
        if (rc == 0)
            part->timestamps[part->nrows - 1] = src->timestamps[r];
    }
    if (appending) {
        int ended = col_rel_append_batch_end(&append, rc == 0);
        if (ended != 0 && rc == 0)
            rc = ended;
    }
    free(row);
    if (rc != 0) {
        col_rel_destroy(part);
        return rc;
    }
    *out = part;
    return 0;
}

/* The first row of part @j when @rows rows are split into @parts parts. */
static uint32_t
tdd_test_split_bound(uint32_t rows, uint32_t parts, uint32_t j)
{
    return (uint32_t)(((uint64_t)rows * j) / parts);
}

/* Publish @delta as @parts batches: @batch carries the first range, the
 * others get descriptors of their own.  See
 * wl_columnar_eval_tdd_channel_publish_reserved(). */
static int
tdd_test_publish_split(wl_col_session_t *sess, wl_tdd_batch_t *batch,
    col_rel_t *delta, uint32_t parts, bool overlap)
{
    wl_col_session_t *coord = sess->coordinator;
    wl_tdd_channel_t *ch = coord->tdd_channel;
    col_rel_t *part[WL_TDD_CHANNEL_TEST_SPLIT_MAX] = { NULL };
    wl_tdd_batch_t *desc[WL_TDD_CHANNEL_TEST_SPLIT_MAX] = { NULL };
    uint32_t rows = delta->nrows;
    if (parts > WL_TDD_CHANNEL_TEST_SPLIT_MAX)
        parts = WL_TDD_CHANNEL_TEST_SPLIT_MAX;
    if (parts > rows)
        parts = rows;
    if (parts < 2 || !delta->timestamps) {
        tdd_channel_publish_one(coord, batch, delta);
        return 0;
    }
    int rc = 0;
    for (uint32_t j = 1; rc == 0 && j < parts; j++) {
        uint32_t from = tdd_test_split_bound(rows, parts, j);
        uint32_t to = tdd_test_split_bound(rows, parts, j + 1);
        if (overlap)
            from--;
        rc = tdd_test_split_part(sess, delta, from, to, &part[j]);
        if (rc == 0)
            rc = wl_tdd_channel_batch_new(ch, batch->worker, batch->rel_idx,
                    sess, &desc[j]);
    }
    if (rc != 0) {
        /* The split could not be prepared: publish the delta whole. */
        for (uint32_t j = 1; j < parts; j++) {
            if (desc[j])
                wl_tdd_channel_batch_abandon(ch, desc[j]);
            col_rel_destroy(part[j]);
        }
        tdd_channel_publish_one(coord, batch, delta);
        return 0;
    }
    /* @delta keeps the first range; @batch's reservation was sized for the
     * whole delta and is charged as reserved. */
    uint32_t keep = tdd_test_split_bound(rows, parts, 1);
    delta->nrows = keep;
    if (delta->sorted_nrows > keep)
        delta->sorted_nrows = keep;
    wl_columnar_relation_touch_view(delta);
    tdd_channel_publish_one(coord, batch, delta);
    for (uint32_t j = 1; j < parts; j++) {
        uint64_t charge = 0;
        if (rc == 0)
            rc = tdd_delta_charge(part[j], &charge);
        if (rc == 0)
            rc = wl_tdd_channel_reserve(ch, desc[j], charge);
        if (rc == 0) {
            tdd_channel_publish_one(coord, desc[j], part[j]);
            continue;
        }
        wl_tdd_channel_batch_abandon(ch, desc[j]);
        col_rel_destroy(part[j]);
    }
    return rc;
}
#endif

int
wl_columnar_eval_tdd_channel_publish_reserved(wl_col_session_t *sess,
    wl_tdd_batch_t *batch, col_rel_t *delta)
{
    if (!sess || !batch || !delta || !sess->coordinator
        || !sess->coordinator->tdd_channel)
        abort();
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_test_channel_split) {
        bool overlap = false;
        uint32_t parts = wl_columnar_eval_test_channel_split(sess,
                batch->rel_idx, delta->nrows, &overlap);
        if (parts >= 2)
            return tdd_test_publish_split(sess, batch, delta, parts, overlap);
    }
#endif
    tdd_channel_publish_one(sess->coordinator, batch, delta);
    return 0;
}

int
wl_columnar_eval_tdd_channel_publish_delta(col_eval_tdd_worker_ctx_t *ctx,
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

    /* The producer admitted the whole timestamp image before this call. */
    if (!delta->timestamps || delta->timestamp_capacity < delta->nrows)
        return EINVAL;
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc = col_rel_source_writer_acquire(delta, &writer);
    if (rc != 0)
        return rc;
    if (col_rel_storage_alias_borrow_count(delta) != 0) {
        rc = EBUSY;
    } else {
        for (uint32_t ti = 0; ti < delta->nrows; ti++) {
            delta->timestamps[ti].iteration = eff_iter;
            delta->timestamps[ti].stratum = ctx->stratum_idx;
            delta->timestamps[ti].worker = (uint16_t)sess->worker_id;
            delta->timestamps[ti].multiplicity = 1;
        }
        wl_columnar_relation_touch_storage(delta);
    }
    /* The writer is released before publication (#1373): once queued, the
     * payload belongs to the consumer, whose spill needs the relation free
     * of leases.  If the release fails, the candidate is still the
     * caller's. */
    int release_rc = wl_columnar_source_access_writer_release(&writer);
    if (rc != 0)
        return rc;
    if (release_rc != 0)
        return release_rc;

    wl_col_session_t *coord = sess->coordinator;
    if (coord && coord->tdd_channel) {
        wl_tdd_batch_t *batch = NULL;
        rc = wl_columnar_eval_tdd_channel_reserve_delta(sess, rel_idx, delta,
                &batch);
        if (rc != 0)
            return rc;
        *candidate = NULL;
        return wl_columnar_eval_tdd_channel_publish_reserved(sess, batch,
                   delta);
    }
    if (ctx->delta_rels[rel_idx])
        return EBUSY;
    ctx->delta_rels[rel_idx] = delta;
    *candidate = NULL;
    return 0;
}

/* The channel's error after a cancelled round, ECANCELED when it has none. */
static int
tdd_channel_cancel_rc(wl_tdd_channel_t *ch)
{
    wl_tdd_channel_stats_t stats;
    wl_tdd_channel_stats(ch, &stats);
    return stats.channel_rc != 0 ? stats.channel_rc : ECANCELED;
}

#ifdef WL_SESSION_TEST_HOOKS
int (*wl_columnar_eval_test_before_pump)(wl_tdd_channel_t *ch);
#endif

/* One pump, preceded while the round is not cancelled by the stop poll and
 * the test hook. */
static wl_tdd_pump_result_t
tdd_collect_pump(wl_tdd_channel_t *ch, bool cancelled,
    const wl_tdd_stop_t *stop, wl_tdd_batch_t **out, int *err)
{
    if (!cancelled && stop && stop->fn) {
        int stop_rc = stop->fn(stop->ctx);
        if (stop_rc != 0) {
            *out = NULL;
            *err = stop_rc;
            return WL_TDD_PUMP_ERROR;
        }
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (!cancelled && wl_columnar_eval_test_before_pump) {
        int hook_rc = wl_columnar_eval_test_before_pump(ch);
        if (hook_rc != 0) {
            *out = NULL;
            *err = hook_rc;
            return WL_TDD_PUMP_ERROR;
        }
    }
#endif
    return wl_tdd_channel_pump(ch, out, err);
}

int
wl_columnar_eval_tdd_channel_collect(wl_tdd_channel_t *ch,
    wl_mem_ledger_t *ledger, wl_tdd_batch_t **lists, uint32_t nworkers,
    uint32_t nrels, wl_tdd_batch_t **discard, uint64_t *rows,
    const wl_tdd_hold_t *hold, const wl_tdd_stop_t *stop)
{
    if (!ch || !lists || nworkers == 0 || !discard
        || (hold && (!hold->coord || !hold->stats)))
        return EINVAL;
    int rc = 0;
    uint64_t held = hold
        ? wl_columnar_eval_tdd_held_bytes(lists, nworkers, nrels, UINT32_MAX)
        : 0;
    bool cancelled = false;
    bool discarding = false;
    for (;;) {
        wl_tdd_batch_t *batch = NULL;
        int err = 0;
        wl_tdd_pump_result_t result = tdd_collect_pump(ch, cancelled, stop,
                &batch, &err);
        if (result == WL_TDD_PUMP_COMPLETE) {
            /* An orphan reclaimed on a lane that returned 0 leaves its
             * EPROTO on the channel; a completed round must report it. */
            wl_tdd_channel_stats_t stats;
            wl_tdd_channel_stats(ch, &stats);
            return rc != 0 ? rc : stats.channel_rc;
        }
        if (result == WL_TDD_PUMP_CANCELLED)
            return rc != 0 ? rc : tdd_channel_cancel_rc(ch);
        if (result != WL_TDD_PUMP_DATA) {
            if (err == 0)
                err = EPROTO;
            if (rc == 0)
                rc = err;
            /* Pump reports no error once the channel is cancelled; stop
             * rather than spin if it ever does. */
            if (cancelled)
                return rc;
            /* Producers blocked on space return once cancelled; keep
             * pumping until every lane has closed, so the caller's join
             * cannot wait on a producer nobody serves. */
            wl_tdd_channel_cancel(ch, err);
            cancelled = true;
            discarding = true;
            continue;
        }
        if (ledger)
            wl_mem_ledger_free(ledger, WL_MEM_SUBSYS_CHANNEL, batch->charge);
        bool valid = batch->rel_idx < nrels && batch->worker < nworkers;
        if (!valid && rc == 0)
            rc = EPROTO;
        if (!discarding && (!valid || wl_tdd_channel_failed_lanes(ch)))
            discarding = true;
        if (discarding) {
            /* The round fails; the caller disposes the batch after the
             * join, as it does every list. */
            batch->next = *discard;
            *discard = batch;
            continue;
        }
        if (rows)
            *rows += batch->rows;
        size_t index = (size_t)batch->rel_idx * nworkers + batch->worker;
        batch->next = lists[index];
        lists[index] = batch;
        if (hold)
            held = wl_columnar_eval_tdd_hold_batch(hold, lists, nworkers,
                    nrels, batch, held);
    }
}

/* Reverse an intrusive list in place. */
static void
tdd_list_reverse(wl_tdd_batch_t **list)
{
    wl_tdd_batch_t *reversed = NULL;
    wl_tdd_batch_t *batch = *list;
    while (batch) {
        wl_tdd_batch_t *next = batch->next;
        batch->next = reversed;
        reversed = batch;
        batch = next;
    }
    *list = reversed;
}

static uint64_t
tdd_add_saturated(uint64_t a, uint64_t b)
{
    return b > UINT64_MAX - a ? UINT64_MAX : a + b;
}

uint64_t
wl_columnar_eval_tdd_held_bytes(wl_tdd_batch_t *const *lists,
    uint32_t nworkers, uint32_t nrels, uint32_t skip_rel)
{
    uint64_t held = 0;
    if (!lists)
        return 0;
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (ri == skip_rel)
            continue;
        for (uint32_t w = 0; w < nworkers; w++) {
            const wl_tdd_batch_t *b = lists[(size_t)ri * nworkers + w];
            for (; b; b = b->next)
                if (b->kind == WL_TDD_BATCH_RESIDENT)
                    held = tdd_add_saturated(held, b->charge);
        }
    }
    return held;
}

/*
 * The held RESIDENT batch with the largest charge over every relation but
 * @skip_rel; with @spillable, only among batches not marked unspillable.
 */
static wl_tdd_batch_t *
tdd_held_largest(wl_tdd_batch_t **lists, uint32_t nworkers, uint32_t nrels,
    uint32_t skip_rel, bool spillable)
{
    wl_tdd_batch_t *best = NULL;
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (ri == skip_rel)
            continue;
        for (uint32_t w = 0; w < nworkers; w++) {
            wl_tdd_batch_t *b = lists[(size_t)ri * nworkers + w];
            for (; b; b = b->next) {
                if (b->kind != WL_TDD_BATCH_RESIDENT
                    || (spillable
                    && (b->flags & WL_TDD_BATCH_FLAG_UNSPILLABLE)))
                    continue;
                if (!best || b->charge > best->charge)
                    best = b;
            }
        }
    }
    return best;
}

uint64_t
wl_columnar_eval_tdd_spill_held(wl_col_session_t *coord,
    wl_tdd_batch_t **lists, uint32_t nworkers, uint32_t nrels,
    uint32_t skip_rel, uint64_t target, wl_tdd_consumer_stats_t *stats)
{
    uint64_t held = wl_columnar_eval_tdd_held_bytes(lists, nworkers, nrels,
            skip_rel);
    if (!coord || !lists || !coord->spill_store || !coord->memory_governor)
        return held;
    while (held > target) {
        wl_tdd_batch_t *batch = tdd_held_largest(lists, nworkers, nrels,
                skip_rel, true);
        if (!batch)
            break;
        /* A successful spill replaces the charge with the payload floor. */
        uint64_t charge = batch->charge;
        wl_columnar_spill_status_t status = WL_COLUMNAR_SPILL_INVALID;
        int rc = wl_columnar_eval_tdd_batch_spill(batch,
                coord->memory_governor, coord->spill_store, &status);
        if (rc == 0) {
            held = held > charge ? held - charge : 0;
            if (stats)
                stats->consumer_spills++;
            continue;
        }
        /* A batch without timestamps comes back unmarked; mark every
         * failure so the loop never picks the batch again. */
        batch->flags |= WL_TDD_BATCH_FLAG_UNSPILLABLE;
        if (stats)
            stats->consumer_spill_fail[(size_t)status
                < WL_TDD_SPILL_STATUS_SLOTS
                ? (size_t)status : (size_t)WL_COLUMNAR_SPILL_INVALID]++;
    }
    return held;
}

uint64_t
wl_columnar_eval_tdd_hold_batch(const wl_tdd_hold_t *hold,
    wl_tdd_batch_t **lists, uint32_t nworkers, uint32_t nrels,
    const wl_tdd_batch_t *batch, uint64_t held)
{
    wl_tdd_consumer_stats_t *stats = hold->stats;
    if (batch->kind != WL_TDD_BATCH_RESIDENT)
        return held;
    held = tdd_add_saturated(held, batch->charge);
    if (held > stats->round_held_peak_bytes)
        stats->round_held_peak_bytes = held;
    if (batch->charge > stats->max_batch_charge)
        stats->max_batch_charge = batch->charge;
    if (held > hold->cap) {
        if (hold->coord->spill_store && hold->coord->memory_governor)
            held = wl_columnar_eval_tdd_spill_held(hold->coord, lists,
                    nworkers, nrels, UINT32_MAX, hold->cap, stats);
        if (held > hold->cap)
            stats->held_over_cap++;
    }
    if (held > stats->max_held_bytes)
        stats->max_held_bytes = held;
    return held;
}

/* Coalesce relation @ri's lists into ctxs[w].delta_rels[ri]; see
 * wl_columnar_eval_tdd_channel_coalesce_all().  Once @rc is non-zero, on
 * entry or after a failure, the remaining lists are disposed instead. */
static int
tdd_coalesce_relation(wl_col_session_t *coord, wl_tdd_channel_t *ch,
    wl_tdd_batch_t **lists, col_eval_tdd_worker_ctx_t *ctxs,
    uint32_t nworkers, uint32_t ri, const wl_tdd_refusal_sink_t *sink,
    wl_tdd_consumer_stats_t *stats, bool *private_out, int rc)
{
    for (uint32_t w = 0; w < nworkers; w++) {
        wl_tdd_batch_t **list = &lists[(size_t)ri * nworkers + w];
        if (private_out)
            private_out[w] = false;
        if (!*list)
            continue;
        if (rc == 0 && ctxs[w].delta_rels[ri])
            rc = EPROTO;
        if (rc != 0) {
            wl_columnar_eval_tdd_discard_list(ch, list, sink);
            continue;
        }
        /* Collect pushed newest first; coalesce wants seq order. */
        tdd_list_reverse(list);
        const wl_tdd_batch_t *head = *list;
        const char *name = ctxs[w].sp
            ? ctxs[w].sp->relations[ri].delta_name : NULL;
        /* A slot of spilled batches takes the schema of worker w's relation
         * ri, which the ordinary path creates its deltas like.  In owner and
         * global-read mode an adopted slot is created like the relation
         * plan's result instead, so the two kinds of slot need only be
         * compatible under the exchanges' tdd_relation_schema_compatible(),
         * which accepts a declared width of 0 against any other. */
        const col_rel_t *schema = ctxs[w].sp && ctxs[w].worker_sess
            ? session_find_rel(ctxs[w].worker_sess,
                ctxs[w].sp->relations[ri].name) : NULL;
        uint64_t spilled = 0;
        for (const wl_tdd_batch_t *b = head; b; b = b->next)
            if (b->kind == WL_TDD_BATCH_SPILLED)
                spilled++;
        /* A single RESIDENT batch is adopted; any other slot is new. */
        const void *adoptable = head && !head->next
            && head->kind == WL_TDD_BATCH_RESIDENT ? head->payload : NULL;
        rc = wl_columnar_eval_tdd_coalesce_slot(coord, ch, list, nworkers,
                name, schema, sink, &ctxs[w].delta_rels[ri]);
        if (rc != 0) {
            wl_columnar_eval_tdd_discard_list(ch, list, sink);
            continue;
        }
        bool adopted = adoptable
            && (const void *)ctxs[w].delta_rels[ri] == adoptable;
        if (stats) {
            stats->rehydrated_batches += spilled;
            if (adopted)
                stats->stage_adopted++;
        }
        if (private_out)
            private_out[w] = ctxs[w].delta_rels[ri] && !adopted;
    }
    return rc;
}

int
wl_columnar_eval_tdd_channel_coalesce_all(wl_col_session_t *coord,
    wl_tdd_channel_t *ch, wl_tdd_batch_t **lists,
    col_eval_tdd_worker_ctx_t *ctxs, uint32_t nworkers, uint32_t nrels,
    const wl_tdd_refusal_sink_t *sink, wl_tdd_consumer_stats_t *stats)
{
    if (!coord || !ch || !lists || !ctxs || !sink || !sink->fn)
        return EINVAL;
    int rc = 0;
    for (uint32_t ri = 0; ri < nrels; ri++)
        rc = tdd_coalesce_relation(coord, ch, lists, ctxs, nworkers, ri, sink,
                stats, NULL, rc);
    return rc;
}

static uint64_t
tdd_mul_saturated(uint64_t a, uint64_t b)
{
    return a && b > UINT64_MAX / a ? UINT64_MAX : a * b;
}

/* What staging @list holds at its peak; see
 * wl_columnar_eval_tdd_channel_stage_relation(). */
static uint64_t
tdd_stage_estimate(const wl_tdd_batch_t *list)
{
    uint64_t inputs = 0;
    uint64_t rows = 0;
    uint32_t count = 0;
    uint32_t ncols = 0;
    for (const wl_tdd_batch_t *b = list; b; b = b->next) {
        uint32_t nrows = 0;
        count++;
        if (b->kind == WL_TDD_BATCH_RESIDENT) {
            const col_rel_t *rel = b->payload;
            inputs = tdd_add_saturated(inputs, b->charge);
            if (rel) {
                nrows = rel->nrows;
                ncols = rel->ncols;
            }
        } else if (b->payload) {
            (void)wl_columnar_batch_payload_schema(b->payload, &ncols,
                &nrows);
        }
        rows += nrows;
    }
    if (count == 1 && list->kind == WL_TDD_BATCH_RESIDENT)
        return inputs;
    uint64_t row_bytes = tdd_add_saturated(
        tdd_mul_saturated(ncols, sizeof(int64_t)),
        sizeof(col_delta_timestamp_t));
    uint64_t slot = tdd_mul_saturated(rows, row_bytes);
    uint64_t estimate = tdd_add_saturated(inputs, slot);
    if (count >= 2)
        estimate = tdd_add_saturated(tdd_add_saturated(estimate, slot),
                tdd_mul_saturated(rows, 4u));
    return estimate;
}

/* The transport bytes of the matrix slots of every relation but @skip_rel,
 * or of @skip_rel alone when @only is set. */
static uint64_t
tdd_matrix_bytes(const col_eval_tdd_worker_ctx_t *ctxs, uint32_t nworkers,
    uint32_t nrels, uint32_t skip_rel, bool only)
{
    uint64_t bytes = 0;
    for (uint32_t w = 0; w < nworkers; w++) {
        if (!ctxs[w].delta_rels)
            continue;
        for (uint32_t ri = 0; ri < nrels; ri++) {
            if ((ri == skip_rel) != only || !ctxs[w].delta_rels[ri])
                continue;
            bytes = tdd_add_saturated(bytes,
                    col_rel_transport_bytes(ctxs[w].delta_rels[ri]));
        }
    }
    return bytes;
}

#ifdef WL_SESSION_TEST_HOOKS
int (*wl_columnar_eval_test_after_stage)(wl_col_session_t *coord,
    uint32_t ri);
#endif

int
wl_columnar_eval_tdd_channel_stage_relation(wl_col_session_t *coord,
    wl_tdd_channel_t *ch, wl_tdd_batch_t **lists,
    col_eval_tdd_worker_ctx_t *ctxs, uint32_t nworkers, uint32_t nrels,
    uint32_t ri, uint64_t held_cap, bool *private_out,
    const wl_tdd_refusal_sink_t *sink, wl_tdd_consumer_stats_t *stats)
{
    if (!coord || !ch || !lists || !ctxs || !sink || !sink->fn
        || ri >= nrels)
        return EINVAL;
    if (private_out)
        for (uint32_t w = 0; w < nworkers; w++)
            private_out[w] = false;
    uint64_t estimate = 0;
    bool any = false;
    for (uint32_t w = 0; w < nworkers; w++) {
        const wl_tdd_batch_t *list = lists[(size_t)ri * nworkers + w];
        if (list) {
            any = true;
            estimate = tdd_add_saturated(estimate, tdd_stage_estimate(list));
        }
    }
    if (!any)
        return 0;
    wl_tdd_consumer_stats_t local;
    if (!stats) {
        memset(&local, 0, sizeof(local));
        stats = &local;
    }

    /* Spill down: make room for this relation's staging peak. */
    uint64_t retained = tdd_matrix_bytes(ctxs, nworkers, nrels, ri, false);
    uint64_t need = tdd_add_saturated(retained, estimate);
    uint64_t target = held_cap > need ? held_cap - need : 0;
    uint64_t held_other = wl_columnar_eval_tdd_held_bytes(lists, nworkers,
            nrels, ri);
    if (held_other > target) {
        uint64_t spills = stats->consumer_spills;
        held_other = wl_columnar_eval_tdd_spill_held(coord, lists, nworkers,
                nrels, ri, target, stats);
        stats->stage_spills += stats->consumer_spills - spills;
    }
    if (held_other > target || retained > held_cap)
        stats->stage_over_cap++;

    int rc = tdd_coalesce_relation(coord, ch, lists, ctxs, nworkers, ri,
            sink, stats, private_out, 0);
    uint64_t staged = tdd_matrix_bytes(ctxs, nworkers, nrels, ri, true);
    uint64_t peak = tdd_add_saturated(tdd_add_saturated(held_other, retained),
            staged);
    if (peak > stats->stage_peak_bytes)
        stats->stage_peak_bytes = peak;
    if (peak > stats->stratum_stage_peak_bytes)
        stats->stratum_stage_peak_bytes = peak;
    if (staged > stats->max_stage_bytes)
        stats->max_stage_bytes = staged;
#ifdef WL_SESSION_TEST_HOOKS
    if (rc == 0 && wl_columnar_eval_test_after_stage)
        rc = wl_columnar_eval_test_after_stage(coord, ri);
#endif
    return rc;
}

void
wl_columnar_eval_tdd_channel_discard_lists(wl_tdd_channel_t *ch,
    wl_tdd_batch_t **lists, size_t count, const wl_tdd_refusal_sink_t *sink)
{
    if (!ch || !lists)
        return;
    for (size_t i = 0; i < count; i++)
        if (lists[i])
            wl_columnar_eval_tdd_discard_list(ch, &lists[i], sink);
}
