/*
 * columnar/eval_tdd_channel.h - relation adapters for the TDD channel
 * (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL - not installed, not part of public API.
 *
 * The channel (tdd_channel.h) moves descriptors without looking at their
 * payloads.  These helpers give the payloads a type: a RESIDENT batch
 * carries a col_rel_t *, a SPILLED batch a wl_columnar_batch_payload_t *.
 * The producer helpers run on a worker's thread inside its lane; every other
 * function runs on the coordinator thread, on CONSUMER-state descriptors
 * whose producers have released their leases.
 */
#ifndef WL_COLUMNAR_EVAL_TDD_CHANNEL_H
#define WL_COLUMNAR_EVAL_TDD_CHANNEL_H

#include "batch_payload.h"
#include "mem_ledger.h"
#include "tdd_channel.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct col_rel;
struct wl_col_session_t;
struct col_eval_tdd_worker_ctx;

/*
 * Receives a CONSUMER descriptor whose payload could not be retired: @rc is
 * col_rel_destroy_checked()'s refusal for a RESIDENT batch, or the mapped
 * wl_columnar_batch_payload_release() status for a SPILLED one.  The
 * descriptor still holds its payload; the sink takes both and must retire
 * them later (for example on an owner-lifetime retained list).
 */
typedef struct {
    void (*fn)(void *ctx, wl_tdd_batch_t *batch, int rc);
    void *ctx;
} wl_tdd_refusal_sink_t;

/* Context for wl_columnar_eval_tdd_discard_sink().  @refusal.fn must be
 * set: a refused retirement without it aborts.  When @ledger is set, each
 * batch's charge is freed from it under WL_MEM_SUBSYS_CHANNEL before the
 * batch is disposed. */
typedef struct {
    wl_tdd_channel_t *channel;
    wl_tdd_refusal_sink_t refusal;
    wl_mem_ledger_t *ledger;
} wl_tdd_discard_ctx_t;

/* The errno a spill status maps to, as the cold-relation reader maps it:
 * MEMORY -> ENOSPC, ALLOCATION -> ENOMEM, IO/QUOTA/CAPACITY -> EIO,
 * OVERFLOW -> EOVERFLOW, BUSY -> EBUSY, any other failure -> ENOTSUP. */
int
wl_columnar_eval_tdd_spill_errno(wl_columnar_spill_status_t status);

/*
 * Move the relation of a CONSUMER RESIDENT batch into a spilled payload on
 * @store, under @gov (the store's governor).  On success the batch becomes
 * SPILLED, its payload is the new payload, its charge is the payload's
 * metadata floor, and 0 is returned.  On a spill failure the batch is marked
 * WL_TDD_BATCH_FLAG_UNSPILLABLE, its relation, kind and charge are
 * unchanged, and the mapped errno is returned.  EINVAL (invalid arguments,
 * or a relation without timestamps) leaves the batch untouched; the return
 * value, not @status, tells it apart from a spill failure.  @status
 * (optional) receives the spill status, or WL_COLUMNAR_SPILL_INVALID for
 * EINVAL.
 */
int
wl_columnar_eval_tdd_batch_spill(wl_tdd_batch_t *batch,
    wl_columnar_memory_governor_ref_t *gov, wl_columnar_spill_store_t *store,
    wl_columnar_spill_status_t *status);

/*
 * Append every row of a SPILLED batch to @dst, with its lanes and its
 * iteration, stratum, worker and multiplicity.  @dst must have timestamps
 * enabled and the payload's column count and types (EINVAL otherwise).  A
 * row whose worker is not below @nworkers gives EPROTO.  For a non-empty
 * payload the reader is acquired for the call, which charges its floor,
 * and released before returning.  On failure @dst may hold a prefix of the
 * batch's rows and is the caller's to discard; the batch and its payload
 * are unchanged.
 */
int
wl_columnar_eval_tdd_batch_rehydrate(const wl_tdd_batch_t *batch,
    struct col_rel *dst, uint32_t nworkers);

/*
 * Build one (worker, relation) delta slot from @list, an intrusive
 * seq-ordered list of CONSUMER batches of a single worker and relation.
 *
 * - One RESIDENT batch: its relation is adopted without a copy and must have
 *   timestamps (EPROTO otherwise).
 * - Otherwise a fresh slot is created under @coord's governor with the
 *   batches' schema and timestamps, sized to their total rows, and filled in
 *   seq order: RESIDENT relations must have timestamps (EPROTO otherwise);
 *   SPILLED payloads are rehydrated.  When two or more batches arrived and
 *   every row has the same iteration, stratum and worker and a multiplicity
 *   of +1, the slot is consolidated with wl_columnar_eval_delta_consolidate();
 *   otherwise every row is kept with its own timestamp.
 *
 * An adopted relation keeps its own name.  A fresh slot is named @name, or
 * after the first RESIDENT relation when @name is NULL, or "$tdd_slot" when
 * every batch is SPILLED and @name is NULL.  *@out_slot must be NULL on
 * entry.
 *
 * Success: *@out_slot is the slot (NULL for an empty list) and *@list is
 * NULL.  An adopted relation becomes the slot.  Every other source is
 * retired: relations through col_rel_destroy_checked(), payloads through
 * wl_columnar_batch_payload_release().  A source whose retirement is
 * refused goes to @sink with its descriptor instead, which is not a
 * failure; every descriptor not handed to @sink has been retired through
 * wl_tdd_channel_retire().
 *
 * Failure: *@list and every descriptor on it are unchanged (still CONSUMER,
 * payloads intact, same order), *@out_slot stays NULL, and the partly built
 * slot has been destroyed.  The caller disposes the list, for example with
 * wl_columnar_eval_tdd_discard_list().  EINVAL for invalid arguments or a
 * list that is not CONSUMER batches of one worker and relation in strictly
 * increasing seq; EPROTO for a schema mismatch, a missing timestamp array or
 * a spilled row's worker not below @nworkers; ENOSPC, recorded on @coord,
 * when the governor refuses an admission; other errors as the relation,
 * spill and consolidation calls report them.
 */
int
wl_columnar_eval_tdd_coalesce_slot(struct wl_col_session_t *coord,
    wl_tdd_channel_t *ch, wl_tdd_batch_t **list, uint32_t nworkers,
    const char *name, const wl_tdd_refusal_sink_t *sink,
    struct col_rel **out_slot);

/*
 * Dispose a CONSUMER batch: destroy its relation (RESIDENT) or release its
 * payload (SPILLED), then retire the descriptor.  A refused retirement
 * hands the descriptor and payload to @sink, whose fn must be set.
 */
void
wl_columnar_eval_tdd_batch_discard(wl_tdd_channel_t *ch,
    wl_tdd_batch_t *batch, const wl_tdd_refusal_sink_t *sink);

/* Dispose every batch on *@list as wl_columnar_eval_tdd_batch_discard()
 * does, and set *@list to NULL. */
void
wl_columnar_eval_tdd_discard_list(wl_tdd_channel_t *ch, wl_tdd_batch_t **list,
    const wl_tdd_refusal_sink_t *sink);

/* A wl_tdd_channel_discard_all() sink; @ctx is a wl_tdd_discard_ctx_t and
 * must not be NULL (it aborts otherwise). */
void
wl_columnar_eval_tdd_discard_sink(void *ctx, wl_tdd_batch_t *batch);

/*
 * Producer: fund a descriptor on @sess's lane of the coordinator's channel
 * for relation @rel_idx and reserve @delta's transport bytes on it.  On
 * success *@out is the reserved PRODUCER batch and nothing has been
 * published; the caller either publishes it with
 * wl_columnar_eval_tdd_channel_publish_reserved() or drops it with
 * wl_tdd_channel_batch_abandon().  EOVERFLOW when the transport bytes
 * cannot be represented; otherwise errors are the channel's: EINVAL without
 * a coordinator channel or for an empty charge, ENOSPC (also recorded on
 * @sess), ENOMEM, EBUSY, EOVERFLOW, ECANCELED or EPROTO.
 */
int
wl_columnar_eval_tdd_channel_reserve_delta(struct wl_col_session_t *sess,
    uint32_t rel_idx, const struct col_rel *delta, wl_tdd_batch_t **out);

/*
 * Producer: charge the coordinator's CHANNEL ledger with @batch's reserved
 * bytes and publish @delta, which must be stamped, on it.  @delta passes to
 * the channel and 0 is returned.
 *
 * Under WL_SESSION_TEST_HOOKS the split hook may ask for @delta to be
 * published as several batches.  If the extra batches cannot be prepared,
 * @delta is published whole; once the first part is published, a later
 * part's refused reservation is returned and that part is destroyed, so
 * the round fails instead of losing rows silently.
 */
int
wl_columnar_eval_tdd_channel_publish_reserved(struct wl_col_session_t *sess,
    wl_tdd_batch_t *batch, struct col_rel *delta);

/*
 * Outbound producer: stamp *@candidate with (@eff_iter, the context's
 * stratum, @sess's worker id, multiplicity 1) under its writer, release the
 * writer, then publish it on the coordinator's channel; without a channel it
 * goes to ctx->delta_rels[@rel_idx] instead.  An empty candidate is
 * destroyed.  On success *@candidate is NULL.  On any refusal, including a
 * failed writer release, *@candidate still belongs to the caller.
 */
int
wl_columnar_eval_tdd_channel_publish_delta(struct col_eval_tdd_worker_ctx *ctx,
    struct wl_col_session_t *sess, struct col_rel **candidate,
    uint32_t rel_idx, uint32_t eff_iter);

/*
 * Consumer, after the producers are joined: pump @ch until it reports the
 * round complete.  Each taken batch's charge is freed from @ledger (when
 * set) under WL_MEM_SUBSYS_CHANNEL, its rows are added to *@rows (when set),
 * and it is pushed onto lists[rel_idx * @nworkers + worker], so each list
 * holds its batches newest first.  A batch whose relation is not below
 * @nrels, or whose worker is not below @nworkers, is destroyed (a refused
 * retirement goes to @sink) and makes the call return EPROTO after the
 * round has been pumped to its end; its rows are not counted.  Returns 0,
 * EINVAL, EPROTO, or the channel's error: when the round is cancelled or
 * unbalanced, and when it completed with an error recorded on the channel
 * (an orphan reclaimed on a lane that returned 0).  The lists keep what was
 * collected either way.
 */
int
wl_columnar_eval_tdd_channel_collect(wl_tdd_channel_t *ch,
    wl_mem_ledger_t *ledger, wl_tdd_batch_t **lists, uint32_t nworkers,
    uint32_t nrels, const wl_tdd_refusal_sink_t *sink, uint64_t *rows);

/*
 * Consumer: coalesce every list built by wl_columnar_eval_tdd_channel_collect()
 * into ctxs[w].delta_rels[ri] with wl_columnar_eval_tdd_coalesce_slot(), in
 * seq order, naming each slot after relation ri's delta in ctxs[w].sp.
 * Unless an argument is invalid (EINVAL), every list is empty on return.  On
 * failure the remaining lists are destroyed (a refused retirement goes to
 * @sink), slots already built stay in the matrix for the caller's cleanup,
 * and the error is returned; a slot that is already occupied gives EPROTO.
 */
int
wl_columnar_eval_tdd_channel_coalesce_all(struct wl_col_session_t *coord,
    wl_tdd_channel_t *ch, wl_tdd_batch_t **lists,
    struct col_eval_tdd_worker_ctx *ctxs, uint32_t nworkers, uint32_t nrels,
    const wl_tdd_refusal_sink_t *sink);

/* Dispose every batch on lists[0..@count) through
 * wl_columnar_eval_tdd_discard_list() and leave each list empty. */
void
wl_columnar_eval_tdd_channel_discard_lists(wl_tdd_channel_t *ch,
    wl_tdd_batch_t **lists, size_t count, const wl_tdd_refusal_sink_t *sink);

#ifdef WL_SESSION_TEST_HOOKS
/* Test hook, called on a worker thread for every published delta of @rows
 * rows: return the number of batches to publish it as, 0 or 1 for one.  The
 * count is capped at WL_TDD_CHANNEL_TEST_SPLIT_MAX and at @rows.  Setting
 * *@overlap makes every batch after the first repeat the previous batch's
 * last row. */
#define WL_TDD_CHANNEL_TEST_SPLIT_MAX 8u
extern uint32_t (*wl_columnar_eval_test_channel_split)(
    struct wl_col_session_t *worker, uint32_t rel_idx, uint32_t rows,
    bool *overlap);
#endif

#endif /* WL_COLUMNAR_EVAL_TDD_CHANNEL_H */
