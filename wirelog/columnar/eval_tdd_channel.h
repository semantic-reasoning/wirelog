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

/* One counter per wl_columnar_spill_status_t value. */
#define WL_TDD_SPILL_STATUS_SLOTS ((size_t)WL_COLUMNAR_SPILL_CLOSED + 1u)

/*
 * Consumer-side accounting of the TDD channel (#1373).  The coordinator
 * owns it and is its only writer.  "Held" bytes are the charges of the
 * RESIDENT batches the consumer has taken and not yet staged; a SPILLED
 * batch holds no resident payload and is not counted.
 *
 * - held_cap: the cap in force for the last stratum.
 * - round_held_peak_bytes: the most held bytes right after a take, before
 *   the cap pass that follows it.
 * - max_held_bytes: the most held bytes left after a take and its cap
 *   pass.
 * - max_batch_charge: the largest charge of a batch taken onto a list.
 * - held_over_cap: cap passes that ended above their target, because no
 *   batch was left that could be spilled or the session has no spill
 *   store.
 * - consumer_spills: batches spilled by the consumer.
 * - consumer_spill_fail[s]: spill attempts that failed with status s;
 *   a batch without timestamps counts as WL_COLUMNAR_SPILL_INVALID.
 * - rehydrated_batches: SPILLED batches read back into a staged slot.
 *
 * Staging (wl_columnar_eval_tdd_channel_stage_relation()):
 * - stage_peak_bytes: the largest, over the stagings, of the held bytes of
 *   the other relations after their spill-down, plus the matrix slots of
 *   other relations still in place (owner mode keeps its adopted ones
 *   until its commit), plus the transport bytes of the slots staged for the
 *   relation itself, taken once the staging has coalesced.  It does not
 *   count the inputs and scratch the coalesce frees, the buffers the
 *   exchange then builds from those slots, the publication candidates owner
 *   mode has built for the relations exchanged before, nor the next round's
 *   inputs.
 * - max_stage_bytes: the most transport bytes one relation's staged slots
 *   held.
 * - stage_over_cap: stagings whose spill-down ended above its target, for
 *   the reasons a cap pass does, or that found the other relations' matrix
 *   slots alone over the cap.
 * - stage_adopted: slots a staging adopted from a single RESIDENT batch.
 * - stage_spills: the consumer_spills made by a spill-down.
 * - owner_private_released: coordinator-private owner-mode slots destroyed
 *   as soon as the exchange had copied them, before its commit.
 * - owner_private_at_commit: private slots still in the matrix when the
 *   owner exchange prepared its input retirements; 0 unless a release was
 *   refused.
 * - stratum_stage_peak_bytes: stage_peak_bytes over the current recursive
 *   stratum alone; col_eval_stratum_tdd_recursive() restarts it at 0.
 */
typedef struct {
    uint64_t held_cap;
    uint64_t round_held_peak_bytes;
    uint64_t max_held_bytes;
    uint64_t max_batch_charge;
    uint64_t held_over_cap;
    uint64_t consumer_spills;
    uint64_t consumer_spill_fail[WL_TDD_SPILL_STATUS_SLOTS];
    uint64_t rehydrated_batches;
    uint64_t stage_peak_bytes;
    uint64_t max_stage_bytes;
    uint64_t stage_over_cap;
    uint64_t stage_adopted;
    uint64_t stage_spills;
    uint64_t owner_private_released;
    uint64_t owner_private_at_commit;
    uint64_t stratum_stage_peak_bytes;
} wl_tdd_consumer_stats_t;

/*
 * The held cap wl_columnar_eval_tdd_channel_collect() applies while the
 * round's producers run (#1373): spills go to @coord's spill store under
 * its governor, @cap is the held cap and @stats, which must not be NULL,
 * receives the counts.
 */
typedef struct {
    struct wl_col_session_t *coord;
    uint64_t cap;
    wl_tdd_consumer_stats_t *stats;
} wl_tdd_hold_t;

/*
 * An evaluation stop wl_columnar_eval_tdd_channel_collect() polls before
 * each pump until it has cancelled the channel: @fn returns 0 to go on, or
 * the error that stops the round.
 */
typedef struct {
    int (*fn)(void *ctx);
    void *ctx;
} wl_tdd_stop_t;

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
 * A fresh slot takes its schema (column names, types, declared width and
 * the other metadata wl_columnar_relation_new_like_governed_checked_mode()
 * copies) from the first RESIDENT relation, or from @schema when every
 * batch is SPILLED.  @schema may be NULL; a slot of SPILLED batches then
 * gets only the payload's column types.  A @schema whose column count or
 * types differ from the payload's is not used.
 *
 * An adopted relation keeps its own name.  A fresh slot is named @name, or
 * after the relation its schema came from when @name is NULL, or
 * "$tdd_slot" when it has no such relation.  *@out_slot must be NULL on
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
    const char *name, const struct col_rel *schema,
    const wl_tdd_refusal_sink_t *sink, struct col_rel **out_slot);

/* The held bytes of lists[ri * @nworkers + w] over every relation ri below
 * @nrels except @skip_rel (pass UINT32_MAX to count them all): the summed
 * charge of their RESIDENT batches. */
uint64_t
wl_columnar_eval_tdd_held_bytes(wl_tdd_batch_t *const *lists,
    uint32_t nworkers, uint32_t nrels, uint32_t skip_rel);

/*
 * Consumer: spill held RESIDENT batches of every relation but @skip_rel,
 * largest charge first, until their held bytes are at most @target or no
 * batch is left to try, and return the held bytes left.  Spills go to
 * @coord's spill store under its governor; without both nothing is
 * spilled.  The call may run while the round's producers do: a producer
 * no longer touches a delta once it has published it.
 *
 * A batch is tried at most once per round: a failed spill marks it
 * WL_TDD_BATCH_FLAG_UNSPILLABLE, and so does a batch without timestamps
 * (EINVAL), so a pass makes at most one attempt per held batch and ends.
 * A marked batch stays resident and governed.  MEMORY and ALLOCATION are
 * not retried either, although memory freed later in the round might let
 * a retry succeed: each attempt runs at most one capture on the pump
 * thread, and one attempt per batch bounds that work.  BUSY would mean a
 * published delta still had a lease or a borrow, which docs/THREADING.md
 * argues it does not.  Successes and failures are counted in @stats
 * (optional).
 */
uint64_t
wl_columnar_eval_tdd_spill_held(struct wl_col_session_t *coord,
    wl_tdd_batch_t **lists, uint32_t nworkers, uint32_t nrels,
    uint32_t skip_rel, uint64_t target, wl_tdd_consumer_stats_t *stats);

/*
 * Consumer, the cap pass after @batch was pushed onto @lists: @held is the
 * held bytes before the take, to which a RESIDENT @batch adds its charge.
 * When the sum exceeds @hold->cap, held batches are spilled with
 * wl_columnar_eval_tdd_spill_held() (nothing is spilled without a spill
 * store), and held_over_cap is counted when the held bytes still exceed
 * the cap.  Records round_held_peak_bytes, max_batch_charge and
 * max_held_bytes, and returns the held bytes left.  Nothing fails: a batch
 * that cannot be spilled stays resident, governed as before.
 */
uint64_t
wl_columnar_eval_tdd_hold_batch(const wl_tdd_hold_t *hold,
    wl_tdd_batch_t **lists, uint32_t nworkers, uint32_t nrels,
    const wl_tdd_batch_t *batch, uint64_t held);

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
 * Consumer: pump @ch until the round ends, while its producers may still be
 * running.  The call disposes of no batch.  Each taken batch's charge is
 * freed from @ledger (when set) under WL_MEM_SUBSYS_CHANNEL.  A batch is
 * pushed onto lists[rel_idx * @nworkers + worker], so each list holds its
 * batches newest first, and its rows are added to *@rows (when set).
 *
 * With @hold, each batch pushed onto a list is followed by its cap pass,
 * wl_columnar_eval_tdd_hold_batch(), before the next pump, so a batch may
 * be spilled while producers still run.  Without @hold the call touches
 * descriptors only, never a payload.
 *
 * With @stop, its function is polled before each pump until the call has
 * cancelled the channel, and a non-zero result is handled as a pump error
 * with that value.  The cancel that follows returns ECANCELED to a
 * producer waiting for space, so a stop reaches producers blocked in
 * wl_tdd_channel_reserve() as well.
 *
 * Once the round cannot succeed the call keeps pumping in discard mode, so
 * producers still make progress, and pushes every later batch onto
 * *@discard instead; the caller disposes that list after the producers are
 * joined.  Discard mode starts when a lane has closed with an error, when a
 * batch names a relation not below @nrels or a worker not below @nworkers
 * (that batch is discarded too and the call returns EPROTO), or when pump
 * reports an error.  On a pump error the call cancels the channel with that
 * error and keeps pumping until every lane has closed.
 *
 * The call returns once pump reports the round COMPLETE or CANCELLED.  The
 * only earlier returns are EINVAL for invalid arguments and a second pump
 * error after the cancel, which pump does not report while the channel is
 * cancelled.  Returns 0, EPROTO, the pump error, or the channel's error:
 * for a cancelled round, and for a completed round with an error recorded
 * on the channel (an orphan reclaimed on a lane that returned 0).  The lists
 * keep what was collected either way, and batches still queued after a
 * cancel stay in the channel.
 */
int
wl_columnar_eval_tdd_channel_collect(wl_tdd_channel_t *ch,
    wl_mem_ledger_t *ledger, wl_tdd_batch_t **lists, uint32_t nworkers,
    uint32_t nrels, wl_tdd_batch_t **discard, uint64_t *rows,
    const wl_tdd_hold_t *hold, const wl_tdd_stop_t *stop);

/*
 * Consumer: coalesce every list built by
 * wl_columnar_eval_tdd_channel_collect() into ctxs[w].delta_rels[ri] with
 * wl_columnar_eval_tdd_coalesce_slot(), in seq order, all relations at once
 * (the exchanges stage one relation at a time with
 * wl_columnar_eval_tdd_channel_stage_relation() instead; the unit tests use
 * this form), naming each slot after relation ri's delta in ctxs[w].sp and
 * taking its schema from relation ri in worker w's session when every batch
 * was spilled.  The SPILLED batches of each list that coalesces are added to
 * @stats->rehydrated_batches (@stats is optional).  Unless an argument is
 * invalid (EINVAL), every list is empty on return.  On failure the remaining
 * lists are destroyed (a refused retirement goes to @sink), slots already
 * built stay in the matrix for the caller's cleanup, and the error is
 * returned; a slot that is already occupied gives EPROTO.
 */
int
wl_columnar_eval_tdd_channel_coalesce_all(struct wl_col_session_t *coord,
    wl_tdd_channel_t *ch, wl_tdd_batch_t **lists,
    struct col_eval_tdd_worker_ctx *ctxs, uint32_t nworkers, uint32_t nrels,
    const wl_tdd_refusal_sink_t *sink, wl_tdd_consumer_stats_t *stats);

/*
 * Consumer, at the top of an exchange's per-relation loop (#1373): stage
 * relation @ri of the round held on @lists into ctxs[w].delta_rels[ri],
 * one relation at a time, so the round's other relations need not be
 * resident while this one is coalesced.
 *
 * First the held RESIDENT batches of the other relations are spilled down,
 * largest first, while their held bytes, plus the matrix slots other
 * relations still hold, plus an estimate of what staging @ri holds at its
 * peak, exceed @held_cap.  The estimate counts an adopted batch's charge.
 * For a fresh slot it counts the resident inputs and the rows' bytes, and
 * when two or more batches may be consolidated, the rows' bytes a second
 * time and a 4-byte index per row.  When the spill-down cannot get under
 * its target, or the other relations' matrix slots alone exceed
 * @held_cap, stage_over_cap is counted.  Then relation @ri's lists are
 * coalesced as wl_columnar_eval_tdd_channel_coalesce_all() coalesces them.
 *
 * @private_out (optional, @nworkers entries) records, per worker, whether
 * its slot is a fresh relation private to the coordinator; it is false for
 * an adopted batch and for no slot.  With no batch held for @ri the call
 * only clears @private_out and returns 0.  Returns EINVAL for invalid
 * arguments, otherwise what the coalesce returns; on failure the slots
 * already built for @ri stay in the matrix for the caller and ri's other
 * lists are disposed through @sink.  @stats (optional) receives the staging
 * counts.
 */
int
wl_columnar_eval_tdd_channel_stage_relation(struct wl_col_session_t *coord,
    wl_tdd_channel_t *ch, wl_tdd_batch_t **lists,
    struct col_eval_tdd_worker_ctx *ctxs, uint32_t nworkers, uint32_t nrels,
    uint32_t ri, uint64_t held_cap, bool *private_out,
    const wl_tdd_refusal_sink_t *sink, wl_tdd_consumer_stats_t *stats);

/* Dispose every batch on lists[0..@count) through
 * wl_columnar_eval_tdd_discard_list() and leave each list empty. */
void
wl_columnar_eval_tdd_channel_discard_lists(wl_tdd_channel_t *ch,
    wl_tdd_batch_t **lists, size_t count, const wl_tdd_refusal_sink_t *sink);

#ifdef WL_SESSION_TEST_HOOKS
/* Test hook, called on the coordinator thread before each pump of
 * wl_columnar_eval_tdd_channel_collect() until the round is cancelled.  A
 * non-zero return is handled as a pump error with that value. */
extern int (*wl_columnar_eval_test_before_pump)(wl_tdd_channel_t *ch);

/* Test hook, called on a worker thread before a delta for relation
 * @rel_idx, holding @rows rows, is reserved on the channel.  A non-zero
 * return is returned by wl_columnar_eval_tdd_channel_reserve_delta() in
 * place of reserving. */
extern int (*wl_columnar_eval_test_channel_reserve)(
    struct wl_col_session_t *worker, uint32_t rel_idx, uint32_t rows);

/* Test hook, called on a worker thread for every published delta of @rows
 * rows: return the number of batches to publish it as, 0 or 1 for one.  The
 * count is capped at WL_TDD_CHANNEL_TEST_SPLIT_MAX and at @rows.  Setting
 * *@overlap makes every batch after the first repeat the previous batch's
 * last row. */
#define WL_TDD_CHANNEL_TEST_SPLIT_MAX 8u
extern uint32_t (*wl_columnar_eval_test_channel_split)(
    struct wl_col_session_t *worker, uint32_t rel_idx, uint32_t rows,
    bool *overlap);

/* Test hook, called on the coordinator thread when
 * wl_columnar_eval_tdd_channel_stage_relation() has staged relation @ri.
 * A non-zero return is returned as the staging's failure, with ri's staged
 * slots left in the matrix. */
extern int (*wl_columnar_eval_test_after_stage)(
    struct wl_col_session_t *coord, uint32_t ri);
#endif

#endif /* WL_COLUMNAR_EVAL_TDD_CHANNEL_H */
