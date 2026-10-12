/*
 * eval_tdd_insert.c - Issue #2114: evaluate an insert into one stratum with
 * occurrence-bound instances.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * After rows are appended to relations a stratum reads, the stratum's new
 * derivations are those that use at least one new row.  This finds them
 * without re-deriving what its heads hold:
 *   - round 0 runs every instance driven by an occurrence of a changed
 *     relation, binding that occurrence to the new rows and every other
 *     occurrence to the whole relation as it stands, new rows included;
 *   - a recursive stratum then runs every instance driven by an occurrence
 *     of one of its heads, bound to the rows the previous round added,
 *     until a round adds nothing.
 * Each round's outputs are merged into the heads at its end, and the rows
 * that were not already there become the next round's deltas.  A relation
 * outside the stratum is never read as a delta after round 0.
 *
 * Issue #2122: in a session with more than one worker, a round's instances
 * run on up to that many worker clones at once.  Each instance may be split
 * into ranges of its leading input, the VARIABLE its rule slice starts
 * with, whether that reads the delta or a whole relation: every operator a
 * slice holds turns each row of its left operand into output independently,
 * so the ranges' outputs together are the unsplit instance's.  Clones pull
 * the runs from a shared counter, each into bags of its own, and the
 * coordinator merges every bag after all clones are done -- the round's
 * barrier.  The order in which bags are merged is not specified.
 */

#include "internal.h"
#include "../wirelog-internal.h"
#include "../workqueue.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#ifdef WL_SESSION_TEST_HOOKS
int (*wl_columnar_eval_tdd_insert_test_after_merge)(
    const wl_plan_stratum_t *sp);
void (*wl_columnar_eval_tdd_insert_test_round)(const wl_plan_stratum_t *sp,
    uint32_t units, uint32_t pieces, uint32_t tasks);
void (*wl_columnar_eval_tdd_insert_test_piece_start)(uint32_t task,
    uint32_t tasks, bool first_piece);
#endif

/* Issue #2122: the ranges one leading relation is split into for a round,
 * built on the coordinator.  Its address is the runs' partition token. */
typedef struct {
    const col_rel_t *src;
    uint32_t count;
    col_rel_t **range;
} insert_split_t;

/* Issue #2122: one run, instance @slice of @m over range @piece of its
 * leading input (the whole input when @split is NULL).  @rels holds the
 * relation each read binds, resolved on the coordinator. */
typedef struct {
    const wl_columnar_eval_tdd_plan_manifest_t *m;
    uint32_t ri;
    uint32_t slice;
    uint32_t lead;
    col_rel_t **rels;
    const insert_split_t *split;
    uint32_t piece;
} insert_piece_t;

/* A round's runs and the counter its tasks pull them by. */
typedef struct {
    wl_col_session_t *sess;
    const wl_plan_stratum_t *sp;
    col_rel_t **heads;
    const insert_piece_t *pieces;
    uint32_t npieces;
    uint32_t tasks;
    wl_atomic_u64 next;
    wl_atomic_u64 stop;
} insert_round_t;

/* One task: a clone and the bags, one per head, its runs append to. */
typedef struct {
    insert_round_t *round;
    wl_col_session_t *worker;
    col_rel_t **acc;
    uint32_t task;
    int rc;
} insert_task_t;

/* A private copy of @src's rows with @src's schema and without delta
 * timestamps: what an instance's DELTA read and a returned head delta are. */
static int
insert_untimed_copy(const col_rel_t *src, const char *name,
    wl_col_session_t *sess, col_rel_t **out)
{
    col_rel_t *copy = NULL;
    int rc = wl_columnar_relation_new_like_governed_checked_mode(name, src,
            sess->memory_governor, false, &copy);
    if (rc != 0)
        return rc;
    rc = col_rel_append_all(copy, src, NULL);
    if (rc != 0) {
        col_rel_destroy(copy);
        return rc;
    }
    *out = copy;
    return 0;
}

/* The delta bound to an instance's driver: the changed relation's new rows
 * in round 0, the head's last additions afterwards.  NULL when the driving
 * relation has none. */
static col_rel_t *
insert_driver_delta(const wl_plan_stratum_t *sp, const char *name,
    const char *const *changed, col_rel_t *const *changed_delta,
    uint32_t nchanged, col_rel_t *const *round_delta, bool round0)
{
    if (round0) {
        for (uint32_t c = 0; c < nchanged; c++) {
            if (strcmp(changed[c], name) == 0)
                return changed_delta[c];
        }
        return NULL;
    }
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        if (strcmp(sp->relations[ri].name, name) == 0)
            return round_delta[ri];
    }
    return NULL;
}

/* Run @p on @worker and append what it derives to @acc, creating @acc like
 * @head on first use.  Begin and finish run on the calling thread, which the
 * input readers they take are bound to. */
static int
insert_run_piece(wl_col_session_t *sess, wl_col_session_t *worker,
    const wl_plan_stratum_t *sp, const insert_piece_t *p,
    const col_rel_t *head, col_rel_t **acc)
{
    static const int snapshot_token = 0;
    const wl_columnar_eval_tdd_plan_manifest_t *m = p->m;
    const wl_columnar_eval_tdd_plan_slice_t *sl = &m->slices[p->slice];
    wl_columnar_eval_tdd_input_t *inputs = calloc(sl->read_count,
            sizeof(*inputs));
    wl_columnar_eval_tdd_run_t run;
    col_rel_t *out = NULL;
    int rc = 0;

    if (!inputs)
        return ENOMEM;
    for (uint32_t r = 0; r < sl->read_count; r++) {
        const wl_columnar_eval_tdd_plan_read_t *read =
            &m->reads[sl->read_start + r];
        bool ranged = p->split && r == p->lead;
        col_rel_t *rel = ranged ? p->split->range[p->piece] : p->rels[r];
        inputs[r] = (wl_columnar_eval_tdd_input_t){
            .read = read, .relation = rel, .name = rel->name,
            .snapshot = &snapshot_token,
            .partition = ranged ? (const void *)p->split : NULL,
            .worker_index = ranged ? p->piece : 0,
            .worker_count = ranged ? p->split->count : 1,
            .view_generation = rel->view_generation,
            .storage_generation = rel->storage_generation,
            .ncols = rel->ncols, .declared_ncols = rel->declared_ncols,
            .schema_ok = rel->schema_ok,
            .column_names = (const char *const *)rel->col_names,
            .column_types = rel->column_types,
            .compound_kind = rel->compound_kind,
            .compound_count = rel->compound_count,
            .compound_arity_len = rel->compound_arity_len,
            .compound_arity_map = rel->compound_arity_map,
            .inline_physical_offset = rel->inline_physical_offset,
            .has_graph_column = rel->has_graph_column,
            .graph_col_idx = rel->graph_col_idx,
        };
    }
    memset(&run, 0, sizeof(run));
    rc = wl_columnar_eval_tdd_run_bound_slice_begin(worker, sp, m, p->slice,
            &snapshot_token, p->split, p->split ? p->piece : 0,
            p->split ? p->split->count : 1, inputs, sl->read_count, &run);
    if (rc == 0)
        rc = wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out);
    if (rc != 0 || !out || out->nrows == 0)
        goto done;
    if (!*acc) {
        /* A head no evaluation has written yet has no schema; the rows
         * derived for it give it one, as in the stratum loop. */
        rc = wl_columnar_relation_new_like_governed_checked_mode("$insert",
                head->ncols ? head : out, sess->memory_governor, false, acc);
        if (rc != 0)
            goto done;
    }
    rc = col_rel_append_all(*acc, out, NULL);
done:
    if (out)
        col_rel_destroy(out);
    free(inputs);
    return rc;
}

/* A task: pull runs until none is left or another task failed. */
static void
insert_task_fn(void *arg)
{
    insert_task_t *t = (insert_task_t *)arg;
    insert_round_t *round = t->round;
#ifdef WL_SESSION_TEST_HOOKS
    bool first = true;
#endif
    for (;;) {
        if (atomic_load_explicit(&round->stop, memory_order_acquire))
            break;
        uint64_t i = (uint64_t)atomic_fetch_add_explicit(&round->next, 1,
                memory_order_relaxed);
        if (i >= round->npieces)
            break;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_eval_tdd_insert_test_piece_start)
            wl_columnar_eval_tdd_insert_test_piece_start(t->task,
                round->tasks, first);
        first = false;
#endif
        const insert_piece_t *p = &round->pieces[i];
        int rc = insert_run_piece(round->sess, t->worker, round->sp, p,
                round->heads[p->ri], &t->acc[p->ri]);
        if (rc != 0) {
            t->rc = rc;
            atomic_store_explicit(&round->stop, 1, memory_order_release);
            break;
        }
    }
}

/* Rows per range below which a leading input is not split further:
 * WIRELOG_TDD_MIN_ROWS_PER_WORKER when set to a positive integer, else 16.
 * The full path reads the same variable with a default of 4096; the
 * instances here are much smaller units of work. */
static uint64_t
insert_min_rows(void)
{
    const char *env = getenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    if (env && env[0] != '\0') {
        char *endp = NULL;
        errno = 0;
        unsigned long long v = strtoull(env, &endp, 10);
        if (endp != env && *endp == '\0' && errno != ERANGE && v > 0)
            return (uint64_t)v;
    }
    return 16;
}

/* How many ranges a leading input of @nrows rows is split into in a session
 * with @workers workers: up to twice the workers, to give the tasks more
 * runs than there are workers so that runs of uneven cost balance better,
 * and never ranges below @min_rows rows. */
static uint32_t
insert_range_count(uint32_t nrows, uint32_t workers, uint64_t min_rows)
{
    if (workers <= 1 || nrows < 2)
        return 1;
    uint64_t count = nrows / min_rows;
    uint64_t cap = (uint64_t)workers * 2u;
    if (count > cap)
        count = cap;
    if (count > nrows)
        count = nrows;
    return count < 1 ? 1 : (uint32_t)count;
}

static void
insert_split_free(insert_split_t *split)
{
    if (!split->range)
        return;
    for (uint32_t k = 0; k < split->count; k++)
        col_rel_destroy(split->range[k]);
    free((void *)split->range);
    split->range = NULL;
}

/* The split of @src into @count ranges, built on first request in a round
 * and shared by every run that leads with @src. */
static int
insert_split_get(wl_col_session_t *sess, insert_split_t *splits,
    uint32_t *nsplits, const col_rel_t *src, uint32_t count,
    const insert_split_t **out)
{
    for (uint32_t i = 0; i < *nsplits; i++) {
        if (splits[i].src == src && splits[i].count == count) {
            *out = &splits[i];
            return 0;
        }
    }
    insert_split_t *split = &splits[(*nsplits)++];
    split->src = src;
    split->count = count;
    split->range = (col_rel_t **)calloc(count, sizeof(*split->range));
    if (!split->range)
        return ENOMEM;
    uint32_t n = src->nrows;
    for (uint32_t k = 0; k < count; k++) {
        int rc = wl_columnar_relation_copy_rows(src->name, src,
                (uint32_t)((uint64_t)n * k / count),
                (uint32_t)((uint64_t)n * (k + 1) / count),
                sess->memory_governor, &split->range[k]);
        if (rc != 0)
            return rc;
    }
    *out = split;
    return 0;
}

/* Clear on @worker the session states the bound runner refuses -- they
 * choose operands by name or batch a join, and a clone copies them from the
 * coordinator -- which also turns off, for these runs, the bounded join
 * batching a session may have asked for, strict mode included.  A round
 * with more than one task tells extension calls so, which refuses a
 * callback not declared thread-safe. */
static void
insert_clone_prepare(wl_col_session_t *worker, uint32_t tasks)
{
    worker->delta_seeded = false;
    worker->retraction_seeded = false;
    worker->retraction_right_pass = false;
    worker->diff_operators_active = false;
    worker->join_batch_bytes = 0;
    worker->join_batch_strict = false;
    worker->callback_active_workers = tasks ? tasks : 1;
    worker->callback_parallel_execution = tasks > 1;
    worker->extension_expr_status = 0;
}

/* Merge the @nbags bags in @bags (NULL entries skipped) into @head and
 * leave in @added the rows that were not already there, untimed.  @head's
 * rows must be sorted and unique. */
static int
insert_merge(wl_col_session_t *sess, col_rel_t *head,
    col_rel_t *const *bags, uint32_t nbags, col_rel_t **added)
{
    uint32_t old_nrows = head->nrows;
    col_rel_t *fresh = NULL;
    int fast = 0;
    int rc = 0;
    for (uint32_t b = 0; b < nbags; b++) {
        const col_rel_t *acc = bags[b];
        if (!acc)
            continue;
        if (head->ncols == 0 && acc->ncols > 0) {
            rc = col_rel_set_schema(head, acc->ncols,
                    (const char *const *)acc->col_names);
            if (rc != 0)
                return rc;
        }
        rc = col_rel_append_all(head, acc, NULL);
        if (rc != 0)
            return rc;
    }
    /* The appended rows come from untimed outputs, and appending copies
     * timestamps only between two timestamped relations, so a timestamped
     * head would carry indeterminate slots into the merge below. */
    if (head->timestamps)
        memset(&head->timestamps[old_nrows], 0,
            (size_t)(head->nrows - old_nrows) * sizeof(*head->timestamps));
    rc = wl_columnar_relation_new_like_governed_checked_mode("$insert_new",
            head, sess->memory_governor, false, &fresh);
    if (rc != 0)
        return rc;
    if (head->timestamps) {
        rc = col_rel_enable_timestamps(fresh);
        if (rc != 0)
            goto done;
    }
    rc = col_op_consolidate_incremental_delta(head, old_nrows, fresh, &fast);
    col_session_invalidate_arrangements(&sess->base, head->name);
    if (rc != 0)
        goto done;
    if (fresh->nrows)
        rc = insert_untimed_copy(fresh, "$insert_delta", sess, added);
done:
    col_rel_destroy(fresh);
    return rc;
}

/* Append @add's rows to @*total, creating it like @add on first use. */
static int
insert_accumulate(wl_col_session_t *sess, const col_rel_t *add,
    col_rel_t **total)
{
    if (!*total)
        return insert_untimed_copy(add, "$insert_head_delta", sess, total);
    return col_rel_append_all(*total, add, NULL);
}

/* Issue #2122: list the round's runs.  For each instance with a non-empty
 * driver, resolve every read on the coordinator and split the leading input
 * into ranges when the session has workers to spread them over. */
static int
insert_round_plan(wl_col_session_t *sess, const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *ms, const char *const *changed,
    col_rel_t *const *changed_delta, uint32_t nchanged,
    col_rel_t *const *round_delta, bool first, uint32_t workers,
    uint64_t min_rows, insert_split_t *splits, uint32_t *nsplits,
    insert_piece_t **pieces_out, uint32_t *npieces_out, col_rel_t ***rels_out,
    uint32_t *units_out)
{
    uint32_t nunits = 0, npieces = 0, nreads = 0;
    insert_piece_t *pieces = NULL;
    col_rel_t **rels = NULL;
    int rc = 0;

    *pieces_out = NULL;
    *npieces_out = 0;
    *rels_out = NULL;
    *units_out = 0;
    /* Size the arrays: every read of every slice, and up to 2 * workers
     * ranges for each slice. */
    uint64_t max_pieces = 0, all_reads = 0;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        for (uint32_t s = 0; s < ms[ri].slice_count; s++) {
            all_reads += ms[ri].slices[s].read_count;
            max_pieces += workers > 1 ? (uint64_t)workers * 2u : 1u;
        }
    }
    if (max_pieces > UINT32_MAX || all_reads > UINT32_MAX)
        return EOVERFLOW;
    nreads = (uint32_t)all_reads;
    pieces = (insert_piece_t *)calloc(max_pieces ? max_pieces : 1,
            sizeof(*pieces));
    rels = (col_rel_t **)calloc(nreads ? nreads : 1, sizeof(*rels));
    if (!pieces || !rels) {
        rc = ENOMEM;
        goto done;
    }
    uint32_t used_reads = 0;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_columnar_eval_tdd_plan_manifest_t *m = &ms[ri];
        for (uint32_t s = 0; s < m->slice_count; s++) {
            const wl_columnar_eval_tdd_plan_slice_t *sl = &m->slices[s];
            const wl_plan_op_t *driver = &m->owner_ops[sl->driver];
            const char *name = driver->op == WL_PLAN_OP_VARIABLE
                ? driver->relation_name : driver->right_relation;
            col_rel_t *delta = insert_driver_delta(sp, name, changed,
                    changed_delta, nchanged, round_delta, first);
            if (!delta || delta->nrows == 0)
                continue;
            if (sl->read_count > nreads - used_reads) {
                rc = EOVERFLOW;
                goto done;
            }
            col_rel_t **r = &rels[used_reads];
            uint32_t lead = UINT32_MAX;
            for (uint32_t k = 0; k < sl->read_count; k++) {
                const wl_columnar_eval_tdd_plan_read_t *read =
                    &m->reads[sl->read_start + k];
                r[k] = read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
                    ? delta : session_find_rel(sess, read->relation_name);
                if (!r[k]) {
                    rc = ENOENT;
                    goto done;
                }
                if (read->op_index == sl->start)
                    lead = k;
                /* A relation's storage owner is set up lazily under a
                 * writer gate; do it here, before any clone reads it. */
                rc = col_rel_storage_owner_ensure_initialized(r[k]);
                if (rc != 0)
                    goto done;
            }
            if (lead == UINT32_MAX) {
                rc = EINVAL;
                goto done;
            }
            used_reads += sl->read_count;
            nunits++;
            uint32_t count = insert_range_count(r[lead]->nrows, workers,
                    min_rows);
            const insert_split_t *split = NULL;
            if (count > 1) {
                rc = insert_split_get(sess, splits, nsplits, r[lead], count,
                        &split);
                if (rc != 0)
                    goto done;
                for (uint32_t k = 0; k < count; k++) {
                    rc = col_rel_storage_owner_ensure_initialized(
                        split->range[k]);
                    if (rc != 0)
                        goto done;
                }
            }
            if (count > max_pieces - npieces) {
                rc = EOVERFLOW;
                goto done;
            }
            for (uint32_t k = 0; k < count; k++)
                pieces[npieces++] = (insert_piece_t){
                    .m = m, .ri = ri, .slice = s, .lead = lead, .rels = r,
                    .split = split, .piece = k,
                };
        }
    }
done:
    if (rc != 0) {
        free(pieces);
        free((void *)rels);
        return rc;
    }
    *pieces_out = pieces;
    *npieces_out = npieces;
    *rels_out = rels;
    *units_out = nunits;
    return 0;
}

int
wl_columnar_eval_tdd_insert_stratum(wl_col_session_t *sess,
    const wl_plan_stratum_t *sp, const char *const *changed,
    col_rel_t *const *changed_delta, uint32_t nchanged,
    col_rel_t **head_delta)
{
    if (!sess || !sp || !head_delta || (nchanged && (!changed
        || !changed_delta)))
        return EINVAL;
    uint32_t nrels = sp->relation_count;
    for (uint32_t ri = 0; ri < nrels; ri++)
        head_delta[ri] = NULL;
    for (uint32_t c = 0; c < nchanged; c++) {
        if (!changed[c] || !changed_delta[c] || changed_delta[c]->timestamps)
            return EINVAL;
    }

    /* Workers this stratum may spread a round over.  A session whose
     * extensions include a callback not declared thread-safe runs its
     * rounds on one clone, as it would have before #2122. */
    uint32_t workers = sess->num_workers ? sess->num_workers : 1;
    if (!wl_extension_snapshot_all_thread_safe(sess->base.extension_snapshot))
        workers = 1;
    uint64_t min_rows = insert_min_rows();

    /* Admission: every manifest is built before anything is mutated, so a
     * relation the binder refuses leaves the session as it was. */
    wl_columnar_eval_tdd_plan_manifest_t *round0 = calloc(nrels ? nrels : 1,
            sizeof(*round0));
    wl_columnar_eval_tdd_plan_manifest_t *later = calloc(nrels ? nrels : 1,
            sizeof(*later));
    col_rel_t **heads = (col_rel_t **)calloc(nrels ? nrels : 1,
            sizeof(*heads));
    col_rel_t **cur = (col_rel_t **)calloc(nrels ? nrels : 1,
            sizeof(*cur));
    col_rel_t **next = (col_rel_t **)calloc(nrels ? nrels : 1,
            sizeof(*next));
    col_rel_t **bags = (col_rel_t **)calloc((size_t)workers * nrels + 1,
            sizeof(*bags));
    col_rel_t **merge = (col_rel_t **)calloc(workers, sizeof(*merge));
    wl_col_session_t *clones = (wl_col_session_t *)calloc(workers,
            sizeof(*clones));
    insert_task_t *tasks = (insert_task_t *)calloc(workers, sizeof(*tasks));
    insert_split_t *splits = NULL;
    uint32_t nsplits = 0;
    uint32_t nclones = 0;
    insert_piece_t *pieces = NULL;
    col_rel_t **piece_rels = NULL;
    int rc = 0;

    if (!round0 || !later || !heads || !cur || !next || !bags || !merge
        || !clones || !tasks) {
        rc = ENOMEM;
        goto done;
    }
    uint64_t slices0 = 0, slices1 = 0;
    for (uint32_t ri = 0; ri < nrels; ri++) {
        rc = wl_columnar_eval_tdd_plan_insert_bindings(sp, ri, changed,
                nchanged, true, &round0[ri]);
        if (rc == 0 && sp->is_recursive)
            rc = wl_columnar_eval_tdd_plan_insert_bindings(sp, ri, NULL, 0,
                    false, &later[ri]);
        if (rc != 0)
            goto done;
        heads[ri] = session_find_rel(sess, sp->relations[ri].name);
        if (!heads[ri]) {
            rc = ENOENT;
            goto done;
        }
        slices0 += round0[ri].slice_count;
        slices1 += later[ri].slice_count;
    }
    /* A round builds at most one split per slice it runs. */
    uint64_t max_slices = slices0 > slices1 ? slices0 : slices1;
    splits = (insert_split_t *)calloc(max_slices ? max_slices : 1,
            sizeof(*splits));
    if (!splits) {
        rc = ENOMEM;
        goto done;
    }

    /* The merge below needs each head sorted and unique. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *head = heads[ri];
        if (head->nrows > 1) {
            rc = wl_columnar_eval_delta_consolidate(head, sess);
            col_session_invalidate_arrangements(&sess->base, head->name);
            if (rc != 0)
                goto done;
        }
    }

    for (bool first = true;; first = false) {
        const wl_columnar_eval_tdd_plan_manifest_t *ms = first ? round0
            : later;
        bool any = false;
        uint32_t npieces = 0, nunits = 0;
        rc = insert_round_plan(sess, sp, ms, changed, changed_delta, nchanged,
                cur, first, workers, min_rows, splits, &nsplits, &pieces,
                &npieces, &piece_rels, &nunits);
        if (rc != 0)
            goto done;

        uint32_t ntasks = npieces < workers ? npieces : workers;
        if (ntasks == 0)
            ntasks = 1;
        if (ntasks > 1) {
            rc = wl_columnar_session_ensure_workqueue(sess, ntasks);
            if (rc != 0)
                goto done;
        }
        while (nclones < ntasks) {
            rc = col_worker_session_create(sess, nclones, NULL, 0,
                    &clones[nclones]);
            if (rc != 0)
                goto done;
            nclones++;
        }
        /* next and stop start at zero: the initializer leaves them out. */
        insert_round_t round = {
            .sess = sess, .sp = sp, .heads = heads, .pieces = pieces,
            .npieces = npieces, .tasks = ntasks,
        };
        for (uint32_t t = 0; t < ntasks; t++) {
            insert_clone_prepare(&clones[t], ntasks);
            tasks[t] = (insert_task_t){
                .round = &round, .worker = &clones[t],
                .acc = &bags[(size_t)t * nrels], .task = t, .rc = 0,
            };
        }
        if (ntasks == 1) {
            insert_task_fn(&tasks[0]);
        } else {
            uint32_t submitted = 0;
            for (; submitted < ntasks; submitted++) {
                if (wl_workqueue_submit(sess->wq, insert_task_fn,
                    &tasks[submitted]) != 0)
                    break;
            }
            if (submitted < ntasks) {
                atomic_store_explicit(&round.stop, 1, memory_order_release);
                rc = wl_workqueue_drain(sess->wq) != 0 ? EIO : ENOMEM;
                goto done;
            }
            if (wl_workqueue_wait_all(sess->wq) != 0) {
                rc = EIO;
                goto done;
            }
        }
        /* The barrier: every task is done.  The first failure, in task
         * order, fails the stratum. */
        for (uint32_t t = 0; t < ntasks && rc == 0; t++)
            rc = tasks[t].rc;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_eval_tdd_insert_test_round)
            wl_columnar_eval_tdd_insert_test_round(sp, nunits, npieces,
                ntasks);
#endif
        free(pieces);
        pieces = NULL;
        free((void *)piece_rels);
        piece_rels = NULL;
        for (uint32_t i = 0; i < nsplits; i++)
            insert_split_free(&splits[i]);
        nsplits = 0;
        if (rc != 0)
            goto done;

        /* Merge at the round's end, so every run of a round read the same
         * heads. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            uint32_t nbags = 0;
            for (uint32_t t = 0; t < ntasks; t++) {
                merge[t] = bags[(size_t)t * nrels + ri];
                nbags += merge[t] != NULL;
            }
            if (nbags == 0)
                continue;
            rc = insert_merge(sess, heads[ri], merge, ntasks, &next[ri]);
            for (uint32_t t = 0; t < ntasks; t++) {
                col_rel_destroy(bags[(size_t)t * nrels + ri]);
                bags[(size_t)t * nrels + ri] = NULL;
            }
            if (rc != 0)
                goto done;
            if (next[ri]) {
                rc = insert_accumulate(sess, next[ri], &head_delta[ri]);
                if (rc != 0)
                    goto done;
                any = true;
            }
        }
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_destroy(cur[ri]);
            cur[ri] = next[ri];
            next[ri] = NULL;
        }
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_eval_tdd_insert_test_after_merge) {
            rc = wl_columnar_eval_tdd_insert_test_after_merge(sp);
            if (rc != 0)
                goto done;
        }
#endif
        if (!any || !sp->is_recursive)
            break;
    }

done:
    for (uint32_t c = 0; c < nclones; c++) {
        int destroy_rc = col_worker_session_destroy(&clones[c]);
        if (rc == 0)
            rc = destroy_rc;
    }
    free(pieces);
    free((void *)piece_rels);
    for (uint32_t i = 0; splits && i < nsplits; i++)
        insert_split_free(&splits[i]);
    free(splits);
    if (bags) {
        for (size_t b = 0; b < (size_t)workers * nrels; b++)
            col_rel_destroy(bags[b]);
    }
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (round0)
            wl_columnar_eval_tdd_plan_bindings_free(&round0[ri]);
        if (later)
            wl_columnar_eval_tdd_plan_bindings_free(&later[ri]);
        if (cur)
            col_rel_destroy(cur[ri]);
        if (next)
            col_rel_destroy(next[ri]);
        if (rc != 0) {
            col_rel_destroy(head_delta[ri]);
            head_delta[ri] = NULL;
        }
    }
    free(round0);
    free(later);
    free((void *)heads);
    free((void *)cur);
    free((void *)next);
    free((void *)bags);
    free((void *)merge);
    free(clones);
    free(tasks);
    return rc;
}
