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
 */

#include "internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#ifdef WL_SESSION_TEST_HOOKS
int (*wl_columnar_eval_tdd_insert_test_after_merge)(
    const wl_plan_stratum_t *sp);
#endif

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

/* Run instance @slice of @m on @worker and append what it derives to
 * @acc, creating @acc like @head on first use. */
static int
insert_run_instance(wl_col_session_t *sess, wl_col_session_t *worker,
    const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *m, uint32_t slice,
    col_rel_t *delta, const col_rel_t *head, col_rel_t **acc)
{
    static const int snapshot_token = 0;
    const wl_columnar_eval_tdd_plan_slice_t *sl = &m->slices[slice];
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
        col_rel_t *rel = read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
            ? delta : session_find_rel(sess, read->relation_name);
        if (!rel) {
            rc = ENOENT;
            goto done;
        }
        inputs[r] = (wl_columnar_eval_tdd_input_t){
            .read = read, .relation = rel, .name = rel->name,
            .snapshot = &snapshot_token, .partition = NULL,
            .worker_index = 0, .worker_count = 1,
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
    rc = wl_columnar_eval_tdd_run_bound_slice_begin(worker, sp, m, slice,
            &snapshot_token, NULL, 0, 1, inputs, sl->read_count, &run);
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

/* Merge @acc into @head and leave in @added the rows that were not
 * already there, untimed.  @head's rows must be sorted and unique. */
static int
insert_merge(wl_col_session_t *sess, col_rel_t *head, const col_rel_t *acc,
    col_rel_t **added)
{
    uint32_t old_nrows = head->nrows;
    col_rel_t *fresh = NULL;
    int fast = 0;
    int rc = 0;
    if (head->ncols == 0 && acc->ncols > 0) {
        rc = col_rel_set_schema(head, acc->ncols,
                (const char *const *)acc->col_names);
        if (rc != 0)
            return rc;
    }
    rc = col_rel_append_all(head, acc, NULL);
    if (rc != 0)
        return rc;
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

    /* Admission: every manifest is built before anything is mutated, so a
     * relation the binder refuses leaves the session as it was. */
    wl_columnar_eval_tdd_plan_manifest_t *round0 = calloc(nrels ? nrels : 1,
            sizeof(*round0));
    wl_columnar_eval_tdd_plan_manifest_t *later = calloc(nrels ? nrels : 1,
            sizeof(*later));
    col_rel_t **acc = (col_rel_t **)calloc(nrels ? nrels : 1,
            sizeof(*acc));
    col_rel_t **cur = (col_rel_t **)calloc(nrels ? nrels : 1,
            sizeof(*cur));
    col_rel_t **next = (col_rel_t **)calloc(nrels ? nrels : 1,
            sizeof(*next));
    wl_col_session_t worker;
    bool worker_live = false;
    int rc = 0;

    memset(&worker, 0, sizeof(worker));
    if (!round0 || !later || !acc || !cur || !next) {
        rc = ENOMEM;
        goto done;
    }
    for (uint32_t ri = 0; ri < nrels; ri++) {
        rc = wl_columnar_eval_tdd_plan_insert_bindings(sp, ri, changed,
                nchanged, true, &round0[ri]);
        if (rc == 0 && sp->is_recursive)
            rc = wl_columnar_eval_tdd_plan_insert_bindings(sp, ri, NULL, 0,
                    false, &later[ri]);
        if (rc != 0)
            goto done;
        if (!session_find_rel(sess, sp->relations[ri].name)) {
            rc = ENOENT;
            goto done;
        }
    }

    /* The instances run serially on one worker clone.  The bound runner
     * refuses session states that choose operands by name or batch a join;
     * a clone copies them from the coordinator, so they are cleared here, on
     * the clone only.  That also turns off, for these runs, the bounded join
     * batching a session may have asked for, strict mode included. */
    rc = col_worker_session_create(sess, 0, NULL, 0, &worker);
    if (rc != 0)
        goto done;
    worker_live = true;
    worker.delta_seeded = false;
    worker.retraction_seeded = false;
    worker.retraction_right_pass = false;
    worker.diff_operators_active = false;
    worker.join_batch_bytes = 0;
    worker.join_batch_strict = false;

    /* The merge below needs each head sorted and unique. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *head = session_find_rel(sess, sp->relations[ri].name);
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
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *head = session_find_rel(sess, sp->relations[ri].name);
            for (uint32_t s = 0; s < ms[ri].slice_count; s++) {
                const wl_columnar_eval_tdd_plan_slice_t *sl = &ms[ri].slices[s];
                const wl_plan_op_t *driver = &ms[ri].owner_ops[sl->driver];
                const char *name = driver->op == WL_PLAN_OP_VARIABLE
                    ? driver->relation_name : driver->right_relation;
                col_rel_t *delta = insert_driver_delta(sp, name, changed,
                        changed_delta, nchanged, cur, first);
                if (!delta || delta->nrows == 0)
                    continue;
                rc = insert_run_instance(sess, &worker, sp, &ms[ri], s, delta,
                        head, &acc[ri]);
                if (rc != 0)
                    goto done;
            }
        }
        /* Merge at the round's end, so every instance of a round read the
         * same heads. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            if (!acc[ri])
                continue;
            col_rel_t *head = session_find_rel(sess, sp->relations[ri].name);
            rc = insert_merge(sess, head, acc[ri], &next[ri]);
            col_rel_destroy(acc[ri]);
            acc[ri] = NULL;
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
    if (worker_live) {
        int destroy_rc = col_worker_session_destroy(&worker);
        if (rc == 0)
            rc = destroy_rc;
    }
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (round0)
            wl_columnar_eval_tdd_plan_bindings_free(&round0[ri]);
        if (later)
            wl_columnar_eval_tdd_plan_bindings_free(&later[ri]);
        if (acc)
            col_rel_destroy(acc[ri]);
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
    free((void *)acc);
    free((void *)cur);
    free((void *)next);
    return rc;
}
