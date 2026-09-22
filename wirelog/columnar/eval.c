/*
 * columnar/eval.c - wirelog Columnar Backend Evaluator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Stratum evaluation, relation plan dispatch, and delta/frontier tracking
 * extracted from backend/columnar_nanoarrow.c for modular compilation.
 */

#define _GNU_SOURCE

#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TDD_OWNER_FALLBACK_MIN_ITER 31u
#define TDD_OWNER_FALLBACK_DELTA_ROWS 512u

/* Only the standalone decision regression targets enable this wrapper. */
#ifdef WL_COLUMNAR_EVAL_TEST_SUBMISSION
extern int
wl_columnar_eval_test_submit(wl_work_queue_t *wq,
    void (*fn)(void *), void *ctx);
extern void
wl_columnar_eval_test_before_worker_cleanup(wl_col_session_t *coord);
#define WL_COLUMNAR_EVAL_SUBMIT wl_columnar_eval_test_submit
#else
#define WL_COLUMNAR_EVAL_SUBMIT wl_workqueue_submit
#endif

#ifdef WL_COLUMNAR_EVAL_TEST_OWNER_LIFETIME
extern void
wl_columnar_eval_test_tdd_worker_start(wl_col_session_t *worker);
#endif

static void
tdd_destroy_delta_payload(void *payload)
{
    col_rel_destroy((col_rel_t *)payload);
}

static bool
wl_columnar_eval_tdd_owner_lifetime_contains(
    const wl_columnar_eval_tdd_owner_lifetime_t *lifetime,
    const col_rel_t *relation)
{
    size_t count = lifetime->matrix_slots + lifetime->overflow_slots;
    for (size_t i = 0; i < count; i++)
        if (lifetime->relations[i] == relation)
            return true;
    return false;
}

static int
wl_columnar_eval_tdd_owner_lifetime_capture_queue(wl_col_session_t *coord,
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime)
{
    wl_delta_msg_t msg;

    if (!lifetime->queue)
        return 0;
    while (wl_mpsc_dequeue(lifetime->queue, &msg)) {
        if (!msg.delta)
            continue;
        wl_mem_ledger_free(&coord->mem_ledger, WL_MEM_SUBSYS_CHANNEL,
            col_rel_transport_bytes((const col_rel_t *)msg.delta));
        col_rel_t *relation = (col_rel_t *)msg.delta;
        if (wl_columnar_eval_tdd_owner_lifetime_contains(lifetime, relation))
            continue;
        size_t slot = lifetime->matrix_slots;
        if (msg.worker_id < lifetime->worker_count
            && msg.rel_idx < lifetime->relation_count) {
            size_t candidate = (size_t)msg.worker_id
                * lifetime->relation_count + msg.rel_idx;
            if (!lifetime->relations[candidate])
                slot = candidate;
        }
        if (slot == lifetime->matrix_slots) {
            size_t overflow = 0;
            while (overflow < lifetime->overflow_slots
                && lifetime->relations[lifetime->matrix_slots + overflow])
                overflow++;
            if (overflow == lifetime->overflow_slots) {
                /* This storage was sized for every message slot in every
                 * worker ring before dispatch. Losing the dequeued payload
                 * here would violate the queue's single-owner contract. */
                abort();
            }
            slot = lifetime->matrix_slots + overflow;
        }
        lifetime->relations[slot] = relation;
    }
    return 0;
}

static int
wl_columnar_eval_tdd_owner_lifetime_dispose_slots(
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime, size_t destroy_count);

int
wl_columnar_eval_tdd_owner_lifetime_retry(wl_col_session_t *coord)
{
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime;
    size_t count;

    if (!coord)
        return EINVAL;
    lifetime = coord->tdd_owner_lifetime;
    if (!lifetime)
        return 0;
    if (lifetime->evaluation_active || lifetime->dispatch_active)
        return EBUSY;
    int rc = wl_columnar_eval_tdd_owner_lifetime_capture_queue(coord, lifetime);
    if (rc != 0)
        return rc;
    count = lifetime->matrix_slots + lifetime->overflow_slots;
    rc = wl_columnar_eval_tdd_owner_lifetime_dispose_slots(lifetime, count);
    if (rc != 0)
        return rc;

    if (lifetime->queue) {
        wl_mem_ledger_free(&coord->mem_ledger, WL_MEM_SUBSYS_CHANNEL,
            lifetime->queue_ring_bytes);
        if (coord->delta_queue == lifetime->queue) {
            coord->delta_queue = NULL;
            coord->mem_channel_ring_bytes = 0;
        }
        wl_mpsc_queue_destroy(lifetime->queue);
        lifetime->queue = NULL;
    }
    coord->tdd_owner_lifetime = NULL;
    free(lifetime);
    return 0;
}

int
wl_columnar_eval_tdd_owner_lifetime_quiesce(wl_col_session_t *coord)
{
    if (!coord)
        return EINVAL;
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime
        = coord->tdd_owner_lifetime;
    if (!lifetime)
        return 0;
    if (lifetime->evaluation_active)
        return EBUSY;
    if (!lifetime->dispatch_active)
        return 0;
    if (!coord->wq)
        return EBUSY;
    int rc = wl_workqueue_drain(coord->wq);
    if (rc != 0)
        return rc;
    lifetime->dispatch_active = false;
    free(lifetime->worker_ctxs);
    lifetime->worker_ctxs = NULL;
    return 0;
}

static int
wl_columnar_eval_tdd_owner_lifetime_create(wl_col_session_t *coord,
    col_eval_tdd_worker_ctx_t *ctxs, uint32_t workers, uint32_t nrels,
    uint32_t queue_capacity)
{
    size_t matrix_slots;
    size_t overflow_slots;
    size_t total_slots;
    size_t slot_bytes;
    size_t allocation_bytes;
    uint32_t ring_capacity = 2;
    int rc;

    if (!coord || !ctxs || workers == 0 || nrels == 0
        || coord->tdd_owner_lifetime)
        return EINVAL;
    rc = wl_columnar_eval_checked_size_mul(workers, nrels, &matrix_slots);
    if (rc != 0)
        return rc;
    while (ring_capacity < queue_capacity) {
        if (ring_capacity > UINT32_MAX / 2u)
            return EOVERFLOW;
        ring_capacity *= 2u;
    }
    rc = wl_columnar_eval_checked_size_mul(workers, ring_capacity,
            &overflow_slots);
    if (rc != 0 || overflow_slots > SIZE_MAX - matrix_slots)
        return EOVERFLOW;
    total_slots = matrix_slots + overflow_slots;
    rc = wl_columnar_eval_checked_size_mul(total_slots, sizeof(col_rel_t *),
            &slot_bytes);
    if (rc != 0 || slot_bytes > SIZE_MAX - sizeof(
            wl_columnar_eval_tdd_owner_lifetime_t))
        return EOVERFLOW;
    allocation_bytes = sizeof(wl_columnar_eval_tdd_owner_lifetime_t) +
        slot_bytes;
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime = calloc(1,
            allocation_bytes);
    if (!lifetime)
        return ENOMEM;
    lifetime->worker_count = workers;
    lifetime->relation_count = nrels;
    lifetime->matrix_slots = matrix_slots;
    lifetime->overflow_slots = overflow_slots;
    lifetime->evaluation_active = true;
    lifetime->worker_ctxs = ctxs;
    for (uint32_t w = 0; w < workers; w++)
        ctxs[w].delta_rels = lifetime->relations + (size_t)w * nrels;
    coord->tdd_owner_lifetime = lifetime;
    return 0;
}

static int
wl_columnar_eval_tdd_owner_lifetime_dispose_all(
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime)
{
    if (!lifetime)
        return EINVAL;
    return wl_columnar_eval_tdd_owner_lifetime_dispose_slots(lifetime,
               lifetime->matrix_slots + lifetime->overflow_slots);
}

static int
wl_columnar_eval_tdd_owner_lifetime_dispose_slots(
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime, size_t destroy_count)
{
    size_t total;
    int first_rc = 0;
    if (!lifetime)
        return EINVAL;
    total = lifetime->matrix_slots + lifetime->overflow_slots;
    if (destroy_count > total)
        return EINVAL;
    /* Canonicalize every aliased pointer before a destructor can free it. */
    for (size_t i = 0; i < total; i++) {
        col_rel_t *relation = lifetime->relations[i];
        if (!relation)
            continue;
        for (size_t j = 0; j < i; j++) {
            if (lifetime->relations[j] == relation) {
                lifetime->relations[i] = NULL;
                break;
            }
        }
    }
    /* Keep making progress so aliases can release roots that refused earlier
     * in the same pass. The number of successful disposals is bounded. */
    for (size_t pass = 0; pass <= destroy_count; pass++) {
        bool progress = false;
        first_rc = 0;
        for (size_t i = 0; i < destroy_count; i++) {
            col_rel_t *relation = lifetime->relations[i];
            if (!relation)
                continue;
            int rc = col_rel_destroy_checked(relation);
            if (rc == 0) {
                lifetime->relations[i] = NULL;
                progress = true;
            } else if (first_rc == 0) {
                first_rc = rc;
            }
        }
        if (!progress)
            break;
    }
    for (size_t i = 0; i < destroy_count; i++)
        if (lifetime->relations[i])
            return first_rc != 0 ? first_rc : EBUSY;
    return 0;
}

static int
wl_columnar_eval_tdd_owner_lifetime_dispose_slot(wl_col_session_t *coord,
    col_rel_t **slot)
{
    if (!coord || !slot)
        return EINVAL;
    if (!*slot)
        return 0;
    if (!coord->tdd_owner_lifetime)
        return EINVAL;
    size_t total = coord->tdd_owner_lifetime->matrix_slots
        + coord->tdd_owner_lifetime->overflow_slots;
    for (size_t i = 0; i < total; i++) {
        if (&coord->tdd_owner_lifetime->relations[i] != slot
            && coord->tdd_owner_lifetime->relations[i] == *slot)
            coord->tdd_owner_lifetime->relations[i] = NULL;
    }
    int rc = col_rel_destroy_checked(*slot);
    if (rc == 0)
        *slot = NULL;
    return rc;
}

#define WL_COLUMNAR_EVAL_DEDUP_ROW_HASH wl_columnar_eval_dedup_row_hash
#define WL_COLUMNAR_EVAL_DEDUP_SET_INSERT wl_columnar_eval_dedup_set_insert
#define WL_COLUMNAR_EVAL_DEDUP_SET_CONTAINS wl_columnar_eval_dedup_set_contains
#define WL_COLUMNAR_EVAL_DEDUP_SET_INIT_FROM_REL \
        wl_columnar_eval_dedup_set_init_from_rel

int
wl_columnar_eval_delta_queue_capacity(uint32_t nrels, uint32_t *out)
{
    if (!out)
        return EINVAL;
    /* wl_mpsc_queue_create rounds capacity to a uint32 power of two;
     * 2^31 is its largest non-wrapping request on the current base. */
    if (nrels > (UINT32_C(1) << 30))
        return EOVERFLOW;
    *out = nrels > 0 ? nrels * 2u : 2u;
    return 0;
}

int
wl_columnar_eval_tdd_matrix_size(uint32_t W, uint32_t nrels,
    size_t element_size, size_t *out)
{
    if (!out || element_size == 0)
        return EINVAL;
    if (W == 0 || nrels == 0) {
        *out = 0;
        return 0;
    }
    if ((size_t)nrels > SIZE_MAX / (size_t)W)
        return EOVERFLOW;
    size_t count = (size_t)W * nrels;
    if (count > UINT32_MAX || count > SIZE_MAX / element_size)
        return EOVERFLOW;
    *out = count;
    return 0;
}

/* Checked multiplication for relation-count allocations. */
int
wl_columnar_eval_checked_size_mul(size_t count, size_t element_size,
    size_t *out)
{
    if (!out)
        return EINVAL;
    if (element_size != 0 && count > SIZE_MAX / element_size)
        return EOVERFLOW;
    *out = count * element_size;
    return 0;
}

int
wl_columnar_eval_checked_count_inc(uint32_t count, size_t *out)
{
    if (!out)
        return EINVAL;
#if SIZE_MAX <= UINT32_MAX
    if (count == UINT32_MAX)
        return EOVERFLOW;
#endif
    *out = (size_t)count + 1;
    return 0;
}

int
wl_columnar_eval_checked_hash_capacity(uint32_t nrows, uint32_t *out)
{
    if (!out)
        return EINVAL;
    if (nrows > UINT32_MAX / 2u)
        return EOVERFLOW;

    uint32_t required = nrows * 2u;
    uint32_t cap = 4u;
    while (cap < required) {
        if (cap > UINT32_MAX / 2u)
            return EOVERFLOW;
        cap <<= 1;
    }
#if SIZE_MAX <= UINT32_MAX
    if (cap > SIZE_MAX / sizeof(uint32_t))
        return EOVERFLOW;
#endif
    *out = cap;
    return 0;
}

/* Checked addition for TDD row-count fields; UINT32_MAX is a valid total. */
int
wl_columnar_eval_checked_row_add(uint32_t total, uint32_t addend,
    uint32_t *out)
{
    if (!out)
        return EINVAL;
    if (addend > UINT32_MAX - total)
        return EOVERFLOW;
    *out = total + addend;
    return 0;
}

static void
tdd_destroy_delta_slots(col_eval_tdd_worker_ctx_t *ctxs, uint32_t W,
    uint32_t nrels)
{
    if (!ctxs)
        return;
    for (uint32_t w = 0; w < W; w++) {
        if (!ctxs[w].delta_rels)
            continue;
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_destroy(ctxs[w].delta_rels[ri]);
            ctxs[w].delta_rels[ri] = NULL;
        }
    }
}

/* Relation-plan dispatch is implemented in columnar/eval_plan.c. */
/* Serial stratum evaluation is implemented in columnar/eval_serial.c. */

/* ======================================================================== */
/* Distributed Stratum Evaluator (Issue #318)                               */
/* ======================================================================== */

/*
 * tdd_cleanup_workers:
 * Destroy and zero all initialized TDD worker sessions.
 * Safe to call on a coordinator with no workers (tdd_workers_count == 0).
 */
static int
tdd_cleanup_workers(wl_col_session_t *coord)
{
    int result = 0;

    if (coord->tdd_owner_lifetime) {
        int quiesce_rc
            = wl_columnar_eval_tdd_owner_lifetime_quiesce(coord);
        if (quiesce_rc != 0)
            return quiesce_rc;
        int rc = wl_columnar_eval_tdd_owner_lifetime_retry(coord);
        if (rc != 0)
            return rc;
    }

#ifdef WL_COLUMNAR_EVAL_TEST_SUBMISSION
    wl_columnar_eval_test_before_worker_cleanup(coord);
#endif
    for (uint32_t w = coord->tdd_workers_count; w > 0; w--) {
        uint32_t worker_index = w - 1;
        int rc = col_worker_session_destroy(
            &coord->tdd_workers[worker_index]);
        if (rc != 0) {
            if (result == 0)
                result = rc;
            continue;
        }
        memset(&coord->tdd_workers[worker_index], 0,
            sizeof(wl_col_session_t));
    }
    if (result != 0)
        return result;
    coord->tdd_workers_count = 0;
    coord->tdd_active_workers = 0;
    coord->callback_active_workers = 1;
    coord->callback_parallel_execution = false;
    return 0;
}

static int
tdd_cleanup_preserve_error(wl_col_session_t *coord, int operation_rc)
{
    int cleanup_rc = tdd_cleanup_workers(coord);
    return cleanup_rc != 0 ? cleanup_rc : operation_rc;
}

static void
tdd_record_active_workers(wl_col_session_t *coord, uint32_t W)
{
    coord->callback_active_workers = W > 0 ? W : 1;
    coord->callback_parallel_execution = coord->callback_active_workers > 1;
    coord->tdd_last_active_workers = W;
    if (W > coord->tdd_max_active_workers)
        coord->tdd_max_active_workers = W;
}

static void
record_worker_expr_status(wl_col_session_t *coord,
    const wl_col_session_t *worker, int rc)
{
    if (!coord || coord->extension_expr_status != 0)
        return;
    if (worker && worker->extension_expr_status != 0)
        coord->extension_expr_status = worker->extension_expr_status;
    else if (rc >= WL_COLUMNAR_EXPR_EXTENSION_MALFORMED
        && rc != WL_COLUMNAR_EXPR_ALLOCATION_FAILURE
        && rc <= WL_COLUMNAR_EXPR_CALLBACK_REENTRANT)
        coord->extension_expr_status = rc;
}

static void
tdd_dedup_rel(col_rel_t *r);

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_test_before_final_normalize)(wl_col_session_t *,
    col_rel_t *);
#endif

static int
wl_columnar_eval_finalize_relation(wl_col_session_t *coord, col_rel_t *target)
{
    if (!target)
        return 0;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_test_before_final_normalize)
        wl_columnar_eval_test_before_final_normalize(coord, target);
#endif
    if (target->nrows <= 1)
        return 0;
    int rc = wl_columnar_eval_delta_consolidate(target, coord);
    if (rc == 0)
        col_session_invalidate_arrangements(&coord->base, target->name);
    return rc;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_test_nonrec_worker)(wl_col_session_t *, eval_stack_t *,
    eval_entry_t *, int *);
int (*wl_columnar_eval_test_nonrec_boundary)(wl_col_session_t *, unsigned,
    uint32_t, col_rel_t *);
#define WL_COLUMNAR_EVAL_NONREC_BOUNDARY(coord, phase, worker, candidate) \
        (wl_columnar_eval_test_nonrec_boundary \
    ? wl_columnar_eval_test_nonrec_boundary(coord, phase, worker, \
        candidate) : 0)
#else
#define WL_COLUMNAR_EVAL_NONREC_BOUNDARY(coord, phase, worker, candidate) (0)
#endif

typedef struct {
    const wl_plan_relation_t *rp;
    wl_col_session_t *worker_sess;
    eval_entry_t *stage;
    int rc;
} nonrec_rule_worker_ctx_t;

static void
nonrec_rule_worker_fn(void *arg)
{
    nonrec_rule_worker_ctx_t *ctx = arg;
    wl_col_session_t *worker = ctx->worker_sess;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    if (worker->cleanup_active || worker->cleanup_pending) {
        ctx->rc = EBUSY;
        return;
    }
    ctx->rc = wl_columnar_session_cleanup_ready_quiescent(worker);
    if (ctx->rc != 0)
        return;
    ctx->rc = wl_columnar_eval_stack_cleanup_begin(worker, &frame);
    if (ctx->rc != 0)
        return;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    ctx->rc = col_eval_relation_plan(ctx->rp, stack, worker);
    if (ctx->rc == 0 && stack->top > 0)
        ctx->rc = eval_stack_pop_relation(stack, result);
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_test_nonrec_worker)
        wl_columnar_eval_test_nonrec_worker(worker, stack, result, &ctx->rc);
#endif
    if (ctx->rc == 0 && result->rel) {
        col_rel_t *copy = wl_columnar_relation_new_like_governed(ctx->rp->name,
                result->rel, worker->memory_governor);
        if (!copy) {
            ctx->rc = ENOMEM;
        } else {
            /* The coordinator admitted and initialized this disjoint slot
             * before dispatch. It observes it only after the worker barrier. */
            ctx->stage->rel = copy;
            ctx->stage->owned = true;
            ctx->rc = col_rel_append_all(copy, result->rel, NULL);
        }
    }
    int cleanup_rc = wl_columnar_eval_stack_cleanup_finish(&frame);
    if (cleanup_rc != 0)
        ctx->rc = cleanup_rc;
}

static bool
nonrec_plan_has_consolidate(const wl_plan_relation_t *rp)
{
    for (uint32_t oi = 0; oi < rp->op_count; oi++)
        if (rp->ops[oi].op == WL_PLAN_OP_CONSOLIDATE)
            return true;
    return false;
}

static bool
nonrec_plan_parallel_safe(const wl_plan_relation_t *rp,
    const char **out_driver)
{
    const char *driver = NULL;
    uint32_t variables = 0;

    for (uint32_t oi = 0; oi < rp->op_count; oi++) {
        const wl_plan_op_t *op = &rp->ops[oi];
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
            if (!op->relation_name || op->delta_mode != WL_DELTA_AUTO)
                return false;
            if (variables++ == 0)
                driver = op->relation_name;
            break;
        case WL_PLAN_OP_FILTER:
        case WL_PLAN_OP_MAP:
        case WL_PLAN_OP_ANTIJOIN:
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_CONSOLIDATE:
            break;
        case WL_PLAN_OP_JOIN:
            if (op->key_count == 0)
                return false;
            break;
        default:
            return false;
        }
    }
    if (variables != 1 || !driver || strcmp(driver, rp->name) == 0)
        return false;
    if (out_driver)
        *out_driver = driver;
    return true;
}

static uint32_t
nonrec_parallel_min_rows_per_worker(void)
{
    const char *env = getenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER");
    uint32_t min_rows = 32768u;
    if (env && env[0] != '\0') {
        char *endp = NULL;
        errno = 0;
        unsigned long v = strtoul(env, &endp, 10);
        if (endp != env && *endp == '\0' && errno != ERANGE && v > 0
            && v <= UINT32_MAX)
            min_rows = (uint32_t)v;
    }
    return min_rows;
}

static int
nonrec_copy_relation_slice(const col_rel_t *src, const char *name,
    uint32_t begin, uint32_t end, wl_columnar_memory_governor_ref_t *governor,
    col_rel_t **out)
{
    if (!src || !out || begin > end || end > src->nrows)
        return EINVAL;
    wl_columnar_source_access_reader_t reader = { 0 };
    int rc = col_rel_source_reader_acquire(src, &reader);
    if (rc != 0)
        return rc;
    col_rel_t *dst = wl_columnar_relation_new_like_governed(name, src,
            governor);
    int64_t *row = NULL;
    if (!dst) {
        rc = ENOMEM;
        goto done;
    }
    row = malloc(sizeof(int64_t) * (src->ncols ? src->ncols : 1));
    if (!row) {
        rc = ENOMEM;
        goto done;
    }
    for (uint32_t r = begin; r < end && rc == 0; r++) {
        for (uint32_t c = 0; c < src->ncols; c++)
            row[c] = src->columns[c][r];
        rc = col_rel_append_row(dst, row);
        if (rc == 0 && src->timestamps)
            dst->timestamps[dst->nrows - 1] = src->timestamps[r];
    }
done:
    free(row);
    int release_rc = col_rel_source_reader_release(&reader);
    if (rc == 0)
        rc = release_rc;
    if (rc == 0)
        *out = dst;
    else
        col_rel_destroy(dst);
    return rc;
}

static int
tdd_shared_view_deep_copy_fallback(wl_col_session_t *sess, col_rel_t *dst,
    const col_rel_t *src, int shared_view_rc);

static int
nonrec_make_shared_relation_view(const col_rel_t *src, col_rel_t **out)
{
    if (!src || !out)
        return EINVAL;
    col_rel_t *view = col_rel_new_like(src->name, src);
    if (!view)
        return ENOMEM;
    int rc = col_rel_install_shared_view(view, src);
    if (rc != 0) {
        rc = tdd_shared_view_deep_copy_fallback(NULL, view, src, rc);
        if (rc != 0) {
            col_rel_destroy(view);
            return rc;
        }
    }
    *out = view;
    return 0;
}

static int
tdd_shared_view_deep_copy_fallback(wl_col_session_t *sess, col_rel_t *dst,
    const col_rel_t *src, int shared_view_rc)
{
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_t *source_owner = NULL;
    col_rel_t *destination_owner = NULL;
    int rc;

    /* EBUSY can mean terminal source destruction already claimed the gate.
     * Never turn that exclusion result into an unprotected source read. */
    if (shared_view_rc != ENOMEM && shared_view_rc != EINVAL
        && shared_view_rc != EOVERFLOW)
        return shared_view_rc;
    rc = col_rel_storage_owner_resolve(src, &source_owner);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(dst, &destination_owner);
    if (rc != 0)
        return rc;
    rc = col_rel_source_reader_acquire(src, &reader);
    if (rc != 0)
        return rc;
    bool reader_acquired = true;
    /* An empty source has no mutable column data to read.  When the source
     * and reused destination share one gate, release the reader before COW
     * so the destination writer cannot self-conflict.  The live destination
     * alias still keeps that owner alive through the transition. */
    if (src->nrows == 0 && source_owner == destination_owner) {
        rc = col_rel_source_reader_release(&reader);
        reader_acquired = false;
        if (rc != 0)
            return rc;
    }
    if (sess) {
        dst->nrows = 0;
        wl_columnar_relation_touch_view(dst);
    }
    rc = col_rel_append_all(dst, src, NULL);
    int release_rc = reader_acquired
        ? col_rel_source_reader_release(&reader) : 0;
    if (rc == 0 && release_rc != 0)
        rc = release_rc;
    if (rc == 0 && sess) {
        /* append_all also detaches an empty shared destination
         * before it returns, so a successful fallback must retire the
         * old lease. */
        int retire_rc
            = wl_columnar_session_retire_source_lease(sess, dst);
        if (retire_rc != 0)
            rc = retire_rc;
    }
    return rc;
}

int
wl_columnar_eval_nonrec_relation_parallel(const wl_plan_relation_t *rp,
    wl_col_session_t *coord)
{
    if (coord->cleanup_active || coord->cleanup_pending)
        return EBUSY;
    const char *driver_name = NULL;
    if (!nonrec_plan_parallel_safe(rp, &driver_name))
        return EAGAIN;
    if (coord->coordinator != NULL)
        return EAGAIN;
    if (coord->delta_seeded || coord->retraction_seeded
        || coord->retraction_right_pass || coord->diff_operators_active)
        return EAGAIN;
    if (coord->num_workers <= 1 || coord->nrels == 0)
        return EAGAIN;
    int ready_rc = wl_columnar_session_cleanup_ready_quiescent(coord);
    if (ready_rc == 0)
        ready_rc = tdd_cleanup_workers(coord);
    if (ready_rc != 0)
        return ready_rc;

    col_rel_t *driver = session_find_rel(coord, driver_name);
    if (!driver || driver->nrows == 0)
        return EAGAIN;

    uint32_t min_rows = nonrec_parallel_min_rows_per_worker();
    uint32_t W = driver->nrows / min_rows;
    if (W > coord->num_workers)
        W = coord->num_workers;
    if (W > 32)
        W = 32;
    if (W <= 1)
        return EAGAIN;
    if (W > COL_STACK_MAX)
        return ENOBUFS;

    char slice_name[256];
    int sn = snprintf(slice_name, sizeof(slice_name), "$nonrec$%s",
            driver_name);
    if (sn < 0 || (size_t)sn >= sizeof(slice_name))
        return EAGAIN;

    wl_plan_relation_t rp_copy = *rp;
    wl_plan_op_t *ops_copy = (wl_plan_op_t *)malloc(
        sizeof(wl_plan_op_t) * (rp->op_count ? rp->op_count : 1));
    if (!ops_copy)
        return ENOMEM;
    memcpy(ops_copy, rp->ops, sizeof(wl_plan_op_t) * rp->op_count);
    bool replaced_driver = false;
    for (uint32_t oi = 0; oi < rp->op_count; oi++) {
        if (ops_copy[oi].op == WL_PLAN_OP_VARIABLE
            && ops_copy[oi].relation_name
            && strcmp(ops_copy[oi].relation_name, driver_name) == 0) {
            ops_copy[oi].relation_name = slice_name;
            replaced_driver = true;
            break;
        }
    }
    if (!replaced_driver) {
        free(ops_copy);
        return EAGAIN;
    }
    rp_copy.ops = ops_copy;

    int rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (rc != 0) {
        free(ops_copy);
        return rc;
    }
    rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (rc != 0) {
        free(ops_copy);
        return rc;
    }
    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);
    size_t relation_slots = 0;
    if (wl_columnar_eval_checked_count_inc(coord->nrels,
        &relation_slots) != 0) {
        free(ops_copy);
        return tdd_cleanup_preserve_error(coord, EOVERFLOW);
    }
    size_t relation_slot_bytes = 0;
    if (wl_columnar_eval_checked_size_mul(relation_slots,
        sizeof(col_rel_t *), &relation_slot_bytes) != 0) {
        free(ops_copy);
        return tdd_cleanup_preserve_error(coord, EOVERFLOW);
    }

    col_rel_t ***worker_rels = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    nonrec_rule_worker_ctx_t *ctxs = (nonrec_rule_worker_ctx_t *)calloc(
        W, sizeof(nonrec_rule_worker_ctx_t));
    if (!worker_rels || !ctxs) {
        free((void *)worker_rels);
        free(ctxs);
        free(ops_copy);
        return tdd_cleanup_preserve_error(coord, ENOMEM);
    }

    wl_columnar_eval_stack_cleanup_frame_t *stage_frame = NULL;
    rc = WL_COLUMNAR_EVAL_NONREC_BOUNDARY(coord, 0, 0, NULL);
    if (rc == 0)
        rc = wl_columnar_eval_stack_cleanup_begin(coord, &stage_frame);
    if (rc != 0) {
        free((void *)worker_rels);
        free(ctxs);
        free(ops_copy);
        return rc;
    }
    eval_stack_t *stages = wl_columnar_eval_stack_cleanup_stack(stage_frame);
    eval_entry_t *final = wl_columnar_eval_stack_cleanup_result(stage_frame);
    stages->top = W;
    for (uint32_t w = 0; w < W; w++)
        ctxs[w].stage = &stages->items[w];
    uint32_t chunk = (driver->nrows + W - 1u) / W;
    atomic_uint_fast64_t shared_join_count;
    atomic_store_explicit(&shared_join_count, 0, memory_order_relaxed);
    for (uint32_t w = 0; w < W && rc == 0; w++) {
        worker_rels[w] = (col_rel_t **)calloc(relation_slots,
                sizeof(col_rel_t *)); /* NOLINT(clang-analyzer-security.ArrayBound) */
        if (!worker_rels[w]) {
            rc = ENOMEM;
            break;
        }
        uint32_t rels_built = 0;
        for (uint32_t ri = 0; ri < coord->nrels && rc == 0; ri++) {
            col_rel_t *src = coord->rels[ri];
            if (!src)
                continue;
            col_rel_t *rel = NULL;
            rc = nonrec_make_shared_relation_view(src, &rel);
            if (rc == 0)
                worker_rels[w][rels_built++] = rel; /* NOLINT(clang-analyzer-security.ArrayBound) */
        }
        if (rc == 0) {
            uint32_t begin = w * chunk;
            uint32_t end = begin + chunk;
            if (begin > driver->nrows)
                begin = driver->nrows;
            if (end > driver->nrows)
                end = driver->nrows;
            col_rel_t *slice = NULL;
            rc = nonrec_copy_relation_slice(driver, slice_name, begin, end,
                    coord->memory_governor, &slice);
            if (rc == 0)
                worker_rels[w][rels_built++] = slice; /* NOLINT(clang-analyzer-security.ArrayBound) */
        }
        if (rc != 0)
            break;
        coord->tdd_workers_count = w + 1;
        rc = col_worker_session_create(coord, w, worker_rels[w], rels_built,
                &coord->tdd_workers[w]);
        if (rc == 0) {
            coord->tdd_workers[w].join_output_limit =
                coord->join_output_limit;
            if (coord->join_output_limit > 0) {
                coord->tdd_workers[w].join_output_shared_count =
                    &shared_join_count;
                coord->tdd_workers[w].join_output_shared_limit =
                    coord->join_output_limit;
            }
        }
    }
    if (rc == 0)
        coord->tdd_workers_count = W;

    if (rc == 0) {
        for (uint32_t w = 0; w < W; w++) {
            ctxs[w].rp = &rp_copy;
            ctxs[w].worker_sess = &coord->tdd_workers[w];
            rc = WL_COLUMNAR_EVAL_NONREC_BOUNDARY(coord, 1, w, NULL);
            if (rc != 0)
                break;
            if (wl_workqueue_submit(coord->wq, nonrec_rule_worker_fn,
                &ctxs[w]) != 0) {
                rc = ENOMEM;
                break;
            }
        }
        wl_workqueue_wait_all(coord->wq);
        for (uint32_t w = 0; w < W; w++) {
            record_worker_expr_status(coord, ctxs[w].worker_sess,
                ctxs[w].rc);
            if (ctxs[w].rc != 0 && rc == 0)
                rc = ctxs[w].rc;
        }
    }

    bool evaluation_complete = rc == 0;
    /* No retained cohort may refer to this function's stack-local counter. */
    for (uint32_t w = 0; w < coord->tdd_workers_count; w++) {
        coord->tdd_workers[w].join_output_shared_count = NULL;
        coord->tdd_workers[w].join_output_shared_limit = 0;
    }
    if (rc == 0)
        rc = WL_COLUMNAR_EVAL_NONREC_BOUNDARY(coord, 2, 0, NULL);
    int cleanup_rc = tdd_cleanup_workers(coord);
    if (cleanup_rc != 0)
        rc = cleanup_rc;
    bool publication_started = false;
    col_rel_replacement_t replacement = { 0 };
    col_rel_t *target = NULL;
    uint64_t identity = 0, view = 0, storage = 0;
    if (rc == 0) {
        target = session_find_rel(coord, rp->name);
        const col_rel_t *prototype = target && target->ncols ? target : NULL;
        for (uint32_t w = 0; !prototype && w < W; w++)
            if (stages->items[w].rel)
                prototype = stages->items[w].rel;
        rc = WL_COLUMNAR_EVAL_NONREC_BOUNDARY(coord, 3, 0, NULL);
        if (rc == 0 && prototype) {
            final->rel = wl_columnar_relation_new_like_governed(rp->name,
                    prototype, coord->memory_governor);
            final->owned = final->rel != NULL;
            if (!final->rel)
                rc = ENOMEM;
        } else if (rc == 0) {
            rc = col_rel_alloc(&final->rel, rp->name);
            final->owned = final->rel != NULL;
            if (rc == 0)
                rc = col_rel_attach_memory_governor(final->rel,
                        coord->memory_governor);
        }
        for (uint32_t w = 0; rc == 0 && w < W; w++) {
            col_rel_t *part = stages->items[w].rel;
            if (part && part->timestamps && !final->rel->timestamps)
                rc = col_rel_enable_timestamps(final->rel);
        }
        wl_columnar_source_access_reader_t reader = { 0 };
        if (rc == 0 && target) {
            rc = col_rel_source_reader_acquire(target, &reader);
            if (rc == 0) {
                identity = target->relation_identity;
                view = target->view_generation;
                storage = target->storage_generation;
                if (target->nrows > 0)
                    rc = col_rel_append_all(final->rel, target, NULL);
                int release_rc = col_rel_source_reader_release(&reader);
                if (rc == 0)
                    rc = release_rc;
            }
        }
        for (uint32_t w = 0; rc == 0 && w < W; w++)
            if (stages->items[w].rel)
                rc = col_rel_append_all(final->rel, stages->items[w].rel, NULL);


        if (rc == 0)
            rc = WL_COLUMNAR_EVAL_NONREC_BOUNDARY(coord, 4, 0, final->rel);
        if (rc == 0 && nonrec_plan_has_consolidate(rp))
            rc = wl_columnar_eval_delta_consolidate(final->rel, coord);
        if (rc == 0)
            rc = WL_COLUMNAR_EVAL_NONREC_BOUNDARY(coord, 5, 0, final->rel);
        if (rc == 0 && target) {
            rc = col_rel_prepare_replacement(target, final->rel, &replacement);
            if (rc == 0 && (session_find_rel(coord, rp->name) != target
                || target->relation_identity != identity
                || target->view_generation != view
                || target->storage_generation != storage))
                rc = EBUSY;
            if (rc == 0) {
                rc = wl_columnar_eval_stack_cleanup_finish(&stage_frame);
                if (rc == 0) {
                    col_rel_commit_replacement_locked(target, &replacement);
                    publication_started = true;
                }
            }
        } else if (rc == 0) {
            rc = eval_stack_drain(stages);
            if (rc == 0)
                rc = wl_columnar_relation_rename_checked(final->rel, rp->name);
            if (rc == 0) {
                col_rel_t *candidate = final->rel;
                rc = session_add_rel(coord, candidate);
                if (session_find_rel(coord, rp->name) == candidate) {
                    memset(final, 0, sizeof(*final));
                    publication_started = true;
                }
            }
        }
    }
    col_rel_discard_replacement(&replacement);
    if (stage_frame) {
        int stage_rc = wl_columnar_eval_stack_cleanup_finish(&stage_frame);
        if (stage_rc != 0)
            rc = stage_rc;
    }
    if (publication_started)
        col_session_invalidate_arrangements(&coord->base, rp->name);
    free(ctxs);
    free(ops_copy);
    for (uint32_t w = 0; w < W; w++) {
        if (worker_rels[w]) {
            for (size_t ri = 0; ri < relation_slots; ri++)
                col_rel_destroy(worker_rels[w][ri]);
            free((void *)worker_rels[w]);
        }
    }
    free((void *)worker_rels);
    if (!evaluation_complete && !publication_started && rc == EOVERFLOW
        && cleanup_rc == 0
        && !coord->cleanup_pending && coord->tdd_workers_count == 0)
        return EAGAIN;
    return rc;
}

/*
 * tdd_init_workers:
 * Partition all coordinator relations across W worker sessions.
 * Each worker gets 1/W rows of each relation, partitioned by column 0.
 * Empty/zero-column relations are replicated as empty copies.
 *
 * Any previously initialized workers are destroyed first.
 * On failure, any partially-created workers are destroyed.
 */
#ifdef WL_TEST_BDX_SEED
void (*wl_columnar_eval_test_initializer_boundary)(unsigned, unsigned,
    uint32_t, wl_col_session_t *);
#define WL_COLUMNAR_EVAL_INIT_BOUNDARY(mode, phase, worker, coord) \
        do { if (wl_columnar_eval_test_initializer_boundary) \
             wl_columnar_eval_test_initializer_boundary(mode, phase, worker, \
                 coord); \
        } while (0)
#else
#define WL_COLUMNAR_EVAL_INIT_BOUNDARY(mode, phase, worker, coord) ((void)0)
#endif

static int
tdd_init_workers(wl_col_session_t *coord, uint32_t W)
{
    int cleanup_rc = tdd_cleanup_workers(coord);
    if (cleanup_rc != 0)
        return cleanup_rc;

    if (W == 0 || W > coord->num_workers)
        return EINVAL;
    int ensure_rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    ensure_rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);
    uint32_t nrels = coord->nrels;
    size_t relation_ptr_bytes = 0;
    if (wl_columnar_eval_checked_size_mul(nrels, sizeof(col_rel_t *),
        &relation_ptr_bytes) != 0) {
        return tdd_cleanup_preserve_error(coord, EOVERFLOW);
    }

    /* No relations: create empty worker sessions */
    if (nrels == 0) {
        for (uint32_t w = 0; w < W; w++) {
            coord->tdd_workers_count = w + 1;
            int rc = col_worker_session_create(coord, w, NULL, 0,
                    &coord->tdd_workers[w]);
            if (rc != 0) {
                return tdd_cleanup_preserve_error(coord, rc);
            }
            coord->tdd_workers_count = w + 1;
        }
        return 0;
    }

    /* Allocate W x nrels partition matrix */
    col_rel_t ***worker_parts = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!worker_parts) {
        return tdd_cleanup_preserve_error(coord, ENOMEM);
    }

    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        worker_parts[w] = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
        if (!worker_parts[w]) {
            for (uint32_t j = 0; j < w; j++)
                free((void *)worker_parts[j]);
            free((void *)worker_parts);
            return ENOMEM;
        }
    }

    /* Partition each coordinator relation by column 0 */
    uint32_t key_cols[] = { 0 };
    uint32_t parts_built = 0;

    for (uint32_t r = 0; r < nrels && rc == 0; r++) {
        col_rel_t *rel = coord->rels[r];
        if (!rel)
            continue;

        const char *name = rel->name;

        if (rel->ncols == 0 || rel->nrows == 0) {
            /* Empty: give each worker an empty relation */
            for (uint32_t w = 0; w < W && rc == 0; w++) {
                WL_COLUMNAR_EVAL_INIT_BOUNDARY(0, 0, w, coord);
                worker_parts[w][parts_built]
                    = wl_columnar_relation_new_like_governed(name, rel,
                        coord->memory_governor);
                if (!worker_parts[w][parts_built])
                    rc = ENOMEM;
            }
        } else {
            col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
            if (!parts) {
                rc = ENOMEM;
            } else {
                rc = col_rel_exchange_partition(rel, key_cols, 1, W, parts);
                if (rc == 0) {
                    for (uint32_t w = 0; w < W && rc == 0; w++) {
                        free(parts[w]->name);
                        WL_COLUMNAR_EVAL_INIT_BOUNDARY(0, 1, w, coord);
                        parts[w]->name = wl_strdup(name);
                        if (!parts[w]->name) {
                            rc = ENOMEM;
                        } else {
                            worker_parts[w][parts_built] = parts[w];
                            parts[w] = NULL; /* ownership transferred */
                        }
                    }
                }
                /* Free any unowned partition slots on error */
                for (uint32_t w = 0; w < W; w++)
                    col_rel_destroy(parts[w]); /* NULL-safe */
                free((void *)parts);
            }
        }

        if (rc == 0)
            parts_built++;
    }

    /* Create worker sessions */
    if (rc == 0) {
        for (uint32_t w = 0; w < W; w++) {
            coord->tdd_workers_count = w + 1;
            WL_COLUMNAR_EVAL_INIT_BOUNDARY(0, 3, w, coord);
            rc = col_worker_session_create(coord, w,
                    worker_parts[w], parts_built, &coord->tdd_workers[w]);
            if (rc != 0)
                break;
        }
    }

    /* Transfer NULLs are the ownership ledger, including incomplete rows.
     * Construction counts cannot exclude a partially built relation. */
    if (rc != 0) {
        for (uint32_t w = 0; w < W; w++) {
            for (uint32_t p = 0; p < nrels; p++)
                col_rel_destroy(worker_parts[w][p]);
        }
        /* Keep the attempted worker in the cohort: a refused partial
         * teardown must remain reachable for a later retry. */
        int cleanup_rc = tdd_cleanup_workers(coord);
        if (cleanup_rc != 0)
            rc = cleanup_rc;
    } else {
        coord->tdd_workers_count = W;
    }

    for (uint32_t w = 0; w < W; w++)
        free((void *)worker_parts[w]);
    free((void *)worker_parts);

    return rc;
}

/*
 * tdd_replicate_workers:
 * Give every worker a FULL COPY of all coordinator relations.
 *
 * Unlike tdd_init_workers (which partitions by column 0), this function
 * replicates each relation to every worker.  Required for recursive strata
 * where multi-way joins may reference columns other than the partition key
 * (e.g. the same-generation self-join on parent.col1).
 *
 * Issue #352: partitioning by col0 breaks self-joins on non-col0 columns
 * and 3-body recursive rules where IDB appears in the middle of the join.
 */
static int
tdd_replicate_workers(wl_col_session_t *coord, uint32_t W)
{
    int cleanup_rc = tdd_cleanup_workers(coord);
    if (cleanup_rc != 0)
        return cleanup_rc;

    if (W == 0 || W > coord->num_workers)
        return EINVAL;
    int ensure_rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    ensure_rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);
    uint32_t nrels = coord->nrels;
    size_t relation_ptr_bytes = 0;
    if (wl_columnar_eval_checked_size_mul(nrels, sizeof(col_rel_t *),
        &relation_ptr_bytes) != 0) {
        return tdd_cleanup_preserve_error(coord, EOVERFLOW);
    }

    /* No relations: create empty worker sessions */
    if (nrels == 0) {
        for (uint32_t w = 0; w < W; w++) {
            coord->tdd_workers_count = w + 1;
            int rc = col_worker_session_create(coord, w, NULL, 0,
                    &coord->tdd_workers[w]);
            if (rc != 0) {
                return tdd_cleanup_preserve_error(coord, rc);
            }
            coord->tdd_workers_count = w + 1;
        }
        return 0;
    }

    /* Allocate W x nrels relation matrix */
    col_rel_t ***worker_rels = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!worker_rels) {
        return tdd_cleanup_preserve_error(coord, ENOMEM);
    }

    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        worker_rels[w] = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
        if (!worker_rels[w]) {
            for (uint32_t j = 0; j < w; j++)
                free((void *)worker_rels[j]);
            free((void *)worker_rels);
            return ENOMEM;
        }
    }

    /* Replicate each coordinator relation to every worker */
    uint32_t rels_built = 0;

    for (uint32_t r = 0; r < nrels && rc == 0; r++) {
        col_rel_t *rel = coord->rels[r];
        if (!rel)
            continue;

        const char *name = rel->name;

        for (uint32_t w = 0; w < W && rc == 0; w++) {
            WL_COLUMNAR_EVAL_INIT_BOUNDARY(1, 0, w, coord);
            col_rel_t *copy = wl_columnar_relation_new_like_governed(name,
                    rel, coord->memory_governor);
            if (!copy) {
                rc = ENOMEM;
                break;
            }
            if (rel->nrows > 0) {
                WL_COLUMNAR_EVAL_INIT_BOUNDARY(1, 2, w, coord);
                rc = col_rel_append_all(copy, rel, NULL);
                if (rc != 0) {
                    col_rel_destroy(copy);
                    break;
                }
            }
            worker_rels[w][rels_built] = copy;
        }

        if (rc == 0)
            rels_built++;
    }

    /* Create worker sessions */
    if (rc == 0) {
        for (uint32_t w = 0; w < W; w++) {
            coord->tdd_workers_count = w + 1;
            WL_COLUMNAR_EVAL_INIT_BOUNDARY(1, 3, w, coord);
            rc = col_worker_session_create(coord, w,
                    worker_rels[w], rels_built, &coord->tdd_workers[w]);
            if (rc != 0)
                break;
        }
    }

    /* Inspect all original slots: transferred entries are already NULL. */
    if (rc != 0) {
        for (uint32_t w = 0; w < W; w++) {
            for (uint32_t p = 0; p < nrels; p++)
                col_rel_destroy(worker_rels[w][p]);
        }
        int cleanup_rc = tdd_cleanup_workers(coord);
        if (cleanup_rc != 0)
            rc = cleanup_rc;
    } else {
        coord->tdd_workers_count = W;
    }

    for (uint32_t w = 0; w < W; w++)
        free((void *)worker_rels[w]);
    free((void *)worker_rels);

    return rc;
}

static int
tdd_init_workers_global_read(wl_col_session_t *coord, uint32_t W)
{
    if (W == 0 || W > coord->num_workers)
        return EINVAL;
    int rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (rc != 0)
        return rc;
    rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (rc != 0)
        return rc;

    rc = tdd_cleanup_workers(coord);
    if (rc != 0)
        return rc;

    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);

    uint32_t nrels = coord->nrels;
    size_t relation_ptr_bytes = 0;
    if (wl_columnar_eval_checked_size_mul(nrels, sizeof(col_rel_t *),
        &relation_ptr_bytes) != 0) {
        return tdd_cleanup_preserve_error(coord, EOVERFLOW);
    }
    col_rel_t ***worker_rels = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!worker_rels) {
        return tdd_cleanup_preserve_error(coord, ENOMEM);
    }

    for (uint32_t w = 0; w < W; w++) {
        worker_rels[w] = (col_rel_t **)calloc(nrels ? nrels : 1,
                sizeof(col_rel_t *));
        if (!worker_rels[w]) {
            rc = ENOMEM;
            goto cleanup;
        }
        for (uint32_t r = 0; r < nrels; r++) {
            col_rel_t *src = coord->rels[r];
            if (!src)
                continue;
            WL_COLUMNAR_EVAL_INIT_BOUNDARY(2, 0, w, coord);
            rc = nonrec_make_shared_relation_view(src, &worker_rels[w][r]);
            if (rc != 0)
                goto cleanup;
        }
        coord->tdd_workers_count = w + 1;
        WL_COLUMNAR_EVAL_INIT_BOUNDARY(2, 3, w, coord);
        rc = col_worker_session_create(coord, w, worker_rels[w], nrels,
                &coord->tdd_workers[w]);
        if (rc != 0)
            goto cleanup;
    }

cleanup:
    if (worker_rels) {
        /* An attempted worker can still leave every input caller-owned. */
        for (uint32_t w = 0; w < W; w++) {
            if (!worker_rels[w])
                continue;
            for (uint32_t r = 0; r < nrels; r++)
                col_rel_destroy(worker_rels[w][r]);
        }
        for (uint32_t w = 0; w < W; w++)
            free((void *)worker_rels[w]);
        free((void *)worker_rels);
    }
    if (rc != 0)
        rc = tdd_cleanup_preserve_error(coord, rc);
    return rc;
}

#ifdef WL_TEST_BDX_SEED
int
wl_columnar_eval_test_initializer(unsigned mode, wl_col_session_t *coord,
    uint32_t workers)
{
    switch (mode) {
    case 0: return tdd_init_workers(coord, workers);
    case 1: return tdd_replicate_workers(coord, workers);
    case 2: return tdd_init_workers_global_read(coord, workers);
    default: return EINVAL;
    }
}
#endif

#ifdef WL_SESSION_TEST_HOOKS
int (*wl_columnar_eval_test_global_publication)(wl_col_session_t *, uint32_t,
    unsigned);
#define WL_COLUMNAR_EVAL_GLOBAL_BOUNDARY(coord, ri, phase) \
        (wl_columnar_eval_test_global_publication \
    ? wl_columnar_eval_test_global_publication(coord, ri, phase) : 0)
#else
#define WL_COLUMNAR_EVAL_GLOBAL_BOUNDARY(coord, ri, phase) (0)
#endif

static int
tdd_refresh_global_read_relation(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t ri, uint32_t W)
{
    if (!sp || !coord || ri >= sp->relation_count)
        return EINVAL;

    const char *name = sp->relations[ri].name;
    col_rel_t *src = session_find_rel(coord, name);
    if (!src || src->ncols == 0)
        return EINVAL;

    for (uint32_t w = 0; w < W; w++) {
        int boundary_rc = WL_COLUMNAR_EVAL_GLOBAL_BOUNDARY(coord, ri, 5 + w);
        if (boundary_rc != 0)
            return boundary_rc;
        col_rel_t *dst = session_find_rel(&coord->tdd_workers[w], name);
        if (!dst)
            continue;
        if (src->ncols > 0) {
            if (dst->ncols != src->ncols) {
                col_rel_t *view = wl_columnar_relation_new_like_governed(
                    src->name, src, coord->memory_governor);
                if (!view)
                    return ENOMEM;
                int rc = col_rel_install_shared_view(view, src);
                bool shared = rc == 0;
                if (!shared) {
                    rc = tdd_shared_view_deep_copy_fallback(NULL, view,
                            src, rc);
                    if (rc != 0) {
                        col_rel_destroy(view);
                        return rc;
                    }
                }
                rc = session_add_rel(&coord->tdd_workers[w], view);
                if (rc != 0) {
                    col_rel_destroy(view);
                    return rc;
                }
                if (shared) {
                    rc = wl_columnar_session_adopt_shared_view(
                        &coord->tdd_workers[w], view);
                    if (rc != 0) {
                        /* #1661: this discard stays a discard.  The entry was
                         * registered by the session_add_rel just above, and
                         * every failure path in
                         * wl_columnar_session_adopt_shared_view releases its
                         * reader before returning -- so nothing holds one here
                         * and the removal cannot refuse.  The exception is
                         * the path where the release itself fails: the reader
                         * is then still held either way, and only the code
                         * differs -- EINVAL when the preceding lease_prepare
                         * succeeded (the `&& rc == 0` guard), otherwise
                         * prepare's own code.  Either way this
                         * function returns the adopt failure below, so
                         * propagating EBUSY from the rollback could only
                         * replace a specific diagnosis with a vaguer one. */
                        (void)session_remove_rel(&coord->tdd_workers[w],
                            view->name);
                        return rc;
                    }
                }
            } else {
                int rc = wl_columnar_session_install_shared_view(
                    &coord->tdd_workers[w], dst, src);
                if (rc != 0) {
                    rc = tdd_shared_view_deep_copy_fallback(
                        &coord->tdd_workers[w], dst, src, rc);
                }
                if (rc != 0)
                    return rc;
            }
        }
        col_session_invalidate_arrangements(&coord->tdd_workers[w].base,
            name);
    }
    return 0;
}

static int
tdd_seed_global_read_initial_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t W)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const char *dname = sp->relations[ri].delta_name;
        const char *rel_name = sp->relations[ri].name;
        col_rel_t *src = session_find_rel(coord, rel_name);

        /* #1661: the discard is safe because a refusal here cannot go
         * unnoticed, not because refusals cannot happen.  A refusal leaves
         * the worker's registry owner in place; the same-name session_add_rel
         * below then re-attempts col_rel_destroy_checked on that owner,
         * restores its lease and returns EBUSY, so the refusal surfaces
         * there.  TWO paths reach `return 0` without registering: the
         * empty-source `continue` just below, and the per-worker
         * `!part || part->nrows == 0` skip in the partition loop, which
         * bypasses session_add_rel for that worker alone.  A stale owner left
         * by either is caught when the cohort is torn down:
         * col_worker_session_destroy refuses through its relation-alias pass
         * and the caller keeps the cohort for retry
         * (tests/test_tdd_inline_workers.c). */
        for (uint32_t w = W; w-- > 0; )
            (void)session_remove_rel(&coord->tdd_workers[w], dname);

        if (!src || src->nrows == 0)
            continue;

        const uint32_t *key_cols = NULL;
        uint32_t key_count = 0;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta =
                    (const wl_plan_op_exchange_t *)
                    sp->relations[ri].ops[oi].opaque_data;
                if (meta && meta->key_col_count > 0) {
                    key_cols = meta->key_col_idxs;
                    key_count = meta->key_col_count;
                }
                break;
            }
        }
        uint32_t default_key[] = { 0 };
        if (!key_cols || key_count == 0) {
            key_cols = default_key;
            key_count = 1;
        }
        bool key_valid = src->ncols > 0;
        for (uint32_t ki = 0; key_valid && ki < key_count; ki++) {
            if (key_cols[ki] >= src->ncols)
                key_valid = false;
        }
        if (!key_valid) {
            key_cols = default_key;
            key_count = 1;
        }

        col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
        if (!parts)
            return ENOMEM;
        int rc = col_rel_exchange_partition(src, key_cols, key_count, W,
                parts);
        if (rc != 0) {
            for (uint32_t w = 0; w < W; w++)
                col_rel_destroy(parts[w]);
            free((void *)parts);
            return rc;
        }

        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *part = parts[w];
            parts[w] = NULL;
            if (!part || part->nrows == 0) {
                col_rel_destroy(part);
                continue;
            }
            free(part->name);
            part->name = wl_strdup(dname);
            if (!part->name) {
                col_rel_destroy(part);
                rc = ENOMEM;
                break;
            }
            rc = session_add_rel(&coord->tdd_workers[w], part);
            if (rc != 0) {
                col_rel_destroy(part);
                break;
            }
        }
        for (uint32_t w = 0; w < W; w++)
            col_rel_destroy(parts[w]);
        free((void *)parts);
        if (rc != 0)
            return rc;
    }
    return 0;
}

/*
 * tdd_worker_subpass_fn:
 * Execute one sub-pass of the semi-naive iteration on a worker's partition.
 *
 * Mirrors eval_serial.c:377-721 for a single (iter, sub) effective iteration,
 * operating entirely on the worker's local session.  The coordinator
 * controls the outer iteration loop and convergence detection.
 *
 * Three differences from the single-worker path (per IMPLEMENTATION_PLAN
 * Clarification 1):
 *   1. eff_iter comes from ctx->eff_iter (set by coordinator).
 *   2. diff_operators_active set explicitly (Clarification 3).
 *   3. Produced delta_rels stored in ctx->delta_rels[] for broadcast
 *      exchange; deltas are heap-allocated (col_rel_new_like) so they
 *      remain valid across delta_pool_reset.
 */
static int bdx_hash_diff(col_rel_t *delta, const col_rel_t *base);
static int tdd_hashset_diff(col_rel_t *delta, const col_rel_t *base);
static int
tdd_install_empty_delta_on_workers(wl_col_session_t *coord,
    const char *dname, uint32_t ncols, uint32_t W)
{
    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *empty = col_rel_new_auto(dname, ncols);
        if (!empty)
            return ENOMEM;
        int rc = session_add_rel(&coord->tdd_workers[w], empty);
        if (rc != 0) {
            if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG")) {
                fprintf(stderr,
                    "TDD install empty delta error rel=%s worker=%u cols=%u rc=%d\n",
                    dname, w, ncols, rc);
            }
            col_rel_destroy(empty);
            return rc;
        }
    }
    return 0;
}

typedef struct {
    wl_col_session_t *worker;
    col_rel_t *prior;
    col_rel_t *empty;
    wl_columnar_source_access_reader_t reader;
    bool alias;
} wl_columnar_eval_delta_retirement_t;

/* The dispatch barrier and successful worker/cleanup checks precede this
 * transaction. Empty registrations preserve later-iteration AUTO semantics.
 * Preparation is atomic; publication is atomic per registration. On refusal,
 * already installed empty owners remain valid and the refused owner/lease is
 * unchanged. The caller aborts exchange and cleans up fresh delta payloads. */
static int
wl_columnar_eval_retire_worker_relations(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord,
    uint32_t workers, uint32_t first, uint32_t relation_count, bool full)
{
    if (!sp || !coord || workers > coord->tdd_workers_count
        || first > sp->relation_count
        || relation_count > sp->relation_count - first)
        return EINVAL;
    for (uint32_t w = 0; w < workers; w++) {
        const wl_col_session_t *worker = &coord->tdd_workers[w];
        if (worker->cleanup_active || worker->cleanup_pending ||
            worker->delta_rollback)
            return EBUSY;
    }
    size_t count = 0;
    int rc = wl_columnar_eval_tdd_matrix_size(workers, relation_count,
            sizeof(wl_columnar_eval_delta_retirement_t), &count);
    if (rc != 0 || count == 0)
        return rc;
    size_t bytes = count * sizeof(wl_columnar_eval_delta_retirement_t);
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_reservation_init(&reservation);
    bool admitted = coord->memory_governor != NULL;
    if (admitted) {
        wl_columnar_memory_admission_status_t status =
            wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(coord->memory_governor),
            bytes, &reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
    }
    wl_columnar_eval_delta_retirement_t *entries = calloc(count,
            sizeof(*entries));
    if (!entries) {
        if (admitted)
            (void)wl_columnar_memory_rollback(&reservation);
        return ENOMEM;
    }
    if (admitted && !wl_columnar_memory_commit(&reservation, entries)) {
        (void)wl_columnar_memory_rollback(&reservation);
        free(entries);
        return EINVAL;
    }
    size_t used = 0;
    for (uint32_t w = 0; w < workers; w++) {
        wl_col_session_t *worker = &coord->tdd_workers[w];
        for (uint32_t ri = first; ri < first + relation_count; ri++) {
            const char *name = full ? sp->relations[ri].name
                : sp->relations[ri].delta_name;
            col_rel_t *prior = name ? session_find_rel(worker, name) : NULL;
            if (!prior)
                continue;
            bool duplicate = false;
            for (size_t i = 0; i < used; i++)
                if (entries[i].worker == worker && entries[i].prior == prior)
                    duplicate = true;
            if (duplicate)
                continue;
            wl_columnar_eval_delta_retirement_t *entry = &entries[used++];
            entry->worker = worker;
            entry->prior = prior;
            col_rel_t *owner = NULL;
            rc = col_rel_source_reader_acquire(prior, &entry->reader);
            if (rc == 0)
                rc = col_rel_storage_owner_resolve(prior, &owner);
            if (rc != 0)
                goto cleanup;
            entry->alias = owner != prior;
            entry->empty = wl_columnar_relation_new_like_governed(name, prior,
                    coord->memory_governor);
            if (!entry->empty) {
                rc = ENOMEM;
                goto cleanup;
            }
            entry->empty->declared_ncols = prior->declared_ncols;
            if (!prior->schema_ok && entry->empty->schema_ok) {
                ArrowSchemaRelease(&entry->empty->schema);
                memset(&entry->empty->schema, 0, sizeof(entry->empty->schema));
                entry->empty->schema_ok = false;
            }
        }
    }
    for (size_t i = 0; i < used; i++) {
        rc = col_rel_source_reader_release(&entries[i].reader);
        if (rc != 0)
            goto cleanup;
    }
    /* Every alias across the cohort precedes every root, independently of
     * worker numbering. Checked replacement also retires its session lease. */
    for (unsigned phase = 0; phase < 2; phase++) {
        for (size_t i = 0; i < used; i++) {
            wl_columnar_eval_delta_retirement_t *entry = &entries[i];
            if (entry->alias != (phase == 0))
                continue;
            rc = session_add_rel(entry->worker, entry->empty);
            if (rc != 0)
                goto cleanup;
            entry->empty = NULL;
        }
    }
cleanup:
    for (size_t i = 0; i < used; i++) {
        if (entries[i].reader.owner)
            (void)col_rel_source_reader_release(&entries[i].reader);
        col_rel_destroy(entries[i].empty);
    }
    free(entries);
    if (admitted)
        (void)wl_columnar_memory_release(&reservation);
    return rc;
}

static int
wl_columnar_eval_retire_prior_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t workers)
{
    return wl_columnar_eval_retire_worker_relations(sp, coord, workers, 0,
               sp ? sp->relation_count : 0, false);
}

#if defined(WL_TEST_BDX_SEED) || defined(WL_SESSION_TEST_HOOKS)
void (*wl_columnar_eval_test_subpass_boundary)(wl_col_session_t *, uint32_t,
    bool);
#endif

#ifdef WL_TEST_BDX_SEED
int
wl_columnar_eval_test_retire_prior_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t workers)
{
    return wl_columnar_eval_retire_prior_deltas(sp, coord, workers);
}
#endif

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_test_outbound_after_plan)(wl_col_session_t *,
    eval_stack_t *, eval_entry_t *);
void (*wl_columnar_eval_test_outbound_before_publish)(wl_col_session_t *,
    eval_entry_t *);
#endif

static int
wl_columnar_eval_outbound_framed_relation(col_eval_tdd_worker_ctx_t *ctx,
    uint32_t ri, bool *produced)
{
    wl_col_session_t *sess = ctx->worker_sess;
    const wl_plan_relation_t *rp = &ctx->sp->relations[ri];
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    int rc = wl_columnar_eval_stack_cleanup_begin(sess, &frame);
    if (rc != 0)
        return rc;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    rc = col_eval_relation_plan(rp, stack, sess);
    if (rc == 0 && stack->top != 0)
        *result = eval_stack_pop(stack);
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_test_outbound_after_plan)
        wl_columnar_eval_test_outbound_after_plan(sess, stack, result);
#endif
    if (rc != 0)
        goto done;
    if (result->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION) {
        rc = ENOTSUP;
        goto done;
    }
    rc = eval_stack_drain(stack);
    if (rc != 0 || !result->rel || result->rel->nrows == 0)
        goto done;

    col_rel_t *delta = wl_columnar_relation_new_like_governed(
        rp->delta_name, result->rel, sess->memory_governor);
    if (!delta) {
        rc = ENOMEM;
        goto done;
    }
    /* Lower entries have drained, so this push has a vacant slot. */
    rc = eval_stack_push(stack, delta, true);
    if (rc != 0) {
        col_rel_destroy(delta);
        goto done;
    }
    rc = col_rel_append_all(delta, result->rel, NULL);
    if (rc == 0)
        rc = eval_entry_dispose(result);
    if (rc != 0)
        goto done;
    *result = eval_stack_pop(stack);

    /* This independent candidate has not escaped. Keep preparation private:
     * the existing diff/dedup helpers may acquire their own source writers. */
    col_rel_t *target = session_find_rel(sess, rp->name);
    if (target && target->nrows > 0 && !ctx->force_diff) {
        rc = bdx_hash_diff(delta, target);
        if (rc != 0)
            goto done;
    }
    if (sess->coordinator && delta->nrows > 0) {
        col_rel_t *coord_target = session_find_rel(sess->coordinator, rp->name);
        if (coord_target && coord_target->nrows > 0) {
            rc = tdd_hashset_diff(delta, coord_target);
            if (rc != 0)
                goto done;
        }
    }
    if (delta->nrows > 1)
        tdd_dedup_rel(delta);
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_test_outbound_before_publish)
        wl_columnar_eval_test_outbound_before_publish(sess, result);
#endif
    bool nonempty = delta->nrows > 0;
    rc = wl_columnar_eval_tdd_queue_publish_delta(ctx, sess, &result->rel,
            ri, ctx->eff_iter);
    if (!result->rel)
        result->owned = false;
    if (rc == 0 && nonempty)
        *produced = true;
done:;
    int cleanup_rc = wl_columnar_eval_stack_cleanup_finish(&frame);
    return cleanup_rc != 0 ? cleanup_rc : rc;
}

static void
tdd_worker_subpass_fn(void *arg)
{
    col_eval_tdd_worker_ctx_t *ctx = (col_eval_tdd_worker_ctx_t *)arg;
    wl_col_session_t *sess = ctx->worker_sess;
    const wl_plan_stratum_t *sp = ctx->sp;
    uint32_t eff_iter = ctx->eff_iter;
    uint32_t nrels = sp->relation_count;
    uint64_t worker_t0 = now_ns();

#ifdef WL_COLUMNAR_EVAL_TEST_OWNER_LIFETIME
    wl_columnar_eval_test_tdd_worker_start(sess);
#endif

    int readiness_rc = sess->cleanup_active ? EBUSY
        : wl_columnar_session_cleanup_ready(sess);
    if (readiness_rc != 0) {
        ctx->rc = readiness_rc;
        ctx->runtime_ns = now_ns() - worker_t0;
        return;
    }

    /* Issue #282: Enable differential operators from eff_iter 1 onward.
     * Mirrors eval_serial.c:395-396 / Clarification 3.
     * Issue #390: BDX mode forces diff from eff_iter 0 so that K_FUSION
     * evaluates Δr ⋈ r_w (broadcast delta × local partition) instead of
     * r_w ⋈ r_w (incomplete local self-join). */
    bool saved_diff = sess->diff_operators_active;
    sess->diff_operators_active = sess->diff_enabled
        && (eff_iter > 0 || ctx->force_diff);
    /* Issue #318: Signal to col_op_variable AUTO heuristic that we are inside
     * a TDD worker sub-pass.  Broadcast $d$<rel> may be >= local partition
     * size, so the normal "delta < full" guard must be bypassed. */
    bool saved_tdd_subpass = sess->tdd_subpass_active;
    bool saved_outbound_only = sess->tdd_outbound_only_active;
    bool saved_delta_seeded = sess->delta_seeded;
    sess->tdd_subpass_active = true;
    sess->tdd_outbound_only_active = ctx->outbound_only;
    if (ctx->force_diff && ctx->outbound_only && eff_iter > 0)
        sess->delta_seeded = true;
    sess->current_iteration = eff_iter;
#define TDD_WORKER_RETURN() \
        do { \
            sess->tdd_subpass_active = saved_tdd_subpass; \
            sess->tdd_outbound_only_active = saved_outbound_only; \
            sess->delta_seeded = saved_delta_seeded; \
            sess->diff_operators_active = saved_diff; \
            ctx->runtime_ns = now_ns() - worker_t0; \
            return; \
        } while (0)

    /* Free per-sub-pass delta arrangements (eval_serial.c:414) */
    col_session_free_delta_arrangements(sess);

    /* Early-exit: all relation plans have empty FORCE_DELTA
     * (eval_serial.c:471-483).  Worker reports all_empty_delta;
     * coordinator skips to next outer iter. */
    if (eff_iter > 0) {
        bool all_empty = true;
        for (uint32_t ri = 0; ri < nrels; ri++) {
            if (!has_empty_forced_delta(&sp->relations[ri], sess, eff_iter)) {
                all_empty = false;
                break;
            }
        }
        if (all_empty) {
            ctx->all_empty_delta = true;
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->tdd_outbound_only_active = saved_outbound_only;
            sess->diff_operators_active = saved_diff;
            TDD_WORKER_RETURN();
        }
    }

    /* Snapshot nrows before evaluation (eval_serial.c:431-434) */
    size_t snap_bytes = 0;
    if (wl_columnar_eval_checked_size_mul(nrels, sizeof(uint32_t),
        &snap_bytes) != 0) {
        ctx->rc = EOVERFLOW;
        TDD_WORKER_RETURN();
    }
    uint32_t *snap = (uint32_t *)calloc(nrels, sizeof(uint32_t));
    if (!snap) {
        ctx->rc = ENOMEM;
        sess->tdd_subpass_active = saved_tdd_subpass;
        sess->tdd_outbound_only_active = saved_outbound_only;
        sess->diff_operators_active = saved_diff;
        TDD_WORKER_RETURN();
    }
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        snap[ri] = r ? r->nrows : 0;
    }

    bool any_new = false;

    /* Evaluate all relation plans (eval_serial.c:490-589) */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        const wl_plan_relation_t *rp = &sp->relations[ri];

        if (has_empty_forced_delta(rp, sess, eff_iter))
            continue;

        if (!ctx->outbound_only) {
            /* Fresh queue/matrix deltas are produced only after every
             * ordinary rule frame has finished. Prior-round dependencies
             * remain registered until checked retirement after the barrier. */
            for (uint32_t di = 0; di < nrels; di++)
                assert(ctx->delta_rels[di] == NULL);
            int rc = wl_columnar_eval_serial_framed_relation(rp, sess, true);
            if (rc != 0) {
                ctx->rc = rc;
                free(snap);
                TDD_WORKER_RETURN();
            }
            continue;
        }

        int rc = wl_columnar_eval_outbound_framed_relation(ctx, ri, &any_new);
        if (rc != 0) {
            ctx->rc = rc;
            free(snap);
            TDD_WORKER_RETURN();
        }
    }

    /* Consolidate + produce deltas (eval_serial.c:606-698).
     * Use col_rel_new_like (heap) so deltas survive delta_pool_reset. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || snap[ri] >= r->nrows)
            continue;

        const char *dname = sp->relations[ri].delta_name;

        /* Heap-allocate delta so it survives delta_pool_reset below. */
        col_rel_t *delta = col_rel_new_like(dname, r);
        if (!delta) {
            ctx->rc = ENOMEM;
            free(snap);
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->tdd_outbound_only_active = saved_outbound_only;
            sess->diff_operators_active = saved_diff;
            TDD_WORKER_RETURN();
        }

        int rc2 = 0;
        if (r->dedup_slots) {
            /* Hash-set dedup: O(D) per subpass instead of O(N) merge.
             * For each new row, check the hash set. Keep only truly
             * new rows in the relation and emit them to delta. */
            int64_t row_buf[8];
            int64_t *rbuf = r->ncols <= 8 ? row_buf
                : (int64_t *)malloc((size_t)r->ncols * sizeof(int64_t));
            if (!rbuf) {
                col_rel_destroy(delta);
                ctx->rc = ENOMEM;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
            }
            uint32_t keep = snap[ri];
            for (uint32_t i = snap[ri]; i < r->nrows; i++) {
                uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(r, i);
                if (WL_COLUMNAR_EVAL_DEDUP_SET_INSERT(r, h)) {
                    /* New row: compact into [keep] and emit to delta. */
                    if (keep != i)
                        col_rel_row_move_raw(r, keep, i);
                    for (uint32_t c = 0; c < r->ncols; c++)
                        rbuf[c] = r->columns[c][keep];
                    rc2 = col_rel_append_row(delta, rbuf);
                    if (rc2 != 0)
                        break;
                    keep++;
                }
            }
            r->nrows = keep;
            r->sorted_nrows = keep; /* not truly sorted but OK for hash joins */
            wl_columnar_relation_touch_view(r);
            if (rbuf != row_buf)
                free(rbuf);
        } else {
            int fast_flag = 0;
            rc2 = col_op_consolidate_incremental_delta(r, snap[ri], delta,
                    &fast_flag);
        }
        /* rc2 != 0 propagates as a worker error so any_new is not set.
         * Sources: col_op_consolidate_incremental_delta (EOVERFLOW/ENOMEM)
         * or col_rel_append_row (ENOMEM) from the hash-set dedup path.
         * Both are hard errors requiring coordinator intervention. */
        if (rc2 != 0) {
            /* Sorting/deduplication may have changed the source before a
             * later admission error.  Keep arrangements coherent on error. */
            col_session_invalidate_arrangements(&sess->base,
                sp->relations[ri].name);
            col_rel_destroy(delta);
            ctx->rc = rc2;
            free(snap);
            sess->tdd_subpass_active = saved_tdd_subpass;
            sess->tdd_outbound_only_active = saved_outbound_only;
            sess->diff_operators_active = saved_diff;
            TDD_WORKER_RETURN();
        }

        /* Consolidation changed the relation only after rc2 succeeded. */
        col_session_invalidate_arrangements(&sess->base,
            sp->relations[ri].name);

        if (delta->nrows > 0) {
            /* Stamp timestamps (eval_serial.c:653-681) */
            delta->timestamps = (col_delta_timestamp_t *)calloc(
                delta->nrows, sizeof(col_delta_timestamp_t));
            if (!delta->timestamps) {
                col_rel_destroy(delta);
                ctx->rc = ENOMEM;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
            }
            delta->timestamp_capacity = delta->nrows;
            wl_columnar_relation_touch_storage(delta);
            for (uint32_t ti = 0; ti < delta->nrows; ti++) {
                delta->timestamps[ti].iteration = eff_iter;
                delta->timestamps[ti].stratum = ctx->stratum_idx;
                delta->timestamps[ti].worker = (uint16_t)sess->worker_id;
                delta->timestamps[ti].multiplicity = 1;
            }

            /* Enable target timestamps */
            int timestamp_rc = col_rel_enable_timestamps(r);
            if (timestamp_rc != 0) {
                col_rel_destroy(delta);
                ctx->rc = timestamp_rc;
                free(snap);
                sess->tdd_subpass_active = saved_tdd_subpass;
                sess->tdd_outbound_only_active = saved_outbound_only;
                sess->diff_operators_active = saved_diff;
                TDD_WORKER_RETURN();
            }

            /* Issue #410, Commit 5: Queue-only transport.
             * delta ownership transfers to queue; coordinator reconstructs
             * ctxs via wl_columnar_eval_tdd_queue_reconstruct_delta_matrix
             * after barrier.
             * Fallback to ctx write when queue unavailable (alloc failure). */
            if (sess->coordinator && sess->coordinator->delta_queue) {
                /* Issue #1380: mirror of eval_tdd_queue.c publish. */
                uint64_t transport_bytes = col_rel_transport_bytes(delta);
                int enq_rc = wl_mpsc_enqueue(
                    sess->coordinator->delta_queue,
                    sess->worker_id, delta, ctx->stratum_idx, ri);
                if (enq_rc == 0)
                    wl_mem_ledger_alloc(&sess->coordinator->mem_ledger,
                        WL_MEM_SUBSYS_CHANNEL, transport_bytes);
                if (enq_rc != 0) {
                    /* Queue full — signal error; destroy orphaned delta. */
                    col_rel_destroy(delta);
                    ctx->rc = ENOMEM;
                    free(snap);
                    sess->tdd_subpass_active = saved_tdd_subpass;
                    sess->tdd_outbound_only_active = saved_outbound_only;
                    sess->diff_operators_active = saved_diff;
                    TDD_WORKER_RETURN();
                }
            } else {
                /* No queue (creation failed): fall back to direct ctx write. */
                ctx->delta_rels[ri] = delta;
            }
            any_new = true;
        } else {
            col_rel_destroy(delta);
        }
    }

    free(snap);

    /* Issue #1380: sample STORED/TEMPORARY at the high-water point, just
     * before the per-sub-pass temporaries are released. */
    col_session_mem_sample(sess);

    /* Reset per-sub-pass allocators and cache (eval_serial.c:701-712) */
    col_mat_cache_release_pins(&sess->mat_cache);
    delta_pool_reset(sess->delta_pool);
    sess->rotation_ops->rotate_eval_arena(sess);
    if (sess->cache_evict_threshold == 0) {
        assert(sess->mat_cache.active_pins == 0);
        col_mat_cache_clear(&sess->mat_cache);
        assert(sess->mat_cache.active_pins == 0);
    } else {
        col_mat_cache_evict_until(&sess->mat_cache,
            sess->cache_evict_threshold);
    }

    ctx->any_new = any_new;
    sess->tdd_subpass_active = saved_tdd_subpass;
    sess->tdd_outbound_only_active = saved_outbound_only;
    sess->diff_operators_active = saved_diff;
    ctx->runtime_ns = now_ns() - worker_t0;
#undef TDD_WORKER_RETURN
}

/*
 * tdd_worker_nonrecursive_fn:
 * Work function for non-recursive distributed stratum evaluation.
 * Each worker evaluates the stratum on its local data partition.
 */
static void
tdd_worker_nonrecursive_fn(void *arg)
{
    col_eval_tdd_worker_ctx_t *ctx = (col_eval_tdd_worker_ctx_t *)arg;

    ctx->rc = col_eval_stratum(ctx->sp, ctx->worker_sess, ctx->stratum_idx);
}

/*
 * tdd_merge_worker_results is defined below the checked staging helpers.
 * Keeping the declaration here leaves the evaluator's phase layout intact.
 */
static int
tdd_merge_worker_results(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord);

/*
 * tdd_dedup_rel:
 * Sort a relation in-place and remove consecutive duplicate rows.
 * Called after merging worker results for recursive strata to eliminate
 * duplicates introduced by broadcast exchange (multiple partitions may
 * independently derive the same tuple via different equal-length paths).
 *
 * Storage is column-major: r->columns[col][row].
 */
static bool
tdd_relation_rows_sorted(const col_rel_t *r)
{
    if (!r || r->nrows <= 1 || r->ncols == 0)
        return true;
    for (uint32_t i = 1; i < r->nrows; i++) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->columns[c][i - 1] < r->columns[c][i])
                break;
            if (r->columns[c][i - 1] > r->columns[c][i])
                return false;
        }
    }
    return true;
}

static void
tdd_dedup_rel(col_rel_t *r)
{
    if (!r || r->nrows <= 1 || r->ncols == 0)
        return;

    uint32_t ncols = r->ncols;
    uint32_t nrows = r->nrows;

    /* Presort check: O(n) scan avoids sort for already-ordered input
     * (e.g. single-worker runs or pre-sorted merge results). */
    bool sorted = true;
    for (uint32_t i = 1; i < nrows && sorted; i++) {
        for (uint32_t c = 0; c < ncols; c++) {
            int64_t a = r->columns[c][i - 1];
            int64_t b = r->columns[c][i];
            if (a < b)
                break;         /* this pair is in order */
            if (a > b) {
                sorted = false;
                break;
            }
        }
    }

    if (!sorted) {
        /* Hash-based dedup: O(n), avoids O(n log n) sort.
         * Open-addressing table with FNV-1a row hashing, load <= 0.5.
         * Two-pass: first pass marks unique rows (read-only on columns),
         * second pass compacts in-place. */
        uint32_t cap = 0;
        if (wl_columnar_eval_checked_hash_capacity(nrows, &cap) != 0) {
            /* The hash table cannot represent this row count safely.
             * Sorting has no capacity-doubling arithmetic and preserves the
             * same deduplication result. */
            /* The status is deliberately discarded: the post-condition
             * check below is stronger, catching both a refused sort and a
             * relation that was never sorted to begin with. */
            WL_IGNORE_RESULT(col_rel_radix_sort_int64(r));
            if (!tdd_relation_rows_sorted(r))
                return;
        } else {
            uint32_t mask = cap - 1;
            uint32_t *ht = (uint32_t *)malloc(cap * sizeof(uint32_t));
            uint8_t  *keep = (uint8_t *)malloc(nrows);

            if (!ht || !keep) {
                /* Allocation failure: fall back to sort-based path */
                free(ht);
                free(keep);
                /* Discarded for the same reason as above: the
                 * post-condition check is the stronger guard. */
                WL_IGNORE_RESULT(col_rel_radix_sort_int64(r));
                if (!tdd_relation_rows_sorted(r))
                    return;
                /* Fall through to the sorted dedup below. */
            } else {
                memset(ht, 0xFF, cap * sizeof(uint32_t)); /* 0xFF = UINT32_MAX */
                memset(keep, 0, nrows);

                /* First pass: build hash table, mark unique rows */
                for (uint32_t i = 0; i < nrows; i++) {
                    uint64_t h = 14695981039346656037ULL; /* FNV-1a offset basis */
                    for (uint32_t c = 0; c < ncols; c++) {
                        h ^= (uint64_t)r->columns[c][i];
                        h *= 1099511628211ULL; /* FNV prime */
                    }
                    uint32_t slot = (uint32_t)(h & mask);
                    for (;;) {
                        uint32_t ex = ht[slot];
                        if (ex == UINT32_MAX) {
                            ht[slot] = i;
                            keep[i] = 1;
                            break;
                        }
                        bool eq = true;
                        for (uint32_t c = 0; c < ncols; c++) {
                            if (r->columns[c][ex] != r->columns[c][i]) {
                                eq = false;
                                break;
                            }
                        }
                        if (eq)
                            break; /* duplicate */
                        slot = (slot + 1) & mask;
                    }
                }
                free(ht);

                /* Second pass: compact columns in-place */
                uint32_t out = 0;
                for (uint32_t i = 0; i < nrows; i++) {
                    if (!keep[i])
                        continue;
                    if (out != i) {
                        col_columns_copy_row(r->columns, out,
                            (int64_t *const *)r->columns, i, ncols);
                        if (r->timestamps)
                            r->timestamps[out] = r->timestamps[i];
                    }
                    out++;
                }
                free(keep);
                r->nrows = out;
                wl_columnar_relation_touch_view(r);
                return;
            }
        }
    }

    /* Sorted path: linear scan dedup (input sorted, no sort needed) */
    uint32_t out = 1;
    for (uint32_t i = 1; i < r->nrows; i++) {
        bool dup = true;
        for (uint32_t c = 0; c < ncols; c++) {
            if (r->columns[c][i - 1] != r->columns[c][i]) {
                dup = false;
                break;
            }
        }
        if (!dup) {
            if (out != i)
                col_columns_copy_row(r->columns, out,
                    (int64_t *const *)r->columns, i, ncols);
            if (r->timestamps)
                r->timestamps[out] = r->timestamps[i];
            out++;
        }
    }
    r->nrows = out;
    wl_columnar_relation_touch_view(r);
}

static int
wl_columnar_eval_owner_publication_entry_compare(const void *left,
    const void *right)
{
    const wl_columnar_eval_owner_publication_entry_t *a = left;
    const wl_columnar_eval_owner_publication_entry_t *b = right;
    uint64_t ai = a->owner ? a->owner->relation_identity : UINT64_MAX;
    uint64_t bi = b->owner ? b->owner->relation_identity : UINT64_MAX;

    if (ai < bi)
        return -1;
    if (ai > bi)
        return 1;
    if (a->owner != b->owner)
        return (uintptr_t)a->owner < (uintptr_t)b->owner ? -1 : 1;
    if (a->session != b->session)
        return (uintptr_t)a->session < (uintptr_t)b->session ? -1 : 1;
    {
        int name_cmp = strcmp(a->name, b->name);
        if (name_cmp != 0)
            return name_cmp;
    }
    if (a->target != b->target)
        return (uintptr_t)a->target < (uintptr_t)b->target ? -1 : 1;
    if (a->candidate != b->candidate)
        return (uintptr_t)a->candidate < (uintptr_t)b->candidate ? -1 : 1;
    return 0;
}

static col_rel_t *
wl_columnar_eval_owner_publication_find_rel_linear(wl_col_session_t *session,
    const char *name)
{
    if (!session || !name || (session->nrels > 0 && !session->rels))
        return NULL;
    for (uint32_t i = 0; i < session->nrels; i++)
        if (session->rels[i] && session->rels[i]->name
            && strcmp(session->rels[i]->name, name) == 0)
            return session->rels[i];
    return NULL;
}

#ifdef WL_TEST_OWNER_PUBLICATION
static wl_col_session_t *wl_columnar_eval_owner_publication_fail_session;
static const char *wl_columnar_eval_owner_publication_fail_name;
static bool wl_columnar_eval_owner_publication_registration_fail_hit;
static bool wl_columnar_eval_owner_publication_registration_fail_after_image;
static wl_col_session_t *wl_columnar_eval_owner_publication_prepare_fail_session;
static const char *wl_columnar_eval_owner_publication_prepare_fail_name;
static bool wl_columnar_eval_owner_publication_prepare_fail_hit;
static bool wl_columnar_eval_owner_publication_prepare_fail_after_prior;

void
wl_columnar_eval_test_owner_publication_fail_registration(
    wl_col_session_t *session, const char *name)
{
    wl_columnar_eval_owner_publication_fail_session = session;
    wl_columnar_eval_owner_publication_fail_name = name;
    wl_columnar_eval_owner_publication_registration_fail_hit = false;
    wl_columnar_eval_owner_publication_registration_fail_after_image = false;
    wl_columnar_session_hash_test_fail_registry_image_prepare(NULL);
}

bool
wl_columnar_eval_test_owner_publication_registration_failure_hit(void)
{
    return wl_columnar_eval_owner_publication_registration_fail_hit;
}

bool
wl_columnar_eval_test_owner_publication_registration_failure_followed_image(
    void)
{
    return wl_columnar_eval_owner_publication_registration_fail_after_image;
}

void
wl_columnar_eval_test_owner_publication_fail_prepare(
    wl_col_session_t *session, const char *name)
{
    wl_columnar_eval_owner_publication_prepare_fail_session = session;
    wl_columnar_eval_owner_publication_prepare_fail_name = name;
    wl_columnar_eval_owner_publication_prepare_fail_hit = false;
    wl_columnar_eval_owner_publication_prepare_fail_after_prior = false;
}

bool
wl_columnar_eval_test_owner_publication_prepare_failure_hit(void)
{
    return wl_columnar_eval_owner_publication_prepare_fail_hit;
}

bool
wl_columnar_eval_test_owner_publication_prepare_failure_followed_prepared(
    void)
{
    return wl_columnar_eval_owner_publication_prepare_fail_after_prior;
}
#endif

void
wl_columnar_eval_owner_publication_init(
    wl_columnar_eval_owner_publication_txn_t *txn)
{
    if (txn)
        memset(txn, 0, sizeof(*txn));
}

static int
wl_columnar_eval_owner_publication_discard_entries(
    wl_columnar_eval_owner_publication_entry_t *entries, uint32_t count)
{
    if (!entries)
        return 0;
    for (uint32_t i = count; i-- > 0; ) {
        wl_columnar_eval_owner_publication_entry_t *entry = &entries[i];
        if (entry->replacement_prepared
            || entry->replacement.writer_acquired)
            col_rel_discard_replacement(&entry->replacement);
        if (entry->candidate)
            col_rel_destroy(entry->candidate);
    }
    return 0;
}

static void
wl_columnar_eval_owner_publication_discard_registry_images(
    wl_columnar_eval_owner_publication_txn_t *txn)
{
    for (uint32_t i = 0; i < txn->registry_image_count; i++)
        wl_columnar_session_hash_registry_image_discard(
            &txn->registry_images[i]);
    free(txn->registry_images);
    txn->registry_images = NULL;
    txn->registry_image_count = 0;
    txn->registry_images_prepared = false;
}

int
wl_columnar_eval_owner_publication_add(
    wl_columnar_eval_owner_publication_txn_t *txn, wl_col_session_t *session,
    const char *name, col_rel_t *target, col_rel_t *candidate)
{
    wl_columnar_eval_owner_publication_entry_t *entries;
    col_rel_t *candidate_owner = NULL;
    uint32_t new_capacity;
    size_t allocation_size;

    if (!txn || !session || !name || !candidate || txn->prepared)
        return EINVAL;
    /* Registration does not own the pool/arena lifecycle.  Reject these
     * candidates so a failed transaction leaves ownership with the caller. */
    if (candidate->pool_owned || candidate->arena_owned)
        return EINVAL;
    if (!candidate->name || strcmp(candidate->name, name) != 0)
        return EINVAL;
    if (col_rel_storage_owner_resolve(candidate, &candidate_owner) != 0
        || candidate_owner != candidate
        || candidate->storage_alias_borrows > 0)
        return EINVAL;
    for (uint32_t i = 0; i < txn->count; i++) {
        if (txn->entries[i].candidate == candidate)
            return EEXIST;
    }
    if (target && (!target->name || strcmp(target->name, name) != 0
        || wl_columnar_eval_owner_publication_find_rel_linear(session, name)
        != target))
        return EINVAL;
    if (txn->count == txn->capacity) {
        new_capacity = txn->capacity ? txn->capacity * 2u : 4u;
        if (new_capacity < txn->capacity)
            return EOVERFLOW;
        allocation_size = (size_t)new_capacity * sizeof(*entries);
        if (new_capacity != 0
            && allocation_size / new_capacity != sizeof(*entries))
            return EOVERFLOW;
        entries = (wl_columnar_eval_owner_publication_entry_t *)realloc(
            txn->entries, allocation_size);
        if (!entries)
            return ENOMEM;
        txn->entries = entries;
        txn->capacity = new_capacity;
    }
    entries = txn->entries;
    entries[txn->count] = (wl_columnar_eval_owner_publication_entry_t){
        .session = session,
        .name = name,
        .target = target,
        .candidate = candidate,
    };
    txn->count++;
    return 0;
}

int
wl_columnar_eval_owner_publication_prepare(
    wl_columnar_eval_owner_publication_txn_t *txn)
{
    int rc;

    if (!txn || txn->prepared || txn->count == 0)
        return EINVAL;
    for (uint32_t i = 0; i < txn->count; i++) {
        wl_columnar_eval_owner_publication_entry_t *entry = &txn->entries[i];
        if (entry->target) {
            if (!entry->target->name
                || strcmp(entry->target->name, entry->name) != 0
                || wl_columnar_eval_owner_publication_find_rel_linear(
                    entry->session, entry->name)
                != entry->target) {
                rc = EINVAL;
                goto fail;
            }
            rc = col_rel_storage_owner_resolve(entry->target, &entry->owner);
            if (rc != 0 || entry->owner != entry->target
                || entry->owner->storage_alias_borrows > 0) {
                rc = rc != 0 ? rc : EBUSY;
                goto fail;
            }
        } else if (wl_columnar_eval_owner_publication_find_rel_linear(
                entry->session, entry->name)) {
            rc = EEXIST;
            goto fail;
        }
    }
    qsort(txn->entries, txn->count, sizeof(*txn->entries),
        wl_columnar_eval_owner_publication_entry_compare);
    for (uint32_t i = 1; i < txn->count; i++) {
        wl_columnar_eval_owner_publication_entry_t *previous
            = &txn->entries[i - 1];
        wl_columnar_eval_owner_publication_entry_t *entry = &txn->entries[i];
        if (entry->owner && previous->owner == entry->owner) {
            rc = EBUSY;
            goto fail;
        }
        if (!entry->owner && !previous->owner
            && entry->session == previous->session
            && strcmp(entry->name, previous->name) == 0) {
            rc = EEXIST;
            goto fail;
        }
    }
    for (uint32_t i = 0; i < txn->count; i++) {
        wl_columnar_eval_owner_publication_entry_t *entry = &txn->entries[i];
        if (!entry->target)
            continue;
        wl_columnar_memory_reservation_init(&entry->replacement.reservation);
        rc = wl_columnar_source_access_writer_acquire(
            &entry->owner->source_access, &entry->replacement.writer);
        if (rc != 0)
            goto fail;
        /* Declare ownership of the token before prepare: that is what makes
         * a prepare failure release this writer rather than strand the
         * destination's gate.  Same handshake as the TDD restore
         * transaction below. */
        entry->replacement.writer_acquired = true;
#ifdef WL_TEST_OWNER_PUBLICATION
        if (wl_columnar_eval_owner_publication_prepare_fail_session
            == entry->session
            && wl_columnar_eval_owner_publication_prepare_fail_name
            && strcmp(wl_columnar_eval_owner_publication_prepare_fail_name,
            entry->name) == 0) {
            wl_columnar_eval_owner_publication_prepare_fail_hit = true;
            for (uint32_t prior = 0; prior < i; prior++)
                if (txn->entries[prior].replacement_prepared)
                    wl_columnar_eval_owner_publication_prepare_fail_after_prior
                        = true;
            wl_columnar_eval_owner_publication_prepare_fail_session = NULL;
            wl_columnar_eval_owner_publication_prepare_fail_name = NULL;
            rc = ENOMEM;
            goto fail;
        }
#endif
        rc = col_rel_prepare_replacement_locked(entry->target,
                entry->candidate, &entry->replacement);
        if (rc != 0)
            goto fail;
        entry->replacement_prepared = true;
        col_rel_destroy(entry->candidate);
        entry->candidate = NULL;
    }
    txn->prepared = true;
    return 0;

fail:
    wl_columnar_eval_owner_publication_discard(txn);
    return rc;
}

int
wl_columnar_eval_owner_publication_register(
    wl_columnar_eval_owner_publication_txn_t *txn)
{
    if (!txn || !txn->prepared)
        return EINVAL;
    if (txn->registry_images_prepared)
        return 0;
    uint32_t additions = 0;
    for (uint32_t i = 0; i < txn->count; i++) {
        if (!txn->entries[i].target)
            additions++;
    }
    if (additions == 0) {
        txn->registry_images_prepared = true;
        return 0;
    }
    txn->registry_images = calloc(additions, sizeof(*txn->registry_images));
    if (!txn->registry_images)
        return ENOMEM;
    for (uint32_t i = 0; i < txn->count; i++) {
        wl_columnar_eval_owner_publication_entry_t *entry = &txn->entries[i];
        uint32_t nadd = 0;
        col_rel_t **candidates;
        bool prior_image = false;
        int rc;

        if (entry->target)
            continue;
        for (uint32_t p = 0; p < txn->registry_image_count; p++)
            if (txn->registry_images[p].session == entry->session) {
                prior_image = true;
                break;
            }
        if (prior_image)
            continue;
        for (uint32_t j = i; j < txn->count; j++)
            if (!txn->entries[j].target
                && txn->entries[j].session == entry->session)
                nadd++;
        candidates = malloc((size_t)nadd * sizeof(*candidates));
        if (!candidates) {
            wl_columnar_eval_owner_publication_discard_registry_images(txn);
            return ENOMEM;
        }
        uint32_t at = 0;
        for (uint32_t j = i; j < txn->count; j++)
            if (!txn->entries[j].target
                && txn->entries[j].session == entry->session)
                candidates[at++] = txn->entries[j].candidate;
#ifdef WL_TEST_OWNER_PUBLICATION
        if (wl_columnar_eval_owner_publication_fail_session == entry->session
            && wl_columnar_eval_owner_publication_fail_name) {
            for (uint32_t j = 0; j < nadd; j++) {
                if (candidates[j]->name
                    && strcmp(candidates[j]->name,
                    wl_columnar_eval_owner_publication_fail_name) == 0) {
                    wl_columnar_session_hash_test_fail_registry_image_prepare(
                        entry->session);
                    wl_columnar_eval_owner_publication_fail_session = NULL;
                    wl_columnar_eval_owner_publication_fail_name = NULL;
                    break;
                }
            }
        }
#endif
        rc = wl_columnar_session_hash_registry_image_prepare(entry->session,
                candidates, nadd,
                &txn->registry_images[txn->registry_image_count]);
        free(candidates);
        if (rc != 0) {
#ifdef WL_TEST_OWNER_PUBLICATION
            if (wl_columnar_session_hash_test_registry_image_prepare_failure_hit())
            {
                wl_columnar_eval_owner_publication_registration_fail_hit = true;
                wl_columnar_eval_owner_publication_registration_fail_after_image
                    = txn->registry_image_count > 0;
                wl_columnar_eval_owner_publication_fail_session = NULL;
                wl_columnar_eval_owner_publication_fail_name = NULL;
            }
#endif
            wl_columnar_eval_owner_publication_discard_registry_images(txn);
            return rc;
        }
        txn->registry_image_count++;
    }
    txn->registry_images_prepared = true;
    return 0;
}

static int
wl_columnar_eval_owner_publication_validate_replacement(
    const wl_columnar_eval_owner_publication_entry_t *entry)
{
    const col_rel_t *staged;
    col_rel_t *owner = NULL;

    if (!entry || !entry->target || !entry->replacement_prepared
        || !entry->replacement.writer_acquired)
        return EINVAL;
    staged = entry->replacement.staged;
    if (!staged || staged->nrows > staged->capacity
        || entry->target->view_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || entry->target->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || !entry->target->name
        || strcmp(entry->target->name, entry->name) != 0
        || wl_columnar_eval_owner_publication_find_rel_linear(
            entry->session, entry->name) != entry->target
        || col_rel_storage_owner_resolve(entry->target, &owner) != 0
        || owner != entry->target
        || entry->replacement.writer.owner != &owner->source_access
        || entry->replacement.writer.identity
        != (uintptr_t)&entry->replacement.writer
        || !wl_columnar_source_access_writer_thread_equal(
            &entry->replacement.writer))
        return EINVAL;
    if (entry->replacement.reservation_active) {
        uint64_t state = atomic_load_explicit(
            &entry->replacement.reservation.state, memory_order_acquire);
        if (entry->replacement.reservation.identity
            != &entry->replacement.reservation
            || (state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            && state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED))
            return EINVAL;
    }
    return 0;
}

int
wl_columnar_eval_owner_publication_commit(
    wl_columnar_eval_owner_publication_txn_t *txn)
{
    if (!txn || !txn->prepared)
        return EINVAL;
    if (!txn->registry_images_prepared)
        return EINVAL;
    for (uint32_t i = 0; i < txn->registry_image_count; i++)
        if (wl_columnar_session_hash_registry_image_validate(
                &txn->registry_images[i]) != 0)
            return EBUSY;
    for (uint32_t i = 0; i < txn->count; i++) {
        wl_columnar_eval_owner_publication_entry_t *entry = &txn->entries[i];
        if (!entry->target) {
            if (!entry->candidate
                || !entry->candidate->name
                || strcmp(entry->candidate->name, entry->name) != 0
                || wl_columnar_eval_owner_publication_find_rel_linear(
                    entry->session, entry->name))
                return EBUSY;
            continue;
        }
        if (wl_columnar_eval_owner_publication_validate_replacement(entry)
            != 0)
            return EINVAL;
    }
    for (uint32_t i = 0; i < txn->registry_image_count; i++) {
        wl_columnar_session_hash_registry_image_publish(
            &txn->registry_images[i]);
    }
    free(txn->registry_images);
    txn->registry_images = NULL;
    txn->registry_image_count = 0;
    txn->registry_images_prepared = false;
    for (uint32_t i = 0; i < txn->count; i++) {
        wl_columnar_eval_owner_publication_entry_t *entry = &txn->entries[i];
        if (entry->replacement_prepared) {
            /* Publication is no-fail: the preflight loop above has already
             * established every invariant commit asserts, so there is no
             * error channel to report through and nothing left to roll
             * back once the first destination has been replaced. */
            col_rel_commit_replacement_locked(entry->target,
                &entry->replacement);
            entry->replacement_prepared = false;
        }
        if (!entry->target)
            entry->candidate = NULL;
    }
    txn->prepared = false;
    return 0;
}

int
wl_columnar_eval_owner_publication_discard(
    wl_columnar_eval_owner_publication_txn_t *txn)
{
    int rc;

    if (!txn)
        return EINVAL;
    wl_columnar_eval_owner_publication_discard_registry_images(txn);
    rc = wl_columnar_eval_owner_publication_discard_entries(txn->entries,
            txn->count);
    free(txn->entries);
    memset(txn, 0, sizeof(*txn));
    return rc;
}

static int
tdd_bdx_sort_candidate(col_rel_t *candidate)
{
    int64_t **original_columns = NULL;
    col_delta_timestamp_t *original_timestamps = NULL;
    bool *used = NULL;
    int rc;

    if (!candidate || candidate->nrows <= 1)
        return 0;
    if (!candidate->timestamps)
        return col_rel_radix_sort(candidate, 0, candidate->nrows);

    original_columns = col_columns_alloc(candidate->ncols,
            candidate->nrows);
    original_timestamps = (col_delta_timestamp_t *)malloc(
        (size_t)candidate->nrows * sizeof(*original_timestamps));
    used = (bool *)calloc(candidate->nrows, sizeof(*used));
    if (!original_columns || !original_timestamps || !used) {
        col_columns_free(original_columns, candidate->ncols);
        free(original_timestamps);
        free(used);
        return ENOMEM;
    }
    for (uint32_t col = 0; col < candidate->ncols; col++)
        memcpy(original_columns[col], candidate->columns[col],
            (size_t)candidate->nrows * sizeof(**original_columns));
    memcpy(original_timestamps, candidate->timestamps,
        (size_t)candidate->nrows * sizeof(*original_timestamps));

    rc = col_rel_radix_sort(candidate, 0, candidate->nrows);
    if (rc != 0)
        goto cleanup;
    for (uint32_t row = 0; row < candidate->nrows; row++) {
        uint32_t source = 0;
        for (; source < candidate->nrows; source++) {
            bool match = !used[source];
            for (uint32_t col = 0; match && col < candidate->ncols; col++)
                match = candidate->columns[col][row]
                    == original_columns[col][source];
            if (match)
                break;
        }
        if (source == candidate->nrows) {
            rc = EINVAL;
            goto cleanup;
        }
        used[source] = true;
        candidate->timestamps[row] = original_timestamps[source];
    }

cleanup:
    col_columns_free(original_columns, candidate->ncols);
    free(original_timestamps);
    free(used);
    return rc;
}

#ifdef WL_TEST_BDX_SEED
static int bdx_seed_test_fail_worker = -1;
static bool bdx_seed_test_fail_sort;
#endif

static int
tdd_seed_bdx_coordinator_idb(col_rel_t *cidb, col_rel_t *const *worker_idbs,
    uint32_t worker_count)
{
    col_rel_t *schema_source = cidb;
    col_rel_t *candidate = NULL;
    col_rel_replacement_t replacement;
    int rc = 0;

    if (!cidb || (!worker_idbs && worker_count > 0))
        return EINVAL;
    for (uint32_t w = 0; w < worker_count; w++) {
        col_rel_t *worker = worker_idbs[w];
        if (!worker || worker->nrows == 0)
            continue;
        if (schema_source->ncols == 0)
            schema_source = worker;
        else if (worker->ncols != schema_source->ncols)
            return EINVAL;
    }

    candidate = col_rel_new_like(cidb->name, schema_source);
    if (!candidate)
        return ENOMEM;
    candidate->nrows = 0;
    candidate->base_nrows = 0;
    candidate->sorted_nrows = 0;
    candidate->run_count = 0;
    if (cidb->timestamps) {
        rc = col_rel_enable_timestamps(candidate);
        if (rc != 0)
            goto cleanup;
    }

    for (uint32_t w = 0; w < worker_count; w++) {
        col_rel_t *worker = worker_idbs[w];
        if (!worker || worker->nrows == 0)
            continue;
#ifdef WL_TEST_BDX_SEED
        if (bdx_seed_test_fail_worker == (int)w) {
            bdx_seed_test_fail_worker = -1;
            rc = ENOMEM;
            goto cleanup;
        }
#endif
        rc = col_rel_append_all(candidate, worker, NULL);
        if (rc != 0)
            goto cleanup;
    }

    if (candidate->nrows > 1 && candidate->ncols > 0) {
        if (!tdd_relation_rows_sorted(candidate)) {
#ifdef WL_TEST_BDX_SEED
            if (bdx_seed_test_fail_sort) {
                bdx_seed_test_fail_sort = false;
                rc = ENOMEM;
                goto cleanup;
            }
#endif
            rc = tdd_bdx_sort_candidate(candidate);
            if (rc != 0)
                goto cleanup;
        }
        uint32_t out = 1;
        for (uint32_t row = 1; row < candidate->nrows; row++) {
            bool duplicate = true;
            for (uint32_t col = 0; col < candidate->ncols; col++) {
                if (candidate->columns[col][row - 1]
                    != candidate->columns[col][row]) {
                    duplicate = false;
                    break;
                }
            }
            if (!duplicate) {
                if (out != row)
                    col_columns_copy_row(candidate->columns, out,
                        (int64_t *const *)candidate->columns, row,
                        candidate->ncols);
                if (candidate->timestamps)
                    candidate->timestamps[out] = candidate->timestamps[row];
                out++;
            }
        }
        candidate->nrows = out;
        candidate->sorted_nrows = out;
        wl_columnar_relation_touch_view(candidate);
    }

    memset(&replacement, 0, sizeof(replacement));
    rc = col_rel_prepare_replacement(cidb, candidate, &replacement);
    if (rc == 0) {
        col_rel_commit_replacement_locked(cidb, &replacement);
        rc = 0;
    }
    if (rc != 0)
        col_rel_discard_replacement(&replacement);

cleanup:
    col_rel_destroy(candidate);
    return rc;
}

#ifdef WL_TEST_TDD_MERGE
static int tdd_merge_test_fail_worker = -1;
static bool tdd_merge_test_fail_sort;
static bool tdd_merge_test_fail_overflow;
#endif

/* Return the number of logical entries in an inline compound arity map.
 * The map is valid only when its widths cover exactly the physical schema. */
static bool
tdd_compound_map_entries(const col_rel_t *rel, uint32_t *entries_out)
{
    uint32_t entries = 0;
    uint32_t physical = 0;

    if (!rel || !entries_out)
        return false;
    if (rel->compound_kind != WIRELOG_COMPOUND_KIND_INLINE) {
        *entries_out = 0;
        return rel->compound_arity_map == NULL
               && rel->compound_count == 0;
    }
    if (!rel->compound_arity_map || rel->ncols == 0
        || rel->compound_count == 0)
        return false;
    while (physical < rel->ncols) {
        uint32_t arity;

        if (entries >= rel->ncols)
            return false;
        arity = rel->compound_arity_map[entries];
        if (arity == 0 || arity > rel->ncols - physical)
            return false;
        physical += arity;
        entries++;
    }
    *entries_out = entries;
    return true;
}

/* Worker results must have compatible physical and logical schema metadata
* before any result row is copied into the private candidate.  A missing
* type vector is the legacy spelling of an all-INT64 schema; append promotes
* that destination metadata transactionally when the source is explicit. */
static bool
tdd_relation_schema_compatible(const col_rel_t *expected,
    const col_rel_t *actual)
{
    uint32_t expected_entries;
    uint32_t actual_entries;

    if (!expected || !actual || expected->ncols != actual->ncols
        || expected->declared_ncols != actual->declared_ncols
        || expected->schema_ok != actual->schema_ok
        || expected->has_graph_column != actual->has_graph_column
        || expected->graph_col_idx != actual->graph_col_idx
        || expected->compound_kind != actual->compound_kind
        || expected->compound_count != actual->compound_count
        || expected->inline_physical_offset
        != actual->inline_physical_offset)
        return false;

    for (uint32_t col = 0; col < expected->ncols; col++) {
        wirelog_column_type_t expected_type = expected->column_types
            ? expected->column_types[col] : WIRELOG_TYPE_INT64;
        wirelog_column_type_t actual_type = actual->column_types
            ? actual->column_types[col] : WIRELOG_TYPE_INT64;
        if (expected_type != actual_type)
            return false;
    }

    if ((expected->col_names == NULL) != (actual->col_names == NULL))
        return false;
    if (expected->col_names) {
        for (uint32_t col = 0; col < expected->ncols; col++) {
            if ((expected->col_names[col] == NULL)
                != (actual->col_names[col] == NULL))
                return false;
            if (expected->col_names[col]
                && strcmp(expected->col_names[col], actual->col_names[col])
                != 0)
                return false;
        }
    }

    if (!tdd_compound_map_entries(expected, &expected_entries)
        || !tdd_compound_map_entries(actual, &actual_entries)
        || expected_entries != actual_entries)
        return false;
    for (uint32_t entry = 0; entry < expected_entries; entry++) {
        if (expected->compound_arity_map[entry]
            != actual->compound_arity_map[entry])
            return false;
    }
    return true;
}

/*
 * Merge one coordinator relation transactionally.  The target is never
 * changed while worker results are being checked, appended, sorted, or
 * deduplicated.  When *target_io is NULL, the completed candidate is
 * returned through that slot so the caller can register it after staging.
 * This is intentionally relation-scoped; callers still sequence relations
 * independently.
 */
static int
tdd_merge_relation_results(col_rel_t **target_io, const char *rel_name,
    col_rel_t *const *worker_rels, uint32_t worker_count)
{
    col_rel_t *target;
    col_rel_t *candidate = NULL;
    col_rel_t *schema_source = NULL;
    col_rel_replacement_t replacement;
    int rc = 0;

    if (!target_io || !rel_name || (!worker_rels && worker_count > 0))
        return EINVAL;
    target = *target_io;

    for (uint32_t w = 0; w < worker_count; w++) {
        col_rel_t *worker = worker_rels[w];
        if (!worker || worker->nrows == 0)
            continue;
        if (!schema_source)
            schema_source = worker;
        else if (!tdd_relation_schema_compatible(schema_source, worker))
            return EINVAL;
    }

    if (!schema_source) {
        if (target)
            return 0;
        candidate = col_rel_new_auto(rel_name, 0);
        if (!candidate)
            return ENOMEM;
        *target_io = candidate;
        return 0;
    }

    if (target && target->ncols != 0) {
        rc = col_rel_deep_copy(target, &candidate, NULL);
        if (rc != 0)
            return rc;
    } else {
        candidate = col_rel_new_like(rel_name, schema_source);
        if (!candidate)
            return ENOMEM;
        if (target && target->nrows != 0) {
            col_rel_destroy(candidate);
            return EINVAL;
        }
    }

    for (uint32_t w = 0; w < worker_count; w++) {
        col_rel_t *worker = worker_rels[w];
        uint32_t total_rows;

        if (!worker || worker->nrows == 0)
            continue;
        if (!tdd_relation_schema_compatible(candidate, worker)) {
            rc = EINVAL;
            goto cleanup;
        }
#ifdef WL_TEST_TDD_MERGE
        if (tdd_merge_test_fail_worker == (int)w) {
            tdd_merge_test_fail_worker = -1;
            rc = ENOMEM;
            goto cleanup;
        }
        if (tdd_merge_test_fail_overflow) {
            tdd_merge_test_fail_overflow = false;
            rc = EOVERFLOW;
            goto cleanup;
        }
#endif
        if (wl_columnar_eval_checked_row_add(candidate->nrows,
            worker->nrows, &total_rows) != 0) {
            rc = EOVERFLOW;
            goto cleanup;
        }
        rc = col_rel_append_all(candidate, worker, NULL);
        if (rc != 0)
            goto cleanup;
    }

    if (candidate->nrows > 1 && candidate->ncols > 0) {
        if (!tdd_relation_rows_sorted(candidate)) {
#ifdef WL_TEST_TDD_MERGE
            if (tdd_merge_test_fail_sort) {
                tdd_merge_test_fail_sort = false;
                rc = ENOMEM;
                goto cleanup;
            }
#endif
            rc = tdd_bdx_sort_candidate(candidate);
            if (rc != 0)
                goto cleanup;
        }

        uint32_t out = 1;
        for (uint32_t row = 1; row < candidate->nrows; row++) {
            bool duplicate = true;
            for (uint32_t col = 0; col < candidate->ncols; col++) {
                if (candidate->columns[col][row - 1]
                    != candidate->columns[col][row]) {
                    duplicate = false;
                    break;
                }
            }
            if (!duplicate) {
                if (out != row)
                    col_columns_copy_row(candidate->columns, out,
                        (int64_t *const *)candidate->columns, row,
                        candidate->ncols);
                if (candidate->timestamps)
                    candidate->timestamps[out] = candidate->timestamps[row];
                out++;
            }
        }
        candidate->nrows = out;
        candidate->sorted_nrows = out;
        wl_columnar_relation_touch_view(candidate);
    }

    if (!target) {
        *target_io = candidate;
        candidate = NULL;
        rc = 0;
        goto cleanup;
    }

    memset(&replacement, 0, sizeof(replacement));
    rc = col_rel_prepare_replacement(target, candidate, &replacement);
    if (rc == 0) {
        col_rel_commit_replacement_locked(target, &replacement);
        rc = 0;
    }
    if (rc != 0)
        col_rel_discard_replacement(&replacement);

cleanup:
    col_rel_destroy(candidate);
    return rc;
}

static int
tdd_merge_worker_results(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    uint32_t worker_count;

    if (!sp || !coord)
        return EINVAL;
    worker_count = coord->tdd_workers_count;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const char *rel_name = sp->relations[ri].name;
        col_rel_t *target = session_find_rel(coord, rel_name);
        col_rel_t **worker_rels = (col_rel_t **)calloc(worker_count,
                sizeof(*worker_rels));
        int rc;

        if (!worker_rels)
            return ENOMEM;
        for (uint32_t w = 0; w < worker_count; w++)
            worker_rels[w] = session_find_rel(
                &coord->tdd_workers[w], rel_name);
        rc = tdd_merge_relation_results(&target, rel_name,
                worker_rels, worker_count);
        /* col_rel_t ** -> void * is a multilevel conversion;
         * cast explicitly for #1100's
         * bugprone-multi-level-implicit-pointer-conversion. */
        free((void *)worker_rels);
        if (rc != 0)
            return rc;
        if (!session_find_rel(coord, rel_name)) {
            rc = session_add_rel(coord, target);
            if (rc != 0) {
                col_rel_destroy(target);
                return rc;
            }
        }
    }
    return 0;
}

#ifdef WL_TEST_TDD_MERGE
int
wl_columnar_eval_test_tdd_merge(col_rel_t **target,
    col_rel_t *const *worker_rels, uint32_t worker_count)
{
    return tdd_merge_relation_results(target, "test", worker_rels,
               worker_count);
}

void
wl_columnar_eval_test_tdd_merge_fail_worker(uint32_t worker_index)
{
    tdd_merge_test_fail_worker = (int)worker_index;
}

void
wl_columnar_eval_test_tdd_merge_fail_sort_once(void)
{
    tdd_merge_test_fail_sort = true;
}

void
wl_columnar_eval_test_tdd_merge_fail_overflow_once(void)
{
    tdd_merge_test_fail_overflow = true;
}
#endif

#ifdef WL_TEST_BDX_SEED
void
wl_columnar_eval_test_bdx_seed_fail_worker(uint32_t worker_index)
{
    bdx_seed_test_fail_worker = (int)worker_index;
}

void
wl_columnar_eval_test_bdx_seed_fail_sort_once(void)
{
    bdx_seed_test_fail_sort = true;
}

int
wl_columnar_eval_test_bdx_seed(col_rel_t *cidb,
    col_rel_t *const *worker_idbs, uint32_t worker_count)
{
    return tdd_seed_bdx_coordinator_idb(cidb, worker_idbs, worker_count);
}
#endif

/*
 * tdd_sorted_merge_append:
 * Merge src (sorted, no overlap with dst) into dst (sorted), maintaining
 * lexicographic sorted order.  O(N + D) where N = dst->nrows, D = src->nrows.
 *
 * Kept with external linkage for compatibility with the existing non-header
 * symbol.  The BDX coordinator path no longer calls it because BDX deltas are
 * not guaranteed to arrive sorted.
 */
int
tdd_sorted_merge_append(col_rel_t *dst, col_rel_t *src)
{
    if (!src || src->nrows == 0)
        return 0;
    if (!dst || dst->ncols == 0)
        return 0;

    uint32_t N = dst->nrows;
    uint32_t D = src->nrows;
    uint32_t ncols = dst->ncols;
    uint32_t total = 0;
    if (wl_columnar_eval_checked_row_add(N, D, &total) != 0)
        return EOVERFLOW;

    if (N == 0)
        return col_rel_append_all(dst, src, NULL);

    /* Copy current dst rows into the persistent merge buffer */
    if (dst->merge_buf_cap < N) {
        int64_t **mc = col_columns_alloc(ncols, N);
        if (!mc)
            return ENOMEM;
        col_columns_free(dst->merge_columns, ncols);
        dst->merge_columns = mc;
        dst->merge_buf_cap = N;
    }
    for (uint32_t c = 0; c < ncols; c++)
        memcpy(dst->merge_columns[c], dst->columns[c], N * sizeof(int64_t));

    /* Grow dst columns to hold the merged result */
    if (dst->capacity < total) {
        if (col_columns_realloc(dst->columns, ncols, total) != 0)
            return ENOMEM;
        dst->capacity = total;
        wl_columnar_relation_touch_storage(dst);
    }

    /* Two-pointer merge: both sequences are sorted, no overlap */
    uint32_t i = 0, j = 0, wr = 0;
    while (i < N && j < D) {
        int cmp = 0;
        for (uint32_t c = 0; c < ncols; c++) {
            int64_t a = dst->merge_columns[c][i];
            int64_t b = src->columns[c][j];
            if (a < b) {
                cmp = -1;
                break;
            }
            if (a > b) {
                cmp = 1;
                break;
            }
        }
        if (cmp <= 0) {
            col_columns_copy_row(dst->columns, wr,
                (int64_t *const *)dst->merge_columns, i, ncols);
            i++;
            wr++;
        } else {
            col_columns_copy_row(dst->columns, wr,
                (int64_t *const *)src->columns, j, ncols);
            j++;
            wr++;
        }
    }
    while (i < N) {
        col_columns_copy_row(dst->columns, wr,
            (int64_t *const *)dst->merge_columns, i, ncols);
        i++;
        wr++;
    }
    while (j < D) {
        col_columns_copy_row(dst->columns, wr,
            (int64_t *const *)src->columns, j, ncols);
        j++;
        wr++;
    }
    dst->nrows = total;
    wl_columnar_relation_touch_view(dst);
    return 0;
}

/*
 * tdd_preregister_idb_on_workers:
 * Pre-register empty IDB relations on each worker session so that
 * VARIABLE ops can find them on the first sub-pass (eff_iter == 0).
 * Mirrors eval_serial.c:276-289.
 */
static int
tdd_preregister_idb_on_workers(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    uint32_t W = coord->tdd_workers_count;

    for (uint32_t w = 0; w < W; w++) {
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const char *rname = sp->relations[ri].name;
            if (session_find_rel(&coord->tdd_workers[w], rname))
                continue;
            col_rel_t *empty = NULL;
            int rc = col_rel_alloc(&empty, rname);
            if (rc != 0)
                return ENOMEM;
            rc = session_add_rel(&coord->tdd_workers[w], empty);
            if (rc != 0) {
                col_rel_destroy(empty);
                return rc;
            }
        }
    }
    return 0;
}

/* Forward declaration — defined below tdd_exchange_deltas. */
static int tdd_broadcast_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W);

/*
 * tdd_broadcast_relation_delta:
 * Union all worker deltas for relation index ri and install the union as
 * $d$<relname> on every worker.  Used by tdd_exchange_deltas for relations
 * that require broadcast (e.g. IDB self-join strata in asymmetric mode).
 *
 * Ownership of ctxs[w].delta_rels[ri] transfers here; all entries are
 * consumed (freed or moved) before return.
 */
static int
tdd_broadcast_relation_delta(const wl_plan_stratum_t *sp, uint32_t ri,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W)
{
    const char *dname = sp->relations[ri].delta_name;

    /* #1661: a refusal leaves the worker's registry owner in place and is
     * surfaced by the same-name session_add_rel below, which re-attempts
     * col_rel_destroy_checked on that owner and returns EBUSY.  The
     * `total == 0` early return below registers nothing, so a refusal on
     * that path surfaces later instead, at cohort teardown: the
     * relation-alias pass of col_worker_session_destroy refuses and the
     * cohort is kept for retry (tests/test_tdd_inline_workers.c). */
    for (uint32_t w = W; w-- > 0; )
        (void)session_remove_rel(&coord->tdd_workers[w], dname);

    uint32_t total = 0, ncols = 0;
    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *d = ctxs[w].delta_rels[ri];
        if (d && d->nrows > 0) {
            if (wl_columnar_eval_checked_row_add(total, d->nrows,
                &total) != 0) {
                tdd_destroy_delta_slots(ctxs, W, sp->relation_count);
                return EOVERFLOW;
            }
            if (ncols == 0)
                ncols = d->ncols;
        }
    }
    if (total == 0) {
        for (uint32_t w = 0; w < W; w++) {
            col_rel_destroy(ctxs[w].delta_rels[ri]);
            ctxs[w].delta_rels[ri] = NULL;
        }
        return 0;
    }

    col_rel_t *union_d = col_rel_new_auto(dname, ncols);
    if (!union_d)
        return ENOMEM;

    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        col_rel_t *d = ctxs[w].delta_rels[ri];
        ctxs[w].delta_rels[ri] = NULL;
        if (d && d->nrows > 0)
            rc = col_rel_append_all(union_d, d, NULL);
        col_rel_destroy(d);
        if (rc != 0) {
            col_rel_destroy(union_d);
            return rc;
        }
    }

    /* Issue #390: Dedup broadcast union to prevent duplicate delta
     * amplification.  In self_join_mode, workers hold disjoint 1/W IDB
     * partitions but can independently derive the same tuple via
     * different join paths.  Matches the Issue #388 dedup in
     * tdd_broadcast_deltas. */
    if (union_d->nrows > 1)
        tdd_dedup_rel(union_d);

    /* Issue #390: zero-copy broadcast via col_shared.
     * Anchor union_d in worker 0's session; workers 1..W-1 borrow
     * column pointers via col_rel_install_shared_view (O(ncols) pointer
     * setup) instead of O(|delta|) deep copies.  Mirrors the Issue #396
     * shared-view broadcast in tdd_broadcast_deltas. */
    rc = session_add_rel(&coord->tdd_workers[0], union_d);
    if (rc != 0) {
        col_rel_destroy(union_d);
        return rc;
    }
    /* union_d is now owned by worker 0's session */
    for (uint32_t dst = 1; dst < W; dst++) {
        col_rel_t *view = col_rel_new_auto(dname, ncols);
        if (!view)
            return ENOMEM;
        rc = col_rel_install_shared_view(view, union_d);
        bool shared = rc == 0;
        if (!shared) {
            /* Fallback: deep copy on shared-view alloc failure */
            rc = tdd_shared_view_deep_copy_fallback(NULL, view, union_d,
                    rc);
            if (rc != 0) {
                col_rel_destroy(view);
                return rc;
            }
        }
        rc = session_add_rel(&coord->tdd_workers[dst], view);
        if (rc != 0) {
            col_rel_destroy(view);
            return rc;
        }
        if (shared) {
            rc = wl_columnar_session_adopt_shared_view(
                &coord->tdd_workers[dst], view);
            if (rc != 0) {
                /* #1661: rollback discard; see the note in
                 * tdd_refresh_global_read_relation. */
                (void)session_remove_rel(&coord->tdd_workers[dst], dname);
                return rc;
            }
        }
    }
    return 0;
}

/*
 * tdd_alloc_exchange_bufs:
 * Allocate the W x W mailbox matrix on the coordinator session.
 * exchange_bufs[src][dst] will hold rows that worker src sends to dst.
 * On failure, exchange_bufs remains NULL.
 */
static int
tdd_alloc_exchange_bufs(wl_col_session_t *coord, uint32_t W)
{
    coord->exchange_bufs
        = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!coord->exchange_bufs)
        return ENOMEM;

    for (uint32_t w = 0; w < W; w++) {
        coord->exchange_bufs[w]
            = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
        if (!coord->exchange_bufs[w]) {
            for (uint32_t j = 0; j < w; j++)
                free((void *)coord->exchange_bufs[j]);
            free((void *)coord->exchange_bufs);
            coord->exchange_bufs = NULL;
            return ENOMEM;
        }
    }

    coord->exchange_num_workers = W;
    return 0;
}

/*
 * tdd_free_exchange_bufs:
 * Destroy all relations held in the W x W mailbox matrix, free the
 * matrix, and clear the coordinator fields.
 */
static void
tdd_free_exchange_bufs(wl_col_session_t *coord)
{
    if (!coord->exchange_bufs)
        return;

    uint32_t W = coord->exchange_num_workers;

    for (uint32_t src = 0; src < W; src++) {
        if (!coord->exchange_bufs[src])
            continue;
        for (uint32_t dst = 0; dst < W; dst++)
            col_rel_destroy(coord->exchange_bufs[src][dst]);
        free((void *)coord->exchange_bufs[src]);
    }

    free((void *)coord->exchange_bufs);
    coord->exchange_bufs = NULL;
    coord->exchange_num_workers = 0;
}

/*
 * tdd_gather_for_worker:
 * Merge exchange_bufs[0..W-1][dst] into a single heap-allocated relation
 * named dname.  Returns NULL in *out_gathered when all source partitions
 * are empty (no new tuples for this worker).
 */
static int
tdd_gather_for_worker(wl_col_session_t *coord, uint32_t dst, uint32_t W,
    const char *dname, col_rel_t **out_gathered)
{
    uint32_t total_rows = 0;
    uint32_t ncols = 0;

    for (uint32_t src = 0; src < W; src++) {
        col_rel_t *part = coord->exchange_bufs[src][dst];
        if (part && part->nrows > 0) {
            if (wl_columnar_eval_checked_row_add(total_rows, part->nrows,
                &total_rows) != 0) {
                tdd_free_exchange_bufs(coord);
                return EOVERFLOW;
            }
            if (ncols == 0)
                ncols = part->ncols;
        }
    }

    *out_gathered = NULL;

    if (total_rows == 0 || ncols == 0)
        return 0;

    col_rel_t *gathered = col_rel_new_auto(dname, ncols);
    if (!gathered)
        return ENOMEM;

    for (uint32_t src = 0; src < W; src++) {
        col_rel_t *part = coord->exchange_bufs[src][dst];
        if (!part || part->nrows == 0)
            continue;
        int rc = col_rel_append_all(gathered, part, NULL);
        if (rc != 0) {
            col_rel_destroy(gathered);
            return rc;
        }
    }

    *out_gathered = gathered;
    return 0;
}

/*
 * tdd_exchange_deltas:
 * After a sub-pass barrier, redistribute worker deltas using
 * hash-partitioned scatter/gather.
 *
 * If the stratum plan contains WL_PLAN_OP_EXCHANGE ops, uses the
 * key column metadata from those ops to hash-partition each worker's
 * delta into exchange_bufs[w][*], then gathers exchange_bufs[*][dst]
 * for each destination worker and installs the result as $d$<relname>.
 * This eliminates broadcast duplicates for plans that carry EXCHANGE ops.
 *
 * If no EXCHANGE ops are present (e.g. transitive closure with a join
 * key that differs from the partition key), falls back to
 * tdd_broadcast_deltas to preserve correctness.
 *
 * Issue #372: When self_join_mode is true (IDB self-join stratum using
 * asymmetric partition-replicate), all relation deltas are broadcast to
 * every worker.  Each worker holds 1/W of the IDB (partitioned by hash)
 * and needs the full delta to probe against its local partition.
 * Hash-partitioning the delta would send each worker only 1/W of the new
 * tuples, causing missed joins with the complementary IDB partition.
 *
 * Ownership of ctxs[w].delta_rels[ri] entries transfers here;
 * all entries are consumed (freed or moved) before return.
 */
static int
tdd_exchange_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W,
    bool default_hash, bool self_join_mode)
{
    uint32_t nrels = sp->relation_count;

    /* Issue #372: Self-join strata use asymmetric partition-replicate.
     * IDB is partitioned (via hybrid init); deltas are broadcast so each
     * worker can probe its 1/W IDB partition with the full delta. */
    if (self_join_mode && default_hash) {
        int rc = 0;
        uint64_t t0 = now_ns();
        for (uint32_t ri = 0; rc == 0 && ri < nrels; ri++)
            rc = tdd_broadcast_relation_delta(sp, ri, coord, ctxs, W);
        coord->tdd_exchange_broadcast_ns += now_ns() - t0;
        return rc;
    }

    /* Check whether any relation plan carries EXCHANGE op metadata. */
    bool has_exchange = false;
    for (uint32_t ri = 0; ri < nrels && !has_exchange; ri++) {
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                has_exchange = true;
                break;
            }
        }
    }

    /* No EXCHANGE metadata and no default hash: broadcast (replicate mode). */
    if (!has_exchange && !default_hash) {
        uint64_t t0 = now_ns();
        int rc = tdd_broadcast_deltas(sp, coord, ctxs, W);
        coord->tdd_exchange_broadcast_ns += now_ns() - t0;
        return rc;
    }

    /* Replicate mode: all workers hold identical data, so hash scatter/gather
     * would produce W× duplicate deltas.  Force broadcast to avoid bloat. */
    if (!default_hash) {
        uint64_t t0 = now_ns();
        int rc = tdd_broadcast_deltas(sp, coord, ctxs, W);
        coord->tdd_exchange_broadcast_ns += now_ns() - t0;
        return rc;
    }

    /* Hash-partitioned scatter/gather exchange. */
    uint64_t matrix_t0 = now_ns();
    int rc = tdd_alloc_exchange_bufs(coord, W);
    coord->tdd_exchange_matrix_ns += now_ns() - matrix_t0;
    if (rc != 0) {
        for (uint32_t w = 0; w < W; w++)
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_destroy(ctxs[w].delta_rels[ri]);
                ctxs[w].delta_rels[ri] = NULL;
            }
        return rc;
    }

    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;

        /* Locate EXCHANGE key columns for this relation (if any). */
        uint64_t prepare_t0 = now_ns();
        const uint32_t *key_cols = NULL;
        uint32_t key_count = 0;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta
                    = (const wl_plan_op_exchange_t *)
                    sp->relations[ri].ops[oi].opaque_data;
                if (meta && meta->key_col_count > 0) {
                    key_cols = meta->key_col_idxs;
                    key_count = meta->key_col_count;
                }
                break;
            }
        }

        /* Remove stale $d$ from every worker before scatter.  #1661: as at
         * the other exchange sites, a refusal leaves the worker's registry
         * owner in place and the same-name registration that follows
         * re-attempts the destroy and returns EBUSY.  This sweep precedes
         * both arms below, but only the scatter arm is reachable from here:
         * the `!default_hash` early return above means default_hash is true
         * past it, so key_count is never left at 0 -- either EXCHANGE
         * metadata supplied key cols, or the default at the top of this loop
         * supplies col0 -- and the broadcast arm's `total == 0` path, and the
         * arm itself, cannot run.  The live non-registering path is
         * a NULL `gathered` in the scatter arm, where tdd_gather_for_worker
         * yields nothing for a destination and the `if (gathered)` guard
         * skips session_add_rel; it defers to cohort teardown.  A refusal is
         * in any case pre-empted upstream by
         * wl_columnar_eval_retire_prior_deltas, which runs unconditionally
         * immediately before the exchange dispatch and re-registers every
         * delta name each worker currently holds.
         *
         * Unlike the other four, this claim rests on reading alone: no test
         * in the suite reaches this site.  Counting executions of all five
         * removal loops over a full `meson test` run recorded zero hits here
         * while the others ran 5840 times between them.  Treat the reasoning
         * above as unverified for this path. */
        for (uint32_t w = W; w-- > 0; )
            (void)session_remove_rel(&coord->tdd_workers[w], dname);

        /* Issue #361: Relations without EXCHANGE ops use default col0
         * hash-exchange when default_hash is set (hybrid init partitions
         * IDB by col0).  EDB is replicated, so joins against partitioned
         * $d$ are complete within each worker. */
        uint32_t default_key[] = { 0 };
        if ((!key_cols || key_count == 0) && default_hash) {
            key_cols = default_key;
            key_count = 1;
        }
        coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;

        if (!key_cols || key_count == 0) {
            /* Broadcast: union worker deltas, install on every worker.
             * Used for replicate-mode strata where IDB is not partitioned. */
            uint64_t broadcast_t0 = now_ns();
            uint32_t total = 0, ncols = 0;
            for (uint32_t w = 0; w < W; w++) {
                col_rel_t *d = ctxs[w].delta_rels[ri];
                if (d && d->nrows > 0) {
                    if (wl_columnar_eval_checked_row_add(total, d->nrows,
                        &total) != 0) {
                        tdd_destroy_delta_slots(ctxs, W, nrels);
                        rc = EOVERFLOW;
                        goto exchange_done;
                    }
                    if (ncols == 0) ncols = d->ncols;
                }
            }
            if (total == 0) {
                for (uint32_t w = 0; w < W; w++) {
                    col_rel_destroy(ctxs[w].delta_rels[ri]);
                    ctxs[w].delta_rels[ri] = NULL;
                }
            } else {
                col_rel_t *union_d = col_rel_new_auto(dname, ncols);
                if (!union_d) {
                    rc = ENOMEM; goto exchange_done;
                }
                for (uint32_t w = 0; w < W; w++) {
                    col_rel_t *d = ctxs[w].delta_rels[ri];
                    ctxs[w].delta_rels[ri] = NULL;
                    if (d && d->nrows > 0)
                        rc = col_rel_append_all(union_d, d, NULL);
                    col_rel_destroy(d);
                    if (rc != 0) {
                        col_rel_destroy(union_d); goto exchange_done;
                    }
                }
                for (uint32_t dst = 0; dst < W; dst++) {
                    col_rel_t *copy;
                    if (dst < W - 1) {
                        copy = col_rel_new_auto(dname, ncols);
                        if (!copy) {
                            col_rel_destroy(union_d); rc = ENOMEM;
                            goto exchange_done;
                        }
                        rc = col_rel_append_all(copy, union_d, NULL);
                        if (rc != 0) {
                            col_rel_destroy(copy); col_rel_destroy(union_d);
                            goto exchange_done;
                        }
                    } else {
                        copy = union_d;
                        union_d = NULL;
                    }
                    rc = session_add_rel(&coord->tdd_workers[dst], copy);
                    if (rc != 0) {
                        col_rel_destroy(copy);
                        if (union_d) col_rel_destroy(union_d);
                        goto exchange_done;
                    }
                }
            }
            coord->tdd_exchange_broadcast_ns += now_ns() - broadcast_t0;
        } else {
            /* Hash-partitioned scatter/gather for EXCHANGE-keyed relations */
            uint64_t scatter_t0 = now_ns();
            for (uint32_t w = 0; w < W; w++) {
                col_rel_t *d = ctxs[w].delta_rels[ri];
                ctxs[w].delta_rels[ri] = NULL;

                if (!d || d->nrows == 0 || d->ncols == 0) {
                    col_rel_destroy(d);
                    continue;
                }

                rc = col_rel_exchange_partition(d, key_cols, key_count,
                        W, coord->exchange_bufs[w]);
                col_rel_destroy(d);

                if (rc != 0)
                    goto exchange_done;
            }
            coord->tdd_exchange_scatter_ns += now_ns() - scatter_t0;

            /* Gather: worker dst receives exchange_bufs[*][dst]. */
            uint64_t gather_t0 = now_ns();
            for (uint32_t dst = 0; dst < W; dst++) {
                col_rel_t *gathered = NULL;
                rc = tdd_gather_for_worker(coord, dst, W, dname, &gathered);
                if (rc != 0)
                    goto exchange_done;

                if (gathered) {
                    rc = session_add_rel(&coord->tdd_workers[dst], gathered);
                    if (rc != 0) {
                        col_rel_destroy(gathered);
                        goto exchange_done;
                    }
                }
            }
            coord->tdd_exchange_gather_ns += now_ns() - gather_t0;
        }

        /* Release exchange_bufs[*][*] for this relation — data was copied
         * into the gathered relations above. */
        uint64_t matrix_release_t0 = now_ns();
        for (uint32_t src = 0; src < W; src++) {
            for (uint32_t dst = 0; dst < W; dst++) {
                col_rel_destroy(coord->exchange_bufs[src][dst]);
                coord->exchange_bufs[src][dst] = NULL;
            }
        }
        coord->tdd_exchange_matrix_ns += now_ns() - matrix_release_t0;
    }

exchange_done:
    matrix_t0 = now_ns();
    tdd_free_exchange_bufs(coord);
    coord->tdd_exchange_matrix_ns += now_ns() - matrix_t0;
    return rc;
}

/*
 * tdd_broadcast_deltas:
 * After a sub-pass barrier, union all worker deltas for each IDB
 * relation and install the union as $d$<relname> on EVERY worker.
 *
 * Ownership of entries in ctxs[w].delta_rels[ri] transfers here;
 * all entries are consumed (freed or moved to a worker session).
 * After return, ctxs[w].delta_rels[ri] == NULL for all w, ri.
 *
 * Workers with no delta for a relation receive no $d$ entry, so
 * has_empty_forced_delta fires and skips that rule next sub-pass.
 */

/*
 * tdd_init_workers_hybrid:
 * Hybrid initialization for data-partitioned strata.
 *
 * IDB relations (those in sp->relations[]) are partitioned across workers
 * by their EXCHANGE key columns.  Non-IDB relations (EDB, earlier-stratum
 * derived) are replicated to every worker.
 *
 * This gives each worker ~1/W of the IDB while ensuring complete join
 * coverage: IDB-EDB joins are always complete because EDB is replicated.
 * The hash-partitioned delta exchange maintains the partition invariant.
 */
#ifdef WL_TEST_BDX_SEED
void (*wl_columnar_eval_test_hybrid_boundary)(unsigned, uint32_t);
#endif

static int
tdd_init_workers_hybrid(const wl_plan_stratum_t *sp, wl_col_session_t *coord,
    bool partition_edb, uint32_t W)
{
    int cleanup_rc = tdd_cleanup_workers(coord);
    if (cleanup_rc != 0)
        return cleanup_rc;

    if (W == 0 || W > coord->num_workers)
        return EINVAL;
    int ensure_rc = wl_columnar_session_ensure_workqueue(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    ensure_rc = wl_columnar_session_ensure_tdd_worker_slots(coord, W);
    if (ensure_rc != 0)
        return ensure_rc;
    coord->tdd_active_workers = W;
    tdd_record_active_workers(coord, W);
    uint32_t nrels = coord->nrels;
    size_t relation_ptr_bytes = 0;
    if (wl_columnar_eval_checked_size_mul(nrels, sizeof(col_rel_t *),
        &relation_ptr_bytes) != 0) {
        return tdd_cleanup_preserve_error(coord, EOVERFLOW);
    }

    if (nrels == 0) {
        for (uint32_t w = 0; w < W; w++) {
            coord->tdd_workers_count = w + 1;
            int rc = col_worker_session_create(coord, w, NULL, 0,
                    &coord->tdd_workers[w]);
            if (rc != 0) {
                return tdd_cleanup_preserve_error(coord, rc);
            }
            coord->tdd_workers_count = w + 1;
        }
        return 0;
    }

    col_rel_t ***worker_rels = (col_rel_t ***)calloc(W, sizeof(col_rel_t **));
    if (!worker_rels) {
        return tdd_cleanup_preserve_error(coord, ENOMEM);
    }

    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        worker_rels[w] = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
        if (!worker_rels[w]) {
            for (uint32_t j = 0; j < w; j++)
                free((void *)worker_rels[j]);
            free((void *)worker_rels);
            return ENOMEM;
        }
    }

    /* Pre-scan stratum EXCHANGE ops for EDB partition info.
     * If an EXCHANGE specifies edb_rel_name + edb_key_col_idxs, we can
     * hash-partition that EDB instead of replicating it, reducing each
     * worker's scan from O(|EDB|) to O(|EDB|/W). */
    const char *edb_part_name = NULL;
    const uint32_t *edb_part_keys = NULL;
    uint32_t edb_part_key_count = 0;

    if (partition_edb) {
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
                if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                    const wl_plan_op_exchange_t *meta =
                        (const wl_plan_op_exchange_t *)
                        sp->relations[ri].ops[oi].opaque_data;
                    if (meta && meta->edb_rel_name
                        && meta->edb_key_col_idxs
                        && meta->edb_key_col_count > 0) {
                        edb_part_name = meta->edb_rel_name;
                        edb_part_keys = meta->edb_key_col_idxs;
                        edb_part_key_count = meta->edb_key_col_count;
                    }
                    break;
                }
            }
            if (edb_part_name)
                break;
        }
    }

    /* Issue #535 hardening: if only one of the two co-partitioned sides carries
     * has_graph_column, col_rel_exchange_partition would pick different hash
     * keys for IDB and EDB → matching tuples land on different workers → silent
     * wrong results.  Detect the asymmetry here and force both sides onto the
     * natural join key (Option A: warn + fallback). */
    bool force_natural_key = false;
    if (edb_part_name) {
        bool idb_graph = false;
        bool edb_graph = false;
        const char *idb_name_found = NULL;
        const char *edb_name_found = NULL;
        for (uint32_t r = 0; r < nrels; r++) {
            col_rel_t *rel = coord->rels[r];
            if (!rel)
                continue;
            /* IDB: appears in sp->relations */
            for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
                if (strcmp(rel->name, sp->relations[ri].name) == 0) {
                    idb_graph = rel->has_graph_column;
                    idb_name_found = rel->name;
                    break;
                }
            }
            /* EDB co-partition target */
            if (strcmp(rel->name, edb_part_name) == 0) {
                edb_graph = rel->has_graph_column;
                edb_name_found = rel->name;
            }
        }
        if (idb_name_found && edb_name_found
            && idb_graph != edb_graph) {
            WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_WARN,
                "EDB/IDB graph-column flag mismatch for rel '%s' vs '%s';"
                " falling back to natural key for both sides to preserve"
                " co-partitioning correctness",
                idb_name_found, edb_name_found);
            force_natural_key = true;
        }
    }

    uint32_t rels_built = 0;

    for (uint32_t r = 0; r < nrels && rc == 0; r++) {
        col_rel_t *rel = coord->rels[r];
        if (!rel)
            continue;

        const char *name = rel->name;

        bool is_idb = false;
        const uint32_t *exchange_key = NULL;
        uint32_t exchange_key_count = 0;

        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            if (strcmp(name, sp->relations[ri].name) != 0)
                continue;
            is_idb = true;
            for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
                if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                    const wl_plan_op_exchange_t *meta =
                        (const wl_plan_op_exchange_t *)
                        sp->relations[ri].ops[oi].opaque_data;
                    if (meta && meta->key_col_count > 0) {
                        exchange_key = meta->key_col_idxs;
                        exchange_key_count = meta->key_col_count;
                    }
                    break;
                }
            }
            break;
        }

        if (is_idb && rel->nrows == 0) {
            /* Empty IDBs are writable worker state, not shared EDB views.
             * Their leases would otherwise prevent coordinator publication. */
            wl_columnar_source_access_reader_t reader = { 0 };
            rc = col_rel_source_reader_acquire(rel, &reader);
            if (rc != 0)
                break;
            for (uint32_t w = 0; w < W && rc == 0; w++) {
#ifdef WL_TEST_BDX_SEED
                if (wl_columnar_eval_test_hybrid_boundary)
                    wl_columnar_eval_test_hybrid_boundary(0, w);
#endif
                col_rel_t *empty = wl_columnar_relation_new_like_governed(
                    name, rel, coord->memory_governor);
                if (!empty) {
                    rc = ENOMEM;
                    break;
                }
                empty->declared_ncols = rel->declared_ncols;
                worker_rels[w][rels_built] = empty;
#ifdef WL_TEST_BDX_SEED
                if (wl_columnar_eval_test_hybrid_boundary)
                    wl_columnar_eval_test_hybrid_boundary(2, w);
#endif
                rc = WL_COLUMNAR_EVAL_DEDUP_SET_INIT_FROM_REL(empty);
            }
            int release_rc = col_rel_source_reader_release(&reader);
            if (rc == 0)
                rc = release_rc;
        } else if (is_idb && rel->nrows > 0 && rel->ncols > 0) {
            uint32_t default_key[] = { 0 };
            const uint32_t *key = (exchange_key && exchange_key_count > 0)
                ? exchange_key : default_key;
            uint32_t key_count = (exchange_key && exchange_key_count > 0)
                ? exchange_key_count : 1u;

            col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
            if (!parts) {
                rc = ENOMEM;
            } else {
                /* When graph-flag mismatch detected, bypass the graph-key
                 * override in col_rel_exchange_partition and use the natural
                 * join key directly so both sides hash identically. */
                if (force_natural_key)
                    rc = col_rel_partition_by_key(rel, key, key_count,
                            W, parts);
                else
                    rc = col_rel_exchange_partition(rel, key, key_count,
                            W, parts);
                if (rc == 0) {
                    for (uint32_t w = 0; w < W && rc == 0; w++) {
                        free(parts[w]->name);
                        parts[w]->name = wl_strdup(name);
                        if (!parts[w]->name) {
                            rc = ENOMEM;
                        } else {
                            /* Init hash-set dedup for O(1) consolidation. */
                            WL_COLUMNAR_EVAL_DEDUP_SET_INIT_FROM_REL(parts[w]);
                            worker_rels[w][rels_built] = parts[w];
                            parts[w] = NULL;
                        }
                    }
                }
                for (uint32_t w = 0; w < W; w++)
                    col_rel_destroy(parts[w]);
                free((void *)parts);
            }
        } else if (edb_part_name && rel->nrows > 0
            && strcmp(name, edb_part_name) == 0
            && edb_part_key_count > 0
            && rel->ncols > 0) {
            /* EDB partitioning: hash-partition this EDB by the join key
             * so each worker scans only ~1/W of the rows.  The partition
             * key (edb_part_keys) matches the IDB exchange key through
             * the join condition, ensuring local join completeness. */
            col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
            if (!parts) {
                rc = ENOMEM;
            } else {
                /* When graph-flag mismatch detected, bypass the graph-key
                 * override and use the supplied EDB key directly. */
                if (force_natural_key)
                    rc = col_rel_partition_by_key(rel, edb_part_keys,
                            edb_part_key_count, W, parts);
                else
                    rc = col_rel_exchange_partition(rel, edb_part_keys,
                            edb_part_key_count, W, parts);
                if (rc == 0) {
                    for (uint32_t w = 0; w < W && rc == 0; w++) {
                        free(parts[w]->name);
                        parts[w]->name = wl_strdup(name);
                        if (!parts[w]->name) {
                            rc = ENOMEM;
                        } else {
                            worker_rels[w][rels_built] = parts[w];
                            parts[w] = NULL;
                        }
                    }
                }
                for (uint32_t w = 0; w < W; w++)
                    col_rel_destroy(parts[w]);
                free((void *)parts);
            }
        } else {
            /* Zero-copy EDB sharing: workers borrow the coordinator's
             * column buffers instead of deep-copying.  The coordinator
             * relation outlives the workers, so borrowing is safe.
             * This eliminates W× memory duplication and cache thrashing. */
            for (uint32_t w = 0; w < W && rc == 0; w++) {
                col_rel_t *view = col_rel_new_auto(name, rel->ncols);
                if (!view) {
                    rc = ENOMEM;
                    break;
                }
                if (rel->ncols > 0) {
                    rc = col_rel_install_shared_view(view, rel);
                } else {
                    rc = 0;
                }
                if (rc != 0) {
                    /* Preserve the pre-existing deep-copy fallback when
                     * the shared-view bookkeeping allocation is rejected. */
                    rc = tdd_shared_view_deep_copy_fallback(NULL, view,
                            rel, rc);
                    if (rc != 0) {
                        col_rel_destroy(view);
                        break;
                    }
                }
                worker_rels[w][rels_built] = view;
            }
        }

        if (rc == 0)
            rels_built++;
    }

    uint32_t created = 0;
    if (rc == 0) {
        for (uint32_t w = 0; w < W; w++) {
            coord->tdd_workers_count = w + 1;
#ifdef WL_TEST_BDX_SEED
            if (wl_columnar_eval_test_hybrid_boundary)
                wl_columnar_eval_test_hybrid_boundary(1, w);
#endif
            rc = col_worker_session_create(coord, w,
                    worker_rels[w], rels_built, &coord->tdd_workers[w]);
            if (rc != 0)
                break;
            created++;
        }
    }

    if (rc != 0) {
        for (uint32_t w = created; w < W; w++) {
            /* Untransferred slots include the partly constructed current
             * relation; successful transfers are explicitly NULLed. */
            for (uint32_t p = 0; p < nrels; p++)
                col_rel_destroy(worker_rels[w][p]);
        }
        int cleanup_rc = tdd_cleanup_workers(coord);
        if (cleanup_rc != 0)
            rc = cleanup_rc;
    } else {
        coord->tdd_workers_count = W;
    }

    for (uint32_t w = 0; w < W; w++)
        free((void *)worker_rels[w]);
    free((void *)worker_rels);

    return rc;
}

#ifdef WL_TEST_BDX_SEED
int
wl_columnar_eval_test_hybrid_init(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t workers)
{
    return tdd_init_workers_hybrid(sp, coord, true, workers);
}

int
wl_columnar_eval_test_hybrid_cleanup(wl_col_session_t *coord)
{
    return tdd_cleanup_workers(coord);
}
#endif

static int
tdd_broadcast_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W)
{
    uint32_t nrels = sp->relation_count;

    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;

        /* Count total rows and find ncols */
        uint32_t total_rows = 0;
        uint32_t ncols = 0;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            if (d && d->nrows > 0) {
                if (wl_columnar_eval_checked_row_add(total_rows, d->nrows,
                    &total_rows) != 0) {
                    tdd_destroy_delta_slots(ctxs, W, nrels);
                    return EOVERFLOW;
                }
                if (ncols == 0)
                    ncols = d->ncols;
            }
        }

        /* Issue #361: Reuse pre-installed $d$ on workers when available.
         * Avoids per-iteration alloc/free/session_remove/session_add. */
        if (total_rows == 0) {
            for (uint32_t w = 0; w < W; w++) {
                col_rel_destroy(ctxs[w].delta_rels[ri]);
                ctxs[w].delta_rels[ri] = NULL;
                /* Prior $d$ was retired to an empty owner after the barrier. */
            }
            continue;
        }

        /* Build union delta: try reusing worker 0's existing $d$ as union buf */
        col_rel_t *union_d = NULL;
        bool union_from_session = false;
        col_rel_t *slot0 = session_find_rel(&coord->tdd_workers[0], dname);
        if (slot0 && slot0->ncols == ncols) {
            /* Reuse pre-installed $d$ on worker 0 as union buffer */
            slot0->nrows = 0;
            wl_columnar_relation_touch_view(slot0);
            union_d = slot0;
            union_from_session = true;
        } else {
            union_d = col_rel_new_auto(dname, ncols);
            if (!union_d) {
                for (uint32_t w = 0; w < W; w++) {
                    col_rel_destroy(ctxs[w].delta_rels[ri]);
                    ctxs[w].delta_rels[ri] = NULL;
                }
                return ENOMEM;
            }
        }

        int append_rc = 0;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            ctxs[w].delta_rels[ri] = NULL;
            if (!d) continue;
            if (append_rc == 0 && d->nrows > 0)
                append_rc = col_rel_append_all(union_d, d, NULL);
            col_rel_destroy(d);
        }

        if (append_rc != 0) {
            if (!union_from_session)
                col_rel_destroy(union_d);
            return append_rc;
        }

        /* Issue #388: Dedup broadcast union to prevent W-fold amplification.
         * In replicate mode, all W workers derive the same tuples from
         * identical data. Without dedup, the union contains W copies of
         * each new tuple, compounding exponentially across iterations. */
        if (union_d->nrows > 1)
            tdd_dedup_rel(union_d);

        /* Issue #396: zero-copy broadcast via col_shared.
         * Anchor union_d in worker 0's session so its lifetime covers all
         * workers' reads during the next sub-pass.  Workers 1..W-1 borrow
         * union_d's column pointers via col_rel_install_shared_view (O(ncols)
         * pointer setup) instead of O(|delta|) deep copies. */
        if (!union_from_session) {
            /* Install union_d into worker 0's session as the authoritative $d$.
            * session_add_rel replaces any existing entry with the same name. */
            int rc = session_add_rel(&coord->tdd_workers[0], union_d);
            if (rc != 0) {
                col_rel_destroy(union_d);
                return rc;
            }
            union_from_session = true;
            /* slot0 now points to union_d (owned by worker 0's session) */
        }
        /* worker 0 already holds union_d; install shared views on workers 1..W-1 */
        for (uint32_t w = 1; w < W; w++) {
            col_rel_t *worker_d = session_find_rel(
                &coord->tdd_workers[w], dname);
            if (worker_d && worker_d->ncols == ncols) {
                /* Reuse: install shared view (O(ncols) pointer setup) */
                int rc = wl_columnar_session_install_shared_view(
                    &coord->tdd_workers[w], worker_d, union_d);
                if (rc != 0) {
                    /* Fallback: deep copy on shared-view alloc failure */
                    rc = tdd_shared_view_deep_copy_fallback(
                        &coord->tdd_workers[w], worker_d, union_d, rc);
                    if (rc != 0)
                        return rc;
                }
            } else {
                /* First iteration or schema mismatch: create new relation */
                col_rel_t *new_d = col_rel_new_auto(dname, ncols);
                if (!new_d)
                    return ENOMEM;
                int rc = col_rel_install_shared_view(new_d, union_d);
                bool shared = rc == 0;
                if (!shared) {
                    /* Fallback: deep copy on shared-view alloc failure */
                    rc = tdd_shared_view_deep_copy_fallback(NULL, new_d,
                            union_d, rc);
                    if (rc != 0) {
                        col_rel_destroy(new_d);
                        return rc;
                    }
                }
                rc = session_add_rel(&coord->tdd_workers[w], new_d);
                if (rc != 0) {
                    col_rel_destroy(new_d);
                    return rc;
                }
                if (shared) {
                    rc = wl_columnar_session_adopt_shared_view(
                        &coord->tdd_workers[w], new_d);
                    if (rc != 0) {
                        /* #1661: rollback discard; see the note in
                         * tdd_refresh_global_read_relation. */
                        (void)session_remove_rel(&coord->tdd_workers[w],
                            dname);
                        return rc;
                    }
                }
            }
        }
        /* union_d is owned by worker 0's session; no explicit free needed */
        (void)union_from_session;
    }

    return 0;
}

/*
 * tdd_record_recursive_convergence:
 * After recursive fixed-point convergence, record stratum and per-rule
 * frontiers on the coordinator.  Mirrors eval_serial.c:753-776 for the
 * TDD path.
 */
static void
tdd_record_recursive_convergence(wl_col_session_t *coord,
    const wl_plan_stratum_t *sp, uint32_t stratum_idx,
    uint32_t rule_id_base, uint32_t final_eff_iter)
{
    uint32_t nrels = sp->relation_count;

    /* Per-rule frontier (eval_serial.c:756-760) */
    for (uint32_t ri = 0; ri < nrels && rule_id_base + ri < MAX_RULES; ri++) {
        coord->frontier_ops->record_rule_convergence(coord,
            rule_id_base + ri, coord->outer_epoch, final_eff_iter);
    }

    /* Stratum frontier (eval_serial.c:765-767) */
    coord->frontier_ops->record_stratum_convergence(coord,
        stratum_idx, coord->outer_epoch, final_eff_iter);
}

/*
 * tdd_record_nonrecursive_convergence:
 * After non-recursive stratum dispatch, record stratum and per-rule frontiers.
 * Non-recursive strata always converge in one step; uses UINT32_MAX sentinel.
 * Mirrors eval_serial.c:232-264 for the TDD coordinator path.
 */
static void
tdd_record_nonrecursive_convergence(wl_col_session_t *coord,
    const wl_plan_stratum_t *sp, uint32_t stratum_idx)
{
    /* Stratum frontier: UINT32_MAX sentinel (eval_serial.c:237-238) */
    coord->frontier_ops->record_stratum_convergence(coord,
        stratum_idx, coord->outer_epoch, UINT32_MAX);

    /* Per-rule frontiers (eval_serial.c:250-264) */
    if (coord->plan) {
        uint32_t rule_base = 0;
        for (uint32_t si = 0; si < stratum_idx; si++)
            rule_base += coord->plan->strata[si].relation_count;
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            uint32_t rule_idx = rule_base + ri;
            if (rule_idx < MAX_RULES)
                coord->frontier_ops->reset_rule_frontier(coord, rule_idx,
                    coord->outer_epoch);
        }
    }
}

/*
 * tdd_check_convergence:
 * Returns true if global fixed-point reached: no worker produced new tuples.
 * Called after each sub-pass barrier, before the exchange step.
 */
static bool
tdd_check_convergence(const col_eval_tdd_worker_ctx_t *ctxs, uint32_t W)
{
    for (uint32_t w = 0; w < W; w++) {
        if (ctxs[w].any_new)
            return false;
    }
    return true;
}

static bool
tdd_estimate_add_rel_name(const char **names, uint32_t *count, uint32_t cap,
    const char *name)
{
    if (!name || !names || !count)
        return true;
    for (uint32_t i = 0; i < *count; i++) {
        if (names[i] && strcmp(names[i], name) == 0)
            return true;
    }
    if (*count >= cap)
        return false;
    names[(*count)++] = name;
    return true;
}

static bool
tdd_estimate_collect_ops(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp, const char **names, uint32_t *count,
    uint32_t cap)
{
    (void)sp;
    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        const char *rel_name = NULL;
        if (op->op == WL_PLAN_OP_VARIABLE) {
            rel_name = op->relation_name;
        } else if (op->op == WL_PLAN_OP_JOIN
            || op->op == WL_PLAN_OP_SEMIJOIN
            || op->op == WL_PLAN_OP_ANTIJOIN) {
            rel_name = op->right_relation;
        } else if (op->op == WL_PLAN_OP_K_FUSION && op->opaque_data) {
            const wl_plan_op_k_fusion_t *kf =
                (const wl_plan_op_k_fusion_t *)op->opaque_data;
            for (uint32_t ki = 0; ki < kf->k; ki++) {
                if (!tdd_estimate_collect_ops(kf->k_ops[ki],
                    kf->k_op_counts[ki], sp, names, count, cap))
                    return false;
            }
        } else if (op->op == WL_PLAN_OP_LFTJ && op->opaque_data) {
            const wl_plan_op_lftj_t *lftj =
                (const wl_plan_op_lftj_t *)op->opaque_data;
            for (uint32_t i = 0; i < lftj->k; i++) {
                if (!tdd_estimate_add_rel_name(names, count, cap,
                    lftj->rel_names ? lftj->rel_names[i] : NULL))
                    return false;
            }
        }
        if (!tdd_estimate_add_rel_name(names, count, cap, rel_name))
            return false;
    }
    return true;
}

static uint64_t
tdd_estimate_stratum_work_rows(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    const char *names[256];
    uint32_t name_count = 0;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        if (!tdd_estimate_add_rel_name(names, &name_count, 256,
            sp->relations[ri].name))
            goto fallback;
        if (!tdd_estimate_collect_ops(sp->relations[ri].ops,
            sp->relations[ri].op_count, sp, names, &name_count, 256))
            goto fallback;
    }

    uint64_t rows = 0;
    for (uint32_t i = 0; i < name_count; i++) {
        col_rel_t *r = session_find_rel(coord, names[i]);
        if (r)
            rows += r->nrows;
    }
    return rows;

fallback:
    rows = 0;
    for (uint32_t ri = 0; ri < coord->nrels; ri++) {
        col_rel_t *r = coord->rels[ri];
        if (r)
            rows += r->nrows;
    }
    return rows;
}

static uint32_t
tdd_choose_active_workers(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t max_workers, bool replicate_mode)
{
    if (max_workers <= 1)
        return 1;

    const char *env = getenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    uint64_t rows_per_worker = 4096;
    if (env && env[0] != '\0') {
        char *endp = NULL;
        errno = 0;
        unsigned long long v = strtoull(env, &endp, 10);
        if (endp != env && *endp == '\0' && errno != ERANGE && v > 0)
            rows_per_worker = (uint64_t)v;
    }

    uint64_t rows = tdd_estimate_stratum_work_rows(sp, coord);
    uint32_t active = 1;
    if (rows > 0) {
        uint64_t wanted = (rows + rows_per_worker - 1) / rows_per_worker;
        if (wanted > UINT32_MAX)
            wanted = UINT32_MAX;
        active = (uint32_t)wanted;
    }

    if (replicate_mode && active > 8)
        active = 8;
    if (!replicate_mode) {
        uint32_t max_active = 32;
        env = getenv("WIRELOG_TDD_MAX_ACTIVE_WORKERS");
        if (env && env[0] != '\0') {
            char *endp = NULL;
            errno = 0;
            unsigned long v = strtoul(env, &endp, 10);
            if (endp != env && *endp == '\0' && errno != ERANGE && v > 0
                && v <= UINT32_MAX)
                max_active = (uint32_t)v;
        }
        if (active > max_active)
            active = max_active;
    }
    if (active < 1)
        active = 1;
    if (active > max_workers)
        active = max_workers;
    return active;
}

static uint32_t
tdd_global_read_worker_cap(void)
{
    uint32_t cap = 16;
    const char *env = getenv("WIRELOG_TDD_GLOBAL_READ_MAX_ACTIVE_WORKERS");
    if (env && env[0] != '\0') {
        char *endp = NULL;
        errno = 0;
        unsigned long v = strtoul(env, &endp, 10);
        if (endp != env && *endp == '\0' && errno != ERANGE && v > 0
            && v <= UINT32_MAX)
            cap = (uint32_t)v;
    }
    return cap;
}

/*
 * bdx_hash_diff:
 * Remove from delta any row that already exists in base.  This exact
 * hash-table diff does not require sorted inputs, which matches the current
 * BDX data flow where worker deltas are appended in partition order and
 * tdd_dedup_rel() may preserve unsorted encounter order.
 */
static int
bdx_hash_diff(col_rel_t *delta, const col_rel_t *base)
{
    if (!delta || delta->nrows == 0 || !base || base->nrows == 0)
        return 0;
    if (delta->ncols != base->ncols)
        return EINVAL;
    if (base->nrows > UINT32_MAX - 1)
        return ENOMEM;
#if SIZE_MAX <= UINT32_MAX
    if (base->nrows > SIZE_MAX / 2)
        return ENOMEM;
#endif

    size_t target = (size_t)base->nrows * 2;
    size_t cap_sz = 4;
    while (cap_sz < target) {
        if (cap_sz > SIZE_MAX / 2)
            return ENOMEM;
        cap_sz <<= 1;
    }
    if (cap_sz > UINT32_MAX)
        return ENOMEM;
    if (cap_sz > SIZE_MAX / sizeof(uint32_t))
        return ENOMEM;

    uint32_t cap = (uint32_t)cap_sz;
    uint32_t mask = cap - 1;
    uint32_t *slots = (uint32_t *)malloc(cap * sizeof(uint32_t));
    if (!slots)
        return ENOMEM;
    memset(slots, 0, cap * sizeof(uint32_t));

    uint32_t ncols = base->ncols;
    for (uint32_t i = 0; i < base->nrows; i++) {
        uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(base, i);
        uint32_t slot = (uint32_t)(h & mask);
        while (slots[slot] != 0)
            slot = (slot + 1) & mask;
        slots[slot] = i + 1;
    }

    uint32_t wr = 0;
    for (uint32_t di = 0; di < delta->nrows; di++) {
        uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(delta, di);
        uint32_t slot = (uint32_t)(h & mask);
        bool found = false;
        while (slots[slot] != 0) {
            uint32_t bi = slots[slot] - 1;
            bool eq = true;
            for (uint32_t c = 0; c < ncols; c++) {
                if (base->columns[c][bi] != delta->columns[c][di]) {
                    eq = false;
                    break;
                }
            }
            if (eq) {
                found = true;
                break;
            }
            slot = (slot + 1) & mask;
        }
        if (found)
            continue;
        if (wr != di) {
            col_columns_copy_row(delta->columns, wr,
                (int64_t *const *)delta->columns, di, ncols);
            if (delta->timestamps)
                delta->timestamps[wr] = delta->timestamps[di];
        }
        wr++;
    }

    free(slots);
    delta->nrows = wr;
    wl_columnar_relation_touch_view(delta);
    return 0;
}

static int
tdd_hashset_diff(col_rel_t *delta, const col_rel_t *base)
{
    if (!delta || delta->nrows == 0 || !base || base->nrows == 0)
        return 0;
    if (delta->ncols != base->ncols)
        return EINVAL;
    if (!base->dedup_slots)
        return bdx_hash_diff(delta, base);

    uint32_t wr = 0;
    for (uint32_t di = 0; di < delta->nrows; di++) {
        uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(delta, di);
        if (WL_COLUMNAR_EVAL_DEDUP_SET_CONTAINS(base, h))
            continue;
        if (wr != di) {
            col_columns_copy_row(delta->columns, wr,
                (int64_t *const *)delta->columns, di, delta->ncols);
            if (delta->timestamps)
                delta->timestamps[wr] = delta->timestamps[di];
        }
        wr++;
    }
    delta->nrows = wr;
    wl_columnar_relation_touch_view(delta);
    return 0;
}

static void
tdd_dedup_set_insert_rel(col_rel_t *target, const col_rel_t *rows)
{
    if (!target || !target->dedup_slots || !rows)
        return;
    for (uint32_t row = 0; row < rows->nrows; row++) {
        uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(rows, row);
        WL_COLUMNAR_EVAL_DEDUP_SET_INSERT(target, h);
    }
}

static void
tdd_clear_relation_dedup_set(col_rel_t *r)
{
    if (!r)
        return;
    free(r->dedup_slots);
    r->dedup_slots = NULL;
    r->dedup_cap = 0;
    r->dedup_count = 0;
}

/* Snapshot a published relation without carrying over its session-owned
 * accounting or source-storage leases.  col_rel_deep_copy deliberately
 * treats the dedup table as a rebuildable cache; coordinator fallback needs a
 * lossless snapshot instead, so copy it explicitly and reject any metadata
 * degradation from the deep-copy helper. */
static int
tdd_snapshot_relation(const col_rel_t *source, col_rel_t **out)
{
    col_rel_t *snapshot = NULL;
    uint32_t compound_entries = 0;
    int rc;

    if (!source || !out)
        return EINVAL;
    *out = NULL;
    rc = col_rel_deep_copy(source, &snapshot, NULL);
    if (rc != 0)
        return rc;
    if (snapshot->compound_kind != source->compound_kind
        || snapshot->compound_count != source->compound_count
        || snapshot->inline_physical_offset
        != source->inline_physical_offset
        || snapshot->declared_ncols != source->declared_ncols
        || snapshot->has_graph_column != source->has_graph_column
        || snapshot->graph_col_idx != source->graph_col_idx) {
        col_rel_destroy(snapshot);
        return EINVAL;
    }
    if (source->compound_arity_map) {
        if (!tdd_compound_map_entries(source, &compound_entries)
            || !snapshot->compound_arity_map
            || memcmp(snapshot->compound_arity_map,
            source->compound_arity_map,
            (size_t)compound_entries * sizeof(uint32_t)) != 0) {
            col_rel_destroy(snapshot);
            return EINVAL;
        }
    }
    if (source->dedup_cap > 0) {
        if (!source->dedup_slots)
            goto invalid;
        snapshot->dedup_slots = (uint64_t *)malloc(
            (size_t)source->dedup_cap * sizeof(*snapshot->dedup_slots));
        if (!snapshot->dedup_slots) {
            col_rel_destroy(snapshot);
            return ENOMEM;
        }
        memcpy(snapshot->dedup_slots, source->dedup_slots,
            (size_t)source->dedup_cap * sizeof(*snapshot->dedup_slots));
        snapshot->dedup_cap = source->dedup_cap;
        snapshot->dedup_count = source->dedup_count;
    }
    *out = snapshot;
    return 0;

invalid:
    col_rel_destroy(snapshot);
    return EINVAL;
}

static int
tdd_empty_relation_candidate(const col_rel_t *source, col_rel_t **out)
{
    int rc = tdd_snapshot_relation(source, out);
    col_rel_t *candidate;

    if (rc != 0)
        return rc;
    candidate = *out;
    candidate->nrows = 0;
    candidate->sorted_nrows = 0;
    candidate->base_nrows = 0;
    candidate->run_count = 0;
    memset(candidate->run_ends, 0, sizeof(candidate->run_ends));
    free(candidate->timestamps);
    candidate->timestamps = NULL;
    candidate->timestamp_capacity = 0;
    candidate->ledger_ts_bytes = 0;
    tdd_clear_relation_dedup_set(candidate);
    return 0;
}

static int
tdd_reset_coord_relation(col_rel_t *relation)
{
    col_rel_t *candidate = NULL;
    col_rel_replacement_t replacement;
    int rc;

    if (!relation)
        return EINVAL;
    rc = tdd_empty_relation_candidate(relation, &candidate);
    if (rc != 0)
        return rc;
    memset(&replacement, 0, sizeof(replacement));
    rc = col_rel_prepare_replacement(relation, candidate, &replacement);
    if (rc == 0) {
        col_rel_commit_replacement_locked(relation, &replacement);
        rc = 0;
    }
    if (rc != 0)
        col_rel_discard_replacement(&replacement);
    col_rel_destroy(candidate);
    return rc;
}

static int
tdd_save_coord_idb(const wl_plan_stratum_t *sp, wl_col_session_t *coord,
    col_rel_t ***out_saved)
{
    if (!out_saved)
        return EINVAL;
    *out_saved = NULL;
    size_t saved_bytes = 0;
    if (wl_columnar_eval_checked_size_mul(sp->relation_count,
        sizeof(col_rel_t *), &saved_bytes) != 0)
        return EOVERFLOW;
    col_rel_t **saved = (col_rel_t **)calloc(
        sp->relation_count, sizeof(col_rel_t *));
    int rc = 0;
    if (!saved)
        return ENOMEM;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        col_rel_t *r = session_find_rel(coord, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;
        rc = tdd_snapshot_relation(r, &saved[ri]);
        if (rc != 0)
            goto fail;
    }
    *out_saved = saved;
    return 0;

fail:
    for (uint32_t ri = 0; ri < sp->relation_count; ri++)
        col_rel_destroy(saved[ri]);
    free((void *)saved);
    return rc;
}

typedef struct {
    const char *name;
    col_rel_t *relation;
    col_rel_t *owner;
    col_rel_t *saved;
    col_rel_t *candidate;
    col_rel_replacement_t replacement;
    bool replacement_prepared;
    bool registered;
} tdd_restore_entry_t;

static int
tdd_restore_entry_compare(const void *left, const void *right)
{
    const tdd_restore_entry_t *a = (const tdd_restore_entry_t *)left;
    const tdd_restore_entry_t *b = (const tdd_restore_entry_t *)right;

    if (!a->owner && !b->owner)
        return strcmp(a->name, b->name);
    if (!a->owner)
        return 1;
    if (!b->owner)
        return -1;
    if (a->owner->relation_identity < b->owner->relation_identity)
        return -1;
    if (a->owner->relation_identity > b->owner->relation_identity)
        return 1;
    return strcmp(a->name, b->name);
}

/* Roll back a failed tdd_restore_coord_idb transaction.
 *
 * This unwinds registered candidates with unchecked col_rel_destroy and
 * clears the registry slot unconditionally -- the shape #1661's integration
 * audit flags elsewhere.  It is safe here only because the registered set is
 * closed: every candidate comes from col_rel_alloc, tdd_snapshot_relation or
 * tdd_empty_relation_candidate, is heap-backed, and is never leased,
 * evaluated or aliased between registration and this rollback, so
 * col_rel_destroy_checked cannot refuse.  Reachability is the whole
 * invariant; the slot arithmetic is not a second line of defence, because on
 * a refusal the slot would stay live and the coord->nrels truncation below
 * would strand it.  The branch that would need repair first is the
 * hash-fallback recovery in tdd_restore_coord_idb, which removes a
 * registered candidate without setting entry->registered.  If candidates
 * ever become leasable, fix that branch before this one. */
static void
tdd_restore_entries_discard(wl_col_session_t *coord,
    tdd_restore_entry_t *entries, uint32_t count, uint32_t initial_nrels)
{
    if (!entries)
        return;
    for (uint32_t i = count; i-- > 0; ) {
        if (entries[i].registered) {
            for (uint32_t ri = 0; ri < coord->nrels; ri++) {
                if (coord->rels[ri]
                    && strcmp(coord->rels[ri]->name, entries[i].name) == 0) {
                    col_rel_destroy(coord->rels[ri]);
                    coord->rels[ri] = NULL;
                    break;
                }
            }
            entries[i].registered = false;
            entries[i].candidate = NULL;
        }
    }
    coord->nrels = initial_nrels;
    (void)session_rel_build_hash(coord);
    for (uint32_t i = 0; i < count; i++) {
        if (entries[i].replacement_prepared
            || entries[i].replacement.writer_acquired)
            col_rel_discard_replacement(&entries[i].replacement);
        if (entries[i].candidate)
            col_rel_destroy(entries[i].candidate);
    }
    free(entries);
}

static int
tdd_restore_coord_idb(const wl_plan_stratum_t *sp, wl_col_session_t *coord,
    col_rel_t **saved)
{
    tdd_restore_entry_t *entries = NULL;
    size_t entry_bytes;
    uint32_t count;
    uint32_t initial_nrels;
    int rc = 0;

    if (!saved)
        return 0;
    initial_nrels = coord->nrels;
    count = sp->relation_count;
    if (wl_columnar_eval_checked_size_mul(count, sizeof(*entries),
        &entry_bytes) != 0)
        return EOVERFLOW;
    entries = (tdd_restore_entry_t *)calloc(1, entry_bytes);
    if (!entries && count > 0)
        return ENOMEM;

    /* Build every private candidate before touching a published relation or
     * the session registry.  This includes candidates for relations that are
     * absent from the registry; registration is delayed until all existing
     * owners have passed admission. */
    for (uint32_t ri = 0; ri < count; ri++) {
        tdd_restore_entry_t *entry = &entries[ri];
        entry->name = sp->relations[ri].name;
        entry->relation = session_find_rel(coord, entry->name);
        entry->saved = saved[ri];
        if (entry->relation) {
            rc = col_rel_storage_owner_resolve(entry->relation,
                    &entry->owner);
            if (rc != 0)
                goto fail;
            if (entry->owner != entry->relation
                || entry->owner->storage_alias_borrows > 0) {
                rc = EBUSY;
                goto fail;
            }
        }
        if (entry->saved)
            rc = tdd_snapshot_relation(entry->saved, &entry->candidate);
        else if (entry->relation)
            rc = tdd_empty_relation_candidate(entry->relation,
                    &entry->candidate);
        else
            rc = col_rel_alloc(&entry->candidate, entry->name);
        if (rc != 0)
            goto fail;
    }

    /* Acquire every canonical writer in a deterministic order.  Rejecting a
     * later reader/alias therefore happens before any relation is changed. */
    qsort(entries, count, sizeof(*entries), tdd_restore_entry_compare);
    for (uint32_t i = 0; i < count; i++) {
        tdd_restore_entry_t *entry = &entries[i];
        if (!entry->relation)
            continue;
        if (i > 0 && entries[i - 1].owner == entry->owner) {
            rc = EBUSY;
            goto fail;
        }
        memset(&entry->replacement, 0, sizeof(entry->replacement));
        wl_columnar_memory_reservation_init(
            &entry->replacement.reservation);
        rc = wl_columnar_source_access_writer_acquire(
            &entry->owner->source_access, &entry->replacement.writer);
        if (rc != 0)
            goto fail;
        entry->replacement.writer_acquired = true;
    }

    /* Prepare all replacements while all writers remain held.  Preparation
     * may allocate or fail, but publication has not begun and cleanup can
     * release every admission without changing a destination. */
    for (uint32_t i = 0; i < count; i++) {
        tdd_restore_entry_t *entry = &entries[i];
        if (!entry->relation)
            continue;
        rc = col_rel_prepare_replacement_locked(entry->relation,
                entry->candidate, &entry->replacement);
        if (rc != 0)
            goto fail;
        entry->replacement_prepared = true;
        col_rel_destroy(entry->candidate);
        entry->candidate = NULL;
    }

    /* Register new relations only after all fallible replacement preparation
     * has completed.  If registration fails, remove only the registrations
     * made by this transaction; no existing relation has been published. */
    for (uint32_t i = 0; i < count; i++) {
        tdd_restore_entry_t *entry = &entries[i];
        if (entry->relation)
            continue;
        rc = session_add_rel(coord, entry->candidate);
        if (rc != 0) {
            if (session_find_rel(coord, entry->name) == entry->candidate)
                (void)session_remove_rel(coord, entry->name);
            else
                col_rel_destroy(entry->candidate);
            entry->candidate = NULL;
            goto fail;
        }
        entry->registered = true;
        entry->candidate = NULL;
    }

    /* Commit is deliberately last.  The replacement primitive guarantees
     * that this phase performs no allocation, schema construction, or
     * admission; every possible failure was handled above. */
    for (uint32_t i = 0; i < count; i++) {
        tdd_restore_entry_t *entry = &entries[i];
        if (!entry->relation)
            continue;
        col_rel_commit_replacement_locked(entry->relation,
            &entry->replacement);
        entry->replacement_prepared = false;
        col_session_invalidate_arrangements(&coord->base, entry->name);
    }
    free(entries);
    return 0;

fail:
    tdd_restore_entries_discard(coord, entries, count, initial_nrels);
    return rc;
}

static void
tdd_free_saved_coord_idb(const wl_plan_stratum_t *sp, col_rel_t **saved)
{
    if (!saved)
        return;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++)
        col_rel_destroy(saved[ri]);
    free((void *)saved);
}

#ifdef WL_TEST_TDD_RESET_RESTORE
int
wl_columnar_eval_test_tdd_reset(col_rel_t *relation)
{
    return tdd_reset_coord_relation(relation);
}

int
wl_columnar_eval_test_tdd_save(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_rel_t ***out_saved)
{
    return tdd_save_coord_idb(sp, coord, out_saved);
}

int
wl_columnar_eval_test_tdd_restore(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_rel_t **saved)
{
    return tdd_restore_coord_idb(sp, coord, saved);
}

void
wl_columnar_eval_test_tdd_free_saved(const wl_plan_stratum_t *sp,
    col_rel_t **saved)
{
    tdd_free_saved_coord_idb(sp, saved);
}
#endif

/*
 * tdd_bdx_exchange_deltas:
 * Broadcast-Delta with Hash-Exchange Output (BDX) for Category C strata
 * (non-exchange-aligned IDB-IDB joins with <= 2 IDB body atoms).
 *
 * Algorithm per iteration:
 *   1. Union all worker deltas per relation into combined_delta
 *   2. Dedup combined_delta against coordinator's accumulated IDB
 *   3. Append combined_delta to coordinator IDB (monotonic growth)
 *   4. Truncate worker IDB to pre-subpass snapshot (remove pollution)
 *   5. Hash-partition combined_delta by EXCHANGE key -> append to workers
 *   6. Update snap after hash-exchange
 *   7. Insert hashes into worker dedup_sets
 *   8. Broadcast combined_delta as $d$ via col_shared zero-copy views
 *
 * snap[w * nrels + ri] holds the pre-subpass IDB nrows for truncation.
 * Ownership of ctxs[w].delta_rels[ri] entries transfers here.
 */
static int
tdd_bdx_exchange_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W,
    uint32_t *snap)
{
    uint32_t nrels = sp->relation_count;
    int rc = 0;

    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;
        const char *rel_name = sp->relations[ri].name;

        uint64_t prepare_t0 = now_ns();

        /* Remove stale $d$ from workers.  #1661: a refusal leaves the
         * worker's registry owner in place and the same-name session_add_rel
         * further down re-attempts col_rel_destroy_checked on it and returns
         * EBUSY.  This site has TWO empty-delta paths that register nothing
         * -- `total == 0` and the post-diff `combined->nrows == 0` -- and
         * neither installs an empty delta, so a refusal on either defers to
         * cohort teardown, where col_worker_session_destroy's relation-alias
         * pass refuses and the cohort is kept for retry
         * (tests/test_tdd_inline_workers.c). */
        for (uint32_t w = W; w-- > 0; )
            (void)session_remove_rel(&coord->tdd_workers[w], dname);

        /* Step 1: Union all worker deltas */
        uint32_t total = 0, ncols = 0;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            if (d && d->nrows > 0) {
                if (wl_columnar_eval_checked_row_add(total, d->nrows,
                    &total) != 0) {
                    tdd_destroy_delta_slots(ctxs, W, nrels);
                    return EOVERFLOW;
                }
                if (ncols == 0)
                    ncols = d->ncols;
            }
        }

        if (total == 0) {
            for (uint32_t w = 0; w < W; w++) {
                col_rel_destroy(ctxs[w].delta_rels[ri]);
                ctxs[w].delta_rels[ri] = NULL;
            }
            coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;
            continue;
        }

        col_rel_t *combined = col_rel_new_auto(dname, ncols);
        if (!combined)
            return ENOMEM;

        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            ctxs[w].delta_rels[ri] = NULL;
            if (d && d->nrows > 0)
                rc = col_rel_append_all(combined, d, NULL);
            col_rel_destroy(d);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
        }

        /* Step 2: Dedup combined within itself, then merge-diff against
         * coordinator's accumulated IDB to keep only truly new rows. */
        if (combined->nrows > 1)
            tdd_dedup_rel(combined);

        col_rel_t *coord_idb = session_find_rel(coord, rel_name);
        if (coord_idb && coord_idb->nrows > 0 && combined->nrows > 0) {
            rc = tdd_hashset_diff(combined, coord_idb);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
        }

        if (combined->nrows == 0) {
            col_rel_destroy(combined);
            coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;
            continue;
        }

        /* Step 3: Append combined_delta to coordinator IDB */
        if (coord_idb) {
            if (coord_idb->ncols == 0 && combined->ncols > 0) {
                rc = col_rel_set_schema(coord_idb, combined->ncols,
                        (const char *const *)combined->col_names);
                if (rc != 0) {
                    col_rel_destroy(combined);
                    return rc;
                }
            }
            rc = col_rel_append_all(coord_idb, combined, NULL);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
            tdd_dedup_set_insert_rel(coord_idb, combined);
            col_session_invalidate_arrangements(&coord->base, rel_name);
        }

        /* Step 4: Truncate worker IDB to pre-subpass snapshot */
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *widb = session_find_rel(
                &coord->tdd_workers[w], rel_name);
            if (widb){
                uint32_t old_nrows = widb->nrows;
                widb->nrows = snap[(size_t)w * nrels + ri];
                if (widb->nrows != old_nrows)
                    wl_columnar_relation_touch_view(widb);
            }
        }
        coord->tdd_exchange_coordinator_ns += now_ns() - prepare_t0;

        /* Step 5: Hash-partition combined_delta by EXCHANGE key,
         * append to correct worker's IDB */
        uint64_t scatter_t0 = now_ns();
        const uint32_t *key_cols = NULL;
        uint32_t key_count = 0;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta
                    = (const wl_plan_op_exchange_t *)
                    sp->relations[ri].ops[oi].opaque_data;
                if (meta && meta->key_col_count > 0) {
                    key_cols = meta->key_col_idxs;
                    key_count = meta->key_col_count;
                }
                break;
            }
        }
        uint32_t default_key[] = { 0 };
        if (!key_cols || key_count == 0) {
            key_cols = default_key;
            key_count = 1;
        }

        col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
        if (!parts) {
            col_rel_destroy(combined);
            return ENOMEM;
        }
        rc = col_rel_exchange_partition(combined, key_cols, key_count, W,
                parts);
        if (rc != 0) {
            for (uint32_t w = 0; w < W; w++)
                col_rel_destroy(parts[w]);
            free((void *)parts);
            col_rel_destroy(combined);
            return rc;
        }

        for (uint32_t w = 0; w < W; w++) {
            if (parts[w] && parts[w]->nrows > 0) {
                col_rel_t *widb = session_find_rel(
                    &coord->tdd_workers[w], rel_name);
                if (widb) {
                    rc = col_rel_append_all(widb, parts[w], NULL);
                    /* Step 7: Insert hashes of new rows into dedup_set */
                    if (rc == 0) {
                        for (uint32_t row = 0; row < parts[w]->nrows;
                            row++) {
                            uint64_t h = WL_COLUMNAR_EVAL_DEDUP_ROW_HASH(
                                parts[w], row);
                            WL_COLUMNAR_EVAL_DEDUP_SET_INSERT(widb, h);
                        }
                    }
                }
            }
            /* Step 6: Update snap after hash-exchange */
            col_rel_t *widb = session_find_rel(
                &coord->tdd_workers[w], rel_name);
            snap[(size_t)w * nrels + ri] = widb ? widb->nrows : 0;
            col_rel_destroy(parts[w]);
        }
        free((void *)parts);
        coord->tdd_exchange_scatter_ns += now_ns() - scatter_t0;

        /* Step 8: Broadcast combined_delta as $d$ via col_shared zero-copy */
        uint64_t broadcast_t0 = now_ns();
        rc = session_add_rel(&coord->tdd_workers[0], combined);
        if (rc != 0) {
            col_rel_destroy(combined);
            return rc;
        }
        /* combined is now owned by worker 0's session */
        for (uint32_t dst = 1; dst < W; dst++) {
            col_rel_t *view = col_rel_new_auto(dname, ncols);
            if (!view)
                return ENOMEM;
            rc = col_rel_install_shared_view(view, combined);
            bool shared = rc == 0;
            if (!shared) {
                rc = tdd_shared_view_deep_copy_fallback(NULL, view,
                        combined, rc);
                if (rc != 0) {
                    col_rel_destroy(view);
                    return rc;
                }
            }
            rc = session_add_rel(&coord->tdd_workers[dst], view);
            if (rc != 0) {
                col_rel_destroy(view);
                return rc;
            }
            if (shared) {
                rc = wl_columnar_session_adopt_shared_view(
                    &coord->tdd_workers[dst], view);
                if (rc != 0) {
                    /* #1661: rollback discard; see the note in
                     * tdd_refresh_global_read_relation. */
                    (void)session_remove_rel(&coord->tdd_workers[dst],
                        dname);
                    return rc;
                }
            }
        }
        coord->tdd_exchange_broadcast_ns += now_ns() - broadcast_t0;
    }

    return 0;
}

static int
tdd_owner_build_candidate(col_rel_t *target, const char *name,
    col_rel_t *const *inputs, uint32_t input_count, col_rel_t **out)
{
    col_rel_t *source = target;
    col_rel_t *candidate = NULL;
    int rc;
    if (!name || !out || (!inputs && input_count > 0))
        return EINVAL;
    *out = NULL;
    if (!source || source->ncols == 0) {
        for (uint32_t i = 0; i < input_count; i++) {
            if (inputs[i] && inputs[i]->nrows > 0) {
                source = inputs[i];
                break;
            }
        }
    }
    if (target && target->ncols == 0 && source && source->ncols > 0) {
        /* Preserve the previous exchange behavior for an existing empty,
         * schema-less IDB: the first accepted delta supplies its schema.
         * The target has no rows to carry over, so building from the input
         * also preserves its complete logical metadata. */
        candidate = col_rel_new_like(name, source);
        if (!candidate)
            return ENOMEM;
        if (target->timestamps && col_rel_enable_timestamps(candidate) != 0) {
            col_rel_destroy(candidate);
            return ENOMEM;
        }
    } else if (target) {
        rc = col_rel_deep_copy(target, &candidate, NULL);
        if (rc != 0)
            return rc;
        free(candidate->name);
        candidate->name = wl_strdup(name);
        if (!candidate->name) {
            col_rel_destroy(candidate);
            return ENOMEM;
        }
    } else if (source) {
        candidate = col_rel_new_like(name, source);
        if (!candidate)
            return ENOMEM;
    } else {
        candidate = col_rel_new_auto(name, 0);
        if (!candidate)
            return ENOMEM;
    }
    for (uint32_t i = 0; i < input_count; i++) {
        if (!inputs[i] || inputs[i]->nrows == 0)
            continue;
        if (!tdd_relation_schema_compatible(candidate, inputs[i])) {
            col_rel_destroy(candidate);
            return EINVAL;
        }
        rc = col_rel_append_all(candidate, inputs[i], NULL);
        if (rc != 0) {
            col_rel_destroy(candidate);
            return rc;
        }
    }
    *out = candidate;
    return 0;
}

static int
tdd_owner_exchange_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W,
    bool *out_any_accepted, uint32_t *out_accepted_rows)
{
    wl_columnar_eval_owner_publication_txn_t txn;
    uint32_t nrels = sp->relation_count;
    uint32_t accepted_total = 0;
    bool any_accepted = false;
    int rc = 0;
    col_rel_t *combined = NULL;
    col_rel_t **parts = NULL;
    col_rel_t **empty_inputs = NULL;
    wl_columnar_eval_owner_publication_init(&txn);
    if (out_any_accepted)
        *out_any_accepted = false;
    if (out_accepted_rows)
        *out_accepted_rows = 0;
    if (W == 0)
        return EINVAL;

    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;
        const char *rel_name = sp->relations[ri].name;
        col_rel_t *schema_source = NULL;
        uint32_t total = 0;
        uint32_t ncols = 0;
        uint32_t accepted_rows;

        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            if (!d || d->nrows == 0)
                continue;
            if (!schema_source)
                schema_source = d;
            else if (!tdd_relation_schema_compatible(schema_source, d)) {
                rc = EINVAL;
                goto fail;
            }
            ncols = d->ncols;
            if (wl_columnar_eval_checked_row_add(total, d->nrows, &total)
                != 0) {
                rc = EOVERFLOW;
                goto fail;
            }
        }
        if (!schema_source) {
            for (uint32_t w = 0; w < W; w++) {
                col_rel_t *old = session_find_rel(&coord->tdd_workers[w],
                        dname);
                if (old) {
                    schema_source = old;
                    ncols = old->ncols;
                    break;
                }
            }
        }
        combined = schema_source ? col_rel_new_like(dname, schema_source)
            : col_rel_new_auto(dname, ncols);
        if (!combined) {
            rc = ENOMEM;
            goto fail;
        }
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            if (d && d->nrows > 0) {
                rc = col_rel_append_all(combined, d, NULL);
                if (rc != 0)
                    goto fail;
            }
        }
        if (combined->nrows > 1) {
            tdd_dedup_rel(combined);
        }
        col_rel_t *coord_idb = session_find_rel(coord, rel_name);
        if (coord_idb && combined->nrows > 0) {
            if (coord_idb->ncols != 0
                && !tdd_relation_schema_compatible(coord_idb, combined)) {
                rc = EINVAL;
                goto fail;
            }
            rc = tdd_hashset_diff(combined, coord_idb);
            if (rc != 0)
                goto fail;
        }
        accepted_rows = combined->nrows;
        if (wl_columnar_eval_checked_row_add(accepted_total, accepted_rows,
            &accepted_total) != 0) {
            rc = EOVERFLOW;
            goto fail;
        }

        /* Build every private image before prepare.  Existing targets receive
        * full replacement candidates; new names remain private candidates. */
        if (accepted_rows > 0 && coord_idb) {
            col_rel_t *candidate = NULL;
            rc = tdd_owner_build_candidate(coord_idb, rel_name,
                    (col_rel_t *const[]){ combined }, 1, &candidate);
            if (rc != 0)
                goto fail;
            rc = wl_columnar_eval_owner_publication_add(&txn, coord, rel_name,
                    coord_idb, candidate);
            if (rc != 0) {
                col_rel_destroy(candidate);
                goto fail;
            }
        }

        uint32_t default_key[] = { 0 };
        const uint32_t *key_cols = default_key;
        uint32_t key_count = 1;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta =
                    (const wl_plan_op_exchange_t *)
                    sp->relations[ri].ops[oi].opaque_data;
                if (meta && meta->key_col_count > 0) {
                    key_cols = meta->key_col_idxs;
                    key_count = meta->key_col_count;
                }
                break;
            }
        }
        parts = calloc(W, sizeof(*parts));
        empty_inputs = calloc(1, sizeof(*empty_inputs));
        if (!parts || !empty_inputs) {
            rc = ENOMEM;
            goto fail;
        }
        if (accepted_rows > 0) {
            rc = col_rel_exchange_partition(combined, key_cols, key_count,
                    W, parts);
            if (rc != 0)
                goto fail;
        }
        for (uint32_t w = 0; w < W; w++) {
            wl_col_session_t *worker = &coord->tdd_workers[w];
            col_rel_t *widb = session_find_rel(worker, rel_name);
            col_rel_t *part = parts[w];
            if (widb && part && part->nrows > 0) {
                col_rel_t *candidate = NULL;
                rc = tdd_owner_build_candidate(widb, rel_name,
                        (col_rel_t *const[]){ part }, 1, &candidate);
                if (rc != 0)
                    goto fail;
                rc = wl_columnar_eval_owner_publication_add(&txn, worker,
                        rel_name, widb, candidate);
                if (rc != 0) {
                    col_rel_destroy(candidate);
                    goto fail;
                }
            }
            col_rel_t *old_delta = session_find_rel(worker, dname);
            /* Preserve the old empty-partition behavior: a worker with no
             * prior delta and no rows must not receive a synthetic empty
             * relation.  Existing deltas still get an empty replacement so
             * their registration is retired transactionally. */
            if (old_delta || (part && part->nrows > 0)) {
                col_rel_t *candidate = NULL;
                empty_inputs[0] = part;
                rc = tdd_owner_build_candidate(old_delta, dname,
                        empty_inputs, 1, &candidate);
                if (rc != 0)
                    goto fail;
                rc = wl_columnar_eval_owner_publication_add(&txn, worker,
                        dname, old_delta, candidate);
                if (rc != 0) {
                    col_rel_destroy(candidate);
                    goto fail;
                }
            }
        }
        free(empty_inputs);
        empty_inputs = NULL;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_destroy(parts[w]);
            parts[w] = NULL;
        }
        free(parts);
        parts = NULL;
        col_rel_destroy(combined);
        combined = NULL;
        any_accepted = any_accepted || accepted_rows > 0;
    }

    if (txn.count > 0) {
        rc = wl_columnar_eval_owner_publication_prepare(&txn);
        if (rc == 0)
            rc = wl_columnar_eval_owner_publication_register(&txn);
        if (rc == 0)
            rc = wl_columnar_eval_owner_publication_commit(&txn);
    }
    if (rc != 0)
        goto fail;
    for (uint32_t w = 0; w < W; w++)
        for (uint32_t ri = 0; ri < nrels; ri++)
            (void)wl_columnar_eval_tdd_owner_lifetime_dispose_slot(coord,
                &ctxs[w].delta_rels[ri]);
    if (out_any_accepted)
        *out_any_accepted = any_accepted;
    if (out_accepted_rows)
        *out_accepted_rows = accepted_total;
    /* Commit transfers published candidates and replacement state; release
     * the transaction container itself without touching published objects. */
    (void)wl_columnar_eval_owner_publication_discard(&txn);
    return 0;

fail:
    if (parts) {
        for (uint32_t w = 0; w < W; w++)
            col_rel_destroy(parts[w]);
    }
    free(parts);
    free(empty_inputs);
    col_rel_destroy(combined);
    (void)wl_columnar_eval_owner_publication_discard(&txn);
    for (uint32_t w = 0; w < W; w++)
        for (uint32_t ri = 0; ri < nrels; ri++)
            (void)wl_columnar_eval_tdd_owner_lifetime_dispose_slot(coord,
                &ctxs[w].delta_rels[ri]);
    return rc;
}

static int
tdd_global_read_exchange_deltas(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t W,
    bool *out_any_accepted, uint32_t *out_accepted_rows)
{
    uint32_t nrels = sp->relation_count;
    int rc = 0;
    if (out_any_accepted)
        *out_any_accepted = false;
    if (out_accepted_rows)
        *out_accepted_rows = 0;

    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;
        const char *rel_name = sp->relations[ri].name;

        uint32_t total = 0;
        const col_rel_t *prototype = NULL;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            if (d && d->nrows > 0) {
                if (wl_columnar_eval_checked_row_add(total, d->nrows,
                    &total) != 0) {
                    tdd_destroy_delta_slots(ctxs, W, nrels);
                    return EOVERFLOW;
                }
                if (!prototype)
                    prototype = d;
            }
        }
        if (total == 0) {
            for (uint32_t w = 0; w < W; w++) {
                col_rel_destroy(ctxs[w].delta_rels[ri]);
                ctxs[w].delta_rels[ri] = NULL;
            }
            continue;
        }

        rc = WL_COLUMNAR_EVAL_GLOBAL_BOUNDARY(coord, ri, 0);
        if (rc != 0)
            return rc;
        col_rel_t *combined = wl_columnar_relation_new_like_governed(dname,
                prototype, coord->memory_governor);
        if (!combined)
            return ENOMEM;

        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *d = ctxs[w].delta_rels[ri];
            ctxs[w].delta_rels[ri] = NULL;
            if (d && d->nrows > 0)
                rc = col_rel_append_all(combined, d, NULL);
            col_rel_destroy(d);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
        }

        rc = wl_columnar_eval_delta_consolidate(combined, coord);
        if (rc != 0) {
            col_rel_destroy(combined);
            return rc;
        }

        col_rel_t *coord_idb = session_find_rel(coord, rel_name);
        if (coord_idb && coord_idb->nrows > 0 && combined->nrows > 0) {
            rc = tdd_hashset_diff(combined, coord_idb);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
        }
        if (combined->nrows == 0) {
            col_rel_destroy(combined);
            continue;
        }

        if (out_any_accepted)
            *out_any_accepted = true;
        if (out_accepted_rows) {
            if (wl_columnar_eval_checked_row_add(*out_accepted_rows,
                combined->nrows, out_accepted_rows) != 0) {
                col_rel_destroy(combined);
                tdd_destroy_delta_slots(ctxs, W, nrels);
                return EOVERFLOW;
            }
        }

        if (coord_idb) {
            rc = WL_COLUMNAR_EVAL_GLOBAL_BOUNDARY(coord, ri, 1);
            if (rc == 0)
                rc = wl_columnar_eval_retire_worker_relations(sp, coord, W,
                        ri, 1, true);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
            rc = WL_COLUMNAR_EVAL_GLOBAL_BOUNDARY(coord, ri, 2);
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
            if (coord_idb->ncols == 0 && combined->ncols > 0) {
                col_rel_replacement_t replacement = { 0 };
                rc = col_rel_prepare_replacement(coord_idb, combined,
                        &replacement);
                if (rc == 0)
                    col_rel_commit_replacement_locked(coord_idb, &replacement);
                col_rel_discard_replacement(&replacement);
            } else {
                rc = col_rel_append_all(coord_idb, combined, NULL);
            }
            if (rc != 0) {
                col_rel_destroy(combined);
                return rc;
            }
            tdd_dedup_set_insert_rel(coord_idb, combined);
            col_session_invalidate_arrangements(&coord->base, rel_name);
            rc = WL_COLUMNAR_EVAL_GLOBAL_BOUNDARY(coord, ri, 3);
            if (rc == 0)
                rc = tdd_refresh_global_read_relation(sp, coord, ri, W);
            if (rc == 0)
                rc = WL_COLUMNAR_EVAL_GLOBAL_BOUNDARY(coord, ri, 4);
            if (rc != 0) {
                if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG")) {
                    fprintf(stderr,
                        "TDD global-read refresh error rel=%s cols=%u rows=%u rc=%d\n",
                        rel_name, coord_idb->ncols, coord_idb->nrows, rc);
                }
                col_rel_destroy(combined);
                return rc;
            }
        }

        const uint32_t *key_cols = NULL;
        uint32_t key_count = 0;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op == WL_PLAN_OP_EXCHANGE) {
                const wl_plan_op_exchange_t *meta =
                    (const wl_plan_op_exchange_t *)
                    sp->relations[ri].ops[oi].opaque_data;
                if (meta && meta->key_col_count > 0) {
                    key_cols = meta->key_col_idxs;
                    key_count = meta->key_col_count;
                }
                break;
            }
        }
        uint32_t default_key[] = { 0 };
        if (!key_cols || key_count == 0) {
            key_cols = default_key;
            key_count = 1;
        }
        bool key_valid = combined->ncols > 0;
        for (uint32_t ki = 0; key_valid && ki < key_count; ki++) {
            if (key_cols[ki] >= combined->ncols)
                key_valid = false;
        }
        if (!key_valid) {
            key_cols = default_key;
            key_count = 1;
        }

        col_rel_t **parts = (col_rel_t **)calloc(W, sizeof(col_rel_t *));
        if (!parts) {
            col_rel_destroy(combined);
            return ENOMEM;
        }
        uint32_t combined_cols = combined->ncols;
        uint32_t combined_rows = combined->nrows;
        uint32_t debug_key0 = key_count > 0 ? key_cols[0] : 0;
        rc = col_rel_exchange_partition(combined, key_cols, key_count, W,
                parts);
        col_rel_destroy(combined);
        if (rc != 0) {
            if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG")) {
                fprintf(stderr,
                    "TDD global-read partition error rel=%s cols=%u rows=%u "
                    "key_count=%u key0=%u rc=%d\n",
                    rel_name, combined_cols, combined_rows, key_count,
                    debug_key0, rc);
            }
            for (uint32_t w = 0; w < W; w++)
                col_rel_destroy(parts[w]);
            free((void *)parts);
            return rc;
        }

        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *part = parts[w];
            parts[w] = NULL;
            if (!part || part->nrows == 0) {
                col_rel_destroy(part);
                continue;
            }
            free(part->name);
            part->name = wl_strdup(dname);
            if (!part->name) {
                col_rel_destroy(part);
                rc = ENOMEM;
                break;
            }
            rc = session_add_rel(&coord->tdd_workers[w], part);
            if (rc != 0) {
                col_rel_destroy(part);
                break;
            }
        }
        for (uint32_t w = 0; w < W; w++)
            col_rel_destroy(parts[w]);
        free((void *)parts);
        if (rc != 0)
            return rc;
    }

    return 0;
}

#ifdef WL_TEST_BDX_SEED
int
wl_columnar_eval_test_global_exchange(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, col_eval_tdd_worker_ctx_t *ctxs, uint32_t workers)
{
    int rc = tdd_global_read_exchange_deltas(sp, coord, ctxs, workers,
            NULL, NULL);
    if (rc != 0)
        tdd_destroy_delta_slots(ctxs, workers, sp->relation_count);
    return rc;
}
#endif

/*
 * col_eval_stratum_tdd_recursive:
 * Coordinator-driven semi-naive fixed-point for recursive strata.
 *
 * Pipeline per sub-pass:
 *   DISPATCH (W workers via workqueue) → BARRIER → CONVERGENCE CHECK
 *   → EXCHANGE (hash/broadcast/BDX depending on mode) → next sub-pass
 *
 * After convergence, merges worker IDB into coordinator and deduplicates.
 * Broadcast exchange may produce the same derived tuple on multiple workers
 * (when multiple equal-length paths lead to the same conclusion); the final
 * sort+dedup step removes these.
 */
static int
col_eval_stratum_tdd_recursive(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t stratum_idx)
{
    /* A retained record is cleanup-only. Once it is cleared, this invocation
     * is a fresh evaluation with its own plan/context. */
    if (coord && coord->tdd_owner_lifetime) {
        int quiesce_rc
            = wl_columnar_eval_tdd_owner_lifetime_quiesce(coord);
        if (quiesce_rc != 0)
            return quiesce_rc;
        int cleanup_rc = wl_columnar_eval_tdd_owner_lifetime_retry(coord);
        if (cleanup_rc != 0)
            return cleanup_rc;
    }
    uint32_t W = coord->num_workers;
    uint32_t nrels = sp->relation_count;
    size_t delta_rel_bytes = 0;
    if (wl_columnar_eval_checked_size_mul(nrels, sizeof(col_rel_t *),
        &delta_rel_bytes) != 0)
        return EOVERFLOW;
    int rc = 0;
    uint64_t tdd_total_t0 = now_ns();

    uint32_t delta_queue_capacity = 0;
    if (wl_columnar_eval_delta_queue_capacity(nrels,
        &delta_queue_capacity) != 0) {
        coord->tdd_total_ns += now_ns() - tdd_total_t0;
        return ENOMEM;
    }

    /* Pre-register empty IDB relations on coordinator
     * (eval_serial.c:276-289) */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (session_find_rel(coord, sp->relations[ri].name))
            continue;
        col_rel_t *empty = NULL;
        int alloc_rc = col_rel_alloc(&empty, sp->relations[ri].name);
        if (alloc_rc != 0) {
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
            return ENOMEM;
        }
        alloc_rc = session_add_rel(coord, empty);
        if (alloc_rc != 0) {
            col_rel_destroy(empty);
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
            return alloc_rc;
        }
    }

    /* Issue #350: On incremental steps (new EDB inserted), clear pre-existing
     * IDB rows so workers recompute from scratch.  Without this, stale IDB
     * partitioned by col0 prevents cross-partition recursive joins
     * (e.g. tc(1,3) on worker 1 cannot join edge(3,4) on worker 3).
     * Also reset stratum frontier so should_skip_iteration does not skip
     * iterations beyond the previous step's convergence point.
     * When no new EDB was inserted, skip clearing to preserve frontier skip.
     * Issue #372: Skip clearing on first snapshot (has_evaluated == false):
     * IDB relations may hold EDB seeds (e.g. r(1,2) for r(x,z):-r(x,y),r(y,z))
     * that must survive into the first evaluation pass. */
    if (coord->last_inserted_relation != NULL
        && coord->has_evaluated) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(coord, sp->relations[ri].name);
            if (r && r->nrows > 0) {
                rc = tdd_reset_coord_relation(r);
                if (rc != 0) {
                    coord->tdd_total_ns += now_ns() - tdd_total_t0;
                    return rc;
                }
                col_session_invalidate_arrangements(&coord->base,
                    sp->relations[ri].name);
            }
        }
        coord->frontier_ops->reset_stratum_frontier(coord, stratum_idx,
            coord->outer_epoch);
    }

    /* Issue #361, #372, #390: Determine init strategy:
     *
     *   self_join_mode (asymmetric partition-replicate): IDB self-join strata
     *   where the join key == EXCHANGE partition key on both sides (e.g. CSPA
     *   valueAlias: vA(x,y):-vF(z,x),vF(z,y) joins on col0 which is the
     *   EXCHANGE key).  Workers hold 1/W of the IDB; delta is broadcast.
     *   Each join is fully local because the join key equals the partition key.
     *
     *   bdx_mode (broadcast-delta with hash-exchange): IDB self-join strata
     *   where the join key != EXCHANGE key but rules have at most 2 IDB body
     *   atoms.  Workers hold 1/W of IDB (hybrid init), combined delta is
     *   deduped against coordinator IDB, hash-exchanged to workers, and
     *   broadcast as $d$.
     *
     *   replicate_mode (full replication): used when:
     *     - No new EDB inserted (frontier-skip path), OR
     *     - Stratum is not exchange-aligned AND has >2 IDB body atoms (BDX unsafe).
     *
     * Issue #388: Optional W=1 fallback for replicate mode only. */
    bool has_idb_self_join = tdd_stratum_has_idb_self_join(sp);
    bool self_join_mode = tdd_stratum_idb_self_join_exchange_aligned(sp, coord);
    bool owner_exchange_mode = !has_idb_self_join
        && stratum_max_idb_body_atoms(sp) <= 1
        && tdd_stratum_single_idb_join_keys_exchange_aligned(sp);
    bool bdx_mode = has_idb_self_join && !self_join_mode;
    const char *global_read_env = getenv("WIRELOG_TDD_GLOBAL_READ");
    bool global_read_mode = !(global_read_env && global_read_env[0] == '0'
        && global_read_env[1] == '\0')
        && !owner_exchange_mode && !self_join_mode && !bdx_mode
        && tdd_stratum_global_read_candidate(sp);
    bool replicate_mode = !owner_exchange_mode
        && !global_read_mode
        && ((coord->last_inserted_relation == NULL)
        || (!self_join_mode && stratum_max_idb_body_atoms(sp) > 2));
    bdx_mode = bdx_mode && !replicate_mode;
    if (replicate_mode) {
        const char *env = getenv("WIRELOG_TDD_REPLICATE_W1");
        if (env && env[0] == '1')
            W = 1;
    }
    W = tdd_choose_active_workers(sp, coord, W, replicate_mode);
    if (global_read_mode) {
        uint32_t cap = tdd_global_read_worker_cap();
        if (W > cap)
            W = cap;
    }
    if (coord->tdd_audit.enabled) {
        coord->tdd_audit.strategy = owner_exchange_mode ? "owner"
            : global_read_mode ? "global_read"
            : replicate_mode ? "replicate"
            : bdx_mode ? "bdx" : "aligned";
        coord->tdd_audit.selected_workers = W;
    }
    if (W <= 1) {
        tdd_record_active_workers(coord, 1);
        if (coord->tdd_decision_tracking_active) {
            if (coord->tdd_executed_strata > 0)
                coord->tdd_executed_strata--;
            coord->tdd_fallback_strata++;
            coord->tdd_last_fallback_reason =
                WL_COLUMNAR_INTERNAL_TDD_FALLBACK_ADAPTIVE_WORKERS;
            coord->tdd_fallback_reason_counts[
                WL_COLUMNAR_INTERNAL_TDD_FALLBACK_ADAPTIVE_WORKERS]++;
        }
        uint32_t saved_total_iterations = coord->total_iterations;
        int seq_rc = col_eval_stratum(sp, coord, stratum_idx);
        if (seq_rc == 0 && saved_total_iterations > 0)
            coord->total_iterations += saved_total_iterations;
        return seq_rc;
    }

    col_rel_t **owner_fallback_saved = NULL;
    col_rel_t **global_read_saved = NULL;
    bool owner_adaptive_fallback = false;
    if (owner_exchange_mode) {
        int save_rc = tdd_save_coord_idb(sp, coord, &owner_fallback_saved);
        if (save_rc != 0) {
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
            return save_rc;
        }
    } else if (global_read_mode) {
        int save_rc = tdd_save_coord_idb(sp, coord, &global_read_saved);
        if (save_rc != 0) {
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
            return save_rc;
        }
    }

    if (global_read_mode)
        rc = tdd_init_workers_global_read(coord, W);
    else if (replicate_mode)
        rc = tdd_replicate_workers(coord, W);
    else
        rc = tdd_init_workers_hybrid(sp, coord, true, W);
    if (rc != 0) {
        tdd_free_saved_coord_idb(sp, owner_fallback_saved);
        tdd_free_saved_coord_idb(sp, global_read_saved);
        coord->tdd_total_ns += now_ns() - tdd_total_t0;
        return rc;
    }

    if (owner_exchange_mode) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(coord, sp->relations[ri].name);
            if (r && !r->dedup_slots) {
                rc = WL_COLUMNAR_EVAL_DEDUP_SET_INIT_FROM_REL(r);
                if (rc != 0) {
                    rc = tdd_cleanup_preserve_error(coord, rc);
                    tdd_free_saved_coord_idb(sp, owner_fallback_saved);
                    tdd_free_saved_coord_idb(sp, global_read_saved);
                    coord->tdd_total_ns += now_ns() - tdd_total_t0;
                    return rc;
                }
            }
        }
    }

    /* Pre-register empty IDB on each worker */
    rc = tdd_preregister_idb_on_workers(sp, coord);
    if (rc != 0) {
        rc = tdd_cleanup_preserve_error(coord, rc);
        tdd_free_saved_coord_idb(sp, owner_fallback_saved);
        tdd_free_saved_coord_idb(sp, global_read_saved);
        coord->tdd_total_ns += now_ns() - tdd_total_t0;
        return rc;
    }

    /* Sort pre-existing IDB data on workers (eval_serial.c:309-314).
    * The serial twin of this loop propagates a refused sort for the same
    * reason: it establishes the sorted prefix that
    * col_op_consolidate_incremental_delta requires, and continuing on an
    * unsorted prefix makes the two-pointer merge miss duplicates. */
    for (uint32_t w = 0; w < W; w++) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(&coord->tdd_workers[w],
                    sp->relations[ri].name);
            if (r && r->nrows > 1) {
                int sort_rc = col_rel_radix_sort_int64(r);
                if (sort_rc != 0) {
                    sort_rc = tdd_cleanup_preserve_error(coord, sort_rc);
                    tdd_free_saved_coord_idb(sp, owner_fallback_saved);
                    tdd_free_saved_coord_idb(sp, global_read_saved);
                    coord->tdd_total_ns += now_ns() - tdd_total_t0;
                    return sort_rc;
                }
            }
        }
    }

    /* Issue #361: Pre-install empty $d$ delta relations on each worker.
     * Persistent across iterations — worker clears (nrows=0) instead of
     * session_remove_rel, and broadcast refills instead of create+add.
     * Eliminates per-iteration alloc/free/session-ops (14k iters for CRDT). */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        const char *dname = sp->relations[ri].delta_name;
        col_rel_t *coord_rel = session_find_rel(coord,
                sp->relations[ri].name);
        uint32_t ncols_ri = coord_rel ? coord_rel->ncols : 0;
        for (uint32_t w = 0; w < W; w++) {
            col_rel_t *slot = col_rel_new_auto(dname, ncols_ri);
            if (!slot) {
                rc = tdd_cleanup_preserve_error(coord, ENOMEM);
                tdd_free_saved_coord_idb(sp, owner_fallback_saved);
                tdd_free_saved_coord_idb(sp, global_read_saved);
                coord->tdd_total_ns += now_ns() - tdd_total_t0;
                return rc;
            }
            rc = session_add_rel(&coord->tdd_workers[w], slot);
            if (rc != 0) {
                col_rel_destroy(slot);
                rc = tdd_cleanup_preserve_error(coord, rc);
                tdd_free_saved_coord_idb(sp, owner_fallback_saved);
                tdd_free_saved_coord_idb(sp, global_read_saved);
                coord->tdd_total_ns += now_ns() - tdd_total_t0;
                return rc;
            }
        }
    }

    /* Phase 4: Frontier Initialization (eval_serial.c:321)
     * Initialize per-stratum frontier tracking for convergence detection.
     * Frontier records the iteration at which each stratum converged
     * (fixed-point reached with no new tuples).
     */
    coord->frontier_ops->init_stratum(coord, stratum_idx);

    /* Compute rule_id_base for per-rule frontier recording
     * Each rule (IDB relation) gets a unique frontier slot indexed by
     * rule_id_base + relation_index within stratum.
     */
    uint32_t rule_id_base = 0;
    if (coord->plan) {
        for (uint32_t si = 0;
            si < stratum_idx && si < coord->plan->stratum_count; si++) {
            rule_id_base += coord->plan->strata[si].relation_count;
        }
    }
    if (rule_id_base >= MAX_RULES)
        rule_id_base = MAX_RULES;

    uint32_t final_eff_iter = 0;
    bool saved_diff = coord->diff_operators_active;

    /* Phase 4: Iteration Loop Control
     * Semi-naive fixed-point computation with two nested loops:
     *  - outer loop: tracks global convergence across all sub-passes
     *  - inner sub loop: one EVAL_STRIDE sub-iteration per outer iteration
     *
     * Terminates when:
     *  1. Fixed-point reached: no worker produced new tuples in iteration
     *  2. Coordinator-level frontier skip: iteration > stratum frontier
     *
     * Each iteration:
     *  - DISPATCH W workers to evaluate sub-pass
     *  - BARRIER to wait for all workers
     *  - CONVERGENCE CHECK: if all workers have empty delta → fixed point
     *  - EXCHANGE: broadcast/hash-partition deltas to next iteration
     */

    /* Issue #361: Pre-allocate worker contexts and delta_rels arrays once.
     * Reuse across iterations to avoid calloc/free overhead per sub-pass
     * (~14k iterations for CRDT). */
    /* Keep this initialized before any goto done path. */
    uint32_t *bdx_snap = NULL;
    bool owner_slots_embedded = false;
    bool owner_dispatch_quiesced = true;
    col_eval_tdd_worker_ctx_t *ctxs
        = (col_eval_tdd_worker_ctx_t *)calloc(
            W, sizeof(col_eval_tdd_worker_ctx_t));
    if (!ctxs) {
        rc = ENOMEM;
        goto done;
    }
    if (owner_exchange_mode) {
        rc = wl_columnar_eval_tdd_owner_lifetime_create(coord, ctxs, W, nrels,
                delta_queue_capacity);
        if (rc != 0)
            goto done;
        owner_slots_embedded = true;
    } else {
        for (uint32_t w = 0; w < W; w++) {
            ctxs[w].delta_rels = (col_rel_t **)calloc(
                nrels, sizeof(col_rel_t *));
            if (!ctxs[w].delta_rels) {
                for (uint32_t j = 0; j < w; j++)
                    free((void *)ctxs[j].delta_rels);
                free(ctxs);
                ctxs = NULL;
                rc = ENOMEM;
                goto done;
            }
        }
    }

    /* Issue #410: Create MPSC delta queue for dual-write transport.
     * Capacity = W × nrels × 2 (2x headroom; at most W×nrels per sub-pass).
     * Failure is non-fatal: enqueue is skipped when delta_queue is NULL.
     * An unrepresentable nrels*2 request is rejected before any stratum
     * state allocation, using the evaluator's ENOMEM failure signal. */
    coord->delta_queue = wl_mpsc_queue_create_with_destructor(
        W, delta_queue_capacity, tdd_destroy_delta_payload);
    if (coord->tdd_owner_lifetime)
        coord->tdd_owner_lifetime->queue = coord->delta_queue;
    /* Issue #1380: ring storage is fixed for the stratum; charge once. */
    coord->mem_channel_ring_bytes
        = wl_mpsc_queue_footprint_bytes(coord->delta_queue);
    if (coord->tdd_owner_lifetime)
        coord->tdd_owner_lifetime->queue_ring_bytes
            = coord->mem_channel_ring_bytes;
    wl_mem_ledger_alloc(&coord->mem_ledger, WL_MEM_SUBSYS_CHANNEL,
        coord->mem_channel_ring_bytes);

    /* Issue #390: BDX snap array — pre-subpass IDB sizes per worker/relation.
     * Used to truncate worker IDB back to clean partition state after each
     * sub-pass (removes cross-partition pollution from join output). */
    if (bdx_mode) {
        size_t bdx_count = 0;
        if (wl_columnar_eval_tdd_matrix_size(W, nrels,
            sizeof(uint32_t), &bdx_count) != 0) {
            rc = EOVERFLOW;
            goto done;
        }
        bdx_snap = (uint32_t *)calloc(bdx_count, sizeof(uint32_t));
        if (!bdx_snap) {
            rc = ENOMEM;
            goto done;
        }
        /* Initialize snap from current worker IDB sizes */
        for (uint32_t w = 0; w < W; w++) {
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *widb = session_find_rel(
                    &coord->tdd_workers[w], sp->relations[ri].name);
                bdx_snap[(size_t)w * nrels + ri] = widb ? widb->nrows : 0;
            }
        }
        /* Initialize coordinator IDB from worker partitions for dedup.
        * Coordinator needs a sorted copy of all IDB for merge-diff. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *cidb = session_find_rel(coord,
                    sp->relations[ri].name);
            col_rel_t **worker_idbs = NULL;
            if (cidb) {
                size_t worker_idb_bytes = 0;
                if (wl_columnar_eval_checked_size_mul(W, sizeof(*worker_idbs),
                    &worker_idb_bytes) != 0) {
                    free(bdx_snap);
                    bdx_snap = NULL;
                    rc = EOVERFLOW;
                    goto done;
                }
                worker_idbs = (col_rel_t **)calloc(1,
                        worker_idb_bytes);
                if (!worker_idbs) {
                    free(bdx_snap);
                    bdx_snap = NULL;
                    rc = ENOMEM;
                    goto done;
                }
                for (uint32_t w = 0; w < W; w++)
                    worker_idbs[w] = session_find_rel(
                        &coord->tdd_workers[w], sp->relations[ri].name);
                rc = tdd_seed_bdx_coordinator_idb(cidb, worker_idbs, W);
                /* col_rel_t ** -> void * is a multilevel conversion;
                 * cast explicitly for #1100's
                 * bugprone-multi-level-implicit-pointer-conversion. */
                free((void *)worker_idbs);
                if (rc != 0) {
                    free(bdx_snap);
                    bdx_snap = NULL;
                    goto done;
                }
            }
        }
        /* Pre-seed $d$ with full initial IDB on all workers.
         * BDX forces diff from eff_iter 0, so K_FUSION uses the broadcast
         * delta (complete IDB) rather than the local partition self-join
         * which would be incomplete for non-aligned join keys. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *cidb = session_find_rel(coord,
                    sp->relations[ri].name);
            if (!cidb || cidb->nrows == 0)
                continue;
            const char *dname = sp->relations[ri].delta_name;
            uint32_t ncols = cidb->ncols;
            /* Install full IDB as $d$ on worker 0 */
            col_rel_t *d0 = session_find_rel(&coord->tdd_workers[0], dname);
            if (d0 && d0->ncols == ncols) {
                d0->nrows = 0;
                wl_columnar_relation_touch_view(d0);
                rc = col_rel_append_all(d0, cidb, NULL);
            } else {
                d0 = col_rel_new_auto(dname, ncols);
                if (!d0) {
                    rc = ENOMEM; break;
                }
                rc = col_rel_append_all(d0, cidb, NULL);
                if (rc == 0)
                    rc = session_add_rel(&coord->tdd_workers[0], d0);
                else
                    col_rel_destroy(d0);
            }
            if (rc != 0)
                break;
            /* Shared views on workers 1..W-1 */
            d0 = session_find_rel(&coord->tdd_workers[0], dname);
            for (uint32_t w = 1; w < W && rc == 0; w++) {
                col_rel_t *dw = session_find_rel(
                    &coord->tdd_workers[w], dname);
                if (dw && dw->ncols == ncols) {
                    rc = wl_columnar_session_install_shared_view(
                        &coord->tdd_workers[w], dw, d0);
                    if (rc != 0) {
                        rc = tdd_shared_view_deep_copy_fallback(
                            &coord->tdd_workers[w], dw, d0, rc);
                    }
                } else {
                    dw = col_rel_new_auto(dname, ncols);
                    if (!dw) {
                        rc = ENOMEM; break;
                    }
                    int vrc = col_rel_install_shared_view(dw, d0);
                    bool shared = vrc == 0;
                    if (vrc != 0)
                        rc = tdd_shared_view_deep_copy_fallback(NULL, dw,
                                d0, vrc);
                    if (rc == 0)
                        rc = session_add_rel(&coord->tdd_workers[w], dw);
                    if (rc != 0) {
                        col_rel_destroy(dw);
                    } else if (shared) {
                        rc = wl_columnar_session_adopt_shared_view(
                            &coord->tdd_workers[w], dw);
                        if (rc != 0) {
                            /* #1661: rollback discard; see the note in
                             * tdd_refresh_global_read_relation. */
                            (void)session_remove_rel(
                                &coord->tdd_workers[w], dname);
                        }
                    }
                }
            }
            if (rc != 0)
                break;
        }
        if (rc != 0) {
            free(bdx_snap);
            bdx_snap = NULL;
            goto done;
        }
    }

    if (global_read_mode) {
        rc = tdd_seed_global_read_initial_deltas(sp, coord, W);
        if (rc != 0)
            goto done;
    }

    for (uint32_t iter = 0; iter < MAX_ITERATIONS; iter++) {
        bool outer_any_new = false;
        bool converged = false;
        bool stride_all_skipped = true;
        bool outer_continue_next = false;

        for (uint32_t sub = 0; sub < EVAL_STRIDE; sub++) {
            uint32_t eff_iter = iter * EVAL_STRIDE + sub;

            /* Phase 4: Frontier Skip Optimization (eval_serial.c:405-408) */
            if (coord->frontier_ops->should_skip_iteration(coord,
                stratum_idx, eff_iter)) {
                continue;
            }
            stride_all_skipped = false;

            /* Reset worker contexts for this sub-pass (reuse allocation) */
            for (uint32_t w = 0; w < W; w++) {
                memset((void *)ctxs[w].delta_rels, 0,
                    delta_rel_bytes);
                ctxs[w].sp = sp;
                ctxs[w].worker_sess = &coord->tdd_workers[w];
                ctxs[w].stratum_idx = stratum_idx;
                ctxs[w].eff_iter = eff_iter;
                ctxs[w].any_new = false;
                ctxs[w].all_empty_delta = false;
                ctxs[w].force_diff = bdx_mode || global_read_mode;
                ctxs[w].outbound_only = owner_exchange_mode
                    || global_read_mode;
                ctxs[w].runtime_ns = 0;
                ctxs[w].rc = 0;
            }
#if defined(WL_TEST_BDX_SEED) || defined(WL_SESSION_TEST_HOOKS)
            if (wl_columnar_eval_test_subpass_boundary)
                wl_columnar_eval_test_subpass_boundary(coord, eff_iter, true);
#endif
            /* DISPATCH */
            bool submit_ok = true;
            uint64_t dispatch_t0 = now_ns();
            for (uint32_t w = 0; w < W; w++) {
                if (WL_COLUMNAR_EVAL_SUBMIT(coord->wq, tdd_worker_subpass_fn,
                    &ctxs[w]) != 0) {
                    int drain_rc = wl_workqueue_drain(coord->wq);
                    owner_dispatch_quiesced = drain_rc == 0;
                    if (owner_dispatch_quiesced && coord->tdd_owner_lifetime)
                        coord->tdd_owner_lifetime->dispatch_active = false;
                    submit_ok = false;
                    break;
                }
                owner_dispatch_quiesced = false;
                if (coord->tdd_owner_lifetime)
                    coord->tdd_owner_lifetime->dispatch_active = true;
                if (coord->tdd_audit.enabled)
                    coord->tdd_audit.submitted_tasks++;
            }
            uint64_t submit_ns = now_ns() - dispatch_t0;
            coord->tdd_submit_loop_ns += submit_ns;

            if (!submit_ok) {
                if (!coord->tdd_owner_lifetime) {
                    wl_columnar_eval_tdd_queue_discard_delta_queue_ledger(
                        coord->delta_queue, &coord->mem_ledger);
                    for (uint32_t w = 0; w < W; w++)
                        for (uint32_t ri = 0; ri < nrels; ri++)
                            col_rel_destroy(ctxs[w].delta_rels[ri]);
                }
                rc = ENOMEM;
                goto done;
            }

            /* BARRIER */
            uint64_t wait_t0 = now_ns();
            int wait_rc = wl_workqueue_wait_all(coord->wq);
            owner_dispatch_quiesced = wait_rc == 0;
            if (coord->tdd_owner_lifetime)
                coord->tdd_owner_lifetime->dispatch_active
                    = !owner_dispatch_quiesced;
            if (wait_rc != 0) {
                int drain_rc = wl_workqueue_drain(coord->wq);
                owner_dispatch_quiesced = drain_rc == 0;
                if (owner_dispatch_quiesced && coord->tdd_owner_lifetime)
                    coord->tdd_owner_lifetime->dispatch_active = false;
                rc = wait_rc;
                goto done;
            }
            if (coord->tdd_audit.enabled)
                coord->tdd_audit.completed_rounds++;
            uint64_t wait_ns = now_ns() - wait_t0;
            coord->tdd_wait_barrier_ns += wait_ns;
            coord->tdd_dispatch_wait_ns += submit_ns + wait_ns;
            uint64_t worker_sum_ns = 0;
            uint64_t worker_max_ns = 0;
            for (uint32_t w = 0; w < W; w++) {
                worker_sum_ns += ctxs[w].runtime_ns;
                if (ctxs[w].runtime_ns > worker_max_ns)
                    worker_max_ns = ctxs[w].runtime_ns;
                if (coord->tdd_audit.enabled) {
                    uint64_t runtime = ctxs[w].runtime_ns;
                    if ((coord->tdd_audit.completed_rounds == 1 && w == 0)
                        || runtime < coord->tdd_audit.worker_min_ns)
                        coord->tdd_audit.worker_min_ns = runtime;
                    if (runtime > coord->tdd_audit.worker_max_ns)
                        coord->tdd_audit.worker_max_ns = runtime;
                    coord->tdd_audit.worker_sum_ns += runtime;
                }
            }
            coord->tdd_worker_sum_ns += worker_sum_ns;
            coord->tdd_worker_max_ns += worker_max_ns;
            if (wait_ns > worker_max_ns)
                coord->tdd_idle_estimate_ns += wait_ns - worker_max_ns;

#if defined(WL_TEST_BDX_SEED) || defined(WL_SESSION_TEST_HOOKS)
            if (wl_columnar_eval_test_subpass_boundary)
                wl_columnar_eval_test_subpass_boundary(coord, eff_iter, false);
#endif
            /* Collect first worker error */
            for (uint32_t w = 0; w < W; w++) {
                record_worker_expr_status(coord, ctxs[w].worker_sess,
                    ctxs[w].rc);
                if (ctxs[w].rc != 0 && rc == 0)
                    rc = ctxs[w].rc;
                wl_col_session_t *worker = ctxs[w].worker_sess;
                if (rc == 0 &&
                    (worker->cleanup_active || worker->cleanup_pending
                    || worker->delta_rollback))
                    rc = EBUSY;
            }

            if (rc != 0) {
                if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG"))
                    fprintf(stderr,
                        "TDD worker error stratum=%u iter=%u rc=%d\n",
                        stratum_idx, eff_iter, rc);
                if (!coord->tdd_owner_lifetime) {
                    wl_columnar_eval_tdd_queue_discard_delta_queue_ledger(
                        coord->delta_queue, &coord->mem_ledger);
                    for (uint32_t w = 0; w < W; w++)
                        for (uint32_t ri = 0; ri < nrels; ri++)
                            col_rel_destroy(ctxs[w].delta_rels[ri]);
                }
                goto done;
            }

            /* Issue #410, Commit 4: Drain MPSC queue and reconstruct
             * ctxs[w].delta_rels[ri] via adapter.  Workers dual-write to both
             * ctx and queue; shadow assert verifies agreement (debug only). */
            if (coord->delta_queue) {
                uint64_t queue_t0 = now_ns();
                if (coord->tdd_owner_lifetime) {
                    /* The owner record is the allocation-free queue
                     * destination. It retains every rejected/duplicate slot
                     * message instead of invoking the legacy destructor. */
                    rc = wl_columnar_eval_tdd_owner_lifetime_capture_queue(
                        coord, coord->tdd_owner_lifetime);
                    coord->tdd_queue_drain_ns += now_ns() - queue_t0;
                    if (rc != 0)
                        goto done;
                } else {
                    size_t matrix_count = 0;
                    if (wl_columnar_eval_tdd_matrix_size(W, nrels,
                        sizeof(wl_delta_msg_t), &matrix_count) != 0) {
                        wl_columnar_eval_tdd_queue_discard_delta_queue_ledger(
                            coord->delta_queue, &coord->mem_ledger);
                        rc = EOVERFLOW;
                        goto done;
                    }
                    uint32_t max_msgs = (uint32_t)matrix_count;
                    wl_delta_msg_t *msgs = (wl_delta_msg_t *)calloc(
                        max_msgs > 0 ? max_msgs : 1u, sizeof(wl_delta_msg_t));
                    if (!msgs) {
                        /* Queue-only workers have not written ctxs yet.  Drain
                         * and destroy their queued deltas before aborting; do
                         * not continue with a silently empty exchange. */
                        wl_columnar_eval_tdd_queue_discard_delta_queue_ledger(
                            coord->delta_queue, &coord->mem_ledger);
                        rc = ENOMEM;
                        coord->tdd_queue_drain_ns += now_ns() - queue_t0;
                        goto done;
                    }
                    uint32_t msg_count = wl_mpsc_dequeue_all(
                        coord->delta_queue, msgs, max_msgs);
                    /* Issue #1380: payloads leave the channel here; whatever
                     * the matrix keeps becomes a coordinator-side delta. */
                    for (uint32_t mi = 0; mi < msg_count && mi < max_msgs;
                        mi++) {
                        if (msgs[mi].delta)
                            wl_mem_ledger_free(&coord->mem_ledger,
                                WL_MEM_SUBSYS_CHANNEL,
                                col_rel_transport_bytes(
                                    (const col_rel_t *)msgs[mi].delta));
                    }

                    /* Clear and reconstruct from queue messages. */
                    for (uint32_t w = 0; w < W; w++)
                        memset((void *)ctxs[w].delta_rels, 0,
                            delta_rel_bytes);
                    wl_columnar_eval_tdd_queue_reconstruct_delta_matrix(
                        ctxs, msgs, msg_count, W, nrels);
                    free(msgs);
                    coord->tdd_queue_drain_ns += now_ns() - queue_t0;
                }
            }

            /* Queue mode transfers ownership out of ctxs in the worker.
             * Count only after its delta matrix has been reconstructed. */
            if (coord->tdd_audit.enabled) {
                for (uint32_t w = 0; w < W; w++) {
                    for (uint32_t ri = 0; ri < nrels; ri++) {
                        const col_rel_t *delta = ctxs[w].delta_rels[ri];
                        if (delta)
                            coord->tdd_audit.worker_delta_rows += delta->nrows;
                    }
                }
            }
            uint64_t convergence_t0 = now_ns();

            /* Stratum-level early exit: all workers have all_empty_delta */
            bool all_workers_empty = true;
            for (uint32_t w = 0; w < W; w++) {
                if (!ctxs[w].all_empty_delta) {
                    all_workers_empty = false;
                    break;
                }
            }
            if (all_workers_empty) {
                coord->tdd_convergence_ns += now_ns() - convergence_t0;
                if (coord->tdd_owner_lifetime) {
                    rc = wl_columnar_eval_tdd_owner_lifetime_dispose_all(
                        coord->tdd_owner_lifetime);
                    if (rc != 0)
                        goto done;
                } else {
                    for (uint32_t w = 0; w < W; w++)
                        for (uint32_t ri = 0; ri < nrels; ri++)
                            col_rel_destroy(ctxs[w].delta_rels[ri]);
                }
                outer_continue_next = true;
                break;
            }

            /* Phase 4: Global Convergence Detection
             * CONVERGENCE: fixed point if no worker produced new tuples.
             *
             * Each worker tracks any_new = true if its partition produced
             * at least one new tuple during this sub-pass. Global convergence
             * occurs when ALL workers have any_new = false.
             *
             * Correctness: Under distributed execution with hash-partitioned
             * exchange, each worker independently computes new tuples from
             * its partition. No tuple can be created without appearing in
             * at least one worker's delta. Therefore, checking all workers'
             * any_new flags is both necessary and sufficient for fixed-point
             * detection.
             */
            if (!owner_exchange_mode && !global_read_mode
                && tdd_check_convergence(ctxs, W)) {
                coord->tdd_convergence_ns += now_ns() - convergence_t0;
                for (uint32_t w = 0; w < W; w++)
                    for (uint32_t ri = 0; ri < nrels; ri++)
                        col_rel_destroy(ctxs[w].delta_rels[ri]);
                converged = true;
                break;
            }
            coord->tdd_convergence_ns += now_ns() - convergence_t0;

            /* #1661: the sweep comments in tdd_exchange_deltas and
             * tdd_owner_exchange_deltas depend on this call staying
             * immediately before the exchange dispatch below.  Moving or
             * guarding it invalidates them. */
            rc = wl_columnar_eval_retire_prior_deltas(sp, coord, W);
            if (rc != 0) {
                if (coord->tdd_owner_lifetime) {
                    int cleanup_rc =
                        wl_columnar_eval_tdd_owner_lifetime_dispose_all(
                        coord->tdd_owner_lifetime);
                    if (cleanup_rc != 0)
                        rc = cleanup_rc;
                } else {
                    tdd_destroy_delta_slots(ctxs, W, nrels);
                }
                goto done;
            }

            /* EXCHANGE: BDX for Category C, hash scatter/gather for
             * standard hybrid, broadcast for replicate/self_join_mode.
             * Issue #372: pass self_join_mode so asymmetric strata broadcast
             * deltas to all workers (each holds 1/W IDB, needs full delta). */
            int brc;
            bool owner_any_accepted = false;
            uint32_t owner_accepted_rows = 0;
            {
                uint64_t t0 = now_ns();
                coord->current_iteration = eff_iter;
                if (owner_exchange_mode)
                    brc = tdd_owner_exchange_deltas(sp, coord, ctxs, W,
                            &owner_any_accepted, &owner_accepted_rows);
                else if (global_read_mode)
                    brc = tdd_global_read_exchange_deltas(sp, coord, ctxs, W,
                            &owner_any_accepted, &owner_accepted_rows);
                else if (bdx_mode)
                    brc = tdd_bdx_exchange_deltas(sp, coord, ctxs, W,
                            bdx_snap);
                else
                    brc = tdd_exchange_deltas(sp, coord, ctxs, W,
                            !replicate_mode, self_join_mode);
                uint64_t exchange_ns = now_ns() - t0;
                coord->exchange_time_ns += exchange_ns;
                coord->tdd_exchange_ns += exchange_ns;
            }

            if (brc != 0) {
                if (getenv("WIRELOG_TDD_GLOBAL_READ_DEBUG"))
                    fprintf(stderr,
                        "TDD exchange error stratum=%u iter=%u rc=%d "
                        "owner=%d global=%d bdx=%d replicate=%d\n",
                        stratum_idx, eff_iter, brc, owner_exchange_mode,
                        global_read_mode, bdx_mode, replicate_mode);
                if (global_read_mode)
                    tdd_destroy_delta_slots(ctxs, W, nrels);
                rc = brc;
                goto done;
            }

            /* Linear owner-mode can be slower than the sequential recursive
             * evaluator on high-diameter, tiny-frontier workloads: every
             * sub-pass pays a W-worker barrier and an exchange for only a few
             * accepted rows.  Bail out early and replay the stratum through
             * the existing single-threaded evaluator when that shape is
             * detected. */
            if (owner_exchange_mode
                && eff_iter >= TDD_OWNER_FALLBACK_MIN_ITER
                && owner_accepted_rows > 0
                && owner_accepted_rows < TDD_OWNER_FALLBACK_DELTA_ROWS) {
                owner_adaptive_fallback = true;
                converged = true;
                break;
            }

            if ((owner_exchange_mode || global_read_mode)
                && !owner_any_accepted) {
                converged = true;
                break;
            }

            outer_any_new = (owner_exchange_mode || global_read_mode)
                ? owner_any_accepted : true;
            if (outer_any_new)
                final_eff_iter = eff_iter;
        } /* end sub loop */

        /* Issue #1380: coordinator-side STORED/TEMPORARY sample per outer
         * iteration (worker partitions are sampled by the workers). */
        col_session_mem_sample(coord);

        if (stride_all_skipped)
            continue;
        if (outer_continue_next)
            continue;
        if (converged || !outer_any_new)
            break;
    } /* end outer loop */

done:
    /* An owner lifetime record owns queue messages and slot arrays across a
     * refused checked destroy. Do not let the queue destructor consume them. */
    if (coord->tdd_owner_lifetime) {
        coord->tdd_owner_lifetime->evaluation_active = false;
        if (owner_dispatch_quiesced) {
            coord->tdd_owner_lifetime->dispatch_active = false;
            coord->tdd_owner_lifetime->worker_ctxs = NULL;
        }
        int lifetime_rc
            = wl_columnar_eval_tdd_owner_lifetime_retry(coord);
        if (lifetime_rc != 0 && rc == 0)
            rc = lifetime_rc;
    } else {
        /* Issue #410: Destroy MPSC delta queue created for this stratum. */
        /* Issue #1380: drain with accounting first so the destructor path has
         * nothing left to reclaim, then release the ring charge. */
        wl_columnar_eval_tdd_queue_discard_delta_queue_ledger(
            coord->delta_queue, &coord->mem_ledger);
        wl_mem_ledger_free(&coord->mem_ledger, WL_MEM_SUBSYS_CHANNEL,
            coord->mem_channel_ring_bytes);
        coord->mem_channel_ring_bytes = 0;
        wl_mpsc_queue_destroy(coord->delta_queue);
        coord->delta_queue = NULL;
    }

    /* Free pre-allocated worker contexts */
    if (ctxs && (!coord->tdd_owner_lifetime
        || !coord->tdd_owner_lifetime->dispatch_active)) {
        if (!owner_slots_embedded)
            for (uint32_t w = 0; w < W; w++)
                free((void *)ctxs[w].delta_rels);
        free(ctxs);
    }
    free(bdx_snap);
    coord->diff_operators_active = saved_diff;

    if (owner_adaptive_fallback && rc == 0) {
        int cleanup_rc = tdd_cleanup_workers(coord);
        if (cleanup_rc != 0) {
            tdd_free_saved_coord_idb(sp, owner_fallback_saved);
            tdd_free_saved_coord_idb(sp, global_read_saved);
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
            return cleanup_rc;
        }
        rc = tdd_restore_coord_idb(sp, coord, owner_fallback_saved);
        tdd_free_saved_coord_idb(sp, owner_fallback_saved);
        tdd_free_saved_coord_idb(sp, global_read_saved);
        coord->tdd_total_ns += now_ns() - tdd_total_t0;
        if (rc != 0)
            return rc;
        if (coord->tdd_audit.enabled)
            coord->tdd_audit.replay = "owner_tiny_frontier";
        coord->frontier_ops->reset_stratum_frontier(coord, stratum_idx,
            coord->outer_epoch);
        return col_eval_stratum(sp, coord, stratum_idx);
    }
    if (global_read_mode && rc == EOVERFLOW) {
        int cleanup_rc = tdd_cleanup_workers(coord);
        if (cleanup_rc != 0) {
            tdd_free_saved_coord_idb(sp, owner_fallback_saved);
            tdd_free_saved_coord_idb(sp, global_read_saved);
            coord->tdd_total_ns += now_ns() - tdd_total_t0;
            return cleanup_rc;
        }
        if (coord->tdd_audit.enabled) {
            coord->tdd_audit.replay = "global_read_overflow";
            coord->tdd_audit.replay_rc = rc;
        }
        int restore_rc = tdd_restore_coord_idb(sp, coord, global_read_saved);
        tdd_free_saved_coord_idb(sp, owner_fallback_saved);
        tdd_free_saved_coord_idb(sp, global_read_saved);
        coord->tdd_total_ns += now_ns() - tdd_total_t0;
        if (restore_rc != 0)
            return restore_rc;
        coord->frontier_ops->reset_stratum_frontier(coord, stratum_idx,
            coord->outer_epoch);
        return col_eval_stratum(sp, coord, stratum_idx);
    }
    tdd_free_saved_coord_idb(sp, owner_fallback_saved);
    tdd_free_saved_coord_idb(sp, global_read_saved);

    uint64_t merge_t0 = now_ns();
    /* Merge while worker results are still owned by the cohort. Coordinator
     * owned modes already accumulated their results during exchange. */
    if (rc == 0 && !bdx_mode && !owner_exchange_mode && !global_read_mode)
        rc = tdd_merge_worker_results(sp, coord);
    int cleanup_rc = tdd_cleanup_workers(coord);
    if (cleanup_rc != 0)
        rc = cleanup_rc;

    /* All internal worker views must retire before coordinator publication. */
    for (uint32_t ri = 0; rc == 0 && ri < nrels; ri++)
        rc = wl_columnar_eval_finalize_relation(coord,
                session_find_rel(coord, sp->relations[ri].name));
    if (rc == 0)
        rc = wl_columnar_eval_serial_canonicalize_aggregates(sp, coord);
    coord->tdd_final_merge_ns += now_ns() - merge_t0;

    if (rc == 0) {
        tdd_record_recursive_convergence(coord, sp, stratum_idx,
            rule_id_base, final_eff_iter);
        /* Preserve completed history, but never mark refused finalization
         * as a completed evaluation for the first-snapshot guard. */
        coord->total_iterations += final_eff_iter;
    }
    coord->tdd_total_ns += now_ns() - tdd_total_t0;
    return rc;
}

/*
 * col_eval_stratum_tdd_nonrecursive:
 * Non-recursive distributed path: PARTITION → DISPATCH → BARRIER →
 * CONSOLIDATE (no exchange needed for non-recursive rules).
 */
static int
col_eval_stratum_tdd_nonrecursive(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t stratum_idx)
{
    uint32_t W = coord->num_workers;
    int rc;

    W = tdd_choose_active_workers(sp, coord, W, false);
    if (W <= 1) {
        tdd_record_active_workers(coord, 1);
        return col_eval_stratum(sp, coord, stratum_idx);
    }

    /* Phase 1: PARTITION — partition all coordinator relations to workers */
    rc = tdd_init_workers(coord, W);
    if (rc != 0)
        return rc;

    /* Phase 2: DISPATCH — submit W workers */
    col_eval_tdd_worker_ctx_t *ctxs
        = (col_eval_tdd_worker_ctx_t *)calloc(
            W, sizeof(col_eval_tdd_worker_ctx_t));
    if (!ctxs) {
        return tdd_cleanup_preserve_error(coord, ENOMEM);
    }

    for (uint32_t w = 0; w < W; w++) {
        ctxs[w].sp = sp;
        ctxs[w].worker_sess = &coord->tdd_workers[w];
        ctxs[w].stratum_idx = stratum_idx;
        ctxs[w].rc = 0;
        if (wl_workqueue_submit(coord->wq, tdd_worker_nonrecursive_fn,
            &ctxs[w])
            != 0) {
            rc = ENOMEM;
            wl_workqueue_drain(coord->wq);
            free(ctxs);
            return tdd_cleanup_preserve_error(coord, rc);
        }
    }

    /* Phase 3: BARRIER */
    wl_workqueue_wait_all(coord->wq);

    /* Collect first worker error */
    for (uint32_t w = 0; w < W; w++) {
        record_worker_expr_status(coord, ctxs[w].worker_sess,
            ctxs[w].rc);
        if (ctxs[w].rc != 0 && rc == 0)
            rc = ctxs[w].rc;
    }
    free(ctxs);

    /* Phase 6: CONSOLIDATE — merge worker IDB results to coordinator.
     * This is a serial coordinator phase: accumulate into exchange_time_ns
     * so serial_fraction / exchange_fraction accounts for non-recursive strata
     * as well as recursive exchange barriers. */
    if (rc == 0) {
        uint64_t t0 = now_ns();
        rc = tdd_merge_worker_results(sp, coord);
        coord->exchange_time_ns += now_ns() - t0;
    }

    if (coord->plain_step_completion_step_context && rc == 0) {
        /* Arm before checked worker retirement: a refusal here is also a
         * post-merge completion failure and must be retryable. */
        coord->plain_step_completion_pending = true;
        coord->plain_step_completion_active = true;
        coord->plain_step_completion_stratum = stratum_idx;
        coord->plain_step_completion_phase =
            WL_COLUMNAR_PLAIN_STEP_COMPLETION_FINALIZE_NONREC;
        coord->plain_step_completion_relation = 0;
        coord->plain_step_completion_workers_retired = false;
    }

    /* Worker results have been merged; retire their dependencies before
     * normalizing any registered coordinator relation. */
    int cleanup_rc = tdd_cleanup_workers(coord);
    if (cleanup_rc != 0)
        rc = cleanup_rc;
    if (rc != 0) {
        coord->plain_step_completion_active = false;
        return rc;
    }
    coord->plain_step_completion_workers_retired =
        coord->plain_step_completion_step_context;

    if (!coord->plain_step_completion_step_context) {
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            rc = wl_columnar_eval_finalize_relation(coord,
                    session_find_rel(coord, sp->relations[ri].name));
            if (rc != 0)
                return rc;
        }
        tdd_record_nonrecursive_convergence(coord, sp, stratum_idx);
        return 0;
    }

    /* Worker merge is complete.  Keep the finalization cursor outside the
     * worker contexts so a checked reader refusal can be retried through a
     * later public STEP or SNAPSHOT without dispatching the stratum again. */
    return wl_columnar_eval_resume_nonrecursive_completion(coord);
}

int
wl_columnar_eval_resume_nonrecursive_completion(wl_col_session_t *coord)
{
    if (!coord || !coord->plain_step_completion_pending)
        return EINVAL;
    if (coord->plain_step_completion_phase
        != WL_COLUMNAR_PLAIN_STEP_COMPLETION_FINALIZE_NONREC)
        return EINVAL;
    if (!coord->plan
        || coord->plain_step_completion_stratum >= coord->plan->stratum_count)
        return EINVAL;

    const wl_plan_stratum_t *sp =
        &coord->plan->strata[coord->plain_step_completion_stratum];
    if (!coord->plain_step_completion_workers_retired) {
        int cleanup_rc = tdd_cleanup_workers(coord);
        if (cleanup_rc != 0) {
            coord->plain_step_completion_active = false;
            return cleanup_rc;
        }
        coord->plain_step_completion_workers_retired = true;
    }
    while (coord->plain_step_completion_relation < sp->relation_count) {
        uint32_t ri = coord->plain_step_completion_relation;
        int rc = wl_columnar_eval_finalize_relation(coord,
                session_find_rel(coord, sp->relations[ri].name));
        if (rc != 0) {
            coord->plain_step_completion_active = false;
            return rc;
        }
        coord->plain_step_completion_relation++;
    }
    tdd_record_nonrecursive_convergence(coord, sp,
        coord->plain_step_completion_stratum);
    coord->plain_step_completion_phase =
        WL_COLUMNAR_PLAIN_STEP_COMPLETION_EVALUATE_REMAINING;
    coord->plain_step_completion_relation = 0;
    return 0;
}

/*
 * col_eval_stratum_tdd:
 * Distributed stratum evaluator with 7-phase pipeline.
 *
 * For W=1: delegates to col_eval_stratum() (zero overhead).
 * For W>1: orchestrates PARTITION → DISPATCH → BARRIER →
 *          EXCHANGE → BARRIER → CONSOLIDATE → CONVERGENCE
 *          per iteration of the semi-naive fixed-point loop.
 *
 * Called from col_session_step() in place of col_eval_stratum()
 * when distributed evaluation is possible.
 */
int
col_eval_stratum_tdd(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t stratum_idx)
{
    if (!sp || !coord)
        return EINVAL;

    if (tdd_stratum_has_unsupported_lftj(sp))
        return col_eval_stratum(sp, coord, stratum_idx);

    /* Single-worker fast path: zero overhead delegation */
    if (coord->num_workers <= 1)
        return col_eval_stratum(sp, coord, stratum_idx);

    if (!sp->is_recursive)
        return col_eval_stratum_tdd_nonrecursive(sp, coord, stratum_idx);

    return col_eval_stratum_tdd_recursive(sp, coord, stratum_idx);
}

/* ======================================================================== */
/* Multi-Worker Stratum Evaluation (Issue #317)                             */
/* ======================================================================== */

/*
 * col_eval_stratum_worker_ctx_t:
 * Per-worker context for col_eval_stratum_multiworker dispatch.
 */
typedef struct {
    const wl_plan_stratum_t *sp;   /* borrowed: stratum plan */
    wl_col_session_t *worker_sess; /* borrowed: isolated worker session */
    uint32_t stratum_idx;
    int rc; /* return code from col_eval_stratum */
} col_eval_stratum_worker_ctx_t;

/*
 * col_eval_stratum_worker_fn:
 * Work function executed by each worker thread.  Runs col_eval_stratum on
 * the worker's partition, then reports the resulting frontier to the
 * coordinator's progress tracker (Issue #317).
 *
 * Thread safety: writes only to its own progress slot (worker_id dimension),
 * so no synchronization is needed during the scatter phase.
 */
static void
col_eval_stratum_worker_fn(void *arg)
{
    col_eval_stratum_worker_ctx_t *ctx = (col_eval_stratum_worker_ctx_t *)arg;

    ctx->rc = col_eval_stratum(ctx->sp, ctx->worker_sess, ctx->stratum_idx);
}

/*
 * col_eval_stratum_multiworker:
 * Evaluate one stratum in parallel across num_workers pre-created worker
 * sessions.  After wl_workqueue_wait_all(), merges per-worker frontier
 * progress reports into the coordinator's global frontier.
 *
 * Protocol (Issue #317):
 *   1. Reset progress for this stratum (stale epoch entries cleared).
 *   2. Submit num_workers tasks; each runs col_eval_stratum + progress_record.
 *   3. wl_workqueue_wait_all() barrier: all workers complete.
 *   4. If all workers converged, update coordinator's frontier with the
 *      global minimum iteration (conservative lower bound for skip logic).
 *
 * Preconditions:
 *   - coord->wq is non-NULL (thread pool created at col_session_create)
 *   - workers[0..num_workers-1] are valid worker sessions with coordinator
 *     pointer set to coord
 *   - coord->progress is initialized (done in col_session_create)
 *
 * Returns 0 on success, EINVAL on bad arguments, or the first non-zero
 * error code returned by a worker.
 */
int
col_eval_stratum_multiworker(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord, uint32_t stratum_idx,
    wl_col_session_t *workers, uint32_t num_workers)
{
    if (!sp || !coord || !workers || num_workers == 0)
        return EINVAL;

    /* Step 1: Reset this stratum's progress slots for the current epoch.
     * Prevents stale convergence reports from a previous epoch blocking
     * the all_converged check after the barrier. */
    wl_frontier_progress_reset_stratum(&coord->progress, stratum_idx,
        coord->outer_epoch);

    /* Step 2: Allocate per-worker contexts and submit to workqueue */
    col_eval_stratum_worker_ctx_t *ctxs
        = (col_eval_stratum_worker_ctx_t *)calloc(
            num_workers, sizeof(col_eval_stratum_worker_ctx_t));
    if (!ctxs)
        return ENOMEM;

    int rc = 0;
    for (uint32_t w = 0; w < num_workers; w++) {
        ctxs[w].sp = sp;
        ctxs[w].worker_sess = &workers[w];
        ctxs[w].stratum_idx = stratum_idx;
        ctxs[w].rc = 0;
        if (wl_workqueue_submit(coord->wq, col_eval_stratum_worker_fn,
            &ctxs[w])
            != 0) {
            rc = ENOMEM;
            wl_workqueue_drain(coord->wq);
            free(ctxs);
            return rc;
        }
    }

    /* Step 3: Barrier — wait for all workers to complete and report */
    wl_workqueue_wait_all(coord->wq);

    /* Collect first worker error (if any) */
    for (uint32_t w = 0; w < num_workers; w++) {
        record_worker_expr_status(coord, ctxs[w].worker_sess,
            ctxs[w].rc);
        if (ctxs[w].rc != 0 && rc == 0)
            rc = ctxs[w].rc;
    }
    free(ctxs);
    if (rc != 0)
        return rc;

    /* Step 4: Merge per-worker frontiers into coordinator's global frontier.
     * The global minimum iteration is the conservative bound: the coordinator
     * can safely claim "all workers have processed up to iteration min_iter",
     * enabling the frontier skip optimization for subsequent incremental eval. */
    if (wl_frontier_progress_all_converged(&coord->progress, stratum_idx,
        coord->outer_epoch)) {
        uint32_t min_iter = wl_frontier_progress_min_iteration(
            &coord->progress, stratum_idx, coord->outer_epoch);
        if (min_iter != UINT32_MAX) {
            coord->frontier_ops->record_stratum_convergence(coord,
                stratum_idx, coord->outer_epoch, min_iter);
        }
    }

    return 0;
}

/* ======================================================================== */
/* Inline compound helpers are implemented in columnar/inline.c. */
