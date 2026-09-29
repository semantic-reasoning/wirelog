/*
 * test_tdd_occurrence_execution.c - executable occurrence-bound TDD inputs
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */
#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #c); return 1; \
                      } } while (0)

extern int (*wl_columnar_eval_test_bound_slice_after_eval)(
    wl_columnar_eval_tdd_run_t *);
extern int (*wl_columnar_eval_test_bound_slice_before_release)(
    wl_columnar_eval_tdd_run_t *);
#ifdef WL_TEST_ALLOC_WRAP
void *__real_calloc(size_t, size_t);
static int fail_calloc_after = -1;
void *__wrap_calloc(size_t n, size_t size)
{
    if (fail_calloc_after >= 0 && fail_calloc_after-- == 0)
        return NULL;
    return __real_calloc(n, size);
}
#endif

static const int64_t full_flow[][2] = {{1, 8}, {8, 19}, {19, 31}, {31, 1}};
static const int64_t delta_flow[][2] = {{1, 8}, {19, 31}, {31, 1}};
static const int64_t full_memory[][2] = {{8, 19}, {19, 8}, {31, 19}};
static const int64_t delta_memory[][2] = {{8, 19}, {31, 19}};
static int snapshot_token, partition_token;

static col_rel_t *
relation(const char *name, const int64_t rows[][2], uint32_t count,
    uint32_t worker, uint32_t width)
{
    col_rel_t *r = col_rel_new_auto(name, 2);
    if (!r)
        return NULL;
    for (uint32_t i = 0; i < count; i++) {
        if (width && (uint64_t)rows[i][0] % width != worker)
            continue;
        if (col_rel_append_row(r, rows[i]) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

static wl_columnar_eval_tdd_input_t
capture(const wl_columnar_eval_tdd_plan_read_t *read, col_rel_t *r,
    uint32_t worker, uint32_t width)
{
    bool delta = read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA;
    return (wl_columnar_eval_tdd_input_t){
               .read = read, .relation = r, .name = r->name,
               .snapshot = &snapshot_token,
               .partition = delta ? &partition_token : NULL,
               .worker_index = worker, .worker_count = width,
               .view_generation = r->view_generation,
               .storage_generation = r->storage_generation,
               .ncols = r->ncols, .declared_ncols = r->declared_ncols,
               .schema_ok = r->schema_ok,
               .column_names = (const char *const *)r->col_names,
               .column_types = r->column_types,
               .compound_kind = r->compound_kind,
               .compound_count = r->compound_count,
               .compound_arity_len = r->compound_arity_len,
               .compound_arity_map = r->compound_arity_map,
               .inline_physical_offset = r->inline_physical_offset,
               .has_graph_column = r->has_graph_column,
               .graph_col_idx = r->graph_col_idx,
    };
}

static uint64_t
reserved(wl_col_session_t *worker)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(worker->memory_governor));
}

static int
run_one(wl_col_session_t *coord, const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *manifest, uint32_t si,
    uint32_t w, uint32_t width, unsigned seen[4])
{
    const wl_columnar_eval_tdd_plan_slice_t *slice = &manifest->slices[si];
    wl_col_session_t worker = {0};
    CHECK(col_worker_session_create(coord, w, NULL, 0, &worker) == 0);
    /* A bound read must win over both full and delta registry aliases. */
    const char *wrong_names[] = {"valueFlow", "memoryAlias", "$d$valueFlow",
                                 "$r$valueFlow", "$d$memoryAlias",
                                 "$r$memoryAlias"};
    const int64_t wrong[][2] = {{777, 888}};
    for (uint32_t i = 0; i < 6; i++) {
        col_rel_t *bad = relation(wrong_names[i], wrong, 1, 0, 0);
        CHECK(bad && session_add_rel(&worker, bad) == 0);
    }
    col_rel_t *flow = relation("valueFlow", full_flow, 4, 0, 0);
    col_rel_t *memory = relation("memoryAlias", full_memory, 3, 0, 0);
    col_rel_t *dflow = relation("flow-partition", delta_flow, 3, w, width);
    col_rel_t *dmemory = relation("memory-partition", delta_memory, 2, w,
            width);
    CHECK(flow && memory && dflow && dmemory);
    CHECK(slice->read_count <= 4);
    wl_columnar_eval_tdd_input_t inputs[4];
    for (uint32_t i = 0; i < slice->read_count; i++) {
        const wl_columnar_eval_tdd_plan_read_t *read =
            &manifest->reads[slice->read_start+i];
        bool f = strcmp(read->relation_name, "valueFlow") == 0;
        bool d = read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA;
        inputs[i] = capture(read,
                d ? (f ? dflow : dmemory) : (f ? flow : memory), w, width);
    }
    uint64_t before = reserved(&worker);
    uint32_t cache_before = worker.mat_cache.count;
    wl_columnar_eval_tdd_run_t run = {0};
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest, si,
        &snapshot_token, &partition_token, w, width, inputs, slice->read_count,
        &run) == 0);
    CHECK(run.worker == &worker && worker.tdd_input_run == &run && run.output);
    CHECK(col_worker_session_destroy(&worker) == EBUSY);
    CHECK(wl_columnar_session_cleanup_ready(&worker) == EBUSY);
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest, si,
        &snapshot_token, &partition_token, w, width, inputs, slice->read_count,
        &run) == EBUSY);
    for (uint32_t i = 0; i < slice->read_count; i++) {
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(col_rel_source_writer_acquire(inputs[i].relation,
            &writer) == EBUSY);
        CHECK(inputs[i].relation->timestamps == NULL);
        CHECK(col_rel_enable_timestamps(inputs[i].relation) == EBUSY);
    }
    CHECK(worker.mat_cache.count == cache_before);
    col_rel_t *out = NULL;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out) == 0);
    CHECK(out && !run.worker && !worker.tdd_input_run && !out->pool_owned &&
        !out->arena_owned);
    CHECK(out->ncols == 2 && out->memory_governor == worker.memory_governor);
    static const int64_t rule0[][2] = {{8, 8}, {31, 31}, {1, 1}};
    static const int64_t rule1[][2] = {{31, 19}, {1, 31}};
    static const int64_t rule2[][2] = {{19, 31}, {1, 31}};
    const int64_t (*expected)[2] = slice->ordinal == 0 ? rule0
        : slice->alternative == 2 ? rule1 : rule2;
    uint32_t n = slice->ordinal == 0 ? 3 : 2;
    for (uint32_t i = 0; i < out->nrows; i++) {
        uint32_t j = 0;
        while (j < n && (out->columns[0][i] != expected[j][0]
            || out->columns[1][i] != expected[j][1]))
            j++;
        if (j == n) fprintf(stderr,
                "slice %u alt %u ordinal %u worker %u/%u got %lld,%lld\n", si,
                slice->alternative, slice->ordinal, w, width,
                (long long)out->columns[0][i], (long long)out->columns[1][i]);
        CHECK(j < n);
        seen[j]++;
    }
    CHECK(col_rel_destroy_checked(out) == 0);
    CHECK(reserved(&worker) == before);
    for (uint32_t i = 0; i < slice->read_count; i++) {
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(col_rel_source_writer_acquire(inputs[i].relation, &writer) == 0);
        CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
        CHECK(inputs[i].relation->view_generation == inputs[i].view_generation);
    }
    CHECK(flow->nrows == 4 && memory->nrows == 3);
    for (uint32_t i = 0; i < 4; i++)
        CHECK(flow->columns[0][i] == full_flow[i][0] &&
            flow->columns[1][i] == full_flow[i][1]);
    col_rel_destroy(flow); col_rel_destroy(memory);
    col_rel_destroy(dflow); col_rel_destroy(dmemory);
    CHECK(col_worker_session_destroy(&worker) == 0);
    return 0;
}

static wl_columnar_source_access_reader_t held_result;
static unsigned evaluations, release_calls;

static int
pin_result_and_fail(wl_columnar_eval_tdd_run_t *run)
{
    evaluations++;
    eval_entry_t *entry = wl_columnar_eval_stack_cleanup_result(run->cleanup);
    if (!entry->owned || col_rel_source_reader_acquire(entry->rel,
        &held_result))
        return EINVAL;
    return ENOMEM;
}

static int
count_evaluation(wl_columnar_eval_tdd_run_t *run)
{
    (void)run;
    evaluations++;
    return 0;
}

static int
refuse_release_once(wl_columnar_eval_tdd_run_t *run)
{
    (void)run;
    return release_calls++ == 0 ? EBUSY : 0;
}

static int
negative_cases(wl_col_session_t *coord, const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *manifest)
{
    uint32_t si = 0;
    while (si < manifest->slice_count && (manifest->slices[si].inactive
        || manifest->slices[si].seed ||
        manifest->slices[si].ordinal != 1)) si++;
    CHECK(si < manifest->slice_count);
    const wl_columnar_eval_tdd_plan_slice_t *slice = &manifest->slices[si];
    wl_col_session_t worker = {0};
    CHECK(col_worker_session_create(coord, 0, NULL, 0, &worker) == 0);
    col_rel_t *flow = relation("valueFlow", full_flow, 4, 0, 0);
    col_rel_t *memory = relation("memoryAlias", full_memory, 3, 0, 0);
    col_rel_t *delta = relation("partition", delta_flow, 3, 0, 0);
    CHECK(flow && memory && delta && slice->read_count == 4);
    wl_columnar_eval_tdd_input_t good[4], inputs[4];
    for (uint32_t i = 0; i < 4; i++) {
        const wl_columnar_eval_tdd_plan_read_t *read =
            &manifest->reads[slice->read_start+i];
        good[i] = capture(read,
                read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA ? delta
            : strcmp(read->relation_name, "valueFlow") == 0 ? flow : memory, 0,
                1);
    }
    wl_columnar_eval_tdd_run_t run = {0};
    uint64_t before = reserved(&worker);
    uint32_t driver = 0;
    while (good[driver].read->kind != WL_COLUMNAR_EVAL_TDD_PLAN_DELTA) driver++;
    /* Each case must fail before evaluation and restore all setup ownership. */
    for (uint32_t c = 0; c < 23; c++) {
        memcpy(inputs, good, sizeof(inputs));
        uint32_t count = 4;
        wl_columnar_eval_tdd_plan_manifest_t wrong = *manifest;
        const wl_columnar_eval_tdd_plan_manifest_t *m = manifest;
        switch (c) {
        case 0: inputs[0].read = good[1].read; break;
        case 1: count--; break;
        case 2: inputs[0].snapshot = &partition_token; break;
        case 3: inputs[0].view_generation++; break;
        case 4: inputs[0].storage_generation++; break;
        case 5: inputs[0].ncols++; break;
        case 6: inputs[0].name = "wrong"; break;
        case 7: inputs[driver].partition = &snapshot_token; break;
        case 8: inputs[driver].worker_index = 1; break;
        case 9: inputs[driver].worker_count = 2; break;
        case 10: wrong.owner_ops++; m = &wrong; break;
        case 11: wrong.owner_relation = NULL; m = &wrong; break;
        case 12: wrong.alternative_count = 1; m = &wrong; break;
        case 13: worker.diff_operators_active = true; break;
        case 14: worker.retraction_seeded = true; break;
        case 15: worker.retraction_right_pass = true; break;
        case 16: worker.delta_seeded = true; break;
        case 17: worker.join_batch_bytes = 1024; break;
        case 18: worker.join_batch_strict = true; break;
        case 19: inputs[0].compound_count++; break;
        case 20: inputs[0].declared_ncols++; break;
        case 21: inputs[0].schema_ok = !inputs[0].schema_ok; break;
        case 22: inputs[0].column_names = NULL; break;
        }
        int rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, m, si,
                &snapshot_token, &partition_token, 0, 1, inputs, count, &run);
        CHECK(rc == (c >= 12 && c <= 18 ? ENOTSUP : EINVAL));
        CHECK(!run.worker && !worker.tdd_input_run && !worker.cleanup_active
            && !worker.cleanup_pending && reserved(&worker) == before);
        worker.diff_operators_active = worker.retraction_seeded = false;
        worker.retraction_right_pass = worker.delta_seeded = false;
        worker.join_batch_bytes = 0; worker.join_batch_strict = false;
    }
    for (uint32_t c = 0; c < 4; c++) {
        wl_columnar_eval_tdd_plan_read_t *read =
            &manifest->reads[slice->read_start];
        wl_columnar_eval_tdd_plan_read_t saved = *read;
        if (c == 0) read->op_index++;
        if (c == 1) read->right_operand = !read->right_operand;
        if (c == 2) read->kind = WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER;
        if (c == 3) read->relation_name = "wrong";
        int rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp,
                manifest, si,
                &snapshot_token, &partition_token, 0, 1, good, 4, &run);
        *read = saved;
        CHECK(rc == EINVAL && !run.worker && reserved(&worker) == before);
    }
    for (uint32_t s = 0; s < manifest->slice_count; s++) {
        if (!manifest->slices[s].inactive &&
            !manifest->slices[s].seed) continue;
        CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest,
            s,
            &snapshot_token, &partition_token, 0, 1, good, 4, &run) == ENOTSUP);
    }
    /* Failure at the second distinct descriptor unwinds the first reader. */
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(col_rel_source_writer_acquire(memory, &writer) == 0);
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest, si,
        &snapshot_token, &partition_token, 0, 1, good, 4, &run) == EBUSY);
    CHECK(!run.worker && reserved(&worker) == before);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    CHECK(col_rel_source_writer_acquire(delta, &writer) == 0);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    CHECK(col_rel_source_writer_acquire(flow, &writer) == 0);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);

    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(worker.memory_governor);
    uint64_t limit = atomic_load_explicit(&governor->usable_bytes,
            memory_order_seq_cst);
    wl_columnar_memory_mode_t mode = governor->mode;
    governor->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    atomic_store_explicit(&governor->usable_bytes, before,
        memory_order_seq_cst);
    worker.memory_budget_denied = false;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest, si,
        &snapshot_token, &partition_token, 0, 1, good, 4, &run) == ENOSPC);
    CHECK(worker.memory_budget_denied && !run.worker &&
        reserved(&worker) == before);
    atomic_store_explicit(&governor->usable_bytes,
        before + 4 * sizeof(wl_columnar_eval_tdd_input_slot_t),
        memory_order_seq_cst);
    worker.memory_budget_denied = false;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest, si,
        &snapshot_token, &partition_token, 0, 1, good, 4, &run) == ENOSPC);
    CHECK(worker.memory_budget_denied && !run.worker &&
        reserved(&worker) == before);
    atomic_store_explicit(&governor->usable_bytes, limit,
        memory_order_seq_cst);
    governor->mode = mode;
    worker.memory_budget_denied = false;
#ifdef WL_TEST_ALLOC_WRAP
    /* Sweep preparation, cleanup-frame, operators and detached-result calloc.
     * Some existing operator allocations may recover, but every success must
     * yield correct output and every failure must return all ownership. */
    unsigned failures = 0, successes = 0;
    for (int at = 0; at < 40; at++) {
        fail_calloc_after = at;
        int rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp,
                manifest, si,
                &snapshot_token, &partition_token, 0, 1, good, 4, &run);
        fail_calloc_after = -1;
        if (rc == 0) {
            CHECK(run.output && run.output->nrows == 2);
            CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, NULL) == 0);
            successes++;
        } else {
            CHECK(rc == ENOMEM);
            failures++;
        }
        CHECK(!run.worker && !worker.tdd_input_run && !worker.cleanup_active
            && !worker.cleanup_pending && !worker.memory_budget_denied);
        CHECK(reserved(&worker) == before);
    }
    CHECK(failures >= 4 && successes > 0);
#endif
    /* Real cleanup refusal: pin the owned intermediate, then preserve ENOMEM
     * and all sources until the pin is released. Finish must never re-evaluate. */
    evaluations = 0;
    wl_columnar_eval_test_bound_slice_after_eval = pin_result_and_fail;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest, si,
        &snapshot_token, &partition_token, 0, 1, good, 4, &run) == ENOMEM);
    CHECK(run.worker && worker.cleanup_pending && held_result.owner &&
        evaluations == 1);
    CHECK(col_worker_session_destroy(&worker) == EBUSY);
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, NULL) == ENOMEM);
    CHECK(run.worker && evaluations == 1);
    CHECK(col_rel_source_reader_release(&held_result) == 0);
    wl_columnar_eval_test_bound_slice_after_eval = NULL;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, NULL) == ENOMEM);
    CHECK(!run.worker && !worker.tdd_input_run && reserved(&worker) == before);

    evaluations = release_calls = 0;
    wl_columnar_eval_test_bound_slice_after_eval = count_evaluation;
    wl_columnar_eval_test_bound_slice_before_release = refuse_release_once;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest, si,
        &snapshot_token, &partition_token, 0, 1, good, 4, &run) == 0);
    col_rel_t *saved = run.output, *out = NULL;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out) == EBUSY);
    CHECK(!out && run.output == saved && evaluations == 1);
    CHECK(!run.slots && !run.input_count && reserved(&worker) > before);
    CHECK(col_rel_source_writer_acquire(flow, &writer) == 0);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    CHECK(col_worker_session_create(coord, 0, NULL, 0, &worker) == EBUSY);
    /* A real token refusal also keeps the output and outstanding credit. */
    run.reservation.identity = NULL;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out) == EINVAL);
    CHECK(!out && run.output == saved && !run.slots &&
        reserved(&worker) > before);
    run.reservation.identity = &run.reservation;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out) == 0);
    CHECK(out == saved && evaluations == 1 && !worker.tdd_input_run);
    wl_columnar_eval_test_bound_slice_after_eval = NULL;
    wl_columnar_eval_test_bound_slice_before_release = NULL;
    CHECK(col_rel_destroy_checked(out) == 0 && reserved(&worker) == before);
    worker.filt_cache_active_pins = 1;
    CHECK(col_worker_session_destroy(&worker) == EBUSY &&
        worker.teardown_started);
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest, si,
        &snapshot_token, &partition_token, 0, 1, good, 4, &run) == EBUSY);
    CHECK(!run.worker && !worker.tdd_input_run && reserved(&worker) == before);
    worker.filt_cache_active_pins = 0;
    col_rel_destroy(flow); col_rel_destroy(memory); col_rel_destroy(delta);
    CHECK(col_worker_session_destroy(&worker) == 0);
    return 0;
}

/* Hand-built expanded plans exercise the materialization hint and borrowed
 * 0/1-row VARIABLE outputs separately from the untouched generated CSPA plan. */
static int
synthetic_cases(wl_col_session_t *coord)
{
    const char *keys[] = {"col0"};
    uint32_t exchange_key = 0;
    wl_plan_op_exchange_t exchange = {.key_col_idxs = &exchange_key,
                                      .key_col_count = 1};
    for (uint32_t join = 0; join < 2; join++) {
        wl_plan_op_t ops[10] = {0};
        uint32_t width = join ? 3 : 4;
        for (uint32_t alt = 0; alt < 2; alt++) {
            wl_plan_op_t *b = &ops[alt*width];
            b[0] = (wl_plan_op_t){.op = WL_PLAN_OP_VARIABLE,
                                  .relation_name = "p",
                                  .delta_mode = alt == 0 ? WL_DELTA_FORCE_DELTA
                    : join ? WL_DELTA_FORCE_FULL : WL_DELTA_FORCE_EMPTY};
            b[1] =
                (wl_plan_op_t){.op =
                                   join ? WL_PLAN_OP_JOIN : WL_PLAN_OP_VARIABLE,
                               .relation_name = join ? NULL : "p",
                               .right_relation = join ? "p" : NULL,
                               .delta_mode = alt == 1 ? WL_DELTA_FORCE_DELTA
                    : join ? WL_DELTA_FORCE_FULL : WL_DELTA_FORCE_EMPTY,
                               .materialized = join != 0, .key_count = join,
                               .left_keys = join ? keys : NULL,
                               .right_keys = join ? keys : NULL};
            for (uint32_t i = 2; i < width; i++) b[i].op = WL_PLAN_OP_CONCAT;
        }
        ops[2*width].op = WL_PLAN_OP_CONSOLIDATE;
        ops[2*width+1] = (wl_plan_op_t){.op = WL_PLAN_OP_EXCHANGE,
                                        .opaque_data = &exchange};
        wl_plan_relation_t rel = {.name = "p", .ops = ops,
                                  .op_count = 2*width+2};
        wl_plan_stratum_t sp = {.is_recursive = true, .relations = &rel,
                                .relation_count = 1};
        wl_columnar_eval_tdd_plan_manifest_t manifest = {0};
        CHECK(wl_columnar_eval_tdd_plan_bindings(&sp, 0, &manifest) == 0);
        for (uint32_t rows = 0; rows < 3; rows++) {
            wl_col_session_t worker = {0};
            CHECK(col_worker_session_create(coord, 0, NULL, 0, &worker) == 0);
            col_rel_t *full = relation("p", full_flow, 4, 0, 0);
            col_rel_t *delta = relation("partition", delta_flow, rows, 0, 0);
            CHECK(full && delta);
            wl_columnar_eval_tdd_input_t inputs[2];
            const wl_columnar_eval_tdd_plan_slice_t *slice =
                &manifest.slices[0];
            for (uint32_t i = 0; i < slice->read_count; i++) {
                const wl_columnar_eval_tdd_plan_read_t *r =
                    &manifest.reads[slice->read_start+i];
                inputs[i] = capture(r,
                        r->kind ==
                        WL_COLUMNAR_EVAL_TDD_PLAN_DELTA ? delta : full, 0, 1);
            }
            if (join) {
                const int64_t wrong[][2] = {{999, 999}};
                col_rel_t *cached = relation("bad-cache", wrong, 1, 0, 0);
                CHECK(cached && col_mat_cache_insert(&worker.mat_cache, delta,
                    full, cached) == 0);
            }
            uint64_t before = reserved(&worker), hits = worker.mat_cache.hits,
                misses = worker.mat_cache.misses;
            uint32_t cached_count = worker.mat_cache.count;
            wl_columnar_eval_tdd_run_t run = {0};
            if (join) {
                ops[1].right_filter_expr.size = 1;
                CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, &sp,
                    &manifest, 0,
                    &snapshot_token, &partition_token, 0, 1, inputs,
                    slice->read_count, &run) == ENOTSUP);
                ops[1].right_filter_expr.size = 0;
                ops[1].op = WL_PLAN_OP_SEMIJOIN;
                manifest.reads[1].kind = WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER;
                CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, &sp,
                    &manifest, 0,
                    &snapshot_token, &partition_token, 0, 1, inputs,
                    slice->read_count, &run) == ENOTSUP);
                ops[1].op = WL_PLAN_OP_JOIN;
                manifest.reads[1].kind = WL_COLUMNAR_EVAL_TDD_PLAN_FULL;
                CHECK(!run.worker && !worker.cleanup_active &&
                    reserved(&worker) == before);
            }
            CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, &sp,
                &manifest, 0,
                &snapshot_token, &partition_token, 0, 1, inputs,
                slice->read_count, &run) == 0);
            col_rel_t *out = NULL;
            CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out) == 0);
            CHECK(out && out != delta && out->nrows == rows &&
                out->ncols == (join ? 4u : 2u));
            CHECK(worker.mat_cache.count == cached_count &&
                worker.mat_cache.hits == hits
                && worker.mat_cache.misses == misses &&
                ops[1].materialized == (join != 0));
            CHECK(col_rel_destroy_checked(delta) == 0 &&
                col_rel_destroy_checked(full) == 0);
            for (uint32_t r = 0; r < rows; r++) {
                CHECK(out->columns[0][r] == delta_flow[r][0] &&
                    out->columns[1][r] == delta_flow[r][1]);
                if (join) CHECK(out->columns[2][r] == delta_flow[r][0] &&
                        out->columns[3][r] == delta_flow[r][1]);
            }
            CHECK(col_rel_destroy_checked(out) == 0 &&
                reserved(&worker) == before);
            CHECK(col_worker_session_destroy(&worker) == 0);
        }
        wl_columnar_eval_tdd_plan_bindings_free(&manifest);
    }
    return 0;
}

static const int64_t seed_assign[][2] = {
    {1, 4}, {1, 4}, {4, 1}, {1, 1}, {9, 4},
};
static const int64_t seed_expected[3][5][2] = {
    {{1, 4}, {1, 4}, {4, 1}, {1, 1}, {9, 4}},
    {{1, 1}, {1, 1}, {4, 4}, {1, 1}, {9, 9}},
    {{4, 4}, {4, 4}, {1, 1}, {1, 1}, {4, 4}},
};

/* Compare occurrences, including duplicate rows; no consolidation oracle. */
static int
seed_bag(const int64_t actual[][2], const int64_t expected[][2], uint32_t n)
{
    bool used[15] = {0};
    CHECK(n <= 15);
    for (uint32_t i = 0; i < n; i++) {
        uint32_t j = 0;
        while (j < n && (used[j] || actual[i][0] != expected[j][0]
            || actual[i][1] != expected[j][1])) j++;
        CHECK(j < n);
        used[j] = true;
    }
    return 0;
}

static int
seed_refusals(wl_col_session_t *coord, const wl_plan_stratum_t *sp,
    wl_columnar_eval_tdd_plan_manifest_t *manifest, uint32_t si)
{
    wl_col_session_t worker = {0};
    CHECK(col_worker_session_create(coord, 0, NULL, 0, &worker) == 0);
    col_rel_t *assign = relation("assign", seed_assign, 5, 0, 0);
    CHECK(assign);
    wl_columnar_eval_tdd_plan_slice_t *slice = &manifest->slices[si];
    wl_columnar_eval_tdd_plan_read_t *read =
        &manifest->reads[slice->read_start];
    wl_columnar_eval_tdd_input_t good = capture(read, assign, 0, 1);
    wl_plan_op_t *var = (wl_plan_op_t *)&manifest->owner_ops[slice->start];
    wl_plan_op_t *map = var + 1;
    wl_columnar_eval_tdd_run_t run = {0};
    uint64_t before = reserved(&worker);
    evaluations = 0;
    wl_columnar_eval_test_bound_slice_after_eval = count_evaluation;
    for (uint32_t c = 0; c < 42; c++) {
        wl_columnar_eval_tdd_input_t input = good;
        wl_columnar_eval_tdd_plan_manifest_t changed = *manifest;
        const wl_columnar_eval_tdd_plan_manifest_t *m = manifest;
        wl_columnar_eval_tdd_plan_slice_t saved_slice = *slice;
        wl_columnar_eval_tdd_plan_read_t saved_read = *read;
        wl_plan_op_t saved_var = *var, saved_map = *map;
        wl_plan_expr_buffer_t saved_expr = map->map_exprs[0];
        uint8_t saved_byte = map->map_exprs[0].data[6];
        uint32_t width = 1, index = 0;
        const void *partition = NULL;
        int expected = EINVAL;
        switch (c) {
        case 0: worker.current_iteration = 1; expected = ENOTSUP; break;
        case 1: worker.diff_operators_active = true; expected = ENOTSUP; break;
        case 2: worker.delta_seeded = true; expected = ENOTSUP; break;
        case 3: worker.retraction_seeded = true; expected = ENOTSUP; break;
        case 4: worker.retraction_right_pass = true; expected = ENOTSUP; break;
        case 5: width = 2; expected = ENOTSUP; break;
        case 6: width = 2; index = 1; expected = ENOTSUP; break;
        case 7: partition = &partition_token; expected = ENOTSUP; break;
        case 8: input.partition = &partition_token; break;
        case 9: input.worker_count = 2; break;
        case 10: input.snapshot = &partition_token; break;
        case 11: input.view_generation++; break;
        case 12: input.storage_generation++; break;
        case 13: input.ncols++; break;
        case 14: changed.owner_ops++; m = &changed; break;
        case 15: slice->driver = slice->start; break;
        case 16: slice->inactive = true; expected = ENOTSUP; break;
        case 17: slice->seed = false; break;
        case 18: slice->ordinal++; expected = ENOTSUP; break;
        case 19: slice->start++; expected = ENOTSUP; break;
        case 20: read->kind = WL_COLUMNAR_EVAL_TDD_PLAN_DELTA; break;
        case 21: read->right_operand = true; break;
        case 22: read->op_index++; break;
        case 23: var->delta_mode = WL_DELTA_AUTO; expected = ENOTSUP; break;
        case 24: var->relation_name = "valueFlow"; expected = ENOTSUP; break;
        case 25: map->project_count = 1; expected = ENOTSUP; break;
        case 26: map->map_exprs[0].size--; expected = ENOTSUP; break;
        case 27: map->map_exprs[0].data[6] = '2'; expected = ENOTSUP; break;
        case 28: map->filter_expr.size = 1; expected = ENOTSUP; break;
        case 29: map->opaque_data = &partition_token; expected = ENOTSUP; break;
        case 30: input.schema_ok = !input.schema_ok; break;
        case 31: input.declared_ncols++; break;
        case 32: input.name = "wrong"; break;
        case 33: worker.join_batch_bytes = 1; expected = ENOTSUP; break;
        case 34: map->delta_mode = WL_DELTA_FORCE_FULL; expected = ENOTSUP;
            break;
        case 35: input.read = read + 1; break;
        case 36: input.worker_index = 1; break;
        case 37: var->materialized = true; expected = ENOTSUP; break;
        case 38: map->materialized = true; expected = ENOTSUP; break;
        case 39: var->map_exprs = map->map_exprs; expected = ENOTSUP; break;
        case 40: map->filter_expr.data = &saved_byte; expected = ENOTSUP;
            break;
        case 41: var->project_indices = map->project_indices;
            expected = ENOTSUP; break;
        }
        /* A held writer proves each refusal precedes reader acquisition. */
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(col_rel_source_writer_acquire(assign, &writer) == 0);
        int rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, m,
                si, &snapshot_token, partition, index, width, &input, 1, &run);
        CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
        *slice = saved_slice; *read = saved_read;
        *var = saved_var; *map = saved_map;
        map->map_exprs[0] = saved_expr;
        map->map_exprs[0].data[6] = saved_byte;
        if (rc != expected) fprintf(stderr, "seed refusal %u: %d != %d\n",
                c, rc, expected);
        CHECK(rc == expected && !run.worker && !worker.tdd_input_run &&
            !worker.cleanup_active && !worker.cleanup_pending &&
            reserved(&worker) == before && evaluations == 0);
        worker.current_iteration = 0;
        worker.diff_operators_active = worker.delta_seeded = false;
        worker.retraction_seeded = worker.retraction_right_pass = false;
        worker.join_batch_bytes = 0;
    }
    uint32_t copies = 0;
    for (uint32_t s = 0; s < manifest->slice_count; s++) {
        const wl_columnar_eval_tdd_plan_slice_t *other = &manifest->slices[s];
        if (!other->seed || !other->alternative) continue;
        wl_columnar_eval_tdd_input_t input = capture(
            &manifest->reads[other->read_start], assign, 0, 1);
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(col_rel_source_writer_acquire(assign, &writer) == 0);
        CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp,
            manifest, s, &snapshot_token, NULL, 0, 1, &input, 1,
            &run) == ENOTSUP);
        CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
        CHECK(!run.worker && !worker.cleanup_active && !worker.cleanup_pending
            && reserved(&worker) == before && evaluations == 0);
        copies++;
    }
    CHECK(copies == 3 * (manifest->alternative_count - 1));
    wl_columnar_eval_test_bound_slice_after_eval = NULL;
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(worker.memory_governor);
    uint64_t limit = atomic_load_explicit(&governor->usable_bytes,
            memory_order_seq_cst);
    wl_columnar_memory_mode_t mode = governor->mode;
    governor->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    atomic_store_explicit(&governor->usable_bytes, before,
        memory_order_seq_cst);
    worker.memory_budget_denied = false;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest,
        si, &snapshot_token, NULL, 0, 1, &good, 1, &run) == ENOSPC);
    CHECK(worker.memory_budget_denied && !run.worker &&
        reserved(&worker) == before);
    atomic_store_explicit(&governor->usable_bytes, limit,
        memory_order_seq_cst);
    governor->mode = mode;
    worker.memory_budget_denied = false;
#ifdef WL_TEST_ALLOC_WRAP
    fail_calloc_after = 0;
    int rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest,
            si, &snapshot_token, NULL, 0, 1, &good, 1, &run);
    fail_calloc_after = -1;
    CHECK(rc == ENOMEM && !run.worker && !worker.memory_budget_denied &&
        reserved(&worker) == before);
#endif
    /* A retained intermediate keeps the seed input pinned; retry only cleans. */
    evaluations = 0;
    wl_columnar_eval_test_bound_slice_after_eval = pin_result_and_fail;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest,
        si, &snapshot_token, NULL, 0, 1, &good, 1, &run) == ENOMEM);
    CHECK(run.worker && worker.cleanup_pending && evaluations == 1);
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(col_rel_source_writer_acquire(assign, &writer) == EBUSY);
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, NULL) == ENOMEM &&
        run.worker && evaluations == 1);
    CHECK(col_rel_source_reader_release(&held_result) == 0);
    wl_columnar_eval_test_bound_slice_after_eval = NULL;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, NULL) == ENOMEM &&
        !run.worker && reserved(&worker) == before);
    evaluations = release_calls = 0;
    wl_columnar_eval_test_bound_slice_after_eval = count_evaluation;
    wl_columnar_eval_test_bound_slice_before_release = refuse_release_once;
    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp, manifest,
        si, &snapshot_token, NULL, 0, 1, &good, 1, &run) == 0);
    col_rel_t *saved = run.output, *out = NULL;
    CHECK(saved && saved->nrows == 5);
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out) == EBUSY &&
        !out && run.output == saved && evaluations == 1);
    CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out) == 0 &&
        out == saved && evaluations == 1);
    wl_columnar_eval_test_bound_slice_after_eval = NULL;
    wl_columnar_eval_test_bound_slice_before_release = NULL;
    CHECK(col_rel_destroy_checked(out) == 0 && reserved(&worker) == before);
    CHECK(assign->nrows == 5);
    for (uint32_t i = 0; i < 5; i++)
        CHECK(assign->columns[0][i] == seed_assign[i][0] &&
            assign->columns[1][i] == seed_assign[i][1]);
    CHECK(col_rel_destroy_checked(assign) == 0);
    CHECK(col_worker_session_destroy(&worker) == 0);
    return 0;
}

/* Independent positive bag oracle for the unexpanded memoryAlias rule.
 * Duplicate dereference matches must multiply JOIN, but not SEMIJOIN. */
static int
k1_rejections(wl_col_session_t *worker, const wl_plan_stratum_t *sp,
    wl_columnar_eval_tdd_plan_manifest_t *manifest,
    wl_columnar_eval_tdd_input_t inputs[4])
{
    uint64_t before = reserved(worker);
    wl_columnar_eval_tdd_run_t run = {0};
    for (unsigned c = 0; c < 10; c++) {
        wl_columnar_eval_tdd_plan_manifest_t wrong = *manifest;
        wl_columnar_eval_tdd_plan_slice_t slice = manifest->slices[0];
        wrong.slices = &slice;
        switch (c) {
        case 0: wrong.form = WL_COLUMNAR_EVAL_TDD_PLAN_EXPANDED; break;
        case 1: wrong.form = (wl_columnar_eval_tdd_plan_form_t)99; break;
        case 2: wrong.alternative_count = 2; break;
        case 3: slice.driver = 8; break;
        case 4: slice.count = 5; break;
        case 5: slice.start = 0; break;
        case 6: slice.read_start = 1; break;
        case 7: slice.ordinal = 1; break;
        case 8: slice.seed = true; break;
        case 9: wrong.block_size = 12; break;
        }
        CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(worker, sp, &wrong,
            0, &snapshot_token, &partition_token, 0, 1, inputs, 4, &run) != 0);
        CHECK(!run.worker && reserved(worker) == before);
    }
    for (unsigned c = 0; c < 5; c++) {
        wl_columnar_eval_tdd_plan_read_t saved = manifest->reads[1];
        if (c == 0) manifest->reads[1].op_index = 8;
        if (c == 1) manifest->reads[1].right_operand = false;
        if (c == 2) manifest->reads[1].source_index = 3;
        if (c == 3) manifest->reads[1].relation_name = "dereference";
        if (c == 4) manifest->reads[1].kind = WL_COLUMNAR_EVAL_TDD_PLAN_FULL;
        int rc = wl_columnar_eval_tdd_run_bound_slice_begin(worker, sp,
                manifest, 0, &snapshot_token, &partition_token, 0, 1,
                inputs, 4, &run);
        manifest->reads[1] = saved;
        CHECK(rc == EINVAL && !run.worker && reserved(worker) == before);
    }
    CHECK(sp->relation_count == 3);
    for (unsigned c = 0; c < 37; c++) {
        wl_plan_relation_t relations[3];
        memcpy(relations, sp->relations, sizeof(relations));
        wl_plan_op_t ops[12]; memcpy(ops, manifest->owner_ops, sizeof(ops));
        wl_plan_stratum_t altered = *sp; altered.relations = relations;
        wl_plan_relation_t *rel = &relations[manifest->relation_index];
        rel->ops = ops;
        wl_plan_op_exchange_t meta =
            *(wl_plan_op_exchange_t *)ops[11].opaque_data;
        ops[11].opaque_data = &meta;
        uint32_t projection[] = {0, 1};
        const char *null_key[] = {NULL};
        wl_plan_expr_buffer_t exprs[2];
        memcpy(exprs, ops[1].map_exprs, sizeof(exprs));
        uint8_t bad_expr[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0'};
        switch (c) {
        case 0: ops[4].op = WL_PLAN_OP_CONSOLIDATE; break;
        case 1: ops[6].delta_mode = WL_DELTA_FORCE_DELTA; break;
        case 2: ops[7].op = WL_PLAN_OP_LFTJ; break;
        case 3: ops[8].right_relation = "valueAlias"; break;
        case 4: ops[9].op = WL_PLAN_OP_CONSOLIDATE; break;
        case 5: ops[10].op = WL_PLAN_OP_CONCAT; break;
        case 6: ops[6].left_keys = NULL; break;
        case 7: ops[6].left_keys = null_key; break;
        case 8: ops[6].project_indices = projection; break;
        case 9: ops[7].right_filter_expr.size = 1; break;
        case 10: ops[5].relation_name = "valueAlias"; break;
        case 11: ops[1].map_exprs = NULL; break;
        case 12: exprs[0].data = bad_expr; ops[1].map_exprs = exprs; break;
        case 13: meta.key_col_idxs = NULL; break;
        case 14: meta.edb_rel_name = "dereference"; break;
        case 15: rel->recursive_agg.has_spec = true; break;
        case 16: ops[0].relation_name = "valueAlias"; break;
        case 29: ops[5].left_keys = null_key; break;
        case 30: ops[5].right_keys = null_key; break;
        case 31: ops[4].project_indices = projection; break;
        case 32: ops[10].group_by_indices = projection; break;
        case 33: ops[9].map_exprs = exprs; break;
        case 34: ops[11].filter_expr.data = bad_expr; break;
        case 35: ops[11].right_filter_expr.data = bad_expr; break;
        case 36: ops[7].agg_expr.data = bad_expr; break;
        default: ops[c - 17].materialized = true; break;
        }
        wl_columnar_eval_tdd_plan_manifest_t rejected = {0};
        CHECK(wl_columnar_eval_tdd_plan_bindings(&altered,
            manifest->relation_index, &rejected) != 0);
        CHECK(!rejected.slices && !rejected.reads && !rejected.owner_ops);
        wl_columnar_eval_tdd_plan_manifest_t wrong = *manifest;
        wrong.owner_stratum = &altered; wrong.owner_relation = rel;
        wrong.owner_ops = ops;
        CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(worker, &altered,
            &wrong, 0, &snapshot_token, &partition_token, 0, 1,
            inputs, 4, &run) == EINVAL);
        CHECK(!run.worker && !worker->cleanup_active &&
            reserved(worker) == before);
    }
    return 0;
}

static int
seed_cases(wl_col_session_t *coord, const wl_plan_stratum_t *sp)
{
    uint32_t ri = 0;
    while (ri < sp->relation_count && strcmp(sp->relations[ri].name,
        "valueFlow") != 0) ri++;
    CHECK(ri < sp->relation_count);
    wl_columnar_eval_tdd_plan_manifest_t manifest = {0};
    CHECK(wl_columnar_eval_tdd_plan_bindings(sp, ri, &manifest) == 0);
    uint32_t first = UINT32_MAX;
    for (uint32_t rows = 0; rows <= 5; rows += 5) {
        int64_t total[15][2], expected[15][2];
        uint32_t count = 0, shapes = 0;
        for (uint32_t si = 0; si < manifest.slice_count; si++) {
            const wl_columnar_eval_tdd_plan_slice_t *slice =
                &manifest.slices[si];
            if (!slice->seed || slice->alternative != 0) continue;
            if (first == UINT32_MAX) first = si;
            CHECK(slice->count == 2 && slice->read_count == 1);
            const wl_plan_op_t *map = &manifest.owner_ops[slice->start + 1];
            CHECK(map->op == WL_PLAN_OP_MAP && map->project_count == 2 &&
                map->project_indices && map->map_expr_count == 2);
            uint32_t a = map->project_indices[0], b = map->project_indices[1];
            CHECK((a == 0 && b <= 1) || (a == 1 && b == 1));
            uint32_t shape = a == b ? a + 1 : 0;
            CHECK(!(shapes & (1u << shape)));
            shapes |= 1u << shape;
            wl_col_session_t worker = {0};
            CHECK(col_worker_session_create(coord, 0, NULL, 0, &worker) == 0);
            col_rel_t *assign = relation("assign", seed_assign, rows, 0, 0);
            CHECK(assign);
            const wl_columnar_eval_tdd_plan_read_t *read =
                &manifest.reads[slice->read_start];
            CHECK(read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_FULL);
            wl_columnar_eval_tdd_input_t input = capture(read, assign, 0, 1);
            uint64_t before = reserved(&worker);
            wl_columnar_eval_tdd_run_t run = {0};
            CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp,
                &manifest, si, &snapshot_token, NULL, 0, 1, &input, 1,
                &run) == 0);
            wl_columnar_source_access_writer_t writer = {0};
            CHECK(col_rel_source_writer_acquire(assign, &writer) == EBUSY);
            CHECK(col_worker_session_destroy(&worker) == EBUSY);
            col_rel_t *out = NULL;
            CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out) == 0);
            CHECK(out && out->nrows == rows && out->ncols == 2 &&
                !out->pool_owned && !out->arena_owned);
            CHECK(col_rel_destroy_checked(assign) == 0);
            int64_t actual[5][2];
            for (uint32_t i = 0; i < rows; i++) {
                actual[i][0] = out->columns[0][i];
                actual[i][1] = out->columns[1][i];
                memcpy(total[count], actual[i], sizeof(actual[i]));
                memcpy(expected[count++], seed_expected[shape][i],
                    sizeof(actual[i]));
            }
            CHECK(seed_bag(actual, seed_expected[shape], rows) == 0);
            CHECK(col_rel_destroy_checked(out) == 0 &&
                reserved(&worker) == before);
            CHECK(col_worker_session_destroy(&worker) == 0);
        }
        CHECK(shapes == 7 && count == 3 * rows);
        CHECK(seed_bag(total, expected, count) == 0);
    }
    CHECK(first != UINT32_MAX && seed_refusals(coord, sp, &manifest,
        first) == 0);
    wl_columnar_eval_tdd_plan_bindings_free(&manifest);
    return 0;
}

static int
k1_cases(wl_col_session_t *coord, const wl_plan_stratum_t *sp)
{
    static const int64_t edb[][2] = {{1, 10}, {1, 11}, {2, 20}, {2, 20},
                                     {3, 30}, {8, 80}, {9, 90}};
    static const int64_t changes[][2] = {{1, 2}, {2, 3}, {8, 9}, {1, 3},
                                         {17, 2}};
    uint32_t ri = 0;
    while (ri < sp->relation_count && strcmp(sp->relations[ri].name,
        "memoryAlias")) ri++;
    CHECK(ri < sp->relation_count);
    wl_columnar_eval_tdd_plan_manifest_t manifest = {0};
#ifdef WL_TEST_ALLOC_WRAP
    for (int at = 0; at < 2; at++) {
        fail_calloc_after = at;
        int rc = wl_columnar_eval_tdd_plan_bindings(sp, ri, &manifest);
        fail_calloc_after = -1;
        CHECK(rc == ENOMEM && !manifest.reads && !manifest.slices
            && !manifest.owner_ops);
    }
#endif
    CHECK(wl_columnar_eval_tdd_plan_bindings(sp, ri, &manifest) == 0);
    CHECK(manifest.alternative_count == 1 && manifest.slice_count == 1
        && manifest.read_count == 4 && manifest.slices[0].start == 5
        && manifest.slices[0].count == 4 && manifest.slices[0].driver == 6);
    const uint32_t widths[] = {1, 2, 8};
    for (unsigned wi = 0; wi < 3; wi++) {
        unsigned coverage[5] = {0};
        for (uint32_t w = 0; w < widths[wi]; w++) {
            wl_col_session_t worker = {0};
            CHECK(col_worker_session_create(coord, w, NULL, 0, &worker) == 0);
            /* Alternate missing and conflicting registry entries. */
            const int64_t bad_rows[][2] = {{777, 888}};
            const char *bad_names[] = {"assign", "dereference", "valueAlias",
                                       "$d$valueAlias"};
            for (unsigned i = 0; i < (w % 2 ? 1u : 4u); i++) {
                col_rel_t *bad = relation(bad_names[i], bad_rows, 1, 0, 0);
                CHECK(bad && session_add_rel(&worker, bad) == 0);
            }
            col_rel_t *full = relation("dereference", edb, 7, 0, 0);
            col_rel_t *delta = relation("alias-partition", changes, 5,
                    w, widths[wi]);
            CHECK(full && delta);
            int64_t expected[245][2]; unsigned used[245] = {0};
            uint32_t n = 0;
            for (uint32_t row = 0; row < delta->nrows; row++) {
                unsigned d = 0;
                while (d < 5 && (delta->columns[0][row] != changes[d][0]
                    || delta->columns[1][row] != changes[d][1])) d++;
                CHECK(d < 5 && (uint64_t)changes[d][0] % widths[wi] == w);
                coverage[d]++;
            }
            for (unsigned d = 0; d < 5; d++) {
                if ((uint64_t)changes[d][0] % widths[wi] != w) continue;
                for (unsigned a = 0; a < 7; a++)
                    for (unsigned b = 0; b < 7; b++)
                        if (edb[a][0] == changes[d][0]
                            && edb[b][0] == changes[d][1]) {
                            expected[n][0] = edb[a][1];
                            expected[n++][1] = edb[b][1];
                        }
            }
            wl_columnar_eval_tdd_input_t inputs[4];
            for (unsigned i = 0; i < 4; i++)
                inputs[i] = capture(&manifest.reads[i],
                        i == 1 ? delta : full, w, widths[wi]);
            uint64_t before = reserved(&worker);
            wl_columnar_eval_tdd_run_t run = {0};
            if (wi == 0) {
                CHECK(k1_rejections(&worker, sp, &manifest, inputs) == 0);
                for (unsigned i = 0; i < 4; i++) {
                    inputs[i].view_generation++;
                    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker,
                        sp, &manifest, 0, &snapshot_token, &partition_token,
                        w, widths[wi], inputs, 4, &run) == EINVAL);
                    inputs[i].view_generation--;
                    inputs[i].ncols++;
                    CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker,
                        sp, &manifest, 0, &snapshot_token, &partition_token,
                        w, widths[wi], inputs, 4, &run) == EINVAL);
                    inputs[i].ncols--;
                }
                col_rel_t *other_full = relation("dereference", edb, 7, 0, 0);
                CHECK(other_full);
                inputs[2] = capture(&manifest.reads[2], other_full, w,
                        widths[wi]);
                CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker,
                    sp, &manifest, 0, &snapshot_token, &partition_token,
                    w, widths[wi], inputs, 4, &run) == EINVAL);
                inputs[2] = capture(&manifest.reads[2], full, w, widths[wi]);
                col_rel_destroy(other_full);
                wl_columnar_memory_governor_t *g =
                    wl_columnar_memory_governor_ref_get(worker.memory_governor);
                uint64_t limit = atomic_load_explicit(&g->usable_bytes,
                        memory_order_seq_cst);
                wl_columnar_memory_mode_t mode = g->mode;
                g->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
                atomic_store_explicit(&g->usable_bytes, before,
                    memory_order_seq_cst);
                worker.memory_budget_denied = false;
                int rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker,
                        sp, &manifest, 0, &snapshot_token, &partition_token,
                        w, widths[wi], inputs, 4, &run);
                atomic_store_explicit(&g->usable_bytes, limit,
                    memory_order_seq_cst);
                g->mode = mode;
                CHECK(rc == ENOSPC && worker.memory_budget_denied
                    && !run.worker && reserved(&worker) == before);
                worker.memory_budget_denied = false;
#ifdef WL_TEST_ALLOC_WRAP
                fail_calloc_after = 0;
                rc = wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp,
                        &manifest, 0, &snapshot_token, &partition_token,
                        w, widths[wi], inputs, 4, &run);
                fail_calloc_after = -1;
                CHECK(rc == ENOMEM && !run.worker &&
                    !worker.memory_budget_denied
                    && reserved(&worker) == before);
#endif
                evaluations = 0;
                wl_columnar_eval_test_bound_slice_after_eval =
                    pin_result_and_fail;
                CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker,
                    sp, &manifest, 0, &snapshot_token, &partition_token,
                    w, widths[wi], inputs, 4, &run) == ENOMEM);
                CHECK(run.worker && worker.cleanup_pending && held_result.owner
                    && evaluations == 1);
                CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run,
                    NULL) == ENOMEM);
                CHECK(run.worker && evaluations == 1);
                CHECK(col_rel_source_reader_release(&held_result) == 0);
                wl_columnar_eval_test_bound_slice_after_eval = NULL;
                CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run,
                    NULL) == ENOMEM);
                CHECK(!run.worker && !worker.tdd_input_run &&
                    reserved(&worker) == before);
            }
            evaluations = release_calls = 0;
            wl_columnar_eval_test_bound_slice_after_eval = count_evaluation;
            wl_columnar_eval_test_bound_slice_before_release =
                refuse_release_once;
            CHECK(wl_columnar_eval_tdd_run_bound_slice_begin(&worker, sp,
                &manifest, 0, &snapshot_token, &partition_token,
                w, widths[wi], inputs, 4, &run) == 0);
            CHECK(col_worker_session_destroy(&worker) == EBUSY);
            for (unsigned i = 0; i < 4; i++) {
                wl_columnar_source_access_writer_t writer = {0};
                CHECK(col_rel_source_writer_acquire(inputs[i].relation,
                    &writer) == EBUSY);
            }
            col_rel_t *out = NULL, *saved = run.output;
            CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run,
                &out) == EBUSY);
            CHECK(!out && run.output == saved && evaluations == 1);
            CHECK(wl_columnar_eval_tdd_run_bound_slice_finish(&run, &out) == 0);
            wl_columnar_eval_test_bound_slice_after_eval = NULL;
            wl_columnar_eval_test_bound_slice_before_release = NULL;
            CHECK(out == saved && evaluations == 1 && out->ncols == 2
                && out->nrows == n);
            for (unsigned i = 0; i < 4; i++)
                CHECK(inputs[i].relation->view_generation ==
                    inputs[i].view_generation
                    && inputs[i].relation->storage_generation ==
                    inputs[i].storage_generation);
            for (unsigned i = 0; i < 7; i++)
                CHECK(full->columns[0][i] == edb[i][0]
                    && full->columns[1][i] == edb[i][1]);
            CHECK(col_rel_destroy_checked(full) == 0
                && col_rel_destroy_checked(delta) == 0);
            for (uint32_t r = 0; r < out->nrows; r++) {
                uint32_t j = 0;
                while (j < n && (used[j] || expected[j][0] != out->columns[0][r]
                    || expected[j][1] != out->columns[1][r])) j++;
                CHECK(j < n); used[j]++;
            }
            CHECK(col_rel_destroy_checked(out) == 0
                && reserved(&worker) == before);
            CHECK(col_worker_session_destroy(&worker) == 0);
        }
        for (unsigned d = 0; d < 5; d++) CHECK(coverage[d] == 1);
    }
    wl_columnar_eval_tdd_plan_bindings_free(&manifest);
    return 0;
}

int main(void)
{
    const char *path = getenv("WIRELOG_TEST_CSPA_PLAN");
    CHECK(path);
    FILE *file = fopen(path, "r"); CHECK(file);
    char source[8192]; size_t count = fread(source, 1, sizeof(source)-1, file);
    CHECK(!ferror(file) && feof(file)); fclose(file); source[count] = '\0';
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_fusion_apply(program, NULL); wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    CHECK(plan->stratum_count == 1);
    const wl_plan_stratum_t *sp = &plan->strata[0];
    uint32_t ri = 0;
    while (ri < sp->relation_count && strcmp(sp->relations[ri].name,
        "valueAlias") != 0) ri++;
    CHECK(ri < sp->relation_count);
    wl_columnar_eval_tdd_plan_manifest_t manifest = {0};
    CHECK(wl_columnar_eval_tdd_plan_bindings(sp, ri, &manifest) == 0);
    wl_session_t *session = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &session) == 0);
    wl_col_session_t *coord = COL_SESSION(session);
    const uint32_t widths[] = {1, 2, 8};
    uint32_t exercised = 0;
    for (uint32_t s = 0; s < manifest.slice_count; s++) {
        const wl_columnar_eval_tdd_plan_slice_t *slice = &manifest.slices[s];
        if (slice->seed || slice->inactive) continue;
        for (uint32_t wi = 0; wi < 3; wi++) {
            unsigned seen[4] = {0};
            for (uint32_t w = 0; w < widths[wi]; w++)
                CHECK(run_one(coord, sp, &manifest, s, w, widths[wi],
                    seen) == 0);
            for (uint32_t i = 0; i < (slice->ordinal == 0 ? 3u : 2u);
                i++) CHECK(seen[i] == 1);
        }
        exercised++;
    }
    CHECK(exercised == 5);
    CHECK(negative_cases(coord, sp, &manifest) == 0);
    CHECK(synthetic_cases(coord) == 0);
    CHECK(seed_cases(coord, sp) == 0);
    CHECK(k1_cases(coord, sp) == 0);
    wl_session_destroy(session);
    wl_columnar_eval_tdd_plan_bindings_free(&manifest);
    wl_plan_free(plan); wirelog_program_free(program);
    puts("TDD bound execution: five actual CSPA alternatives, W=1/2/8 PASS");
    return 0;
}
