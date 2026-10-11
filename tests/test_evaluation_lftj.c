/* #1821: genuine first-op LFTJ callers and original cleanup authorities. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include <stdio.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); exit(1); } } while (0)
static const char *program_source =
    ".decl r1(x:int32,a:int32)\n.decl r2(x:int32,b:int32)\n"
    ".decl r3(x:int32,c:int32)\n.decl out(x:int32,a:int32,b:int32,c:int32)\n"
    "r1(2,20).r1(1,10).r2(1,100).r2(4,400).r3(3,3000).r3(1,1000).\n"
    "out(x,a,b,c):-r1(x,a),r2(x,b),r3(x,c).\n";
static void destroy(wl_col_session_t *session)
{
    wl_evaluation_control_t *control = session->base.evaluation_control;
    if (control) {
        CHECK(wl_evaluation_control_detach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = NULL;
        wl_evaluation_control_release(control);
    }
    wl_session_destroy(&session->base);
}
static unsigned fallback_copies, fallback_kernel_visits;
static void fallback_checkpoint(wl_columnar_lftj_record_t *record,
    int stage, uint64_t progress)
{
    if (stage == 12 && progress) {
        CHECK(record->inputs[record->input_count - 1u].data
            && !record->cache_backed[record->input_count - 1u]);
        fallback_copies++;
    }
    if (stage == 9 && record->kernel_visits) fallback_kernel_visits++;
}
static void direct_cap0(bool enforcing, unsigned fallback)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(program_source, &error);
    CHECK(program && error == WIRELOG_OK);
    wl_fusion_apply(program, NULL);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    CHECK(plan->stratum_count == 1 && !plan->strata[0].is_recursive
        && plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *rule = &plan->strata[0].relations[0];
    CHECK(rule->op_count == 3 && rule->ops[0].op == WL_PLAN_OP_LFTJ
        && rule->ops[1].op == WL_PLAN_OP_MAP
        && rule->ops[2].op == WL_PLAN_OP_CONSOLIDATE);
    const wl_plan_op_lftj_t *meta = rule->ops[0].opaque_data;
    CHECK(meta && meta->k == 3);
    for (unsigned controlled = 0; controlled < 2; controlled++) {
        wl_session_t *base = NULL;
        const uint64_t budget = UINT64_C(129) * 1024u * 1024u;
        if (enforcing) {
            wl_columnar_memory_resolution_t resolution = {
                .budget_bytes = budget, .usable_bytes = budget,
                .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
            };
            wl_columnar_memory_governor_ref_t *governor =
                wl_columnar_memory_governor_ref_create(&resolution);
            CHECK(governor);
            wl_session_options_t options; wl_session_options_init(&options);
            options.memory_governor = governor;
            CHECK(wl_session_create_with_options(wl_backend_columnar(), plan,
                1, &options, &base) == 0);
            wl_columnar_memory_governor_ref_release(governor);
        } else CHECK(wl_session_create(wl_backend_columnar(), plan, 1,
                &base) == 0);
        CHECK(wl_session_load_facts(base, program) == 0);
        wl_col_session_t *session = COL_SESSION(base);
        CHECK(session->delta_pool && session->eval_arena
            && !session_find_rel(session, "out"));
        col_sorted_arrangement_probe_t external = {0};
        if (fallback == 3) {
            CHECK(col_session_acquire_sorted_arrangement_probe(base,
                session_find_rel(session, "r1"), 0, &external) == 0);
            CHECK(external.active && external.arr->pin_count == 1);
            col_session_invalidate_arrangements(base, "r1");
            CHECK(external.arr->rebuild_deferred && external.arr->sorted);
        }
        if (fallback == 4) {
            CHECK(col_session_get_sorted_arrangement(session, "r1", 0)
                && col_session_get_sorted_arrangement(session, "r2", 0)
                && col_session_get_sorted_arrangement(session, "r3", 0));
        }
        if (fallback == 2) {
            /* Four real published entries fill the raw initial registry.
             * r1(key0) is pinned before r2(key0) needs relocation; actual
             * EBUSY selects owned fallback without changing source bytes. */
            CHECK(col_session_get_sorted_arrangement(session, "r1", 0)
                && col_session_get_sorted_arrangement(session, "r1", 1)
                && col_session_get_sorted_arrangement(session, "r2", 1)
                && col_session_get_sorted_arrangement(session, "r3", 1));
            CHECK(session->sarr_count == 4 && session->sarr_cap == 4);
        }
        if (fallback ==
            1) wl_columnar_arrangement_sorted_test_fail_next_allocation();
        fallback_copies = fallback_kernel_visits = 0;
        if (controlled) wl_columnar_lftj_test_checkpoint = fallback_checkpoint;
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(session, &frame) == 0);
        wl_evaluation_control_t *control = NULL;
        if (controlled) {
            CHECK(wl_evaluation_control_create(0, &control) == 0);
            CHECK(wl_evaluation_control_attach(control,
                session->base.operation_admission) == 0);
            session->base.evaluation_control = control;
            CHECK(wl_evaluation_control_begin(control,
                session->base.operation_admission) == 0);
        }
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        int rc = col_op_lftj(&rule->ops[0], stack, session);
        if (rc) fprintf(stderr, "direct LFTJ controlled=%u rc=%d\n", controlled,
                rc);
        CHECK(rc == 0 && stack->top == 1 && stack->items[0].owned);
        col_rel_t *out = stack->items[0].rel;
        CHECK(out && out->ncols == 6 && out->nrows == 1
            && !out->column_types && out->capacity == COL_REL_INIT_CAP);
        int64_t expected[] = {1, 10, 1, 100, 1, 1000};
        for (uint32_t col = 0; col < 6; col++)
            CHECK(col_rel_get(out, 0, col) == expected[col]);
        CHECK(session->sarr_count ==
            (fallback == 2 ? 4u : fallback == 1 ? 2u : 3u)
            && session->sarr_active_pins == (fallback == 3 ? 1u : 0u));
        if (fallback == 1) {
            CHECK(
                wl_columnar_arrangement_sorted_test_allocation_failure_consumed());
            wl_columnar_arrangement_sorted_test_clear_allocation_failure();
        }
        if (controlled) {
            CHECK(fallback_kernel_visits);
            CHECK(fallback &&
                fallback != 4 ? fallback_copies > 0 : fallback_copies == 0);
            wl_columnar_lftj_test_checkpoint = NULL;
        }
        if (enforcing) {
            wl_columnar_memory_governor_t *governor =
                wl_columnar_memory_governor_ref_get(session->memory_governor);
            CHECK(governor->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
                && governor->budget_bytes == budget
                && wl_columnar_memory_reserved(governor) <= budget);
        }
        if (fallback == 3) {
            CHECK(external.active && external.arr->pin_count == 1
                && external.arr->rebuild_deferred);
            CHECK(col_sorted_arrangement_probe_release(&external) == 0
                && !external.active && !session->sarr_active_pins);
        }
        for (uint32_t i = 0; i < session->sarr_count;
            i++) CHECK(!session->sarr_entries[i].sarr.pin_count);
        if (controlled) CHECK(wl_evaluation_control_finish(control,
                session->base.operation_admission, 0, 0) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
        CHECK(!session->cleanup_pending && !session->cleanup_active
            && session->cleanup_reserved_bytes == 0);
        destroy(session);
    }
    wl_plan_free(plan); wirelog_program_free(program);
}

typedef struct wl_lftj_capture {
    uint64_t count, digest;
    uint32_t width;
} wl_lftj_capture_t;
static void capture(const char *name, const int64_t *row, uint32_t width,
    void *data)
{
    wl_lftj_capture_t *c = data;
    CHECK(name && strcmp(name, "out") == 0 && row && width == c->width);
    for (uint32_t i = 0; i < width; i++)
        c->digest = c->digest * UINT64_C(1099511628211) ^ (uint64_t)row[i];
    c->count++;
}
static unsigned generated_gathers;
static void cache_checkpoint(wl_columnar_lftj_record_t *record, int stage,
    uint64_t progress)
{
    if (stage == 3 && progress) {
        CHECK(record->cache_fresh.sorted && record->cache_ledger_installed);
        generated_gathers++;
    }
}
static wl_session_t *generated_session(const wl_plan_t *plan, bool enforcing)
{
    wl_session_t *base = NULL;
    if (enforcing) {
        const uint64_t budget = UINT64_C(129)*1024u*1024u;
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = budget, .usable_bytes = budget,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
        };
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor);
        wl_session_options_t options; wl_session_options_init(&options);
        options.memory_governor = governor;
        CHECK(wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &base) == 0);
        wl_columnar_memory_governor_ref_release(governor);
    } else CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    return base;
}
static void generated(bool four, bool snapshot, bool enforcing)
{
    const char *four_source =
        ".decl r1(x:int32,a:int32)\n.decl r2(x:int32,b:int32)\n"
        ".decl r3(x:int32,c:int32)\n.decl r4(x:int32,d:int32)\n"
        ".decl out(x:int32,a:int32,b:int32,c:int32,d:int32)\n"
        "r1(2,20).r1(1,10).r2(1,100).r2(4,400).r3(3,3000).r3(1,1000)."
        "r4(1,10000).r4(5,50000).\n"
        "out(x,a,b,c,d):-r1(x,a),r2(x,b),r3(x,c),r4(x,d).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(four ? four_source :
            program_source, &error);
    CHECK(program && error == WIRELOG_OK);
    wl_fusion_apply(program, NULL);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    CHECK(plan->stratum_count == 1 && !plan->strata[0].is_recursive
        && plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *rule = &plan->strata[0].relations[0];
    CHECK(rule->op_count == 3 && rule->ops[0].op == WL_PLAN_OP_LFTJ
        && rule->ops[1].op == WL_PLAN_OP_MAP
        && rule->ops[2].op == WL_PLAN_OP_CONSOLIDATE);
    const wl_plan_op_lftj_t *meta = rule->ops[0].opaque_data;
    CHECK(meta && meta->k == (four ? 4u : 3u));
    for (uint32_t i = 0; i < 3; i++)
        CHECK(rule->ops[i].delta_mode == WL_DELTA_AUTO);
    wl_session_t *base = generated_session(plan, enforcing),
        *raw_base = generated_session(plan, enforcing);
    CHECK(wl_session_load_facts(base, program) == 0
        && wl_session_load_facts(raw_base, program) == 0);
    wl_col_session_t *session = COL_SESSION(base), *raw = COL_SESSION(raw_base);
    CHECK(session->eval_arena && session->delta_pool
        && !session_find_rel(session, "out"));
    wl_arena_t *arena = session->eval_arena;
    delta_pool_t *pool = session->delta_pool;
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0);
    CHECK(wl_evaluation_control_attach(control,
        base->operation_admission) == 0);
    base->evaluation_control = control;
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        wl_lftj_capture_t got = {.width = four ? 5u : 4u},
                twin = {.width = got.width}, expected = {.width = got.width};
        generated_gathers = 0;
        wl_columnar_lftj_test_checkpoint = cache_checkpoint;
        int rc = snapshot ? wl_backend_columnar()->session_snapshot(base,
                capture, &got) : wl_backend_columnar()->session_step(base);
        if (rc) fprintf(stderr,
                "generated LFTJ four=%u snapshot=%u attempt=%u rc=%d\n",
                four, snapshot, attempt, rc);
        CHECK(rc == 0);
        wl_columnar_lftj_test_checkpoint = NULL;
        if (generated_gathers !=
            (attempt ? (four ? 18u : 14u) : four ? 16u : 12u)) fprintf(stderr,
                "LFTJ generated cache four=%u snapshot=%u attempt=%u gathers=%u\n",
                four, snapshot, attempt, generated_gathers);
        CHECK(generated_gathers ==
            (attempt ? (four ? 18u : 14u) : four ? 16u : 12u));
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(control, &report) == 0
            && report.result == 0 && report.execution_status == 0 &&
            report.consumed);
        CHECK((snapshot ? wl_backend_columnar()->session_snapshot(raw_base,
            capture,
            &twin) : wl_backend_columnar()->session_step(raw_base)) == 0);
        if (!snapshot) {
            CHECK(wl_backend_columnar()->session_snapshot(base, capture,
                &got) == 0);
            CHECK(wl_backend_columnar()->session_snapshot(raw_base, capture,
                &twin) == 0);
        }
        int64_t row[] = {1, 10, 100, 1000, 10000};
        capture("out", row, got.width, &expected);
        if (attempt) {
            row[got.width - 1u]++;
            capture("out", row, got.width, &expected);
        }
        CHECK(got.count == expected.count && got.digest == expected.digest
            && got.count == twin.count && got.digest == twin.digest);
        col_rel_t *out = session_find_rel(session, "out");
        CHECK(out && out->ncols == got.width && out->nrows == got.count
            && !out->column_types);
        CHECK(session->eval_arena == arena && session->delta_pool == pool
            && !session->cleanup_pending && !session->cleanup_active
            && !session->serial_completion.frame && !session->sarr_active_pins);
        if (enforcing) {
            wl_columnar_memory_governor_t *governor =
                wl_columnar_memory_governor_ref_get(session->memory_governor),
                *raw_governor =
                wl_columnar_memory_governor_ref_get(raw->memory_governor);
            CHECK(governor->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
                && raw_governor->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
                && governor->budget_bytes == UINT64_C(129)*1024u*1024u
                && raw_governor->budget_bytes == governor->budget_bytes
                && wl_columnar_memory_reserved(governor) <=
                governor->budget_bytes
                && wl_columnar_memory_reserved(raw_governor) <=
                raw_governor->budget_bytes);
        }
        CHECK(session->sarr_count == meta->k);
        for (uint32_t i = 0; i < session->sarr_count; i++)
            CHECK(!session->sarr_entries[i].sarr.pin_count);
        if (!attempt) {
            /* Real phase2 compaction follows LFTJ use and changes every EDB
             * payload generation on this first call. These retained caches
             * therefore rebuild on dirty retry; the separately warmed direct
             * fixture proves actual cache hits without compaction fiction. */
            for (uint32_t i = 0; i < session->sarr_count; i++) {
                col_sorted_arr_entry_t *entry = &session->sarr_entries[i];
                col_rel_t *source = session_find_rel(session, entry->rel_name);
                CHECK(source && source->capacity == COL_REL_INIT_CAP
                    && !wl_columnar_relation_snapshot_equal(
                        entry->sarr.source_snapshot,
                        wl_columnar_relation_snapshot(source)));
            }
            int64_t dirty[] = {1, four ? 10001 : 1001};
            CHECK(wl_backend_columnar()->session_insert(base,
                four ? "r4" : "r3",
                dirty, 1, 2) == 0);
            CHECK(wl_backend_columnar()->session_insert(raw_base,
                four ? "r4" : "r3",
                dirty, 1, 2) == 0);
            CHECK(wl_evaluation_control_reset(control) == 0);
        }
    }
    destroy(session); destroy(raw); wl_plan_free(plan);
    wirelog_program_free(program);
}

static int cancellation_stage;
static unsigned cancellation_visits;
static bool cancellation_hit;
static uint64_t cancelled_products;
static bool sort_payload_seen;
static void cancel_checkpoint(wl_columnar_lftj_record_t *record,
    int stage, uint64_t progress)
{
    (void)progress;
    if (stage != cancellation_stage || cancellation_hit) return;
    if (stage == 3 && !progress) return;
    cancellation_visits++;
    if (stage == 9 ? record->kernel_visits != 257u : cancellation_visits !=
        257u) return;
    cancelled_products = record->kernel_visits;
    if (stage == 5) {
        CHECK(record->radix_temp && record->sort_bytes);
        sort_payload_seen = true;
    }
    CHECK(record && record->ready && record->session && record->control
        && record->outer.identity
        && atomic_load_explicit(&record->outer.state, memory_order_acquire)
        == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
    if (stage >= 6 && stage <= 10)
        CHECK(record->iters && record->iter_count
            && atomic_load_explicit(&record->inner.state, memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
    if (stage == 12) CHECK(record->inputs[0].data && !record->cache_backed[0]);
    cancellation_hit = true;
    wl_evaluation_control_request_cancel(record->control);
}
static void generated_stop(int stage)
{
    wirelog_error_t error;
    const char *disjoint_source =
        ".decl r1(x:int64,a:int64)\n.decl r2(x:int64,b:int64)\n"
        ".decl r3(x:int64,c:int64)\n.decl out(x:int64,a:int64,b:int64,c:int64)\n"
        "out(x,a,b,c):-r1(x,a),r2(x,b),r3(x,c).\n";
    wirelog_program_t *program = wirelog_parse_string(stage == 13 ?
            disjoint_source : program_source, &error);
    CHECK(program && error == WIRELOG_OK);
    wl_fusion_apply(program, NULL);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    CHECK(plan->strata[0].relations[0].ops[0].op == WL_PLAN_OP_LFTJ);
    wl_session_t *base = NULL, *raw_base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
        && wl_session_create(wl_backend_columnar(), plan, 1, &raw_base) == 0);
    CHECK(wl_session_load_facts(base, program) == 0
        && wl_session_load_facts(raw_base, program) == 0);
    wl_col_session_t *session = COL_SESSION(base), *raw = COL_SESSION(raw_base);
    int64_t data[300][2];
    const unsigned rows = stage == 9 ? 7u : 300u;
    const char *names[] = {"r1", "r2", "r3"};
    for (unsigned i = 0; i < 3; i++) {
        for (unsigned row = 0; row < rows; row++) {
            data[row][0] = stage == 9 ? 10 : stage ==
                13 ? row * 3u + i + 10u : row + 10u;
            data[row][1] = (row + 10) * (int64_t)(i + 2u);
        }
        CHECK(wl_backend_columnar()->session_insert(base, names[i], &data[0][0],
            rows, 2) == 0
            && wl_backend_columnar()->session_insert(raw_base, names[i],
            &data[0][0], rows, 2) == 0);
    }
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0
        && wl_evaluation_control_attach(control,
        base->operation_admission) == 0);
    base->evaluation_control = control;
    cancellation_stage = stage == 13 ? 7 : stage; cancellation_visits = 0;
    cancellation_hit = false; cancelled_products = 0;
    wl_columnar_lftj_test_checkpoint = cancel_checkpoint;
    if (stage == 12) wl_columnar_arrangement_sorted_test_fail_next_allocation();
    wl_lftj_capture_t got = {.width = 4}, twin = {.width = 4};
    int rc = wl_backend_columnar()->session_snapshot(base, capture, &got);
    if (!cancellation_hit || rc >= 0) fprintf(stderr,
            "LFTJ stage=%d visits=%u hit=%u rc=%d\n", stage,
            cancellation_visits, cancellation_hit, rc);
    CHECK(cancellation_hit && cancellation_visits >= 257u && rc < 0 &&
        !got.count);
    CHECK(stage == 9 ? cancelled_products == 257u :
        cancellation_visits == 257u);
    if (stage == 13) CHECK(!cancelled_products);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(control, &report) == 0
        && report.result == rc && report.execution_status == 0);
    if (stage == 12) {
        CHECK(
            wl_columnar_arrangement_sorted_test_allocation_failure_consumed());
        wl_columnar_arrangement_sorted_test_clear_allocation_failure();
    }
    wl_columnar_lftj_test_checkpoint = NULL;
    /* The ordinary caller keeps its actually retired completion frame for
     * canonical retry. Its original admission remains live; LFTJ payload and
     * all source/cache authorities have already drained. */
    CHECK(!session->cleanup_active && !session->cleanup_pending
        && session->serial_completion.frame && session->cleanup_reserved_bytes
        && !session->sarr_active_pins);
    for (uint32_t i = 0; i < session->sarr_count; i++)
        CHECK(!session->sarr_entries[i].sarr.pin_count);
    CHECK(wl_evaluation_control_reset(control) == 0);
    CHECK(wl_backend_columnar()->session_snapshot(base, capture, &got) == 0
        && wl_backend_columnar()->session_snapshot(raw_base, capture,
        &twin) == 0);
    CHECK(got.count == (stage == 9 ? 344u : stage == 13 ? 0u : 301u)
        && got.count == twin.count && got.digest == twin.digest);
    CHECK(!session->cleanup_active && !session->cleanup_pending
        && !session->serial_completion.frame && !session->sarr_active_pins);
    destroy(session); destroy(raw); wl_plan_free(plan);
    wirelog_program_free(program);
}

static void exact_trial(unsigned boundary)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl r1(x:int64)\n.decl r2(x:int64)\n.decl out(x:int64)\n"
        "out(x):-r1(x),r2(x).\n", &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    int64_t key = 1;
    CHECK(wl_backend_columnar()->session_insert(base, "r1", &key, 1, 1) == 0
        && wl_backend_columnar()->session_insert(base, "r2", &key, 1, 1) == 0);
    wl_col_session_t *session = COL_SESSION(base);
    CHECK(session->nrels == 2 && !strcmp(session->rels[0]->name, "r1")
        && !strcmp(session->rels[1]->name, "r2"));
    for (uint32_t i = 0; i < 2; i++) {
        CHECK(session->rels[i]->ncols == 1 && session->rels[i]->nrows == 1
            && !session->rels[i]->column_types);
        CHECK(col_session_get_sorted_arrangement(session, i ? "r2" : "r1", 0));
    }
    CHECK(session->sarr_count == 2 && !session->sarr_active_pins);
    char *names[] = {"r1", "r2"}; uint32_t keys[] = {0, 0};
    wl_plan_op_lftj_t meta = {.k = 2, .rel_names = names, .key_cols = keys};
    const wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ, .delta_mode = WL_DELTA_AUTO,
                             .opaque_data = &meta};
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(session, &frame) == 0);
    const uint64_t W = sizeof(wl_columnar_consolidate_record_t);
    CHECK(W == 552u);
    const uint64_t S = sizeof(wl_columnar_relation_mutation_single_t);
    const uint64_t L = 585u + S + 8u * ((S + 127u)/128u);
    const uint64_t P = sizeof(col_sorted_arrangement_probe_t);
    const uint64_t Z = 2u * (sizeof(wl_lftj_input_t) + P + sizeof(col_rel_t *)
        + sizeof(wl_columnar_source_access_reader_t) + sizeof(bool));
    const uint64_t T = wl_columnar_lftj_test_iterator_size();
    const uint64_t cutoff = 2607u + W + Z + 2u * L + 2u * T;
    const uint64_t full = cutoff + 618u + 2u * P;
    uint64_t allowance = boundary == 0 ? 0 : boundary < 3 ? full :
        boundary == 3 ? full - 1u : boundary == 4 ? cutoff - 1u : cutoff;
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(allowance, &control) == 0
        && wl_evaluation_control_attach(control,
        base->operation_admission) == 0);
    base->evaluation_control = control;
    CHECK(wl_evaluation_control_begin(control, base->operation_admission) == 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    int rc = col_op_lftj(&op, stack, session);
    int expected = boundary < 3 ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
    if (rc != expected) fprintf(stderr,
            "LFTJ exact boundary=%u rc=%d expected=%d\n",
            boundary, rc, expected);
    CHECK(rc == expected);
    CHECK(wl_evaluation_control_finish(control, base->operation_admission, 0,
        0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(control,
        &report) == 0 && report.result == rc);
    uint64_t consumed = boundary < 3 ? full : boundary == 3 ? full - 64u :
        boundary == 4 ? cutoff - 173u : cutoff;
    if (report.consumed != consumed) fprintf(stderr,
            "LFTJ exact boundary=%u work=%llu expected=%llu cutoff=%llu full=%llu\n",
            boundary, (unsigned long long)report.consumed,
            (unsigned long long)consumed,
            (unsigned long long)cutoff, (unsigned long long)full);
    CHECK(report.consumed == consumed);
    col_rel_t *out = boundary < 3 ? stack->items[0].rel :
        wl_columnar_eval_stack_cleanup_result(frame)->rel;
    CHECK(out && out->ncols == 2 && out->capacity == 64u
        && out->nrows == (boundary == 4 ? 0u : 1u));
    if (out->nrows) CHECK(out->columns[0][0] == 1 && out->columns[1][0] == 1);
    wl_columnar_lftj_record_t *record =
        wl_columnar_eval_stack_cleanup_lftj(frame);
    CHECK(record);
    if (boundary == 3) CHECK(!record->iters && !record->inputs
            && record->outer.identity == &record->outer
            && record->inner.identity == &record->inner
            && atomic_load_explicit(&record->outer.state, memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
            && atomic_load_explicit(&record->inner.state, memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
            && !session->sarr_active_pins && stack->top == 0);
    if (boundary >= 4) CHECK(record->iters && record->inputs
            && session->sarr_active_pins == 2 && stack->top == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
        && !session->cleanup_pending && !session->cleanup_active
        && !session->cleanup_reserved_bytes && !session->sarr_active_pins);
    CHECK(wl_evaluation_control_reset(control) == 0
        && wl_evaluation_control_configure(control, 0) == 0);
    /* A real fresh invocation follows disposal of the original result and
     * scratch owners. Raw twin uses the same immutable supplied operator. */
    for (unsigned controlled = 0; controlled < 2; controlled++) {
        base->evaluation_control = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(session, &frame) == 0);
        base->evaluation_control = controlled ? control : NULL;
        if (controlled) CHECK(wl_evaluation_control_begin(control,
                base->operation_admission) == 0);
        stack = wl_columnar_eval_stack_cleanup_stack(frame);
        CHECK(col_op_lftj(&op, stack, session) == 0 && stack->top == 1);
        out = stack->items[0].rel;
        CHECK(out->ncols == 2 && out->nrows == 1 && out->columns[0][0] == 1
            && out->columns[1][0] == 1);
        if (controlled) CHECK(wl_evaluation_control_finish(control,
                base->operation_admission, 0, 0) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    }
    base->evaluation_control = control;
    destroy(session); wl_plan_free(plan); wirelog_program_free(program);
}

static unsigned authority_mode;
static bool authority_hit;
static wl_columnar_lftj_record_t *authority_record;
static uintptr_t descriptor_identity;
static const void *credit_identity;
static void authority_checkpoint(wl_columnar_lftj_record_t *record,
    int stage, uint64_t progress)
{
    (void)progress;
    if (authority_hit) return;
    bool selected = authority_mode == 4 || authority_mode == 8 ? stage == 1 &&
        record->inputs :
        authority_mode == 5 || authority_mode == 7 ? stage == 9 &&
        record->iters :
        authority_mode == 3 || authority_mode == 6 ? stage == 10 &&
        record->binary_row : false;
    if (!selected) return;
    authority_record = record; authority_hit = true;
    if (authority_mode == 3) {
        CHECK(record->probes[0].source_reader.owner &&
            record->readers[0].owner);
        descriptor_identity = record->probes[0].source_reader.identity;
        record->probes[0].source_reader.identity ^= 1u;
    } else {
        wl_columnar_memory_reservation_t *token = authority_mode == 5 ||
            authority_mode == 7 ?
            &record->inner : &record->outer;
        CHECK(token->identity == token);
        credit_identity = token->identity; token->identity = NULL;
    }
    if (authority_mode == 7 || authority_mode == 8)
        CHECK(wl_evaluation_control_charge(record->control,
            UINT64_MAX) == EOVERFLOW);
    else if (authority_mode == 4 || authority_mode == 5)
        wl_evaluation_control_request_cancel(record->control);
}
static void release_checkpoint(wl_evaluation_control_t *control,
    wl_columnar_source_access_reader_t *token,
    wl_columnar_source_access_gate_t *gate)
{
    (void)gate;
    if (authority_hit || authority_mode > 2) return;
    wl_columnar_lftj_record_t *record = wl_columnar_eval_stack_cleanup_lftj(
        authority_record->session->cleanup_active);
    CHECK(record == authority_record);
    wl_columnar_source_access_reader_t *selected = authority_mode == 1 ?
        &record->probes[0].source_reader : &record->readers[0];
    if (token != selected) return;
    CHECK(!token->owner && !token->identity && record->probes[0].active
        && record->probes[0].arr->pin_count == 1);
    if (authority_mode != 1) CHECK(record->probes[0].source_reader.owner);
    else CHECK(!record->readers[0].owner);
    authority_hit = true;
    if (authority_mode == 2) {
        descriptor_identity = record->probes[0].source_reader.identity;
        record->probes[0].source_reader.identity ^= 1u;
    }
    wl_evaluation_control_request_cancel(control);
}
static void observe_record(wl_columnar_lftj_record_t *record, int stage,
    uint64_t progress)
{
    if (authority_mode <= 2) authority_record = record;
    else authority_checkpoint(record, stage, progress);
}
static void authority_trial(unsigned mode)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(program_source, &error);
    CHECK(program && error == WIRELOG_OK); wl_fusion_apply(program, NULL);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_session_t *base = NULL, *raw_base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
        && wl_session_create(wl_backend_columnar(), plan, 1, &raw_base) == 0);
    CHECK(wl_session_load_facts(base, program) == 0
        && wl_session_load_facts(raw_base, program) == 0);
    wl_col_session_t *session = COL_SESSION(base), *raw = COL_SESSION(raw_base);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0
        && wl_evaluation_control_attach(control,
        base->operation_admission) == 0);
    base->evaluation_control = control;
    authority_mode = mode; authority_hit = false; authority_record = NULL;
    wl_columnar_lftj_test_checkpoint = observe_record;
    wl_columnar_relation_test_after_single_reader_release = release_checkpoint;
    if (mode == 3 || mode == 6) wl_columnar_lftj_test_fail_next_iters_alloc();
    wl_lftj_capture_t got = {.width = 4}, twin = {.width = 4};
    int rc = wl_backend_columnar()->session_snapshot(base, capture, &got);
    int expected = mode >= 7 ? EOVERFLOW : mode == 3 ||
        mode == 6 ? ENOMEM : mode >= 2 ? EINVAL :
        WL_EVALUATION_CONTROL_CANCELLED;
    if (rc != expected || !authority_hit) fprintf(stderr,
            "LFTJ authority mode=%u hit=%u rc=%d expected=%d\n", mode,
            authority_hit, rc, expected);
    CHECK(authority_hit && rc == expected && !got.count);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(control, &report) == 0
        && report.result == expected
        && report.execution_status == (mode >= 7 ? EOVERFLOW : mode == 3 ||
        mode == 6 ? ENOMEM :
        mode >= 2 ? EINVAL : 0));
    wl_columnar_lftj_test_checkpoint = NULL;
    wl_columnar_relation_test_after_single_reader_release = NULL;
    if (mode == 3 || mode == 6) {
        wl_columnar_lftj_test_hook_state_t hooks;
        wl_columnar_lftj_test_get_hook_state(&hooks);
        CHECK(hooks.fail_iters_consumed && !hooks.fail_iters_pending);
        wl_columnar_lftj_test_clear_hooks();
    }
    CHECK(!session->cleanup_active);
    if (mode >= 2) {
        CHECK(session->cleanup_pending && authority_record->ready
            && authority_record ==
            wl_columnar_eval_stack_cleanup_lftj(session->cleanup_pending));
        if (mode == 2 || mode == 3) {
            CHECK(!authority_record->readers[0].owner
                && authority_record->probes[0].source_reader.owner
                && authority_record->probes[0].source_reader.identity !=
                descriptor_identity);
            authority_record->probes[0].source_reader.identity =
                descriptor_identity;
        } else {
            wl_columnar_memory_reservation_t *token = mode == 5 || mode == 7 ?
                &authority_record->inner : &authority_record->outer;
            CHECK(!token->identity && token->governor && token->bytes);
            CHECK(atomic_load_explicit(&token->state, memory_order_acquire) ==
                (mode == 4 ||
                mode == 8 ? WL_COLUMNAR_MEMORY_RESERVATION_RESERVED :
                WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED));
            if (mode == 5 ||
                mode == 7) CHECK(!authority_record->iters &&
                    !authority_record->row
                    && !authority_record->ranges && authority_record->inputs);
            else CHECK(!authority_record->inputs && !authority_record->probes
                    && !authority_record->readers &&
                    !authority_record->binary_row);
            token->identity = credit_identity;
        }
        CHECK(wl_columnar_eval_stack_cleanup_retry(session) == 0);
    }
    CHECK(!session->cleanup_pending && !session->sarr_active_pins);
    CHECK(wl_evaluation_control_reset(control) == 0);
    CHECK(wl_backend_columnar()->session_snapshot(base, capture, &got) == 0
        && wl_backend_columnar()->session_snapshot(raw_base, capture,
        &twin) == 0);
    CHECK(got.count == 1 && got.count == twin.count && got.digest == twin.digest
        && !session->serial_completion.frame && !session->cleanup_reserved_bytes
        && !session->sarr_active_pins);
    destroy(session); destroy(raw); wl_plan_free(plan);
    wirelog_program_free(program);
}

static void edge_trial(unsigned mode)
{
    const char *float_source =
        ".decl r1(x:float,a:int64)\n.decl r2(x:float,b:int64)\n.decl out(a:int64,b:int64)\n"
        "r1(-0.0,10).r1(-2.0,20).r1(0.0,11)."
        "r2(0.0,100).r2(-0.0,101).r2(-2.0,200).\n"
        "out(a,b):-r1(x,a),r2(x,b).\n";
    const char *integer_source =
        ".decl r1(x:int64,a:int64)\n.decl r2(x:int64,b:int64)\n.decl out(a:int64,b:int64)\n"
        "r1(1,10).r1(2,20).\n out(a,b):-r1(x,a),r2(x,b).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(mode == 0 || mode == 3 ?
            float_source : integer_source, &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_lftj_capture_t twin = {.width = mode == 5 ? 128u : 4u};
    for (unsigned controlled = 0; controlled < 2; controlled++) {
        wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
            && wl_session_load_facts(base, program) == 0);
        wl_col_session_t *session = COL_SESSION(base);
        if (mode == 0 || mode == 3) {
            for (uint32_t i = 0; i < 2; i++) {
                col_rel_t *source = session_find_rel(session, i ? "r2" : "r1");
                CHECK(source && source->column_types
                    && source->column_types[0] == WIRELOG_TYPE_FLOAT
                    && source->column_types[1] == WIRELOG_TYPE_INT64);
            }
        }
        if (mode == 2) {
            /* Normal host operations establish the declared width and then
             * remove its only row. The never-initialized width0 case is a
             * distinct raw EINVAL compatibility check (mode4). */
            int64_t row[] = {1, 100};
            CHECK(wl_backend_columnar()->session_insert(base, "r2", row, 1,
                2) == 0
                && wl_backend_columnar()->session_remove(base, "r2", row, 1,
                2) == 0);
            CHECK(session_find_rel(session, "r2")->ncols == 2
                && !session_find_rel(session, "r2")->nrows);
        }
        if (mode == 6) {
            const int64_t extra[][2] = {{INT64_MIN, 30}, {-1, 40},
                                        {INT64_C(1)<<40, 50}};
            CHECK(wl_backend_columnar()->session_insert(base, "r1",
                &extra[0][0], 3, 2) == 0);
        }
        int64_t saved_float_cell = 0;
        if (mode == 3) {
            int64_t invalid[] = {INT64_C(0x7ff8000000000001), 99};
            CHECK(wl_backend_columnar()->session_insert(base, "r1", invalid, 1,
                2) == EINVAL);
            /* Deliberately malformed supplied typed input, before any leases:
             * normal host insertion correctly rejects these NaN bits. This
             * separately reaches actual raw/controlled inner validation. */
            col_rel_t *source = session_find_rel(session, "r1");
            saved_float_cell = source->columns[0][0];
            source->columns[0][0] = invalid[0];
        }
        char *names[64] = {"r1", mode == 1 || mode == 6 ? "r1" : "r2"};
        uint32_t keys[64] = {0};
        if (mode == 5) for (unsigned i = 0; i < 64; i++) names[i] = "r1";
        wl_plan_op_lftj_t meta = {.k = mode == 5 ? 64u : 2u,
                                  .rel_names = names, .key_cols = keys};
        const wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ,
                                 .delta_mode = WL_DELTA_AUTO,
                                 .opaque_data = &meta};
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(session, &frame) == 0);
        wl_evaluation_control_t *control = NULL;
        if (controlled) {
            CHECK(wl_evaluation_control_create(0, &control) == 0
                && wl_evaluation_control_attach(control,
                base->operation_admission) == 0);
            base->evaluation_control = control;
            CHECK(wl_evaluation_control_begin(control,
                base->operation_admission) == 0);
        }
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        int rc = col_op_lftj(&op, stack, session);
        if (rc != (mode == 3 || mode == 4 ? EINVAL : 0)) fprintf(stderr,
                "LFTJ edge mode=%u controlled=%u rc=%d r1width=%u r2width=%u r2rows=%u caches=%u\n",
                mode, controlled, rc,
                session_find_rel(session, "r1")->ncols,
                session_find_rel(session, "r2")->ncols,
                session_find_rel(session, "r2")->nrows, session->sarr_count);
        CHECK(rc == (mode == 3 || mode == 4 ? EINVAL : 0));
        if (controlled) CHECK(wl_evaluation_control_finish(control,
                base->operation_admission,
                rc, 0) == rc);
        if (!rc) {
            CHECK(stack->top == 1 && stack->items[0].owned);
            col_rel_t *out = stack->items[0].rel;
            CHECK(out->ncols == (mode == 5 ? 128u : 4u) && !out->column_types
                && out->nrows == (mode == 0 || mode == 6 ? 5u : mode == 1 ||
                mode == 5 ? 2u : 0u));
            CHECK(out->capacity == (mode == 2 ? 0u : 64u));
            wl_lftj_capture_t got = {.width = out->ncols}; int64_t row[128];
            for (uint32_t i = 0; i < out->nrows; i++) {
                for (uint32_t col = 0; col < out->ncols;
                    col++) row[col] = col_rel_get(out, i, col);
                if (mode == 6) {
                    const int64_t expected[][2] = {{INT64_MIN, 30}, {-1, 40},
                                                   {1, 10}, {2, 20},
                                                   {INT64_C(1)<<40, 50}};
                    CHECK(row[0] == expected[i][0] && row[2] == row[0]
                        && row[1] == expected[i][1] && row[3] == row[1]);
                }
                if (mode == 5) for (uint32_t col = 0; col < 128; col += 2)
                        CHECK(row[col] == (int64_t)i + 1 &&
                            row[col + 1u] == ((int64_t)i + 1)*10);
                if (mode == 0) CHECK(row[0] == row[2]
                        && (i == 0 ? row[1] == 20 && row[3] == 200 :
                        (row[1] == 10 || row[1] == 11)
                        && (row[3] == 100 || row[3] == 101)));
                if (mode == 1) CHECK(row[0] == (int64_t)i + 1 &&
                        row[2] == row[0]
                        && row[1] == ((int64_t)i + 1) * 10 && row[3] == row[1]);
                capture("out", row, out->ncols, &got);
            }
            if (!controlled) twin = got;
            else CHECK(got.count == twin.count && got.digest == twin.digest);
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
            && !session->cleanup_active && !session->cleanup_pending
            && !session->cleanup_reserved_bytes && !session->sarr_active_pins);
        if (mode == 3) session_find_rel(session,
                "r1")->columns[0][0] = saved_float_cell;
        destroy(session);
    }
    wl_plan_free(plan); wirelog_program_free(program);
}

static void wide_trial(void)
{
    /* Supplied immutable k2 caller; genuine k3/k4 lowering is proved above.
     * Both dimensions exceed256 so one actual radix scatter contains more
     * than a poll interval even for a single row. */
    enum { width = 257, rows = 257 };
    char source[40000]; size_t used = 0;
    for (unsigned input = 0; input < 2; input++) {
        int n = snprintf(source + used, sizeof(source) - used, ".decl r%u(",
                input + 1);
        CHECK(n > 0 && (size_t)n < sizeof(source) - used); used += (size_t)n;
        for (unsigned col = 0; col < width; col++) {
            n = snprintf(source + used, sizeof(source) - used, "%sc%u:int64",
                    col ? "," : "", col);
            CHECK(n > 0 && (size_t)n < sizeof(source) - used);
            used += (size_t)n;
        }
        n = snprintf(source + used, sizeof(source) - used, ")\n");
        CHECK(n > 0 && (size_t)n < sizeof(source) - used); used += (size_t)n;
    }
    int n = snprintf(source + used, sizeof(source) - used,
            ".decl out(x:int64)\nout(x):-r1(x");
    CHECK(n > 0 && (size_t)n < sizeof(source) - used); used += (size_t)n;
    for (unsigned input = 0; input < 2; input++) {
        if (input) {
            n = snprintf(source + used, sizeof(source) - used, ",r2(x");
            CHECK(n > 0 && (size_t)n < sizeof(source) - used);
            used += (size_t)n;
        }
        for (unsigned col = 1; col < width; col++) {
            n = snprintf(source + used, sizeof(source) - used, ",v%u_%u", input,
                    col);
            CHECK(n > 0 && (size_t)n < sizeof(source) - used);
            used += (size_t)n;
        }
        n = snprintf(source + used, sizeof(source) - used, ")");
        CHECK(n > 0 && (size_t)n < sizeof(source) - used); used += (size_t)n;
    }
    n = snprintf(source + used, sizeof(source) - used, ".\n");
    CHECK(n > 0 && (size_t)n < sizeof(source) - used);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    int64_t *data = malloc((size_t)rows * width * sizeof(int64_t)); CHECK(data);
    for (unsigned input = 0; input < 2; input++) {
        for (unsigned row = 0; row < rows; row++) {
            data[(size_t)row * width] = rows - row;
            for (unsigned col = 1; col < width; col++)
                data[(size_t)row * width + col] = (int64_t)(rows - row) * 1000 +
                    input * 300 + col;
        }
        CHECK(wl_backend_columnar()->session_insert(base, input ? "r2" : "r1",
            data, rows, width) == 0);
    }
    free(data);
    wl_col_session_t *session = COL_SESSION(base);
    char *names[] = {"r1", "r2"}; uint32_t keys[] = {0, 0};
    wl_plan_op_lftj_t meta = {.k = 2, .rel_names = names, .key_cols = keys};
    const wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ, .opaque_data = &meta,
                             .delta_mode = WL_DELTA_AUTO};
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0
        && wl_evaluation_control_attach(control,
        base->operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(session, &frame) == 0);
    base->evaluation_control = control;
    CHECK(wl_evaluation_control_begin(control, base->operation_admission) == 0);
    cancellation_stage = 5; cancellation_visits = 0; cancellation_hit = false;
    sort_payload_seen = false;
    wl_columnar_lftj_test_checkpoint = cancel_checkpoint;
    CHECK(col_op_lftj(&op, wl_columnar_eval_stack_cleanup_stack(frame), session)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(cancellation_hit && cancellation_visits == 257u);
    wl_columnar_lftj_record_t *record =
        wl_columnar_eval_stack_cleanup_lftj(frame);
    /* The actual stopped cache build already discards its fresh image and
     * sorter temporary before returning. Outer original credit still owns
     * the raw admitted potential buffers until frame cleanup. */
    CHECK(sort_payload_seen && record && !record->radix_temp &&
        !record->sort_bytes
        && !record->cache_fresh.sorted && !record->cache_ledger_installed
        && !record->kernel_visits && record->outer.identity == &record->outer
        && atomic_load_explicit(&record->outer.state, memory_order_acquire)
        == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
    wl_columnar_lftj_test_checkpoint = NULL;
    CHECK(wl_evaluation_control_finish(control, base->operation_admission, 0, 0)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
        && !session->sarr_active_pins);
    CHECK(wl_evaluation_control_reset(control) == 0);
    for (unsigned controlled = 0; controlled < 2; controlled++) {
        base->evaluation_control = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(session, &frame) == 0);
        base->evaluation_control = controlled ? control : NULL;
        if (controlled) CHECK(wl_evaluation_control_begin(control,
                base->operation_admission) == 0);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        CHECK(col_op_lftj(&op, stack, session) == 0 && stack->top == 1);
        col_rel_t *out = stack->items[0].rel;
        CHECK(out->nrows == rows && out->ncols == 2u * width);
        for (unsigned row = 0; row < rows; row++)
            for (unsigned input = 0; input < 2; input++)
                for (unsigned col = 0; col < width; col++)
                    CHECK(col_rel_get(out, row, input * width + col) ==
                        (col ? (int64_t)(row + 1u) * 1000 + input * 300 +
                        col : row + 1u));
        for (unsigned input = 0; input < 2; input++) {
            col_rel_t *in = session_find_rel(session, names[input]);
            CHECK(in->nrows == rows && in->ncols == width);
            for (unsigned row = 0; row < rows; row++)
                CHECK(col_rel_get(in, row, 0) == rows - row);
        }
        if (controlled) CHECK(wl_evaluation_control_finish(control,
                base->operation_admission, 0, 0) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
            && !session->sarr_active_pins && !session->cleanup_reserved_bytes);
    }
    base->evaluation_control = control;
    destroy(session); wl_plan_free(plan); wirelog_program_free(program);
}

static void metadata_trial(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl r1(x:int64)\n.decl r2(x:int64)\n.decl out(x:int64)\n"
        "r1(1).r2(1).out(x):-r1(x),r2(x).\n", &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_session_t *base = generated_session(plan, false);
    CHECK(wl_session_load_facts(base, program) == 0);
    wl_col_session_t *session = COL_SESSION(base);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0
        && wl_evaluation_control_attach(control,
        base->operation_admission) == 0);
    for (unsigned mode = 0; mode < 8; mode++)
        for (unsigned controlled = 0; controlled < 2; controlled++) {
            char *names[65]; uint32_t keys[65] = {0};
            for (unsigned i = 0; i < 65; i++) names[i] = "r1";
            wl_plan_op_lftj_t meta = {.k = mode == 0 ? 0u : mode == 1 ? 1u :
                                          mode == 2 ? 65u : 2u,
                                      .rel_names = mode == 3 ? NULL : names,
                                      .key_cols = mode == 4 ? NULL : keys};
            if (mode == 5) keys[0] = 1;
            if (mode == 6) names[1] = "missing";
            const wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ,
                                     .delta_mode = WL_DELTA_AUTO,
                                     .opaque_data = mode == 7 ? NULL : &meta};
            wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
            base->evaluation_control = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_begin(session, &frame) == 0);
            base->evaluation_control = controlled ? control : NULL;
            if (controlled) CHECK(wl_evaluation_control_begin(control,
                    base->operation_admission) == 0);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
            int expected = mode == 6 ? ENOENT : EINVAL;
            CHECK(col_op_lftj(&op, stack, session) == expected && !stack->top);
            if (controlled) CHECK(wl_evaluation_control_finish(control,
                    base->operation_admission, expected, 0) == expected);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
                && !session->cleanup_pending && !session->cleanup_reserved_bytes
                && !session->sarr_active_pins);
            if (controlled) CHECK(wl_evaluation_control_reset(control) == 0);
        }
    base->evaluation_control = control;
    destroy(session); wl_plan_free(plan); wirelog_program_free(program);
}

static void unexpected_raw_emit(const int64_t *row, uint32_t width, void *data)
{
    (void)row; (void)width; (void)data; CHECK(false);
}
static int unexpected_controlled_emit(const int64_t *row, uint32_t width,
    void *data)
{
    unexpected_raw_emit(row, width, data); return EINVAL;
}
static void kernel_width_guard(void)
{
    /* Explicit supplied descriptor validation, separate from generated and
     * production allocation proofs. Zero rows permit shape validation without
     * reading an imaginary UINT32_MAX-wide physical buffer. */
    const wl_lftj_input_t inputs[] = {{NULL, 0, UINT32_MAX, 0},
                                      {NULL, 0, 2, 0}};
    CHECK(wl_columnar_lftj_join_typed(inputs, WIRELOG_TYPE_INT64, 2,
        unexpected_raw_emit, NULL) == EINVAL);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl r1(x:int64)\n.decl out(x:int64)\nout(x):-r1(x).\n", &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_session_t *base = generated_session(plan, false);
    wl_col_session_t *session = COL_SESSION(base);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(session, &frame) == 0);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0
        && wl_evaluation_control_attach(control,
        base->operation_admission) == 0);
    base->evaluation_control = control;
    CHECK(wl_evaluation_control_begin(control, base->operation_admission) == 0);
    wl_columnar_lftj_record_t *record = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_lftj_owner(session,
        wl_columnar_eval_stack_cleanup_stack(frame), &record, control) == 0);
    record->k = 2; record->key_type = WIRELOG_TYPE_INT64;
    CHECK(wl_columnar_lftj_reserve(record, &record->outer,
        sizeof(inputs)) == 0);
    record->inputs = malloc(sizeof(inputs)); CHECK(record->inputs);
    for (unsigned i = 0; i < 2; i++) record->inputs[i] = inputs[i];
    CHECK(wl_columnar_lftj_commit(record, &record->outer, record->inputs) == 0);
    CHECK(wl_columnar_lftj_kernel_controlled(record, unexpected_controlled_emit,
        NULL) == EINVAL
        && !record->iters && !record->kernel_visits);
    CHECK(wl_evaluation_control_finish(control, base->operation_admission,
        EINVAL, 0) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
        && !session->cleanup_pending && !session->cleanup_reserved_bytes);
    destroy(session); wl_plan_free(plan); wirelog_program_free(program);
}

int main(void)
{
    for (unsigned fallback = 0; fallback < 5; fallback++) {
        direct_cap0(false, fallback); direct_cap0(true, fallback);
    }
    for (unsigned four = 0; four < 2; four++)
        for (unsigned snapshot = 0; snapshot < 2; snapshot++)
            generated(four != 0, snapshot != 0, false);
    generated(false, true, true);
    for (unsigned boundary = 0; boundary < 6; boundary++) exact_trial(boundary);
    for (unsigned mode = 0; mode < 9; mode++) authority_trial(mode);
    for (unsigned mode = 0; mode < 7; mode++) edge_trial(mode);
    kernel_width_guard(); metadata_trial(); wide_trial();
    const int stages[] = {3, 4, 5, 6, 7, 8, 9, 10, 12, 13};
    for (unsigned i = 0; i < sizeof(stages)/sizeof(stages[0]); i++)
        generated_stop(stages[i]);
    puts("evaluation_lftj: original cap0 production kernel/raw twin OK");
    return 0;
}
