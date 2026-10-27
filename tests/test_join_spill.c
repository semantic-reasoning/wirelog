/* #1372: actual ordinary STEP dispatch before resident index construction. */
#include "columnar/internal.h"
#include "columnar/columnar_nanoarrow.h"
#include "exec_plan_gen.h"
#include "session.h"
#include "session_facts.h"
#include "wirelog/wirelog-parser.h"
#include "test_tmpdir.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)
#define BUDGET (UINT64_C(256) * 1024 * 1024)
static int write_error;
static unsigned fail_read_after;
static unsigned fail_allocate_after;
static wl_col_session_t *pressure_session;
static unsigned pressure_kind;
static uint64_t pressure_budget = BUDGET;
static wl_columnar_memory_reservation_t pressure;
extern bool wl_columnar_relation_test_fail_prepare_resize;

bool wl_columnar_spill_test_allocation(void)
{
    return fail_allocate_after && --fail_allocate_after == 0;
}
int wl_columnar_spill_test_io(bool writing, uint64_t offset, size_t *length)
{
    (void)offset; (void)length;
    if (!writing && pressure_session &&
        pressure_session->join_output_peak >= 60){
        if (pressure_kind == 1) {
            wl_columnar_memory_governor_t *g =
                wl_columnar_memory_governor_ref_get(
                pressure_session->memory_governor);
            CHECK(wl_columnar_memory_reserve(g,
                pressure_budget - wl_columnar_memory_reserved(g), &pressure));
        } else {
            wl_columnar_relation_test_fail_prepare_resize = true;
        }
        pressure_session = NULL;
    }
    if (writing) return write_error;
    return fail_read_after && --fail_read_after == 0 ? EIO : 0;
}

extern void (*wl_columnar_join_test_before_spill_take)(col_rel_t *);
extern void (*wl_columnar_join_test_after_spill_take)(uint64_t, uint64_t,
    uint64_t, uint64_t);
static unsigned take_count, take_fault;
static uint64_t taken_identity, taken_bytes;
static bool take_busy;
static wl_columnar_source_access_reader_t take_pin;
static wl_col_session_t *take_session;
static void
before_take(col_rel_t *source)
{
    if (take_busy) CHECK(col_rel_source_reader_acquire(source, &take_pin) == 0);
}
static void
after_take(uint64_t identity, uint64_t released, uint64_t before,
    uint64_t after)
{
    CHECK(identity && released && after + released > before);
    take_count++; taken_identity = identity; taken_bytes += released;
    if (take_fault == 2) fail_allocate_after = 1;
    if (take_fault == 3) write_error = EIO;
    if (take_fault == 4) fail_read_after = 1;
    if (take_fault == 7) wl_columnar_relation_test_fail_prepare_resize = true;
    if (take_fault == 8) {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(take_session->memory_governor);
        CHECK(wl_columnar_memory_reserve(g,
            BUDGET - wl_columnar_memory_reserved(g), &pressure));
    }
}

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session;
    wl_columnar_memory_governor_ref_t *governor;
} fixture_t;

static uint64_t
reserved(fixture_t *f)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f->governor));
}

static fixture_t
fixture_source_budget(bool enabled, uint64_t quota, const char *source,
    uint64_t budget)
{
    fixture_t f = {0};
    wirelog_error_t error;
    f.program = wirelog_parse_string(source, &error);
    CHECK(f.program && wl_plan_from_program(f.program, &f.plan) == 0);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = budget, .usable_bytes = budget,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK
    };
    f.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = f.governor;
    options.spill_directory = test_tmpdir();
    options.spill_quota_bytes = quota;
    options.spill_max_row_bytes = 64;
    if (enabled) {
        options.spill_join_batch_bytes = 96;
        options.spill_join_index_bytes =
            sizeof(wl_columnar_memory_reservation_t) + 16;
    }
    CHECK(wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session) == 0);
    CHECK(wl_session_load_facts(f.session, f.program) == 0);
    for (int64_t i = 0; i < 18; i++) {
        int64_t row[] = {i % 3, i};
        CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
        row[1] += 100;
        CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
    }
    return f;
}

static fixture_t
fixture_source(bool enabled, uint64_t quota, const char *source)
{
    return fixture_source_budget(enabled, quota, source, BUDGET);
}

static fixture_t
fixture(bool enabled, uint64_t quota)
{
    return fixture_source(enabled, quota,
               ".decl a(k: int64, v: int64)\n"
               ".decl b(k: int64, v: int64)\n"
               ".decl result(k: int64, x: int64, y: int64)\n"
               "result(k, x, y + 0) :- a(k, x), b(k, y).\n");
}

static void
destroy(fixture_t *f)
{
    wl_session_destroy(f->session);
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    CHECK(reserved(f) == 0);
    wl_columnar_memory_governor_ref_release(f->governor);
}

static void
contents(fixture_t *f)
{
    wl_col_session_t *s = COL_SESSION(f->session);
    col_rel_t *out = session_find_rel(s, "result");
    CHECK(out && out->nrows == 108 && out->ncols == 3);
    bool seen[18][18] = {{false}};
    for (uint32_t i = 0; i < out->nrows; i++) {
        int64_t k = out->columns[0][i], x = out->columns[1][i];
        int64_t y = out->columns[2][i] - 100;
        CHECK(x >= 0 && x < 18 && y >= 0 && y < 18);
        CHECK(k == x % 3 && k == y % 3 && !seen[x][y]);
        seen[x][y] = true;
    }
    CHECK(session_find_rel(s, "a")->nrows == 18);
    CHECK(session_find_rel(s, "b")->nrows == 18);
    CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
}

static void
observe(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *context)
{
    CHECK(strcmp(relation, "result") == 0 && ncols == 3 && diff == 1);
    CHECK(row[0] == row[1] % 3 && row[0] == (row[2] - 100) % 3);
    (*(unsigned *)context)++;
}

static void
failures(void)
{
    for (unsigned kind = 0; kind < 7; kind++) {
        /* quota 1000 admits the first snapshot but not the second. */
        fixture_t f = fixture(true, kind == 0 ? 32 : kind == 1 ? 1000 : 65536);
        wl_col_session_t *s = COL_SESSION(f.session);
        unsigned observed = 0;
        wl_session_set_delta_cb(f.session, observe, &observed);
        if (kind == 2) write_error = EIO;
        if (kind == 3) write_error = ENOSPC;
        if (kind == 4) fail_allocate_after = 1;
        if (kind == 5) fail_read_after = 1;
        if (kind == 6) fail_read_after = 100;
        int rc = wl_session_step(f.session);
        int expected = kind <= 1 || kind == 3 ? ENOSPC
            : kind == 4 ? ENOMEM : EIO;
        CHECK(rc == expected);
        CHECK(s->spill_join_selected_count == 1 && s->arr_count == 0);
        CHECK(!wl_session_budget_denied(f.session));
        CHECK(observed == 0);
        CHECK(session_find_rel(s, "a")->nrows == 18 &&
            session_find_rel(s, "b")->nrows == 18);
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
        write_error = 0; fail_read_after = 0; fail_allocate_after = 0;
        destroy(&f);
        /* Fresh legal evaluation after the failure cannot inherit snapshots
        * or duplicate published rows. STEP-wide rollback is not assumed. */
        f = fixture(true, 65536);
        CHECK(wl_session_step(f.session) == 0);
        contents(&f);
        destroy(&f);
    }
}

/* Ownership/error details at the same operator boundary used by STEP. */
static void
direct(void)
{
    for (unsigned kind = 0; kind < 10; kind++) {
        fixture_t f = fixture(true, 65536);
        wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *original = session_find_rel(s, "a");
        col_rel_t *left = original;
        col_rel_t *right = session_find_rel(s, kind == 2 ? "a" : "b");
        uint64_t baseline = reserved(&f);
        bool owned = kind == 1;
        if (owned) {
            CHECK(wl_columnar_relation_new_auto_governed("$owned", 2, 64,
                false, f.governor, &left) == 0);
            col_rel_set_schema(left, 2,
                (const char *const *)original->col_names);
            wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                             WIRELOG_TYPE_FLOAT};
            CHECK(col_rel_set_column_types(left, types, 2) == 0);
            for (uint32_t i = 0; i < original->nrows; i++) {
                double value = (double)i + 0.5;
                int64_t row[] = {i % 3, 0};
                memcpy(&row[1], &value, sizeof(value));
                CHECK(col_rel_append_row(left, row) == 0);
            }
        }
        const char *lk[] = {left->col_names[0]};
        const char *rk[] = {right->col_names[0]};
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN,
                           .right_relation = kind == 2 ? "a" : "b",
                           .key_count = 1, .left_keys = lk, .right_keys = rk,
                           .delta_mode = WL_DELTA_FORCE_FULL};
        uint32_t identity[] = {0, 1, 2, 3};
        if (owned) {
            op.project_count = 4; op.project_indices = identity;
        }
        eval_stack_t stack;
        eval_stack_init(&stack);
        CHECK(eval_stack_push(&stack, left, owned) == 0);
        if (kind == 3) s->join_output_limit = 3;
        if (kind == 4) s->join_output_limit = 4;
        if (kind == 5) s->join_output_limit = 109;
        wl_columnar_memory_reservation_init(&pressure);
        if (kind == 6) {
            /* Admit resolved keys, refuse selected-route private preparation. */
            CHECK(wl_columnar_memory_reserve(
                    wl_columnar_memory_governor_ref_get(f.governor),
                    BUDGET - reserved(&f) - 8, &pressure));
        }
        if (kind == 7) s->spill_join_index_bytes = 1;
        if (kind >= 8) {
            pressure_session = s;
            pressure_kind = kind == 8 ? 1 : 2;
        }
        int rc = wl_columnar_join_op(&op, &stack, s);
        int expected = kind == 3 || kind == 4 ? EOVERFLOW
            : kind >= 6 && kind <= 8 ? ENOSPC : kind == 9 ? ENOMEM : 0;
        CHECK(rc == expected);
        CHECK(s->spill_join_selected_count == 1 && s->arr_count == 0);
        CHECK(wl_session_budget_denied(f.session) == (kind >= 6 && kind <= 8));
        if (kind >= 8) {
            CHECK(!pressure_session && s->join_output_peak >= 60);
            CHECK(!wl_columnar_relation_test_fail_prepare_resize);
        }
        if (kind == 6 ||
            kind == 8) CHECK(wl_columnar_memory_rollback(&pressure));
        if (!rc) {
            CHECK(stack.top == 1);
            eval_entry_t result = eval_stack_pop(&stack);
            CHECK(result.owned && !result.is_delta && result.rel->nrows == 108);
            CHECK(result.rel->ncols == 4);
            bool seen[18][18] = {{false}};
            for (uint32_t i = 0; i < result.rel->nrows; i++) {
                int64_t x = result.rel->columns[1][i];
                int64_t y = result.rel->columns[3][i] - (kind == 2 ? 0 : 100);
                if (owned) {
                    double value;
                    memcpy(&value, &x, sizeof(value));
                    x = (int64_t)(value - 0.5);
                    CHECK(value == (double)x + 0.5);
                    CHECK(result.rel->column_types[1] == WIRELOG_TYPE_FLOAT);
                }
                CHECK(x >= 0 && x < 18 && y >= 0 && y < 18);
                CHECK(!seen[x][y] && x % 3 == y % 3);
                seen[x][y] = true;
            }
            CHECK(eval_entry_dispose(&result) == 0);
        } else CHECK(stack.top == 0);
        CHECK(eval_stack_drain(&stack) == 0);
        CHECK(original->nrows == 18 && right->nrows == 18);
        CHECK(!wl_columnar_source_access_gate_busy(&original->source_access));
        CHECK(!wl_columnar_source_access_gate_busy(&right->source_access));
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
        CHECK(reserved(&f) == baseline);
        destroy(&f);
    }
}

static void
excluded(void)
{
    for (unsigned kind = 0; kind < 8; kind++) {
        fixture_t f = fixture(true, 65536);
        wl_col_session_t *s = COL_SESSION(f.session);
        wl_col_session_t *worker = NULL;
        if (kind == 6) {
            worker = calloc(1, sizeof(*worker));
            col_rel_t *partitions[2] = {NULL, NULL};
            CHECK(wl_columnar_relation_deep_copy_governed(session_find_rel(s,
                "a"),
                &partitions[0], f.governor) == 0);
            CHECK(wl_columnar_relation_deep_copy_governed(session_find_rel(s,
                "b"),
                &partitions[1], f.governor) == 0);
            CHECK(worker && col_worker_session_create(s, 0, partitions, 2,
                worker) == 0);
            CHECK(worker->spill_join_batch_bytes == s->spill_join_batch_bytes);
            CHECK(worker->spill_store == s->spill_store &&
                !worker->owns_spill_store);
            s = worker;
        }
        col_rel_t *left = session_find_rel(s, "a");
        col_rel_t *right = session_find_rel(s, "b");
        CHECK(left && right);
        const char *lk[] = {left->col_names[0]};
        const char *rk[] = {right->col_names[0]};
        uint32_t projection[] = {0, 1, 99};
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "b",
                           .key_count = 1, .left_keys = lk, .right_keys = rk,
                           .delta_mode = WL_DELTA_FORCE_FULL};
        if (kind == 0) {
            op.project_count = 3; op.project_indices = projection;
        }
        if (kind == 1) op.key_count = 0;
        if (kind == 3) s->num_workers = 2;
        if (kind == 4) {
            /* Existing strict batching retains precedence. */
            s->join_batch_bytes = 96; s->join_batch_strict = true;
            op.key_count = 0;
        }
        if (kind == 5) left->nrows = 0;
        if (kind == 7) op.project_count = 3; /* NULL map still concatenates. */
        eval_stack_t stack;
        eval_stack_init(&stack);
        CHECK(eval_stack_push_delta(&stack, left, false, kind == 2) == 0);
        int rc = wl_columnar_join_op(&op, &stack, s);
        CHECK(rc == (kind == 4 ? ENOTSUP : 0));
        CHECK(s->spill_join_selected_count == (kind == 7 ? 1u : 0u));
        if (!rc) {
            eval_entry_t result = eval_stack_pop(&stack);
            CHECK(result.rel->nrows == (kind == 1 ? 324 : kind == 5 ? 0 : 108));
            if (kind == 0) {
                CHECK(result.rel->ncols == 3);
                for (uint32_t i = 0; i < result.rel->nrows; i++)
                    CHECK(result.rel->columns[2][i] == 0);
            }
            if (kind == 7) CHECK(result.rel->ncols == 4);
            CHECK(eval_entry_dispose(&result) == 0);
        }
        CHECK(eval_stack_drain(&stack) == 0);
        if (worker) {
            CHECK(col_worker_session_destroy(worker) == 0);
            free(worker);
        }
        destroy(&f);
    }
}

static void
progress(void)
{
    fixture_t f = fixture(true, 65536);
    wl_col_session_t *s = COL_SESSION(f.session);
    col_rel_t *a = session_find_rel(s, "a");
    for (uint32_t i = 0; i < a->nrows; i++) a->columns[0][i] = 999;
    for (int64_t i = 18; i < 600; i++) {
        int64_t row[] = {999, i};
        CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
    }
    s->join_output_limit = 1;
    /* Each 600-row nonmatching scan exceeds the 256-action quantum.
     * PROGRESS must neither end the JOIN nor consume the one-row cap. */
    CHECK(wl_session_step(f.session) == 0);
    CHECK(s->spill_join_selected_count == 1 && s->arr_count == 0);
    CHECK(s->join_output_peak == 0);
    CHECK(session_find_rel(s, "result")->nrows == 0);
    CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
    destroy(&f);
}

static const wl_plan_op_t *planned_join(fixture_t *f)
{
    for (uint32_t i = 0; i < f->plan->stratum_count; i++)
        for (uint32_t j = 0; j < f->plan->strata[i].relation_count; j++) {
            const wl_plan_relation_t *r = &f->plan->strata[i].relations[j];
            for (uint32_t k = 0; k < r->op_count; k++)
                if (r->ops[k].op == WL_PLAN_OP_JOIN) return &r->ops[k];
        }
    return NULL;
}

static void projected_steps(void)
{
    for (unsigned filtered = 0; filtered < 3; filtered++) {
        for (unsigned enabled = 0; enabled < 2; enabled++) {
            char source[512];
            CHECK(snprintf(source, sizeof(source),
                ".decl a(k: int64, v: int64)\n"
                ".decl b(k: int64, v: int64)\n"
                ".decl result(k: int64, x: int64, y: int64)\n"
                "result(k, x, y) :- a(k, x), b(k, y)%s.\n",
                filtered == 1 ? ", y = 109" : filtered ==
                2 ? ", y = 999" : "") > 0);
            fixture_t f = fixture_source(enabled != 0, 65536, source);
            const wl_plan_op_t *join = planned_join(&f);
            CHECK(join);
            if (!filtered) CHECK(join->project_count == 3 &&
                    join->project_indices);
            CHECK((join->right_filter_expr.size != 0) == (filtered != 0));
            unsigned observed = 0;
            wl_session_set_delta_cb(f.session, observe, &observed);
            CHECK(wl_session_step(f.session) == 0);
            wl_col_session_t *s = COL_SESSION(f.session);
            col_rel_t *out = session_find_rel(s, "result");
            uint32_t expected = filtered == 2 ? 0 : filtered == 1 ? 6 : 108;
            CHECK(out && out->nrows == expected && observed == expected);
            bool seen[18][18] = {{false}};
            for (uint32_t i = 0; i < out->nrows; i++) {
                int64_t x = out->columns[1][i], y = out->columns[2][i] - 100;
                CHECK(x >= 0 && x < 18 && y >= 0 && y < 18 && !seen[x][y]);
                CHECK(x % 3 == y % 3 && out->columns[0][i] == x % 3);
                CHECK(!filtered || y == 9);
                seen[x][y] = true;
                CHECK(out->column_types[0] == WIRELOG_TYPE_INT64 &&
                    out->column_types[1] == WIRELOG_TYPE_INT64 &&
                    out->column_types[2] == WIRELOG_TYPE_INT64);
            }
            CHECK(s->spill_join_selected_count == (enabled &&
                filtered != 2 ? 1u : 0u));
            if (enabled &&
                filtered != 2) CHECK(s->arr_count == 0 &&
                    s->filt_arr_count == 0);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
            /* A completed repeated step neither duplicates output nor events. */
            CHECK(wl_session_step(f.session) == 0);
            CHECK(session_find_rel(s, "result")->nrows == expected);
            CHECK(observed == expected);
            destroy(&f);
        }
    }
}

static void owned_filters(void)
{
    for (unsigned heap = 0; heap < 2; heap++) {
        pressure_budget = heap ? 4 * 1024 * 1024 : BUDGET;
        for (unsigned fault = 0; fault < 6 + heap; fault++) {
            fixture_t f = fixture_source_budget(true, 65536,
                    ".decl a(k: int64, v: int64)\n"
                    ".decl b(k: int64, v: int64)\n"
                    ".decl result(k: int64, x: int64, y: int64)\n"
                    "result(k, x, y) :- a(k, x), b(k, y), y = 109.\n",
                    pressure_budget);
            for (int64_t i = 18; i < 108; i++) {
                int64_t row[] = {0, i};
                CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
            }
            wl_col_session_t *s = COL_SESSION(f.session);
            col_rel_t *left = session_find_rel(s, "a");
            col_rel_t *right = session_find_rel(s, "b");
            wl_plan_op_t op = *planned_join(&f);
            const char *lk[] = {left->col_names[0]},
                *rk[] = {right->col_names[0]};
            uint32_t map[] = {0, 1, 3};
            op.left_keys = lk; op.right_keys = rk;
            op.delta_mode = WL_DELTA_FORCE_FULL;
            op.project_count = 3; op.project_indices = map;
            col_filt_cache_pin_t pin = {0}; col_rel_t *filtered = NULL;
            bool unavailable = false;
            CHECK(wl_columnar_filter_apply_right_filter_cached_pin_checked(s,
                &op.right_filter_expr, op.right_relation, right, &pin,
                &filtered,
                &unavailable) == 0 && filtered && !unavailable && pin.active);
            CHECK(filtered->nrows == 1);
            /* The pinned old view cannot be rebuilt. The real cache reports
             * unavailable and JOIN must own its separate filter result. */
            wl_columnar_relation_touch_view(right);
            uint64_t baseline = reserved(&f);
            if (fault == 1) fail_allocate_after = 3; /* second snapshot */
            if (fault == 2) write_error = EIO;
            if (fault == 3 || fault == 5) {
                pressure_session = s; pressure_kind = fault == 3 ? 2 : 1;
            }
            if (fault == 4) s->join_output_limit = 1;
            take_count = 0; taken_bytes = 0;
            take_busy = fault == 6;
            wl_columnar_join_test_before_spill_take = before_take;
            wl_columnar_join_test_after_spill_take = after_take;
            wl_columnar_memory_reservation_init(&pressure);
            eval_stack_t stack; eval_stack_init(&stack);
            CHECK(eval_stack_push(&stack, left, false) == 0);
            int rc = wl_columnar_join_op(&op, &stack, s);
            CHECK(rc == (fault == 1 || fault == 3 ? ENOMEM : fault == 2 ? EIO
            : fault == 4 ? EOVERFLOW : fault == 5 ? ENOSPC : fault ==
                6 ? EBUSY : 0));
            CHECK(take_count == (!heap || fault == 1 || fault == 2 ||
                fault == 6 ? 0u : 1u));
            if (fault == 6) {
                CHECK(s->deferred_relation_count == 1 && take_pin.owner);
                CHECK(col_rel_source_reader_release(&take_pin) == 0);
                CHECK(wl_columnar_session_retry_deferred(s) == 0 &&
                    s->deferred_relation_count == 0);
            }
            take_busy = false;
            wl_columnar_join_test_before_spill_take = NULL;
            wl_columnar_join_test_after_spill_take = NULL;
            CHECK(pin.entry->evict_deferred && pin.rel == filtered &&
                filtered->nrows == 1);
            CHECK(s->spill_join_selected_count == 1 && s->arr_count == 0 &&
                s->filt_arr_count == 0);
            CHECK(wl_session_budget_denied(f.session) == (fault == 5));
            if (fault == 5) CHECK(wl_columnar_memory_rollback(&pressure));
            if (fault == 3 ||
                fault == 5) CHECK(!pressure_session &&
                    s->join_output_peak >= 60);
            if (!rc) {
                eval_entry_t result = eval_stack_pop(&stack);
                CHECK(result.rel->ncols == 3 && result.rel->nrows == 96);
                bool seen[108] = {false};
                for (uint32_t i = 0; i < result.rel->nrows; i++) {
                    int64_t x = result.rel->columns[1][i];
                    CHECK(result.rel->columns[0][i] == 0 &&
                        result.rel->columns[2][i] == 109);
                    CHECK(x >= 0 && x < 108 && !seen[x]); seen[x] = true;
                }
                CHECK(eval_entry_dispose(&result) == 0);
            }
            CHECK(stack.top == 0 &&
                wl_columnar_spill_store_bytes(s->spill_store) == 0);
            CHECK(reserved(&f) == baseline && left->nrows == 108 &&
                right->nrows == 18);
            CHECK(!wl_columnar_source_access_gate_busy(&right->source_access));
            fail_allocate_after = 0; write_error = 0;
            col_filt_cache_pin_release(&pin);
            destroy(&f);
        }
    }
    pressure_budget = BUDGET;
}

/* Calibrate on a separate fresh session; proof sessions never change budget. */
static uint64_t budget_peak, denied_request, denied_reserved;
static wl_columnar_memory_governor_t *denied_governor;
static void
budget_admitted(wl_columnar_memory_governor_t *g, uint64_t total)
{
    (void)g;
    if (total > budget_peak) budget_peak = total;
}
static void
budget_denied(wl_columnar_memory_governor_t *g, uint64_t bytes, uint64_t total)
{
    denied_governor = g;
    denied_request = bytes;
    denied_reserved = total;
}

static fixture_t
budget_fixture(bool enabled, bool filtered, uint64_t allowance)
{
    fixture_t f = {0};
    wirelog_error_t error;
    f.program = wirelog_parse_string(filtered ?
            ".decl a(k: int64, v: int64)\n.decl b(k: int64, v: int64)\n"
            ".decl result(k: int64, x: int64, y: int64)\n"
            "result(k,x,y) :- a(k,x), b(k,y), y = 100.\n" :
            ".decl a(k: int64, v: int64)\n.decl b(k: int64, v: int64)\n"
            ".decl result(k: int64, x: int64, y: int64)\n"
            "result(k,x,y) :- a(k,x), b(k,y).\n", &error);
    CHECK(f.program && wl_plan_from_program(f.program, &f.plan) == 0);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = allowance, .usable_bytes = allowance,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK
    };
    f.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = f.governor;
    options.spill_directory = test_tmpdir();
    options.spill_quota_bytes = 1024 * 1024;
    options.spill_max_row_bytes = 64;
    if (enabled) {
        options.spill_join_batch_bytes = 96;
        options.spill_join_index_bytes =
            sizeof(wl_columnar_memory_reservation_t) + 16;
    }
    CHECK(wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session) == 0);
    CHECK(wl_session_load_facts(f.session, f.program) == 0);
    int64_t *rows = malloc(4096 * 2 * sizeof(*rows));
    CHECK(rows);
    for (int64_t i = 0; i < 4096; i++) {
        rows[2 * i] = i;
        rows[2 * i + 1] = 100;
        if (i == 0 || i == 2048 || i == 4095) {
            int64_t row[] = {i, i + 10};
            CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
        }
    }
    CHECK(wl_session_insert(f.session, "b", rows, 4096, 2) == 0);
    free(rows);
    const wl_plan_op_t *op = planned_join(&f);
    CHECK(op && (filtered ? op->right_filter_expr.size != 0 :
        op->project_count == 3));
    return f;
}

static void
budget_observe(const char *name, const int64_t *row, uint32_t ncols,
    int32_t diff, void *context)
{
    unsigned *seen = context;
    CHECK(strcmp(name, "result") == 0 && ncols == 3 && diff == 1);
    unsigned bit = row[0] == 0 ? 1 : row[0] == 2048 ? 2 :
        row[0] == 4095 ? 4 : 0;
    CHECK(bit && !(*seen & bit));
    CHECK(row[1] == row[0] + 10 && row[2] == 100);
    *seen |= bit;
}

static void
budget_contents(fixture_t *f)
{
    wl_col_session_t *s = COL_SESSION(f->session);
    col_rel_t *out = session_find_rel(s, "result");
    unsigned seen = 0;
    CHECK(out && out->nrows == 3 && out->ncols == 3);
    for (uint32_t i = 0; i < out->nrows; i++) {
        int64_t row[] = {out->columns[0][i], out->columns[1][i],
                         out->columns[2][i]};
        budget_observe("result", row, 3, 1, &seen);
    }
    CHECK(seen == 7);
}

static void
fixed_budget(void)
{
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    wl_columnar_arrangement_test_reserve_denied = budget_denied;
    for (unsigned filtered = 0; filtered < 2; filtered++) {
        budget_peak = 0;
        fixture_t calibration = budget_fixture(true, filtered, 4 * 1024 * 1024);
        uint64_t preparation_peak = budget_peak;
        unsigned calibration_seen = 0;
        wl_session_set_delta_cb(calibration.session, budget_observe,
            &calibration_seen);
        CHECK(wl_session_step(calibration.session) == 0);
        budget_contents(&calibration);
        uint64_t allowance = budget_peak;
        destroy(&calibration);
        CHECK(allowance > preparation_peak);
        for (unsigned enabled = 0; enabled < 2; enabled++) {
            budget_peak = denied_request = denied_reserved = 0;
            denied_governor = NULL;
            fixture_t f = budget_fixture(enabled, filtered, allowance);
            wl_col_session_t *s = COL_SESSION(f.session);
            unsigned observed = 0;
            wl_session_set_delta_cb(f.session, budget_observe, &observed);
            int rc = wl_session_step(f.session);
            CHECK(budget_peak <= allowance);
            if (enabled) {
                CHECK(rc == 0 && observed == 7);
                CHECK(s->spill_join_selected_count == 1 && s->arr_count == 0);
                CHECK(denied_request == 0);
                budget_contents(&f);
            } else {
                CHECK(rc != 0 && observed == 0);
                CHECK(s->spill_join_selected_count == 0);
                CHECK(denied_governor ==
                    wl_columnar_memory_governor_ref_get(f.governor));
                /* 4096 rows require 8192 buckets and 4096 row links. */
                CHECK(denied_request == 8192 * sizeof(uint64_t) +
                    4096 * sizeof(uint32_t));
                CHECK(denied_reserved <= allowance &&
                    denied_request > allowance - denied_reserved);
            }
            CHECK(session_find_rel(s, "a")->nrows == 3);
            CHECK(session_find_rel(s, "b")->nrows == 4096);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
            destroy(&f);
        }
        if (!filtered) {
            uint64_t floor = 0;
            for (uint64_t fraction = 1; fraction < 32 && !floor; fraction++) {
                uint64_t candidate = preparation_peak +
                    (allowance - preparation_peak) * fraction / 32;
                fixture_t probe = budget_fixture(true, false, candidate);
                unsigned seen = 0;
                wl_session_set_delta_cb(probe.session, budget_observe, &seen);
                int rc = wl_session_step(probe.session);
                if (rc == ENOSPC && wl_session_budget_denied(probe.session) &&
                    COL_SESSION(probe.session)->spill_join_selected_count == 1)
                    floor = candidate;
                destroy(&probe);
            }
            CHECK(floor > preparation_peak && floor < allowance);
            budget_peak = 0;
            fixture_t f = budget_fixture(true, false, floor);
            wl_col_session_t *s = COL_SESSION(f.session);
            unsigned observed = 0;
            wl_session_set_delta_cb(f.session, budget_observe, &observed);
            int rc = wl_session_step(f.session);
            CHECK(rc == ENOSPC && wl_session_budget_denied(f.session));
            CHECK(s->spill_join_selected_count == 1 && s->arr_count == 0);
            CHECK(observed == 0 && budget_peak <= floor);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
            CHECK(!wl_columnar_source_access_gate_busy(&session_find_rel(s,
                "a")->source_access) &&
                !wl_columnar_source_access_gate_busy(&session_find_rel(s,
                "b")->source_access));
            destroy(&f);
        }
    }
    wl_columnar_memory_governor_test_admitted = NULL;
    wl_columnar_arrangement_test_reserve_denied = NULL;
}

typedef struct { int64_t key; int32_t diff; unsigned count; } later_event_t;
static void
later_observe(const char *name, const int64_t *row, uint32_t ncols,
    int32_t diff, void *context)
{
    later_event_t *event = context;
    CHECK(strcmp(name, "result") == 0 && ncols == 3);
    CHECK(row[0] == event->key && row[1] == row[0] + 10 &&
        row[2] == 100 && diff == event->diff);
    CHECK(++event->count == 1);
}

static void
later_steps(void)
{
    /* These established delta/retraction paths may build resident indexes. */
    fixture_t f = budget_fixture(true, false, 4 * 1024 * 1024);
    wl_col_session_t *s = COL_SESSION(f.session);
    CHECK(wl_session_step(f.session) == 0);
    CHECK(s->spill_join_selected_count == 1);
    budget_contents(&f);
    later_event_t event = {.key = 1, .diff = 1};
    wl_session_set_delta_cb(f.session, later_observe, &event);
    int64_t inserted[] = {1, 11}, removed[] = {0, 10};
    CHECK(wl_session_insert(f.session, "a", inserted, 1, 2) == 0);
    CHECK(wl_session_step(f.session) == 0 && event.count == 1);
    CHECK(session_find_rel(s, "result")->nrows == 4);
    event.key = 0; event.diff = -1; event.count = 0;
    CHECK(wl_session_remove(f.session, "a", removed, 1, 2) == 0);
    CHECK(wl_session_step(f.session) == 0 && event.count == 1);
    col_rel_t *out = session_find_rel(s, "result");
    CHECK(out->nrows == 3);
    unsigned seen = 0;
    for (uint32_t i = 0; i < out->nrows; i++) {
        int64_t key = out->columns[0][i];
        unsigned bit = key == 1 ? 1 : key == 2048 ? 2 : key == 4095 ? 4 : 0;
        CHECK(bit && !(seen & bit));
        CHECK(out->columns[1][i] == key + 10 &&
            out->columns[2][i] == 100);
        seen |= bit;
    }
    CHECK(seen == 7);
    CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
    destroy(&f);
}

static void
owned_left_transfer(void)
{
    for (unsigned fault = 0; fault < 10; fault++) {
        fixture_t f = fixture(true, 65536);
        wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *original = session_find_rel(s, "a"), *left = NULL;
        uint64_t baseline = reserved(&f);
        CHECK(wl_columnar_relation_deep_copy_governed(original, &left,
            f.governor) == 0);
        if (fault == 9) CHECK(col_rel_install_shared_view(left, original) == 0);
        uint64_t identity = left->relation_identity;
        col_rel_t *right = session_find_rel(s, "b");
        const char *lk[] = {left->col_names[0]}, *rk[] = {right->col_names[0]};
        uint32_t map[] = {0, 1, 3};
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "b",
                           .key_count = 1, .left_keys = lk, .right_keys = rk,
                           .project_count = 3, .project_indices = map,
                           .delta_mode = WL_DELTA_FORCE_FULL};
        eval_stack_t stack; eval_stack_init(&stack);
        CHECK(eval_stack_push(&stack, left, true) == 0);
        uint32_t *segments = malloc(2 * sizeof(*segments));
        CHECK(segments); segments[0] = 0; segments[1] = left->nrows;
        stack.items[0].seg_boundaries = segments; stack.items[0].seg_count = 1;
        take_count = 0; taken_bytes = 0; take_fault = fault;
        take_busy = fault == 1; take_session = s;
        wl_columnar_join_test_before_spill_take = before_take;
        wl_columnar_join_test_after_spill_take = after_take;
        wl_columnar_memory_reservation_init(&pressure);
        if (fault == 5) s->spill_join_index_bytes = 1;
        if (fault == 6) s->join_output_limit = 1;
        int rc = wl_columnar_join_op(&op, &stack, s);
        CHECK(rc == (fault == 1 ? EBUSY : fault == 2 || fault == 7 ? ENOMEM :
            fault == 3 || fault == 4 ? EIO : fault == 5 || fault == 8 ? ENOSPC :
            fault == 6 ? EOVERFLOW : 0));
        CHECK(take_count == (fault == 1 || fault == 9 ? 0u : 1u));
        if (take_count) CHECK(taken_identity == identity && taken_bytes > 0);
        CHECK(wl_session_budget_denied(f.session) == (fault == 5 ||
            fault == 8));
        if (fault == 8) CHECK(wl_columnar_memory_rollback(&pressure));
        if (fault == 1) {
            CHECK(stack.top == 1 && stack.items[0].rel == left &&
                stack.items[0].owned &&
                stack.items[0].seg_boundaries == segments);
            CHECK(left->relation_identity == identity && left->nrows == 18);
            CHECK(col_rel_source_reader_release(&take_pin) == 0);
            CHECK(eval_stack_drain(&stack) == 0);
        } else if (!rc) {
            CHECK(stack.top == 1);
            eval_entry_t result = eval_stack_pop(&stack);
            CHECK(result.rel->ncols == 3 && result.rel->nrows == 108);
            bool seen[18][18] = {{false}};
            for (uint32_t i = 0; i < result.rel->nrows; i++) {
                int64_t k = result.rel->columns[0][i],
                    x = result.rel->columns[1][i];
                int64_t y = result.rel->columns[2][i] - 100;
                CHECK(x >= 0 && x < 18 && y >= 0 && y < 18 &&
                    k == x % 3 && k == y % 3 && !seen[x][y]);
                seen[x][y] = true;
            }
            CHECK(eval_entry_dispose(&result) == 0);
        }
        CHECK(stack.top == 0 && reserved(&f) == baseline);
        CHECK(original->nrows == 18 && right->nrows == 18 &&
            !wl_columnar_source_access_gate_busy(&original->source_access));
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
        wl_columnar_join_test_before_spill_take = NULL;
        wl_columnar_join_test_after_spill_take = NULL;
        take_busy = false; take_fault = 0; take_session = NULL;
        fail_allocate_after = fail_read_after = 0; write_error = 0;
        CHECK(!wl_columnar_relation_test_fail_prepare_resize);
        destroy(&f);
    }
}

typedef struct { bool seen[18][18]; unsigned count; } chain_events_t;
static void
chain_observe(const char *name, const int64_t *row, uint32_t ncols,
    int32_t diff, void *context)
{
    chain_events_t *events = context;
    CHECK(strcmp(name, "result") == 0 && ncols == 4 && diff == 1);
    int64_t x = row[1], y = row[2] - 100;
    CHECK(x >= 0 && x < 18 && y >= 0 && y < 18 && row[0] == x % 3 &&
        row[0] == y % 3 && row[3] == row[2] + 200 && !events->seen[x][y]);
    events->seen[x][y] = true; events->count++;
}
static void
parsed_chain(void)
{
    for (unsigned enabled = 0; enabled < 2; enabled++) {
        fixture_t f = fixture_source(enabled, 65536,
                ".decl a(k: int64, v: int64)\n.decl b(k: int64, v: int64)\n"
                ".decl c(k: int64, v: int64)\n"
                ".decl result(k: int64, x: int64, y: int64, z: int64)\n"
                "result(k,x,y,z) :- a(k,x), b(k,y), c(y,z).\n");
        for (int64_t k = 100; k < 118; k++) {
            int64_t row[] = {k, k + 200};
            CHECK(wl_session_insert(f.session, "c", row, 1, 2) == 0);
        }
        unsigned joins = 0;
        for (uint32_t i = 0; i < f.plan->stratum_count; i++)
            for (uint32_t j = 0; j < f.plan->strata[i].relation_count; j++)
                for (uint32_t k =
                    0; k < f.plan->strata[i].relations[j].op_count;
                    k++)
                    joins += f.plan->strata[i].relations[j].ops[k].op ==
                        WL_PLAN_OP_JOIN;
        CHECK(joins == 2);
        take_count = 0; taken_bytes = 0;
        wl_columnar_join_test_after_spill_take = after_take;
        chain_events_t events = {0};
        wl_session_set_delta_cb(f.session, chain_observe, &events);
        CHECK(wl_session_step(f.session) == 0 && events.count == 108);
        wl_col_session_t *s = COL_SESSION(f.session);
        CHECK(take_count == (enabled ? 1u : 0u));
        CHECK(s->spill_join_selected_count == (enabled ? 2u : 0u));
        col_rel_t *out = session_find_rel(s, "result");
        CHECK(out && out->nrows == 108 && out->ncols == 4);
        memset(&events, 0, sizeof(events));
        for (uint32_t i = 0; i < out->nrows; i++) {
            int64_t row[] = {out->columns[0][i], out->columns[1][i],
                             out->columns[2][i], out->columns[3][i]};
            chain_observe("result", row, 4, 1, &events);
        }
        CHECK(events.count == 108);
        CHECK(wl_session_step(f.session) == 0 && events.count == 108);
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == 0);
        wl_columnar_join_test_after_spill_take = NULL;
        destroy(&f);
    }
}

int
main(void)
{
    for (unsigned enabled = 0; enabled < 2; enabled++) {
        fixture_t f = fixture(enabled != 0, 65536);
        wl_col_session_t *s = COL_SESSION(f.session);
        unsigned observed = 0;
        wl_session_set_delta_cb(f.session, observe, &observed);
        CHECK(wl_session_step(f.session) == 0);
        CHECK(observed == 108);
        contents(&f);
        CHECK((s->spill_join_selected_count > 0) == (enabled != 0));
        if (enabled) CHECK(s->arr_count == 0);
        destroy(&f);
    }
    owned_left_transfer();
    parsed_chain();
    fixed_budget();
    later_steps();
    projected_steps();
    owned_filters();
    failures();
    direct();
    excluded();
    progress();
    puts("join_spill: OK");
    return 0;
}
