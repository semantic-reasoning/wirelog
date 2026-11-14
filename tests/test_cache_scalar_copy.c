/* #2297: pin-free cache-original cloning and complete private-output abort. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/thread.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); \
                          abort(); \
                      } } while (0)
typedef wl_columnar_relation_cache_scalar_copy_attempt_t copy_t;

static wl_evaluation_control_t *
control_new(uint64_t limit, const void *owner)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, owner));
    CHECK(!wl_evaluation_control_begin(c, owner));
    return c;
}

static void
control_close(wl_evaluation_control_t *c, const void *owner, int semantic)
{
    if (wl_evaluation_control_active_attempt_id(c))
        CHECK(wl_evaluation_control_finish(c, owner, semantic, 0) == semantic);
    CHECK(!wl_evaluation_control_detach(c, owner));
    wl_evaluation_control_release(c);
}

/* A real fresh caller-owned grant per progress action, never reset original. */
static int
turn(copy_t *a, unsigned api, uint64_t limit, col_rel_t **out,
    bool *done, uint64_t *consumed)
{
    int owner = 0;
    wl_evaluation_control_t *c = control_new(limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    int rc;
    switch (api) {
    case 3: rc = wl_columnar_relation_cache_scalar_copy_finish(a, c, id, done);
        break;
    case 4: rc = wl_columnar_relation_cache_scalar_copy_abort(a, ECANCELED, c,
                id, done); break;
    case 5: rc = wl_columnar_relation_cache_scalar_copy_take(a, c, id, out);
        break;
    case 6: rc = wl_columnar_relation_cache_scalar_copy_ack(a, c, id); break;
    default: abort();
    }
    CHECK(wl_evaluation_control_finish(c, &owner, rc, 0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(c, &report));
    if (consumed) *consumed = report.consumed;
    control_close(c, &owner, 0);
    return rc;
}

static void
ready(copy_t *a)
{
    bool complete = false;
    for (unsigned calls = 0; !complete; ++calls) {
        CHECK(calls < 1000000);
        CHECK(!wl_columnar_relation_cache_scalar_copy_step(a, &complete));
    }
}

static void
drain(copy_t *a, bool aborting)
{
    bool done = false;
    for (unsigned calls = 0; !done; ++calls) {
        CHECK(calls < 1000000);
        CHECK(!turn(a, aborting ? 4 : 3, 512, NULL, &done, NULL));
    }
}

static col_rel_t *
relation_new(uint32_t width, uint32_t rows)
{
    col_rel_t *r = col_rel_new_auto("scalar-original", width);
    CHECK(r);
    int64_t *row = malloc((width ? width : 1u)*sizeof(*row)); CHECK(row);
    for (uint32_t i = 0; i < rows; ++i) {
        for (uint32_t c = 0; c < width; ++c) row[c] = (int64_t)i * 1000 + c;
        CHECK(!col_rel_append_row(r, row));
    }
    free(row);
    return r;
}

static void
compare(const col_rel_t *a, const col_rel_t *b)
{
    CHECK(a != b && a->storage_owner == a && b->storage_owner == b);
    CHECK(a->relation_identity != b->relation_identity);
    CHECK(a->ncols == b->ncols && a->nrows == b->nrows
        && a->capacity == b->capacity && a->base_nrows == b->base_nrows
        && a->sorted_nrows == b->sorted_nrows && a->run_count == b->run_count);
    CHECK(!strcmp(a->name, b->name));
    CHECK(a->declared_ncols == b->declared_ncols
        && a->has_graph_column == b->has_graph_column &&
        a->graph_col_idx == b->graph_col_idx);
    CHECK(a->schema_ok == b->schema_ok && !a->timestamps && !b->timestamps);
    CHECK((a->column_types == NULL) == (b->column_types == NULL));
    CHECK(!memcmp(a->run_ends, b->run_ends, sizeof(a->run_ends)));
    CHECK(!strcmp(a->schema.format, b->schema.format));
    for (uint32_t c = 0; c < a->ncols; ++c) {
        CHECK(!strcmp(a->col_names[c], b->col_names[c]));
        if (a->column_types) CHECK(a->column_types[c] == b->column_types[c]);
        CHECK(!strcmp(a->schema.children[c]->format,
            b->schema.children[c]->format));
        CHECK(!strcmp(a->schema.children[c]->name,
            b->schema.children[c]->name));
        CHECK(!memcmp(a->columns[c], b->columns[c], a->nrows*sizeof(int64_t)));
    }
}

static col_rel_t *
clone_scalar(const col_rel_t *source, wl_columnar_memory_governor_ref_t *g)
{
    copy_t a = {0}; int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, g, c,
        wl_evaluation_control_active_attempt_id(c)));
    ready(&a);
    col_rel_t *out = NULL;
    CHECK(turn(&a, 5, 128, &out, NULL, NULL) == EBUSY && !out);
    drain(&a, false);
    CHECK(turn(&a, 6, 128, NULL, NULL, NULL) == EBUSY);
    uint64_t consumed;
    CHECK(turn(&a, 5, 127, &out, NULL,
        &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !consumed && !out && a.output);
    /* Original is complete and no longer active: real independent TAKE/ACK. */
    control_close(c, &owner, 0);
    CHECK(!turn(&a, 5, 128, &out, NULL,
        &consumed) && consumed == 128 && out && !a.output);
    bool done = false;
    CHECK(!turn(&a, 4, 128, NULL, &done,
        NULL) && done && out->storage_owner == out);
    wl_columnar_relation_cache_scalar_copy_result_t result;
    CHECK(!wl_columnar_relation_cache_scalar_copy_query(&a, &result));
    CHECK(result.taken && !result.discarded && !result.semantic_result
        && result.rows == source->nrows && result.width == source->ncols);
    CHECK(turn(&a, 6, 127, NULL, NULL,
        &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !consumed && a.control_ref);
    CHECK(!turn(&a, 6, 128, NULL, NULL,
        &consumed) && consumed == 128 && !a.identity);
    return out;
}

static void
test_domains(void)
{
    const uint32_t widths[] = {0, 1, 65, 257}, rows[] = {0, 1, 3};
    for (unsigned w = 0; w < 4; ++w) for (unsigned r = 0; r < 3; ++r) {
            col_rel_t *s = relation_new(widths[w], rows[r]), *raw = NULL;
            CHECK(!wl_columnar_relation_deep_copy_governed(s, &raw, NULL));
            col_rel_t *out = clone_scalar(s, NULL); compare(out, raw);
            CHECK(out->nrows == rows[r] && out->ncols == widths[w]);
            for (uint32_t c = 0; c < out->ncols; ++c)
                for (uint32_t row = 0; row < out->nrows; ++row)
                    CHECK(out->columns[c][row] == (int64_t)row*1000+c);
            col_rel_destroy(out); col_rel_destroy(raw); col_rel_destroy(s);
        }
    col_rel_t *s = relation_new(3, 0);
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT,
                                     WIRELOG_TYPE_STRING};
    CHECK(!col_rel_set_column_types(s, types, 3));
    for (unsigned i = 0; i < 200; ++i) {
        double f = (double)i;
        int64_t row[] = {i, 0, i+1};
        memcpy(&row[1], &f, sizeof(f));
        CHECK(!col_rel_append_row(s, row));
    }
    CHECK(s->capacity > s->nrows);
    /* Fixture owns both genuine writers before installing literal raw bits.
     * FLOAT -0 and NaN are intentionally not append-canonicalized by copy. */
    wl_columnar_source_access_writer_t descriptor = {0}, storage = {0};
    CHECK(!wl_columnar_source_access_writer_acquire(&s->descriptor_access,
        &descriptor));
    CHECK(!wl_columnar_source_access_writer_acquire(&s->source_access,
        &storage));
    s->columns[1][0] = INT64_MIN;
    s->columns[1][1] = INT64_C(0x7ff8000000000001);
    s->columns[2][0] = INT64_C(0x0102030405060708);
    s->base_nrows = 199; s->sorted_nrows = 200; s->run_count = 2;
    s->run_ends[0] = 97; s->run_ends[1] = 200;
    s->run_ends[COL_MAX_RUNS-1] = 123;
    s->has_graph_column = true; s->graph_col_idx = 2; s->declared_ncols = 2;
    CHECK(!wl_columnar_source_access_writer_release(&storage));
    CHECK(!wl_columnar_source_access_writer_release(&descriptor));
    col_rel_t *raw = NULL;
    CHECK(!wl_columnar_relation_deep_copy_governed(s, &raw, NULL));
    col_rel_t *out = clone_scalar(s, NULL); compare(out, raw);
    CHECK(out->columns[1][0] == INT64_MIN &&
        out->columns[1][1] == INT64_C(0x7ff8000000000001));
    col_rel_destroy(out); col_rel_destroy(raw);
    /* Invalid run tail is normalized exactly as raw; unused entries are zero. */
    CHECK(!wl_columnar_source_access_writer_acquire(&s->descriptor_access,
        &descriptor));
    s->run_ends[1] = 199;
    CHECK(!wl_columnar_source_access_writer_release(&descriptor));
    CHECK(!wl_columnar_relation_deep_copy_governed(s, &raw, NULL));
    out = clone_scalar(s, NULL); compare(out, raw);
    CHECK(!out->run_count && !out->sorted_nrows);
    col_rel_destroy(out); col_rel_destroy(raw); col_rel_destroy(s);
}

static void
raw_clear(wl_col_session_t *s)
{
    col_mat_cache_clear(&s->mat_cache);
    for (uint32_t i = 0; i < s->arr_count; ++i) {
        free(s->arr_entries[i].rel_name); free(s->arr_entries[i].key_cols);
        arr_free_contents(&s->arr_entries[i].arr);
        col_arr_detach_memory_governor(&s->arr_entries[i].arr);
    }
    free(s->arr_entries);
    col_session_free_diff_arrangements(s);
    session_rel_free_hash(s);
}

static void
test_producers(void)
{
    for (unsigned diff = 0; diff < 2; ++diff)
        for (unsigned metadata = 0; metadata < 4; ++metadata) {
            wl_col_session_t session = {0};
            wl_mem_ledger_init(&session.mem_ledger, 0);
            col_rel_t *left = relation_new(2, 3), *right = relation_new(2, 4);
            if (metadata == 1) {
                CHECK(!col_rel_enable_timestamps(left));
                CHECK(!col_rel_enable_timestamps(right));
            } else if (metadata == 2) {
                wl_columnar_source_access_writer_t writer = {0};
                CHECK(!wl_columnar_source_access_writer_acquire(
                        &left->descriptor_access, &writer));
                left->has_graph_column = true; left->graph_col_idx = 1;
                left->declared_ncols = 1;
                CHECK(!wl_columnar_source_access_writer_release(&writer));
            } else if (metadata == 3) {
                col_rel_logical_col_t logical[] = {
                    {WIRELOG_COMPOUND_KIND_NONE, 0, 0},
                    {WIRELOG_COMPOUND_KIND_SIDE, 2, 1}
                };
                CHECK(!col_rel_apply_compound_schema(left, logical, 2));
            }
            col_rel_t *registry[] = {right};
            session.rels = registry; session.nrels = 1;
            const char *keys[] = {"col0"};
            wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN,
                               .right_relation = "scalar-original",
                               .left_keys = keys, .right_keys = keys,
                               .key_count = 1,
                               .delta_mode = WL_DELTA_FORCE_FULL,
                               .materialized = true};
            eval_stack_t stack = {0};
            CHECK(!eval_stack_push(&stack, left, false));
            CHECK(!(diff ? col_op_join_diff(&op, &stack, &session)
                     : col_op_join(&op, &stack, &session)));
            CHECK(stack.top == 1 && stack.items[0].rel->nrows == 3);
            CHECK(session.mat_cache.count == 1 &&
                !session.mat_cache.active_pins);
            col_rel_t *original = session.mat_cache.entries[0].result;
            CHECK(original != stack.items[0].rel && !original->timestamps
                && !original->has_graph_column &&
                !original->compound_arity_map);
            CHECK(session.mat_cache.entries[0].op_signature ==
                col_mat_cache_join_signature(&op, diff != 0));
            col_rel_t *raw = NULL;
            CHECK(!wl_columnar_relation_deep_copy_governed(original, &raw,
                NULL));
            col_rel_t *out = clone_scalar(original, NULL); compare(out, raw);
            CHECK(out->ncols == 4 && out->nrows == 3);
            CHECK(!session.mat_cache.active_pins &&
                session.mat_cache.count == 1);
            col_rel_destroy(out); col_rel_destroy(raw);
            CHECK(!eval_stack_drain(&stack));
            raw_clear(&session); col_rel_destroy(left); col_rel_destroy(right);
        }
}

static wl_columnar_memory_governor_ref_t *
governor_new(uint64_t usable)
{
    wl_columnar_memory_resolution_t r = {.usable_bytes = usable,
                                         .budget_bytes = usable,
                                         .mode =
                                             WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r);
    CHECK(g); return g;
}

static void
test_failures(void)
{
    const wl_columnar_relation_cache_scalar_copy_phase_t stops[] = {
        WL_COLUMNAR_RELATION_CACHE_COPY_ACQUIRE,
        WL_COLUMNAR_RELATION_CACHE_COPY_VALIDATE,
        WL_COLUMNAR_RELATION_CACHE_COPY_OWNER,
        WL_COLUMNAR_RELATION_CACHE_COPY_COLUMN,
        WL_COLUMNAR_RELATION_CACHE_COPY_BUILD,
        WL_COLUMNAR_RELATION_CACHE_COPY_CELL,
        WL_COLUMNAR_RELATION_CACHE_COPY_RUN_VALIDATE,
        WL_COLUMNAR_RELATION_CACHE_COPY_RUN_COPY,
        WL_COLUMNAR_RELATION_CACHE_COPY_METADATA,
        WL_COLUMNAR_RELATION_CACHE_COPY_FINAL,
        WL_COLUMNAR_RELATION_CACHE_COPY_LATCH,
        WL_COLUMNAR_RELATION_CACHE_COPY_READY
    };
    wl_columnar_memory_governor_ref_t *g = governor_new(UINT64_MAX);
    col_rel_t *source = relation_new(3, 2);
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(source);
    for (unsigned mode = 0; mode < 2; ++mode)
        for (unsigned stop = 0; stop < sizeof(stops)/sizeof(stops[0]); ++stop) {
            copy_t a = {0}; int owner = 0;
            wl_evaluation_control_t *c = control_new(UINT64_C(1000000000),
                    &owner);
            CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, g,
                c,
                wl_evaluation_control_active_attempt_id(c)));
            bool complete = false;
            for (unsigned n = 0; a.phase != stops[stop]; ++n) {
                CHECK(n < 10000 &&
                    !wl_columnar_relation_cache_scalar_copy_step(&a,
                    &complete));
            }
            int stopped =
                mode ? WL_EVALUATION_CONTROL_WORK_LIMIT :
                WL_EVALUATION_CONTROL_CANCELLED;
            if (mode) CHECK(wl_evaluation_control_charge(c,
                    UINT64_C(1000000000)) == stopped);
            else wl_evaluation_control_request_cancel(c);
            if (a.phase < WL_COLUMNAR_RELATION_CACHE_COPY_READY)
                CHECK(wl_columnar_relation_cache_scalar_copy_step(&a,
                    &complete) == stopped);
            uint64_t consumed;
            bool done = false;
            CHECK(turn(&a, 4, 127, NULL, &done,
                &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT && !consumed);
            control_close(c, &owner, ECANCELED);
            drain(&a, true);
            CHECK(!a.output && !a.retirement_started && !a.source && !a.held);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_relation_cache_scalar_copy_result_t result;
            CHECK(!wl_columnar_relation_cache_scalar_copy_query(&a,
                &result) && result.discarded);
            CHECK(result.semantic_result ==
                (stops[stop] ==
                WL_COLUMNAR_RELATION_CACHE_COPY_READY ? 0 : stopped));
            CHECK(!turn(&a, 6, 128, NULL, NULL, NULL));
            CHECK(wl_columnar_relation_snapshot_equal(before,
                wl_columnar_relation_snapshot(source)));
            CHECK(source->columns[2][1] == 1002);
            /* ACKed record reuses true terminal retirement storage lazily. */
            c = control_new(UINT64_C(1000000000), &owner);
            CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, g,
                c,
                wl_evaluation_control_active_attempt_id(c)));
            ready(&a); drain(&a, true);
            CHECK(!turn(&a, 6, 128, NULL, NULL, NULL));
            control_close(c, &owner, 0);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
        }
    unsigned allocation_failures = 0;
    bool saw_partial_schema = false, allocator_success = false;
    for (unsigned allocation = 1; allocation <= 128; ++allocation) {
        copy_t a = {0}; int owner = 0; bool complete = false;
        wl_evaluation_control_t *c = control_new(0, &owner);
        CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, g, c,
            wl_evaluation_control_active_attempt_id(c)));
        wl_columnar_relation_test_constructor_fail_allocation = allocation;
        int rc = 0;
        for (unsigned n = 0; !complete && !rc; ++n) {
            CHECK(n < 10000);
            rc = wl_columnar_relation_cache_scalar_copy_step(&a, &complete);
        }
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        if (rc) {
            CHECK(rc == ENOMEM && !complete && !a.output
                && a.constructor_initialized);
            ++allocation_failures;
            if (a.constructor.schema_initialized && a.constructor.relation
                && a.constructor.relation->schema.n_children > 0)
                saw_partial_schema = true;
        } else {
            CHECK(complete && a.output);
            allocator_success = true;
        }
        drain(&a, true);
        wl_columnar_relation_cache_scalar_copy_result_t result;
        CHECK(!wl_columnar_relation_cache_scalar_copy_query(&a, &result)
            && result.discarded && result.semantic_result == rc);
        CHECK(!turn(&a, 6, 128, NULL, NULL, NULL));
        control_close(c, &owner, rc);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        if (allocator_success) break;
    }
    CHECK(allocator_success && allocation_failures > 10 && saw_partial_schema);
    wl_columnar_memory_governor_ref_t *limited_governor = governor_new(1);
    copy_t a = {0}; int owner = 0; bool complete = false;
    wl_evaluation_control_t *c = control_new(0, &owner);
    CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source,
        limited_governor, c,
        wl_evaluation_control_active_attempt_id(c)));
    int rc = 0;
    while (!rc) rc = wl_columnar_relation_cache_scalar_copy_step(&a, &complete);
    CHECK(rc == ENOSPC); drain(&a, true);
    CHECK(!turn(&a, 6, 128, NULL, NULL, NULL));
    control_close(c, &owner, rc);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            limited_governor)));
    wl_columnar_memory_governor_ref_release(limited_governor);
    col_rel_t *future = clone_scalar(source, g); col_rel_destroy(future);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    col_rel_destroy(source); wl_columnar_memory_governor_ref_release(g);
}

static void
test_aliases(void)
{
    col_rel_t *source = relation_new(3, 2), *alias = relation_new(3, 1);
    CHECK(!col_rel_install_shared_view(alias, source));
    CHECK(col_rel_storage_alias_borrow_count(source) == 1);
    copy_t a = {0}; int owner = 0; bool complete = false;
    wl_evaluation_control_t *c = control_new(0, &owner);
    CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, NULL, c,
        wl_evaluation_control_active_attempt_id(c)));
    while (!a.held) CHECK(!wl_columnar_relation_cache_scalar_copy_step(&a,
            &complete));
    int64_t row[] = {9, 9, 9};
    CHECK(col_rel_append_row(source, row) == EBUSY);
    CHECK(col_rel_destroy_checked(source) == EBUSY);
    col_rel_t *raw = NULL;
    CHECK(!wl_columnar_relation_deep_copy_governed(source, &raw, NULL));
    ready(&a); drain(&a, false);
    col_rel_t *out = NULL; CHECK(!turn(&a, 5, 128, &out, NULL, NULL));
    compare(out, raw);
    CHECK(alias->columns[2][1] == 1002 &&
        col_rel_storage_alias_borrow_count(source) == 1);
    CHECK(!turn(&a, 6, 128, NULL, NULL, NULL)); control_close(c, &owner, 0);
    col_rel_destroy(alias); CHECK(!col_rel_storage_alias_borrow_count(source));
    CHECK(!col_rel_append_row(source, row));
    col_rel_destroy(out); col_rel_destroy(raw); col_rel_destroy(source);
}

/* These are explicit leaf-boundary refusals, not natural cache-HIT tests. */
static void
test_unsupported_boundary(void)
{
    for (unsigned mode = 0; mode < 2; ++mode) {
        col_rel_t *s = relation_new(2, 2);
        if (!mode) CHECK(!col_rel_enable_timestamps(s));
        else {
            col_rel_logical_col_t logical[] = {
                {WIRELOG_COMPOUND_KIND_NONE, 0, 0},
                {WIRELOG_COMPOUND_KIND_SIDE, 2, 1}
            };
            CHECK(!col_rel_apply_compound_schema(s, logical, 2));
        }
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(s);
        copy_t a = {0}; int owner = 0; bool complete = false;
        wl_evaluation_control_t *c = control_new(0, &owner);
        CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, s, NULL, c,
            wl_evaluation_control_active_attempt_id(c)));
        int rc = 0;
        while (!rc) rc = wl_columnar_relation_cache_scalar_copy_step(&a,
                    &complete);
        CHECK(rc == ENOTSUP && !a.output);
        drain(&a, true); CHECK(!turn(&a, 6, 128, NULL, NULL, NULL));
        CHECK(wl_columnar_relation_snapshot_equal(before,
            wl_columnar_relation_snapshot(s)));
        control_close(c, &owner, rc); col_rel_destroy(s);
    }
}

static unsigned reentries;
static void
reenter(copy_t *a, wl_evaluation_control_t *c, unsigned api)
{
    (void)api;
    copy_t before = *a;
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    bool ready_value = true, done = true;
    col_rel_t *out = NULL;
    wl_columnar_relation_cache_scalar_copy_result_t result;
    CHECK(wl_columnar_relation_cache_scalar_copy_begin(a, NULL, NULL, c,
        id) == EBUSY);
    CHECK(wl_columnar_relation_cache_scalar_copy_step(a,
        &ready_value) == EBUSY && ready_value);
    CHECK(wl_columnar_relation_cache_scalar_copy_finish(a, c, id,
        &done) == EBUSY && done);
    CHECK(wl_columnar_relation_cache_scalar_copy_abort(a, ECANCELED, c, id,
        &done) == EBUSY && done);
    CHECK(wl_columnar_relation_cache_scalar_copy_take(a, c, id,
        &out) == EBUSY && !out);
    CHECK(wl_columnar_relation_cache_scalar_copy_ack(a, c, id) == EBUSY);
    CHECK(wl_columnar_relation_cache_scalar_copy_query(a, &result) == EBUSY);
    CHECK(!memcmp(&before, a, sizeof(before)));
    reentries++;
}

typedef struct { copy_t *a; wl_evaluation_control_t *fund; uint64_t id;
} foreign_t;
static void *
foreign_call(void *arg)
{
    foreign_t *f = arg; bool done = true, complete = true;
    col_rel_t *out = NULL;
    wl_columnar_relation_cache_scalar_copy_result_t result;
    CHECK(wl_columnar_relation_cache_scalar_copy_step(f->a,
        &complete) == EINVAL && complete);
    CHECK(wl_columnar_relation_cache_scalar_copy_finish(f->a, f->fund, f->id,
        &done) == EINVAL && done);
    CHECK(wl_columnar_relation_cache_scalar_copy_abort(f->a, ECANCELED, f->fund,
        f->id, &done) == EINVAL && done);
    CHECK(wl_columnar_relation_cache_scalar_copy_take(f->a, f->fund, f->id,
        &out) == EINVAL && !out);
    CHECK(wl_columnar_relation_cache_scalar_copy_query(f->a,
        &result) == EINVAL);
    CHECK(wl_columnar_relation_cache_scalar_copy_ack(f->a, f->fund,
        f->id) == EINVAL);
    return NULL;
}

static void
test_authority(void)
{
    col_rel_t *source = relation_new(3, 2);
    copy_t a = {0}; int owner = 0, fund_owner = 0;
    bool complete = true, done = true;
    wl_evaluation_control_t *c = control_new(0, &owner),
        *fund = control_new(0, &fund_owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(c),
        fid = wl_evaluation_control_active_attempt_id(fund);
    reentries = 0;
    wl_columnar_relation_cache_scalar_copy_test_checkpoint = reenter;
    CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, NULL, c,
        id));
    copy_t copied = a;
    CHECK(wl_columnar_relation_cache_scalar_copy_step(&copied,
        &complete) == EINVAL && complete);
    foreign_t foreign = {&a, fund, fid}; wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, foreign_call, &foreign));
    CHECK(!wl_thread_join(&thread));
    CHECK(wl_columnar_relation_cache_scalar_copy_step(&a, &a.held) == EINVAL);
    CHECK(wl_columnar_relation_cache_scalar_copy_finish(&a, fund, fid,
        &a.busy) == EINVAL);
    CHECK(wl_columnar_relation_cache_scalar_copy_finish(&a, c, id,
        &done) == EBUSY);
    CHECK(wl_columnar_relation_cache_scalar_copy_begin(&a, source, NULL, c,
        id) == EBUSY);
    /* Actual canonical storage writer causes a real partial descriptor lease;
     * independently funded abort must release it, without acquiring new gates. */
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(!wl_columnar_source_access_writer_acquire(&source->source_access,
        &writer));
    CHECK(!wl_columnar_relation_cache_scalar_copy_step(&a, &complete));
    CHECK(a.reader.owner == &source->descriptor_access &&
        a.reader.acquire_stage == 1 && !a.held);
    int rc = wl_columnar_relation_cache_scalar_copy_step(&a, &complete);
    CHECK(!rc && !a.held && a.reader.acquire_stage == 1);
    CHECK(!wl_columnar_source_access_writer_release(&writer));
    drain(&a, true);
    CHECK(!wl_columnar_relation_cache_scalar_copy_ack(&a, fund, fid));
    CHECK(!a.identity && !a.source && !a.output);
    /* Reuse, normal success, wrong/same/stale funding and overlap refusals. */
    CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, NULL, c,
        id));
    ready(&a); drain(&a, false);
    CHECK(wl_columnar_relation_cache_scalar_copy_take(&a, c, id,
        &source) == EBUSY);
    col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_cache_scalar_copy_take(&a, c, id,
        &out) == EINVAL && !out);
    CHECK(wl_columnar_relation_cache_scalar_copy_take(&a, fund, fid+1,
        &out) == EINVAL && !out);
    CHECK(wl_columnar_relation_cache_scalar_copy_take(&a, fund, fid,
        &a.output) == EINVAL);
    CHECK(!wl_columnar_relation_cache_scalar_copy_take(&a, fund, fid, &out));
    CHECK(!wl_columnar_relation_cache_scalar_copy_ack(&a, fund, fid));
    CHECK(reentries > 40);
    wl_columnar_relation_cache_scalar_copy_test_checkpoint = NULL;
    control_close(c, &owner, 0); control_close(fund, &fund_owner, 0);
    col_rel_destroy(out); col_rel_destroy(source);
}

static void
commit_cancel(wl_evaluation_control_t *c, unsigned phase)
{
    if (phase == 2) wl_evaluation_control_request_cancel(c);
}

static void
test_constructor_refund_boundaries(bool allocation_failure)
{
    col_rel_t *source = relation_new(3, 2);
    wl_columnar_memory_governor_ref_t *g = governor_new(UINT64_MAX);
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(g);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    unsigned char *peer_buffer = malloc(17); CHECK(peer_buffer);
    memset(peer_buffer, 0x17, 17);
    CHECK(wl_columnar_memory_reserve(governor, 17, &peer));
    CHECK(wl_columnar_memory_commit(&peer, peer_buffer));
    copy_t a = {0}; int owner = 0; bool complete = false;
    wl_evaluation_control_t *c = control_new(0, &owner);
    uint64_t original_id = wl_evaluation_control_active_attempt_id(c);
    CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, g, c,
        original_id));
    wl_columnar_relation_test_constructor_committed =
        allocation_failure ? NULL : commit_cancel;
    wl_columnar_relation_test_constructor_fail_allocation =
        allocation_failure ? 1 : 0;
    int rc = 0;
    while (!rc) rc = wl_columnar_relation_cache_scalar_copy_step(&a, &complete);
    wl_columnar_relation_test_constructor_committed = NULL;
    wl_columnar_relation_test_constructor_fail_allocation = 0;
    CHECK(rc == (allocation_failure ? ENOMEM
        : WL_EVALUATION_CONTROL_CANCELLED) && !a.output);
    /* Partial constructor storage stays owned until independently funded
     * physical retirement and token credit, including after original END. */
    CHECK(wl_columnar_memory_reserved(governor) > 17);
    CHECK(wl_evaluation_control_finish(c, &owner, rc, 0) == rc);
    CHECK(!wl_evaluation_control_begin(c, &owner));
    uint64_t successor_id = wl_evaluation_control_active_attempt_id(c);
    CHECK(successor_id && successor_id != original_id);
    unsigned retired_turns = 0, refunded_tokens = 0;
    while (a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_CONSTRUCTOR_END) {
        CHECK(retired_turns++ < 1000);
        uint64_t consumed;
        bool done = true;
        CHECK(turn(&a, 4, 191, NULL, &done,
            &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !consumed && !done);
        if (a.constructor_cleanup >= WL_COLUMNAR_RELATION_CACHE_CTOR_TOKEN0
            && a.constructor_cleanup <= WL_COLUMNAR_RELATION_CACHE_CTOR_TOKEN2
            && a.constructor.admitted[a.constructor_cleanup
            - WL_COLUMNAR_RELATION_CACHE_CTOR_TOKEN0]) {
            /* Real allocator failure leaves RESERVED tokens; a late stop
             * leaves COMMITTED tokens. Both reach this stage only after
             * their actual physical storage has been retired. */
            unsigned index = a.constructor_cleanup
                - WL_COLUMNAR_RELATION_CACHE_CTOR_TOKEN0;
            wl_columnar_memory_reservation_t *token =
                &a.constructor.tokens[index];
            uint64_t state = atomic_load_explicit(&token->state,
                    memory_order_acquire);
            uint64_t owner_bits = atomic_load_explicit(&token->owner_bits,
                    memory_order_relaxed);
            uint64_t bytes = token->bytes;
            uint64_t reserved = wl_columnar_memory_reserved(governor);
            bool committed = a.constructor.committed[index];
            wl_columnar_relation_cache_constructor_cleanup_t stage =
                a.constructor_cleanup;
            CHECK(state == (allocation_failure
                ? WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
                : WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED));
            CHECK(committed == !allocation_failure && bytes);
            CHECK(!a.constructor.relation && !a.constructor.name
                && !a.constructor.columns && !a.constructor.scratch
                && !a.constructor.names && !a.constructor.types
                && !a.constructor.timestamps && a.constructor.ref_held);
            CHECK(turn(&a, 4, 312, NULL, &done,
                &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
                && consumed == 192 && !done);
            CHECK(a.constructor_cleanup == stage
                && a.constructor.admitted[index]
                && a.constructor.committed[index] == committed
                && a.constructor.ref_held
                && token->identity == token && token->bytes == bytes
                && atomic_load_explicit(&token->state,
                memory_order_acquire) == state
                && atomic_load_explicit(&token->owner_bits,
                memory_order_relaxed) == owner_bits
                && wl_columnar_memory_reserved(governor) == reserved);
            CHECK(!turn(&a, 4, 313, NULL, &done, &consumed)
                && consumed == 313 && !done);
            CHECK(!a.constructor.admitted[index]
                && !a.constructor.committed[index]
                && a.constructor.ref_held
                && atomic_load_explicit(&token->state,
                memory_order_acquire)
                == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
                && wl_columnar_memory_reserved(governor) == reserved - bytes);
            ++refunded_tokens;
        } else {
            CHECK(!turn(&a, 4, 192, NULL, &done, &consumed)
                && consumed == 192 && !done);
        }
        CHECK(!wl_evaluation_control_test_active_consumed(c, successor_id,
            &consumed) && !consumed);
        CHECK(peer.bytes == 17 && peer_buffer[16] == 0x17);
    }
    /* The first failed allocation owns only the descriptor reservation;
     * cancellation after the last commit owns all three real reservations. */
    CHECK(retired_turns > 1
        && refunded_tokens == (allocation_failure ? 1u : 3u)
        && !a.constructor.ref_held
        && wl_columnar_memory_reserved(governor) == 17);
    drain(&a, true);
    wl_columnar_relation_cache_scalar_copy_result_t result;
    CHECK(!wl_columnar_relation_cache_scalar_copy_query(&a, &result)
        && result.discarded && result.semantic_result == rc);
    CHECK(!turn(&a, 6, 128, NULL, NULL, NULL));
    uint64_t successor_consumed;
    CHECK(!wl_evaluation_control_test_active_consumed(c, successor_id,
        &successor_consumed) && !successor_consumed);
    CHECK(wl_columnar_memory_reserved(governor) == 17);
    control_close(c, &owner, 0);
    free(peer_buffer); CHECK(wl_columnar_memory_release(&peer));
    CHECK(!wl_columnar_memory_reserved(governor));
    col_rel_destroy(source); wl_columnar_memory_governor_ref_release(g);
}

static bool chunk_armed, chunk_after;
static unsigned chunk_stop_kind, chunk_stops;

static void
chunk_stop(copy_t *a, wl_evaluation_control_t *funding)
{
    CHECK(a->busy && chunk_armed);
    chunk_armed = false; ++chunk_stops;
    reenter(a, funding, 3);
    if (!chunk_stop_kind) wl_evaluation_control_request_cancel(funding);
    else CHECK(wl_evaluation_control_charge_attempt(funding,
            wl_evaluation_control_active_attempt_id(funding), UINT64_C(1000000))
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}

static void
chunk_before(copy_t *a, wl_evaluation_control_t *funding, unsigned api)
{
    (void)api;
    if (chunk_armed && !chunk_after) chunk_stop(a, funding);
}

static void
chunk_after_action(copy_t *a, wl_evaluation_control_t *funding,
    wl_columnar_relation_cache_scalar_copy_phase_t phase)
{
    (void)phase;
    if (chunk_armed && chunk_after) chunk_stop(a, funding);
}

static void
test_retirement_chunk_stops(void)
{
    const unsigned chunks = (sizeof(wl_columnar_relation_scalar_retirement_t)+
        63)/64;
    for (unsigned original_mode = 0; original_mode < 3; ++original_mode)
        for (unsigned financial_mode = 0; financial_mode < 4;
            ++financial_mode){
            wl_columnar_memory_governor_ref_t *g = governor_new(UINT64_MAX);
            col_rel_t *source = relation_new(3, 2);
            col_relation_snapshot_t source_snapshot =
                wl_columnar_relation_snapshot(source);
            copy_t a = {0}; int owner = 0; bool done = false;
            wl_evaluation_control_t *original =
                control_new(UINT64_C(1000000000), &owner);
            uint64_t original_id =
                wl_evaluation_control_active_attempt_id(original);
            CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, g,
                original, original_id));
            ready(&a); drain(&a, false);
            CHECK(a.output && !a.held && !a.source);
            memset(&a.retirement, 0xa5, sizeof(a.retirement));
            wl_evaluation_control_report_t frozen = {0}; uint64_t successor = 0;
            if (original_mode) {
                CHECK(wl_evaluation_control_finish(original, &owner, 7, 0) == 7
                    && !wl_evaluation_control_report(original, &frozen));
                if (original_mode == 2) {
                    CHECK(!wl_evaluation_control_begin(original, &owner));
                    successor =
                        wl_evaluation_control_active_attempt_id(original);
                }
            }
            CHECK(!turn(&a, 4, 128, NULL, &done, NULL)
                && a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_ZERO &&
                !done);
            unsigned zeros = 0, tokens = 0, headers = 0, clears = 0;
            chunk_stops = 0; chunk_stop_kind = financial_mode%2;
            chunk_after = financial_mode >= 2;
            wl_columnar_relation_cache_scalar_copy_test_checkpoint =
                chunk_before;
            wl_columnar_relation_cache_scalar_copy_test_after_action =
                chunk_after_action;
            for (unsigned call = 0; !done; ++call) {
                CHECK(call < 10000);
                wl_columnar_relation_cache_scalar_copy_phase_t phase = a.phase;
                bool target = phase ==
                    WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_ZERO
                    || phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_TOKENS
                    || phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_BEGIN
                    || phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_CLEAR;
                uint64_t fee = phase ==
                    WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_TOKENS ? 192
                    : target ? 256 : 1024;
                size_t offset = a.retirement_offset;
                unsigned token = a.retirement_token_index;
                col_rel_t *output = a.output;
                wl_columnar_relation_scalar_retirement_t image;
                if (target) memcpy(&image, &a.retirement, sizeof(image));
                chunk_armed = target;
                uint64_t used = UINT64_MAX;
                int rc = turn(&a, 3, fee, NULL, &done, &used);
                CHECK(!chunk_armed);
                if (target) {
                    CHECK(rc ==
                        (chunk_stop_kind ? WL_EVALUATION_CONTROL_WORK_LIMIT
                        : WL_EVALUATION_CONTROL_CANCELLED));
                    if (!chunk_after) {
                        CHECK(!used && !done && a.phase == phase
                            && a.retirement_offset == offset &&
                            a.retirement_token_index == token
                            && a.output == output && !memcmp(&image,
                            &a.retirement, sizeof(image)));
                        CHECK(!turn(&a, 3, fee, NULL, &done, &used));
                    }
                    CHECK(used == fee);
                    if (phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_ZERO) {
                        ++zeros;
                        size_t expected = offset+64;
                        if (expected >
                            sizeof(a.retirement)) expected =
                                sizeof(a.retirement);
                        CHECK(a.retirement_offset == expected &&
                            !a.retirement_started && a.output);
                        const unsigned char *bytes =
                            (const unsigned char *)&a.retirement;
                        for (size_t byte = 0; byte < sizeof(a.retirement);
                            ++byte)
                            CHECK(bytes[byte] == (byte < expected ? 0 : 0xa5));
                    } else if (phase ==
                        WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_TOKENS) {
                        ++tokens;
                        CHECK(a.retirement_token_index == token+1 &&
                            !a.retirement_started
                            && !a.retirement.storage.identity && a.output);
                    } else if (phase ==
                        WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_BEGIN) {
                        ++headers;
                        CHECK(a.retirement_started && a.output &&
                            a.retirement.storage.identity
                            == (uintptr_t)&a.retirement.storage);
                    } else {
                        ++clears;
                        CHECK(!a.output && !a.held && !a.source
                            && !wl_columnar_memory_reserved(
                                wl_columnar_memory_governor_ref_get(g)));
                        CHECK(done == (clears == chunks)
                            && a.retirement_started == !done);
                    }
                } else CHECK(!rc);
                CHECK(wl_columnar_relation_snapshot_equal(source_snapshot,
                    wl_columnar_relation_snapshot(source)));
                if (original_mode) {
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(original, &report)
                        && !memcmp(&report, &frozen, sizeof(report)));
                    if (successor) CHECK(
                            !wl_evaluation_control_test_active_consumed(
                                original, successor, &used) && !used);
                }
            }
            wl_columnar_relation_cache_scalar_copy_test_checkpoint = NULL;
            wl_columnar_relation_cache_scalar_copy_test_after_action = NULL;
            CHECK(zeros == chunks && tokens == 4 && headers == 1 &&
                clears == chunks
                && chunk_stops == zeros+tokens+headers+clears
                && a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_DISCARDED
                && !a.retirement_offset && !a.retirement_token_index &&
                !a.semantic_result);
            CHECK(!turn(&a, 6, 128, NULL, NULL, NULL));
            if (original_mode) {
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(original, &report)
                    && !memcmp(&report, &frozen, sizeof(report)));
            }
            control_close(original, &owner, 0);
            col_rel_destroy(source);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g);
        }
}

static void
test_retirement_funding(void)
{
    col_rel_t *source = relation_new(3, 2);
    wl_columnar_memory_governor_ref_t *g = governor_new(UINT64_MAX);
    copy_t a = {0}; int owner = 0; bool done = false;
    wl_evaluation_control_t *c = control_new(0, &owner);
    CHECK(!wl_columnar_relation_cache_scalar_copy_begin(&a, source, g, c,
        wl_evaluation_control_active_attempt_id(c)));
    ready(&a); drain(&a, false);
    /* Saved success may be discarded by its parent; source is already drained.
     * Every cleanup state gets a short real grant before an exact/fitting one. */
    CHECK(!turn(&a, 4, 128, NULL, &done, NULL));
    CHECK(!a.retirement_started);
    memset(&a.retirement, 0xa5, sizeof(a.retirement));
    control_close(c, &owner, 0);
    c = NULL; /* Sole record-owned original ref remains; cleanup never polls it. */
    bool saw_init = false, saw_begin = false, saw_prepare = false,
        saw_step = false, saw_credit = false;
    for (unsigned n = 0; !done; ++n) {
        CHECK(n < 10000);
        uint64_t fee = 128, consumed = 0;
        if (a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_ZERO
            || a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_CLEAR) {
            fee = 256;
            saw_init |= a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_ZERO;
        } else if (a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_TOKENS) {
            fee = 192;
        } else if (a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_BEGIN) {
            fee = 256; saw_begin = true;
        } else if (a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_PREPARE) {
            fee = 192; saw_prepare = true;
        } else if (a.phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_STEP) {
            fee = 256; saw_step = true;
        }
        wl_columnar_relation_cache_scalar_copy_phase_t phase = a.phase;
        CHECK(turn(&a, 3, fee-1, NULL, &done,
            &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !consumed && a.phase == phase && !done);
        if (phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_PREPARE) {
            unsigned native_phase = a.retirement.phase;
            CHECK(turn(&a, 3, 383, NULL, &done,
                &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
                && consumed == 192 && !done && a.phase == phase
                && a.retirement.phase == native_phase);
            CHECK(!turn(&a, 3, 384, NULL, &done, &consumed)
                && consumed == 384);
        } else if (phase == WL_COLUMNAR_RELATION_CACHE_COPY_RETIRE_STEP) {
            unsigned native_phase = a.retirement.phase;
            uint64_t reserved = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(g));
            CHECK(turn(&a, 3, 256, NULL, &done,
                &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
                && consumed == 256 && !done && a.phase == phase
                && a.retirement.phase == native_phase);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)) == reserved);
            CHECK(!turn(&a, 3, 512, NULL, &done, &consumed)
                && consumed <= 320);
            saw_credit |= consumed == 320;
        } else {
            CHECK(!turn(&a, 3, fee, NULL, &done, &consumed)
                && consumed == fee);
        }
        CHECK(consumed >= fee && a.phase >= phase);
    }
    CHECK(saw_init && saw_begin && saw_prepare && saw_step && saw_credit
        && !a.output);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_relation_cache_scalar_copy_result_t result;
    CHECK(!wl_columnar_relation_cache_scalar_copy_query(&a, &result)
        && result.discarded && !result.semantic_result);
    CHECK(!turn(&a, 6, 128, NULL, NULL, NULL));
    col_rel_destroy(source);
    wl_columnar_memory_governor_ref_release(g);
}

/* Direct physical-retirement evidence, NOT a cache-owner transfer. CACHE debt
 * remains with this explicit caller receipt until after actual physical DONE. */
static void
test_debt_boundaries(void)
{
    for (unsigned external = 0; external < 2; ++external) {
        wl_mem_ledger_t ledger;
        wl_mem_ledger_init(&ledger, 0);
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_CACHE, 17);
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 23);
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 29);
        col_rel_t *source = relation_new(3, 2);
        uint64_t debt = col_rel_owned_ledger_bytes(source);
        wl_mem_ledger_alloc(&ledger,
            external ? WL_MEM_SUBSYS_CACHE : WL_MEM_SUBSYS_RELATION, debt);
        if (!external) source->mem_ledger = &ledger;
        wl_mem_ledger_snapshot_t before, after;
        wl_mem_ledger_snapshot(&ledger, &before);
        col_rel_t *out = clone_scalar(source, NULL); col_rel_destroy(out);
        wl_mem_ledger_snapshot(&ledger, &after);
        CHECK(after.current_bytes == before.current_bytes);
        for (unsigned sub = 0; sub < WL_MEM_SUBSYS_COUNT; ++sub)
            CHECK(after.subsys_bytes[sub] == before.subsys_bytes[sub]);
        wl_columnar_relation_scalar_retirement_t retirement = {0};
        CHECK(!wl_columnar_relation_scalar_retirement_begin(&retirement,
            source));
        int owner = 0;
        wl_evaluation_control_t *fund = control_new(0, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(fund);
        int rc;
        do {
            rc = wl_columnar_relation_scalar_retirement_prepare(&retirement,
                    fund, id);
        } while (rc == EAGAIN);
        CHECK(!rc &&
            !wl_columnar_relation_scalar_retirement_commit(&retirement));
        do {
            rc = wl_columnar_relation_scalar_retirement_step(&retirement, fund,
                    id);
        } while (rc == EAGAIN);
        CHECK(!rc && !retirement.storage.relation);
        wl_mem_ledger_snapshot(&ledger, &after);
        CHECK(after.subsys_bytes[WL_MEM_SUBSYS_CACHE] ==
            17+(external ? debt : 0));
        CHECK(after.subsys_bytes[WL_MEM_SUBSYS_RELATION] == 23
            && after.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == 29);
        if (external) wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_CACHE, debt);
        wl_mem_ledger_snapshot(&ledger, &after);
        CHECK(after.current_bytes == 69);
        control_close(fund, &owner, 0);
    }
}

int
main(int argc, char **argv)
{
    CHECK(argc == 2);
    if (!strcmp(argv[1], "domains")) {
        test_domains(); test_producers();
    }else if (!strcmp(argv[1], "failures")) {
        test_failures(); test_constructor_refund_boundaries(false);
        test_constructor_refund_boundaries(true);
        test_retirement_funding(); test_retirement_chunk_stops();
        test_debt_boundaries();
        test_unsupported_boundary();
    }else if (!strcmp(argv[1], "authority")) {
        test_aliases(); test_authority();
    }else abort();
    return 0;
}
