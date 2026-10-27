/* Persistent #1372 right-only EDB reclamation through the ordinary evaluator. */
#include "columnar/internal.h"
#include "columnar/batch_payload.h"
#include "columnar/columnar_nanoarrow.h"
#include "exec_plan_gen.h"
#include "session.h"
#include "session_facts.h"
#include "io/io_adapter.h"
#include "wirelog/wirelog-parser.h"
#include "test_tmpdir.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)
#define OK WL_COLUMNAR_SPILL_OK
static unsigned write_fault, read_fault, allocation_fault, write_calls;
bool wl_columnar_spill_test_allocation(void)
{
    return allocation_fault && --allocation_fault == 0;
}
int wl_columnar_spill_test_io(bool writing, uint64_t offset, size_t *length)
{
    (void)offset;
    if (writing) {
        write_calls++;
        if (write_fault) {
            if (*length > 3) *length = 3;
            if (--write_fault == 0) return EIO;
        }
    } else if (read_fault && --read_fault == 0) return EIO;
    return 0;
}

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session;
    wl_columnar_memory_governor_ref_t *governor;
} fixture_t;
static const char *query =
    ".decl a(k: int64, v: int64)\n"
    ".decl b(k: int64, v: int64)\n"
    ".decl result(k: int64, x: int64, y: int64)\n"
    "result(k,x,y + 0) :- a(k,x), b(k,y).\n";
static uint64_t reserved(fixture_t *f)
{
    return wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                   f->governor));
}
static fixture_t fixture(const char *text, uint64_t budget, uint64_t quota)
{
    fixture_t f = {0};
    wirelog_error_t error;
    f.program = wirelog_parse_string(text, &error);
    if (!f.program) fprintf(stderr, "parse error %d: %s\n", (int)error, text);
    CHECK(f.program);
    int plan_rc = wl_plan_from_program(f.program, &f.plan);
    if (plan_rc) fprintf(stderr, "plan error %d: %s\n", plan_rc, text);
    CHECK(plan_rc == 0);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = budget, .usable_bytes = budget,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV, .status = WL_COLUMNAR_MEMORY_OK
    };
    f.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = f.governor;
    options.spill_directory = test_tmpdir();
    options.spill_quota_bytes = quota;
    options.spill_max_row_bytes = 64;
    options.spill_join_batch_bytes = 96;
    options.spill_join_index_bytes = sizeof(wl_columnar_memory_reservation_t) +
        16;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session) == 0);
    CHECK(wl_session_load_facts(f.session, f.program) == 0);
    return f;
}
static fixture_t populated(void)
{
    fixture_t f = fixture(query, 4 * 1024 * 1024, 1024 * 1024);
    for (int64_t i = 0; i < 4; i++) {
        int64_t row[] = {i, 10+i};
        CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
    }
    for (int64_t i = 0; i < 128; i++) {
        int64_t row[] = {i, 100+i};
        CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
    }
    return f;
}
static void destroy(fixture_t *f)
{
    wl_session_destroy(f->session);
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    CHECK(reserved(f) == 0);
    wl_columnar_memory_governor_ref_release(f->governor);
}
typedef struct { unsigned count; unsigned seen; } events_t;
static void observe(const char *name, const int64_t *row, uint32_t cols,
    void *ctx)
{
    events_t *e = ctx;
    CHECK(strcmp(name, "result") == 0 && cols == 3);
    CHECK(row[0] >= 0 && row[0] < 4 && row[1] == row[0]+10 &&
        row[2] == row[0]+100);
    unsigned bit = 1u << (unsigned)row[0];
    CHECK(!(e->seen & bit)); e->seen |= bit; e->count++;
}
static void ignore_delta(const char *n, const int64_t *r, uint32_t c,
    int32_t d, void *ctx)
{
    (void)n; (void)r; (void)c; (void)d; (void)ctx;
}
static void contents(fixture_t *f)
{
    wl_col_session_t *s = COL_SESSION(f->session);
    CHECK(s->cold_relation && !session_find_rel(s, "b"));
    wl_columnar_session_cold_relation_t *c = s->cold_relation;
    CHECK(c->ncols == 2 && c->nrows == 128 && !strcmp(c->name, "b"));
    wl_columnar_batch_payload_reader_t *r[2];
    CHECK(wl_columnar_batch_payload_acquire(c->payload, &r[0]) == OK);
    CHECK(wl_columnar_batch_payload_acquire(c->payload, &r[1]) == OK);
    CHECK(wl_columnar_batch_payload_release(c->payload) ==
        WL_COLUMNAR_SPILL_BUSY);
    const uint64_t *lanes;
    wl_columnar_spill_metadata_t m;
    const unsigned ordinals[] = {127, 0, 64, 1};
    for (unsigned i = 0; i < 4; i++) {
        unsigned row = ordinals[i];
        CHECK(wl_columnar_batch_payload_read(r[i%2], row, NULL, NULL,
            &lanes, &m) == OK);
        CHECK(lanes[0] == row && lanes[1] == row+100 && m.multiplicity == 1);
    }
    wl_columnar_batch_payload_reader_release(r[0]);
    wl_columnar_batch_payload_reader_release(r[1]);
}
static void lifecycle(void)
{
    for (unsigned first_snapshot = 0; first_snapshot < 2; first_snapshot++) {
        fixture_t f = populated();
        wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *source = session_find_rel(s, "b");
        col_relation_snapshot_t snapshot =
            wl_columnar_relation_snapshot(source);
        uint64_t before = reserved(&f);
        CHECK(s->last_inserted_relation && !strcmp(s->last_inserted_relation,
            "b"));
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == OK);
        CHECK(reserved(&f) < before &&
            s->cold_relation->source_snapshot.relation_identity ==
            snapshot.relation_identity &&
            s->cold_relation->source_snapshot.view_generation ==
            snapshot.view_generation &&
            s->cold_relation->cold_storage_generation ==
            snapshot.storage_generation+1);
        CHECK(s->last_inserted_relation == s->cold_relation->name);
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == WL_COLUMNAR_SPILL_BUSY);
        contents(&f);
        uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
        CHECK(disk);
        events_t e = {0};
        if (!first_snapshot) CHECK(wl_session_step(f.session) == 0);
        CHECK(wl_session_snapshot(f.session, observe, &e) == 0 && e.count == 4);
        unsigned writes = write_calls;
        for (unsigned i = 0; i < 2; i++) {
            CHECK(wl_session_step(f.session) == 0);
            e = (events_t){0};
            CHECK(wl_session_snapshot(f.session, observe,
                &e) == 0 && e.count == 4);
        }
        CHECK(write_calls == writes && s->arr_count == 0);
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
        /* Guards must precede stable snapshot / no-work STEP shortcuts. */
        wl_session_set_delta_cb(f.session, ignore_delta, NULL);
        e = (events_t){0};
        CHECK(wl_session_step(f.session) == ENOTSUP);
        CHECK(wl_session_snapshot(f.session, observe,
            &e) == ENOTSUP && !e.count);
        int64_t row[] = {1, 2};
        uint32_t epoch = s->outer_epoch;
        bool dirty = s->pending_input_change;
        for (unsigned count = 0; count < 2; count++) {
            CHECK(wl_session_insert(f.session, "b", row, count, 2) == ENOTSUP);
            CHECK(wl_session_remove(f.session, "b", row, count, 2) == ENOTSUP);
            CHECK(wl_session_insert(f.session, "a", row, count, 2) == ENOTSUP);
        }
        CHECK(s->outer_epoch == epoch && s->pending_input_change == dirty &&
            !session_find_rel(s,
            "b") && wl_columnar_spill_store_bytes(s->spill_store) == disk);
        wl_session_set_delta_cb(f.session, NULL, NULL);
        CHECK(wl_session_step(f.session) == 0);
        contents(&f);
        destroy(&f);
    }
}
typedef struct { unsigned calls, stop; } cancellation_t;
static bool cancel(void *ctx)
{
    cancellation_t *c = ctx;
    return ++c->calls == c->stop;
}
static void rollback(void)
{
    unsigned last_cancel = 0;
    fixture_t probe = populated();
    cancellation_t c = {0};
    CHECK(wl_columnar_session_seal_join_input(probe.session, "b", cancel,
        &c) == OK);
    last_cancel = c.calls; destroy(&probe);
    for (unsigned fault = 0; fault < 6; fault++) {
        fixture_t f = populated(); wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *source = session_find_rel(s, "b");
        uint64_t before = reserved(&f);
        wl_columnar_source_access_reader_t pin = {0};
        c = (cancellation_t){0};
        wl_columnar_spill_status_t expected;
        if (fault == 0) {
            write_fault = 4; expected = WL_COLUMNAR_SPILL_IO;
        }
        if (fault == 1) {
            allocation_fault = 1; expected = WL_COLUMNAR_SPILL_ALLOCATION;
        }
        if (fault == 2) {
            c.stop = 1; expected = WL_COLUMNAR_SPILL_CANCELLED;
        }
        if (fault == 3) {
            c.stop = last_cancel; expected = WL_COLUMNAR_SPILL_CANCELLED;
        }
        if (fault == 4) {
            CHECK(col_rel_source_reader_acquire(source, &pin) == 0);
            expected = WL_COLUMNAR_SPILL_BUSY;
        }
        wl_columnar_memory_reservation_t pressure;
        wl_columnar_memory_reservation_init(&pressure);
        if (fault == 5) {
            CHECK(wl_columnar_memory_reserve(
                    wl_columnar_memory_governor_ref_get(f.governor),
                    4*1024*1024-before, &pressure));
            expected = WL_COLUMNAR_SPILL_MEMORY;
        }
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", cancel,
            &c) == expected);
        if (fault == 5) CHECK(wl_columnar_memory_rollback(&pressure));
        if (pin.owner) CHECK(col_rel_source_reader_release(&pin) == 0);
        CHECK(!s->cold_relation && session_find_rel(s, "b") == source &&
            source->nrows == 128 && source->columns[1][127] == 227);
        CHECK(reserved(&f) == before &&
            wl_columnar_spill_store_bytes(s->spill_store) == 0);
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == OK);
        destroy(&f);
    }
}
static void read_failure(void)
{
    fixture_t f = populated(); wl_col_session_t *s = COL_SESSION(f.session);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
    read_fault = 5;
    CHECK(wl_session_step(f.session) == EIO);
    CHECK(s->cold_relation && !session_find_rel(s, "b") &&
        wl_columnar_spill_store_bytes(s->spill_store) == disk);
    CHECK(wl_session_step(f.session) == 0);
    events_t e = {0};
    CHECK(wl_session_snapshot(f.session, observe, &e) == 0 && e.count == 4);
    destroy(&f);
}
static void exclusions(void)
{
    for (unsigned kind = 0; kind < 7; kind++) {
        fixture_t f = populated(); wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *r = session_find_rel(s, "b");
        uint64_t before = reserved(&f);
        uint64_t generation = r->storage_generation;
        if (kind == 0) s->num_workers = 2;
        if (kind == 1) s->join_batch_bytes = 96;
        if (kind == 2) wl_session_set_delta_cb(f.session, ignore_delta, NULL);
        if (kind == 3) s->cold_seal_closed = true;
        if (kind == 4) s->delta_seeded = true;
        if (kind == 5) CHECK(col_rel_enable_timestamps(r) == 0);
        if (kind ==
            6) r->storage_generation = r->storage_owner_generation = UINT64_MAX;
        before = reserved(&f);
        wl_columnar_spill_status_t rc = wl_columnar_session_seal_join_input(
            f.session, "b", NULL, NULL);
        CHECK(rc != OK && !s->cold_relation && session_find_rel(s, "b") == r);
        CHECK(reserved(&f) == before &&
            wl_columnar_spill_store_bytes(s->spill_store) == 0);
        s->num_workers = 1; s->join_batch_bytes = 0; s->delta_seeded = false;
        if (kind ==
            6) r->storage_generation = r->storage_owner_generation = generation;
        wl_session_set_delta_cb(f.session, NULL, NULL);
        destroy(&f);
    }
    const char *mixed =
        ".decl a(k: int64, v: int64)\n.decl b(k: int64, v: int64)\n"
        ".decl result(k: int64, x: int64, y: int64)\n"
        "result(k,x,y + 0) :- a(k,x),b(k,y).\n"
        ".decl scan(k: int64, v: int64)\nscan(k,v) :- b(k,v).\n";
    fixture_t f = fixture(mixed, 4*1024*1024, 1024*1024);
    int64_t row[] = {1, 2};
    CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
    CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(!COL_SESSION(f.session)->cold_relation);
    destroy(&f);
    f = populated();
    CHECK(wl_session_step(f.session) == 0);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == WL_COLUMNAR_SPILL_BUSY);
    destroy(&f);
}
static void count_only(const char *name, const int64_t *row, uint32_t cols,
    void *ctx)
{
    (void)name; (void)row; (void)cols; (*(unsigned *)ctx)++;
}
static void empty_and_multiple(void)
{
    const char *multiple =
        ".decl a(k: int64, v: int64)\n.decl b(k: int64, v: int64)\n"
        ".decl result(k: int64, x: int64, y: int64)\n"
        "result(k,x,y + 0) :- a(k,x),b(k,y).\n"
        ".decl second(k: int64, x: int64, y: int64)\n"
        "second(k,x,y + 1) :- a(k,x),b(k,y).\n";
    for (unsigned empty = 0; empty < 2; empty++) {
        fixture_t f = fixture(multiple, 4*1024*1024, 1024*1024);
        int64_t row[] = {1, 2};
        CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
        CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
        if (empty) CHECK(wl_session_remove(f.session, "b", row, 1, 2) == 0);
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == OK);
        wl_col_session_t *s = COL_SESSION(f.session);
        uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
        CHECK(wl_session_step(f.session) == 0);
        CHECK(s->spill_join_selected_count == 2 && s->arr_count == 0);
#ifdef WL_PROFILE
        CHECK(s->profile.join_calls == 2 && s->profile.join_compute_ns > 0);
        CHECK(s->profile.join_empty_out == (empty ? 2u : 0u));
#endif
        unsigned count = 0;
        CHECK(wl_session_snapshot(f.session, count_only, &count) == 0);
        CHECK(count == (empty ? 0 : 2));
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
        for (uint32_t i = 0; i < s->nrels; i++)
            CHECK(!s->rels[i] || strcmp(s->rels[i]->name, "b"));
        destroy(&f);
    }
}

static void typed_observe(const char *name, const int64_t *row, uint32_t cols,
    void *ctx)
{
    CHECK(!strcmp(name, "result") && cols == 4);
    CHECK((uint64_t)row[0] == UINT64_C(0x3ff8000000000000) &&
        (uint64_t)row[1] == UINT64_C(0xfffffffffffffff0) &&
        row[2] == 7 && row[3] == row[0]);
    (*(unsigned *)ctx)++;
}
static void typed_projection(void)
{
    fixture_t f = fixture(
        ".decl a(k: int64)\n.decl b(k: int64, v: float, u: int64)\n"
        ".decl result(v: float, u: int64, k: int64, again: float)\n"
        "result(v,u,k,v) :- a(k),b(k,v,u).\n", 4*1024*1024, 1024*1024);
    int64_t left[] = {7};
    uint64_t bits[] = {7, UINT64_C(0x3ff8000000000000),
                       UINT64_C(0xfffffffffffffff0)};
    int64_t right[3]; memcpy(right, bits, sizeof(right));
    CHECK(wl_session_insert(f.session, "a", left, 1, 1) == 0);
    CHECK(wl_session_insert(f.session, "b", right, 1, 3) == 0);
    /* The parser has no uint64 declaration spelling; the internal host may
     * provide an explicit physical numeric schema. */
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT,
                                     WIRELOG_TYPE_UINT64};
    CHECK(col_rel_set_column_types(session_find_rel(COL_SESSION(f.session),
        "b"),
        types, 3) == 0);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    unsigned count = 0;
    CHECK(wl_session_snapshot(f.session, typed_observe,
        &count) == 0 && count == 1);
    col_rel_t *out = session_find_rel(COL_SESSION(f.session), "result");
    CHECK(out && out->column_types &&
        out->column_types[0] == WIRELOG_TYPE_FLOAT &&
        out->column_types[1] == WIRELOG_TYPE_UINT64 &&
        out->column_types[2] == WIRELOG_TYPE_INT64 &&
        out->column_types[3] == WIRELOG_TYPE_FLOAT);
    destroy(&f);
}
static void quota_and_alias(void)
{
    fixture_t f = fixture(query, 4*1024*1024, 32);
    int64_t row[] = {1, 2};
    CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
    wl_col_session_t *s = COL_SESSION(f.session);
    col_rel_t *source = session_find_rel(s, "b");
    uint64_t before = reserved(&f);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == WL_COLUMNAR_SPILL_QUOTA);
    CHECK(session_find_rel(s,
        "b") == source && !s->cold_relation && reserved(&f) == before);
    destroy(&f);
    f = populated(); s = COL_SESSION(f.session);
    source = session_find_rel(s, "b");
    col_rel_t *alias = col_rel_new_auto("alias", 2);
    CHECK(alias && col_rel_attach_memory_governor(alias, f.governor) == 0);
    CHECK(col_rel_install_shared_view(alias, source) == 0);
    before = reserved(&f);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == WL_COLUMNAR_SPILL_BUSY);
    CHECK(session_find_rel(s,
        "b") == source && !s->cold_relation && reserved(&f) == before);
    CHECK(col_rel_destroy_checked(alias) == 0);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    wl_col_session_t worker = {0};
    CHECK(col_worker_session_create(s, 1, NULL, 0, &worker) == ENOTSUP);
    destroy(&f);
}

static unsigned adapter_calls, fake_inserts;
static int refuse_adapter(wirelog_io_ctx_t *ctx, char *error,
    size_t cap, void *user)
{
    (void)ctx; (void)error; (void)cap; (void)user;
    adapter_calls++; return ENOTSUP;
}
static int partial_insert(wl_session_t *s, const char *name,
    const int64_t *rows, uint32_t count, uint32_t cols)
{
    (void)s; (void)name; (void)rows; (void)count; (void)cols;
    return ++fake_inserts == 1 ? 0 : ENOTSUP;
}
static void loaders(void)
{
    static const wirelog_io_adapter_t adapter = {
        .scheme = "sealed_probe", .abi_version = WIRELOG_IO_ABI_VERSION,
        .validate = refuse_adapter,
    };
    CHECK(wirelog_io_register_adapter(&adapter) == 0);
    wirelog_error_t error;
    wirelog_program_t *input = wirelog_parse_string(
        ".decl a(k: int64, v: int64)\na(2,12).\n"
        ".decl b(k: int64, v: int64)\nb(2,102).\n"
        ".input b(io=\"sealed_probe\", filename=\"must-not-open\")\n", &error);
    CHECK(input);
    fixture_t f = populated();
    write_fault = 1;
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == WL_COLUMNAR_SPILL_IO);
    CHECK(!f.session->input_sealed && wl_session_load_facts(f.session,
        input) == 0);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    uint64_t before = reserved(&f);
    CHECK(f.session->input_sealed && !f.session->input_load_failed);
    CHECK(wl_session_load_facts(f.session, input) == ENOTSUP);
    CHECK(wl_session_load_input_files(f.session, input) == ENOTSUP);
    CHECK(wl_session_load_facts(f.session, f.program) == ENOTSUP);
    CHECK(wl_session_load_input_files(f.session, f.program) == ENOTSUP);
    CHECK(!adapter_calls && !f.session->input_load_failed &&
        reserved(&f) == before);
    events_t e = {0};
    CHECK(wl_session_step(f.session) == 0);
    CHECK(wl_session_snapshot(f.session, observe, &e) == 0 && e.count == 4);
    destroy(&f);
    /* A real unsealed failure still poisons; the new policy cannot pardon
     * arbitrary ENOTSUP after earlier rows have already committed. */
    static const wl_compute_backend_t backend =
    {.session_insert = partial_insert};
    wl_session_t fake = {.backend = &backend};
    CHECK(wl_session_load_facts(&fake, input) == ENOTSUP);
    CHECK(fake_inserts == 2 && fake.input_load_failed);
    fake.input_sealed = true;
    CHECK(wl_session_load_facts(&fake, input) == -1 && fake.input_load_failed);
    CHECK(wl_session_load_input_files(&fake, input) == -1 && !adapter_calls);
    wirelog_program_free(input);
    CHECK(wirelog_io_unregister_adapter("sealed_probe") == 0);
}

static void generation_boundary(void)
{
    for (unsigned permitted = 0; permitted < 2; permitted++) {
        fixture_t f = populated();
        wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *r = session_find_rel(s, "b");
        uint64_t old = r->storage_generation, before = reserved(&f);
        r->storage_generation = r->storage_owner_generation =
            WL_COLUMNAR_REL_GENERATION_INVALID - 1u - permitted;
        col_relation_snapshot_t snapshot = wl_columnar_relation_snapshot(r);
        CHECK(wl_columnar_batch_payload_can_take(r));
        wl_columnar_spill_status_t rc = wl_columnar_session_seal_join_input(
            f.session, "b", NULL, NULL);
        if (permitted) {
            CHECK(rc == OK && !session_find_rel(s,
                "b") && f.session->input_sealed);
            CHECK(s->cold_relation->cold_storage_generation ==
                WL_COLUMNAR_REL_GENERATION_INVALID - 1u);
            contents(&f);
        } else {
            CHECK(rc == WL_COLUMNAR_SPILL_OVERFLOW && !s->cold_relation &&
                !f.session->input_sealed && session_find_rel(s, "b") == r);
            CHECK(r->relation_identity == snapshot.relation_identity &&
                r->view_generation == snapshot.view_generation &&
                r->storage_generation == snapshot.storage_generation &&
                r->columns[1][127] == 227 && reserved(&f) == before &&
                wl_columnar_spill_store_bytes(s->spill_store) == 0);
            r->storage_generation = r->storage_owner_generation = old;
        }
        destroy(&f);
    }
}

int main(void)
{
    lifecycle(); rollback(); read_failure(); exclusions(); empty_and_multiple();
    typed_projection(); quota_and_alias(); loaders(); generation_boundary();
    return 0;
}
