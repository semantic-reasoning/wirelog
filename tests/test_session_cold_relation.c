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
        ".decl scan(k: int64, v: int64)\nscan(k,v + 1) :- b(k,v).\n";
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

extern bool wl_columnar_session_test_retain_cold_source;
extern bool wl_columnar_session_test_has_retained_cold_source(
    const wl_col_session_t *);
extern void (*wl_columnar_join_test_spill_producer_result)(
    wl_columnar_spill_status_t);
static uint64_t budget_peak;
static unsigned producer_calls;
static wl_columnar_spill_status_t producer_status;
static void budget_admitted(wl_columnar_memory_governor_t *g, uint64_t total)
{
    (void)g; if (total > budget_peak) budget_peak = total;
}
static void producer_result(wl_columnar_spill_status_t status)
{
    producer_calls++; producer_status = status;
}
static fixture_t budget_fixture(uint64_t allowance)
{
    fixture_t f = fixture(query, allowance, 8*1024*1024);
    wl_col_session_t *s = COL_SESSION(f.session);
    s->spill_join_batch_bytes = 1024;
    s->spill_join_index_bytes = 512*1024;
    for (int64_t i = 0; i < 128; i++) {
        int64_t row[] = {0, i};
        CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
    }
    for (int64_t i = 0; i < 32768; i++) {
        int64_t row[] = {i < 64 ? 0 : i+1000, 100+i};
        CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
    }
    return f;
}
typedef struct { unsigned count; unsigned char seen[1024]; } budget_events_t;
static void budget_observe(const char *name, const int64_t *row, uint32_t cols,
    void *ctx)
{
    budget_events_t *e = ctx;
    CHECK(!strcmp(name, "result") && cols == 3 && row[0] == 0 &&
        row[1] >= 0 && row[1] < 128 && row[2] >= 100 && row[2] < 164);
    unsigned ordinal = (unsigned)(row[1]*64 + row[2]-100);
    unsigned char mask = (unsigned char)(1u << (ordinal%8));
    CHECK(!(e->seen[ordinal/8] & mask));
    e->seen[ordinal/8] |= mask; e->count++;
}
static void fixed_budget(void)
{
    CHECK(!wl_columnar_session_test_has_retained_cold_source(NULL));
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    wl_columnar_join_test_spill_producer_result = producer_result;
    budget_peak = producer_calls = 0;
    fixture_t calibration = budget_fixture(8*1024*1024);
    CHECK(wl_columnar_session_seal_join_input(calibration.session, "b", NULL,
        NULL) == OK);
    budget_events_t events = {0};
    CHECK(wl_session_snapshot(calibration.session, budget_observe,
        &events) == 0 && events.count == 8192);
    uint64_t allowance = budget_peak;
    destroy(&calibration);
    for (unsigned retain = 0; retain < 2; retain++) {
        budget_peak = producer_calls = 0;
        producer_status = WL_COLUMNAR_SPILL_INVALID;
        fixture_t f = budget_fixture(allowance);
        wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *r = session_find_rel(s, "b");
        uint64_t identity = r->relation_identity, before = reserved(&f);
        uint64_t resident = r->descriptor_reserved_bytes +
            r->metadata_reserved_bytes +
            r->retained_reserved_bytes + r->dedup_reserved_bytes;
        wl_columnar_session_test_retain_cold_source = retain != 0;
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == OK);
        wl_columnar_session_test_retain_cold_source = false;
        uint64_t after = reserved(&f);
        CHECK(!session_find_rel(s, "b") && s->cold_relation &&
            s->cold_relation->source_snapshot.relation_identity == identity);
        CHECK(wl_columnar_session_test_has_retained_cold_source(s) ==
            (retain != 0));
        CHECK(retain ? after > before : after < before);
        CHECK(resident >= UINT64_C(32768)*2*8);
        uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
        events = (budget_events_t){0};
        int rc = wl_session_snapshot(f.session, budget_observe, &events);
        CHECK(budget_peak <= allowance && s->spill_join_selected_count == 1 &&
            producer_calls == 1);
        if (!retain) {
            CHECK(rc == 0 && events.count == 8192 && producer_status == OK);
            unsigned writes = write_calls;
            events = (budget_events_t){0};
            CHECK(wl_session_snapshot(f.session, budget_observe,
                &events) == 0 && events.count == 8192);
            CHECK(write_calls == writes);
        } else {
            CHECK(rc == ENOSPC && wl_session_budget_denied(f.session) &&
                !events.count);
            CHECK(producer_status == WL_COLUMNAR_SPILL_MEMORY ||
                (producer_status == OK && s->join_output_peak < 8192));
        }
        CHECK(s->arr_count == 0 &&
            wl_columnar_spill_store_bytes(s->spill_store) == disk);
        destroy(&f);
        CHECK(!wl_columnar_session_test_has_retained_cold_source(NULL));
    }
    wl_columnar_join_test_spill_producer_result = NULL;
    wl_columnar_memory_governor_test_admitted = NULL;
}
static void retention_lifetime(void)
{
    fixture_t f = populated(); wl_col_session_t *s = COL_SESSION(f.session);
    wl_columnar_session_test_retain_cold_source = true;
    col_rel_t *r = session_find_rel(s, "b");
    wl_columnar_source_access_reader_t pin = {0};
    CHECK(col_rel_source_reader_acquire(r, &pin) == 0);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == WL_COLUMNAR_SPILL_BUSY);
    CHECK(!s->cold_relation &&
        !wl_columnar_session_test_has_retained_cold_source(s));
    CHECK(col_rel_source_reader_release(&pin) == 0);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    fixture_t other = populated();
    CHECK(wl_columnar_session_seal_join_input(other.session, "b", NULL,
        NULL) == WL_COLUMNAR_SPILL_BUSY);
    destroy(&other);
    CHECK(wl_columnar_session_test_has_retained_cold_source(s));
    wl_columnar_session_test_retain_cold_source = false;
    destroy(&f);
    CHECK(!wl_columnar_session_test_has_retained_cold_source(NULL));
    f = populated();
    wl_columnar_session_test_retain_cold_source = true;
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    wl_columnar_session_test_retain_cold_source = false;
    destroy(&f);
}

/* Each thread owns its cursor; no session operation runs alongside these pins. */
static void *cold_reader(void *context)
{
    wl_columnar_batch_payload_reader_t *reader = context;
    for (unsigned i = 0; i < 1024; i++) {
        unsigned row = (i*37)%128;
        const uint64_t *lanes;
        wl_columnar_spill_metadata_t metadata;
        CHECK(wl_columnar_batch_payload_read(reader, row, NULL, NULL, &lanes,
            &metadata) == OK);
        CHECK(lanes[0] == row && lanes[1] == row+100 &&
            metadata.multiplicity == 1);
    }
    return NULL;
}
static void concurrent_cold_readers(void)
{
    fixture_t f = populated();
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    wl_columnar_batch_payload_t *payload =
        COL_SESSION(f.session)->cold_relation->payload;
    wl_columnar_batch_payload_reader_t *readers[2];
    wl_thread_t threads[2];
    for (unsigned i = 0; i < 2; i++) {
        CHECK(wl_columnar_batch_payload_acquire(payload, &readers[i]) == OK);
        CHECK(wl_thread_create(&threads[i], cold_reader, readers[i]) == 0);
    }
    CHECK(wl_columnar_batch_payload_release(payload) == WL_COLUMNAR_SPILL_BUSY);
    for (unsigned i = 0; i < 2; i++) {
        wl_thread_join(&threads[i]);
        wl_columnar_batch_payload_reader_release(readers[i]);
    }
    events_t events = {0};
    CHECK(wl_session_snapshot(f.session, observe,
        &events) == 0 && events.count == 4);
    destroy(&f);
}
static fixture_t floor_fixture(uint64_t allowance, int64_t rows)
{
    fixture_t f = fixture(query, allowance, 1024*1024);
    for (int64_t i = 0; i < rows; i++) {
        int64_t row[] = {0, i};
        CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
        CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
    }
    return f;
}
static void count_event(const char *name, const int64_t *row, uint32_t cols,
    void *context)
{
    (void)name; (void)row; (void)cols; (*(unsigned *)context)++;
}
static void fixed_floors(void)
{
    /* Fresh immutable allowance admits preparation but not simultaneous capture. */
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    budget_peak = 0;
    fixture_t calibration = floor_fixture(4*1024*1024, 1);
    uint64_t preparation_peak = budget_peak;
    CHECK(wl_columnar_session_seal_join_input(calibration.session, "b", NULL,
        NULL) == OK);
    CHECK(budget_peak > preparation_peak);
    destroy(&calibration);
    fixture_t f = floor_fixture(preparation_peak, 1);
    wl_col_session_t *s = COL_SESSION(f.session);
    col_rel_t *source = session_find_rel(s, "b");
    uint64_t before = reserved(&f);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == WL_COLUMNAR_SPILL_MEMORY);
    CHECK(session_find_rel(s,
        "b") == source && !s->cold_relation && !f.session->input_sealed);
    CHECK(reserved(&f) == before &&
        wl_columnar_spill_store_bytes(s->spill_store) == 0);
    destroy(&f);
    wl_columnar_memory_governor_test_admitted = NULL;
    /* Distinct floor: sealing succeeds, but the 16,384-row final result cannot fit. */
    f = floor_fixture(256*1024, 128); s = COL_SESSION(f.session);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
    unsigned events = 0;
    CHECK(wl_session_snapshot(f.session, count_event, &events) == ENOSPC);
    CHECK(wl_session_budget_denied(f.session) && events == 0 &&
        s->cold_relation);
    CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk && disk > 0);
    destroy(&f);
}

static const char *projection_query =
    ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:int64)\n"
    ".decl result(k:int64,x:int64,y:int64)\n"
    ".decl keys(k:int64)\n"
    "result(k,x,y + 0) :- a(k,x),b(k,y).\nkeys(k) :- b(k,v).\n";
static const wl_plan_relation_t *projection_plan(fixture_t *f)
{
    for (uint32_t i = 0; i < f->plan->stratum_count; i++)
        for (uint32_t j = 0; j < f->plan->strata[i].relation_count; j++) {
            const wl_plan_relation_t *p = &f->plan->strata[i].relations[j];
            if (!strcmp(p->name, "keys"))return p;
        }
    abort();
}
static fixture_t projection_fixture(uint64_t allowance, unsigned rows)
{
    fixture_t f = fixture(projection_query, allowance, 8*1024*1024);
    int64_t left[] = {0, 10};
    CHECK(wl_session_insert(f.session, "a", left, 1, 2) == 0);
    for (unsigned i = 0; i < rows; i++) {
        int64_t r[] = {(int64_t)i, 100+(int64_t)i};
        CHECK(wl_session_insert(f.session, "b", r, 1, 2) == 0);
    }
    if (!rows) {
        int64_t r[] = {0, 100};
        CHECK(wl_session_insert(f.session, "b", r, 1, 2) == 0);
        CHECK(wl_session_remove(f.session, "b", r, 1, 2) == 0);
    }
    CHECK(wl_columnar_eval_plan_cold_projection(projection_plan(&f), "b", 2,
        NULL));
    return f;
}
typedef struct {unsigned rows, keys, joined; uint64_t sum;
                unsigned char seen[1024];} projection_events_t;
static void projection_observe(const char *name, const int64_t *r, uint32_t n,
    void *ctx)
{
    projection_events_t *e = ctx;
    if (!strcmp(name, "keys")) {
        CHECK(n == 1 && r[0] >= 0 && (uint64_t)r[0] < e->rows);
        CHECK((uint64_t)r[0] < 8192);
        unsigned key = (unsigned)r[0], mask = 1u << (key % 8);
        CHECK(!(e->seen[key / 8] & mask));
        e->seen[key / 8] |= (unsigned char)mask;
        e->keys++; e->sum += (uint64_t)r[0];
    } else {
        CHECK(!strcmp(name,
            "result") && n == 3 && r[0] == 0 && r[1] == 10 && r[2] == 100);
        e->joined++;
    }
}
static void projection_consumers(void)
{
    for (unsigned empty = 0; empty < 2; empty++)for (unsigned cold =
            0; cold < 3;
            cold++){
            unsigned rows = empty?0:128;
            fixture_t f = projection_fixture(4*1024*1024, rows);
            if (cold) CHECK(wl_columnar_session_seal_join_input(f.session, "b",
                    NULL, NULL) == OK);
            if (cold == 2) {
                CHECK(wl_session_step(f.session) == 0);
                CHECK(wl_session_step(f.session) == 0);
                col_rel_t *head = session_find_rel(COL_SESSION(f.session),
                        "keys");
                CHECK(head && head->nrows == rows);
            }
            for (unsigned repeat = 0; repeat < 2; repeat++) {
                projection_events_t e = {.rows = rows};
                int rc = wl_session_snapshot(f.session, projection_observe, &e);
                if (rc) fprintf(stderr,
                        "projection snapshot rc=%d cold=%u empty=%u repeat=%u\n",
                        rc, cold, empty, repeat);
                CHECK(rc == 0);
                CHECK(e.keys == rows && e.joined == !empty &&
                    e.sum == (uint64_t)rows*(rows?rows-1:0)/2);
            }
            if (cold) CHECK(!session_find_rel(COL_SESSION(f.session), "b"));
            destroy(&f);
        }
}
static void projection_failures(void)
{
    fixture_t f = projection_fixture(4*1024*1024, 128);
    wl_col_session_t *s = COL_SESSION(f.session);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    const wl_plan_relation_t *rp = projection_plan(&f);
    eval_stack_t stack; eval_stack_init(&stack);
    uint64_t before = reserved(&f),
        disk = wl_columnar_spill_store_bytes(s->spill_store);
    read_fault = 5;
    CHECK(wl_columnar_ops_cold_projection(rp, &stack, s) == EIO);
    CHECK(reserved(&f) == before && stack.top == 0);
    wl_columnar_ops_test_map_fail_output_alloc = true;
    CHECK(wl_columnar_ops_cold_projection(rp, &stack, s) == ENOMEM);
    wl_columnar_ops_test_map_fail_output_alloc = false;
    CHECK(reserved(&f) == before);
    allocation_fault = 1;
    CHECK(wl_columnar_ops_cold_projection(rp, &stack, s) == ENOMEM);
    CHECK(reserved(&f) == before);
    CHECK(wl_columnar_ops_cold_projection(rp, &stack, s) == 0);
    eval_entry_t e = eval_stack_pop(&stack); CHECK(e.rel->nrows == 128);
    CHECK(eval_stack_dispose_entry(&stack, &e) == 0 && reserved(&f) == before);
    CHECK(col_op_variable(&rp->ops[0], &stack, s) == ENOTSUP);
    CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
    projection_events_t events = {.rows = 128};
    read_fault = 5;
    CHECK(wl_session_snapshot(f.session, projection_observe, &events) == EIO);
    CHECK(!events.keys && !events.joined && s->cold_relation);
    events = (projection_events_t){.rows = 128};
    CHECK(wl_session_snapshot(f.session, projection_observe,
        &events) == 0 && events.keys == 128);
    destroy(&f);
}
static void projection_shape(void)
{
    fixture_t f = projection_fixture(4*1024*1024, 4);
    wl_plan_relation_t rp = *projection_plan(&f);
    wl_plan_op_t ops[3]; memcpy(ops, rp.ops, sizeof(ops)); rp.ops = ops;
    uint32_t indices[] = {1, 0, 1}; ops[1].project_count = 3;
    ops[1].project_indices = indices;
    ops[1].map_expr_count = 0; ops[1].map_exprs = NULL;
    CHECK(wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    eval_stack_t stack; eval_stack_init(&stack);
    CHECK(wl_columnar_ops_cold_projection(&rp, &stack,
        COL_SESSION(f.session)) == 0);
    eval_entry_t e = eval_stack_pop(&stack);
    CHECK(e.rel->ncols == 3 && e.rel->nrows == 4);
    for (unsigned i = 0; i < 4;
        i++) CHECK(e.rel->columns[0][i] == 100+i && e.rel->columns[1][i] == i &&
            e.rel->columns[2][i] == 100+i);
    CHECK(eval_stack_dispose_entry(&stack, &e) == 0);
    indices[0] = 2;
    CHECK(!wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
    indices[0] = 1;
    ops[1].project_count = 0;
    CHECK(!wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
    ops[1].project_count = 3;
    ops[1].op = WL_PLAN_OP_FILTER;
    CHECK(!wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
    ops[1].op = WL_PLAN_OP_MAP;
    ops[2].op = WL_PLAN_OP_CONCAT;
    CHECK(!wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
    ops[2].op = WL_PLAN_OP_CONSOLIDATE;
    ops[0].right_relation = "b";
    CHECK(!wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
    ops[0].right_relation = NULL;
    uint8_t data[32] = {WL_PLAN_EXPR_VAR}; const char *bad = "col4294967297";
    uint16_t len = (uint16_t)strlen(bad);
    memcpy(data+1, &len, 2); memcpy(data+3, bad, len);
    wl_plan_expr_buffer_t expr[3] = {{.data = data, .size = (uint32_t)len+3},
                                     {0}, {0}};
    ops[1].map_expr_count = 3; ops[1].map_exprs = expr;
    CHECK(!wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
    len = 4; memcpy(data + 1, &len, 2); memcpy(data + 3, "col1", 4);
    expr[0].size = 7;
    CHECK(wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
    data[0] = WL_PLAN_EXPR_VAR_FLOAT;
    CHECK(!wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT};
    CHECK(wl_columnar_eval_plan_cold_projection(&rp, "b", 2, types));
    expr[0].size = 6;
    CHECK(!wl_columnar_eval_plan_cold_projection(&rp, "b", 2, types));
    destroy(&f);
}

static void projection_budget(void)
{
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    budget_peak = 0;
    fixture_t cal = projection_fixture(8*1024*1024, 8192);
    CHECK(wl_columnar_session_seal_join_input(cal.session, "b", NULL,
        NULL) == OK);
    projection_events_t events = {.rows = 8192};
    CHECK(wl_session_snapshot(cal.session, projection_observe,
        &events) == 0 && events.keys == 8192);
    uint64_t allowance = budget_peak;
    destroy(&cal);
    for (unsigned retained = 0; retained < 2; retained++) {
        budget_peak = 0;
        fixture_t f = projection_fixture(allowance, 8192);
        wl_col_session_t *s = COL_SESSION(f.session);
        wl_columnar_session_test_retain_cold_source = retained != 0;
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == OK);
        wl_columnar_session_test_retain_cold_source = false;
        events = (projection_events_t){.rows = 8192};
        int rc = wl_session_snapshot(f.session, projection_observe, &events);

        if (!retained)CHECK(rc == 0 && events.keys == 8192 &&
                events.sum == UINT64_C(8192)*8191/2 && events.joined == 1);
        else CHECK(rc == ENOSPC && wl_session_budget_denied(f.session) &&
                !events.keys);
        CHECK(budget_peak <= allowance && s->cold_relation &&
            !session_find_rel(s, "b"));
        destroy(&f);
    }
    /* A real 64-column output cannot fit this unchanged 1 MiB allowance. */
    fixture_t f = projection_fixture(1024*1024, 8192);
    wl_col_session_t *s = COL_SESSION(f.session);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    wl_plan_relation_t wide = *projection_plan(&f);
    wl_plan_op_t ops[3]; memcpy(ops, wide.ops, sizeof(ops)); wide.ops = ops;
    uint32_t indices[64] = {0};
    ops[1].project_indices = indices; ops[1].project_count = 64;
    ops[1].map_exprs = NULL; ops[1].map_expr_count = 0;
    uint64_t before = reserved(&f);
    eval_stack_t stack; eval_stack_init(&stack);
    CHECK(wl_columnar_ops_cold_projection(&wide, &stack, s) == ENOSPC);
    CHECK(!stack.top && reserved(&f) == before && s->cold_relation);
    CHECK(wl_columnar_ops_cold_projection(projection_plan(&f), &stack, s) == 0);
    eval_entry_t e = eval_stack_pop(&stack);
    CHECK(eval_stack_dispose_entry(&stack, &e) == 0);
    destroy(&f);
    wl_columnar_memory_governor_test_admitted = NULL;
}

static void projection_scratch_floor(void)
{
    uint64_t extra[2];
    for (unsigned mode = 0; mode < 2; mode++) {
        fixture_t f = projection_fixture(4*1024*1024, mode?8192:128);
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == OK);
        uint64_t before = reserved(&f); budget_peak = before;
        wl_columnar_memory_governor_test_admitted = budget_admitted;
        eval_stack_t stack; eval_stack_init(&stack);
        CHECK(wl_columnar_ops_cold_projection(projection_plan(&f), &stack,
            COL_SESSION(f.session)) == 0);
        uint64_t output = reserved(&f)-before;
        CHECK(budget_peak >= before+output);
        extra[mode] = budget_peak-before-output;
        eval_entry_t e = eval_stack_pop(&stack);
        CHECK(eval_stack_dispose_entry(&stack, &e) == 0);
        CHECK(reserved(&f) == before);
        wl_columnar_memory_governor_test_admitted = NULL;
        destroy(&f);
    }
    CHECK(extra[0] > 0 && extra[0] == extra[1]);
}
static void projection_numeric_lanes(void)
{
    const wirelog_column_type_t kinds[] = {WIRELOG_TYPE_FLOAT,
                                           WIRELOG_TYPE_UINT64,
                                           WIRELOG_TYPE_BOOL};
    for (unsigned kind = 0; kind < 3; kind++) {
        fixture_t f = projection_fixture(4*1024*1024, 4);
        col_rel_t *source = session_find_rel(COL_SESSION(f.session), "b");
        wirelog_column_type_t initial[] = {WIRELOG_TYPE_INT64,
                                           WIRELOG_TYPE_INT64};
        CHECK(col_rel_set_column_types(source, initial, 2) == 0);
        uint64_t bits[4] = {0, 0, 0, 0};
        if (kind == 0) {
            bits[0] = UINT64_C(0x8000000000000000);
            bits[1] = UINT64_C(0xbff8000000000000); bits[2] = bits[1];
            bits[3] = UINT64_C(0x3ff4000000000000);
        }else if (kind == 1) {
            bits[0] = UINT64_MAX; bits[1] = UINT64_C(0x8000000000000000);
            bits[2] = bits[1]; bits[3] = 0;
        }else {
            bits[0] = 1; bits[1] = 0; bits[2] = 0; bits[3] = 1;
        }
        for (unsigned i = 0; i < 4;
            i++) memcpy(&source->columns[1][i], &bits[i], 8);
        wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, kinds[kind]};
        CHECK(col_rel_set_column_types(source, types, 2) == 0);
        if (kind == 0) bits[0] = 0; /* Existing relation zero canonicalization. */
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == OK);
        wl_plan_relation_t rp = *projection_plan(&f); wl_plan_op_t ops[3];
        memcpy(ops, rp.ops, sizeof(ops)); rp.ops = ops;
        uint32_t indices[] = {1, 0, 1}; ops[1].project_count = 3;
        ops[1].project_indices = indices; ops[1].map_exprs = NULL;
        ops[1].map_expr_count = 0;
        eval_stack_t stack; eval_stack_init(&stack);
        CHECK(wl_columnar_ops_cold_projection(&rp, &stack,
            COL_SESSION(f.session)) == 0);
        eval_entry_t e = eval_stack_pop(&stack);
        CHECK(e.rel->nrows == 4 && e.rel->column_types[0] == kinds[kind] &&
            e.rel->column_types[2] == kinds[kind]);
        for (unsigned i = 0; i < 4; i++) {
            uint64_t actual; memcpy(&actual, &e.rel->columns[0][i], 8);
            CHECK(actual == bits[i]); memcpy(&actual, &e.rel->columns[2][i], 8);
            CHECK(actual == bits[i]);
        }
        CHECK(eval_stack_dispose_entry(&stack, &e) == 0);
        ops[1].project_count = 1;
        CHECK(col_eval_relation_plan(&rp, &stack, COL_SESSION(f.session)) == 0);
        e = eval_stack_pop(&stack);
        CHECK(e.rel->nrows == (kind == 2 ? 2u : 3u));
        CHECK(eval_stack_dispose_entry(&stack, &e) == 0); destroy(&f);
    }
}

int main(void)
{
    projection_consumers(); projection_failures(); projection_shape();
    projection_budget(); projection_scratch_floor(); projection_numeric_lanes();
    lifecycle(); rollback(); read_failure(); exclusions(); empty_and_multiple();
    typed_projection(); quota_and_alias(); loaders(); generation_boundary();
    fixed_budget(); retention_lifetime(); fixed_floors();
    concurrent_cold_readers();
    return 0;
}
