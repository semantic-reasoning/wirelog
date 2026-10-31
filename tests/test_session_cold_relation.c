/* Persistent #1372 sealed-input reclamation through the ordinary evaluator. */
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

#ifdef _WIN32
static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}

#define unsetenv wl_test_unsetenv_
#endif

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
    CHECK(!strcmp(name, "result") && cols == 5);
    CHECK((uint64_t)row[0] == UINT64_C(0x3ff8000000000000) &&
        (uint64_t)row[1] == UINT64_C(0xfffffffffffffff0) &&
        row[2] == 7 && row[3] == row[0] && row[4] == 1);
    (*(unsigned *)ctx)++;
}
static void typed_projection(void)
{
    for (unsigned direction = 0; direction < 5; direction++) {
        fixture_t f = fixture(
            ".decl a(k: int64, f: int64)\n.decl b(k: int64, v: float, u: int64)\n"
            ".decl result(v: float, u: int64, k: int64, again: float, flag: int64)\n"
            "result(v,u,k,v,f) :- a(k,f),b(k,v,u).\n", 4*1024*1024, 1024*1024);
        int64_t left[] = {7, 1};
        uint64_t bits[] = {7, UINT64_C(0x3ff8000000000000),
                           UINT64_C(0xfffffffffffffff0)};
        int64_t right[3]; memcpy(right, bits, sizeof(right));
        CHECK(wl_session_insert(f.session, "a", left, 1, 2) == 0);
        CHECK(wl_session_insert(f.session, "b", right, 1, 3) == 0);
        /* The parser has no uint64 declaration spelling; the internal host may
         * provide an explicit physical numeric schema. */
        wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT,
                                         WIRELOG_TYPE_UINT64};
        CHECK(col_rel_set_column_types(session_find_rel(COL_SESSION(f.session),
            "b"),
            types, 3) == 0);
        wirelog_column_type_t left_types[] = {WIRELOG_TYPE_INT64,
                                              WIRELOG_TYPE_BOOL};
        CHECK(col_rel_set_column_types(session_find_rel(COL_SESSION(f.session),
            "a"), left_types, 2) == 0);
        if (direction == 4) CHECK(wl_columnar_session_seal_input(f.session, "b",
                NULL, NULL) == OK);
        if (direction == 1 ||
            direction >= 3) CHECK(wl_columnar_session_seal_input(f.session, "a",
                NULL, NULL) == OK);
        if (direction == 2 ||
            direction == 3) CHECK(wl_columnar_session_seal_input(f.session, "b",
                NULL, NULL) == OK);
        unsigned count = 0;
        CHECK(wl_session_snapshot(f.session, typed_observe,
            &count) == 0 && count == 1);
        col_rel_t *out = session_find_rel(COL_SESSION(f.session), "result");
        CHECK(out && out->column_types &&
            out->column_types[0] == WIRELOG_TYPE_FLOAT &&
            out->column_types[1] == WIRELOG_TYPE_UINT64 &&
            out->column_types[2] == WIRELOG_TYPE_INT64 &&
            out->column_types[3] == WIRELOG_TYPE_FLOAT &&
            out->column_types[4] == WIRELOG_TYPE_BOOL);
        destroy(&f);
    }
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
static const char *filtered_projection_query =
    ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:int64)\n"
    ".decl result(k:int64,x:int64,y:int64)\n.decl keys(k:int64)\n"
    "result(k,x,y + 0) :- a(k,x),b(k,y).\n"
    "keys(k) :- b(k,v), v >= 164.\n";
static const wl_plan_relation_t *projection_plan(fixture_t *f)
{
    for (uint32_t i = 0; i < f->plan->stratum_count; i++)
        for (uint32_t j = 0; j < f->plan->strata[i].relation_count; j++) {
            const wl_plan_relation_t *p = &f->plan->strata[i].relations[j];
            if (!strcmp(p->name, "keys"))return p;
        }
    abort();
}
static fixture_t projection_fixture_mode(uint64_t allowance, unsigned rows,
    bool filtered)
{
    fixture_t f =
        fixture(filtered ? filtered_projection_query : projection_query,
            allowance, 8*1024*1024);
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
static fixture_t projection_fixture(uint64_t allowance, unsigned rows)
{
    return projection_fixture_mode(allowance, rows, false);
}
typedef struct {unsigned rows, keys, joined, first; uint64_t sum;
                unsigned char seen[1024];} projection_events_t;
static void projection_observe(const char *name, const int64_t *r, uint32_t n,
    void *ctx)
{
    projection_events_t *e = ctx;
    if (!strcmp(name, "keys")) {
        CHECK(n == 1 && r[0] >= e->first && (uint64_t)r[0] < e->rows);
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
static void projection_consumers(bool filtered)
{
    for (unsigned empty = 0; empty < 2; empty++)for (unsigned cold =
            0; cold < 3;
            cold++){
            unsigned rows = empty?0:128;
            unsigned first = filtered && rows ? 64 : 0;
            fixture_t f = projection_fixture_mode(4*1024*1024, rows, filtered);
            if (cold) CHECK(wl_columnar_session_seal_join_input(f.session, "b",
                    NULL, NULL) == OK);
            if (cold == 2) {
                CHECK(wl_session_step(f.session) == 0);
                CHECK(wl_session_step(f.session) == 0);
                col_rel_t *head = session_find_rel(COL_SESSION(f.session),
                        "keys");
                CHECK(head && head->nrows == rows-first);
            }
            for (unsigned repeat = 0; repeat < 2; repeat++) {
                projection_events_t e = {.rows = rows, .first = first};
                int rc = wl_session_snapshot(f.session, projection_observe, &e);
                if (rc) fprintf(stderr,
                        "projection snapshot rc=%d cold=%u empty=%u repeat=%u\n",
                        rc, cold, empty, repeat);
                CHECK(rc == 0);
                CHECK(e.keys == rows-first && e.joined == !empty &&
                    e.sum ==
                    (uint64_t)rows*(rows?rows-1:0)/2 - (uint64_t)first*
                    (first?first-1:0)/2);
            }
            if (cold) CHECK(!session_find_rel(COL_SESSION(f.session), "b"));
            destroy(&f);
        }
}
static void projection_failures(bool filtered)
{
    fixture_t f = projection_fixture_mode(4*1024*1024, 128, filtered);
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
    if (filtered) {
        /* Fail after a passing private prefix as well as rejected rows. */
        wl_plan_relation_t all = *rp;
        wl_plan_op_t ops[4]; memcpy(ops, rp->ops, sizeof(ops)); all.ops = ops;
        uint8_t predicate[17]; memcpy(predicate, ops[1].filter_expr.data, 17);
        memset(predicate+8, 0, 8); ops[1].filter_expr.data = predicate;
        read_fault = 5;
        CHECK(wl_columnar_ops_cold_projection(&all, &stack, s) == EIO);
        CHECK(reserved(&f) == before && stack.top == 0);
    }
    wl_columnar_ops_test_map_fail_output_alloc = true;
    CHECK(wl_columnar_ops_cold_projection(rp, &stack, s) == ENOMEM);
    wl_columnar_ops_test_map_fail_output_alloc = false;
    CHECK(reserved(&f) == before);
    allocation_fault = 1;
    CHECK(wl_columnar_ops_cold_projection(rp, &stack, s) == ENOMEM);
    CHECK(reserved(&f) == before);
    CHECK(wl_columnar_ops_cold_projection(rp, &stack, s) == 0);
    eval_entry_t e = eval_stack_pop(&stack);
    CHECK(e.rel->nrows == (filtered ? 64u : 128u));
    CHECK(eval_stack_dispose_entry(&stack, &e) == 0 && reserved(&f) == before);
    CHECK(col_op_variable(&rp->ops[0], &stack, s) == ENOTSUP);
    CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
    projection_events_t events = {.rows = 128, .first = filtered ? 64u : 0u};
    read_fault = 5;
    CHECK(wl_session_snapshot(f.session, projection_observe, &events) == EIO);
    CHECK(!events.keys && !events.joined && s->cold_relation);
    events = (projection_events_t){.rows = 128, .first = filtered ? 64u : 0u};
    CHECK(wl_session_snapshot(f.session, projection_observe,
        &events) == 0 && events.keys == (filtered ? 64u : 128u));
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

static void projection_budget(bool filtered)
{
    unsigned first = filtered ? 64u : 0u;
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    budget_peak = 0;
    fixture_t cal = projection_fixture_mode(8*1024*1024, 8192, filtered);
    CHECK(wl_columnar_session_seal_join_input(cal.session, "b", NULL,
        NULL) == OK);
    projection_events_t events = {.rows = 8192, .first = first};
    CHECK(wl_session_snapshot(cal.session, projection_observe,
        &events) == 0 && events.keys == 8192-first);
    uint64_t allowance = budget_peak;
    destroy(&cal);
    for (unsigned retained = 0; retained < 2; retained++) {
        budget_peak = 0;
        fixture_t f = projection_fixture_mode(allowance, 8192, filtered);
        wl_col_session_t *s = COL_SESSION(f.session);
        wl_columnar_session_test_retain_cold_source = retained != 0;
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == OK);
        wl_columnar_session_test_retain_cold_source = false;
        events = (projection_events_t){.rows = 8192, .first = first};
        int rc = wl_session_snapshot(f.session, projection_observe, &events);

        if (!retained)CHECK(rc == 0 && events.keys == 8192-first &&
                events.sum ==
                UINT64_C(8192)*8191/2 - (uint64_t)first*(first?first-1:0)/2 &&
                events.joined == 1);
        else CHECK(rc == ENOSPC && wl_session_budget_denied(f.session) &&
                !events.keys);
        CHECK(budget_peak <= allowance && s->cold_relation &&
            !session_find_rel(s, "b"));
        destroy(&f);
    }
    /* A real 64-column output cannot fit this unchanged 1 MiB allowance. */
    fixture_t f = projection_fixture_mode(1024*1024, 8192, filtered);
    wl_col_session_t *s = COL_SESSION(f.session);
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    wl_plan_relation_t wide = *projection_plan(&f);
    wl_plan_op_t ops[4]; memcpy(ops, wide.ops, wide.op_count*sizeof(*ops));
    wide.ops = ops;
    wl_plan_op_t *map = &ops[wide.op_count-2];
    uint32_t indices[64] = {0};
    map->project_indices = indices; map->project_count = 64;
    map->map_exprs = NULL; map->map_expr_count = 0;
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

static void projection_scratch_floor(bool filtered)
{
    uint64_t extra[2];
    for (unsigned mode = 0; mode < 2; mode++) {
        fixture_t f = projection_fixture_mode(4*1024*1024, mode?8192:128,
                filtered);
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
static void projection_numeric_lanes(bool filtered)
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
        wl_plan_relation_t rp = *projection_plan(&f); wl_plan_op_t ops[4];
        uint8_t predicate[17] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0',
                                 WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0, 0, 0, 0, 0,
                                 WL_PLAN_EXPR_CMP_GTE};
        ops[0] = rp.ops[0];
        unsigned mi = filtered ? 2u : 1u;
        if (filtered) ops[1] = (wl_plan_op_t){.op = WL_PLAN_OP_FILTER,
                                              .filter_expr = {.data = predicate,
                                                              .size =
                                                                  sizeof(
                                                                  predicate)}};
        ops[mi] = rp.ops[1]; ops[mi+1] = rp.ops[2];
        rp.ops = ops; rp.op_count = mi+2;
        uint32_t indices[] = {1, 0, 1}; ops[mi].project_count = 3;
        ops[mi].project_indices = indices; ops[mi].map_exprs = NULL;
        ops[mi].map_expr_count = 0;
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
        ops[mi].project_count = 1;
        CHECK(col_eval_relation_plan(&rp, &stack, COL_SESSION(f.session)) == 0);
        e = eval_stack_pop(&stack);
        CHECK(e.rel->nrows == (kind == 2 ? 2u : 3u));
        CHECK(eval_stack_dispose_entry(&stack, &e) == 0); destroy(&f);
    }
}

/* Compare the bounded consumer with ordinary FILTER/MAP/CONSOLIDATE for
 * every signed comparison, boundary literal, and reordered/repeated lane. */
static void filtered_predicates(void)
{
    const int64_t values[] = {INT64_MIN, -1, 0, INT64_MAX};
    for (unsigned cmp = WL_PLAN_EXPR_CMP_EQ; cmp <= WL_PLAN_EXPR_CMP_GTE; cmp++)
        for (unsigned literal = 0; literal < 4; literal++) {
            fixture_t f = projection_fixture(4*1024*1024, 4);
            col_rel_t *source = session_find_rel(COL_SESSION(f.session), "b");
            for (unsigned i = 0; i < 4; i++) source->columns[1][i] = values[i];
            wl_plan_relation_t rp = *projection_plan(&f);
            wl_plan_op_t ops[4] = {rp.ops[0], {0}, rp.ops[1], rp.ops[2]};
            uint8_t data[17] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
                                WL_PLAN_EXPR_CONST_INT};
            uint64_t bits; memcpy(&bits, &values[literal], 8);
            for (unsigned i = 0; i < 8;
                i++) data[8+i] = (uint8_t)(bits >> (8*i));
            data[16] = (uint8_t)cmp;
            ops[1].op = WL_PLAN_OP_FILTER;
            ops[1].filter_expr = (wl_plan_expr_buffer_t){.data = data,
                                                         .size = 17};
            uint32_t indices[] = {1, 0, 1};
            ops[2].project_indices = indices; ops[2].project_count = 3;
            ops[2].map_exprs = NULL; ops[2].map_expr_count = 0;
            rp.ops = ops; rp.op_count = 4;
            CHECK(wl_columnar_eval_plan_cold_projection(&rp, "b", 2, NULL));
            eval_stack_t stack; eval_stack_init(&stack);
            CHECK(col_eval_relation_plan(&rp, &stack,
                COL_SESSION(f.session)) == 0);
            eval_entry_t resident = eval_stack_pop(&stack);
            int64_t expected[4][3]; uint32_t count = resident.rel->nrows;
            for (uint32_t i = 0; i < count; i++)
                for (unsigned j = 0; j < 3;
                    j++) expected[i][j] = resident.rel->columns[j][i];
            CHECK(eval_stack_dispose_entry(&stack, &resident) == 0);
            CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
                NULL) == OK);
            CHECK(col_eval_relation_plan(&rp, &stack,
                COL_SESSION(f.session)) == 0);
            eval_entry_t cold = eval_stack_pop(&stack);
            CHECK(cold.rel->nrows == count && cold.rel->ncols == 3);
            for (uint32_t i = 0; i < count; i++)
                for (unsigned j = 0; j < 3;
                    j++) CHECK(expected[i][j] == cold.rel->columns[j][i]);
            CHECK(eval_stack_dispose_entry(&stack, &cold) == 0);
            destroy(&f);
        }
}
static void filtered_shape(void)
{
    fixture_t f = projection_fixture_mode(4*1024*1024, 4, true);
    wl_plan_relation_t *rp = (wl_plan_relation_t *)projection_plan(&f);
    CHECK(rp->op_count == 4 && rp->ops[1].op == WL_PLAN_OP_FILTER);
    wl_plan_op_t *filter = (wl_plan_op_t *)&rp->ops[1];
    wl_plan_expr_buffer_t original = filter->filter_expr;
    uint8_t data[64]; CHECK(original.size == 17);
    uint64_t before = reserved(&f);
    for (unsigned bad = 0; bad < 12; bad++) {
        memcpy(data, original.data, original.size);
        filter->filter_expr = (wl_plan_expr_buffer_t){.data = data, .size = 17};
        switch (bad) {
        case 0: filter->filter_expr.data = NULL; break;
        case 1: filter->filter_expr.size = 0; break;
        case 2: filter->filter_expr.size = 16; break;
        case 3: data[17] = 0; filter->filter_expr.size = 18; break;
        case 4: data[0] = WL_PLAN_EXPR_VAR_FLOAT; break;
        case 5: data[6] = '2'; break;
        case 6: data[7] = WL_PLAN_EXPR_CONST_FLOAT; break;
        case 7: data[16] = WL_PLAN_EXPR_ARITH_ADD; break;
        case 8: data[1] = 255; data[2] = 255; break;
        case 9: /* Integer overflow in the column spelling. */
            data[1] = 13; data[2] = 0; memcpy(data+3, "col4294967297", 13);
            memcpy(data+16, original.data+7, 10); filter->filter_expr.size = 26;
            break;
        case 10: /* Reversed operands. */
            memcpy(data, original.data+7, 9); memcpy(data+9, original.data, 7);
            break;
        case 11: data[6] = '-'; break;
        }
        CHECK(!wl_columnar_eval_plan_cold_projection(rp, "b", 2, NULL));
        CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
            NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
        CHECK(session_find_rel(COL_SESSION(f.session),
            "b") && reserved(&f) == before);
    }
    filter->filter_expr = original;
    const wirelog_column_type_t kinds[] = {WIRELOG_TYPE_FLOAT,
                                           WIRELOG_TYPE_UINT64,
                                           WIRELOG_TYPE_BOOL};
    for (unsigned i = 0; i < 3; i++) {
        wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, kinds[i]};
        CHECK(!wl_columnar_eval_plan_cold_projection(rp, "b", 2, types));
    }
    wl_plan_op_t extra[5] = {rp->ops[0], rp->ops[1], rp->ops[1], rp->ops[2],
                             rp->ops[3]};
    wl_plan_relation_t changed = *rp; changed.ops = extra; changed.op_count = 5;
    CHECK(wl_columnar_eval_plan_cold_projection(&changed, "b", 2, NULL));
    changed.op_count = 4; extra[1] = rp->ops[2]; extra[2] = rp->ops[1];
    extra[3] = rp->ops[3];
    CHECK(!wl_columnar_eval_plan_cold_projection(&changed, "b", 2, NULL));
    CHECK(wl_columnar_session_seal_join_input(f.session, "b", NULL,
        NULL) == OK);
    /* Runtime must refuse a changed predicate, before taking a reader. */
    filter->filter_expr = (wl_plan_expr_buffer_t){.data = NULL, .size = 17};
    eval_stack_t stack; eval_stack_init(&stack); before = reserved(&f);
    CHECK(wl_columnar_ops_cold_projection(rp, &stack,
        COL_SESSION(f.session)) == ENOTSUP);
    CHECK(!stack.top && reserved(&f) == before);
    filter->filter_expr = original;
    CHECK(wl_columnar_ops_cold_projection(rp, &stack,
        COL_SESSION(f.session)) == 0);
    eval_entry_t entry = eval_stack_pop(&stack); CHECK(entry.rel->nrows == 0);
    CHECK(eval_stack_dispose_entry(&stack, &entry) == 0);
    destroy(&f);
}

static void standalone_reads(void)
{
    for (unsigned filtered = 0; filtered < 2; filtered++) {
        char text[256];
        snprintf(text, sizeof(text),
            ".decl b(k:int64,v:int64)\n.decl keys(k:int64)\n"
            "keys(k) :- b(k,v)%s.\n", filtered ? ", v >= 164" : "");
        fixture_t f = fixture(text, 1024*1024, 8*1024*1024);
        for (unsigned i = 0; i < 8192; i++) {
            int64_t r[] = {(int64_t)i, 100+(int64_t)i};
            CHECK(wl_session_insert(f.session, "b", r, 1, 2) == 0);
        }
        CHECK(wl_columnar_session_seal_input(f.session, "b", NULL, NULL) == OK);
        wl_col_session_t *s = COL_SESSION(f.session);
        CHECK(s->cold_relation && !session_find_rel(s, "b"));
        const wl_plan_relation_t *rp = projection_plan(&f);
        wl_plan_relation_t wide = *rp;
        wl_plan_op_t ops[4]; memcpy(ops, rp->ops, rp->op_count*sizeof(*ops));
        wide.ops = ops;
        wl_plan_op_t *map = &ops[rp->op_count-2];
        uint32_t indices[64] = {0};
        map->project_indices = indices; map->project_count = 64;
        map->map_exprs = NULL; map->map_expr_count = 0;
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        eval_stack_t stack; eval_stack_init(&stack);
        CHECK(wl_columnar_ops_cold_projection(&wide, &stack, s) == ENOSPC);
        CHECK(!stack.top && reserved(&f) == before);
        projection_events_t events = {.rows = 8192,
                                      .first = filtered ? 64u : 0u};
        read_fault = 5;
        CHECK(wl_session_snapshot(f.session, projection_observe,
            &events) == EIO);
        CHECK(!events.keys && !events.joined && s->cold_relation);
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
        CHECK(wl_columnar_session_seal_input(f.session, "b", NULL,
            NULL) == WL_COLUMNAR_SPILL_BUSY);
        CHECK(wl_session_snapshot(f.session, projection_observe, &events) == 0);
        CHECK(events.keys == 8192-events.first && !events.joined);
        CHECK(events.sum == UINT64_C(8192)*8191/2 -
            (uint64_t)events.first*(events.first ? events.first-1 : 0)/2);
        destroy(&f);
    }
}

static const char *multiple_query =
    ".decl a(k:int64,v:int64)\n.decl b(k:int64,x:int64,v:int64)\n"
    ".decl c(k:int64,v:int64)\n.decl unused(k:int64)\n"
    ".decl pa(k:int64)\n.decl pb(k:int64)\n.decl pc(k:int64)\n"
    "pa(k) :- a(k,v), v >= 132.\npb(k) :- b(k,x,v), v >= 216.\n"
    "pc(k) :- c(k,v).\n";
static fixture_t multiple_fixture(uint64_t quota)
{
    fixture_t f = fixture(multiple_query, 4*1024*1024, quota);
    int64_t unused = 0;
    CHECK(wl_session_insert(f.session, "unused", &unused, 1, 1) == 0);
    for (int64_t i = 0; i < 64; i++) {
        int64_t a[] = {i, 100+i}, b[] = {i, -i, 200+i}, c[] = {i, 300+i};
        CHECK(wl_session_insert(f.session, "a", a, 1, 2) == 0);
        CHECK(wl_session_insert(f.session, "b", b, 1, 3) == 0);
        CHECK(wl_session_insert(f.session, "c", c, 1, 2) == 0);
    }
    return f;
}
typedef struct { uint64_t bits[3]; } multiple_events_t;
static void multiple_observe(const char *name, const int64_t *row,
    uint32_t width,
    void *context)
{
    multiple_events_t *e = context;
    unsigned index = !strcmp(name, "pa") ? 0u : !strcmp(name, "pb") ? 1u : 2u;
    CHECK(index != 2 || !strcmp(name, "pc"));
    CHECK(width == 1 && row[0] >= 0 && row[0] < 64);
    uint64_t bit = UINT64_C(1) << (unsigned)row[0];
    CHECK(!(e->bits[index] & bit)); e->bits[index] |= bit;
}
static void multiple_result(fixture_t *f)
{
    multiple_events_t e = {0};
    CHECK(wl_session_snapshot(f->session, multiple_observe, &e) == 0);
    CHECK(e.bits[0] == (UINT64_MAX << 32) &&
        e.bits[1] == (UINT64_MAX << 16) && e.bits[2] == UINT64_MAX);
}
static void multiple_bindings(void)
{
    const char *orders[][3] = {{"a", "b", "c"}, {"c", "b", "a"},
                               {"b", "a", "c"}};
    for (unsigned order = 0; order < 3; order++)
        for (unsigned count = 0; count < 4; count++) {
            fixture_t f = multiple_fixture(1024*1024);
            wl_col_session_t *s = COL_SESSION(f.session);
            for (unsigned i = 0; i < count; i++) {
                CHECK(wl_columnar_session_seal_input(f.session,
                    orders[order][i], NULL, NULL) == OK);
                CHECK(wl_columnar_session_cold_find(s, orders[order][i]));
                CHECK(!session_find_rel(s, orders[order][i]));
                CHECK(wl_columnar_session_seal_input(f.session,
                    orders[order][i], NULL, NULL) == WL_COLUMNAR_SPILL_BUSY);
            }
            CHECK(wl_columnar_session_seal_input(f.session, "unused", NULL,
                NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
            if (wl_columnar_session_cold_find(s, "c"))
                CHECK(s->last_inserted_relation ==
                    wl_columnar_session_cold_find(s, "c")->name);
            for (unsigned i = 0; i < count; i++) {
                wl_plan_op_t op = {.op = WL_PLAN_OP_VARIABLE,
                                   .relation_name = orders[order][i]};
                eval_stack_t stack; eval_stack_init(&stack);
                CHECK(col_op_variable(&op, &stack, s) == ENOTSUP && !stack.top);
            }
            if (count == 3) {
                multiple_events_t e = {0}; read_fault = 5;
                CHECK(wl_session_snapshot(f.session, multiple_observe,
                    &e) == EIO);
                CHECK(!e.bits[0] && !e.bits[1] && !e.bits[2]);
            } else CHECK(wl_session_step(f.session) == 0);
            multiple_result(&f); multiple_result(&f);
            CHECK(wl_columnar_session_seal_input(f.session, "c", NULL,
                NULL) == WL_COLUMNAR_SPILL_BUSY);
            destroy(&f);
        }
    for (unsigned retain = 0; retain < 2; retain++) {
        fixture_t f = multiple_fixture(1024*1024);
        wl_col_session_t *s = COL_SESSION(f.session);
        uint64_t before = reserved(&f);
        wl_columnar_session_test_retain_cold_source = retain != 0;
        for (unsigned i = 0; i < 3; i++)
            CHECK(wl_columnar_session_seal_input(f.session, orders[0][i], NULL,
                NULL) == OK);
        wl_columnar_session_test_retain_cold_source = false;
        CHECK(retain ? reserved(&f) > before : reserved(&f) < before);
        unsigned nodes = 0;
        for (wl_columnar_session_cold_relation_t *c = s->cold_relation; c;
            c = c->next){
            CHECK((c->retained_source != NULL) == (retain != 0)); nodes++;
        }
        CHECK(nodes == 3 &&
            wl_columnar_session_test_has_retained_cold_source(s) ==
            (retain != 0));
        multiple_result(&f); destroy(&f);
        CHECK(!wl_columnar_session_test_has_retained_cold_source(NULL));
    }
}
static void multiple_seal_failures(void)
{
    for (unsigned later = 0; later < 2; later++)
        for (unsigned kind = 0; kind < 5; kind++) {
            fixture_t f = multiple_fixture(1024*1024);
            wl_col_session_t *s = COL_SESSION(f.session);
            CHECK(wl_columnar_session_seal_input(f.session, "a", NULL,
                NULL) == OK);
            wl_columnar_session_cold_relation_t *first =
                wl_columnar_session_cold_find(s, "a");
            if (later) CHECK(wl_columnar_session_seal_input(f.session, "b",
                    NULL, NULL) == OK);
            const char *failed = later ? "c" : "b";
            wl_columnar_session_cold_relation_t *second =
                wl_columnar_session_cold_find(s, "b");
            col_rel_t *source = session_find_rel(s, failed);
            uint64_t before = reserved(&f),
                disk = wl_columnar_spill_store_bytes(s->spill_store);
            cancellation_t cancellation = {.stop = 3};
            wl_columnar_source_access_reader_t lease = {0};
            wl_columnar_spill_status_t expected = WL_COLUMNAR_SPILL_IO;
            if (kind == 0) write_fault = 1;
            if (kind == 1) {
                allocation_fault = 1; expected = WL_COLUMNAR_SPILL_ALLOCATION;
            }
            if (kind == 2) expected = WL_COLUMNAR_SPILL_CANCELLED;
            if (kind == 3) {
                CHECK(col_rel_source_reader_acquire(source, &lease) == 0);
                expected = WL_COLUMNAR_SPILL_BUSY;
            }
            wl_plan_expr_buffer_t saved = {0}; wl_plan_op_t *filter = NULL;
            if (kind == 4) {
                for (uint32_t i = 0; i < f.plan->stratum_count; i++)
                    for (uint32_t j = 0; j < f.plan->strata[i].relation_count;
                        j++) {
                        const wl_plan_relation_t *rp =
                            &f.plan->strata[i].relations[j];
                        if (!strcmp(rp->name,
                            "pa")) filter = (wl_plan_op_t *)&rp->ops[1];
                    }
                CHECK(filter && filter->op == WL_PLAN_OP_FILTER);
                saved = filter->filter_expr; filter->filter_expr.data = NULL;
                expected = WL_COLUMNAR_SPILL_UNSUPPORTED;
            }
            CHECK(wl_columnar_session_seal_input(f.session, failed,
                kind == 2 ? cancel : NULL,
                &cancellation) == expected);
            if (kind == 3) CHECK(col_rel_source_reader_release(&lease) == 0);
            if (filter) filter->filter_expr = saved;
            CHECK(wl_columnar_session_cold_find(s,
                "a") == first && !first->next);
            CHECK(session_find_rel(s,
                failed) == source && !wl_columnar_session_cold_find(s, failed));
            CHECK(reserved(&f) == before &&
                wl_columnar_spill_store_bytes(s->spill_store) == disk);
            CHECK(wl_columnar_session_cold_find(s, "b") == second);
            CHECK(wl_columnar_session_seal_input(f.session, failed, NULL,
                NULL) == OK);
            if (!later) CHECK(wl_columnar_session_seal_input(f.session, "c",
                    NULL, NULL) == OK);
            multiple_result(&f); destroy(&f);
        }
    /* Combined persistent quota refuses a later capture, preserving a usable mixed state. */
    for (unsigned third = 0; third < 2; third++) {
        fixture_t f = multiple_fixture(third ? 8000 : 4096);
        wl_col_session_t *s = COL_SESSION(f.session);
        CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
        if (third) CHECK(wl_columnar_session_seal_input(f.session, "b", NULL,
                NULL) == OK);
        const char *name = third ? "c" : "b";
        col_rel_t *source = session_find_rel(s, name);
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        CHECK(wl_columnar_session_seal_input(f.session, name, NULL,
            NULL) == WL_COLUMNAR_SPILL_QUOTA);
        CHECK(reserved(&f) == before &&
            wl_columnar_spill_store_bytes(s->spill_store) == disk);
        CHECK(session_find_rel(s, name) == source);
        multiple_result(&f);
        CHECK(wl_columnar_session_seal_input(f.session, name, NULL,
            NULL) == WL_COLUMNAR_SPILL_BUSY);
        destroy(&f);
    }
}

static void multiple_memory_floor(void)
{
    for (unsigned later = 0; later < 2; later++) {
        fixture_t f = multiple_fixture(1024*1024);
        wl_col_session_t *s = COL_SESSION(f.session);
        CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
        if (later) CHECK(wl_columnar_session_seal_input(f.session, "b", NULL,
                NULL) == OK);
        const char *name = later ? "c" : "b";
        col_rel_t *source = session_find_rel(s, name);
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        uint64_t bytes = 4*1024*1024 - before - 64;
        wl_columnar_memory_reservation_t token;
        wl_columnar_memory_reservation_init(&token);
        CHECK(wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(f.governor),
                bytes, &token) <= WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
        void *pressure = malloc((size_t)bytes); CHECK(pressure);
        memset(pressure, 0, (size_t)bytes);
        CHECK(wl_columnar_memory_commit(&token, pressure));
        CHECK(wl_columnar_session_seal_input(f.session, name, NULL,
            NULL) == WL_COLUMNAR_SPILL_MEMORY);
        CHECK(reserved(&f) == before+bytes && session_find_rel(s,
            name) == source);
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
        free(pressure); CHECK(wl_columnar_memory_release(&token));
        CHECK(reserved(&f) == before);
        CHECK(wl_columnar_session_seal_input(f.session, name, NULL,
            NULL) == OK);
        multiple_result(&f); destroy(&f);
    }
}

static void multiple_idb_filter_refusal(void)
{
    const char *text = ".decl a(k:int64,v:int64)\n.decl z(k:int64,v:int64)\n"
        ".decl keys(k:int64)\nz(k,v) :- a(k,v).\nkeys(k) :- z(k,v), v > 0.\n";
    fixture_t f = fixture(text, 4*1024*1024, 1024*1024);
    int64_t row[] = {1, 2};
    CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
    uint64_t before = reserved(&f);
    CHECK(wl_columnar_session_seal_input(f.session, "a", NULL,
        NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(reserved(&f) == before && session_find_rel(COL_SESSION(f.session),
        "a"));
    destroy(&f);
}

static void multiple_schema_audit(void)
{
    for (unsigned bad = 0; bad < 6; bad++) {
        fixture_t f = multiple_fixture(1024*1024);
        wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *b = session_find_rel(s, "b");
        wirelog_column_type_t *original = b->column_types;
        wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64,
                                         WIRELOG_TYPE_INT64};
        unsigned slot = 0; while (slot < s->nrels && s->rels[slot] != b) slot++;
        CHECK(slot < s->nrels);
        uint64_t before = reserved(&f);
        for (unsigned phase = 0; phase < 2; phase++) {
            b->column_types = types;
            if (bad == 0) types[2] = WIRELOG_TYPE_FLOAT;
            if (bad == 1) types[1] = (wirelog_column_type_t)99; /* Unsupported nonprojected lane. */
            if (bad == 2) b->has_graph_column = true;
            if (bad == 3) b->compound_count = 1;
            if (bad == 4) b->ncols = 2; /* Predicate col2 must use B's width. */
            if (bad == 5) s->rels[slot] = NULL;
            CHECK(wl_columnar_session_seal_input(f.session, phase ? "c" : "a",
                NULL, NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
            b->column_types = original; b->has_graph_column = false;
            b->compound_count = 0; b->ncols = 3; s->rels[slot] = b;
            CHECK(reserved(&f) == before && !wl_columnar_session_cold_find(s,
                "c"));
            if (!phase) {
                CHECK(!s->cold_relation);
                CHECK(wl_columnar_session_seal_input(f.session, "a", NULL,
                    NULL) == OK);
                before = reserved(&f);
            } else CHECK(wl_columnar_session_cold_find(s, "a"));
        }
        CHECK(wl_columnar_session_seal_input(f.session, "b", NULL, NULL) == OK);
        CHECK(wl_columnar_session_seal_input(f.session, "c", NULL, NULL) == OK);
        multiple_result(&f); destroy(&f);
    }
}
typedef struct { unsigned seen[2]; } multiple_join_events_t;
static void multiple_join_observe(const char *name, const int64_t *row,
    uint32_t n,
    void *context)
{
    multiple_join_events_t *e = context;
    unsigned which = !strcmp(name, "r") ? 0u : 1u;
    CHECK(!which || !strcmp(name, "s"));
    CHECK(n == 3 && row[0] >= 0 && row[0] < 4 && row[1] == 10+row[0]);
    CHECK(row[2] == (which ? 200 : 100)+row[0]);
    unsigned bit = 1u << (unsigned)row[0];
    CHECK(!(e->seen[which] & bit)); e->seen[which] |= bit;
}
static void multiple_right_joins(void)
{
    const char *text =
        ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:int64)\n.decl c(k:int64,v:int64)\n"
        ".decl r(k:int64,x:int64,y:int64)\n.decl s(k:int64,x:int64,y:int64)\n"
        "r(k,x,y + 0) :- a(k,x),b(k,y).\ns(k,x,y + 0) :- a(k,x),c(k,y).\n";
    for (unsigned reverse = 0; reverse < 2; reverse++) {
        fixture_t f = fixture(text, 4*1024*1024, 1024*1024);
        for (int64_t i = 0; i < 64; i++) {
            int64_t a[] = {i, 10+i}, b[] = {i, 100+i}, c[] = {i, 200+i};
            if (i < 4) CHECK(wl_session_insert(f.session, "a", a, 1, 2) == 0);
            CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
            CHECK(wl_session_insert(f.session, "c", c, 1, 2) == 0);
        }
        CHECK(wl_columnar_session_seal_input(f.session, reverse ? "c" : "b",
            NULL, NULL) == OK);
        CHECK(wl_columnar_session_seal_input(f.session, reverse ? "b" : "c",
            NULL, NULL) == OK);
        for (unsigned repeat = 0; repeat < 2; repeat++) {
            multiple_join_events_t e = {0};
            CHECK(wl_session_snapshot(f.session, multiple_join_observe,
                &e) == 0);
            CHECK(e.seen[0] == 15 && e.seen[1] == 15);
            if (!repeat) CHECK(COL_SESSION(
                        f.session)->spill_join_selected_count == 2);
        }
        destroy(&f);
    }
}

static void multiple_runtime_guard(void)
{
    fixture_t f = multiple_fixture(1024*1024);
    const char *names[] = {"c", "a", "b"};
    for (unsigned i = 0; i < 3; i++)
        CHECK(wl_columnar_session_seal_input(f.session, names[i], NULL,
            NULL) == OK);
    const wl_plan_relation_t *pb = NULL; wl_plan_op_t *map = NULL;
    for (uint32_t i = 0; i < f.plan->stratum_count; i++)
        for (uint32_t j = 0; j < f.plan->strata[i].relation_count; j++) {
            const wl_plan_relation_t *rp = &f.plan->strata[i].relations[j];
            if (!strcmp(rp->name, "pb")) pb = rp;
            if (!strcmp(rp->name, "pc")) map = (wl_plan_op_t *)&rp->ops[1];
        }
    CHECK(pb && map && map->op == WL_PLAN_OP_MAP);
    const uint32_t *original = map->project_indices;
    uint32_t invalid = 99; map->project_indices = &invalid;
    eval_stack_t stack; eval_stack_init(&stack); uint64_t before = reserved(&f);
    /* B is newest and valid; older C must still be checked before B reads. */
    CHECK(wl_columnar_ops_cold_projection(pb, &stack,
        COL_SESSION(f.session)) == ENOTSUP);
    CHECK(!stack.top && reserved(&f) == before);
    map->project_indices = original;
    CHECK(wl_columnar_ops_cold_projection(pb, &stack,
        COL_SESSION(f.session)) == 0);
    eval_entry_t e = eval_stack_pop(&stack); CHECK(e.rel->nrows == 48);
    CHECK(eval_stack_dispose_entry(&stack, &e) == 0 && reserved(&f) == before);
    multiple_result(&f); destroy(&f);
}

/* Direction matrix fixes the physical first input so optimizer reordering
 * cannot turn cold-left coverage into another cold-right test. */
static wl_plan_relation_t *join_body(fixture_t *f, const char *name)
{
    for (uint32_t i = 0; i < f->plan->stratum_count; i++)
        for (uint32_t j = 0; j < f->plan->strata[i].relation_count; j++) {
            wl_plan_relation_t *rp =
                (wl_plan_relation_t *)&f->plan->strata[i].relations[j];
            if (!strcmp(rp->name, name)) return rp;
        }
    CHECK(false); return NULL;
}
static void seal_direction(fixture_t *f, unsigned direction)
{
    if (direction == 4) CHECK(wl_columnar_session_seal_input(f->session, "b",
            NULL, NULL) == OK);
    if (direction == 1 ||
        direction >= 3) CHECK(wl_columnar_session_seal_input(f->session, "a",
            NULL, NULL) == OK);
    if (direction == 2 ||
        direction == 3) CHECK(wl_columnar_session_seal_input(f->session, "b",
            NULL, NULL) == OK);
}
typedef struct { unsigned count; bool seen[144]; } direction_events_t;
static void direction_observe(const char *name, const int64_t *row,
    uint32_t width, void *context)
{
    direction_events_t *e = context;
    CHECK(!strcmp(name, "result") && width == 3);
    CHECK(row[1] >= 10 && row[1] < 22 && row[2] >= 100 && row[2] < 112);
    unsigned x = (unsigned)(row[1] - 10), y = (unsigned)(row[2] - 100);
    CHECK(x / 6 == y / 6 && row[0] == (x < 6 ? INT64_MIN : INT64_MAX));
    unsigned ordinal = x*12 + y;
    CHECK(!e->seen[ordinal]); e->seen[ordinal] = true; e->count++;
}
static void direction_matrix(void)
{
    for (unsigned shape = 0; shape < 5; shape++)
        for (unsigned direction = 0; direction < 5; direction++) {
            fixture_t f = fixture(query, 4*1024*1024, 1024*1024);
            wl_plan_relation_t *rp = join_body(&f, "result");
            CHECK(rp->ops[0].op == WL_PLAN_OP_VARIABLE &&
                !strcmp(rp->ops[0].relation_name, "a") &&
                rp->ops[1].op == WL_PLAN_OP_JOIN &&
                !strcmp(rp->ops[1].right_relation, "b"));
            for (int64_t i = 0; i < 12; i++) {
                int64_t a[] = {i < 6 ? INT64_MIN : INT64_MAX, 10+i};
                int64_t b[] = {shape == 1 ? 0 : a[0], 100+i};
                CHECK(wl_session_insert(f.session, "a", a, 1, 2) == 0);
                CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
                if (shape == 2 || shape == 4) CHECK(wl_session_remove(f.session,
                        "a", a, 1, 2) == 0);
                if (shape == 3 || shape == 4) CHECK(wl_session_remove(f.session,
                        "b", b, 1, 2) == 0);
            }
            seal_direction(&f, direction);
            wl_col_session_t *s = COL_SESSION(f.session);
            uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
            if (direction == 4) CHECK(wl_session_step(f.session) == 0);
            for (unsigned repeat = 0; repeat < 2; repeat++) {
                direction_events_t e = {0};
                CHECK(wl_session_snapshot(f.session, direction_observe,
                    &e) == 0);
                CHECK(e.count == (shape ? 0 : 72));
                if (direction) CHECK(s->spill_join_selected_count == 1);
                CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
            }
            destroy(&f);
        }
}
static void self_observe(const char *name, const int64_t *row, uint32_t width,
    void *context)
{
    direction_events_t *e = context;
    CHECK(!strcmp(name, "result") && width == 3 && row[0] == 7);
    CHECK(row[1] >= 0 && row[1] < 12 && row[2] >= 0 && row[2] < 12);
    unsigned ordinal = (unsigned)(row[1]*12 + row[2]);
    CHECK(!e->seen[ordinal]); e->seen[ordinal] = true; e->count++;
}
static void self_join(void)
{
    const char *text =
        ".decl a(k:int64,v:int64)\n.decl result(k:int64,x:int64,y:int64)\n"
        "result(k,x,y+0) :- a(k,x),a(k,y).\n";
    for (unsigned cold = 0; cold < 2; cold++) {
        fixture_t f = fixture(text, 4*1024*1024, 1024*1024);
        wl_plan_relation_t *rp = join_body(&f, "result");
        CHECK(rp->ops[0].op == WL_PLAN_OP_VARIABLE &&
            !strcmp(rp->ops[0].relation_name, "a") &&
            rp->ops[1].op == WL_PLAN_OP_JOIN &&
            !strcmp(rp->ops[1].right_relation, "a"));
        for (int64_t i = 0; i < 12; i++) {
            int64_t row[] = {7, i};
            CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
        }
        if (cold) CHECK(wl_columnar_session_seal_input(f.session, "a", NULL,
                NULL) == OK);
        wl_col_session_t *s = COL_SESSION(f.session);
        uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
        if (cold) {
            CHECK(s->cold_relation && !s->cold_relation->next);
            read_fault = 7; unsigned count = 0;
            CHECK(wl_session_snapshot(f.session, count_only,
                &count) == EIO && count == 0);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
            read_fault = 0;
        }
        for (unsigned repeat = 0; repeat < 2; repeat++) {
            direction_events_t e = {0};
            CHECK(wl_session_snapshot(f.session, self_observe,
                &e) == 0 && e.count == 144);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
        }
        destroy(&f);
    }
}
static const char *chain_query =
    ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:int64)\n.decl c(k:int64,v:int64)\n"
    ".decl result(k:int64,x:int64,y:int64,z:int64)\n"
    "result(k,x,y,z+0) :- a(k,x),b(k,y),c(y,z).\n"
    ".decl scan(k:int64)\nscan(k) :- a(k,x).\n"
    ".decl second(k:int64,x:int64,y:int64)\nsecond(k,x,y+0) :- a(k,x),b(k,y).\n";
typedef struct { unsigned bits[3]; } chain_events_t;
static void chain_observe(const char *name, const int64_t *row, uint32_t width,
    void *context)
{
    chain_events_t *e = context;
    unsigned idx = !strcmp(name, "result") ? 0u : !strcmp(name,
            "scan") ? 1u : 2u;
    CHECK(idx != 2 || !strcmp(name, "second"));
    CHECK(row[0] >= 0 && row[0] < 8);
    CHECK(width == (idx == 0 ? 4u : idx == 1 ? 1u : 3u));
    if (idx != 1) CHECK(row[1] == row[0]+10 && row[2] == row[0]+100);
    if (idx == 0) CHECK(row[3] == row[0]+1000);
    unsigned bit = 1u << (unsigned)row[0];
    CHECK(!(e->bits[idx]&bit)); e->bits[idx] |= bit;
}
static void chained_consumers(void)
{
    for (unsigned order = 0; order < 3; order++) {
        fixture_t f = fixture(chain_query, 4*1024*1024, 1024*1024);
        wl_plan_relation_t *rp = join_body(&f, "result");
        CHECK(rp->ops[0].op == WL_PLAN_OP_VARIABLE &&
            !strcmp(rp->ops[0].relation_name,
            "a") && rp->ops[1].op == WL_PLAN_OP_JOIN);
        unsigned joins = 0;
        for (uint32_t i = 0; i < rp->op_count;
            i++) joins += rp->ops[i].op == WL_PLAN_OP_JOIN;
        CHECK(joins == 2);
        for (int64_t i = 0; i < 8; i++) {
            int64_t a[] = {i, 10+i}, b[] = {i, 100+i}, c[] = {100+i, 1000+i};
            CHECK(wl_session_insert(f.session, "a", a, 1, 2) == 0);
            CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
            CHECK(wl_session_insert(f.session, "c", c, 1, 2) == 0);
        }
        if (order) {
            const char *names[] = {"a", "b", "c"};
            for (unsigned i = 0; i < 3;
                i++) CHECK(wl_columnar_session_seal_input(f.session,
                    names[order == 1 ? i : 2-i], NULL, NULL) == OK);
        }
        for (unsigned repeat = 0; repeat < 2; repeat++) {
            chain_events_t e = {0};
            CHECK(wl_session_snapshot(f.session, chain_observe, &e) == 0);
            CHECK(e.bits[0] == 255 && e.bits[1] == 255 && e.bits[2] == 255);
        }
        if (order) CHECK(COL_SESSION(f.session)->spill_join_selected_count ==
                3);
        destroy(&f);
    }
}
static void directional_failures(void)
{
    for (unsigned direction = 1; direction <= 4; direction++)
        for (unsigned fault = 0; fault < 11; fault++) {
            const unsigned failures[] = {1, 2, 3, 4, 5, 6, 7, 8, 16, 80, 512};
            unsigned nth = failures[fault];
            fixture_t f = populated(); seal_direction(&f, direction);
            wl_col_session_t *s = COL_SESSION(f.session);
            uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
            unsigned count = 0; read_fault = nth;
            CHECK(wl_session_snapshot(f.session, count_only,
                &count) == EIO && count == 0);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
            read_fault = 0; events_t e = {0};
            CHECK(wl_session_snapshot(f.session, observe,
                &e) == 0 && e.count == 4);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
            destroy(&f);
        }
}
static void cold_left_missing_right(void)
{
    for (unsigned cold = 0; cold < 2; cold++) {
        fixture_t f = fixture(query, 4*1024*1024, 1024*1024);
        int64_t row[] = {7, 8};
        CHECK(wl_session_insert(f.session, "a", row, 1, 2) == 0);
        wl_col_session_t *s = COL_SESSION(f.session);
        CHECK(session_remove_rel(s, "b") == 0);
        CHECK(!session_find_rel(s, "b"));
        wl_plan_relation_t *rp = join_body(&f, "result");
        /* Exercise absent-right typing directly, including an out-of-left column. */
        wl_plan_op_t *op = (wl_plan_op_t *)&rp->ops[1];
        uint32_t indices[] = {1, 2, 1};
        const uint32_t *saved = op->project_indices;
        uint32_t count = op->project_count;
        op->project_indices = indices; op->project_count = 3;
        if (cold) CHECK(wl_columnar_session_seal_input(f.session, "a", NULL,
                NULL) == OK);
        eval_stack_t stack; eval_stack_init(&stack);
        if (cold) CHECK(wl_columnar_join_cold_left(s, rp, &stack) == 0);
        else {
            CHECK(col_op_variable(&rp->ops[0], &stack, s) == 0);
            CHECK(wl_columnar_join_op(op, &stack, s) == 0);
        }
        eval_entry_t e; CHECK(eval_stack_pop_relation(&stack, &e) == 0);
        CHECK(e.rel->nrows == 0 && e.rel->ncols == 3);
        for (unsigned i = 0; i < 3;
            i++) CHECK(e.rel->column_types[i] == WIRELOG_TYPE_INT64);
        CHECK(eval_stack_dispose_entry(&stack, &e) == 0);
        op->project_indices = saved; op->project_count = count;
        destroy(&f);
    }
}
static void cold_join_preflight(void)
{
    for (unsigned side = 0; side < 2; side++)
        for (unsigned kind = 0; kind < 7; kind++) {
            fixture_t f = populated();
            wl_col_session_t *s = COL_SESSION(f.session);
            wl_plan_relation_t *rp = join_body(&f, "result");
            wl_plan_op_t *op = (wl_plan_op_t *)&rp->ops[1];
            wl_plan_op_t saved = *op;
            col_rel_t *a = session_find_rel(s, "a"),
                *b = session_find_rel(s, "b");

            wirelog_column_type_t *old_a = a->column_types,
                *old_b = b->column_types;
            wirelog_column_type_t bad_a[] = {WIRELOG_TYPE_FLOAT,
                                             WIRELOG_TYPE_INT64};
            wirelog_column_type_t bad_b[] = {WIRELOG_TYPE_UINT64,
                                             WIRELOG_TYPE_INT64};
            uint32_t bad = 99;
            if (kind == 0) op->key_count = 0;
            if (kind == 1) op->key_count = 2;
            if (kind == 2) op->right_filter_expr.size = 1;
            if (kind == 3) {
                op->project_count = 1; op->project_indices = &bad;
            }
            if (kind == 4) a->column_types = bad_a;
            if (kind == 5) b->column_types = bad_b;
            if (kind == 6) op->op = WL_PLAN_OP_FILTER;
            uint64_t before = reserved(&f);
            CHECK(wl_columnar_session_seal_input(f.session, side ? "b" : "a",
                NULL, NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
            CHECK(!s->cold_relation && reserved(&f) == before &&
                session_find_rel(s, "a") == a && session_find_rel(s, "b") == b);
            *op = saved;
            a->column_types = old_a;
            b->column_types = old_b;
            seal_direction(&f, 3); events_t e = {0};
            CHECK(wl_session_snapshot(f.session, observe,
                &e) == 0 && e.count == 4);
            destroy(&f);
        }
}

static fixture_t directional_budget_fixture(uint64_t allowance, bool both)
{
    fixture_t f = fixture(query, allowance, 8*1024*1024);
    wl_col_session_t *s = COL_SESSION(f.session);
    s->spill_join_batch_bytes = 1024; s->spill_join_index_bytes = 64*1024;
    for (int64_t i = 0; i < 4096; i++) {
        int64_t a[] = {i < 128 ? 0 : 10000+i, i};
        CHECK(wl_session_insert(f.session, "a", a, 1, 2) == 0);
    }
    for (int64_t i = 0; i < (both ? 4096 : 128); i++) {
        int64_t b[] = {i < 128 ? 0 : 20000+i, 100+i};
        CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
    }
    return f;
}
typedef struct { unsigned count; uint64_t seen[256]; } reclaimed_events_t;
static void reclaimed_observe(const char *name, const int64_t *row,
    uint32_t width, void *context)
{
    reclaimed_events_t *e = context;
    CHECK(!strcmp(name,
        "result") && width == 3 && row[0] == 0 && row[1] >= 0 && row[1] < 128 &&
        row[2] >= 100 && row[2] < 228);
    unsigned pos = (unsigned)(row[1]*128 + row[2]-100);
    uint64_t bit = UINT64_C(1) << (pos%64);
    CHECK(!(e->seen[pos/64]&bit)); e->seen[pos/64] |= bit; e->count++;
}
static void directional_reclamation(void)
{
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    for (unsigned both = 0; both < 2; both++) {
        budget_peak = 0;
        fixture_t calibration = directional_budget_fixture(8*1024*1024,
                both != 0);
        seal_direction(&calibration, both ? 3 : 1);
        uint64_t preparation = budget_peak;
        reclaimed_events_t e = {0};
        CHECK(wl_session_snapshot(calibration.session, reclaimed_observe,
            &e) == 0 && e.count == 16384);
        uint64_t allowance = budget_peak;
        CHECK(allowance > preparation);
        destroy(&calibration);
        for (unsigned retain = 0; retain < 2; retain++) {
            budget_peak = 0;
            fixture_t f = directional_budget_fixture(allowance, both != 0);
            wl_col_session_t *s = COL_SESSION(f.session);
            uint64_t before = reserved(&f);
            wl_columnar_session_test_retain_cold_source = retain != 0;
            seal_direction(&f, both ? 3 : 1);
            wl_columnar_session_test_retain_cold_source = false;
            CHECK(retain ? reserved(&f) > before : reserved(&f) < before);
            unsigned owners = 0;
            for (wl_columnar_session_cold_relation_t *c = s->cold_relation; c;
                c = c->next) {
                CHECK((c->retained_source != NULL) == (retain != 0)); owners++;
            }
            CHECK(owners == (both ? 2u : 1u));
            uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
            e = (reclaimed_events_t){0};
            int rc = wl_session_snapshot(f.session, reclaimed_observe, &e);
            CHECK(budget_peak <= allowance);
            if (retain) CHECK(rc == ENOSPC &&
                    wl_session_budget_denied(f.session) && e.count == 0);
            else CHECK(rc == 0 && e.count == 16384);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
            destroy(&f);
            CHECK(!wl_columnar_session_test_has_retained_cold_source(NULL));
        }
    }
    wl_columnar_memory_governor_test_admitted = NULL;
}
static uint64_t constructor_peak;
static void constructor_result(wl_columnar_spill_status_t status)
{
    CHECK(status == OK); constructor_peak = budget_peak;
}
static void directional_bounded_scratch(void)
{
    uint64_t peaks[2] = {0};
    for (unsigned large = 0; large < 2; large++) {
        fixture_t f = fixture(query, 4*1024*1024, 1024*1024);
        for (int64_t i = 0; i < (large ? 512 : 64); i++) {
            int64_t a[] = {i, 10+i}, b[] = {10000+i, 100+i};
            CHECK(wl_session_insert(f.session, "a", a, 1, 2) == 0);
            CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
        }
        seal_direction(&f, 3);
        wl_col_session_t *s = COL_SESSION(f.session);
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        budget_peak = before; constructor_peak = 0;
        wl_columnar_memory_governor_test_admitted = budget_admitted;
        wl_columnar_join_test_spill_producer_result = constructor_result;
        eval_stack_t stack; eval_stack_init(&stack);
        CHECK(wl_columnar_join_cold_left(s, join_body(&f, "result"),
            &stack) == 0);
        peaks[large] = constructor_peak-before;
        CHECK(constructor_peak > before && budget_peak == constructor_peak);
        eval_entry_t e;
        CHECK(eval_stack_pop_relation(&stack, &e) == 0 && e.rel->nrows == 0);
        CHECK(eval_stack_dispose_entry(&stack,
            &e) == 0 && reserved(&f) == before);
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
        wl_columnar_memory_governor_test_admitted = NULL;
        wl_columnar_join_test_spill_producer_result = NULL;
        destroy(&f);
    }
    CHECK(peaks[0] == peaks[1]);
    /* Distinct resident output floor, after both inputs have been reclaimed. */
    fixture_t f = floor_fixture(256*1024, 128);
    seal_direction(&f, 3); wl_col_session_t *s = COL_SESSION(f.session);
    uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
    unsigned count = 0;
    CHECK(wl_session_snapshot(f.session, count_only,
        &count) == ENOSPC && count == 0 && wl_session_budget_denied(f.session));
    CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
    destroy(&f);
}

static void directional_constructor_floor(void)
{
    fixture_t f = populated(); seal_direction(&f, 3);
    wl_col_session_t *s = COL_SESSION(f.session);
    uint64_t before = reserved(&f),
        disk = wl_columnar_spill_store_bytes(s->spill_store);
    budget_peak = before; constructor_peak = 0;
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    wl_columnar_join_test_spill_producer_result = constructor_result;
    eval_stack_t stack; eval_stack_init(&stack);
    CHECK(wl_columnar_join_cold_left(s, join_body(&f, "result"), &stack) == 0);
    uint64_t scratch = constructor_peak-before;
    eval_entry_t e; CHECK(eval_stack_pop_relation(&stack, &e) == 0);
    CHECK(eval_stack_dispose_entry(&stack, &e) == 0 && reserved(&f) == before);
    wl_columnar_memory_governor_test_admitted = NULL;
    wl_columnar_join_test_spill_producer_result = producer_result;
    /* A real admitted allocation leaves one byte less than constructor demand. */
    uint64_t pressure_bytes = 4*1024*1024-before-scratch+1;
    wl_columnar_memory_reservation_t pressure;
    wl_columnar_memory_reservation_init(&pressure);
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
            f.governor), pressure_bytes, &pressure));
    void *allocation = malloc((size_t)pressure_bytes); CHECK(allocation);
    memset(allocation, 1, (size_t)pressure_bytes);
    CHECK(wl_columnar_memory_commit(&pressure, allocation));
    producer_calls = 0; producer_status = OK;
    CHECK(wl_columnar_join_cold_left(s, join_body(&f, "result"),
        &stack) == ENOSPC);
    CHECK(stack.top == 0 && producer_calls == 1 &&
        producer_status == WL_COLUMNAR_SPILL_MEMORY);
    CHECK(reserved(&f) == before+pressure_bytes &&
        wl_columnar_spill_store_bytes(s->spill_store) == disk);
    free(allocation); CHECK(wl_columnar_memory_release(&pressure));
    wl_columnar_join_test_spill_producer_result = NULL;
    CHECK(wl_columnar_join_cold_left(s, join_body(&f, "result"), &stack) == 0);
    CHECK(eval_stack_pop_relation(&stack, &e) == 0 && e.rel->nrows == 4);
    CHECK(eval_stack_dispose_entry(&stack, &e) == 0 && reserved(&f) == before);
    destroy(&f);
}

static void cold_intermediate_floor(void)
{
    const char *text =
        ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:int64)\n.decl c(k:int64,v:int64)\n"
        ".decl result(k:int64,x:int64,y:int64,z:int64)\nresult(k,x,y,z+0) :- a(k,x),b(k,y),c(y,z).\n";
    for (unsigned large = 0; large < 2; large++) {
        fixture_t f = fixture(text, 256*1024, 1024*1024);
        for (int64_t i = 0; i < (large ? 128 : 4); i++) {
            int64_t a[] = {0, i}, b[] = {0, 100+i};
            CHECK(wl_session_insert(f.session, "a", a, 1, 2) == 0);
            CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
        }
        int64_t c[] = {100000, 0};
        CHECK(wl_session_insert(f.session, "c", c, 1, 2) == 0);
        seal_direction(&f, 3);
        CHECK(wl_columnar_session_seal_input(f.session, "c", NULL, NULL) == OK);
        wl_col_session_t *s = COL_SESSION(f.session);
        uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store),
            before = reserved(&f);
        wl_plan_relation_t *rp = join_body(&f, "result");
        CHECK(rp->ops[1].op == WL_PLAN_OP_JOIN &&
            rp->ops[2].op == WL_PLAN_OP_JOIN);
        eval_stack_t stack; eval_stack_init(&stack);
        int rc = wl_columnar_join_cold_left(s, rp, &stack);
        if (large) CHECK(rc == ENOSPC && stack.top == 0);
        else {
            eval_entry_t e;
            CHECK(rc == 0 && eval_stack_pop_relation(&stack,
                &e) == 0 && e.rel->nrows == 16);
            CHECK(eval_stack_dispose_entry(&stack, &e) == 0);
        }
        CHECK(reserved(&f) == before);
        unsigned count = 0;
        rc = wl_session_snapshot(f.session, count_only, &count);
        CHECK(rc == (large ? ENOSPC : 0) && count == 0);
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
        destroy(&f);
    }
}

static const char *reference_projection_query =
    ".decl a(k:int64,s:symbol,u:symbol)\n.decl b(s:symbol,k:int64)\n"
    ".decl result(s:symbol,k:int64,t:symbol)\n.decl other(s:symbol,k:int64)\n"
    "result(s,k,s) :- a(k,s,u), k >= 3.\nother(s,k) :- b(s,k).\n";
static const char *reference_values[] = {"", "alpha", "한글"};
static fixture_t reference_projection_fixture(uint64_t allowance, unsigned rows)
{
    fixture_t f = fixture(reference_projection_query, allowance, 1024*1024);
    wl_col_session_t *s = COL_SESSION(f.session);
    int64_t ids[3];
    for (unsigned i = 0; i < 3; i++) {
        ids[i] = wl_intern_put(s->intern, reference_values[i]);
        CHECK(ids[i] >= 0);
    }
    for (unsigned i = 0; i < (rows ? rows : 1); i++) {
        int64_t a[] = {(int64_t)i, ids[i%3], ids[(i+1)%3]},
            b[] = {ids[i%3], (int64_t)i};
        CHECK(wl_session_insert(f.session, "a", a, 1, 3) == 0);
        CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
        if (!rows) {
            CHECK(wl_session_remove(f.session, "a", a, 1, 3) == 0);
            CHECK(wl_session_remove(f.session, "b", b, 1, 2) == 0);
        }
    }
    return f;
}
typedef struct { wl_intern_t *intern; unsigned bits[2]; } reference_events_t;
static void reference_observe(const char *name, const int64_t *row,
    uint32_t cols, void *context)
{
    reference_events_t *e = context;
    unsigned idx = !strcmp(name, "result") ? 0u : 1u;
    CHECK(idx == 0 || !strcmp(name, "other"));
    CHECK(cols == (idx ? 2u : 3u) && row[1] >= (idx ? 0 : 3) && row[1] < 9);
    CHECK(!strcmp(wl_intern_reverse(e->intern, row[0]),
        reference_values[(unsigned)row[1]%3]));
    if (!idx) CHECK(row[2] == row[0]);
    unsigned bit = 1u << (unsigned)row[1]; CHECK(!(e->bits[idx]&bit));
    e->bits[idx] |= bit;
}
static void reference_projections(void)
{
    for (unsigned empty = 0; empty < 2; empty++)
        for (unsigned order = 0; order < 2; order++)
            for (unsigned count = 0; count <= 2; count++) {
                fixture_t f = reference_projection_fixture(4*1024*1024,
                        empty ? 0 : 9);
                wl_col_session_t *s = COL_SESSION(f.session);
                const char *names[] = {"a", "b"};
                for (unsigned i = 0; i < count;
                    i++) CHECK(wl_columnar_session_seal_input(f.session,
                        names[order ? 1-i : i], NULL, NULL) == OK);
                if (order) CHECK(wl_session_step(f.session) == 0);
                for (unsigned repeat = 0; repeat < 2; repeat++) {
                    reference_events_t e = {.intern = s->intern};
                    CHECK(wl_session_snapshot(f.session, reference_observe,
                        &e) == 0);
                    CHECK(e.bits[0] == (empty ? 0u : 504u) &&
                        e.bits[1] == (empty ? 0u : 511u));
                }
                col_rel_t *r = session_find_rel(s, "result");
                if (!empty) CHECK(r && r->column_types &&
                        r->column_types[0] == WIRELOG_TYPE_STRING &&
                        r->column_types[2] == WIRELOG_TYPE_STRING);
                destroy(&f);
            }
}
static void reference_projection_failures(void)
{
    for (unsigned fault = 0; fault < 6; fault++) {
        fixture_t f = reference_projection_fixture(4*1024*1024, 9);
        wl_col_session_t *s = COL_SESSION(f.session);
        CHECK(wl_columnar_session_seal_input(f.session, "b", NULL, NULL) == OK);
        col_rel_t *source = session_find_rel(s, "a");
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        int64_t saved = source->columns[2][8];
        wl_intern_t *intern = s->intern, *foreign = NULL;
        if (fault == 0) source->columns[2][8] = INT64_MAX; /* Unprojected invalid ID. */
        if (fault == 1) s->intern = NULL;
        if (fault == 2) {
            foreign = wl_intern_create(); CHECK(foreign); s->intern = foreign;
        }
        if (fault == 3) allocation_fault = 1;
        if (fault == 4) write_fault = 1;
        cancellation_t cancellation = {.stop = 3};
        wl_columnar_spill_status_t rc =
            wl_columnar_session_seal_input(f.session, "a",
                fault == 5 ? cancel : NULL, &cancellation);
        CHECK(rc != OK && session_find_rel(s,
            "a") == source && !wl_columnar_session_cold_find(s, "a"));
        CHECK(reserved(&f) == before &&
            wl_columnar_spill_store_bytes(s->spill_store) == disk);
        s->intern = intern; if (foreign) wl_intern_free(foreign);
        source->columns[2][8] = saved; allocation_fault = write_fault = 0;
        CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
        const wl_plan_relation_t *rp = join_body(&f, "result");
        eval_stack_t stack; eval_stack_init(&stack); before = reserved(&f);
        wl_columnar_ops_test_map_fail_output_alloc = true;
        CHECK(wl_columnar_ops_cold_projection(rp, &stack, s) == ENOMEM);
        wl_columnar_ops_test_map_fail_output_alloc = false;
        CHECK(!stack.top && reserved(&f) == before);
        s->intern = NULL;
        CHECK(wl_columnar_ops_cold_projection(rp, &stack,
            s) == ENOTSUP && !stack.top);
        s->intern = intern;
        read_fault = 9; unsigned events = 0;
        CHECK(wl_session_snapshot(f.session, count_only,
            &events) == EIO && events == 0);
        read_fault = 0; reference_events_t e = {.intern = intern};
        CHECK(wl_session_snapshot(f.session, reference_observe,
            &e) == 0 && e.bits[0] == 504 && e.bits[1] == 511);
        destroy(&f);
    }
}
static void reference_projection_bytecode(void)
{
    fixture_t f = reference_projection_fixture(4*1024*1024, 9);
    wl_col_session_t *s = COL_SESSION(f.session);
    wl_plan_relation_t *rp = join_body(&f, "result");
    wl_plan_op_t *map = (wl_plan_op_t *)&rp->ops[rp->op_count-2];
    CHECK(map->map_expr_count && map->map_exprs[0].data[0] == WL_PLAN_EXPR_VAR);
    uint8_t *data = (uint8_t *)map->map_exprs[0].data;
    data[0] = WL_PLAN_EXPR_VAR_FLOAT;
    uint64_t before = reserved(&f);
    CHECK(wl_columnar_session_seal_input(f.session, "a", NULL,
        NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(reserved(&f) == before && session_find_rel(s, "a"));
    data[0] = WL_PLAN_EXPR_VAR;
    uint8_t *numeric = (uint8_t *)map->map_exprs[1].data;
    numeric[0] = WL_PLAN_EXPR_VAR_STRING;
    CHECK(wl_columnar_session_seal_input(f.session, "a", NULL,
        NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(reserved(&f) == before && session_find_rel(s, "a"));
    numeric[0] = WL_PLAN_EXPR_VAR;
    data[0] = WL_PLAN_EXPR_VAR_STRING;
    CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
    destroy(&f);
}

static void reference_wide_projection(fixture_t *f, wl_plan_relation_t *wide,
    wl_plan_op_t ops[4], uint32_t indices[32])
{
    *wide = *join_body(f, "result"); CHECK(wide->op_count == 4);
    memcpy(ops, wide->ops, 4*sizeof(*ops)); wide->ops = ops;
    for (unsigned i = 0; i < 32; i++) indices[i] = 1;
    ops[2].project_indices = indices; ops[2].project_count = 32;
    ops[2].map_expr_count = 0; ops[2].map_exprs = NULL;
}
static void reference_projection_memory(void)
{
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    budget_peak = 0;
    fixture_t calibration = reference_projection_fixture(4*1024*1024, 1024);
    CHECK(wl_columnar_session_seal_input(calibration.session, "a", NULL,
        NULL) == OK);
    wl_plan_relation_t wide; wl_plan_op_t ops[4]; uint32_t indices[32];
    reference_wide_projection(&calibration, &wide, ops, indices);
    eval_stack_t stack; eval_stack_init(&stack);
    CHECK(wl_columnar_ops_cold_projection(&wide, &stack,
        COL_SESSION(calibration.session)) == 0);
    uint64_t allowance = budget_peak;
    eval_entry_t entry = eval_stack_pop(&stack);
    CHECK(entry.rel->nrows == 1021 &&
        entry.rel->column_types[0] == WIRELOG_TYPE_STRING);
    CHECK(eval_stack_dispose_entry(&stack, &entry) == 0); destroy(&calibration);
    for (unsigned retain = 0; retain < 2; retain++) {
        budget_peak = 0;
        fixture_t f = reference_projection_fixture(allowance, 1024);
        wl_col_session_t *s = COL_SESSION(f.session);
        wl_columnar_session_test_retain_cold_source = retain != 0;
        CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
        wl_columnar_session_test_retain_cold_source = false;
        reference_wide_projection(&f, &wide, ops, indices);
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        int rc = wl_columnar_ops_cold_projection(&wide, &stack, s);
        CHECK(budget_peak <= allowance);
        if (retain) CHECK(rc == ENOSPC && !stack.top &&
                wl_session_budget_denied(f.session));
        else {
            CHECK(rc == 0); entry = eval_stack_pop(&stack);
            CHECK(entry.rel->nrows == 1021 &&
                entry.rel->column_types[0] == WIRELOG_TYPE_STRING);
            CHECK(eval_stack_dispose_entry(&stack, &entry) == 0);
        }
        CHECK(reserved(&f) == before &&
            wl_columnar_spill_store_bytes(s->spill_store) == disk);
        destroy(&f);
    }
    wl_columnar_memory_governor_test_admitted = NULL;
    /* Intern storage remains resident. Real distinct strings eventually exhaust
     * a fixed allowance even though the row source has already been sealed. */
    fixture_t f = reference_projection_fixture(256*1024, 9);
    CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
    wl_col_session_t *s = COL_SESSION(f.session);
    uint64_t before = reserved(&f),
        disk = wl_columnar_spill_store_bytes(s->spill_store);
    bool denied = false;
    for (unsigned i = 0; i < 16384; i++) {
        char value[256]; memset(value, 'x', sizeof(value));
        int length = snprintf(value, 32, "distinct-%u-", i); CHECK(length > 0);
        value[length] = 'x'; value[sizeof(value)-1] = '\0';
        int64_t id = -1;
        int rc = wl_intern_put_checked(s->intern, value, &id);
        if (rc == WL_INTERN_ERR_MEMORY_BUDGET) {
            denied = true; break;
        }
        CHECK(rc == 0 && id >= 0);
    }
    CHECK(denied && reserved(&f) > before &&
        wl_columnar_spill_store_bytes(s->spill_store) == disk);
    destroy(&f);
}

typedef struct {
    wl_col_session_t *session;
    unsigned count;
    bool seen[144];
    bool numeric;
} reference_join_events_t;
static void reference_join_observe(const char *name, const int64_t *row,
    uint32_t width, void *context)
{
    reference_join_events_t *e = context;
    CHECK(!strcmp(name, "result") && width == 5);
    CHECK(row[1] >= 0 && row[1] < 12 && row[3] >= 0 && row[3] < 12);
    unsigned x = (unsigned)row[1], y = (unsigned)row[3];
    CHECK(x % 3 == y % 3);
    unsigned pair = x*12+y;
    CHECK(!e->seen[pair]); e->seen[pair] = true; e->count++;
    if (e->numeric) CHECK(row[0] == (int64_t)(x%3));
    else CHECK(!strcmp(wl_intern_reverse(e->session->intern, row[0]),
            reference_values[x%3]));
    CHECK(!strcmp(wl_intern_reverse(e->session->intern, row[2]),
        reference_values[(x+1)%3]));
    CHECK(!strcmp(wl_intern_reverse(e->session->intern, row[4]),
        reference_values[(y+2)%3]));
}
static void reference_join_directions(void)
{
    for (unsigned numeric = 0; numeric < 2; numeric++)
        for (unsigned shape = 0; shape < 5; shape++)
            for (unsigned direction = 0; direction < 5; direction++) {
                char text[512];
                snprintf(text, sizeof(text),
                    ".decl a(k:%s,x:int64,p:symbol)\n"
                    ".decl b(k:%s,y:int64,q:symbol)\n"
                    ".decl result(k:%s,x:int64,p:symbol,y:int64,q:symbol)\n"
                    "result(k,x,p,y+0,q) :- a(k,x,p),b(k,y,q).\n",
                    numeric ? "int64" : "symbol", numeric ? "int64" : "symbol",
                    numeric ? "int64" : "symbol");
                fixture_t f = fixture(text, 4*1024*1024, 1024*1024);
                wl_col_session_t *s = COL_SESSION(f.session);
                int64_t ids[3];
                for (unsigned i = 0; i < 3; i++)
                    ids[i] = wl_intern_put(s->intern, reference_values[i]);
                int64_t absent = numeric ? 99 : wl_intern_put(s->intern,
                        "absent");
                wl_plan_relation_t *rp = join_body(&f, "result");
                CHECK(rp->ops[0].op == WL_PLAN_OP_VARIABLE &&
                    !strcmp(rp->ops[0].relation_name, "a") &&
                    rp->ops[1].op == WL_PLAN_OP_JOIN);
                for (unsigned i = 0; i < 12; i++) {
                    int64_t a[] = {numeric ? (int64_t)(i%3) : ids[i%3], i,
                                   ids[(i+1)%3]};
                    int64_t b[] = {shape == 1 ? absent : a[0], i, ids[(i+2)%3]};
                    if (shape != 2 && shape != 4)
                        CHECK(wl_session_insert(f.session, "a", a, 1, 3) == 0);
                    if (shape != 3 && shape != 4)
                        CHECK(wl_session_insert(f.session, "b", b, 1, 3) == 0);
                }
                seal_direction(&f, direction);
                uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
                if (direction && !shape) {
                    unsigned count = 0; read_fault = 7;
                    CHECK(wl_session_snapshot(f.session, count_only,
                        &count) == EIO && !count);
                    read_fault = 0;
                    CHECK(wl_columnar_spill_store_bytes(s->spill_store) ==
                        disk);
                }
                for (unsigned repeat = 0; repeat < 2; repeat++) {
                    reference_join_events_t e = {.session = s,
                                                 .numeric = numeric != 0};
                    CHECK(wl_session_snapshot(f.session, reference_join_observe,
                        &e) == 0);
                    CHECK(e.count == (shape ? 0 : 48));
                    CHECK(wl_columnar_spill_store_bytes(s->spill_store) ==
                        disk);
                    if (!shape) {
                        col_rel_t *out = session_find_rel(s, "result");
                        CHECK(out && out->column_types &&
                            out->column_types[2] == WIRELOG_TYPE_STRING &&
                            out->column_types[4] == WIRELOG_TYPE_STRING);
                    }
                    CHECK(wl_session_step(f.session) == 0);
                }
                destroy(&f);
            }
}

static const char *reference_join_budget_query =
    ".decl a(k:int64,v:int64,s:symbol)\n.decl b(k:int64,v:int64,t:symbol)\n"
    ".decl result(k:int64,x:int64,y:int64,s:symbol)\n"
    "result(k,x,y+0,s) :- a(k,x,s),b(k,y,t).\n";
static fixture_t reference_join_budget_fixture(uint64_t allowance, bool both)
{
    fixture_t f = fixture(reference_join_budget_query, allowance, 8*1024*1024);
    wl_col_session_t *s = COL_SESSION(f.session);
    CHECK(wl_intern_put(s->intern, "shared") == 0);
    s->spill_join_batch_bytes = 1024; s->spill_join_index_bytes = 64*1024;
    for (int64_t i = 0; i < 4096; i++) {
        int64_t a[] = {i < 128 ? 0 : 10000+i, i, 0};
        CHECK(wl_session_insert(f.session, "a", a, 1, 3) == 0);
    }
    for (int64_t i = 0; i < (both ? 4096 : 128); i++) {
        int64_t b[] = {i < 128 ? 0 : 20000+i, 100+i, 0};
        CHECK(wl_session_insert(f.session, "b", b, 1, 3) == 0);
    }
    return f;
}
typedef struct { unsigned count; uint64_t seen[256];
} reference_reclaimed_events_t;
static void reference_reclaimed_observe(const char *name, const int64_t *row,
    uint32_t width, void *context)
{
    reference_reclaimed_events_t *e = context;
    CHECK(!strcmp(name,
        "result") && width == 4 && row[3] == 0 && row[0] == 0 && row[1] >= 0 &&
        row[1] < 128 &&
        row[2] >= 100 && row[2] < 228);
    unsigned pos = (unsigned)(row[1]*128 + row[2]-100);
    uint64_t bit = UINT64_C(1) << (pos%64);
    CHECK(!(e->seen[pos/64]&bit)); e->seen[pos/64] |= bit; e->count++;
}
static void reference_join_reclamation(void)
{
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    for (unsigned both = 0; both < 2; both++) {
        budget_peak = 0;
        fixture_t calibration = reference_join_budget_fixture(8*1024*1024,
                both != 0);
        seal_direction(&calibration, both ? 3 : 1);
        uint64_t preparation = budget_peak;
        reference_reclaimed_events_t e = {0};
        CHECK(wl_session_snapshot(calibration.session,
            reference_reclaimed_observe,
            &e) == 0 && e.count == 16384);
        uint64_t allowance = budget_peak;
        CHECK(allowance > preparation);
        destroy(&calibration);
        for (unsigned retain = 0; retain < 2; retain++) {
            budget_peak = 0;
            fixture_t f = reference_join_budget_fixture(allowance, both != 0);
            wl_col_session_t *s = COL_SESSION(f.session);
            uint64_t before = reserved(&f);
            wl_columnar_session_test_retain_cold_source = retain != 0;
            seal_direction(&f, both ? 3 : 1);
            wl_columnar_session_test_retain_cold_source = false;
            CHECK(retain ? reserved(&f) > before : reserved(&f) < before);
            unsigned owners = 0;
            for (wl_columnar_session_cold_relation_t *c = s->cold_relation; c;
                c = c->next) {
                CHECK((c->retained_source != NULL) == (retain != 0)); owners++;
            }
            CHECK(owners == (both ? 2u : 1u));
            uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
            e = (reference_reclaimed_events_t){0};
            int rc = wl_session_snapshot(f.session, reference_reclaimed_observe,
                    &e);
            CHECK(budget_peak <= allowance);
            if (retain) CHECK(rc == ENOSPC &&
                    wl_session_budget_denied(f.session) && e.count == 0);
            else CHECK(rc == 0 && e.count == 16384);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
            destroy(&f);
            CHECK(!wl_columnar_session_test_has_retained_cold_source(NULL));
        }
    }
    wl_columnar_memory_governor_test_admitted = NULL;
}
static void reference_join_bounded_scratch(void)
{
    uint64_t peaks[2] = {0};
    for (unsigned large = 0; large < 2; large++) {
        fixture_t f = fixture(reference_join_budget_query, 4*1024*1024,
                1024*1024);
        CHECK(wl_intern_put(COL_SESSION(f.session)->intern, "shared") == 0);
        for (int64_t i = 0; i < (large ? 512 : 64); i++) {
            int64_t a[] = {i, 10+i, 0}, b[] = {10000+i, 100+i, 0};
            CHECK(wl_session_insert(f.session, "a", a, 1, 3) == 0);
            CHECK(wl_session_insert(f.session, "b", b, 1, 3) == 0);
        }
        seal_direction(&f, 3);
        wl_col_session_t *s = COL_SESSION(f.session);
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        budget_peak = before; constructor_peak = 0;
        wl_columnar_memory_governor_test_admitted = budget_admitted;
        wl_columnar_join_test_spill_producer_result = constructor_result;
        eval_stack_t stack; eval_stack_init(&stack);
        CHECK(wl_columnar_join_cold_left(s, join_body(&f, "result"),
            &stack) == 0);
        peaks[large] = constructor_peak-before;
        CHECK(constructor_peak > before && budget_peak == constructor_peak);
        eval_entry_t e;
        CHECK(eval_stack_pop_relation(&stack, &e) == 0 && e.rel->nrows == 0);
        CHECK(eval_stack_dispose_entry(&stack,
            &e) == 0 && reserved(&f) == before);
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
        wl_columnar_memory_governor_test_admitted = NULL;
        wl_columnar_join_test_spill_producer_result = NULL;
        destroy(&f);
    }
    CHECK(peaks[0] == peaks[1]);
}

static void inline_initialized_observe(const char *name, const int64_t *row,
    uint32_t width, void *context)
{
    unsigned *bits = context;
    unsigned which = !strcmp(name, "out") ? 0 : !strcmp(name,
            "filtered") ? 1 : 2;
    CHECK(which != 2 || !strcmp(name, "joined"));
    CHECK(width == 3 && row[0] == 7 && row[1] == 11 && row[2] == 13);
    CHECK(!(*bits & (1u<<which))); *bits |= 1u<<which;
}
static void inline_initialized_sources(void)
{
    const char *text =
        ".decl a(id:int64,p:pair/2 inline)\n.decl b(k:int64,v:int64)\n"
        ".decl out(id:int64,x:int64,y:int64)\n"
        ".decl filtered(id:int64,x:int64,y:int64)\n"
        ".decl joined(id:int64,x:int64,y:int64)\n"
        "out(i,x,y) :- a(i,pair(x,y)).\n"
        "filtered(i,x,y) :- a(i,pair(x,y)), x > 0.\n"
        "joined(i,x,y+0) :- a(i,pair(x,y)),b(x,z).\n";
    fixture_t f = fixture(text, 4*1024*1024, 1024*1024);
    wl_col_session_t *s = COL_SESSION(f.session);
    col_rel_t *a = session_find_rel(s, "a");
    CHECK(a && !a->nrows && a->ncols == 3 && a->column_types &&
        a->compound_kind == WIRELOG_COMPOUND_KIND_INLINE &&
        a->compound_count == 1 && a->compound_arity_len == 2 &&
        a->compound_arity_map[0] == 1 && a->compound_arity_map[1] == 2);
    int64_t row[] = {7, 11, 13}, b[] = {11, 17};
    CHECK(wl_session_insert(f.session, "a", row, 1, 3) == 0);
    CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
    for (unsigned repeat = 0; repeat < 2; repeat++) {
        unsigned bits = 0;
        CHECK(wl_session_snapshot(f.session, inline_initialized_observe,
            &bits) == 0 && bits == 7);
        const char *names[] = {"out", "filtered", "joined"};
        for (unsigned i = 0; i < 3; i++) {
            col_rel_t *out = session_find_rel(s, names[i]);
            CHECK(out && out->ncols == 3 &&
                out->compound_kind == WIRELOG_COMPOUND_KIND_NONE &&
                !out->compound_count && !out->compound_arity_map);
        }
        CHECK(wl_session_step(f.session) == 0);
    }
    destroy(&f);
}

static void inline_initialization_budget(void)
{
    const char *text = ".decl a(i:int64,p:pair/2 inline)\n"
        ".decl out(i:int64)\nout(i) :- a(i,pair(x,y)).\n";
    uint64_t floor = 0;
    for (unsigned pass = 0; pass < 3; pass++) {
        bool fits = pass != 1;
        fixture_t f = {0}; wirelog_error_t error;
        f.program = wirelog_parse_string(text, &error); CHECK(f.program);
        CHECK(wl_plan_from_program(f.program, &f.plan) == 0);
        uint64_t limit = pass == 0 ? 4*1024*1024 : floor - (fits ? 0 : 1);
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = limit, .usable_bytes = limit,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
            .status = WL_COLUMNAR_MEMORY_OK
        };
        f.governor = wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(f.governor);
        wl_session_options_t options; wl_session_options_init(&options);
        options.memory_governor = f.governor;
        options.spill_directory = test_tmpdir();
        options.spill_quota_bytes = 1024*1024; options.spill_max_row_bytes = 64;
        options.spill_join_batch_bytes = 96;
        options.spill_join_index_bytes =
            sizeof(wl_columnar_memory_reservation_t)+16;
        budget_peak = 0;
        wl_columnar_memory_governor_test_admitted = budget_admitted;
        int rc = wl_session_create_with_options(wl_backend_columnar(), f.plan,
                1, &options, &f.session);
        wl_columnar_memory_governor_test_admitted = NULL;
        if (!pass) {
            floor = budget_peak; CHECK(floor > 1);
        }
        if (fits) {
            CHECK(rc == 0 && f.session);
            col_rel_t *a = session_find_rel(COL_SESSION(f.session), "a");
            CHECK(a && a->compound_arity_len == 2 && a->ncols == 3);
        } else CHECK(rc == WL_ERR_MEMORY_BUDGET && !f.session);
        destroy(&f);
    }
    const char *patterns[] = {"pair(nested(s),n)", "whole", "_"};
    for (unsigned i = 0; i < 3; i++) {
        char query_text[256];
        snprintf(query_text, sizeof(query_text),
            ".decl a(i:int64,p:pair/2 inline)\n"
            ".decl out(i:int64)\nout(i) :- a(i,%s).\n", patterns[i]);
        fixture_t f = fixture(query_text, 4*1024*1024, 1024*1024);
        col_rel_t *a = session_find_rel(COL_SESSION(f.session), "a");
        CHECK(a && a->compound_kind == WIRELOG_COMPOUND_KIND_INLINE &&
            a->ncols == 3);
        destroy(&f);
    }
}

static void inline_join_directions(void)
{
    for (unsigned numeric = 0; numeric < 2; numeric++)
        for (unsigned shape = 0; shape < 5; shape++)
            for (unsigned direction = 0; direction < 5; direction++) {
                char text[512];
                snprintf(text, sizeof(text),
                    ".decl a(p:triple(%s,int64,symbol) inline)\n"
                    ".decl b(p:triple(%s,int64,symbol) inline)\n"
                    ".decl result(k:%s,x:int64,p:symbol,y:int64,q:symbol)\n"
                    "result(k,x,p,y+0,q) :- a(triple(k,x,p)),b(triple(k,y,q)).\n",
                    numeric ? "int64" : "symbol", numeric ? "int64" : "symbol",
                    numeric ? "int64" : "symbol");
                fixture_t f = fixture(text, 4*1024*1024, 1024*1024);
                wl_col_session_t *s = COL_SESSION(f.session);
                int64_t ids[3];
                for (unsigned i = 0; i < 3; i++)
                    ids[i] = wl_intern_put(s->intern, reference_values[i]);
                int64_t absent = numeric ? 99 : wl_intern_put(s->intern,
                        "absent");
                wl_plan_relation_t *rp = join_body(&f, "result");
                CHECK(rp->ops[0].op == WL_PLAN_OP_VARIABLE &&
                    !strcmp(rp->ops[0].relation_name, "a") &&
                    rp->ops[1].op == WL_PLAN_OP_JOIN);
                for (unsigned i = 0; i < 12; i++) {
                    int64_t a[] = {numeric ? (int64_t)(i%3) : ids[i%3], i,
                                   ids[(i+1)%3]};
                    int64_t b[] = {shape == 1 ? absent : a[0], i, ids[(i+2)%3]};
                    if (shape != 2 && shape != 4)
                        CHECK(wl_session_insert(f.session, "a", a, 1, 3) == 0);
                    if (shape != 3 && shape != 4)
                        CHECK(wl_session_insert(f.session, "b", b, 1, 3) == 0);
                }
                seal_direction(&f, direction);
                uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
                if (direction && !shape) {
                    unsigned count = 0; read_fault = 7;
                    CHECK(wl_session_snapshot(f.session, count_only,
                        &count) == EIO && !count);
                    read_fault = 0;
                    CHECK(wl_columnar_spill_store_bytes(s->spill_store) ==
                        disk);
                }
                for (unsigned repeat = 0; repeat < 2; repeat++) {
                    reference_join_events_t e = {.session = s,
                                                 .numeric = numeric != 0};
                    CHECK(wl_session_snapshot(f.session, reference_join_observe,
                        &e) == 0);
                    CHECK(e.count == (shape ? 0 : 48));
                    CHECK(wl_columnar_spill_store_bytes(s->spill_store) ==
                        disk);
                    if (!shape) {
                        col_rel_t *out = session_find_rel(s, "result");
                        CHECK(out && out->column_types &&
                            out->column_types[2] == WIRELOG_TYPE_STRING &&
                            out->column_types[4] == WIRELOG_TYPE_STRING);
                    }
                    CHECK(wl_session_step(f.session) == 0);
                }
                destroy(&f);
            }
}

static const char *inline_projection_query =
    ".decl a(k:int64,p:pair(symbol,symbol) inline)\n.decl b(p:pair(symbol,int64) inline)\n"
    ".decl result(s:symbol,k:int64,t:symbol)\n.decl other(s:symbol,k:int64)\n"
    "result(s,k,s) :- a(k,pair(s,u)), k >= 3.\nother(s,k) :- b(pair(s,k)).\n";
static fixture_t inline_projection_fixture(uint64_t allowance, unsigned rows)
{
    fixture_t f = fixture(inline_projection_query, allowance, 1024*1024);
    wl_col_session_t *s = COL_SESSION(f.session);
    int64_t ids[3];
    for (unsigned i = 0; i < 3; i++) {
        ids[i] = wl_intern_put(s->intern, reference_values[i]);
        CHECK(ids[i] >= 0);
    }
    for (unsigned i = 0; i < (rows ? rows : 1); i++) {
        int64_t a[] = {(int64_t)i, ids[i%3], ids[(i+1)%3]},
            b[] = {ids[i%3], (int64_t)i};
        CHECK(wl_session_insert(f.session, "a", a, 1, 3) == 0);
        CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
        if (!rows) {
            CHECK(wl_session_remove(f.session, "a", a, 1, 3) == 0);
            CHECK(wl_session_remove(f.session, "b", b, 1, 2) == 0);
        }
    }
    return f;
}
static void inline_projection_failures(void)
{
    for (unsigned fault = 0; fault < 6; fault++) {
        fixture_t f = inline_projection_fixture(4*1024*1024, 9);
        wl_col_session_t *s = COL_SESSION(f.session);
        CHECK(wl_columnar_session_seal_input(f.session, "b", NULL, NULL) == OK);
        col_rel_t *source = session_find_rel(s, "a");
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        int64_t saved = source->columns[2][8];
        wl_intern_t *intern = s->intern, *foreign = NULL;
        if (fault == 0) source->columns[2][8] = INT64_MAX; /* Unprojected invalid ID. */
        if (fault == 1) s->intern = NULL;
        if (fault == 2) {
            foreign = wl_intern_create(); CHECK(foreign); s->intern = foreign;
        }
        if (fault == 3) allocation_fault = 1;
        if (fault == 4) write_fault = 1;
        cancellation_t cancellation = {.stop = 3};
        wl_columnar_spill_status_t rc =
            wl_columnar_session_seal_input(f.session, "a",
                fault == 5 ? cancel : NULL, &cancellation);
        CHECK(rc != OK && session_find_rel(s,
            "a") == source && !wl_columnar_session_cold_find(s, "a"));
        CHECK(reserved(&f) == before &&
            wl_columnar_spill_store_bytes(s->spill_store) == disk);
        s->intern = intern; if (foreign) wl_intern_free(foreign);
        source->columns[2][8] = saved; allocation_fault = write_fault = 0;
        CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
        const wl_plan_relation_t *rp = join_body(&f, "result");
        eval_stack_t stack; eval_stack_init(&stack); before = reserved(&f);
        wl_columnar_ops_test_map_fail_output_alloc = true;
        CHECK(wl_columnar_ops_cold_projection(rp, &stack, s) == ENOMEM);
        wl_columnar_ops_test_map_fail_output_alloc = false;
        CHECK(!stack.top && reserved(&f) == before);
        s->intern = NULL;
        CHECK(wl_columnar_ops_cold_projection(rp, &stack,
            s) == ENOTSUP && !stack.top);
        s->intern = intern;
        read_fault = 9; unsigned events = 0;
        CHECK(wl_session_snapshot(f.session, count_only,
            &events) == EIO && events == 0);
        read_fault = 0; reference_events_t e = {.intern = intern};
        CHECK(wl_session_snapshot(f.session, reference_observe,
            &e) == 0 && e.bits[0] == 504 && e.bits[1] == 511);
        destroy(&f);
    }
}
static void inline_projection_memory(void)
{
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    budget_peak = 0;
    fixture_t calibration = inline_projection_fixture(4*1024*1024, 1024);
    CHECK(wl_columnar_session_seal_input(calibration.session, "a", NULL,
        NULL) == OK);
    wl_plan_relation_t wide; wl_plan_op_t ops[4]; uint32_t indices[32];
    reference_wide_projection(&calibration, &wide, ops, indices);
    eval_stack_t stack; eval_stack_init(&stack);
    CHECK(wl_columnar_ops_cold_projection(&wide, &stack,
        COL_SESSION(calibration.session)) == 0);
    uint64_t allowance = budget_peak;
    eval_entry_t entry = eval_stack_pop(&stack);
    CHECK(entry.rel->nrows == 1021 &&
        entry.rel->column_types[0] == WIRELOG_TYPE_STRING);
    CHECK(eval_stack_dispose_entry(&stack, &entry) == 0); destroy(&calibration);
    for (unsigned retain = 0; retain < 2; retain++) {
        budget_peak = 0;
        fixture_t f = inline_projection_fixture(allowance, 1024);
        wl_col_session_t *s = COL_SESSION(f.session);
        wl_columnar_session_test_retain_cold_source = retain != 0;
        CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
        wl_columnar_session_test_retain_cold_source = false;
        reference_wide_projection(&f, &wide, ops, indices);
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        int rc = wl_columnar_ops_cold_projection(&wide, &stack, s);
        CHECK(budget_peak <= allowance);
        if (retain) CHECK(rc == ENOSPC && !stack.top &&
                wl_session_budget_denied(f.session));
        else {
            CHECK(rc == 0); entry = eval_stack_pop(&stack);
            CHECK(entry.rel->nrows == 1021 &&
                entry.rel->column_types[0] == WIRELOG_TYPE_STRING);
            CHECK(eval_stack_dispose_entry(&stack, &entry) == 0);
        }
        CHECK(reserved(&f) == before &&
            wl_columnar_spill_store_bytes(s->spill_store) == disk);
        destroy(&f);
    }
    wl_columnar_memory_governor_test_admitted = NULL;
    /* Intern storage remains resident. Real distinct strings eventually exhaust
     * a fixed allowance even though the row source has already been sealed. */
    fixture_t f = inline_projection_fixture(256*1024, 9);
    CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
    wl_col_session_t *s = COL_SESSION(f.session);
    uint64_t before = reserved(&f),
        disk = wl_columnar_spill_store_bytes(s->spill_store);
    bool denied = false;
    for (unsigned i = 0; i < 16384; i++) {
        char value[256]; memset(value, 'x', sizeof(value));
        int length = snprintf(value, 32, "distinct-%u-", i); CHECK(length > 0);
        value[length] = 'x'; value[sizeof(value)-1] = '\0';
        int64_t id = -1;
        int rc = wl_intern_put_checked(s->intern, value, &id);
        if (rc == WL_INTERN_ERR_MEMORY_BUDGET) {
            denied = true; break;
        }
        CHECK(rc == 0 && id >= 0);
    }
    CHECK(denied && reserved(&f) > before &&
        wl_columnar_spill_store_bytes(s->spill_store) == disk);
    destroy(&f);
}

static const char *inline_join_budget_query =
    ".decl a(k:int64,v:shared/1 inline,s:symbol)\n.decl b(k:int64,v:shared/1 inline,t:symbol)\n"
    ".decl result(k:int64,x:int64,y:int64,s:symbol)\n"
    "result(k,x,y+0,s) :- a(k,shared(x),s),b(k,shared(y),t).\n";
static fixture_t inline_join_budget_fixture(uint64_t allowance, bool both)
{
    fixture_t f = fixture(inline_join_budget_query, allowance, 8*1024*1024);
    wl_col_session_t *s = COL_SESSION(f.session);
    CHECK(wl_intern_put(s->intern, "shared") == 0);
    s->spill_join_batch_bytes = 1024; s->spill_join_index_bytes = 64*1024;
    for (int64_t i = 0; i < 4096; i++) {
        int64_t a[] = {i < 128 ? 0 : 10000+i, i, 0};
        CHECK(wl_session_insert(f.session, "a", a, 1, 3) == 0);
    }
    for (int64_t i = 0; i < (both ? 4096 : 128); i++) {
        int64_t b[] = {i < 128 ? 0 : 20000+i, 100+i, 0};
        CHECK(wl_session_insert(f.session, "b", b, 1, 3) == 0);
    }
    return f;
}
typedef struct { unsigned count; uint64_t seen[256];} inline_reclaimed_events_t;
static void inline_reclaimed_observe(const char *name, const int64_t *row,
    uint32_t width, void *context)
{
    inline_reclaimed_events_t *e = context;
    CHECK(!strcmp(name,
        "result") && width == 4 && row[3] == 0 && row[0] == 0 && row[1] >= 0 &&
        row[1] < 128 &&
        row[2] >= 100 && row[2] < 228);
    unsigned pos = (unsigned)(row[1]*128 + row[2]-100);
    uint64_t bit = UINT64_C(1) << (pos%64);
    CHECK(!(e->seen[pos/64]&bit)); e->seen[pos/64] |= bit; e->count++;
}
static void inline_join_reclamation(void)
{
    wl_columnar_memory_governor_test_admitted = budget_admitted;
    for (unsigned both = 0; both < 2; both++) {
        budget_peak = 0;
        fixture_t calibration = inline_join_budget_fixture(8*1024*1024,
                both != 0);
        seal_direction(&calibration, both ? 3 : 1);
        uint64_t preparation = budget_peak;
        inline_reclaimed_events_t e = {0};
        CHECK(wl_session_snapshot(calibration.session,
            inline_reclaimed_observe,
            &e) == 0 && e.count == 16384);
        uint64_t allowance = budget_peak;
        CHECK(allowance > preparation);
        destroy(&calibration);
        for (unsigned retain = 0; retain < 2; retain++) {
            budget_peak = 0;
            fixture_t f = inline_join_budget_fixture(allowance, both != 0);
            wl_col_session_t *s = COL_SESSION(f.session);
            uint64_t before = reserved(&f);
            wl_columnar_session_test_retain_cold_source = retain != 0;
            seal_direction(&f, both ? 3 : 1);
            wl_columnar_session_test_retain_cold_source = false;
            CHECK(retain ? reserved(&f) > before : reserved(&f) < before);
            unsigned owners = 0;
            for (wl_columnar_session_cold_relation_t *c = s->cold_relation; c;
                c = c->next) {
                CHECK((c->retained_source != NULL) == (retain != 0)); owners++;
            }
            CHECK(owners == (both ? 2u : 1u));
            uint64_t disk = wl_columnar_spill_store_bytes(s->spill_store);
            e = (inline_reclaimed_events_t){0};
            int rc = wl_session_snapshot(f.session, inline_reclaimed_observe,
                    &e);
            CHECK(budget_peak <= allowance);
            if (retain) CHECK(rc == ENOSPC &&
                    wl_session_budget_denied(f.session) && e.count == 0);
            else CHECK(rc == 0 && e.count == 16384);
            CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
            destroy(&f);
            CHECK(!wl_columnar_session_test_has_retained_cold_source(NULL));
        }
    }
    wl_columnar_memory_governor_test_admitted = NULL;
}
static void inline_join_bounded_scratch(void)
{
    uint64_t peaks[2] = {0};
    for (unsigned large = 0; large < 2; large++) {
        fixture_t f = fixture(inline_join_budget_query, 4*1024*1024,
                1024*1024);
        CHECK(wl_intern_put(COL_SESSION(f.session)->intern, "shared") == 0);
        for (int64_t i = 0; i < (large ? 512 : 64); i++) {
            int64_t a[] = {i, 10+i, 0}, b[] = {10000+i, 100+i, 0};
            CHECK(wl_session_insert(f.session, "a", a, 1, 3) == 0);
            CHECK(wl_session_insert(f.session, "b", b, 1, 3) == 0);
        }
        seal_direction(&f, 3);
        wl_col_session_t *s = COL_SESSION(f.session);
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        budget_peak = before; constructor_peak = 0;
        wl_columnar_memory_governor_test_admitted = budget_admitted;
        wl_columnar_join_test_spill_producer_result = constructor_result;
        eval_stack_t stack; eval_stack_init(&stack);
        CHECK(wl_columnar_join_cold_left(s, join_body(&f, "result"),
            &stack) == 0);
        peaks[large] = constructor_peak-before;
        CHECK(constructor_peak > before && budget_peak == constructor_peak);
        eval_entry_t e;
        CHECK(eval_stack_pop_relation(&stack, &e) == 0 && e.rel->nrows == 0);
        CHECK(eval_stack_dispose_entry(&stack,
            &e) == 0 && reserved(&f) == before);
        CHECK(wl_columnar_spill_store_bytes(s->spill_store) == disk);
        wl_columnar_memory_governor_test_admitted = NULL;
        wl_columnar_join_test_spill_producer_result = NULL;
        destroy(&f);
    }
    CHECK(peaks[0] == peaks[1]);
}

static void inline_source_refusals(void)
{
    for (unsigned bad = 0; bad < 12; bad++) {
        fixture_t f = inline_projection_fixture(4*1024*1024, 9);
        wl_col_session_t *s = COL_SESSION(f.session);
        CHECK(wl_columnar_session_seal_input(f.session, "b", NULL, NULL) == OK);
        col_rel_t *source = session_find_rel(s, "a");
        const wl_plan_edb_schema_t *all = f.plan->edb_schemas;
        uint32_t edb = 0;
        while (edb < f.plan->edb_count && strcmp(f.plan->edb_relations[edb],
            "a")) edb++;
        CHECK(edb < f.plan->edb_count);
        wl_plan_edb_schema_t *schema = (wl_plan_edb_schema_t *)&all[edb];
        wl_plan_edb_schema_t saved_schema = *schema;
        wirelog_column_t *columns = (wirelog_column_t *)schema->columns;
        wirelog_column_t saved[2] = {columns[0], columns[1]};
        wirelog_column_type_t saved_type = source->column_types[1];
        uint32_t saved_count = source->compound_count,
            saved_offset = source->inline_physical_offset;
        uint32_t saved_arity = source->compound_arity_map[1];
        uint64_t before = reserved(&f),
            disk = wl_columnar_spill_store_bytes(s->spill_store);
        uint64_t generation = source->storage_generation;
        if (bad == 0) f.plan->edb_schemas = NULL;
        if (bad == 1) schema->has_unsupported_inline_cold_use = true;
        if (bad == 2) columns[1].compound_functor_id = UINT32_MAX;
        if (bad == 3) columns[1].compound_arity = 0;
        if (bad == 4) columns[1].compound_inline_col_offset++;
        if (bad == 5) source->compound_arity_map[1]++;
        if (bad == 6) source->compound_count = 0;
        if (bad == 7) source->inline_physical_offset++;
        if (bad == 8) columns[0].type = WIRELOG_TYPE_BOOL;
        if (bad == 9) source->column_types[1] = WIRELOG_TYPE_INT64;
        if (bad == 10) source->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        if (bad == 11) schema->column_count--;
        CHECK(wl_columnar_session_seal_input(f.session, "a", NULL,
            NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
        CHECK(reserved(&f) == before &&
            wl_columnar_spill_store_bytes(s->spill_store) == disk &&
            session_find_rel(s,
            "a") == source && source->storage_generation == generation &&
            !wl_columnar_session_cold_find(s,
            "a") && wl_columnar_session_cold_find(s, "b"));
        f.plan->edb_schemas = all; *schema = saved_schema;
        columns[0] = saved[0]; columns[1] = saved[1];
        source->column_types[1] = saved_type;
        source->compound_count = saved_count;
        source->inline_physical_offset = saved_offset;
        source->compound_arity_map[1] = saved_arity;
        source->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
        CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
        schema->has_unsupported_inline_cold_use = true;
        unsigned count = 0;
        CHECK(wl_session_snapshot(f.session, count_only,
            &count) == ENOTSUP && !count);
        schema->has_unsupported_inline_cold_use = false;
        reference_events_t e = {.intern = s->intern};
        CHECK(wl_session_snapshot(f.session, reference_observe, &e) == 0 &&
            e.bits[0] == 504 && e.bits[1] == 511);
        destroy(&f);
    }
    const char *patterns[] = {"pair(nested(s),n)", "whole", "_", "wrong(s,n)",
                              "pair(s)"};
    for (unsigned i = 0; i < 5; i++) {
        char text[512];
        snprintf(text, sizeof(text),
            ".decl a(i:int64,p:pair(symbol,int64) inline)\n"
            ".decl out(i:int64)\n.decl safe(i:int64)\n"
            "out(i) :- a(i,%s).\nsafe(i) :- a(i,pair(s,n)).\n", patterns[i]);
        fixture_t f = fixture(text, 4*1024*1024, 1024*1024);
        wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *r = session_find_rel(s, "a");
        uint64_t before = reserved(&f);
        CHECK(r && r->compound_kind == WIRELOG_COMPOUND_KIND_INLINE);
        CHECK(wl_columnar_session_seal_input(f.session, "a", NULL,
            NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
        CHECK(session_find_rel(s,
            "a") == r && reserved(&f) == before && !s->cold_relation);
        destroy(&f);
    }
}

static void numeric_inline_cold_source(void)
{
    const char *text =
        ".decl a(id:int64,p:pair/2 inline)\n.decl b(k:int64,v:int64)\n"
        ".decl out(id:int64,x:int64,y:int64)\n"
        ".decl filtered(id:int64,x:int64,y:int64)\n"
        ".decl joined(id:int64,x:int64,y:int64)\n"
        "out(i,x,y) :- a(i,pair(x,y)).\n"
        "filtered(i,x,y) :- a(i,pair(x,y)), x > 0.\n"
        "joined(i,x,y+0) :- a(i,pair(x,y)),b(x,z).\n";
    fixture_t f = fixture(text, 4*1024*1024, 1024*1024);
    wl_col_session_t *s = COL_SESSION(f.session);
    col_rel_t *a = session_find_rel(s, "a");
    CHECK(a && !a->nrows && a->ncols == 3 && a->column_types &&
        a->compound_kind == WIRELOG_COMPOUND_KIND_INLINE &&
        a->compound_count == 1 && a->compound_arity_len == 2 &&
        a->compound_arity_map[0] == 1 && a->compound_arity_map[1] == 2);
    int64_t row[] = {7, 11, 13}, b[] = {11, 17};
    CHECK(wl_session_insert(f.session, "a", row, 1, 3) == 0);
    CHECK(wl_session_insert(f.session, "b", b, 1, 2) == 0);
    CHECK(wl_columnar_session_seal_input(f.session, "a", NULL, NULL) == OK);
    wl_columnar_session_cold_relation_t *cold = wl_columnar_session_cold_find(s,
            "a");
    CHECK(cold && wl_columnar_batch_payload_uses_intern(cold->payload,
        s->intern));
    uint32_t logical = 0;
    const wirelog_column_t *decl =
        wl_columnar_batch_payload_inline_schema(cold->payload, &logical);
    CHECK(decl && logical == 2 && decl[1].compound_arity == 2 &&
        decl[1].compound_inline_col_offset == 1);
    wl_columnar_batch_payload_reader_t *reader;
    CHECK(wl_columnar_batch_payload_acquire(cold->payload, &reader) == OK);
    const char *functor = NULL;
    CHECK(wl_columnar_batch_payload_resolve_string(reader,
        decl[1].compound_functor_id, &functor) == OK && !strcmp(functor,
        "pair"));
    wl_columnar_batch_payload_reader_release(reader);
    for (unsigned repeat = 0; repeat < 2; repeat++) {
        unsigned bits = 0;
        CHECK(wl_session_snapshot(f.session, inline_initialized_observe,
            &bits) == 0 && bits == 7);
        const char *names[] = {"out", "filtered", "joined"};
        for (unsigned i = 0; i < 3; i++) {
            col_rel_t *out = session_find_rel(s, names[i]);
            CHECK(out && out->ncols == 3 &&
                out->compound_kind == WIRELOG_COMPOUND_KIND_NONE &&
                !out->compound_count && !out->compound_arity_map);
        }
        CHECK(wl_session_step(f.session) == 0);
    }
    destroy(&f);
}

/* Conjunction uses every predicate, including signed boundary comparisons. */
static void repeated_cold_predicates(void)
{
    const int64_t values[] = {INT64_MIN, -1, 0, 1, INT64_MAX};
    for (unsigned cmp = WL_PLAN_EXPR_CMP_EQ; cmp <= WL_PLAN_EXPR_CMP_GTE; cmp++)
        for (unsigned literal = 0; literal < 5; literal++) {
            fixture_t f = fixture(".decl b(k:int64,v:int64)\n"
                    ".decl keys(k:int64,v:int64)\nkeys(k,v) :- b(k,v).\n",
                    4*1024*1024, 1024*1024);
            for (unsigned i = 0; i < 5; i++) {
                int64_t row[] = {(int64_t)i, values[i]};
                CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
            }
            wl_plan_relation_t *rp = (wl_plan_relation_t *)projection_plan(&f);
            const wl_plan_op_t *saved_ops = rp->ops;
            uint32_t saved_count = rp->op_count;
            CHECK(saved_count == 3);
            uint8_t data[2][17] = {
                {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
                 WL_PLAN_EXPR_CONST_INT},
                {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
                 WL_PLAN_EXPR_CONST_INT}
            };
            for (unsigned i = 0; i < 2; i++) {
                int64_t value = i ? values[literal] : INT64_MIN;
                uint64_t bits; memcpy(&bits, &value, sizeof(bits));
                for (unsigned j = 0; j < 8;
                    j++) data[i][8+j] = (uint8_t)(bits >> (8*j));
                data[i][16] = (uint8_t)(i ? cmp : WL_PLAN_EXPR_CMP_GTE);
            }
            wl_plan_op_t ops[5] = {saved_ops[0], {0}, {0}, saved_ops[1],
                                   saved_ops[2]};
            for (unsigned i = 0; i < 2; i++) {
                ops[i+1].op = WL_PLAN_OP_FILTER;
                ops[i+1].filter_expr = (wl_plan_expr_buffer_t){.data = data[i],
                                                               .size = 17};
            }
            rp->ops = ops; rp->op_count = 5;
            eval_stack_t stack; eval_stack_init(&stack);
            CHECK(col_eval_relation_plan(rp, &stack,
                COL_SESSION(f.session)) == 0);
            eval_entry_t e = eval_stack_pop(&stack);
            int64_t expected[5][2]; uint32_t count = e.rel->nrows;
            for (uint32_t i = 0; i < count; i++)
                for (unsigned j = 0; j < 2;
                    j++) expected[i][j] = e.rel->columns[j][i];
            CHECK(eval_stack_dispose_entry(&stack, &e) == 0);
            uint64_t before = reserved(&f);
            data[1][0] = WL_PLAN_EXPR_VAR_FLOAT;
            CHECK(wl_columnar_session_seal_input(f.session, "b", NULL,
                NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
            CHECK(reserved(&f) == before &&
                session_find_rel(COL_SESSION(f.session), "b"));
            data[1][0] = WL_PLAN_EXPR_VAR;
            CHECK(wl_columnar_session_seal_input(f.session, "b", NULL,
                NULL) == OK);
            CHECK(!session_find_rel(COL_SESSION(f.session), "b"));
            before = reserved(&f);
            data[1][16] = WL_PLAN_EXPR_ARITH_ADD;
            CHECK(col_eval_relation_plan(rp, &stack,
                COL_SESSION(f.session)) == ENOTSUP);
            CHECK(!stack.top && reserved(&f) == before);
            data[1][16] = (uint8_t)cmp;
            if (cmp == WL_PLAN_EXPR_CMP_NEQ && literal == 4) {
                read_fault = 5;
                CHECK(wl_columnar_ops_cold_projection(rp, &stack,
                    COL_SESSION(f.session)) == EIO);
                read_fault = 0;
                CHECK(!stack.top && reserved(&f) == before);
                wl_columnar_ops_test_map_fail_output_alloc = true;
                CHECK(wl_columnar_ops_cold_projection(rp, &stack,
                    COL_SESSION(f.session)) == ENOMEM);
                wl_columnar_ops_test_map_fail_output_alloc = false;
                CHECK(!stack.top && reserved(&f) == before);
            }
            for (unsigned repeat = 0; repeat < 2; repeat++) {
                CHECK(col_eval_relation_plan(rp, &stack,
                    COL_SESSION(f.session)) == 0);
                e = eval_stack_pop(&stack);
                CHECK(e.rel->nrows == count && e.rel->ncols == 2);
                for (uint32_t i = 0; i < count; i++)
                    for (unsigned j = 0; j < 2;
                        j++) CHECK(e.rel->columns[j][i] == expected[i][j]);
                CHECK(eval_stack_dispose_entry(&stack, &e) == 0);
                CHECK(reserved(&f) == before);
            }
            rp->ops = saved_ops; rp->op_count = saved_count;
            destroy(&f);
        }
}

static void parsed_repeated_predicates(void)
{
    fixture_t f = fixture(".decl b(k:int64,v:int64)\n.decl keys(k:int64)\n"
            "keys(k) :- b(k,v), v >= 101, v < 105, v != 103.\n",
            4*1024*1024, 1024*1024);
    const wl_plan_relation_t *rp = projection_plan(&f);
    CHECK(rp->op_count == 6);
    for (unsigned i = 1; i < 4; i++) CHECK(rp->ops[i].op == WL_PLAN_OP_FILTER);
    for (int64_t i = 0; i < 6; i++) {
        int64_t row[] = {i, 100+i};
        CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
    }
    CHECK(wl_columnar_session_seal_input(f.session, "b", NULL, NULL) == OK);
    eval_stack_t stack; eval_stack_init(&stack);
    CHECK(col_eval_relation_plan(rp, &stack, COL_SESSION(f.session)) == 0);
    eval_entry_t e = eval_stack_pop(&stack);
    CHECK(e.rel->nrows == 3 && e.rel->columns[0][0] == 1 &&
        e.rel->columns[0][1] == 2 && e.rel->columns[0][2] == 4);
    CHECK(eval_stack_dispose_entry(&stack, &e) == 0); destroy(&f);
}

static void repeated_predicate_scratch(void)
{
    uint64_t temporary[2];
    for (unsigned size = 0; size < 2; size++) {
        fixture_t f = fixture(".decl b(k:int64,v:int64)\n.decl keys(k:int64)\n"
                "keys(k) :- b(k,v), v >= 0, v != 7, v < 1000.\n",
                4*1024*1024, 1024*1024);
        unsigned rows = size ? 512 : 64;
        for (unsigned i = 0; i < rows; i++) {
            int64_t row[] = {(int64_t)i, (int64_t)i};
            CHECK(wl_session_insert(f.session, "b", row, 1, 2) == 0);
        }
        CHECK(wl_columnar_session_seal_input(f.session, "b", NULL, NULL) == OK);
        CHECK(!session_find_rel(COL_SESSION(f.session), "b"));
        uint64_t before = reserved(&f); budget_peak = before;
        wl_columnar_memory_governor_test_admitted = budget_admitted;
        eval_stack_t stack; eval_stack_init(&stack);
        CHECK(wl_columnar_ops_cold_projection(projection_plan(&f), &stack,
            COL_SESSION(f.session)) == 0);
        uint64_t output = reserved(&f)-before;
        CHECK(budget_peak >= before+output);
        temporary[size] = budget_peak-before-output;
        eval_entry_t e = eval_stack_pop(&stack);
        CHECK(e.rel->nrows == rows-1);
        CHECK(eval_stack_dispose_entry(&stack,
            &e) == 0 && reserved(&f) == before);
        wl_columnar_memory_governor_test_admitted = NULL; destroy(&f);
    }
    CHECK(temporary[0] > 0 && temporary[0] == temporary[1]);
}

int main(void)
{
    /* Issue #2287: session creation reads WIRELOG_JOIN_BATCH_BYTES and
     * _STRICT, and a nonzero batch size refuses spill JOIN selection and
     * sealed cold input. The knob cases here set the session fields
     * directly, so clear the inherited environment before the first
     * session exists. */
    CHECK(!unsetenv("WIRELOG_JOIN_BATCH_BYTES"));
    CHECK(!unsetenv("WIRELOG_JOIN_BATCH_STRICT"));
    repeated_cold_predicates(); parsed_repeated_predicates();
    repeated_predicate_scratch();
    inline_source_refusals(); numeric_inline_cold_source();
    inline_projection_failures(); inline_projection_memory();
    inline_join_reclamation(); inline_join_bounded_scratch();
    inline_join_directions();
    inline_initialization_budget();
    inline_initialized_sources();
    reference_join_reclamation(); reference_join_bounded_scratch();
    reference_join_directions();
    reference_projection_memory();
    reference_projections(); reference_projection_failures();
    reference_projection_bytecode();
    cold_intermediate_floor();
    directional_constructor_floor();
    directional_reclamation(); directional_bounded_scratch();
    direction_matrix(); self_join(); chained_consumers();
    directional_failures(); cold_left_missing_right(); cold_join_preflight();
    multiple_runtime_guard(); multiple_memory_floor();
    multiple_idb_filter_refusal();
    multiple_schema_audit(); multiple_right_joins();
    multiple_bindings(); multiple_seal_failures();
    standalone_reads();
    projection_shape(); filtered_predicates(); filtered_shape();
    for (unsigned filtered = 0; filtered < 2; filtered++) {
        projection_consumers(filtered != 0); projection_failures(filtered != 0);
        projection_budget(filtered != 0);
        projection_scratch_floor(filtered != 0);
        projection_numeric_lanes(filtered != 0);
    }
    lifecycle(); rollback(); read_failure(); exclusions(); empty_and_multiple();
    typed_projection(); quota_and_alias(); loaders(); generation_boundary();
    fixed_budget(); retention_lifetime(); fixed_floors();
    concurrent_cold_readers();
    return 0;
}
