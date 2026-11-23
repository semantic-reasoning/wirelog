#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); abort(); } } while (0)
#define IDS 8
#define CACHE_SLOTS 1024
static const char *const heads[] = {"out", "mirror"};
typedef struct { bool cell[2][IDS][IDS]; } image_t;
typedef struct {
    wl_session_t *session;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    int events[2][IDS][IDS]; unsigned calls[2][IDS][IDS];
    image_t tuples;
} profile_t;
typedef struct {
    uintptr_t container, arr, heads, next;
    uint32_t count, buckets, capacity, indexed, rows, pins;
    uint64_t generation;
    col_relation_snapshot_t snapshot;
    uint32_t bucket[CACHE_SLOTS], link[CACHE_SLOTS];
} cache_t;
typedef struct {
    wirelog_program_t *program; wl_plan_t *plan; const wl_plan_op_t *op;
    profile_t p[2]; unsigned kind, route, stage, profile, fault;
    bool left[IDS], right[IDS], seed, pin_mode, armed, stopped;
    int64_t key[3], payload[2];
    unsigned builds, reuses, indexed, probes, private_rows, delta_mask;
    col_diff_arrangement_pin_t pin; uint64_t failed_attempt;
} fixture_t;
static fixture_t *active;
static unsigned head(const char *name) {
    for (unsigned i = 0; i < 2; ++i)if (!strcmp(name, heads[i])) return i;
    CHECK(false); return 0;
}
static void env_clear(const char *name) {
#ifdef _WIN32
    CHECK(!_putenv_s(name, ""));
#else
    CHECK(!unsetenv(name));
#endif
}
static void event(const char *name, const int64_t *row, uint32_t width,
    int32_t sign, void *arg) {
    profile_t *p = arg; unsigned h = head(name);
    CHECK(width == 2 && row[0] > 0 && row[0] < IDS && row[1] > 0 &&
        row[1] < IDS && (sign == 1 || sign == -1));
    CHECK(!p->calls[h][row[0]][row[1]]++); p->events[h][row[0]][row[1]] = sign;
}
static void tuple(const char *name, const int64_t *row, uint32_t width,
    void *arg) {
    image_t *p = arg; unsigned h = head(name);
    CHECK(width == 2 && row[0] > 0 && row[0] < IDS && row[1] > 0 &&
        row[1] < IDS && !p->cell[h][row[0]][row[1]]);
    p->cell[h][row[0]][row[1]] = true;
}
static const unsigned left_key[IDS] = {0, 0, 1, 2, 0, 2},
    right_key[IDS] = {0, 0, 1, 2, 0};
static image_t reference(const fixture_t *f) {
    image_t image = {0};
    for (unsigned h = 0; h < 2;
        ++h) for (unsigned l = 1; l < IDS; ++l) for (unsigned r = 1;
                r < IDS; ++r)
                image.cell[h][l][r] = f->left[l] && f->right[r] &&
                    left_key[l] == right_key[r];
    return image;
}
static void content(profile_t *p, const image_t *want) {
    for (unsigned h = 0; h < 2; ++h) {
        const col_rel_t *r = session_find_rel(COL_SESSION(p->session),
                heads[h]);
        CHECK(r && r->ncols == 2 && r->column_types &&
            r->column_types[0] == WIRELOG_TYPE_INT64 &&
            r->column_types[1] == WIRELOG_TYPE_INT64);
        bool got[IDS][IDS] = {{false}};
        for (unsigned row = 0; row < r->nrows; ++row) {
            int64_t l = col_rel_get(r, row, 0), v = col_rel_get(r, row, 1);
            CHECK(l > 0 && l < IDS && v > 0 && v < IDS && !got[l][v]);
            got[l][v] = true;
        }
        CHECK(!memcmp(got, want->cell[h], sizeof(got)));
    }
}
static void delivery(fixture_t *f, profile_t *p, const image_t *old,
    const image_t *want) {
    content(p, want);
    for (unsigned h = 0; h < 2;
        ++h) for (unsigned l = 0; l < IDS; ++l) for (unsigned r = 0;
                r < IDS; ++r){
                int d = (int)want->cell[h][l][r]-(int)old->cell[h][l][r];
                CHECK(p->events[h][l][r] == (!f->route?d:0) &&
                    p->calls[h][l][r] == (!f->route && d?1u:0u));
            }
    if (f->route == 2)CHECK(!memcmp(&p->tuples, want, sizeof(*want)));
    memset(p->events, 0, sizeof(p->events));
    memset(p->calls, 0, sizeof(p->calls));
}
static int finish(fixture_t *f, profile_t *p) {
    memset(&p->tuples, 0, sizeof(p->tuples));
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = f->route == 2?wl_session_snapshot(p->session, tuple,
                &p->tuples):wl_session_step(p->session);
        if (rc != EAGAIN)return rc;
    }
    CHECK(false); return EIO;
}
static int64_t key_bits(fixture_t *f, unsigned key, bool negative_zero) {
    return f->kind == 2 && key == 0 &&
           negative_zero?wl_columnar_float_to_bits(-0.0):f->key[key];
}
static void insert_left(fixture_t *f, unsigned id) {
    int64_t row[] = {key_bits(f, left_key[id], id == 4), id};
    for (unsigned p = 0; p < 2;
        ++p) CHECK(!wl_session_insert(f->p[p].session, "left", row, 1, 2));
    f->left[id] = true;
}
static void right_row(fixture_t *f, unsigned id, int64_t row[4]) {
    row[0] = key_bits(f, right_key[id], true); row[1] = id;
    row[2] = f->payload[id%2];
    row[3] = wl_columnar_float_to_bits((double)id+0.25);
}
static void change_right(fixture_t *f, unsigned id, bool remove) {
    int64_t row[4]; right_row(f, id, row);
    for (unsigned p = 0; p < 2;
        ++p) CHECK(!(remove?wl_session_remove(f->p[p].session, "right", row, 1,
            4):wl_session_insert(f->p[p].session, "right", row, 1, 4)));
    f->right[id] = !remove;
}
static cache_t cache(profile_t *p) {
    wl_col_session_t *s = COL_SESSION(p->session); cache_t c = {0};
    c.container = (uintptr_t)s->diff_arr_entries; c.count = s->diff_arr_count;
    if (!c.count)return c;
    CHECK(c.count == 1);
    const col_diff_arr_entry_t *e = &s->diff_arr_entries[0];
    const col_diff_arrangement_t *a = e->diff_arr;
    CHECK(a && e->key_count == 1 && e->key_cols[0] == 0 && !strcmp(e->rel_name,
        "right") && !e->transaction_pending && !e->invalidation_deferred);
    CHECK(a->nbuckets <= CACHE_SLOTS && a->ht_cap <= CACHE_SLOTS &&
        a->ht_head && a->ht_next);
    c.arr = (uintptr_t)a; c.heads = (uintptr_t)a->ht_head;
    c.next = (uintptr_t)a->ht_next;
    c.buckets = a->nbuckets; c.capacity = a->ht_cap;
    c.indexed = a->indexed_rows; c.rows = a->current_nrows;
    c.generation = e->generation; c.pins = e->pin_count;
    c.snapshot = a->source_snapshot;
    memcpy(c.bucket, a->ht_head, c.buckets*sizeof(uint32_t));
    memcpy(c.link, a->ht_next, c.capacity*sizeof(uint32_t)); return c;
}
static void same_cache(const cache_t *a, const cache_t *b, bool container) {
    CHECK(a->count == b->count && (!container || a->container == b->container));
    if (!a->count)return;
    CHECK(a->arr == b->arr &&
        (a->heads == b->heads && a->next == b->next) &&
        a->buckets == b->buckets && a->capacity == b->capacity
        && a->indexed == b->indexed && a->rows == b->rows &&
        a->generation == b->generation && a->pins == b->pins
        && a->snapshot.relation_identity == b->snapshot.relation_identity
        && a->snapshot.view_generation == b->snapshot.view_generation
        && a->snapshot.storage_generation == b->snapshot.storage_generation
        && !memcmp(a->bucket, b->bucket,
        a->buckets*sizeof(uint32_t)) && !memcmp(a->link, b->link,
        a->capacity*sizeof(uint32_t)));
}
static wl_columnar_filter_context_t *context(fixture_t *f) {
    wl_col_session_t *s = COL_SESSION(f->p[f->profile].session);
    wl_columnar_eval_serial_framed_attempt_t *r = NULL;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(s->cleanup_active, s,
        &r) && r->capture_only && r->child_bound && r->private_bindings);
    CHECK(wl_columnar_session_observed_bindings_authorized(s,
        r->private_bindings));
    CHECK(!wl_columnar_eval_stack_filter_context_owned(s->cleanup_active, &ctx,
        &r->child)
        && ctx->join_operation == &r->child.plan->ops[r->child.cursor] &&
        ctx->join_operation->op == WL_PLAN_OP_JOIN
        && ctx->join_operation->key_count == 1 &&
        !ctx->join_operation->right_filter_expr.size);
    const wl_columnar_relation_scalar_source_t *source = &ctx->join_right;
    const col_rel_t *right = source->descriptor;
    CHECK(right && source->identity == source &&
        source->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD
        && source->owner_reader.owner == &right->storage_owner->source_access &&
        source->descriptor_reader.owner == &right->descriptor_access
        && right->compound_kind == WIRELOG_COMPOUND_KIND_NONE &&
        source->width == 4 && source->types
        && source->types[0] ==
        (f->kind == 0?WIRELOG_TYPE_INT64:f->kind ==
        1?WIRELOG_TYPE_STRING:WIRELOG_TYPE_FLOAT)
        && source->types[1] == WIRELOG_TYPE_INT64 &&
        source->types[2] == WIRELOG_TYPE_STRING &&
        source->types[3] == WIRELOG_TYPE_FLOAT
        && s->intern == f->plan->intern);
    unsigned h = head(r->child.plan->name);
    const col_rel_t *canonical = session_find_rel(s, "right");
    if (right != canonical) {
        CHECK(!f->profile && f->stage == 2 && right->nrows == 1 &&
            col_rel_get(right, 0, 1) == 4); f->delta_mask |= 1u<<h;
    }
    for (unsigned row = 0; row < right->nrows; ++row) {
        int64_t id = col_rel_get(right, row, 1), want[4];
        CHECK(id > 0 && id < 5 && f->right[id]);
        right_row(f, (unsigned)id, want);
        if (f->kind == 2 && right_key[id] == 0)want[0] = 0; /* public FLOAT storage canonicalizes signed zero */
        for (unsigned c = 0; c < 4;
            ++c) CHECK(col_rel_get(right, row, c) == want[c]);
    }
    return ctx;
}
static void arrangement(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress) {
    (void)progress; fixture_t *f = active;
    if (control != f->p[f->profile].control ||
        (phase != 13 && phase != 14)) return;
    wl_columnar_filter_context_t *ctx = context(f);
    CHECK(!f->profile &&
        ctx->join_right.descriptor ==
        session_find_rel(COL_SESSION(f->p[0].session), "right"));
    if (phase == 14)++f->builds; else ++f->reuses;
}
static void join(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress) {
    fixture_t *f = active;
    if (control != f->p[f->profile].control ||
        (phase != 28 && phase != 47 && phase != 48)) return;
    wl_columnar_filter_context_t *ctx = context(f);
    if (phase == 28)++f->private_rows;
    if (phase == 48)++f->probes;
    if (phase == 47) {
        CHECK(!f->profile && ctx->join_right.nrows &&
            progress < ctx->join_right.nrows); ++f->indexed;
        if (f->armed && !f->stopped && f->fault == 1 && f->stage == 3 &&
            progress == 1){
            f->stopped = true; wl_evaluation_control_request_cancel(control);
        }
    }
}
static bool allocation(wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_arrangement_test_diff_alloc_site_t site, size_t bytes) {
    fixture_t *f = active;
    if (f->profile || !f->armed || f->stopped || f->fault != 2 ||
        f->stage != 3 ||
        site != WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NEXT) return false;
    wl_columnar_filter_context_t *ctx = context(f);
    CHECK(control == f->p[0].control &&
        id == wl_evaluation_control_active_attempt_id(control) &&
        bytes == ctx->join_right.nrows*sizeof(uint32_t));
    f->stopped = true; return true;
}
static void watch(fixture_t *f, unsigned p, bool enabled) {
    active = f; f->profile = p;
    f->builds = f->reuses = f->indexed = f->probes = f->private_rows =
        f->delta_mask = 0;
    wl_columnar_arrangement_test_diff_checkpoint = enabled?arrangement:NULL;
    wl_columnar_join_test_attempt_checkpoint = enabled?join:NULL;
    wl_columnar_arrangement_test_diff_fail_allocation = enabled?allocation:NULL;
}
static void stopped(fixture_t *f, const image_t *old, const cache_t *before) {
    profile_t *p = &f->p[0]; wl_col_session_t *s = COL_SESSION(p->session);
    col_rel_t *l = session_find_rel(s, "left"),
        *r = session_find_rel(s, "right");
    col_relation_snapshot_t ls = wl_columnar_relation_snapshot(l),
        rs = wl_columnar_relation_snapshot(r);
    const col_rel_t *ptr[2] = {session_find_rel(s, heads[0]),
                               session_find_rel(s, heads[1])};
    uint64_t denials =
        wl_columnar_memory_denials(wl_columnar_memory_governor_ref_get(
                p->governor));
    f->armed = true;
    int expected = f->fault == 1?WL_EVALUATION_CONTROL_CANCELLED:ENOMEM;
    CHECK(finish(f, p) == expected && f->stopped && s->pending_input_change);
    f->armed = false;
    wl_columnar_arrangement_test_diff_fail_allocation = NULL;
    cache_t after = cache(p); same_cache(before, &after, true);
    content(p, old);
    CHECK(!memcmp(p->events, (int[2][IDS][IDS]){{{0}}},
        sizeof(p->events)) && !memcmp(p->calls, (unsigned[2][IDS][IDS]){{{0}}},
        sizeof(p->calls)));
    CHECK(session_find_rel(s, heads[0]) == ptr[0] && session_find_rel(s,
        heads[1]) == ptr[1]
        && wl_columnar_relation_snapshot_equal(ls,
        wl_columnar_relation_snapshot(l)) &&
        wl_columnar_relation_snapshot_equal(rs,
        wl_columnar_relation_snapshot(r)));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(p->control,
        &report) && report.result == expected && report.consumed &&
        !report.cleanup_status
        && !wl_evaluation_control_active_attempt_id(p->control) &&
        !s->cleanup_active && !s->cleanup_pending && !s->diff_txn_count &&
        !s->memory_budget_denied);
    f->failed_attempt = report.attempt_id;
    CHECK(wl_columnar_memory_denials(wl_columnar_memory_governor_ref_get(
            p->governor)) == denials);
    uint64_t owner[8];
    CHECK(!wl_columnar_session_test_retained_work_owner(s,
        owner) && !owner[0] && !owner[1] && !owner[2]);
    for (unsigned i = 0; i < 2; ++i) {
        wl_columnar_source_access_writer_t w = {0};
        CHECK(!col_rel_source_writer_acquire(i?r:l,
            &w) && !wl_columnar_source_access_writer_release(&w));
    }
    CHECK(!wl_evaluation_control_reset(p->control));
}
static void seed(fixture_t *f) {
    profile_t *p = &f->p[0];
    CHECK(!wl_evaluation_control_active_attempt_id(p->control));
    eval_stack_t stack; eval_stack_init(&stack);
    CHECK(!eval_stack_push(&stack,
        session_find_rel(COL_SESSION(p->session), "left"), false));
    CHECK(!col_op_join_diff(f->op, &stack, COL_SESSION(p->session)));
    eval_stack_drain(&stack);
    cache_t c = cache(p); CHECK(c.count == 1 && c.indexed == 3);
    printf("actual raw DIFF seed kind=%u rows=%u\n", f->kind, c.indexed);
}
static void invalid_float(fixture_t *f) {
    if (f->kind != 2)return;
    const int64_t invalid[] = {INT64_C(0x7ff8000000000001),
                               INT64_C(0x7ff0000000000000)};
    for (unsigned p = 0; p < 2; ++p)for (unsigned i = 0; i < 2; ++i) {
            profile_t *q = &f->p[p];
            wl_col_session_t *s = COL_SESSION(q->session);
            col_rel_t *right = session_find_rel(s, "right");
            col_relation_snapshot_t before =
                wl_columnar_relation_snapshot(right); cache_t index = cache(q);
            int64_t row[4]; right_row(f, 1, row); row[0] = invalid[i];
            CHECK(wl_session_insert(q->session, "right", row, 1, 4) == EINVAL);
            CHECK(session_find_rel(s,
                "right") == right && !s->pending_input_change &&
                wl_columnar_relation_snapshot_equal(before,
                wl_columnar_relation_snapshot(right)));
            cache_t after = cache(q); same_cache(&index, &after, true);
        }
}
int main(int argc, char **argv) {
    CHECK(argc == 4); fixture_t f = {0}; f.kind = (unsigned)atoi(argv[1]);
    f.route = (unsigned)atoi(argv[2]); CHECK(f.kind < 3 && f.route < 3);
    f.seed = !strcmp(argv[3], "seed"); f.pin_mode = !strcmp(argv[3], "pin");
    f.fault = !strcmp(argv[3], "cancel")?1u:!strcmp(argv[3], "nomem")?2u:0u;
    CHECK(!strcmp(argv[3], "healthy") || f.seed || f.pin_mode || f.fault);
    const char *vars[] = {"WIRELOG_DIFF_ENABLED", "WIRELOG_JOIN_BATCH_BYTES",
                          "WIRELOG_JOIN_BATCH_STRICT",
                          "WIRELOG_JOIN_OUTPUT_LIMIT",
                          "WIRELOG_SPILL_JOIN_BATCH_BYTES"};
    for (unsigned i = 0; i < sizeof(vars)/sizeof(vars[0]);
        ++i) env_clear(vars[i]);
    char source[1024];
    const char *type = f.kind == 0?"int64":f.kind == 1?"symbol":"float";
    snprintf(source, sizeof(source),
        ".decl left(k:%s,l:int64)\n.decl right(k:%s,r:int64,s:symbol,v:float)\n.decl out(l:int64,r:int64)\n.decl mirror(l:int64,r:int64)\nout(l,r):-left(k,l),right(k,r,s,v).\nmirror(l,r):-left(k,l),right(k,r,s,v).\n",
        type, type);
    wirelog_error_t error; f.program = wirelog_parse_string(source, &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    for (uint32_t st = 0; st < f.plan->stratum_count; ++st)for (uint32_t h = 0;
            h < f.plan->strata[st].relation_count; ++h)
            for (uint32_t i = 0; i < f.plan->strata[st].relations[h].op_count;
                ++i){
                const wl_plan_op_t *op =
                    &f.plan->strata[st].relations[h].ops[i];
                if (op->op == WL_PLAN_OP_JOIN) {
                    CHECK(!op->materialized && !op->opaque_data &&
                        op->key_count == 1 && !op->right_filter_expr.size);
                    f.op = op;
                }
            }
    CHECK(f.op);
    f.key[0] = f.kind == 1?wl_intern_put(f.plan->intern,
            "zero"):f.kind == 2?wl_columnar_float_to_bits(0.0):0;
    f.key[1] = f.kind == 1?wl_intern_put(f.plan->intern,
            "one"):f.kind == 2?wl_columnar_float_to_bits(1.5):2;
    f.key[2] = f.kind == 1?wl_intern_put(f.plan->intern,
            "negative"):f.kind == 2?wl_columnar_float_to_bits(-2.0):-3;
    f.payload[0] = wl_intern_put(f.plan->intern, "payload-a");
    f.payload[1] = wl_intern_put(f.plan->intern, "payload-b");
    CHECK(f.payload[0] >= 0 && f.payload[1] >= 0 &&
        !strcmp(wl_intern_reverse(f.plan->intern, f.payload[0]), "payload-a"));
    if (f.kind == 1)CHECK(f.key[0] >= 0 && f.key[1] >= 0 && f.key[2] >= 0 &&
            wl_intern_get(f.plan->intern,
            "zero") == f.key[0] && !strcmp(wl_intern_reverse(f.plan->intern,
            f.key[2]), "negative"));
    for (unsigned p = 0; p < 2; ++p) {
        profile_t *q = &f.p[p];
        CHECK(!wl_evaluation_control_create(UINT64_C(1000000000), &q->control));
        wl_columnar_memory_resolution_t res =
        {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
        q->governor = wl_columnar_memory_governor_ref_create(&res);
        CHECK(q->governor);
        wl_session_options_t opts; wl_session_options_init(&opts);
        opts.evaluation_control = q->control;
        opts.memory_governor = q->governor;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
            &opts, &q->session));
        if (!p)CHECK(COL_SESSION(q->session)->diff_enabled);
        else COL_SESSION(q->session)->diff_enabled = false;
        if (!f.route)wl_session_set_delta_cb(q->session, event, q);
        CHECK(!finish(&f, q));
    }
    image_t old = {0};
    for (f.stage = 0; f.stage < 5; ++f.stage) {
        if (!f.stage) {
            insert_left(&f, 1); insert_left(&f, 2); insert_left(&f, 3);
            change_right(&f, 1, false); change_right(&f, 2, false);
            change_right(&f, 3, false); if (f.seed) seed(&f);
        }
        if (f.stage == 1) {
            invalid_float(&f); insert_left(&f, 4);
        }
        if (f.stage == 2)change_right(&f, 4, false);
        if (f.stage == 3)change_right(&f, 2, true);
        if (f.stage == 4)insert_left(&f, 5);
        image_t want = reference(&f);
        if (f.stage == 1 && f.pin_mode) {
            uint32_t key = 0;
            CHECK(!col_session_pin_diff_arrangement(COL_SESSION(f.p[0].session),
                "right", session_find_rel(COL_SESSION(f.p[0].session), "right"),
                &key, 1, &f.pin));
        }
        for (unsigned p = 0; p < 2; ++p) {
            profile_t *q = &f.p[p]; cache_t before = cache(q);
            watch(&f, p, true);
            if (!p && f.fault && !f.stopped && f.stage == 3) {
                stopped(&f, &old, &before); watch(&f, p, true);
            }
            CHECK(!finish(&f, q)); delivery(&f, q, &old, &want);
            if (!p && f.stage == 3 && f.failed_attempt) {
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(q->control,
                    &report) && report.attempt_id &&
                    report.attempt_id != f.failed_attempt);
            }

            printf(
                "literal checked kind=%u route=%u profile=%u stage=%u build=%u reuse=%u private=%u\n",
                f.kind, f.route, p, f.stage, f.builds, f.reuses,
                f.private_rows); fflush(stdout);
            if (!p) {
                cache_t after = cache(q);
                if (f.stage == 1 && f.pin_mode) {
                    CHECK(!f.builds && !f.reuses && !f.indexed && !f.probes &&
                        f.private_rows);
                    same_cache(&before, &after, true);
                    CHECK(f.pin.active && f.pin.entry->pin_count == 1);
                    col_diff_arrangement_pin_release(&f.pin);
                    CHECK(!f.pin.active);
                }else if (f.delta_mask) {
                    CHECK(f.stage == 2 && f.delta_mask == 3 && !f.builds &&
                        !f.reuses && !f.indexed);
                    same_cache(&before, &after, true);
                }else if (f.stage == 1 || f.stage == 4 ||
                    (!f.stage && f.seed)) {
                    CHECK(!f.builds && f.reuses == 2 && !f.indexed &&
                        !f.private_rows && f.probes);
                    same_cache(&before, &after, false);
                }else {
                    CHECK(f.builds == 1 && f.reuses == 1 && f.indexed &&
                        f.probes && !f.private_rows);
                    CHECK(after.count == 1 &&
                        after.indexed ==
                        session_find_rel(COL_SESSION(q->session),
                        "right")->nrows &&
                        wl_columnar_relation_snapshot_equal(after.snapshot,
                        wl_columnar_relation_snapshot(session_find_rel(
                            COL_SESSION(q->session), "right"))));
                }
            } else CHECK(!cache(q).count && !f.builds && !f.reuses &&
                    !f.probes && f.private_rows);
            watch(&f, p, false); CHECK(!finish(&f, q));
            delivery(&f, q, &want, &want);
        }
        old = want;
    }
    CHECK(!f.fault || f.stopped); CHECK(!f.pin.active);
    for (unsigned p = 0; p < 2; ++p) {
        CHECK(!wl_session_try_destroy(&f.p[p].session));
        wl_evaluation_control_release(f.p[p].control);
    }
    wl_plan_free(f.plan); wirelog_program_free(f.program);
    for (unsigned p = 0; p < 2; ++p) {
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                f.p[p].governor)));
        wl_columnar_memory_governor_ref_release(f.p[p].governor);
    }
    puts("typed RHS literal results and persistent reuse passed"); return 0;
}
