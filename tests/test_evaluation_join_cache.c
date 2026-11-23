/* Natural JOIN index/source lifecycle against an independent logical set. */
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

#define CHECK(x)                                                               \
        do {                                                                         \
            if (!(x)) {                                                                \
                fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
                abort();                                                                 \
            }                                                                          \
        } while (0)
#define HEADS 5
#define VALUES 10
#define INDEX_ROWS 32
static const char *const names[HEADS] = {"first", "mirror", "reverse", "exact",
                                         "filtered"};
typedef struct {
    bool rows[HEADS][VALUES][VALUES];
} image_t;
typedef struct {
    int diff[HEADS][VALUES][VALUES];
    unsigned calls[HEADS][VALUES][VALUES];
} events_t;
typedef struct {
    wl_session_t *session;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    events_t events;
    image_t tuples;
} profile_t;
typedef struct {
    uintptr_t address;
    uint64_t generation;
    col_relation_snapshot_t snapshot;
    uint32_t rows, current, buckets, capacity;
    uint32_t pins;
    bool pending, deferred;
    uint32_t heads[INDEX_ROWS], next[INDEX_ROWS];
} index_image_t;
typedef struct {
    uintptr_t container;
    unsigned count;
    index_image_t entries[3];
} registry_t;
typedef struct {
    unsigned builds, reuses, indexed, persistent, private_rows;
    unsigned sources[HEADS], probes[HEADS], gathers[HEADS], rejected;
    unsigned append_reads[HEADS];
    unsigned output_extent[HEADS];
    unsigned image_cells, copied_entries;
    unsigned schema[HEADS], empty_left[HEADS], empty_right[HEADS];
    unsigned overrides[HEADS], selected[HEADS], false_rows[HEADS];
    unsigned private_head[HEADS];
    unsigned refill_builds, metadata_name_bytes, metadata_key_cells;
    unsigned header_cells, bucket_cells;
} witnesses_t;
/* Descriptive build-occurrence tuple, never an authority or private pointer. */
typedef struct {
    uintptr_t session, control, plan, operation, source;
    uint64_t attempt, control_identity, generation;
    col_relation_snapshot_t snapshot;
    unsigned occurrence, slot, key_count, key, rows, inserted, ordinal;
    bool pending;
} wl_test_join_cache_build_t;
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    profile_t profiles[3]; /* default controlled, private controlled, raw */
    bool compound, left[VALUES][VALUES], right[VALUES][VALUES][2];
    unsigned route, stage, stop_site;
    unsigned cap;
    unsigned resource;
    bool allocation_failed;
    unsigned index_allocation, allocation_calls[9], allocation_phase;
    uint64_t allocation_progress, allocation_attempt, failed_allocation_attempt;
    unsigned injected_allocations, allocation_ordinal, new_metadata;
    wl_test_join_cache_build_t prefix[3], start[3];
    witnesses_t build_base[3];
    unsigned build_calls[3][9], later_signature;
    uint64_t denials;
    wl_columnar_memory_reservation_t peer;
    bool pin_mode;
    col_diff_arrangement_pin_t pin;
    registry_t unpinned;
    bool cycle;
    unsigned cycle_fault;
    wl_test_join_cache_build_t cycle_build;
    uint64_t failed_build_attempt;
    unsigned build_ordinal;
    bool work_stop, armed, stopped;
    int64_t tag, floating;
    profile_t *active;
    unsigned profile, head, occurrence;
    const wl_columnar_eval_private_bindings_t *bindings;
    const wl_plan_relation_t *selected;
    const wl_plan_op_t *building;
    col_rel_t *append;
    const wl_plan_op_t *append_op;
    bool reading_selector;
    witnesses_t seen;
} fixture_t;
static fixture_t *active;
extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);

static unsigned head_number(const char *name) {
    for (unsigned i = 0; i < HEADS; ++i)
        if (!strcmp(name, names[i]))
            return i;
    CHECK(false);
    return 0;
}
static void environment(const char *name) {
#ifdef _WIN32
    CHECK(!_putenv_s(name, ""));
#else
    CHECK(!unsetenv(name));
#endif
}
static void capture(const char *name, const int64_t *row, uint32_t width,
    int32_t sign, void *payload) {
    events_t *e = payload;
    unsigned h = head_number(name);
    CHECK(width == 2 && row[0] > 0 && row[0] < VALUES && row[1] > 0 &&
        row[1] < VALUES && (sign == 1 || sign == -1));
    CHECK(!e->calls[h][row[0]][row[1]]++);
    e->diff[h][row[0]][row[1]] = sign;
}
static void snapshot_tuple(const char *name, const int64_t *row, uint32_t width,
    void *payload) {
    image_t *image = payload;
    unsigned h = head_number(name);
    CHECK(width == 2 && row[0] > 0 && row[0] < VALUES && row[1] > 0 &&
        row[1] < VALUES && !image->rows[h][row[0]][row[1]]);
    image->rows[h][row[0]][row[1]] = true;
}
/* Enumerate logical facts, without calling an evaluator or hashing routine. */
static image_t reference(const fixture_t *f) {
    image_t image = {0};
    for (unsigned x = 1; x < VALUES; ++x)
        for (unsigned y = 1; y < VALUES; ++y) {
            bool left_x = false, left_y = false;
            for (unsigned z = 1; z < VALUES; ++z) {
                left_x |= f->left[x][z];
                left_y |= f->left[z][y];
            }
            bool right = f->right[x][y][0] || f->right[x][y][1];
            image.rows[0][x][y] = image.rows[1][x][y] = left_x && right;
            image.rows[2][x][y] = left_y && right;
            image.rows[3][x][y] = f->left[x][y] && right;
            image.rows[4][x][y] = left_x && f->right[x][y][1];
        }
    return image;
}
/* Count the logical first/mirror JOIN pairs before projection DISTINCT. */
static unsigned first_pairs(const fixture_t *f) {
    unsigned pairs = 0;
    for (unsigned x = 1; x < VALUES; ++x)
        for (unsigned z = 1; z < VALUES; ++z)
            for (unsigned y = 1; y < VALUES; ++y)
                for (unsigned flag = 0; flag < 2; ++flag)
                    pairs += f->left[x][z] && f->right[x][y][flag];
    return pairs;
}
static void content(profile_t *p, const image_t *expected) {
    for (unsigned h = 0; h < HEADS; ++h) {
        const col_rel_t *r = session_find_rel(COL_SESSION(p->session),
                names[h]);
        CHECK(r && r->ncols == 2 &&
            r->compound_kind == WIRELOG_COMPOUND_KIND_NONE && r->column_types &&
            r->column_types[0] == WIRELOG_TYPE_INT64 &&
            r->column_types[1] == WIRELOG_TYPE_INT64);
        bool seen[VALUES][VALUES] = {{false}};
        unsigned count = 0;
        for (unsigned x = 1; x < VALUES; ++x)
            for (unsigned y = 1; y < VALUES; ++y)
                count += expected->rows[h][x][y];
        CHECK(r->nrows == count);
        for (uint32_t i = 0; i < r->nrows; ++i) {
            int64_t x = col_rel_get(r, i, 0), y = col_rel_get(r, i, 1);
            CHECK(x > 0 && x < VALUES && y > 0 && y < VALUES &&
                expected->rows[h][x][y] && !seen[x][y]);
            seen[x][y] = true;
        }
    }
}
static void check_delivery(fixture_t *f, profile_t *p, const image_t *old,
    const image_t *now) {
    content(p, now);
    for (unsigned h = 0; h < HEADS; ++h)
        for (unsigned x = 1; x < VALUES; ++x)
            for (unsigned y = 1; y < VALUES; ++y) {
                int diff = (int)now->rows[h][x][y] - (int)old->rows[h][x][y];
                CHECK(p->events.diff[h][x][y] == (f->route ? 0 : diff) &&
                    p->events.calls[h][x][y] == (!f->route && diff ? 1u : 0u));
                if (f->route == 2)
                    CHECK(p->tuples.rows[h][x][y] == now->rows[h][x][y]);
            }
    memset(&p->events, 0, sizeof(p->events));
}
static int finish(fixture_t *f, profile_t *p) {
    memset(&p->tuples, 0, sizeof(p->tuples));
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = f->route == 2
                 ? wl_session_snapshot(p->session, snapshot_tuple, &p->tuples)
                 : wl_session_step(p->session);
        if (f->route == 2 && rc)
            for (unsigned h = 0; h < HEADS; ++h)
                for (unsigned x = 0; x < VALUES; ++x)
                    for (unsigned y = 0; y < VALUES; ++y)
                        CHECK(!p->tuples.rows[h][x][y]);
        if (rc != EAGAIN)
            return rc;
    }
    CHECK(false);
    return EIO;
}
static unsigned signature(const col_diff_arr_entry_t *entry) {
    CHECK(!strcmp(entry->rel_name, "right") && entry->key_cols);
    if (entry->key_count == 2) {
        CHECK(entry->key_cols[0] == 0 && entry->key_cols[1] == 1);
        return 2;
    }
    CHECK(entry->key_count == 1 && entry->key_cols[0] < 2);
    return entry->key_cols[0];
}
static registry_t registry(profile_t *p) {
    const wl_col_session_t *s = COL_SESSION(p->session);
    registry_t image = {0};
    image.container = (uintptr_t)s->diff_arr_entries;
    image.count = s->diff_arr_count;
    CHECK(image.count == 0 || image.count == 3);
    unsigned bits = 0;
    for (uint32_t i = 0; i < s->diff_arr_count; ++i) {
        const col_diff_arr_entry_t *e = &s->diff_arr_entries[i];
        unsigned key = signature(e);
        bool pinned = active && active->pin.active
            && active->pin.session == s && active->pin.entry == e;
        CHECK(!(bits & (1u << key)) && e->diff_arr
            && e->pin_count == (pinned ? 1u : 0u) &&
            !e->transaction_pending && !e->invalidation_deferred);
        bits |= 1u << key;
        const col_diff_arrangement_t *a = e->diff_arr;
        CHECK(a->nbuckets <= INDEX_ROWS && a->indexed_rows <= INDEX_ROWS &&
            a->ht_cap <= INDEX_ROWS &&
            a->key_count == e->key_count);
        for (uint32_t k = 0; k < e->key_count; ++k)
            CHECK(a->key_cols[k] == e->key_cols[k]);
        index_image_t *v = &image.entries[key];
        v->address = (uintptr_t)a;
        v->generation = e->generation;
        v->snapshot = a->source_snapshot;
        v->rows = a->indexed_rows;
        v->current = a->current_nrows;
        v->buckets = a->nbuckets;
        v->capacity = a->ht_cap;
        v->pins = e->pin_count;
        v->pending = e->transaction_pending;
        v->deferred = e->invalidation_deferred;
        memcpy(v->heads, a->ht_head, a->nbuckets * sizeof(uint32_t));
        memcpy(v->next, a->ht_next, a->ht_cap * sizeof(uint32_t));
    }
    return image;
}
static void same_registry(const registry_t *a, const registry_t *b,
    bool container) {
    CHECK(a->count == b->count && (!container || a->container == b->container));
    for (unsigned i = 0; i < a->count; ++i) {
        const index_image_t *x = &a->entries[i], *y = &b->entries[i];
        CHECK(x->address == y->address && x->generation == y->generation &&
            x->snapshot.relation_identity == y->snapshot.relation_identity &&
            x->snapshot.view_generation == y->snapshot.view_generation &&
            x->snapshot.storage_generation == y->snapshot.storage_generation &&
            x->rows == y->rows && x->current == y->current &&
            x->buckets == y->buckets && x->capacity == y->capacity
            && x->pins == y->pins && x->pending == y->pending
            && x->deferred == y->deferred &&
            !memcmp(x->heads, y->heads, x->buckets * sizeof(uint32_t)) &&
            !memcmp(x->next, y->next, x->capacity * sizeof(uint32_t)));
    }
}
static void
wl_test_join_cache_bind(fixture_t *f,
    const wl_columnar_eval_private_bindings_t *bindings)
{
    CHECK(bindings && bindings->dynamic_entries
        && wl_columnar_session_observed_bindings_authorized(
            COL_SESSION(f->active->session), bindings));
    if (f->selected != bindings->plan) {
        f->append = NULL; f->append_op = NULL; f->building = NULL;
        f->cycle_build.pending = false;
    }
    f->bindings = bindings; f->selected = bindings->plan;
    f->head = head_number(bindings->plan->name);
}
static wl_columnar_filter_context_t *
context(fixture_t *f)
{
    wl_col_session_t *s = COL_SESSION(f->active->session);
    wl_columnar_eval_serial_framed_attempt_t *r = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(s->cleanup_active, s, &r)
        && r->capture_only && r->child_bound && r->private_bindings);
    wl_test_join_cache_bind(f, r->private_bindings);
    CHECK(r->child.plan == f->selected &&
        r->child.cursor < f->selected->op_count);
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(s->cleanup_active, &ctx,
        &r->child) && ctx->join_operation == &f->selected->ops[r->child.cursor]
        && ctx->join_operation->op == WL_PLAN_OP_JOIN);
    return ctx;
}
static void held(const wl_columnar_relation_scalar_source_t *s) {
    const col_rel_t *r = s->descriptor;
    CHECK(r && s->identity == s &&
        s->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD &&
        s->columns == (const int64_t *const *)r->columns &&
        s->descriptor_reader.identity == (uintptr_t)&s->descriptor_reader &&
        s->descriptor_reader.owner == &r->descriptor_access &&
        s->owner_reader.identity == (uintptr_t)&s->owner_reader &&
        s->owner_reader.owner == &r->storage_owner->source_access);
}
static bool rhs_delta(const fixture_t *f) {
    return !f->profile && !f->compound && !f->route && f->stage == 2;
}
static void sources(fixture_t *f, wl_columnar_filter_context_t *ctx) {
    held(&ctx->source);
    held(&ctx->join_right);
    const col_rel_t *right =
        session_find_rel(COL_SESSION(f->active->session), "right");
    const col_rel_t *chosen = ctx->join_right.descriptor;
    CHECK(ctx->join_right.width == 3 &&
        chosen->compound_kind == WIRELOG_COMPOUND_KIND_NONE);
    for (unsigned c = 0; c < 3; ++c)
        CHECK((!ctx->join_right.types ||
            ctx->join_right.types[c] == WIRELOG_TYPE_INT64));
    bool expected_delta = rhs_delta(f);
    CHECK((chosen != right) == expected_delta);
    if (expected_delta) {
        CHECK(f->append_op == ctx->join_operation && f->append == chosen &&
            chosen->nrows == 1 && col_rel_get(chosen, 0, 0) == 4 &&
            col_rel_get(chosen, 0, 1) == 2 && col_rel_get(chosen, 0, 2) == 1 &&
            wl_columnar_relation_snapshot_valid(
                wl_columnar_relation_snapshot(chosen)));
    } else {
        bool bound = false;
        for (uint32_t i = 0; i < f->bindings->count; ++i)
            if (!strcmp(f->bindings->dynamic_entries[i].name, "right")) {
                CHECK(f->bindings->dynamic_entries[i].relation == right &&
                    f->bindings->dynamic_entries[i].public_relation == right &&
                    wl_columnar_relation_snapshot_equal(
                        f->bindings->dynamic_entries[i].snapshot,
                        wl_columnar_relation_snapshot(chosen)));
                bound = true;
            }
        CHECK(bound);
    }
    unsigned offset = f->compound ? 2 : 0;
    CHECK(ctx->source.width == offset + 2 && ctx->source.nrows);
    if (f->compound) {
        const col_rel_t *l = ctx->source.descriptor;
        CHECK(l->compound_kind == WIRELOG_COMPOUND_KIND_INLINE &&
            l->compound_count == 1 && l->compound_arity_len == 3 &&
            l->compound_arity_map[0] == 2 && l->compound_arity_map[1] == 1 &&
            l->compound_arity_map[2] == 1 &&
            ctx->source.types[0] == WIRELOG_TYPE_STRING &&
            ctx->source.types[1] == WIRELOG_TYPE_FLOAT);
        for (uint32_t row = 0; row < ctx->source.nrows; ++row)
            CHECK(ctx->source.columns[0][row] == f->tag &&
                ctx->source.columns[1][row] == f->floating);
    }
    CHECK(!ctx->source.types ||
        (ctx->source.types[offset] == WIRELOG_TYPE_INT64 &&
        ctx->source.types[offset + 1] == WIRELOG_TYPE_INT64));
    if (!f->profile && !f->compound && !f->route &&
        (f->stage == 1 || f->stage == 4 || (f->index_allocation >=
        WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER && f->stage == 7)))
        CHECK(f->append_op->op == WL_PLAN_OP_VARIABLE &&
            ctx->source.descriptor == f->append && ctx->source.nrows == 1 &&
            ctx->source.columns[0][0] == (f->stage == 7 ? 8 : 4)
            && ctx->source.columns[1][0] == (f->stage == 7 ? 8 : 4));
}
static void observed(wl_evaluation_control_t *control, unsigned phase,
    uint32_t progress,
    const wl_columnar_eval_private_bindings_t *bindings) {
    fixture_t *f = active;
    if (control != f->active->control) return;
    if (f->reading_selector)
        return;
    if (phase == 8 && bindings) wl_test_join_cache_bind(f, bindings);
    if (phase == 44)
        f->occurrence = progress;
    if (phase == 28) {
        wl_test_join_cache_bind(f, bindings);
        CHECK(f->occurrence < f->selected->op_count);
        f->append_op = &f->selected->ops[f->occurrence];
        f->reading_selector = true;
        CHECK(!wl_columnar_session_delta_source(COL_SESSION(f->active->session),
            bindings, f->append_op,
            &f->append) &&
            f->append);
        f->reading_selector = false;
        ++f->seen.append_reads[f->head];
    }
}
static void
stop_at(fixture_t *f, wl_evaluation_control_t *control, unsigned site)
{
    if (f->profile || !f->armed || f->stopped || f->stop_site != site) return;
    if (site == 1) CHECK(f->seen.reuses && !f->seen.builds && !f->seen.indexed);
    if (site == 2) CHECK(f->seen.builds && f->seen.indexed >= 2);
    if (site == 3) CHECK(f->seen.persistent && !f->seen.builds &&
            !f->seen.indexed);
    f->stopped = true;
    if (f->work_stop) CHECK(wl_evaluation_control_charge_attempt(control,
            wl_evaluation_control_active_attempt_id(control), UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}
static void reserve_peer(fixture_t *f) {
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(f->active->governor);
    uint64_t usable = atomic_load_explicit(&g->usable_bytes,
            memory_order_acquire), used = wl_columnar_memory_reserved(g);
    CHECK(!f->stopped && usable > used && usable - used > 1);
    wl_columnar_memory_reservation_init(&f->peer);
    CHECK(wl_columnar_memory_reserve(g, usable - used - 1, &f->peer)
        && f->peer.identity == &f->peer
        && atomic_load_explicit(&f->peer.state, memory_order_acquire)
        == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && wl_columnar_memory_reserved(g) == usable - 1);
    f->denials = wl_columnar_memory_denials(g);
    f->stopped = true;
}
static unsigned
wl_test_join_cache_allocation_stage(const fixture_t *f)
{
    return f->new_metadata ? 0u : f->index_allocation >=
           WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER ? 5u : 1u;
}
static void
wl_test_join_cache_signature(fixture_t *f, wl_columnar_filter_context_t *ctx,
    unsigned signature)
{
    const wl_plan_op_t *op = ctx->join_operation;
    CHECK(signature >= 1 && signature <= 3 && !op->right_filter_expr.size
        && (signature == 1 ? f->head < 2 : f->head == signature)
        && op->key_count == (signature == 3 ? 2u : 1u));
    for (unsigned k = 0; k < op->key_count; ++k) {
        unsigned key = signature == 2 ? 1u : k;
        char left[16], right[16];
        snprintf(left, sizeof(left), "col%u", (f->compound ? 2u : 0u) + key);
        snprintf(right, sizeof(right), "col%u", key);
        CHECK(!strcmp(op->left_keys[k], left) && !strcmp(op->right_keys[k],
            right));
    }
}
/* Descriptive installed/insertion/probe records, not private index pointers. */
static void
wl_test_join_cache_prefix(fixture_t *f, wl_columnar_filter_context_t *ctx,
    unsigned action, uint64_t progress)
{
    unsigned signature = f->head < 2 ? 1u : f->head;
    wl_test_join_cache_signature(f, ctx, signature);
    wl_test_join_cache_build_t *b = &f->prefix[signature - 1];
    const wl_plan_op_t *op = ctx->join_operation;
    const col_rel_t *source = ctx->join_right.descriptor;
    if (!action) {
        CHECK(!b->operation && progress == signature - 1u);
        b->session = (uintptr_t)COL_SESSION(f->active->session);
        b->control = (uintptr_t)f->active->control;
        b->control_identity =
            wl_evaluation_control_identity(f->active->control);
        b->attempt =
            wl_evaluation_control_active_attempt_id(f->active->control);
        b->generation = f->bindings->generation;
        b->plan = (uintptr_t)f->selected; b->operation = (uintptr_t)op;
        b->occurrence = (unsigned)(op - f->selected->ops);
        b->source = (uintptr_t)source;
        b->snapshot = wl_columnar_relation_snapshot(source);
        b->slot = (unsigned)progress; b->key_count = op->key_count;
        b->key = signature == 2 ? 1u : 0u; b->rows = source->nrows;
        b->pending = true;
        return;
    }
    if (action == 2 && b->operation != (uintptr_t)op) return; /* sibling reuse */
    /* Warm reuse has no new build record; it cannot supply an owned prefix. */
    if (action == 2 && !b->operation) return;
    CHECK(b->operation == (uintptr_t)op && b->plan == (uintptr_t)f->selected
        && b->occurrence == (unsigned)(op - f->selected->ops)
        && b->generation == f->bindings->generation &&
        b->source == (uintptr_t)source
        && b->attempt ==
        wl_evaluation_control_active_attempt_id(f->active->control)
        && b->control_identity ==
        wl_evaluation_control_identity(f->active->control)
        && wl_columnar_relation_snapshot_equal(b->snapshot,
        wl_columnar_relation_snapshot(source)));
    if (action == 1) {
        CHECK(b->pending && progress == b->inserted && progress < b->rows);
        ++b->inserted;
    } else {
        CHECK(action == 2 && b->inserted == b->rows && progress < b->rows);
        /* Real probe follows final indexed_rows/source snapshot assignment. */
        b->pending = false; b->ordinal = f->head + 1;
    }
}
static void
wl_test_join_cache_prior(fixture_t *f, wl_columnar_filter_context_t *ctx,
    unsigned count)
{
    CHECK(f->seen.builds == count &&
        f->seen.indexed == count * ctx->join_right.nrows
        && f->seen.reuses == (count ? 1u : 0u));
    for (unsigned i = 0; i < 3; ++i) {
        const wl_test_join_cache_build_t *b = &f->prefix[i];
        if (i >= count) {
            CHECK(!b->operation); continue;
        }
        CHECK(b->operation && b->plan && b->ordinal && b->ordinal <= f->head &&
            !b->pending
            && b->session == (uintptr_t)COL_SESSION(f->active->session)
            && b->control == (uintptr_t)f->active->control
            && b->control_identity ==
            wl_evaluation_control_identity(f->active->control)
            && b->attempt ==
            wl_evaluation_control_active_attempt_id(f->active->control)
            && b->generation + (f->head + 1u - b->ordinal) ==
            f->bindings->generation && b->slot == i
            && b->key_count == (i == 2 ? 2u : 1u) &&
            b->key == (i == 1 ? 1u : 0u)
            && b->source == (uintptr_t)ctx->join_right.descriptor
            && b->rows == ctx->join_right.nrows && b->inserted == b->rows
            && wl_columnar_relation_snapshot_equal(b->snapshot,
            wl_columnar_relation_snapshot(ctx->join_right.descriptor)));
    }
}
static void
wl_test_join_cache_new_metadata(fixture_t *f, wl_columnar_filter_context_t *ctx,
    wl_columnar_arrangement_test_diff_alloc_site_t site, size_t bytes)
{
    wl_col_session_t *s = COL_SESSION(f->active->session);
    unsigned slot = f->new_metadata - 1;
    bool keys = site == WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS;
    wl_test_join_cache_signature(f, ctx, slot + 1);
    wl_test_join_cache_prior(f, ctx, slot);
    uint64_t capacity = 0, buckets = 1;
    for (uint32_t st = 0; st < f->plan->stratum_count; ++st)
        for (uint32_t h = 0; h < f->plan->strata[st].relation_count; ++h)
            capacity += f->plan->strata[st].relations[h].op_count + 2u;
    while (buckets < 2u * ctx->join_right.nrows) buckets *= 2;
    CHECK(f->stage == 0 && !s->diff_arr_count && !f->seen.copied_entries
        && ctx->join_right.nrows && f->allocation_phase == 2
        && f->allocation_progress == slot &&
        f->allocation_calls[site] == slot + 1
        && (keys || site == WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NAME)
        && bytes ==
        (keys ? ctx->join_operation->key_count *
        sizeof(uint32_t) : sizeof("right"))
        && f->seen.image_cells ==
        capacity *
        (sizeof(col_diff_arr_entry_t) +
        sizeof(wl_columnar_arrangement_diff_slot_t))
        && f->allocation_calls[WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_IMAGE] ==
        1
        && f->allocation_calls[WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_SLOTS] ==
        1
        && f->allocation_calls[WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NAME] ==
        slot + 1
        && f->allocation_calls[
            WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS] ==
        slot + (keys ? 1u : 0u)
        && f->seen.metadata_name_bytes ==
        (slot + (keys ? 1u : 0u)) * sizeof("right")
        /* phase4 includes prior metadata AND prior initialized build keys. */
        && f->seen.metadata_key_cells == 2u * slot
        && f->seen.header_cells == slot * (sizeof(col_diff_arrangement_t) + 1u)
        && f->seen.bucket_cells == slot * (buckets + ctx->join_right.nrows));
    for (unsigned i = WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER;
        i <= WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NEXT; ++i)
        CHECK(f->allocation_calls[i] == slot);
    fprintf(stdout, "new signature=%u slot=%u keys=%u prior-owned=%u\n",
        slot + 1, slot, ctx->join_operation->key_count, slot);
}
static void
wl_test_join_cache_later_start(fixture_t *f, wl_columnar_filter_context_t *ctx)
{
    unsigned signature = f->head < 2 ? 1u : f->head, slot = signature - 1;
    wl_test_join_cache_signature(f, ctx, signature);
    wl_test_join_cache_build_t *b = &f->start[slot];
    CHECK(!b->operation);
    b->session = (uintptr_t)COL_SESSION(f->active->session);
    b->control = (uintptr_t)f->active->control;
    b->control_identity = wl_evaluation_control_identity(f->active->control);
    b->attempt = wl_evaluation_control_active_attempt_id(f->active->control);
    b->generation = f->bindings->generation;
    b->plan = (uintptr_t)f->selected;
    b->operation = (uintptr_t)ctx->join_operation;
    b->occurrence = (unsigned)(ctx->join_operation - f->selected->ops);
    b->source = (uintptr_t)ctx->join_right.descriptor;
    b->snapshot = wl_columnar_relation_snapshot(ctx->join_right.descriptor);
    b->slot = slot; b->key_count = ctx->join_operation->key_count;
    b->key = signature == 2 ? 1u : 0u; b->rows = ctx->join_right.nrows;
    b->pending = true;
    f->build_base[slot] = f->seen;
    memcpy(f->build_calls[slot], f->allocation_calls,
        sizeof(f->allocation_calls));
}
static void
wl_test_join_cache_later_allocation(fixture_t *f,
    wl_columnar_filter_context_t *ctx,
    wl_columnar_arrangement_test_diff_alloc_site_t site, size_t bytes)
{
    unsigned prior = f->later_signature - 1;
    wl_test_join_cache_signature(f, ctx, f->later_signature);
    wl_test_join_cache_prior(f, ctx, prior);
    const wl_test_join_cache_build_t *b = &f->start[prior];
    const witnesses_t *base = &f->build_base[prior];
    wl_col_session_t *s = COL_SESSION(f->active->session);
    CHECK(f->stage == 5 && s->diff_arr_count == 3
        && site >= WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER
        && f->allocation_phase == 2 && b->pending
        && b->operation == (uintptr_t)ctx->join_operation &&
        b->plan == (uintptr_t)f->selected
        && b->occurrence == (unsigned)(ctx->join_operation - f->selected->ops)
        && b->session == (uintptr_t)s &&
        b->control == (uintptr_t)f->active->control
        && b->control_identity ==
        wl_evaluation_control_identity(f->active->control)
        && b->attempt ==
        wl_evaluation_control_active_attempt_id(f->active->control)
        && b->generation == f->bindings->generation && b->slot == prior
        && b->key_count == ctx->join_operation->key_count
        && b->source == (uintptr_t)ctx->join_right.descriptor &&
        b->rows == ctx->join_right.nrows
        && wl_columnar_relation_snapshot_equal(b->snapshot,
        wl_columnar_relation_snapshot(ctx->join_right.descriptor)));
    uint64_t buckets = 1, capacity = s->diff_arr_count;
    while (buckets < 2u * b->rows) buckets *= 2;
    for (uint32_t st = 0; st < f->plan->stratum_count; ++st)
        for (uint32_t h = 0; h < f->plan->strata[st].relation_count; ++h)
            capacity += f->plan->strata[st].relations[h].op_count + 2u;
    CHECK(base->image_cells ==
        capacity *
        (sizeof(col_diff_arr_entry_t) +
        sizeof(wl_columnar_arrangement_diff_slot_t))
        && base->copied_entries == 3 &&
        base->metadata_name_bytes == 3u * sizeof("right")
        && base->metadata_key_cells == 4u + prior /* copied pairs + earlier build keys */
        && base->header_cells == prior * (sizeof(col_diff_arrangement_t) + 1u)
        && base->bucket_cells == prior * (buckets + b->rows)
        && f->seen.image_cells == base->image_cells &&
        f->seen.copied_entries == base->copied_entries
        && f->seen.metadata_name_bytes == base->metadata_name_bytes
        && f->seen.metadata_key_cells == base->metadata_key_cells
        && f->seen.bucket_cells == base->bucket_cells
        && f->allocation_calls[WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_IMAGE] ==
        1
        && f->allocation_calls[WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_SLOTS] ==
        1
        && f->allocation_calls[WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NAME] ==
        3
        && f->allocation_calls[
            WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS] == 3);
    uint64_t sizes[] = {sizeof(col_diff_arrangement_t),
                        b->key_count * sizeof(uint32_t),
                        buckets * sizeof(uint32_t), b->rows * sizeof(uint32_t)};
    unsigned target = site - WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER;
    CHECK(bytes == sizes[target] &&
        f->allocation_progress ==
        (target ? bytes : sizes[0]+sizes[1]+sizes[2]+sizes[3])
        && f->seen.header_cells - base->header_cells ==
        (target ? sizeof(col_diff_arrangement_t) + 1u : 0u));
    for (unsigned i = 0; i < 4; ++i) {
        unsigned tag = WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER + i;
        CHECK(f->build_calls[prior][tag] == prior
            && f->allocation_calls[tag] == prior + (i <= target ? 1u : 0u));
    }
    fprintf(stdout,
        "later signature=%u keys=%u prior-owned=%u target-prefix=%u\n",
        f->later_signature, b->key_count, prior, target);
}
/* Observe only the real controlled allocator boundary, never its private owner. */
static bool
wl_test_join_cache_index_allocation(wl_evaluation_control_t *control,
    uint64_t id, wl_columnar_arrangement_test_diff_alloc_site_t site,
    size_t bytes)
{
    fixture_t *f = active;
    CHECK(f && !f->profile && f->index_allocation && bytes
        && control == f->active->control && id
        && id == wl_evaluation_control_active_attempt_id(control)
        && site >= WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_IMAGE
        && site <= WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NEXT);
    wl_columnar_filter_context_t *ctx = context(f);
    sources(f, ctx);
    wl_col_session_t *s = COL_SESSION(f->active->session);
    CHECK(s->diff_txn_count == 1 && !rhs_delta(f)
        && !ctx->join_operation->right_filter_expr.size);
    if (!f->allocation_attempt) f->allocation_attempt = id;
    CHECK(f->allocation_attempt == id && (!f->armed || !f->stopped));
    ++f->allocation_calls[site];
    if (!f->armed || site != f->index_allocation
        || (f->allocation_ordinal &&
        f->allocation_calls[site] < f->allocation_ordinal)) return false;
    if (f->new_metadata) wl_test_join_cache_new_metadata(f, ctx, site, bytes);
    else if (f->later_signature) wl_test_join_cache_later_allocation(f, ctx,
            site, bytes);
    else {
        CHECK(f->stage ==
            (f->index_allocation >=
            WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER ? 5u : 1u)
            && f->head < 2 && s->diff_arr_count == 3
            && !f->seen.builds && !f->seen.reuses && !f->seen.indexed
            && f->allocation_calls[site] ==
            (f->allocation_ordinal ? f->allocation_ordinal : 1u)
            && f->allocation_phase == 2);
        /* The real transaction image contains every head's operations plus its
         * two reserved suffix slots, not only this current head's plan. */
        uint64_t capacity = s->diff_arr_count;
        unsigned heads = 0;
        for (uint32_t st = 0; st < f->plan->stratum_count; ++st) {
            const wl_plan_stratum_t *stratum = &f->plan->strata[st];
            CHECK(!stratum->is_recursive);
            for (uint32_t h = 0; h < stratum->relation_count; ++h) {
                capacity += stratum->relations[h].op_count + 2u;
                ++heads;
            }
        }
        CHECK(heads == HEADS);
        if (site == WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_IMAGE) {
            CHECK(f->allocation_progress == bytes &&
                bytes == capacity * sizeof(col_diff_arr_entry_t)
                && !f->seen.image_cells && !f->seen.copied_entries
                && !f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_SLOTS]);
        } else if (site == WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_SLOTS) {
            CHECK(f->allocation_progress == bytes
                && bytes ==
                capacity * sizeof(wl_columnar_arrangement_diff_slot_t)
                && f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_IMAGE]
                == 1
                && f->seen.image_cells ==
                capacity * sizeof(col_diff_arr_entry_t)
                && !f->seen.copied_entries);
        } else if (site <=
            WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS) {
            CHECK((site == WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NAME
                || site ==
                WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS)
                && f->allocation_ordinal >= 1 && f->allocation_ordinal <= 2);
            unsigned slot = f->allocation_ordinal - 1;
            const col_diff_arr_entry_t *entry = &s->diff_arr_entries[slot];
            unsigned length = (unsigned)strlen("right") + 1, prior_keys = 0;
            for (unsigned i = 0; i < slot; ++i)
                prior_keys += s->diff_arr_entries[i].key_count;
            bool keys = site ==
                WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS;
            CHECK(!strcmp(entry->rel_name, "right") && entry->key_count
                && f->allocation_progress == slot
                && f->seen.image_cells ==
                capacity * (sizeof(col_diff_arr_entry_t)
                + sizeof(wl_columnar_arrangement_diff_slot_t))
                && f->seen.copied_entries == f->allocation_ordinal
                && f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_IMAGE]
                == 1
                && f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_SLOTS]
                == 1
                && f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NAME]
                == slot + 1
                && f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS] ==
                slot + (keys ? 1u : 0u)
                && f->seen.metadata_name_bytes ==
                (slot + (keys ? 1u : 0u)) * length
                && f->seen.metadata_key_cells == prior_keys
                && bytes ==
                (keys ? entry->key_count * sizeof(uint32_t) : length));
        } else {
            CHECK(site >= WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER
                && ctx->join_operation->key_count == 1
                && !strcmp(ctx->join_operation->right_keys[0], "col0")
                && ctx->join_right.nrows && f->seen.copied_entries == 3
                && f->seen.image_cells ==
                capacity * (sizeof(col_diff_arr_entry_t)
                + sizeof(wl_columnar_arrangement_diff_slot_t))
                && f->seen.metadata_name_bytes == 3u * sizeof("right")
                && f->seen.metadata_key_cells == 4 && !f->seen.bucket_cells
                && f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_IMAGE]
                == 1
                && f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_SLOTS]
                == 1
                && f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NAME]
                == 3
                && f->allocation_calls[
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS] ==
                3);
            uint64_t buckets = 1;
            while (buckets < 2u * ctx->join_right.nrows) buckets *= 2;
            uint64_t sizes[] = {sizeof(col_diff_arrangement_t),
                                sizeof(uint32_t),
                                buckets * sizeof(uint32_t),
                                ctx->join_right.nrows * sizeof(uint32_t)};
            uint64_t total = sizes[0] + sizes[1] + sizes[2] + sizes[3];
            unsigned ordinal = site -
                WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER;
            CHECK(bytes == sizes[ordinal] &&
                f->allocation_progress == (ordinal ? bytes : total)
                && f->seen.header_cells ==
                (ordinal ? sizeof(col_diff_arrangement_t) + 1u : 0u));
            for (unsigned i = 0; i < 4; ++i)
                CHECK(
                    f->allocation_calls[
                        WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER + i]
                    == (i <= ordinal ? 1u : 0u));
            /* Reaching the next malloc proves earlier pointers succeeded. Keys
             * and hash heads are allocated, not populated, until ALL succeed. */
        }
    }
    f->denials = wl_columnar_memory_denials(
        wl_columnar_memory_governor_ref_get(f->active->governor));
    f->failed_allocation_attempt = id;
    CHECK(!f->injected_allocations++);
    f->stopped = true;
    fprintf(stdout,
        "index malloc NULL site=%u bytes=%zu attempt=%llu image=%u copied=%u\n",
        (unsigned)site, bytes, (unsigned long long)id,
        f->seen.image_cells, f->seen.copied_entries);
    return true;
}
static void arrangement(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress) {
    (void)progress;
    fixture_t *f = active;
    if (control != f->active->control) return;
    f->allocation_phase = phase; f->allocation_progress = progress;
    if (f->index_allocation) {
        if (phase == 5) ++f->seen.image_cells;
        if (phase == 6) ++f->seen.copied_entries;
        if (phase == 3) ++f->seen.metadata_name_bytes;
        if (phase == 4) ++f->seen.metadata_key_cells;
        if (phase == 8) ++f->seen.header_cells;
        if (phase == 9) ++f->seen.bucket_cells;
    }
    if (!f->profile && f->later_signature && phase == 7 && progress == 1)
        wl_test_join_cache_later_start(f, context(f));
    if (!f->profile && f->armed && f->pin_mode && !f->stopped
        && phase == 0 && COL_SESSION(f->active->session)->diff_txn_count == 1) {
        wl_columnar_filter_context_t *ctx = context(f);
        sources(f, ctx);
        CHECK(f->stage == 1 && f->head < 2 && !progress
            && f->pin.active && f->pin.entry ==
            COL_SESSION(f->active->session)->diff_arr_entries
            && f->pin.entry->pin_count == 1 && !f->seen.builds
            && !f->seen.reuses);
        f->stopped = true; /* Observe the genuine image pin refusal. */
    }
    if (!f->profile && f->armed && f->resource && !f->stopped) {
        wl_columnar_filter_context_t *ctx = context(f);
        sources(f, ctx);
        wl_col_session_t *s = COL_SESSION(f->active->session);
        CHECK(f->head < 2 && !ctx->join_operation->right_filter_expr.size
            && !rhs_delta(f) && s->diff_arr_count == 3);
        if (phase == 5) ++f->seen.image_cells;
        if (phase == 6) ++f->seen.copied_entries;
        if (f->resource == 1 && phase == 0 && !s->diff_txn_count) {
            CHECK(f->stage == 1 && !f->seen.image_cells
                && !f->seen.builds && !f->seen.reuses);
            reserve_peer(f); /* Before image/slot admission. */
        }
        if (f->resource == 2 && phase == 7) {
            CHECK(f->stage == 5 && s->diff_txn_count == 1
                && f->seen.image_cells && f->seen.copied_entries == 3
                && !f->seen.builds && !f->seen.indexed && progress == 1);
            reserve_peer(f); /* Admitted/copied image, before build credit. */
        }
    }
    if (phase != 13 && phase != 14)
        return;
    wl_columnar_filter_context_t *ctx = context(f);
    sources(f, ctx);
    CHECK(!f->profile && !ctx->join_operation->right_filter_expr.size &&
        ctx->join_right.nrows && !rhs_delta(f));
    if (phase == 14) {
        if (f->new_metadata || f->later_signature) wl_test_join_cache_prefix(f,
                ctx, 0, progress);
        ++f->seen.builds;
        f->building = ctx->join_operation;
    } else {
        ++f->seen.reuses;
        f->building = NULL;
        stop_at(f, control, 1);
    }
}
static void join(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress) {
    fixture_t *f = active;
    if (control != f->active->control) return;
    if (phase != 15 && phase != 28 && phase != 47 && phase != 48 &&
        phase != 29 && phase != 20 && phase != 9 && phase != 11 &&
        phase != 18 && phase != 44)
        return;
    wl_columnar_filter_context_t *ctx = context(f);
    sources(f, ctx);
    if (!f->profile && f->armed && f->resource == 3) {
        wl_columnar_join_scratch_t *keys = &ctx->join_scratch[0];
        uint64_t bytes = 2u * sizeof(uint32_t)
            * ctx->join_operation->key_count;
        if (phase == 9 && !f->stopped && keys->initialized
            && !ctx->join_scratch[1].initialized) {
            CHECK(f->stage == 1 && f->head < 2 && progress == bytes
                && !keys->allocated && !keys->physical
                && !f->seen.builds && !f->seen.reuses);
            f->denials = wl_columnar_memory_denials(
                wl_columnar_memory_governor_ref_get(f->active->governor));
            wl_columnar_join_test_attempt_fail_allocation = 0;
            f->stopped = true;
        }
        if (phase == 11 && f->stopped && !f->allocation_failed) {
            CHECK(progress == bytes && keys->initialized && keys->admitted
                && !keys->allocated && !keys->physical
                && !wl_columnar_join_test_attempt_fail_allocation);
            f->allocation_failed = true;
        }
    }
    if (phase == 20) {
        CHECK(ctx->output && progress == ctx->output->nrows);
        if (progress > f->seen.output_extent[f->head])
            f->seen.output_extent[f->head] = (unsigned)progress;
        if (f->armed && f->cap && progress == f->cap) {
            CHECK(!f->stage && f->head < 2 && first_pairs(f) == 5);
            f->stopped = true; /* Observe the real cap; do not inject it. */
        }
    }
    if (phase == 15)
        ++f->seen.sources[f->head];
    if (phase == 29)
        ++f->seen.probes[f->head];
    if (phase == 18)
        ++f->seen.gathers[f->head];
    if (phase == 28)
        ++f->seen.private_rows;
    if (phase == 47) {
        CHECK(!ctx->join_operation->right_filter_expr.size &&
            ctx->join_right.nrows && f->building == ctx->join_operation &&
            !rhs_delta(f));
        if (f->new_metadata || f->later_signature) wl_test_join_cache_prefix(f,
                ctx, 1, progress);
        ++f->seen.indexed;
        if (progress == 1) stop_at(f, control, 2);
    }
    if (phase == 48) {
        CHECK(!f->profile && !ctx->join_operation->right_filter_expr.size &&
            !rhs_delta(f) && progress < ctx->join_right.nrows);
        if (f->new_metadata || f->later_signature) wl_test_join_cache_prefix(f,
                ctx, 2, progress);
        ++f->seen.persistent;
        stop_at(f, control, 3);
    }
    if (phase == 44 && ctx->join_right.columns[0][progress] == 2 &&
        ctx->join_right.columns[1][progress] == 7 &&
        ctx->join_right.columns[2][progress] == 0) {
        CHECK(f->head == 4 && ctx->join_operation->right_filter_expr.size);
        ++f->seen.rejected;
    }
}
/* Cycle assertions intentionally do not relax the legacy nonempty contract. */
static void
wl_test_join_cache_cycle_held(fixture_t *f,
    const wl_columnar_relation_scalar_source_t *source)
{
    held(source);
    const col_rel_t *r = source->descriptor;
    CHECK(source->control == f->active->control
        && source->attempt_id == f->bindings->attempt_id
        && source->owner == r->storage_owner && source->thread_valid
        && source->descriptor_identity == r->relation_identity
        && source->descriptor_view_generation == r->view_generation
        && source->descriptor_storage_generation == r->storage_generation
        && source->owner_identity == r->storage_owner->relation_identity
        && source->owner_storage_generation ==
        r->storage_owner->storage_generation
        && source->nrows == r->nrows && source->width == r->ncols);
}
static bool
wl_test_join_cache_cycle_sources(fixture_t *f,
    wl_columnar_filter_context_t *ctx)
{
    CHECK(!ctx->source_bare && !ctx->join_right_bare);
    wl_test_join_cache_cycle_held(f, &ctx->source);
    wl_test_join_cache_cycle_held(f, &ctx->join_right);
    CHECK(ctx->join_right.width == 3);
    const wl_plan_op_t *op = ctx->join_operation;
    unsigned offset = f->compound ? 2 : 0;
    CHECK(op->key_count == (f->head == 3 ? 2u : 1u)
        && op->project_count == 2 && op->project_indices);
    for (unsigned k = 0; k < op->key_count; ++k) {
        char left_key[16], right_key[16];
        snprintf(left_key, sizeof(left_key), "col%u",
            offset + (f->head == 2 ? 1 : k));
        snprintf(right_key, sizeof(right_key), "col%u", f->head == 2 ? 1 : k);
        CHECK(!strcmp(op->left_keys[k], left_key) && !strcmp(op->right_keys[k],
            right_key));
    }
    CHECK(op->project_indices[0] == (f->head == 2 ? offset + 2 : offset)
        && op->project_indices[1] == (f->head == 2 ||
        f->head == 3 ? offset + 1 : offset + 3));
    col_rel_t *right = session_find_rel(COL_SESSION(f->active->session),
            "right");
    CHECK(right && right->schema_ok && right->ncols == 3);
    col_rel_t *override = NULL;
    f->reading_selector = true;
    CHECK(!wl_columnar_session_delta_source(COL_SESSION(f->active->session),
        f->bindings, ctx->join_operation, &override));
    f->reading_selector = false;
    CHECK(ctx->join_right.descriptor == (override ? override : right));
    bool eligible = !f->profile && !f->compound && !f->route;
    bool appended = eligible &&
        (f->stage == 3 || f->stage == 5 || f->stage == 8);
    CHECK((override != NULL) == appended);
    if (override) {
        unsigned key = f->stage == 3 ? 1 : f->stage == 5 ? 2 : 4;
        CHECK(f->append_op == ctx->join_operation && f->append == override
            && override->nrows == 1 && col_rel_get(override, 0, 0) == key
            && col_rel_get(override, 0, 1) == key
            && col_rel_get(override, 0, 2) == 1
            && wl_columnar_relation_snapshot_valid(
                wl_columnar_relation_snapshot(override)));
    } else {
        bool found = false;
        for (unsigned i = 0; i < f->bindings->count; ++i) {
            const wl_columnar_eval_private_dynamic_binding_t *b =
                &f->bindings->dynamic_entries[i];
            if (strcmp(b->name, "right")) continue;
            CHECK(b->relation == right && b->public_relation == right
                && wl_columnar_relation_snapshot_equal(b->snapshot,
                wl_columnar_relation_snapshot(right)));
            found = true;
        }
        CHECK(found);
    }
    for (unsigned c = 0; c < 3; ++c)
        CHECK(!ctx->join_right.types
            || ctx->join_right.types[c] == WIRELOG_TYPE_INT64);
    unsigned count = 0;
    for (unsigned x = 1; x < VALUES; ++x)
        for (unsigned y = 1; y < VALUES; ++y)
            for (unsigned flag = 0; flag < 2; ++flag)
                count += f->right[x][y][flag];
    CHECK(right->nrows == count);
    if (!override) CHECK(ctx->join_right.nrows == count);
    bool right_seen[VALUES][VALUES][2] = {{{false}}};
    for (unsigned row = 0; row < ctx->join_right.nrows; ++row) {
        int64_t x = ctx->join_right.columns[0][row],
            y = ctx->join_right.columns[1][row],
            flag = ctx->join_right.columns[2][row];
        CHECK(x > 0 && x < VALUES && y > 0 && y < VALUES
            && (flag == 0 || flag == 1) && f->right[x][y][flag]
            && !right_seen[x][y][flag]);
        right_seen[x][y][flag] = true;
    }
    CHECK(ctx->source.width == offset + 2);
    if (f->compound) {
        const col_rel_t *l = ctx->source.descriptor;
        CHECK(l->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
            && l->compound_count == 1 && l->compound_arity_len == 3
            && l->compound_arity_map[0] == 2 && l->compound_arity_map[1] == 1
            && l->compound_arity_map[2] == 1
            && ctx->source.types[0] == WIRELOG_TYPE_STRING
            && ctx->source.types[1] == WIRELOG_TYPE_FLOAT);
        for (unsigned row = 0; row < ctx->source.nrows; ++row)
            CHECK(ctx->source.columns[0][row] == f->tag
                && ctx->source.columns[1][row] == f->floating);
    }
    CHECK(!ctx->source.types ||
        (ctx->source.types[offset] == WIRELOG_TYPE_INT64
        && ctx->source.types[offset + 1] == WIRELOG_TYPE_INT64));
    if (eligible && (f->stage == 6 || f->stage == 7)) {
        col_rel_t *frontier = NULL;
        CHECK(f->append_op && f->append_op->op == WL_PLAN_OP_VARIABLE);
        f->reading_selector = true;
        CHECK(!wl_columnar_session_delta_source(COL_SESSION(f->active->session),
            f->bindings, f->append_op, &frontier));
        f->reading_selector = false;
        CHECK(frontier && frontier == f->append &&
            frontier == ctx->source.descriptor
            && frontier->nrows == (f->stage == 6 ? 2u : 1u));
    } else {
        unsigned rows = 0;
        for (unsigned x = 1; x < VALUES; ++x)
            for (unsigned z = 1; z < VALUES; ++z) rows += f->left[x][z];
        CHECK(ctx->source.nrows == rows);
    }
    bool left_seen[VALUES][VALUES] = {{false}};
    for (unsigned row = 0; row < ctx->source.nrows; ++row) {
        int64_t x = ctx->source.columns[offset][row],
            z = ctx->source.columns[offset + 1][row];
        CHECK(x > 0 && x < VALUES && z > 0 && z < VALUES && f->left[x][z]
            && !left_seen[x][z]);
        left_seen[x][z] = true;
        if (eligible && f->stage == 7) CHECK(x == 4 && z == 4);
    }
    return override != NULL;
}
static void
wl_test_join_cache_cycle_build_capture(fixture_t *f,
    wl_columnar_filter_context_t *ctx, uint64_t slot)
{
    wl_col_session_t *s = COL_SESSION(f->active->session);
    const wl_plan_op_t *op = ctx->join_operation;
    const col_rel_t *source = ctx->join_right.descriptor;
    CHECK(!f->cycle_build.pending && !f->build_ordinal && s->diff_enabled
        && s->diff_txn_count == 1 && s->diff_arr_count == 3
        && ctx->source.nrows && source->nrows == 2
        && !op->right_filter_expr.size && op->key_count == 1
        && !strcmp(op->right_keys[0], "col0") && slot == 0
        && s->diff_arr_entries[slot].key_count == 1
        && s->diff_arr_entries[slot].key_cols[0] == 0
        && f->seen.schema[f->head] && f->seen.sources[f->head]);
    wl_test_join_cache_build_t *b = &f->cycle_build;
    b->session = (uintptr_t)s; b->control = (uintptr_t)f->active->control;
    b->control_identity = wl_evaluation_control_identity(f->active->control);
    b->attempt = wl_evaluation_control_active_attempt_id(f->active->control);
    CHECK(b->attempt && b->attempt == f->bindings->attempt_id);
    b->generation = f->bindings->generation;
    b->plan = (uintptr_t)f->selected; b->operation = (uintptr_t)op;
    b->occurrence = (unsigned)(op - f->selected->ops);
    b->source = (uintptr_t)source;
    b->snapshot = wl_columnar_relation_snapshot(source);
    b->slot = (unsigned)slot; b->key_count = op->key_count; b->key = 0;
    b->rows = source->nrows; b->inserted = 0;
    b->ordinal = ++f->build_ordinal; b->pending = true;
    if (f->failed_build_attempt) CHECK(b->attempt != f->failed_build_attempt);
}
static void
wl_test_join_cache_cycle_build_row(fixture_t *f,
    wl_columnar_filter_context_t *ctx, uint64_t row)
{
    wl_test_join_cache_build_t *b = &f->cycle_build;
    const wl_plan_op_t *op = ctx->join_operation;
    wl_col_session_t *s = COL_SESSION(f->active->session);
    CHECK(b->pending && b->ordinal == 1 && b->session == (uintptr_t)s
        && s->diff_txn_count == 1 && b->control == (uintptr_t)f->active->control
        && b->control_identity ==
        wl_evaluation_control_identity(f->active->control)
        && b->attempt ==
        wl_evaluation_control_active_attempt_id(f->active->control)
        && b->attempt == f->bindings->attempt_id &&
        b->generation == f->bindings->generation
        && b->plan == (uintptr_t)f->selected && b->operation == (uintptr_t)op
        && b->occurrence == (unsigned)(op - f->selected->ops)
        && b->source == (uintptr_t)ctx->join_right.descriptor
        && wl_columnar_relation_snapshot_equal(b->snapshot,
        wl_columnar_relation_snapshot(ctx->join_right.descriptor))
        && b->slot == 0 && b->key_count == op->key_count && b->key == 0
        && !strcmp(op->right_keys[0],
        "col0") && b->rows == ctx->join_right.nrows
        && row == b->inserted && row < b->rows);
    ++b->inserted;
    if (b->inserted != b->rows) return;
    CHECK(row == 1 && b->inserted == 2);
    b->pending = false; ++f->seen.refill_builds;
    if (f->failed_build_attempt)
        fprintf(stdout, "refill retry old=%llu new=%llu rows=%u\n",
            (unsigned long long)f->failed_build_attempt,
            (unsigned long long)b->attempt, b->inserted);
    if (f->cycle_fault != 3 || !f->armed || f->stopped) return;
    f->stopped = true; f->failed_build_attempt = b->attempt;
    fprintf(stdout,
        "refill build attempt=%llu generation=%llu slot=%u rows=%u ordinal=%u\n",
        (unsigned long long)b->attempt, (unsigned long long)b->generation,
        b->slot, b->inserted, b->ordinal);
    if (f->work_stop)
        CHECK(wl_evaluation_control_charge_attempt(f->active->control,
            b->attempt, UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(f->active->control);
}
static void
wl_test_join_cache_cycle_arrangement(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    (void)progress;
    fixture_t *f = active;
    if (control != f->active->control || (phase != 13 && phase != 14)) return;
    wl_columnar_filter_context_t *ctx = context(f);
    CHECK(!wl_test_join_cache_cycle_sources(f, ctx) && !f->profile
        && ctx->source.nrows && ctx->join_right.nrows
        && !ctx->join_operation->right_filter_expr.size);
    if (phase == 14) {
        ++f->seen.builds; f->building = ctx->join_operation;
        if (!f->profile && f->stage == 6 && f->head < 2)
            wl_test_join_cache_cycle_build_capture(f, ctx, progress);
    } else {
        f->cycle_build.pending = false;
        ++f->seen.reuses; f->building = NULL;
    }
}
static void
wl_test_join_cache_cycle_join(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    fixture_t *f = active;
    if (control != f->active->control) return;
    if (phase != 13 && phase != 15 && phase != 18 && phase != 28
        && phase != 29 && phase != 44 && phase != 47 && phase != 48) return;
    wl_columnar_filter_context_t *ctx = context(f);
    bool overridden = wl_test_join_cache_cycle_sources(f, ctx);
    if (phase == 13) CHECK(ctx->width == 2);
    if (phase == 13 && progress == ctx->width - 1u) {
        ++f->seen.schema[f->head];
        f->seen.empty_left[f->head] += !ctx->source.nrows;
        f->seen.empty_right[f->head] += !ctx->join_right.nrows;
        f->seen.overrides[f->head] += overridden;
        if (!f->profile && f->armed && !f->stopped &&
            ((f->cycle_fault == 1 && f->stage == 2 && f->head < 2)
            || (f->cycle_fault == 2 && f->stage == 1 && f->head == 4))) {
            CHECK(!ctx->output && ctx->source.nrows && !overridden);
            if (f->cycle_fault == 1)
                CHECK(!ctx->join_right.nrows
                    && !ctx->join_operation->right_filter_expr.size);
            else
                CHECK(ctx->join_right.nrows == 1
                    && ctx->join_operation->right_filter_expr.size
                    && f->seen.selected[4] == 1 && f->seen.false_rows[4] == 1
                    && !f->seen.private_head[4] && !f->seen.probes[4]
                    && f->seen.builds && f->seen.indexed);
            f->stopped = true;
            if (f->work_stop)
                CHECK(wl_evaluation_control_charge_attempt(control,
                    wl_evaluation_control_active_attempt_id(control),
                    UINT64_MAX)
                    == WL_EVALUATION_CONTROL_WORK_LIMIT);
            else wl_evaluation_control_request_cancel(control);
        }
    }
    if (phase == 15) ++f->seen.sources[f->head];
    if (phase == 18) ++f->seen.gathers[f->head];
    if (phase == 29) ++f->seen.probes[f->head];
    if (phase == 28) {
        ++f->seen.private_rows; ++f->seen.private_head[f->head];
    }
    if (phase == 44) {
        CHECK(f->head == 4 && ctx->join_operation->right_filter_expr.size
            && progress < ctx->join_right.nrows);
        ++f->seen.selected[f->head];
        if (!ctx->join_right.columns[2][progress]) {
            CHECK(ctx->join_right.columns[0][progress] == 2
                && ctx->join_right.columns[1][progress] == 7);
            ++f->seen.false_rows[f->head];
        }
    }
    if (phase == 47) {
        if (ctx->join_operation->right_filter_expr.size) {
            CHECK(!ctx->join_right.nrows); /* Empty-filter recipe, not index work. */
        } else {
            CHECK(!overridden && f->building == ctx->join_operation
                && ctx->join_right.nrows && progress < ctx->join_right.nrows);
            ++f->seen.indexed;
            if (!f->profile && f->stage == 6 && f->head < 2)
                wl_test_join_cache_cycle_build_row(f, ctx, progress);
        }
    }
    if (phase == 48) {
        CHECK(!f->profile && !overridden
            && !ctx->join_operation->right_filter_expr.size);
        ++f->seen.persistent;
    }
}
static void watch(fixture_t *f, unsigned profile, bool enabled) {
    active = f;
    f->profile = profile;
    f->active = &f->profiles[profile];
    memset(&f->seen, 0, sizeof(f->seen));
    memset(&f->cycle_build, 0, sizeof(f->cycle_build));
    f->build_ordinal = 0;
    memset(f->prefix, 0, sizeof(f->prefix));
    memset(f->start, 0, sizeof(f->start));
    memset(f->build_base, 0, sizeof(f->build_base));
    memset(f->build_calls, 0, sizeof(f->build_calls));
    memset(f->allocation_calls, 0, sizeof(f->allocation_calls));
    f->allocation_attempt = 0;
    f->allocation_phase = 0; f->allocation_progress = 0;
    wl_columnar_arrangement_test_diff_fail_allocation =
        enabled && !profile && f->index_allocation
        ? wl_test_join_cache_index_allocation : NULL;
    f->bindings = NULL;
    f->selected = NULL;
    f->building = NULL;
    f->append = NULL;
    f->append_op = NULL;
    wl_columnar_session_test_observed_transaction_checkpoint =
        enabled ? observed : NULL;
    wl_columnar_arrangement_test_diff_checkpoint =
        enabled ?
        (f->cycle ? wl_test_join_cache_cycle_arrangement : arrangement) : NULL;
    wl_columnar_join_test_attempt_checkpoint =
        enabled ? (f->cycle ? wl_test_join_cache_cycle_join : join) : NULL;
}
static void left_insert(fixture_t *f, unsigned x, unsigned z) {
    int64_t row[] = {f->tag, f->floating, x, z};
    for (unsigned p = 0; p < 3; ++p)
        CHECK(!wl_session_insert(f->profiles[p].session, "left",
            row + (f->compound ? 0 : 2), 1,
            f->compound ? 4 : 2));
    f->left[x][z] = true;
}
static void right_change(fixture_t *f, unsigned x, unsigned y, unsigned flag,
    bool remove) {
    int64_t row[] = {x, y, flag};
    for (unsigned p = 0; p < 3; ++p) {
        int rc =
            remove ? wl_session_remove(f->profiles[p].session, "right", row, 1,
                3)
               : wl_session_insert(f->profiles[p].session, "right", row, 1, 3);
        CHECK(!rc);
    }
    f->right[x][y][flag] = !remove;
}
static void script(fixture_t *f) {
    switch (f->stage) {
    case 0:
        left_insert(f, 1, 1);
        left_insert(f, 2, 2);
        right_change(f, 1, 1, 1, false);
        right_change(f, 2, 2, 1, false);
        right_change(f, 1, 2, 1, false);
        right_change(f, 4, 4, 1, false);
        right_change(f, 1, 2, 0, false);
        right_change(f, 2, 7, 0, false);
        break;
    case 1:
    case 4:
        left_insert(f, 4, 4);
        break;
    case 2:
        right_change(f, 4, 2, 1, false);
        break;
    case 3:
        right_change(f, 4, 2, 1, true);
        break;
    case 5:
        left_insert(f, 9, 9);
        right_change(f, 9, 9, 1, false);
        break;
    case 6:
        right_change(f, 9, 9, 1, true);
        break;
    case 7:
        CHECK(f->index_allocation >=
            WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER);
        left_insert(f, 8, 8); /* Unmatched append must really reuse after recovery. */
        break;
    default:
        CHECK(false);
    }
}
static void validate_plan(fixture_t *f) {
    unsigned found = 0, offset = f->compound ? 2 : 0, width = offset + 2;
    for (uint32_t s = 0; s < f->plan->stratum_count; ++s)
        for (uint32_t h = 0; h < f->plan->strata[s].relation_count; ++h) {
            const wl_plan_relation_t *r = &f->plan->strata[s].relations[h];
            unsigned n = head_number(r->name), joins = 0;
            found |= 1u << n;
            for (uint32_t i = 0; i < r->op_count; ++i) {
                const wl_plan_op_t *o = &r->ops[i];
                if (o->op != WL_PLAN_OP_JOIN)
                    continue;
                ++joins;
                CHECK(!o->materialized && !o->opaque_data &&
                    !strcmp(o->right_relation, "right") &&
                    o->key_count == (n == 3 ? 2u : 1u) &&
                    (o->right_filter_expr.size != 0) == (n == 4) &&
                    o->project_count == 2 && o->project_indices);
                for (uint32_t k = 0; k < o->key_count; ++k) {
                    char left[16], right[16];
                    snprintf(left, sizeof(left), "col%u",
                        offset + (n == 2 ? 1 : k));
                    snprintf(right, sizeof(right), "col%u", n == 2 ? 1 : k);
                    CHECK(!strcmp(o->left_keys[k], left) &&
                        !strcmp(o->right_keys[k], right));
                }
                CHECK(o->project_indices[0] == (n == 2 ? width : offset) &&
                    o->project_indices[1] ==
                    (n == 2 || n == 3 ? offset + 1 : width + 1));
            }
            CHECK(joins == 1);
        }
    CHECK(found == (1u << HEADS) - 1u);
}
static void check_work(fixture_t *f, const registry_t *before) {
    registry_t now = registry(f->active);
    for (unsigned h = 0; h < HEADS; ++h)
        CHECK(f->seen.sources[h] && f->seen.probes[h]);
    CHECK(f->seen.private_rows); /* genuine filtered sibling, also on warm FULL */
    if (rhs_delta(f)) {
        CHECK(!f->seen.builds && !f->seen.reuses && !f->seen.indexed &&
            !f->seen.persistent);
        for (unsigned h = 0; h < HEADS; ++h)
            CHECK(f->seen.append_reads[h]);
        same_registry(before, &now, false);
    } else {
        CHECK(f->seen.rejected);
        if (!f->profile) {
            bool warm = f->stage == 1 || f->stage == 4 || f->stage == 7;
            CHECK(f->seen.builds == (warm ? 0u : 3u) &&
                f->seen.reuses == (warm ? 4u : 1u) && f->seen.persistent);
            if (warm) {
                CHECK(!f->seen.indexed);
                same_registry(before, &now, false);
            } else
                CHECK(f->seen.indexed);
            CHECK(now.count == 3);
            const col_rel_t *r =
                session_find_rel(COL_SESSION(f->active->session), "right");
            for (unsigned k = 0; k < 3; ++k)
                CHECK(now.entries[k].rows == r->nrows &&
                    now.entries[k].current == r->nrows &&
                    wl_columnar_relation_snapshot_equal(
                        now.entries[k].snapshot,
                        wl_columnar_relation_snapshot(r)));
        } else
            CHECK(!now.count && !f->seen.builds && !f->seen.reuses &&
                !f->seen.persistent);
    }
}
static void
wl_test_join_cache_cycle_script(fixture_t *f)
{
    switch (f->stage) {
    case 0:
        left_insert(f, 1, 1); left_insert(f, 2, 2);
        right_change(f, 1, 1, 1, false);
        right_change(f, 2, 2, 1, false);
        right_change(f, 2, 7, 0, false);
        break;
    case 1:
        right_change(f, 1, 1, 1, true);
        right_change(f, 2, 2, 1, true);
        break;
    case 2: right_change(f, 2, 7, 0, true); break;
    case 3: right_change(f, 1, 1, 1, false); break;
    case 4:
        /* This independent script inserts each fact once before removal. */
        for (unsigned x = 1; x <= 2; ++x) {
            int64_t row[] = {f->tag, f->floating, x, x};
            CHECK(f->left[x][x]);
            for (unsigned p = 0; p < 3; ++p)
                CHECK(!wl_session_remove(f->profiles[p].session, "left",
                    row + (f->compound ? 0 : 2), 1, f->compound ? 4 : 2));
            f->left[x][x] = false;
        }
        break;
    case 5: right_change(f, 2, 2, 1, false); break;
    case 6: left_insert(f, 1, 1); left_insert(f, 2, 2); break;
    case 7: left_insert(f, 4, 4); break;
    case 8: right_change(f, 4, 4, 1, false); break;
    case 9: right_change(f, 4, 4, 1, true); break;
    default: CHECK(false);
    }
}
static void
wl_test_join_cache_cycle_work(fixture_t *f, const registry_t *before)
{
    bool empty = f->stage == 2 || f->stage == 4 || f->stage == 5;
    bool override = !f->profile && !f->compound && !f->route
        && (f->stage == 3 || f->stage == 5 || f->stage == 8);
    registry_t now = registry(f->active);
    for (unsigned h = 0; h < HEADS; ++h) {
        CHECK(f->seen.schema[h] == 1
            && f->seen.empty_right[h] == (f->stage == 2 ? 1u : 0u)
            && f->seen.empty_left[h] == (f->stage == 4 ||
            f->stage == 5 ? 1u : 0u)
            && f->seen.overrides[h] == (override ? 1u : 0u));
        if (empty || (h == 4 && f->stage == 1))
            CHECK(!f->seen.sources[h] && !f->seen.probes[h]
                && !f->seen.gathers[h] && !f->seen.private_head[h]);
        else CHECK(f->seen.sources[h] && f->seen.probes[h]);
    }
    CHECK(f->seen.false_rows[4] == (f->stage <= 1 ? 1u : 0u));
    if (f->stage == 1) CHECK(f->seen.selected[4] == 1);
    if (empty) {
        CHECK(!f->seen.builds && !f->seen.reuses && !f->seen.indexed
            && !f->seen.persistent && !f->seen.private_rows);
        same_registry(before, &now, true);
    } else if (!f->profile && !override) {
        bool warm = f->stage == 7;
        CHECK(f->seen.builds == (warm ? 0u : 3u)
            && f->seen.reuses == (warm ? 4u : 1u) && now.count == 3);
        if (warm) {
            CHECK(!f->seen.indexed); same_registry(before, &now, false);
        } else CHECK(f->seen.indexed);
        col_rel_t *r = session_find_rel(COL_SESSION(f->active->session),
                "right");
        for (unsigned k = 0; k < 3; ++k)
            CHECK(now.entries[k].rows == r->nrows &&
                now.entries[k].current == r->nrows
                && wl_columnar_relation_snapshot_equal(now.entries[k].snapshot,
                wl_columnar_relation_snapshot(r)));
    } else {
        CHECK(!f->seen.builds && !f->seen.reuses && !f->seen.indexed
            && !f->seen.persistent);
        if (!f->profile) same_registry(before, &now, true);
        else CHECK(!now.count);
    }
    if (!f->profile && f->stage == 6) CHECK(f->seen.refill_builds == 1);
    fprintf(stdout,
        "cycle stage=%u profile=%u builds=%u reuse=%u override=%u\n",
        f->stage, f->profile, f->seen.builds, f->seen.reuses, override);
}
static void
wl_test_join_cache_cycle_failure(fixture_t *, const registry_t *,
    const image_t *);
static void writers(profile_t *);
static void
wl_test_join_cache_cycle_run(fixture_t *f)
{
    image_t old = {0};
    for (f->stage = 0; f->stage < 10; ++f->stage) {
        wl_test_join_cache_cycle_script(f);
        image_t now = reference(f);
        if (f->stage == 1) CHECK(now.rows[0][2][7] && !now.rows[4][2][7]);
        for (unsigned p = 0; p < 3; ++p) {
            registry_t before = registry(&f->profiles[p]);
            watch(f, p, p < 2);
            if (!p && f->cycle_fault && !f->stopped
                && f->stage ==
                (f->cycle_fault == 1 ? 2u : f->cycle_fault == 2 ? 1u : 6u)) {
                wl_test_join_cache_cycle_failure(f, &before, &old);
                watch(f, p, true);
            }
            CHECK(!finish(f, &f->profiles[p]));
            if (!p && f->stopped) writers(&f->profiles[p]);
            if (p < 2) wl_test_join_cache_cycle_work(f, &before);
            watch(f, p, false);
            check_delivery(f, &f->profiles[p], &old, &now);
            CHECK(!finish(f, &f->profiles[p]));
            check_delivery(f, &f->profiles[p], &now, &now);
        }
        old = now;
    }
}
typedef struct {
    const col_rel_t *relation, *owner;
    col_relation_snapshot_t snapshot;
    unsigned width, rows, kind, arity_len, offset;
    bool explicit_types;
    uint32_t arity[3];
    wirelog_column_type_t types[4];
    int64_t cells[INDEX_ROWS][4];
} input_image_t;
static input_image_t
input_image(profile_t *p, const char *name)
{
    input_image_t image = {0};
    const col_rel_t *r = session_find_rel(COL_SESSION(p->session), name);
    CHECK(r && r->ncols <= 4 && r->nrows <= INDEX_ROWS &&
        r->compound_arity_len <= 3);
    image.relation = r; image.owner = r->storage_owner;
    image.snapshot = wl_columnar_relation_snapshot(r);
    image.width = r->ncols; image.rows = r->nrows;
    image.kind = r->compound_kind; image.arity_len = r->compound_arity_len;
    image.offset = r->inline_physical_offset;
    image.explicit_types = r->column_types != NULL;
    for (unsigned c = 0; c < image.arity_len;
        ++c) image.arity[c] = r->compound_arity_map[c];
    for (unsigned c = 0; c < image.width; ++c) {
        image.types[c] =
            r->column_types ? r->column_types[c] : WIRELOG_TYPE_INT64;
        for (unsigned row = 0; row < image.rows;
            ++row) image.cells[row][c] = col_rel_get(r, row, c);
    }
    return image;
}
static void
same_input(const input_image_t *a, const input_image_t *b)
{
    CHECK(a->relation == b->relation && a->owner == b->owner
        && wl_columnar_relation_snapshot_equal(a->snapshot, b->snapshot)
        && a->width == b->width && a->rows == b->rows && a->kind == b->kind
        && a->arity_len == b->arity_len && a->offset == b->offset
        && a->explicit_types == b->explicit_types);
    for (unsigned c = 0; c < a->arity_len;
        ++c) CHECK(a->arity[c] == b->arity[c]);
    for (unsigned c = 0; c < a->width; ++c) {
        CHECK(a->types[c] == b->types[c]);
        for (unsigned row = 0; row < a->rows;
            ++row) CHECK(a->cells[row][c] == b->cells[row][c]);
    }
}
static void
same_report(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b)
{
    CHECK(a->attempt_id == b->attempt_id && a->limit == b->limit
        && a->consumed == b->consumed && a->stop_reason == b->stop_reason
        && a->execution_status == b->execution_status
        && a->cleanup_status == b->cleanup_status && a->result == b->result);
}
static void
writers(profile_t *p)
{
    const char *const inputs[] = {"left", "right"};
    for (unsigned i = 0; i < 2; ++i) {
        col_rel_t *live = session_find_rel(COL_SESSION(p->session), inputs[i]);
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(live && !col_rel_source_writer_acquire(live, &writer)
            && !wl_columnar_source_access_writer_release(&writer));
    }
}
static void check_pin(fixture_t *f) {
    profile_t *p = &f->profiles[0];
    col_rel_t *r = session_find_rel(COL_SESSION(p->session), "right");
    const col_diff_arrangement_pin_t *pin = &f->pin;
    CHECK(pin->active && pin->session == COL_SESSION(p->session)
        && pin->source == r && pin->entry->diff_arr == pin->arr
        && pin->entry->pin_count == 1 &&
        pin->generation == pin->entry->generation
        && wl_columnar_relation_snapshot_equal(pin->source_snapshot,
        wl_columnar_relation_snapshot(r))
        && wl_columnar_relation_snapshot_equal(pin->arr->source_snapshot,
        pin->source_snapshot)
        && pin->source_reader.identity == (uintptr_t)&pin->source_reader
        && pin->source_reader.owner == &r->storage_owner->source_access
        && pin->source_reader.secondary_owner == &r->descriptor_access);
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(col_rel_source_writer_acquire(r, &writer) == EBUSY);
}
static wl_columnar_session_test_completed_view_t
completion(profile_t *p, unsigned route,
    const wl_evaluation_control_report_t *report)
{
    wl_columnar_session_test_completed_view_t v;
    CHECK(!wl_columnar_session_test_completed_view(COL_SESSION(p->session),
        &v));
    CHECK(!v.transaction_present && !v.main_pending && !v.parent_pending
        && !v.actor_current && !v.invocation_accepted && !v.backend_active
        && !v.endpoint_busy && !v.source_ready && !v.ack_paid && !v.ack_prepared
        && !v.finance_kind && !v.finance_consumed && !v.local_reset_completed
        && !v.report.attempt_id);
    if (route == 2) {
        /* INERT: Snapshot has its distinct retryable public boundary. */
        CHECK(p->session->backend->session_controlled_step_state(p->session)
            == WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE && v.phase == 0
            && !v.ticket && !v.operation_identity && !v.original_attempt_id);
    } else {
        /* RESERVED(1), not WAIT_SOURCE(2): fixed descriptive metadata owns
         * no resource/reference and does not make completed_live true. */
        CHECK(p->session->backend->session_controlled_step_state(p->session)
            == WL_CONTROLLED_STEP_PENDING && v.phase == 1
            && v.ticket && v.operation_identity && v.original_attempt_id
            && v.original_attempt_id == report->attempt_id);
    }
    return v;
}
static void
protected_step(fixture_t *f)
{
    profile_t *p = f->active;
    const int64_t left[] = {f->tag, f->floating, 1, 1}, right[] = {1, 1, 1};
    CHECK(wl_session_insert(p->session, "left", left + (f->compound ? 0 : 2),
        1, f->compound ? 4 : 2) == EBUSY);
    CHECK(wl_session_remove(p->session, "right", right, 1, 3) == EBUSY);
    image_t unexpected = {0};
    CHECK(wl_session_snapshot(p->session, snapshot_tuple,
        &unexpected) == EBUSY);
    for (unsigned h = 0; h < HEADS; ++h)
        for (unsigned x = 0; x < VALUES; ++x)
            for (unsigned y = 0; y < VALUES;
                ++y) CHECK(!unexpected.rows[h][x][y]);
    /* CLOSE may legitimately consume this bookkeeping. Do not probe it by
     * destroying the session needed for the same-input recovery history. */
}
static void
failure(fixture_t *f, const registry_t *before, const image_t *old,
    int expected)
{
    profile_t *p = f->active;
    wl_col_session_t *s = COL_SESSION(p->session);
    const col_rel_t *heads[HEADS];
    for (unsigned h = 0; h < HEADS;
        ++h) heads[h] = session_find_rel(s, names[h]);
    input_image_t left = input_image(p, "left"),
        right = input_image(p, "right");
    f->armed = true;
    int rc = finish(f, p);
    CHECK(f->stopped && rc == expected && s->pending_input_change);
    f->armed = false;
    registry_t after = registry(p);
    same_registry(before, &after, true);
    content(p, old);
    for (unsigned h = 0; h < HEADS; ++h) {
        CHECK(session_find_rel(s, names[h]) == heads[h]);
        for (unsigned x = 0; x < VALUES; ++x)
            for (unsigned y = 0; y < VALUES; ++y)
                CHECK(!p->events.calls[h][x][y] && !p->events.diff[h][x][y]
                    && !p->tuples.rows[h][x][y]);
    }
    wl_evaluation_control_report_t report, frozen;
    CHECK(!wl_evaluation_control_report(p->control, &report)
        && report.result == expected
        && (expected > 0 ? !report.stop_reason : report.stop_reason == expected)
        && report.consumed && !report.cleanup_status
        && !wl_evaluation_control_active_attempt_id(p->control)
        && !s->cleanup_active && !s->cleanup_pending &&
        !s->cleanup_reserved_bytes);
    if (f->index_allocation) {
        CHECK(expected == ENOMEM && f->injected_allocations == 1
            && f->allocation_attempt == f->failed_allocation_attempt
            && !s->memory_budget_denied && !s->diff_txn_count
            && wl_columnar_memory_denials(
                wl_columnar_memory_governor_ref_get(p->governor)) ==
            f->denials);
        wl_columnar_arrangement_test_diff_fail_allocation = NULL;
    }
    if (f->resource) {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(p->governor);
        CHECK(s->memory_budget_denied == (f->resource != 3));
        if (f->resource == 3) {
            CHECK(f->allocation_failed &&
                !wl_columnar_join_test_attempt_fail_allocation
                && wl_columnar_memory_denials(g) == f->denials);
            wl_columnar_join_test_attempt_fail_allocation = -1;
        } else {
            /* Capacity stays exhausted through completed failure/cleanup0. */
            CHECK(wl_columnar_memory_denials(g) > f->denials
                && atomic_load_explicit(&f->peer.state, memory_order_acquire)
                == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
            uint64_t held = wl_columnar_memory_reserved(g),
                bytes = f->peer.bytes;
            CHECK(held >= bytes && wl_columnar_memory_rollback(&f->peer)
                && wl_columnar_memory_reserved(g) == held - bytes
                && atomic_load_explicit(&f->peer.state, memory_order_acquire)
                == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED);
        }
    }
    /* These ordinary failures dispose private transactions/readers, including
     * WORK, unlike the retained recursive WORK case. */
    uint64_t owner[8];
    CHECK(!wl_columnar_session_test_retained_work_owner(s, owner)
        && !owner[0] && !owner[1] && !owner[2]);
    wl_columnar_session_test_completed_view_t slot = completion(p, f->route,
            &report);
    if (f->route != 2) protected_step(f);
    if (f->pin_mode) {
        check_pin(f);
        col_diff_arrangement_pin_release(&f->pin);
        CHECK(!f->pin.active && !f->pin.entry && !f->pin.source_reader.owner);
        registry_t released = registry(p);
        same_registry(&f->unpinned, &released, true);
    }
    writers(p);
    content(p, old);
    CHECK(s->pending_input_change);
    for (unsigned h = 0; h < HEADS;
        ++h) CHECK(session_find_rel(s, names[h]) == heads[h]);
    input_image_t current = input_image(p, "left"); same_input(&left, &current);
    current = input_image(p, "right"); same_input(&right, &current);
    after = registry(p);
    same_registry(f->pin_mode ? &f->unpinned : before, &after, true);
    CHECK(!wl_evaluation_control_report(p->control, &frozen));
    same_report(&report, &frozen);
    CHECK(!wl_evaluation_control_active_attempt_id(p->control));
    wl_columnar_session_test_completed_view_t unchanged = completion(p,
            f->route, &report);
    CHECK(slot.phase == unchanged.phase && slot.ticket == unchanged.ticket
        && slot.operation_identity == unchanged.operation_identity
        && slot.original_attempt_id == unchanged.original_attempt_id);
    same_report(&slot.report, &unchanged.report);
    /* completion() separately reasserts every inactive flag on both reads. */
    fprintf(stdout,
        "site=%u work=%u cap=%u resource=%u route=%u state=%u disposed private index and readers\n",
        f->stop_site, f->work_stop, f->cap, f->resource, f->route,
        p->session->backend->session_controlled_step_state(p->session));
    CHECK(!wl_evaluation_control_reset(p->control));
}
static void
wl_test_join_cache_cycle_failure(fixture_t *f, const registry_t *before,
    const image_t *old)
{
    profile_t *p = f->active;
    wl_col_session_t *s = COL_SESSION(p->session);
    const col_rel_t *heads[HEADS];
    for (unsigned h = 0; h < HEADS;
        ++h) heads[h] = session_find_rel(s, names[h]);
    input_image_t left = input_image(p, "left"),
        right = input_image(p, "right");
    f->armed = true;
    int expected = f->work_stop ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : WL_EVALUATION_CONTROL_CANCELLED;
    CHECK(finish(f, p) == expected && f->stopped && s->pending_input_change);
    f->armed = false;
    content(p, old);
    for (unsigned h = 0; h < HEADS; ++h) {
        CHECK(session_find_rel(s, names[h]) == heads[h]);
        for (unsigned x = 0; x < VALUES; ++x)
            for (unsigned y = 0; y < VALUES; ++y)
                CHECK(!p->events.calls[h][x][y] && !p->events.diff[h][x][y]
                    && !p->tuples.rows[h][x][y]);
    }
    registry_t after = registry(p); same_registry(before, &after, true);
    wl_evaluation_control_report_t report, frozen;
    CHECK(!wl_evaluation_control_report(p->control, &report)
        && report.result == expected && report.stop_reason == expected
        && report.consumed && !report.cleanup_status
        && !wl_evaluation_control_active_attempt_id(p->control)
        && !s->cleanup_active && !s->cleanup_pending &&
        !s->cleanup_reserved_bytes);
    uint64_t owner[8], again[8];
    CHECK(!wl_columnar_session_test_retained_work_owner(s, owner));
    fprintf(stdout,
        "cycle fault=%u work=%u route=%u owner=%llu/%llu/%llu phase=%llu gen=%llu/%llu finance=%llu drained=%llu\n",
        f->cycle_fault, f->work_stop, f->route,
        (unsigned long long)owner[0], (unsigned long long)owner[1],
        (unsigned long long)owner[2], (unsigned long long)owner[3],
        (unsigned long long)owner[4], (unsigned long long)owner[5],
        (unsigned long long)owner[6], (unsigned long long)owner[7]);
    bool retained = f->cycle_fault == 1 && f->work_stop && f->route != 2;
    if (retained) {
        /* Actual acyclic EVALUATE(8) parent, without a started index image. */
        CHECK(owner[0] && owner[1] && owner[2] && owner[3] == 8
            && owner[4] && owner[4] == owner[5] && owner[7]
            && p->session->backend->session_controlled_step_state(p->session)
            == WL_CONTROLLED_STEP_PENDING);
    } else {
        CHECK(!owner[0] && !owner[1] && !owner[2]);
        (void)completion(p, f->route, &report);
    }
    const char *const inputs[] = {"left", "right"};
    for (unsigned i = 0; i < 2; ++i) {
        col_rel_t *r = session_find_rel(s, inputs[i]);
        wl_columnar_source_access_writer_t writer = {0};
        int rc = col_rel_source_writer_acquire(r, &writer);
        fprintf(stdout, "cycle writer=%s rc=%d retained=%u\n", inputs[i], rc,
            retained);
        CHECK(rc == (retained ? EBUSY : 0));
        if (!retained) CHECK(!wl_columnar_source_access_writer_release(
                    &writer));
    }
    if (f->route != 2) protected_step(f);
    content(p, old);
    CHECK(s->pending_input_change);
    for (unsigned h = 0; h < HEADS;
        ++h) CHECK(session_find_rel(s, names[h]) == heads[h]);
    input_image_t current = input_image(p, "left"); same_input(&left, &current);
    current = input_image(p, "right"); same_input(&right, &current);
    after = registry(p); same_registry(before, &after, true);
    CHECK(!wl_evaluation_control_report(p->control, &frozen));
    same_report(&report, &frozen);
    CHECK(!wl_columnar_session_test_retained_work_owner(s, again));
    for (unsigned i = 0; i < 8; ++i) CHECK(owner[i] == again[i]);
    CHECK(!wl_evaluation_control_reset(p->control));
}
int main(int argc, char **argv) {
    CHECK(argc == 3 || argc == 4);
    fixture_t f = {0};
    f.compound = !strcmp(argv[1], "inline");
    CHECK(f.compound || !strcmp(argv[1], "scalar"));
    f.route = !strcmp(argv[2], "observed") ? 0
            : !strcmp(argv[2], "plain")  ? 1
                                         : 2;
    CHECK(f.route != 2 || !strcmp(argv[2], "snapshot"));
    if (argc == 4) {
        const char *const modes[] = {"reuse-cancel", "reuse-work",
                                     "rebuild-cancel",
                                     "rebuild-work", "probe-cancel",
                                     "probe-work"};
        for (unsigned i = 0; i < 6; ++i) if (!strcmp(argv[3], modes[i])) {
                f.stop_site = i / 2 + 1; f.work_stop = (i % 2) != 0;
            }
        if (!strncmp(argv[3], "cap", 3) && strlen(argv[3]) == 4
            && argv[3][3] >= '4' && argv[3][3] <= '6')
            f.cap = (unsigned)(argv[3][3] - '0');
        if (!strcmp(argv[3], "image-denied")) f.resource = 1;
        if (!strcmp(argv[3], "build-denied")) f.resource = 2;
        if (!strcmp(argv[3], "key-nomem")) f.resource = 3;
        if (!strcmp(argv[3], "index-image-nomem"))
            f.index_allocation = WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_IMAGE;
        if (!strcmp(argv[3], "index-slots-nomem"))
            f.index_allocation = WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_SLOTS;
        const char *const metadata_modes[] = {"index-first-name-nomem",
                                              "index-first-keys-nomem",
                                              "index-second-name-nomem",
                                              "index-second-keys-nomem"};
        for (unsigned i = 0; i < 4;
            ++i) if (!strcmp(argv[3], metadata_modes[i])) {
                f.index_allocation = (i %
                    2) ? WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS
                : WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NAME;
                f.allocation_ordinal = i / 2 + 1;
            }
        const char *const build_modes[] = {"index-header-nomem",
                                           "index-keys-nomem",
                                           "index-heads-nomem",
                                           "index-next-nomem"};
        for (unsigned i = 0; i < 4; ++i) if (!strcmp(argv[3], build_modes[i]))
                f.index_allocation =
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER + i;
        const char *const new_modes[] = {"index-new-key0-name",
                                         "index-new-key0-keys",
                                         "index-new-key1-name",
                                         "index-new-key1-keys",
                                         "index-new-pair-name",
                                         "index-new-pair-keys"};
        for (unsigned i = 0; i < 6; ++i) if (!strcmp(argv[3], new_modes[i])) {
                f.new_metadata = i / 2 + 1;
                f.allocation_ordinal = f.new_metadata;
                f.index_allocation = (i %
                    2) ? WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_METADATA_KEYS
                : WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_NAME;
            }
        const char *const later_modes[] = {"index-key1-header",
                                           "index-key1-keys",
                                           "index-key1-heads",
                                           "index-key1-next"};
        for (unsigned i = 0; i < 4; ++i) if (!strcmp(argv[3], later_modes[i])) {
                f.later_signature = 2; f.allocation_ordinal = 2;
                f.index_allocation =
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER + i;
            }
        f.pin_mode = !strcmp(argv[3], "pin-held");
        f.cycle = !strcmp(argv[3], "empty-cycle");
        const char *const cycle_modes[] = {"empty-right-cancel",
                                           "empty-right-work",
                                           "rejected-cancel", "rejected-work",
                                           "refill-cancel", "refill-work"};
        for (unsigned i = 0; i < 6; ++i) if (!strcmp(argv[3], cycle_modes[i])) {
                f.cycle = true; f.cycle_fault = i / 2 + 1;
                f.work_stop = (i % 2) != 0;
            }
        CHECK(f.stop_site || f.cap || f.resource || f.pin_mode || f.cycle ||
            f.index_allocation);
    }
    const char *env[] = {"WIRELOG_DIFF_ENABLED", "WIRELOG_JOIN_BATCH_BYTES",
                         "WIRELOG_JOIN_BATCH_STRICT",
                         "WIRELOG_JOIN_OUTPUT_LIMIT",
                         "WIRELOG_SPILL_JOIN_BATCH_BYTES"};
    for (unsigned i = 0; i < sizeof(env) / sizeof(env[0]); ++i)
        environment(env[i]);
    char source[2048];
    const char *decl =
        f.compound ? "p:pair(symbol,float) inline,x:int64,z:int64"
                                : "x:int64,z:int64";
    const char *prefix = f.compound ? "pair(t,v)," : "";
    snprintf(source, sizeof(source),
        ".decl left(%s)\n.decl right(x:int64,y:int64,flag:int64)\n"
        ".decl first(x:int64,y:int64)\n.decl mirror(x:int64,y:int64)\n"
        ".decl reverse(x:int64,y:int64)\n.decl "
        "exact(x:int64,y:int64)\n.decl filtered(x:int64,y:int64)\n"
        "first(x,y):-left(%sx,z),right(x,y,f).\n"
        "mirror(x,y):-left(%sx,z),right(x,y,f).\n"
        "reverse(x,y):-left(%sz,y),right(x,y,f).\n"
        "exact(x,y):-left(%sx,y),right(x,y,f).\n"
        "filtered(x,y):-left(%sx,z),right(x,y,1).\n",
        decl, prefix, prefix, prefix, prefix, prefix);
    wirelog_error_t error;
    f.program = wirelog_parse_string(source, &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    validate_plan(&f);
    f.tag = wl_intern_put(f.plan->intern, "tag");
    f.floating = wl_columnar_float_to_bits(1.5);
    CHECK(f.tag >= 0);
    for (unsigned i = 0; i < 3; ++i) {
        profile_t *p = &f.profiles[i];
        wl_session_options_t options;
        wl_session_options_init(&options);
        if (i < 2) {
            CHECK(!wl_evaluation_control_create(UINT64_C(1000000000),
                &p->control));
            options.evaluation_control = p->control;
        }
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
        };
        p->governor = wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(p->governor);
        options.memory_governor = p->governor;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
            &options, &p->session));
        if (i == 0) CHECK(COL_SESSION(p->session)->diff_enabled);
        else COL_SESSION(p->session)->diff_enabled = false;
        if (!f.route)
            wl_session_set_delta_cb(p->session, capture, &p->events);
    }
    image_t old = {0};
    /* Plain STEP first uses the legacy frame. Establish an empty session
     * before mutating BOTH sides, so the first nonempty call naturally uses
     * the public transaction without a single-source append shortcut. */
    for (unsigned p = 0; p < 3; ++p) {
        CHECK(!finish(&f, &f.profiles[p]));
        CHECK(!registry(&f.profiles[p]).count);
    }
    if (f.cycle) wl_test_join_cache_cycle_run(&f);
    for (f.stage = 0; !f.cycle && f.stage < (f.index_allocation >=
        WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER ? 8u : 7u); ++f.stage) {
        if (f.stage == 1 && f.pin_mode) {
            profile_t *p = &f.profiles[0];
            f.unpinned = registry(p);
            uint32_t key = 0;
            col_rel_t *r = session_find_rel(COL_SESSION(p->session),
                    "right");
            CHECK(f.unpinned.count == 3 &&
                !col_session_pin_diff_arrangement(
                    COL_SESSION(p->session), "right", r, &key, 1, &f.pin));
            CHECK(f.pin.arr == (const col_diff_arrangement_t *)
                f.unpinned.entries[0].address);
            check_pin(&f);
        }
        script(&f);
        image_t now = reference(&f);
        if (!f.stage && f.cap) {
            CHECK(first_pairs(&f) == 5);
            unsigned distinct = 0;
            for (unsigned x = 1; x < VALUES; ++x)
                for (unsigned y = 1; y < VALUES; ++y)
                    distinct += now.rows[0][x][y];
            CHECK(distinct == 4);
            for (unsigned p = 0; p < 3; ++p)
                COL_SESSION(f.profiles[p].session)->join_output_limit =
                    f.cap;
        }
        CHECK(now.rows[0][2][7] && !now.rows[4][2][7]);
        for (unsigned p = 0; p < 3; ++p) {
            registry_t before = registry(&f.profiles[p]);
            watch(&f, p, p < 2);
            if (!p && f.pin_mode && !f.stopped && f.stage == 1) {
                check_pin(&f);
                failure(&f, &before, &old, ENOTSUP);
                before = registry(&f.profiles[p]);
                watch(&f, p, true);
            }
            if (!p && f.resource && !f.stopped
                && f.stage == (f.resource == 2 ? 5u : 1u)) {
                failure(&f, &before, &old,
                    f.resource == 3 ? ENOMEM : ENOSPC);
                watch(&f, p, true);
            }
            if (!p && f.index_allocation && !f.stopped && f.stage ==
                wl_test_join_cache_allocation_stage(&f)) {
                failure(&f, &before, &old, ENOMEM);
                watch(&f, p, true);
            }
            if (!f.stage && f.cap && f.cap < 6) {
                if (p < 2) {
                    failure(&f, &before, &old, EOVERFLOW);
                    CHECK(f.seen.output_extent[f.head] == f.cap);
                } else {
                    /* Equal raw policy/status, without a raw rollback claim. */
                    CHECK(finish(&f, &f.profiles[p]) == EOVERFLOW);
                    memset(&f.profiles[p].events, 0,
                        sizeof(f.profiles[p].events));
                }
                COL_SESSION(f.profiles[p].session)->join_output_limit = 0;
                watch(&f, p, p < 2);
            }
            if (!p && f.stop_site && !f.stopped
                && f.stage == (f.stop_site == 2 ? 5u : 1u)) {
                failure(&f, &before, &old, f.work_stop
                    ? WL_EVALUATION_CONTROL_WORK_LIMIT
                    : WL_EVALUATION_CONTROL_CANCELLED);
                watch(&f, p, true);
            }
            CHECK(!finish(&f, &f.profiles[p]));
            if (!p && f.index_allocation && f.stage ==
                wl_test_join_cache_allocation_stage(&f)) {
                CHECK(f.injected_allocations == 1 && f.allocation_attempt
                    && f.allocation_attempt != f.failed_allocation_attempt
                    && f.allocation_calls[f.index_allocation] ==
                    (f.allocation_ordinal ||
                    f.index_allocation >=
                    WL_COLUMNAR_ARRANGEMENT_TEST_DIFF_ALLOC_HEADER ? 3u : 1u)
                    && f.seen.image_cells &&
                    f.seen.copied_entries == (f.new_metadata ? 0u : 3u));
                if (f.new_metadata || f.later_signature) {
                    CHECK(f.seen.builds == 3 && f.seen.reuses == 1);
                    for (unsigned i = 0; i < 3;
                        ++i) CHECK(f.prefix[i].ordinal && !f.prefix[i].pending
                            && f.prefix[i].inserted == f.prefix[i].rows);
                }
                fprintf(stdout,
                    "index malloc retry site=%u attempt=%llu reuses=%u\n",
                    f.index_allocation,
                    (unsigned long long)f.allocation_attempt,
                    f.seen.reuses);
            }
            if (p < 2)
                check_work(&f, &before);
            if (!f.stage && f.cap && p < 2)
                CHECK(f.seen.output_extent[0] == 5
                    && f.seen.output_extent[1] == 5);
            watch(&f, p, false);
            if (!p && f.stopped) writers(&f.profiles[p]);
            check_delivery(&f, &f.profiles[p], &old, &now);
            CHECK(!finish(&f, &f.profiles[p]));
            check_delivery(&f, &f.profiles[p], &now, &now);
            if (!f.stage && f.cap)
                COL_SESSION(f.profiles[p].session)->join_output_limit = 0;
        }
        old = now;
    }
    CHECK((!f.stop_site && !f.resource && !f.pin_mode) || f.stopped);
    CHECK(!f.cycle_fault || f.stopped);
    CHECK(!f.index_allocation || (f.stopped && f.injected_allocations == 1));
    CHECK(!wl_columnar_arrangement_test_diff_fail_allocation);
    CHECK(!f.pin.active);
    CHECK(wl_columnar_join_test_attempt_fail_allocation == -1);
    for (unsigned p = 0; p < 3; ++p) {
        CHECK(!wl_session_try_destroy(&f.profiles[p].session));
        if (f.profiles[p].control)
            wl_evaluation_control_release(f.profiles[p].control);
    }
    wl_plan_free(f.plan);
    wirelog_program_free(f.program);
    for (unsigned p = 0; p < 3; ++p) {
        CHECK(!wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(f.profiles[p].governor)));
        wl_columnar_memory_governor_ref_release(f.profiles[p].governor);
    }
    puts("natural JOIN full/delta source identity, persistent/private indexes "
        "and literal histories passed");
    return 0;
}
