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
} witnesses_t;
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    profile_t profiles[3]; /* default controlled, private controlled, raw */
    bool compound, left[VALUES][VALUES], right[VALUES][VALUES][2];
    unsigned route, stage;
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
        CHECK(!(bits & (1u << key)) && e->diff_arr && !e->pin_count &&
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
            x->buckets == y->buckets && x->capacity == y->capacity &&
            !memcmp(x->heads, y->heads, x->buckets * sizeof(uint32_t)) &&
            !memcmp(x->next, y->next, x->capacity * sizeof(uint32_t)));
    }
}
static void
bind(fixture_t *f, const wl_columnar_eval_private_bindings_t *bindings)
{
    CHECK(bindings && bindings->dynamic_entries
        && wl_columnar_session_observed_bindings_authorized(
            COL_SESSION(f->active->session), bindings));
    if (f->selected != bindings->plan) {
        f->append = NULL; f->append_op = NULL; f->building = NULL;
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
    bind(f, r->private_bindings);
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
        (f->stage == 1 || f->stage == 4))
        CHECK(f->append_op->op == WL_PLAN_OP_VARIABLE &&
            ctx->source.descriptor == f->append && ctx->source.nrows == 1 &&
            ctx->source.columns[0][0] == 4 && ctx->source.columns[1][0] == 4);
}
static void observed(wl_evaluation_control_t *control, unsigned phase,
    uint32_t progress,
    const wl_columnar_eval_private_bindings_t *bindings) {
    fixture_t *f = active;
    if (control != f->active->control) return;
    if (f->reading_selector)
        return;
    if (phase == 8 && bindings) bind(f, bindings);
    if (phase == 44)
        f->occurrence = progress;
    if (phase == 28) {
        bind(f, bindings);
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
static void arrangement(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress) {
    (void)progress;
    fixture_t *f = active;
    if (control != f->active->control) return;
    if (phase != 13 && phase != 14)
        return;
    wl_columnar_filter_context_t *ctx = context(f);
    sources(f, ctx);
    CHECK(!f->profile && !ctx->join_operation->right_filter_expr.size &&
        ctx->join_right.nrows && !rhs_delta(f));
    if (phase == 14) {
        ++f->seen.builds;
        f->building = ctx->join_operation;
    } else {
        ++f->seen.reuses;
        f->building = NULL;
    }
}
static void join(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress) {
    fixture_t *f = active;
    if (control != f->active->control) return;
    if (phase != 15 && phase != 28 && phase != 47 && phase != 48 &&
        phase != 29 &&
        phase != 18 && phase != 44)
        return;
    wl_columnar_filter_context_t *ctx = context(f);
    sources(f, ctx);
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
        ++f->seen.indexed;
    }
    if (phase == 48) {
        CHECK(!f->profile && !ctx->join_operation->right_filter_expr.size &&
            !rhs_delta(f) && progress < ctx->join_right.nrows);
        ++f->seen.persistent;
    }
    if (phase == 44 && ctx->join_right.columns[0][progress] == 2 &&
        ctx->join_right.columns[1][progress] == 7 &&
        ctx->join_right.columns[2][progress] == 0) {
        CHECK(f->head == 4 && ctx->join_operation->right_filter_expr.size);
        ++f->seen.rejected;
    }
}
static void watch(fixture_t *f, unsigned profile, bool enabled) {
    active = f;
    f->profile = profile;
    f->active = &f->profiles[profile];
    memset(&f->seen, 0, sizeof(f->seen));
    f->bindings = NULL;
    f->selected = NULL;
    f->building = NULL;
    f->append = NULL;
    f->append_op = NULL;
    wl_columnar_session_test_observed_transaction_checkpoint =
        enabled ? observed : NULL;
    wl_columnar_arrangement_test_diff_checkpoint = enabled ? arrangement : NULL;
    wl_columnar_join_test_attempt_checkpoint = enabled ? join : NULL;
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
            bool warm = f->stage == 1 || f->stage == 4;
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
int main(int argc, char **argv) {
    CHECK(argc == 3);
    fixture_t f = {0};
    f.compound = !strcmp(argv[1], "inline");
    CHECK(f.compound || !strcmp(argv[1], "scalar"));
    f.route = !strcmp(argv[2], "observed") ? 0
            : !strcmp(argv[2], "plain")  ? 1
                                         : 2;
    CHECK(f.route != 2 || !strcmp(argv[2], "snapshot"));
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
    for (f.stage = 0; f.stage < 7; ++f.stage) {
        script(&f);
        image_t now = reference(&f);
        CHECK(now.rows[0][2][7] && !now.rows[4][2][7]);
        for (unsigned p = 0; p < 3; ++p) {
            registry_t before = registry(&f.profiles[p]);
            watch(&f, p, p < 2);
            CHECK(!finish(&f, &f.profiles[p]));
            if (p < 2)
                check_work(&f, &before);
            watch(&f, p, false);
            check_delivery(&f, &f.profiles[p], &old, &now);
            CHECK(!finish(&f, &f.profiles[p]));
            check_delivery(&f, &f.profiles[p], &now, &now);
        }
        old = now;
    }
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
