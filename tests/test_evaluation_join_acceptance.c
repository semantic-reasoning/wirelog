/* Ordinary JOIN acceptance: actual compiler/session routes and local contracts. */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/ir/ir.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)
typedef struct {
    wl_session_t *session;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    unsigned route, tuple_mask, evaluated, selected;
    int events[5]; unsigned calls[5];
} profile_t;
typedef struct {
    wirelog_program_t *program; wl_plan_t *plan; const wl_plan_op_t *op;
    profile_t p[3]; unsigned width, right_mask, left_mask, kind;
    int bad; bool bad_active, saw_bare, saw_typed_empty;
} chain_t;
static chain_t *active;
static profile_t *current;
static void environment(const char *name) {
#ifdef _WIN32
    CHECK(!_putenv_s(name, ""));
#else
    CHECK(!unsetenv(name));
#endif
}
static void delta(const char *name, const int64_t *row, uint32_t width,
    int32_t sign, void *arg) {
    profile_t *p = arg;
    CHECK(!strcmp(name,
        "out") && width == 1 && row[0] >= 1 && row[0] <= 4 &&
        (sign == 1 || sign == -1));
    unsigned key = (unsigned)row[0]; CHECK(!p->calls[key]++);
    p->events[key] = sign;
}
static void tuple(const char *name, const int64_t *row, uint32_t width,
    void *arg) {
    profile_t *p = arg;
    CHECK(!strcmp(name, "out") && width == 1 && row[0] >= 1 && row[0] <= 4);
    unsigned bit = 1u << ((unsigned)row[0]-1); CHECK(!(p->tuple_mask & bit));
    p->tuple_mask |= bit;
}
static unsigned result(const chain_t *f) {
    unsigned matched = f->left_mask & f->right_mask;
    if (f->bad_active) matched &= ~1u;
    return f->kind == 2 ? f->left_mask & ~matched : matched;
}
static void checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress) {
    if (!current || control != current->control ||
        (phase != 44 && phase != 46)) return;
    wl_col_session_t *s = COL_SESSION(current->session);
    wl_columnar_eval_serial_framed_attempt_t *r = NULL;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(s->cleanup_active, s, &r)
        && r->capture_only && r->child_bound && r->private_bindings
        && wl_columnar_session_observed_bindings_authorized(s,
        r->private_bindings));
    CHECK(!wl_columnar_eval_stack_filter_context_owned(s->cleanup_active, &ctx,
        &r->child)
        && ctx->join_operation == &r->child.plan->ops[r->child.cursor]
        && ctx->join_operation->op == WL_PLAN_OP_JOIN &&
        ctx->join_operation->right_filter_expr.size);
    if (ctx->join_right_bare) {
        const wl_columnar_relation_empty_source_t *empty =
            &ctx->join_empty_reader;
        CHECK(phase == 46 && empty->identity == empty
            && empty->phase == WL_COLUMNAR_RELATION_EMPTY_SOURCE_HELD
            && empty->descriptor == session_find_rel(s, "right")
            && !empty->descriptor->nrows && !empty->descriptor->ncols);
        active->saw_bare = true; return;
    }
    const wl_columnar_relation_scalar_source_t *source = &ctx->join_right;
    const col_rel_t *right = source->descriptor;
    CHECK(source->identity == source && right
        && source->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD
        && source->owner_reader.owner == &right->storage_owner->source_access
        && source->descriptor_reader.owner == &right->descriptor_access);
    const wl_columnar_eval_private_dynamic_binding_t *binding = NULL;
    for (uint32_t i = 0; i < r->private_bindings->count; ++i) {
        const wl_columnar_eval_private_dynamic_binding_t *entry =
            &r->private_bindings->dynamic_entries[i];
        if (!strcmp(entry->name, "right")) binding = entry;
    }
    CHECK(binding && binding->relation == right
        && binding->public_relation == session_find_rel(s, "right")
        && wl_columnar_relation_snapshot_equal(binding->snapshot,
        wl_columnar_relation_snapshot(right)));
    if (!source->width) {
        CHECK(!active->right_mask && !source->nrows && !right->ncols
            && !binding->public_relation->nrows
            && !binding->public_relation->ncols);
        active->saw_bare = true;
        return;
    }
    CHECK(right == binding->public_relation
        && source->width == active->width + 1);
    /* Empty-filter validation also has a phase46; it is not a selected row. */
    if (!source->nrows) {
        active->saw_typed_empty = true; return;
    }
    CHECK(progress < source->nrows);
    int64_t key = col_rel_get(right, (uint32_t)progress, 0);
    CHECK(key >= 1 && key <= 2 &&
        (active->right_mask & (1u << ((unsigned)key-1))));
    for (unsigned c = 1; c <= active->width; ++c) {
        int64_t want = active->bad_active && key == 1 &&
            c == (unsigned)(active->bad+1) ? 2 : 1;
        CHECK(col_rel_get(right, (uint32_t)progress, c) == want);
    }
    if (phase == 44) current->evaluated |= 1u << ((unsigned)key-1);
    else current->selected |= 1u << ((unsigned)key-1);
}
static int finish(profile_t *p) {
    p->tuple_mask = 0;
    for (unsigned i = 0; i < 100000; ++i) {
        int rc = p->route == 1 ? wl_session_snapshot(p->session, tuple,
                p) : wl_session_step(p->session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}
static void content(profile_t *p, unsigned mask) {
    const col_rel_t *r = session_find_rel(COL_SESSION(p->session), "out");
    CHECK(r && r->ncols == 1 && r->column_types &&
        r->column_types[0] == WIRELOG_TYPE_INT64);
    unsigned seen = 0;
    for (uint32_t i = 0; i < r->nrows; ++i) {
        int64_t key = col_rel_get(r, i, 0); CHECK(key >= 1 && key <= 4);
        unsigned bit = 1u << ((unsigned)key-1); CHECK(!(seen & bit));
        seen |= bit;
    }
    CHECK(seen == mask); if (p->route == 1) CHECK(p->tuple_mask == mask);
}
static void evaluate(chain_t *f, unsigned old, bool rows) {
    unsigned want = result(f);
    for (unsigned q = f->kind ? 2u : 0u; q < 3; ++q) {
        profile_t *p = &f->p[q]; active = f; current = p;
        p->evaluated = p->selected = 0;
        wl_columnar_join_test_attempt_checkpoint =
            p->control ? checkpoint : NULL;
        CHECK(!finish(p)); content(p, want);
        for (unsigned k = 1; k <= 4; ++k) {
            int change = (int)((want >> (k-1)) & 1u) - (int)((old >>
                (k-1)) & 1u);
            CHECK(p->events[k] == (p->route == 1 ? 0 : change)
                && p->calls[k] == (p->route != 1 && change ? 1u : 0u));
        }
        if (p->control) {
            CHECK(p->evaluated == (rows ? f->right_mask : 0));
            CHECK(p->selected ==
                (rows ? f->right_mask & (f->bad_active ? ~1u : ~0u) : 0));
        }
        memset(p->events, 0, sizeof(p->events));
        memset(p->calls, 0, sizeof(p->calls));
        p->evaluated = p->selected = 0; CHECK(!finish(p)); content(p, want);
        CHECK(!p->evaluated && !p->selected);
        for (unsigned k = 1; k <= 4; ++k) CHECK(!p->events[k] && !p->calls[k]);
    }
    wl_columnar_join_test_attempt_checkpoint = NULL; current = NULL;
    active = NULL;
}
static wirelog_ir_node_t *join_node(wirelog_ir_node_t *node) {
    if (node->type == WIRELOG_IR_JOIN ||
        node->type == WIRELOG_IR_ANTIJOIN) return node;
    for (uint32_t i = 0; i < node->child_count; ++i) {
        wirelog_ir_node_t *got = join_node(node->children[i]);
        if (got) return got;
    }
    return NULL;
}
static void insert_left(chain_t *f, unsigned key) {
    int64_t row = key;
    for (unsigned q = f->kind?2u:0u; q < 3;
        ++q) CHECK(!wl_session_insert(f->p[q].session, "left", &row, 1, 1));
    f->left_mask |= 1u << (key-1);
}
static void change_right(chain_t *f, unsigned key, bool remove) {
    int64_t row[66]; CHECK(f->width < 66); row[0] = key;
    for (unsigned c = 1; c <= f->width;
        ++c) row[c] = f->bad_active && key == 1 &&
            c == (unsigned)(f->bad+1)?2:1;
    for (unsigned q = f->kind?2u:0u; q < 3;
        ++q) CHECK(!(remove?wl_session_remove(f->p[q].session, "right", row, 1,
            f->width+1):wl_session_insert(f->p[q].session, "right", row, 1,
            f->width+1)));
    if (remove)f->right_mask &= ~(1u<<(key-1));
    else f->right_mask |= 1u<<(key-1);
}
static void chain_case(unsigned width, int bad, unsigned kind) {
    chain_t f = {0}; f.width = width; f.bad = bad; f.bad_active = bad >= 0;
    f.kind = kind;
    char source[8192]; size_t at = 0;
    at += (size_t)snprintf(source+at, sizeof(source)-at,
            ".decl left(k:int64)\n.decl right(k:int64");
    for (unsigned c = 0; c < width;
        ++c) at += (size_t)snprintf(source+at, sizeof(source)-at, ",c%u:int64",
                c);
    at += (size_t)snprintf(source+at, sizeof(source)-at,
            ")\n.decl out(k:int64)\nout(k):-left(k),%sright(k",
            kind == 2?"!":"");
    for (unsigned c = 0; c < width;
        ++c) at += (size_t)snprintf(source+at, sizeof(source)-at, ",1");
    at += (size_t)snprintf(source+at, sizeof(source)-at, ").\n");
    CHECK(at < sizeof(source));
    wirelog_error_t error; f.program = wirelog_parse_string(source, &error);
    CHECK(f.program);
    if (kind == 1) { /* Explicit accepted probe IR, not claimed natural emission. */
        wirelog_ir_node_t *node = join_node(f.program->rules[0].ir_root);
        CHECK(node && node->type == WIRELOG_IR_JOIN);
        node->type = WIRELOG_IR_SEMIJOIN;
        CHECK(!wl_ir_program_rebuild_relation_irs(f.program));
    }
    CHECK(!wl_plan_from_program(f.program, &f.plan));
    wl_plan_op_type_t expected = kind == 0?WL_PLAN_OP_JOIN:kind ==
        1?WL_PLAN_OP_SEMIJOIN:WL_PLAN_OP_ANTIJOIN;
    for (uint32_t st = 0; st < f.plan->stratum_count; ++st)for (uint32_t r = 0;
            r < f.plan->strata[st].relation_count; ++r) for (uint32_t o = 0;
                o < f.plan->strata[st].relations[r].op_count; ++o){
                const wl_plan_op_t *op =
                    &f.plan->strata[st].relations[r].ops[o];
                if (op->op == expected) {
                    CHECK(!f.op && !strcmp(op->right_relation,
                        "right") && op->right_filter_expr.data &&
                        op->right_filter_expr.size); f.op = op;
                }
            }
    CHECK(f.op);
    for (unsigned q = kind?2u:0u; q < 3; ++q) {
        profile_t *p = &f.p[q]; p->route = q == 1?1u:0u;
        wl_columnar_memory_resolution_t res =
        {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
        p->governor = wl_columnar_memory_governor_ref_create(&res);
        CHECK(p->governor);
        wl_session_options_t opts; wl_session_options_init(&opts);
        opts.memory_governor = p->governor;
        if (q < 2) {
            CHECK(!wl_evaluation_control_create(UINT64_C(1000000000),
                &p->control)); opts.evaluation_control = p->control;
        }
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
            &opts, &p->session));
        if (q == 1)COL_SESSION(p->session)->diff_enabled = false;
        if (p->route != 1)wl_session_set_delta_cb(p->session, delta, p);
    }
    evaluate(&f, 0, false); insert_left(&f, 1); insert_left(&f, 2);
    evaluate(&f, 0, false);
    unsigned old = result(&f); insert_left(&f, 3); change_right(&f, 1, false);
    change_right(&f, 2, false); evaluate(&f, old, true);
    old = result(&f);
    if (f.bad_active) {
        change_right(&f, 1, true); f.bad_active = false;
        change_right(&f, 1, false);
    }else change_right(&f, 2, true);
    evaluate(&f, old, true); old = result(&f);
    if (f.right_mask&1u)change_right(&f, 1, true);
    if (f.right_mask&2u) change_right(&f, 2, true);
    evaluate(&f, old, false); old = result(&f);
    insert_left(&f, 4); change_right(&f, 1, false); evaluate(&f, old, true);
    for (unsigned q = kind?2u:0u; q < 3; ++q) {
        CHECK(!wl_session_try_destroy(&f.p[q].session));
        if (f.p[q].control) wl_evaluation_control_release(f.p[q].control);
    }
    CHECK(kind || (f.saw_bare && f.saw_typed_empty));
    wl_plan_free(f.plan); wirelog_program_free(f.program);
    for (unsigned q = kind?2u:0u; q < 3; ++q) {
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                f.p[q].governor)));
        wl_columnar_memory_governor_ref_release(f.p[q].governor);
    }
    printf(
        "chain predicates=%u bad=%d kind=%u literal/selection/future histories passed\n",
        width, bad, kind);
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    const char *vars[] = {"WIRELOG_DIFF_ENABLED", "WIRELOG_JOIN_BATCH_BYTES",
                          "WIRELOG_JOIN_BATCH_STRICT",
                          "WIRELOG_JOIN_OUTPUT_LIMIT",
                          "WIRELOG_SPILL_JOIN_BATCH_BYTES"};
    for (unsigned i = 0; i < sizeof(vars)/sizeof(vars[0]);
        ++i) environment(vars[i]);
    if (!strcmp(argv[1], "chains")) {
        unsigned widths[] = {31, 32, 33, 65};
        for (unsigned i = 0; i < sizeof(widths)/sizeof(widths[0]); ++i) {
            chain_case(widths[i], -1, 0); chain_case(widths[i], 0, 0);
            chain_case(widths[i], (int)widths[i]-1, 0);
        }
    } else {
        CHECK(!strcmp(argv[1], "probe-chains")); for (unsigned kind = 1;
            kind < 3; ++kind) {
            chain_case(33, -1, kind); chain_case(33, 0, kind);
            chain_case(33, 32, kind);
        }
    }
    return 0;
}
