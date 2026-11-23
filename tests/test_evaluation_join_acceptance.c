/* Ordinary JOIN acceptance: actual compiler/session routes and local contracts. */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
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
/* Natural post-JOIN FILTER and explicitly repositioned read-only RHS IR are
 * distinct compiler routes, each checked against independent fixture literals. */
typedef struct {
    profile_t p;
    const wl_plan_op_t *op;
    unsigned evaluated, selected, numeric, scalar;
    wl_columnar_memory_reservation_t peer;
    bool peer_held, injected;
    unsigned freed;
} expression_fixture_t;
static unsigned expression_fault;
static expression_fixture_t *expression_active;
static bool reposition_rhs(wirelog_ir_node_t **slot) {
    wirelog_ir_node_t *node = *slot;
    if (node->type == WIRELOG_IR_FILTER && node->child_count == 1 &&
        node->children[0]->type == WIRELOG_IR_JOIN) {
        wirelog_ir_node_t *join = node->children[0];
        CHECK(join->child_count == 2);
        node->children[0] = join->children[1]; join->children[1] = node;
        *slot = join; return true;
    }
    for (uint32_t i = 0; i < node->child_count; ++i)
        if (reposition_rhs(&node->children[i])) return true;
    return false;
}
static void expression_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress) {
    expression_fixture_t *f = expression_active;
    if (!f || control != f->p.control || (phase != 44 && phase != 46)) return;
    wl_col_session_t *s = COL_SESSION(f->p.session);
    wl_columnar_eval_serial_framed_attempt_t *r = NULL;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(s->cleanup_active, s, &r)
        && r->capture_only && r->child_bound && r->private_bindings
        && wl_columnar_session_observed_bindings_authorized(s,
        r->private_bindings));
    CHECK(!wl_columnar_eval_stack_filter_context_owned(s->cleanup_active,
        &ctx, &r->child) && ctx->join_operation ==
        &r->child.plan->ops[r->child.cursor] &&
        ctx->join_operation->op == WL_PLAN_OP_JOIN &&
        ctx->join_operation->right_filter_expr.size);
    const wl_columnar_relation_scalar_source_t *source = &ctx->join_right;
    const col_rel_t *right = source->descriptor;
    CHECK(source->identity == source &&
        source->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD &&
        right == session_find_rel(s, "right") && source->width == 4 &&
        source->nrows == 2 && progress < 2 &&
        source->owner_reader.owner == &right->storage_owner->source_access &&
        source->descriptor_reader.owner == &right->descriptor_access);
    int64_t key = col_rel_get(right, (uint32_t)progress, 0);
    CHECK(key >= 1 && key <= 2 &&
        col_rel_get(right, (uint32_t)progress, 1) == key+1);
    const char *text = wl_intern_reverse(s->intern,
            col_rel_get(right, (uint32_t)progress, 2));
    CHECK(text && !strcmp(text, key == 1 ? "yes" : "no"));
    CHECK(col_rel_get(right, (uint32_t)progress, 3) ==
        wl_columnar_float_to_bits((double)(key+1)));
    CHECK((ctx->program != NULL) != (ctx->scalar_program != NULL));
    if (phase == 44) {
        f->numeric += ctx->program != NULL;
        f->scalar += ctx->scalar_program != NULL;
        f->evaluated |= 1u << ((unsigned)key-1);
    }else f->selected |= 1u << ((unsigned)key-1);
}
static void expression_fault_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress, bool failed) {
    (void)failed;
    expression_fixture_t *f = expression_active;
    if (!f) return;
    if (phase == 143 && f->injected) {
        CHECK(!control && progress == 5); ++f->freed; return;
    }
    if (f->injected || control != f->p.control ||
        phase !=
        (expression_fault == 1 ? 131u : expression_fault ==
        2 ? 130u : 135u)) return;
    CHECK(progress == (expression_fault == 1 ? 5u : 4u));
    wl_col_session_t *s = COL_SESSION(f->p.session);
    wl_columnar_eval_serial_framed_attempt_t *r = NULL;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(s->cleanup_active, s, &r) &&
        r->capture_only && r->child_bound && r->abort_supported &&
        !wl_columnar_eval_stack_filter_context_owned(s->cleanup_active, &ctx,
        &r->child) &&
        ctx->scalar_program && !ctx->program && ctx->values &&
        ctx->join_right.descriptor == session_find_rel(s, "right") &&
        ctx->join_right.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD);
    f->injected = true;
    if (expression_fault == 1) wl_columnar_expr_test_fail_allocation = 0;
    else if (expression_fault == 2) {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(f->p.governor);
        uint64_t available = atomic_load(&g->usable_bytes)-
            wl_columnar_memory_reserved(g);
        CHECK(available > 5);
        wl_columnar_memory_reservation_init(&f->peer);
        CHECK(wl_columnar_memory_reserve_checked(g, available-1, &f->peer) ==
            WL_COLUMNAR_MEMORY_ADMISSION_OK &&
            wl_columnar_memory_commit(&f->peer, &f->peer));
        f->peer_held = true;
    } else if (expression_fault ==
        3) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge(control, UINT64_MAX) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
}
static void expression_case(const char *predicate, unsigned mask,
    bool explicit_rhs) {
    char source[2048];
    snprintf(source, sizeof(source),
        ".decl left(k:int64)\n.decl right(k:int64,n:int64,s:symbol,v:float)\n"
        ".decl out(k:int64)\nout(k):-left(k),right(k,n,s,v),%s.\n", predicate);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    if (explicit_rhs) {
        CHECK(reposition_rhs(&program->rules[0].ir_root));
        CHECK(!wl_ir_program_rebuild_relation_irs(program));
    }
    wl_plan_t *plan = NULL; CHECK(!wl_plan_from_program(program, &plan));
    expression_fixture_t f = {0}; unsigned filters = 0;
    for (uint32_t st = 0; st < plan->stratum_count; ++st)
        for (uint32_t r = 0; r < plan->strata[st].relation_count; ++r)
            for (uint32_t o = 0; o < plan->strata[st].relations[r].op_count;
                ++o){
                const wl_plan_op_t *op = &plan->strata[st].relations[r].ops[o];
                filters += op->op == WL_PLAN_OP_FILTER;
                if (op->op == WL_PLAN_OP_JOIN) {
                    CHECK(!f.op); f.op = op;
                }
            }
    CHECK(f.op && !strcmp(f.op->right_relation, "right") && f.op->key_count == 1
        && (f.op->right_filter_expr.size != 0) == explicit_rhs &&
        filters == (explicit_rhs ? 0u : 1u));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f.p.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.p.governor && !wl_evaluation_control_create(UINT64_C(1000000000),
        &f.p.control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = f.p.governor;
    options.evaluation_control = f.p.control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &f.p.session));
    wl_session_set_delta_cb(f.p.session, delta, &f.p);
    CHECK(!finish(&f.p));
    int64_t yes = wl_intern_put(plan->intern, "yes"),
        no = wl_intern_put(plan->intern, "no");
    CHECK(yes >= 0 && no >= 0);
    int64_t rows[2][4] = {{1, 2, yes, 0}, {2, 3, no, 0}};
    for (unsigned i = 0; i < 2; ++i) {
        rows[i][3] = wl_columnar_float_to_bits((double)(i+2));
        CHECK(!wl_session_insert(f.p.session, "left", rows[i], 1, 1) &&
            !wl_session_insert(f.p.session, "right", rows[i], 1, 4));
    }
    expression_active = &f;
    wl_columnar_join_test_attempt_checkpoint = expression_checkpoint;
    if (expression_fault) {
        CHECK(explicit_rhs);
        const col_rel_t *old = session_find_rel(COL_SESSION(f.p.session),
                "out");
        CHECK(old && !old->nrows);
        col_relation_snapshot_t old_snapshot =
            wl_columnar_relation_snapshot(old);
        const col_rel_t *right = session_find_rel(COL_SESSION(f.p.session),
                "right");
        col_relation_snapshot_t input_snapshot =
            wl_columnar_relation_snapshot(right);
        uint64_t denials =
            wl_columnar_memory_denials(wl_columnar_memory_governor_ref_get(
                    f.p.governor));
        wl_columnar_expr_test_checkpoint = expression_fault_checkpoint;
        int rc = finish(&f.p);
        wl_columnar_expr_test_checkpoint = NULL;
        int expected = expression_fault == 1 ? ENOMEM : expression_fault ==
            2 ? ENOSPC :
            expression_fault ==
            3 ? WL_EVALUATION_CONTROL_CANCELLED :
            WL_EVALUATION_CONTROL_WORK_LIMIT;
        fprintf(stdout,
            "RHS expression fault=%u rc=%d expected=%d injected=%u free=%u\n",
            expression_fault, rc, expected, f.injected, f.freed);
        CHECK(rc == expected && f.injected && !f.p.calls[1] && !f.p.calls[2]);
        CHECK(f.freed == (expression_fault >= 3 ? 1u : 0u));
        wl_col_session_t *session = COL_SESSION(f.p.session);
        CHECK(!session->cleanup_active && !session->cleanup_pending &&
            !session->cleanup_reserved_bytes &&
            session_find_rel(session, "out") == old &&
            wl_columnar_relation_snapshot_equal(old_snapshot,
            wl_columnar_relation_snapshot(old)) &&
            wl_columnar_relation_snapshot_equal(input_snapshot,
            wl_columnar_relation_snapshot(right)) &&
            session->memory_budget_denied == (expression_fault == 2));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.p.control, &report) &&
            report.result == rc && !report.cleanup_status && report.consumed);
        CHECK(wl_columnar_memory_denials(wl_columnar_memory_governor_ref_get(
                f.p.governor)) == denials+(expression_fault == 2?1u:0u));
        CHECK(session->pending_input_change &&
            !wl_evaluation_control_active_attempt_id(f.p.control));
        uint64_t owner[8];
        CHECK(!wl_columnar_session_test_retained_work_owner(session, owner));
        bool retained = expression_fault == 4;
        if (retained) CHECK(owner[0] && owner[1] && owner[2] && owner[3] == 8 &&
                owner[4] && owner[4] == owner[5] && owner[6] && owner[7]);
        else CHECK(!owner[0] && !owner[1] && !owner[2]);
        CHECK(f.p.session->backend->session_controlled_step_state(
                f.p.session) ==
            (expression_fault >=
            3?WL_CONTROLLED_STEP_PENDING:WL_CONTROLLED_STEP_RETRYABLE));
        col_rel_t *left = session_find_rel(session, "left");
        col_rel_t *inputs[] = {left, (col_rel_t *)right};
        for (unsigned i = 0; i < 2; ++i) {
            CHECK(atomic_load(&inputs[i]->source_access.state) ==
                (retained?1u:0u) &&
                atomic_load(&inputs[i]->descriptor_access.state) ==
                (retained?1u:0u));
            wl_columnar_source_access_writer_t writer = {0};
            if (retained) CHECK(col_rel_source_writer_acquire(inputs[i],
                    &writer) == EBUSY);
            else CHECK(!col_rel_source_writer_acquire(inputs[i], &writer) &&
                    !wl_columnar_source_access_writer_release(&writer));
        }
        if (retained) CHECK(wl_session_insert(f.p.session, "left", rows[0], 1,
                1) == EBUSY &&
                wl_session_remove(f.p.session, "right", rows[0], 1,
                4) == EBUSY &&
                wl_session_snapshot(f.p.session, tuple,
                &f.p) == EBUSY && !f.p.tuple_mask);
        wl_evaluation_control_report_t frozen;
        CHECK(!wl_evaluation_control_report(f.p.control, &frozen) &&
            frozen.attempt_id == report.attempt_id &&
            frozen.result == report.result &&
            frozen.limit == report.limit &&
            frozen.consumed == report.consumed &&
            frozen.stop_reason == report.stop_reason &&
            frozen.execution_status == report.execution_status &&
            frozen.cleanup_status == report.cleanup_status);
        uint64_t again[8];
        CHECK(!wl_columnar_session_test_retained_work_owner(session, again));
        CHECK(!memcmp(owner, again, sizeof(owner)));
        if (f.peer_held) CHECK(wl_columnar_memory_release(&f.peer));
        CHECK(wl_columnar_expr_test_fail_allocation == -1);
        CHECK(!wl_evaluation_control_reset(f.p.control));
        f.evaluated = f.selected = 0;
    }
    CHECK(!finish(&f.p)); content(&f.p, mask);
    CHECK(f.evaluated == (explicit_rhs ? 3u : 0u) &&
        f.selected == (explicit_rhs ? mask : 0u));
    CHECK(f.numeric + f.scalar == (explicit_rhs ? 2u : 0u));
    if (explicit_rhs && !strcmp(predicate,
        "n+1=3")) CHECK(f.numeric == 2 && !f.scalar);
    if (explicit_rhs && !strcmp(predicate,
        "cat(s,\"!\")=\"yes!\"")) CHECK(f.scalar == 2 && !f.numeric);
    for (unsigned i = 1; i <= 2; ++i) {
        unsigned selected = (mask >> (i-1)) & 1u;
        CHECK(f.p.events[i] == (int)selected && f.p.calls[i] == selected);
    }
    memset(f.p.events, 0, sizeof(f.p.events));
    memset(f.p.calls, 0, sizeof(f.p.calls)); f.evaluated = f.selected = 0;
    CHECK(!finish(&f.p)); content(&f.p, mask);
    CHECK(!f.evaluated && !f.selected && !f.p.calls[1] && !f.p.calls[2]);
    wl_columnar_join_test_attempt_checkpoint = NULL; expression_active = NULL;
    CHECK(!wl_session_remove(f.p.session, "right", rows[0], 1,
        4) && !finish(&f.p));
    content(&f.p, mask & ~1u);
    CHECK(f.p.events[1] == -(int)(mask & 1u) && !f.p.events[2]);
    CHECK(!wl_session_try_destroy(&f.p.session));
    wl_evaluation_control_release(f.p.control);
    wl_plan_free(plan); wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f.p.governor)));
    wl_columnar_memory_governor_ref_release(f.p.governor);
    printf("expression route=%s mask=%u %s passed\n",
        explicit_rhs?"explicit-RHS-IR":"natural-post-FILTER", mask, predicate);
}
static void expression_cases(void) {
    static const struct { const char *predicate; unsigned mask; bool crypto;
    } cases[] = {
        {"n+1=3", 1, false}, {"n-1=1", 1, false}, {"n*2=4", 1, false},
        {"n/2=1", 3, false}, {"n%2=0", 1, false}, {"band(n,1)=0", 1, false},
        {"bor(n,1)=3", 3, false}, {"bxor(n,1)=3", 1, false},
        {"bnot(n)=-3", 1, false},
        {"bshl(n,1)=4", 1, false}, {"bshr(n,1)=1", 3, false},
        {"n!=2", 2, false}, {"n<3", 1, false}, {"n>2", 2, false},
        {"n<=2", 1, false}, {"n>=3", 2, false}, {"n/0>0", 0, false},
        {"n%0>0", 0, false},
        {"v+1.0=3.0", 1, false}, {"v-1.0=1.0", 1, false},
        {"v*2.0=4.0", 1, false},
        {"v/2.0=1.0", 1, false}, {"v/0.0>0.0", 0, false}, {"v!=2.0", 2, false},
        {"v<3.0", 1, false},
        {"v>2.0", 2, false}, {"v<=2.0", 1, false}, {"v>=3.0", 2, false},
        {"cat(s,\"\")=\"yes\"", 1, false}, {"s!=\"yes\"", 2, false},
        {"s<\"yes\"", 2, false}, {"s>\"no\"", 1, false},
        {"s<=\"no\"", 2, false}, {"s>=\"yes\"", 1, false},
        {"strlen(s)=3", 1, false},
        {"contains(s,\"e\")", 1, false},
        {"contains(cat(s,\"한\"),\"한\")", 3, false},
        {"str_prefix(s,\"y\")", 1, false},
        {"str_suffix(s,\"s\")", 1, false}, {"str_ord(s)=121", 1, false},
        {"cat(s,\"!\")=\"yes!\"", 1, false}, {"substr(s,0,1)=\"y\"", 1, false},
        {"to_upper(s)=\"YES\"", 1, false}, {"to_lower(s)=\"yes\"", 1, false},
        {"str_replace(s,\"e\",\"a\")=\"yas\"", 1, false},
        {"trim(cat(\" \",s))=\"yes\"", 1, false},
        {"to_string(n)=\"2\"", 1, false}, {"to_number(s)>0", 0, false},
        {"hash(s)=hash(\"yes\")", 1, false},
        {"crc32_ethernet(s)=crc32_ethernet(\"yes\")", 1, false},
        {"crc32_castagnoli(s)=crc32_castagnoli(\"yes\")", 1, false},
        {"hash(n)=hash(2)", 1, false},
        {"crc32_ethernet(n)=crc32_ethernet(2)", 1, false},
        {"crc32_castagnoli(n)=crc32_castagnoli(2)", 1, false},
        {"md5(s)=md5(\"yes\")", 1, true}, {"sha1(s)=sha1(\"yes\")", 1, true},
        {"sha256(s)=sha256(\"yes\")", 1, true},
        {"sha512(s)=sha512(\"yes\")", 1, true},
        {"md5(n)=md5(2)", 1, true}, {"sha1(n)=sha1(2)", 1, true},
        {"sha256(n)=sha256(2)", 1, true}, {"sha512(n)=sha512(2)", 1, true},
        {"hmac_sha256(s,s)=hmac_sha256(\"yes\",\"yes\")", 1, true},
        {"hmac_sha256(s,n)=hmac_sha256(\"yes\",2)", 1, true},
        {"hmac_sha256(n,s)=hmac_sha256(2,\"yes\")", 1, true},
        {"hmac_sha256(n,n)=hmac_sha256(2,2)", 1, true},
        {"uuid5(s,s)=uuid5(\"yes\",\"yes\")", 1, true},
        {"uuid5(s,n)=uuid5(\"yes\",2)", 1, true},
        {"uuid5(n,s)=uuid5(2,\"yes\")", 1, true},
        {"uuid5(n,n)=uuid5(2,2)", 1, true},
        {"band(uuid4(),0)=0", 3, true},
        {
            "uuid5_rfc(\"6ba7b810-9dad-11d1-80b4-00c04fd430c8\",s)=uuid5_rfc(\"6ba7b810-9dad-11d1-80b4-00c04fd430c8\",\"yes\")",
            1, true
        }
    };
    for (unsigned i = 0; i < sizeof(cases)/sizeof(cases[0]); ++i)
        for (unsigned route = 0; route < 2; ++route) {
            unsigned mask = cases[i].mask;
#ifndef WL_MBEDTLS_ENABLED
            if (cases[i].crypto) mask = 0;
#endif
            expression_case(cases[i].predicate, mask, route != 0);
        }
}

typedef struct {
    profile_t p;
    const col_rel_t *expected, *full;
    col_relation_snapshot_t snapshot;
    bool empty, is_delta;
    unsigned observed;
} variable_fixture_t;
static variable_fixture_t *variable_active;
static bool variable_append_wait, variable_recursing;
static unsigned variable_append_reads;
extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);
static void variable_append(wl_evaluation_control_t *control, unsigned phase,
    uint32_t progress, const wl_columnar_eval_private_bindings_t *bindings) {
    if (!variable_append_wait || variable_recursing || phase != 44)return;
    variable_fixture_t *f = variable_active;
    CHECK(f && control == f->p.control && bindings &&
        bindings->dynamic_entries &&
        wl_columnar_session_observed_bindings_authorized(COL_SESSION(
            f->p.session), bindings) &&
        progress < bindings->plan->op_count);
    const wl_plan_op_t *op = &bindings->plan->ops[progress];
    CHECK(op->op == WL_PLAN_OP_VARIABLE && !strcmp(op->relation_name, "left"));
    col_rel_t *override = NULL; variable_recursing = true;
    CHECK(!wl_columnar_session_delta_source(COL_SESSION(f->p.session), bindings,
        op, &override));
    variable_recursing = false;
    CHECK(override && override->nrows == 1 && override->ncols == 1 &&
        override != session_find_rel(COL_SESSION(f->p.session), "left") &&
        col_rel_get(override, 0, 0) == 4);
    f->expected = override;
    f->snapshot = wl_columnar_relation_snapshot(override);
    f->empty = false; f->is_delta = true; ++variable_append_reads;
}
static void variable_finished(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *grant, int protocol) {
    (void)action; (void)grant; (void)protocol;
    variable_fixture_t *f = variable_active;
    if (!f || session != COL_SESSION(f->p.session) ||
        !session->cleanup_active) return;
    wl_columnar_eval_serial_framed_attempt_t *r = NULL;
    if (wl_columnar_eval_stack_framed_context(session->cleanup_active,
        (wl_col_session_t *)session, &r) || !r->child_bound)return;
    wl_columnar_eval_plan_numeric_attempt_t *child = &r->child;
    if (!child->source_initialized || child->source.phase !=
        WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD) return;
    const wl_plan_op_t *op = &child->plan->ops[child->source_begin];
    if (op->op != WL_PLAN_OP_VARIABLE || strcmp(op->relation_name,
        "left")) return;
    CHECK(!wl_columnar_eval_stack_framed_authenticate(session->cleanup_active,
        session, r) &&
        r->control == f->p.control && child->source_full == f->full &&
        child->source_selected == f->expected &&
        child->source_empty == f->empty &&
        child->source_is_delta == f->is_delta &&
        child->source.identity == &child->source &&
        child->source.descriptor == f->expected &&
        child->source.width == 1 && child->source.nrows == f->expected->nrows &&
        child->source.owner_reader.owner ==
        &f->expected->storage_owner->source_access &&
        child->source.descriptor_reader.owner ==
        &f->expected->descriptor_access &&
        wl_columnar_relation_snapshot_equal(f->snapshot,
        wl_columnar_relation_snapshot(f->expected)));
    ++f->observed;
}
static void variable_selectors(void) {
    const wl_delta_mode_t modes[] = {WL_DELTA_AUTO, WL_DELTA_FORCE_FULL,
                                     WL_DELTA_FORCE_DELTA, WL_DELTA_FORCE_EMPTY,
                                     WL_DELTA_FORCE_EMPTY_AFTER_SEED,
                                     (wl_delta_mode_t)99};
    const int extents[] = {-1, 0, 1, 3, 5};
    for (unsigned mode = 0; mode < 9; ++mode)
        for (unsigned seeded = 0; seeded < (mode < 5?2u:1u); ++seeded)
            for (unsigned d = 0; d < (mode < 5?5u:1u); ++d) {
                wirelog_error_t error;
                wirelog_program_t *program = wirelog_parse_string(
                    ".decl left(k:int64)\n.decl right(k:int64)\n.decl out(k:int64)\n"
                    "out(k):-left(k),right(k),k<100.\n", &error);
                CHECK(program); wl_plan_t *plan = NULL;
                CHECK(!wl_plan_from_program(program, &plan));
                unsigned vars = 0;
                for (uint32_t st = 0; st < plan->stratum_count; ++st)
                    for (uint32_t r = 0; r < plan->strata[st].relation_count;
                        ++r)
                        for (uint32_t o = 0;
                            o < plan->strata[st].relations[r].op_count; ++o){
                            wl_plan_op_t *op =
                                (wl_plan_op_t *)&plan->strata[st].relations[r].
                                ops[o];
                            if (op->op == WL_PLAN_OP_VARIABLE) {
                                CHECK(!strcmp(op->relation_name, "left"));
                                op->delta_mode = mode <
                                    6?modes[mode]:WL_DELTA_AUTO; vars++;
                            }
                            if (op->op ==
                                WL_PLAN_OP_JOIN) op->delta_mode = mode == 0 &&
                                    !seeded &&
                                    !d?WL_DELTA_AUTO:mode ==
                                    6?WL_DELTA_FORCE_EMPTY:mode ==
                                    7?WL_DELTA_FORCE_EMPTY_AFTER_SEED:mode ==
                                    8?(wl_delta_mode_t)99:WL_DELTA_FORCE_FULL;
                        }
                CHECK(vars == 1);
                variable_fixture_t f = {0};
                CHECK(!wl_evaluation_control_create(UINT64_C(1000000000),
                    &f.p.control));
                wl_session_options_t options; wl_session_options_init(&options);
                options.evaluation_control = f.p.control;
                CHECK(!wl_session_create_with_options(wl_backend_columnar(),
                    plan, 1, &options, &f.p.session));
                wl_col_session_t *session = COL_SESSION(f.p.session);
                if (!mode && !seeded && !d) {
                    wl_session_set_delta_cb(f.p.session, delta, &f.p);
                    CHECK(!finish(&f.p));
                }
                int64_t left[] = {1, 2, 3},
                    right[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                    delta_rows[] = {7, 8, 9, 10, 11};
                CHECK(!wl_session_insert(f.p.session, "left", left, 3, 1) &&
                    !wl_session_insert(f.p.session, "right", right, 11, 1));
                col_rel_t *full = session_find_rel(session, "left"),
                    *delta = NULL;
                if (extents[d] >= 0) {
                    delta = col_rel_new_like("$d$left", full); CHECK(delta);
                    delta->nrows = delta->base_nrows = delta->sorted_nrows =
                        delta->run_count = 0;
                    bool denied = false;
                    CHECK(!col_rel_append_rows_atomic(delta, delta_rows,
                        (uint32_t)extents[d], 1, &denied) &&
                        !session_add_rel(session, delta));
                }
                session->delta_seeded = seeded != 0;
                bool use_delta = extents[d] > 0 &&
                    (mode == 2 || ((mode == 0 || mode == 4) && extents[d] < 3));
                f.full = full; f.expected = use_delta?delta:full;
                f.snapshot = wl_columnar_relation_snapshot(f.expected);
                f.empty = mode == 3 || (mode == 2 && extents[d] <= 0 && seeded);
                f.is_delta = use_delta || (mode == 2 && f.empty);
                variable_active = &f;
                wl_columnar_session_test_framed_grant_finished =
                    variable_finished;
                int rc = finish(&f.p);
                wl_columnar_session_test_framed_grant_finished = NULL;
                variable_active = NULL;
                fprintf(stdout,
                    "VARIABLE selector=%u seed=%u delta=%d rc=%d held=%u\n",
                    mode, seeded, extents[d], rc, f.observed);
                CHECK(!rc && f.observed);
                const col_rel_t *out = session_find_rel(session, "out");
                CHECK(out && out->ncols == 1 &&
                    out->nrows == (f.empty?0u:f.expected->nrows));
                unsigned seen = 0;
                for (uint32_t row = 0; row < out->nrows; ++row) {
                    int64_t value = col_rel_get(out, row, 0),
                        base = use_delta?7:1;
                    CHECK(value >= base &&
                        value < base+(int64_t)f.expected->nrows);
                    unsigned bit = 1u<<(unsigned)(value-base);
                    CHECK(!(seen&bit)); seen |= bit;
                }
                if (!mode && !seeded && !d) {
                    int64_t added = 4;
                    CHECK(!wl_session_insert(f.p.session, "left", &added, 1,
                        1));
                    f.observed = 0; variable_append_reads = 0;
                    variable_active = &f;
                    variable_append_wait = true;
                    wl_columnar_session_test_observed_transaction_checkpoint =
                        variable_append;
                    wl_columnar_session_test_framed_grant_finished =
                        variable_finished;
                    CHECK(!finish(&f.p));
                    wl_columnar_session_test_observed_transaction_checkpoint =
                        NULL;
                    wl_columnar_session_test_framed_grant_finished = NULL;
                    variable_append_wait = false; variable_active = NULL;
                    CHECK(f.observed && variable_append_reads);
                    out = session_find_rel(session, "out");
                    CHECK(out && out->nrows == 4);
                    unsigned mask = 0;
                    for (uint32_t row = 0; row < out->nrows; ++row) {
                        int64_t k = col_rel_get(out, row, 0);
                        CHECK(k >= 1 && k <= 4);
                        unsigned bit = 1u<<((unsigned)k-1); CHECK(!(mask&bit));
                        mask |= bit;
                    }
                    CHECK(mask == 15 && !finish(&f.p));
                }
                CHECK(!wl_session_try_destroy(&f.p.session));
                wl_evaluation_control_release(f.p.control);
                wl_plan_free(plan); wirelog_program_free(program);
            }
}

/* Raw ordinary permissive policy records its real exclusion reason; controlled
 * strict/owner/source interfaces are covered separately, without equating stats. */
static void ordinary_fallback_reasons(void) {
    for (unsigned filtered = 0; filtered < 2; ++filtered) {
        char text[512]; snprintf(text, sizeof(text),
            ".decl left(k:int64)\n.decl right(k:int64,f:int64)\n.decl out(k:int64)\n"
            "out(k):-left(k),right(k,%s).\n", filtered?"1":"f");
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        CHECK(program); wl_plan_t *plan = NULL;
        CHECK(!wl_plan_from_program(program, &plan));
        unsigned joins = 0;
        for (uint32_t st = 0; st < plan->stratum_count; ++st)
            for (uint32_t r = 0; r < plan->strata[st].relation_count; ++r)
                for (uint32_t o = 0; o < plan->strata[st].relations[r].op_count;
                    ++o) {
                    const wl_plan_op_t *op =
                        &plan->strata[st].relations[r].ops[o];
                    if (op->op == WL_PLAN_OP_JOIN) {
                        joins++;
                        CHECK((op->right_filter_expr.size != 0) ==
                            (filtered != 0));
                    }
                }
        CHECK(joins == 1); wl_session_t *base = NULL;
        CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &base));
        wl_col_session_t *session = COL_SESSION(base);
        session->join_batch_bytes = 64; session->join_batch_strict = false;
        const int64_t left[] = {1, 2, 3}, right[] = {1, 1, 2, 1, 3, 0},
            delta[] = {2, 1};
        CHECK(!wl_session_insert(base, "left", left, 3,
            1) && !wl_session_insert(base, "right", right, 3, 2));
        col_rel_t *canonical = session_find_rel(session, "right");
        CHECK(canonical);

        if (!filtered) {
            col_rel_t *selected = col_rel_new_like("$d$right", canonical);
            CHECK(selected);
            selected->nrows = selected->base_nrows = selected->sorted_nrows =
                selected->run_count = 0;
            bool denied = false;
            CHECK(!col_rel_append_rows_atomic(selected, delta, 1, 2, &denied) &&
                !session_add_rel(session, selected));
        }
        CHECK(!wl_session_step(base));
        unsigned expected = filtered?3u:2u, seen = 0;
        const col_rel_t *out = session_find_rel(session, "out");
        CHECK(out && out->ncols == 1);
        for (uint32_t r = 0; r < out->nrows; ++r) {
            int64_t key = col_rel_get(out, r, 0); CHECK(key >= 1 && key <= 3);
            unsigned bit = 1u<<((unsigned)key-1); CHECK(!(seen&bit));
            seen |= bit;
        }
        fprintf(stdout,
            "raw ordinary fallback filtered=%u count=%u reason=%u mask=%u\n",
            filtered,
            session->join_batch_fallback_count, session->join_batch_last_reason,
            seen);
        CHECK(seen == expected && session->join_batch_fallback_count == 1 &&
            session->join_batch_last_reason ==
            (filtered?COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT:
            COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT) &&
            canonical->nrows == 3 && canonical->ncols == 2 &&
            !atomic_load(&canonical->source_access.state) &&
            !atomic_load(&canonical->descriptor_access.state));
        unsigned source_keys = 0;
        for (uint32_t row = 0; row < canonical->nrows; ++row) {
            int64_t key = col_rel_get(canonical, row, 0);
            CHECK(key >= 1 && key <= 3 &&
                col_rel_get(canonical, row, 1) == (key == 3?0:1));
            unsigned bit = 1u<<((unsigned)key-1); CHECK(!(source_keys&bit));
            source_keys |= bit;
        }
        CHECK(source_keys == 7);
        CHECK(!wl_session_try_destroy(&base)); wl_plan_free(plan);
        wirelog_program_free(program);
    }
}

typedef struct {
    profile_t p;
    unsigned builds, reuses, indexed, probes, private_rows, source_mask;
} cache_cycle_t;
static cache_cycle_t *cache_cycle_active;
static void cache_cycle_source(wl_evaluation_control_t *control) {
    cache_cycle_t *f = cache_cycle_active; CHECK(f && control == f->p.control);
    wl_col_session_t *s = COL_SESSION(f->p.session);
    wl_columnar_eval_serial_framed_attempt_t *r = NULL;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(s->cleanup_active, s, &r) &&
        r->capture_only && r->child_bound && r->private_bindings &&
        wl_columnar_session_observed_bindings_authorized(s,
        r->private_bindings) &&
        !wl_columnar_eval_stack_filter_context_owned(s->cleanup_active, &ctx,
        &r->child) &&
        ctx->join_operation == &r->child.plan->ops[r->child.cursor] &&
        ctx->join_operation->op == WL_PLAN_OP_JOIN &&
        ctx->join_operation->key_count == 1 &&
        !ctx->join_operation->right_filter_expr.size);
    const wl_columnar_relation_scalar_source_t *source = &ctx->join_right;
    const col_rel_t *right = session_find_rel(s, "right");
    CHECK(source->identity == source &&
        source->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD &&
        source->descriptor == right && source->width == 2 &&
        source->nrows == 2 &&
        source->owner_reader.owner == &right->storage_owner->source_access &&
        source->descriptor_reader.owner == &right->descriptor_access);
    unsigned keys = 0;
    for (uint32_t row = 0; row < 2; ++row) {
        int64_t key = col_rel_get(right, row, 0); CHECK(key >= 1 && key <= 3 &&
            col_rel_get(right, row, 1) == 10*key);
        unsigned bit = 1u<<((unsigned)key-1); CHECK(!(keys&bit)); keys |= bit;
    }
    CHECK(keys == f->source_mask);
}
static void cache_cycle_arrangement(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress) {
    (void)progress; if (phase != 13 && phase != 14)return;
    cache_cycle_source(control);
    if (phase == 14)++cache_cycle_active->builds;
    else ++cache_cycle_active->reuses;
}
static void cache_cycle_join(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress) {
    (void)progress; if (phase != 28 && phase != 47 && phase != 29)return;
    cache_cycle_source(control);
    if (phase == 28)++cache_cycle_active->private_rows;
    else if (phase == 47)++cache_cycle_active->indexed;
    else ++cache_cycle_active->probes;
}
static void cache_cycle_evaluate(cache_cycle_t *f, unsigned old, unsigned want,
    unsigned builds, unsigned reuses) {
    f->builds = f->reuses = f->indexed = f->probes = f->private_rows = 0;
    cache_cycle_active = f;
    wl_columnar_arrangement_test_diff_checkpoint = cache_cycle_arrangement;
    wl_columnar_join_test_attempt_checkpoint = cache_cycle_join;
    CHECK(!finish(&f->p)); content(&f->p, want);
    fprintf(stdout,
        "CACHE CYCLE mask=%u build=%u/%u reuse=%u/%u indexed=%u probes=%u private=%u count=%u\n",
        want, f->builds, builds, f->reuses, reuses, f->indexed, f->probes,
        f->private_rows, COL_SESSION(f->p.session)->diff_arr_count);
    CHECK(f->builds == builds && f->reuses == reuses &&
        f->indexed == 2u*builds &&
        f->probes && !f->private_rows);
    for (unsigned i = 1; i <= 4; ++i) {
        int change = (int)((want>>(i-1))&1u)-(int)((old>>(i-1))&1u);
        CHECK(f->p.events[i] == change && f->p.calls[i] == (change?1u:0u));
    }
    memset(f->p.events, 0, sizeof(f->p.events));
    memset(f->p.calls, 0, sizeof(f->p.calls));
    f->builds = f->reuses = f->indexed = f->probes = f->private_rows = 0;
    CHECK(!finish(&f->p)); content(&f->p, want);
    CHECK(!f->builds && !f->reuses && !f->indexed && !f->probes &&
        !f->private_rows);
    for (unsigned i = 1; i <= 4; ++i)CHECK(!f->p.events[i] && !f->p.calls[i]);
    wl_columnar_arrangement_test_diff_checkpoint = NULL;
    wl_columnar_join_test_attempt_checkpoint = NULL;
    cache_cycle_active = NULL;
}
static void cache_clear_cycle(void) {
    wirelog_error_t error; wirelog_program_t *program = wirelog_parse_string(
        ".decl left(k:int64)\n.decl right(k:int64,v:int64)\n.decl out(k:int64)\n"
        "out(k):-left(k),right(k,v).\n", &error);
    CHECK(program); wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    cache_cycle_t f = {0}; wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f.p.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.p.governor && !wl_evaluation_control_create(UINT64_C(1000000000),
        &f.p.control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = f.p.governor;
    options.evaluation_control = f.p.control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &f.p.session));
    wl_session_set_delta_cb(f.p.session, delta, &f.p); CHECK(!finish(&f.p));
    const int64_t left[] = {1, 2, 3}, right[] = {1, 10, 2, 20},
        removed[] = {1, 10}, replacement[] = {3, 30};
    CHECK(!wl_session_insert(f.p.session, "left", left, 3, 1) &&
        !wl_session_insert(f.p.session, "right", right, 2, 2));
    f.source_mask = 3; cache_cycle_evaluate(&f, 0, 3, 1, 0);
    wl_col_session_t *session = COL_SESSION(f.p.session);
    CHECK(session->diff_arr_count == 1);
    col_relation_snapshot_t old_snapshot =
        session->diff_arr_entries[0].diff_arr->source_snapshot;
    CHECK(!wl_session_remove(f.p.session, "right", removed, 1, 2) &&
        !wl_session_insert(f.p.session, "right", replacement, 1, 2));
    f.source_mask = 6; cache_cycle_evaluate(&f, 3, 6, 1, 0);
    col_rel_t *canonical = session_find_rel(session, "right");
    CHECK(canonical->nrows == 2 &&
        !wl_columnar_relation_snapshot_equal(old_snapshot,
        wl_columnar_relation_snapshot(canonical)));
    col_relation_snapshot_t snapshot = wl_columnar_relation_snapshot(canonical);
    int64_t extra = 4;
    CHECK(!wl_session_insert(f.p.session, "left", &extra, 1, 1));
    cache_cycle_evaluate(&f, 6, 6, 0, 1);
    CHECK(wl_columnar_relation_snapshot_equal(snapshot,
        wl_columnar_relation_snapshot(canonical)));
    CHECK(session->diff_arr_count == 1 && !session->diff_txn_count);
    col_diff_arr_entry_t *entry = &session->diff_arr_entries[0];
    col_diff_arrangement_t *index = entry->diff_arr;
    CHECK(index && index->indexed_rows == 2 && index->nbuckets <= 8 &&
        index->ht_cap <= 8);
    uint32_t heads[8], next[8];
    memcpy(heads, index->ht_head, index->nbuckets*sizeof(*heads));
    memcpy(next, index->ht_next, index->ht_cap*sizeof(*next));
    uint64_t generation = entry->generation;
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(f.p.governor);
    uint64_t baseline = wl_columnar_memory_reserved(g);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(canonical, &reader));
    wl_columnar_arrangement_diff_txn_t txn = {0}; uint32_t key = 0;
    CHECK(!wl_columnar_arrangement_diff_txn_begin(session, "right", canonical,
        &key, 1, &txn) &&
        txn.working && txn.entry == entry && entry->transaction_pending &&
        session->diff_txn_count == 1);
    col_diff_arrangement_t *working = txn.working;
    CHECK(col_session_free_diff_arrangements(session) == EBUSY &&
        txn.working == working &&
        txn.entry == entry && session->diff_txn_count == 1 &&
        entry->transaction_pending &&
        entry->diff_arr == index && entry->generation == generation &&
        wl_columnar_relation_snapshot_equal(snapshot, index->source_snapshot) &&
        !memcmp(heads, index->ht_head, index->nbuckets*sizeof(*heads)) &&
        !memcmp(next, index->ht_next, index->ht_cap*sizeof(*next)));
    wl_columnar_arrangement_diff_txn_abort(&txn);
    CHECK(!col_rel_source_reader_release(&reader));
    CHECK(!session->diff_txn_count && !entry->transaction_pending &&
        entry->diff_arr == index && wl_columnar_memory_reserved(g) == baseline);
    col_diff_arrangement_pin_t pin = {0};
    CHECK(!col_session_pin_diff_arrangement(session,
        "right", canonical, &key, 1, &pin));
    CHECK(pin.active && pin.entry == entry && pin.arr == index &&
        pin.source == canonical &&
        pin.source_reader.identity == (uintptr_t)&pin.source_reader &&
        pin.source_reader.owner == &canonical->storage_owner->source_access &&
        pin.source_reader.secondary_owner == &canonical->descriptor_access &&
        wl_columnar_relation_snapshot_equal(snapshot, pin.source_snapshot) &&
        atomic_load(&canonical->source_access.state) == 1 &&
        atomic_load(&canonical->descriptor_access.state) == 1 &&
        entry->pin_count == 1 &&
        col_session_free_diff_arrangements(session) == EBUSY && pin.active &&
        entry->generation == generation && entry->pin_count == 1 &&
        entry->diff_arr == index &&
        !entry->transaction_pending && !entry->invalidation_deferred &&
        wl_columnar_relation_snapshot_equal(snapshot, index->source_snapshot) &&
        !memcmp(heads, index->ht_head, index->nbuckets*sizeof(*heads)) &&
        !memcmp(next, index->ht_next, index->ht_cap*sizeof(*next)));
    col_diff_arrangement_pin_release(&pin);
    CHECK(!entry->pin_count && !pin.active &&
        !atomic_load(&canonical->source_access.state) &&
        !atomic_load(&canonical->descriptor_access.state));
    uint64_t owned = index->reserved_bytes+
        session->diff_arr_registry_reservation.bytes;
    CHECK(index->reservation.bytes == index->reserved_bytes &&
        baseline >= owned &&
        !col_session_free_diff_arrangements(session) &&
        !session->diff_arr_entries &&
        !session->diff_arr_count && !session->diff_arr_cap &&
        !session->diff_arr_registry_reserved_bytes &&
        !session->diff_arr_registry_metadata_reserved_bytes &&
        wl_columnar_memory_reserved(g) == baseline-owned);
    extra = -1; CHECK(!wl_session_insert(f.p.session, "left", &extra, 1, 1));
    cache_cycle_evaluate(&f, 6, 6, 1, 0);
    CHECK(session->diff_arr_count == 1 &&
        wl_columnar_relation_snapshot_equal(snapshot,
        wl_columnar_relation_snapshot(canonical)));
    index = session->diff_arr_entries[0].diff_arr;
    generation = session->diff_arr_entries[0].generation;
    extra = -2; CHECK(!wl_session_insert(f.p.session, "left", &extra, 1, 1));
    cache_cycle_evaluate(&f, 6, 6, 0, 1);
    CHECK(session->diff_arr_entries[0].diff_arr == index &&
        session->diff_arr_entries[0].generation == generation &&
        wl_columnar_relation_snapshot_equal(snapshot, index->source_snapshot));
    CHECK(!wl_session_try_destroy(&f.p.session));
    wl_evaluation_control_release(f.p.control);
    wl_plan_free(plan); wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(g));
    wl_columnar_memory_governor_ref_release(f.p.governor);
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
    } else if (!strcmp(argv[1], "cache-clear-cycle")) {
        cache_clear_cycle();
    } else if (!strcmp(argv[1], "fallbacks")) {
        ordinary_fallback_reasons();
    } else if (!strcmp(argv[1], "selectors")) {
        variable_selectors();
    } else if (!strcmp(argv[1], "expression-faults")) {
        for (expression_fault = 1; expression_fault <= 4; ++expression_fault)
            expression_case("cat(s,\"!\")=\"yes!\"", 1, true);
        expression_fault = 0;
    } else if (!strcmp(argv[1], "expressions")) {
        expression_cases();
    } else {
        CHECK(!strcmp(argv[1], "probe-chains")); for (unsigned kind = 1;
            kind < 3; ++kind) {
            chain_case(33, -1, kind); chain_case(33, 0, kind);
            chain_case(33, 32, kind);
        }
    }
    return 0;
}
