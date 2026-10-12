/* #1821: genuine generated ordinary membership production callers. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"
#include <stdarg.h>
#include <stdio.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); exit(1); } } while (0)
static const char *k_semijoin_chain_src
    = ".decl a(p1: int64, p2: int64)\n"
    "a(1, 2).\n"
    ".decl b(q1: int64, q2: int64)\n"
    "b(2, 3).\n"
    ".decl c(r1: int64, r2: int64)\n"
    "c(3, 4).\n"
    ".decl d(s1: int64, s2: int64)\n"
    "d(4, 5).\n"
    ".decl e(t1: int64, t2: int64)\n"
    "e(5, 6).\n"
    ".decl chain(c1: int64, c2: int64, c3: int64, c4: int64, c5: int64,"
    " c6: int64)\n"
    "chain(v1, v2, v3, v4, v5, v6) :- a(v1, v2), b(v2, v3), c(v3, v4),"
    " d(v4, v5), e(v5, v6).\n";
static const char *k_semijoin_negation_src
    = ".decl a(p1: int64, p2: int64)\n"
    "a(1, 2).\n"
    ".decl b(q1: int64, q2: int64)\n"
    "b(2, 3).\n"
    ".decl n(z1: int64)\n"
    "n(99).\n"
    ".decl c(r1: int64, r2: int64)\n"
    "c(3, 4).\n"
    ".decl d(s1: int64, s2: int64)\n"
    "d(4, 5).\n"
    ".decl chain(c1: int64, c2: int64, c3: int64, c4: int64, c5: int64)\n"
    "chain(v1, v2, v3, v4, v5) :- a(v1, v2), b(v2, v3), !n(v3),"
    " c(v3, v4), d(v4, v5).\n";

typedef struct wl_membership_capture {
    uint64_t count, digest;
    uint32_t width;
} wl_membership_capture_t;
static void capture(const char *name, const int64_t *row, uint32_t width,
    void *data)
{
    wl_membership_capture_t *c = data;
    CHECK(name && row && width == c->width);
    for (const unsigned char *p = (const unsigned char *)name; *p; p++)
        c->digest = c->digest * UINT64_C(1099511628211) ^ *p;
    for (uint32_t i = 0; i < width; i++)
        c->digest = c->digest * UINT64_C(1099511628211) ^ (uint64_t)row[i];
    c->count++;
}
static uint64_t enforcing_budget;
static wl_col_session_t *create(const wl_plan_t *plan, unsigned kind)
{
    wl_session_t *base = NULL;
    if (enforcing_budget) {
        wl_columnar_memory_resolution_t resolution =
        {.budget_bytes = enforcing_budget,
         .usable_bytes =
             enforcing_budget, .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor);
        wl_session_options_t options; wl_session_options_init(&options);
        options.memory_governor = governor;
        CHECK(wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &base) == 0);
        wl_columnar_memory_governor_ref_release(governor);
    } else CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    CHECK(s->eval_arena && s->delta_pool && !s->has_evaluated);
    CHECK(s->nrels == plan->edb_count);
    CHECK(!session_find_rel(s, kind == 2 ? "H" : "chain"));
    const char *names[] = {"a", "b", "c", "d", "e"};
    int64_t values[][2] = {{1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}};
    if (kind < 2) {
        for (unsigned i = 0; i < (kind ? 4u : 5u); i++)
            CHECK(wl_backend_columnar()->session_insert(base, names[i],
                values[i], 1, 2) == 0);
        if (kind) {
            int64_t n = 99;
            CHECK(wl_backend_columnar()->session_insert(base, "n", &n, 1,
                1) == 0);
        }
    } else {
        int64_t a[] = {1, 7}, b = 99;
        CHECK(wl_backend_columnar()->session_insert(base, "A", a, 1, 2) == 0);
        CHECK(wl_backend_columnar()->session_insert(base, "B", &b, 1, 1) == 0);
    }
    return s;
}
static void destroy(wl_col_session_t *s)
{
    wl_evaluation_control_t *c = s->base.evaluation_control;
    if (c) {
        CHECK(wl_evaluation_control_detach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = NULL;
        wl_evaluation_control_release(c);
    }
    wl_session_destroy(&s->base);
}
static void generated(unsigned kind, bool snapshot)
{
    const char *small =
        ".decl A(k:int64,v:int64)\n.decl B(k:int64)\n.decl H(v:int64)\nH(v):-A(k,v),!B(k).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(kind == 0 ?
            k_semijoin_chain_src : kind == 1 ? k_semijoin_negation_src : small,
            &error);
    CHECK(program && error == WIRELOG_OK);
    wl_fusion_apply(program, NULL); wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    uint32_t semi = 0, anti = 0, rules = 0;
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        CHECK(!plan->strata[si].is_recursive);
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ri++) {
            const wl_plan_relation_t *r = &plan->strata[si].relations[ri];
            CHECK(r->op_count == (kind == 2 ? 4u : 9u)); rules++;
            for (uint32_t i = 0; i < r->op_count; i++) {
                semi += r->ops[i].op == WL_PLAN_OP_SEMIJOIN;
                anti += r->ops[i].op == WL_PLAN_OP_ANTIJOIN;
                CHECK(r->ops[i].delta_mode == WL_DELTA_AUTO);
            }
        }
    }
    CHECK(rules == 1 && semi == (kind == 0 ? 3u : kind == 1 ? 2u : 0u));
    CHECK(anti == (kind ? 1u : 0u));
    wl_col_session_t *s = create(plan, kind), *raw = create(plan, kind);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0);
    CHECK(wl_evaluation_control_attach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = control;
    wl_arena_t *arena = s->eval_arena;
    delta_pool_t *pool = s->delta_pool;
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        wl_membership_capture_t got = {.width = kind == 0 ? 6u : kind ==
                                           1 ? 5u : 1u},
                                           twin = {.width = got.width};
        int rc = snapshot ? wl_backend_columnar()->session_snapshot(&s->base,
                capture, &got) : wl_backend_columnar()->session_step(&s->base);
        if (rc) fprintf(stderr,
                "membership kind=%u snapshot=%u attempt=%u rc=%d\n", kind,
                snapshot, attempt, rc);
        CHECK(rc == 0);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(control, &report) == 0
            && report.result == 0 && report.execution_status == 0 &&
            report.consumed);
        CHECK((snapshot ? wl_backend_columnar()->session_snapshot(&raw->base,
            capture,
            &twin) : wl_backend_columnar()->session_step(&raw->base)) == 0);
        if (!snapshot) {
            CHECK(wl_backend_columnar()->session_snapshot(&s->base, capture,
                &got) == 0);
            CHECK(wl_backend_columnar()->session_snapshot(&raw->base, capture,
                &twin) == 0);
        }
        CHECK(got.count == twin.count && got.digest == twin.digest);
        wl_membership_capture_t expected_rows = {.width = got.width};
        if (!(kind == 2 && attempt)) {
            int64_t expected_row[6] = {1, 2, 3, 4, 5, 6};
            if (kind == 2) expected_row[0] = 7;
            capture(kind == 2 ? "H" : "chain", expected_row, got.width,
                &expected_rows);
            if (attempt && kind < 2) {
                expected_row[got.width - 1u]++;
                capture("chain", expected_row, got.width, &expected_rows);
            }
        }
        CHECK(got.count == expected_rows.count &&
            got.digest == expected_rows.digest);
        CHECK(got.count == (kind == 2 && attempt ? 0u : attempt ? 2u : 1u));
        CHECK(s->eval_arena == arena && s->delta_pool == pool);
        if (enforcing_budget) {
            wl_columnar_memory_governor_t *gov =
                wl_columnar_memory_governor_ref_get(s->memory_governor),
                *raw_gov =
                wl_columnar_memory_governor_ref_get(raw->memory_governor);
            CHECK(gov->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
                && raw_gov->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
                && gov->budget_bytes == enforcing_budget &&
                raw_gov->budget_bytes == enforcing_budget);
            CHECK(wl_columnar_memory_reserved(gov) <= enforcing_budget
                && wl_columnar_memory_reserved(raw_gov) <= enforcing_budget);
            CHECK(!s->cleanup_active && !s->cleanup_pending &&
                !s->serial_completion.frame);
            for (uint32_t i = 0; i < s->nrels; i++)
                CHECK(s->rels[i]->memory_governor == s->memory_governor);
        }
        if (!attempt) {
            int64_t row[] = {kind == 0 ? 5 : 4, kind == 0 ? 7 : 6};
            int64_t b = 1;
            const char *name = kind == 0 ? "e" : kind == 1 ? "d" : "B";
            CHECK(wl_backend_columnar()->session_insert(&s->base, name,
                kind == 2 ? &b : row, 1, kind == 2 ? 1 : 2) == 0);
            CHECK(wl_backend_columnar()->session_insert(&raw->base, name,
                kind == 2 ? &b : row, 1, kind == 2 ? 1 : 2) == 0);
            CHECK(wl_evaluation_control_reset(control) == 0);
        }
    }
    destroy(s); destroy(raw); wl_plan_free(plan); wirelog_program_free(program);
}

static int stop_stage;
static unsigned stop_seen;
static uint64_t completed_candidates;
static void stop_checkpoint(wl_columnar_join_membership_record_t *r,
    int stage, uint64_t progress)
{
    CHECK(r && r->ready && r->session && r->control);
    if (stage == 11) completed_candidates = progress;
    if (stage == stop_stage && progress == 257) {
        CHECK(!stop_seen++);
        if (stage == 11) CHECK(completed_candidates == 257);
        wl_evaluation_control_request_cancel(r->control);
    }
}
static wl_columnar_eval_stack_cleanup_frame_t *frame(wl_col_session_t *s,
    col_rel_t *left)
{
    wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &f) == 0);
    CHECK(eval_stack_push(wl_columnar_eval_stack_cleanup_stack(f), left,
        false) == 0);
    return f;
}
static void unpinned(col_rel_t *r)
{
    wl_columnar_source_access_writer_t w = {0};
    CHECK(wl_columnar_source_access_writer_acquire(&r->source_access, &w) == 0);
    CHECK(wl_columnar_source_access_writer_release(&w) == 0);
}
/* Invoke the actual dispatched production kernel with a supplied immutable
 * legacy operator; genuine generated publication is covered separately. */
static void collision_stop(bool anti, int stage)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl A(k:int64,v:int64)\n.decl B(k:int64)\n.decl H(v:int64)\nH(v):-A(k,v),!B(k).\n",
        &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_col_session_t *s = create(plan, 2);
    col_rel_t *left = session_find_rel(s, "A"),
        *right = session_find_rel(s, "B");
    /* 301 right rows imply the actual legacy power-of-two bucket count1024.
     * Every added key is unequal to1 but hashes to its exact bucket. */
    uint32_t target = wl_columnar_hash_value(2166136261u, right, 0, 1) & 1023u;
    int64_t data[300]; uint32_t n = 0;
    for (int64_t value = 2; n < 300; value++)
        if ((wl_columnar_hash_value(2166136261u, right, 0,
            value) & 1023u) == target)
            data[n++] = value;
    CHECK(wl_backend_columnar()->session_insert(&s->base, "B", data, 300,
        1) == 0);
    CHECK(right->nrows == 301);
    for (uint32_t i = 0; i < 300; i++) CHECK(data[i] != 1);
    wl_columnar_eval_stack_cleanup_frame_t *f = frame(s, left);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    const char *keys[] = {"col0"};
    wl_plan_op_t op = {.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN,
                       .right_relation = "B", .key_count = 1, .left_keys = keys,
                       .right_keys = keys, .delta_mode = WL_DELTA_AUTO};
    stop_stage = stage; stop_seen = 0; completed_candidates = 0;
    wl_columnar_join_membership_test_checkpoint = stop_checkpoint;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
    int rc = anti ? wl_columnar_antijoin_op(&op, stack, s) :
        wl_columnar_semijoin_op(&op, stack, s);
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && stop_seen == 1);
    CHECK(stack->top == 1 && stack->items[0].rel == left);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(f);
    CHECK(result->rel && !result->rel->nrows);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c, &report) == 0 && report.result == rc);
    wl_columnar_join_membership_test_checkpoint = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0 && !f);
    unpinned(left); unpinned(right);
    CHECK(!s->cleanup_pending && !s->cleanup_active);
    CHECK(wl_evaluation_control_reset(c) == 0);
    s->base.evaluation_control = NULL;
    f = frame(s, left);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    stack = wl_columnar_eval_stack_cleanup_stack(f);
    CHECK((anti ? wl_columnar_antijoin_op(&op, stack, s) :
        wl_columnar_semijoin_op(&op, stack, s)) == 0);
    CHECK(stack->top == 1 && stack->items[0].owned);
    col_rel_t *out = stack->items[0].rel;
    CHECK(out->nrows == (anti ? 1u : 0u));
    if (anti) CHECK(out->columns[0][0] == 1 && out->columns[1][0] == 7);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    /* Raw twin uses the same immutable source context after true checked drain. */
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
    s->base.evaluation_control = NULL;
    f = frame(s, left); stack = wl_columnar_eval_stack_cleanup_stack(f);
    CHECK((anti ? wl_columnar_antijoin_op(&op, stack, s) :
        wl_columnar_semijoin_op(&op, stack, s)) == 0);
    CHECK(stack->items[0].rel->nrows == (anti ? 1u : 0u));
    if (anti) CHECK(stack->items[0].rel->columns[0][0] == 1
            && stack->items[0].rel->columns[1][0] == 7);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
    s->base.evaluation_control = c;
    unpinned(left); unpinned(right); destroy(s);
    wl_plan_free(plan); wirelog_program_free(program);
}

extern bool wl_columnar_join_test_fail_next_key_scratch_alloc;
static bool primary_overflow, reserved_fault;
static unsigned primary_seen, fault_seen;
static wl_columnar_join_membership_record_t *fault_record;
static wl_columnar_memory_reservation_t *fault_token;
static const void *fault_identity;
static void authority_checkpoint(wl_columnar_join_membership_record_t *r,
    int stage, uint64_t progress)
{
    (void)progress;
    if (stage == 13 && !primary_seen++) {
        if (primary_overflow)
            CHECK(wl_evaluation_control_charge(r->control,
                UINT64_MAX) == EOVERFLOW);
        else wl_evaluation_control_request_cancel(r->control);
    }
    if (stage == 1 && (primary_seen || reserved_fault) && !fault_seen) {
        CHECK(!r->left_keys && !r->right_keys && !r->types_payload);
        CHECK(r->keys.bytes && atomic_load(&r->keys.state) ==
            (reserved_fault ? WL_COLUMNAR_MEMORY_RESERVATION_RESERVED :
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED));
        fault_record = r; fault_token = &r->keys;
        fault_identity = fault_token->identity;
        CHECK(fault_identity == fault_token);
        fault_token->identity = NULL; fault_seen++;
    }
}
static void authority_fault(unsigned mode)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl A(k:int64,v:int64)\n.decl B(k:int64)\n.decl H(v:int64)\nH(v):-A(k,v),!B(k).\n",
        &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_col_session_t *s = create(plan, 2), *raw = create(plan, 2);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    bool positive = mode == 1; reserved_fault = mode == 2;
    wl_columnar_join_test_fail_next_key_scratch_alloc = reserved_fault;
    primary_overflow = positive; primary_seen = fault_seen = 0;
    fault_record = NULL; fault_token = NULL; fault_identity = NULL;
    wl_columnar_join_membership_test_checkpoint = authority_checkpoint;
    wl_membership_capture_t got = {.width = 1}, twin = {.width = 1};
    int rc = wl_backend_columnar()->session_snapshot(&s->base, capture, &got);
    CHECK(rc == (reserved_fault ? ENOMEM : positive ? EOVERFLOW : EINVAL));
    CHECK((primary_seen || reserved_fault) && fault_seen == 1 && !got.count
        && !wl_columnar_join_test_fail_next_key_scratch_alloc);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c, &report) == 0
        && report.result == rc && report.execution_status == rc);
    CHECK(s->cleanup_pending && fault_record && fault_token
        && !fault_record->left_keys && !fault_record->right_keys
        && fault_token->identity == NULL && fault_token->bytes);
    CHECK(atomic_load(&fault_token->state) ==
        (reserved_fault ? WL_COLUMNAR_MEMORY_RESERVATION_RESERVED :
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED));
    /* Only restore the saved original scalar after the serialized return. */
    wl_columnar_join_membership_test_checkpoint = NULL;
    fault_token->identity = fault_identity;
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
    CHECK(!s->cleanup_pending && !s->cleanup_active);
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_backend_columnar()->session_snapshot(&s->base, capture,
        &got) == 0);
    CHECK(wl_backend_columnar()->session_snapshot(&raw->base, capture,
        &twin) == 0);
    CHECK(got.count == 1 && got.count == twin.count &&
        got.digest == twin.digest);
    unpinned(session_find_rel(s, "A")); unpinned(session_find_rel(s, "B"));
    destroy(s); destroy(raw); wl_plan_free(plan); wirelog_program_free(program);
}

extern void (*wl_columnar_filter_test_controlled_checkpoint)(
    wl_evaluation_control_t *, int, uint64_t,
    wl_columnar_relation_source_group_t *, col_rel_t *);
static wl_col_session_t *filter_session;
static unsigned filter_mode, filter_rows, filter_gathers, filter_stop_seen;
static bool filter_cancel;
static void right_filter_checkpoint(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_relation_source_group_t *group,
    col_rel_t *source)
{
    if (stage != 3 && stage != 4) return;
    CHECK(group && source);
    if (stage == 4) {
        filter_gathers++;
        CHECK(progress <= source->ncols);
    }
    if (stage != 3) return;
    filter_rows++;
    wl_columnar_expr_operator_preparation_t *prep =
        wl_columnar_eval_stack_cleanup_operator_preparation(
        filter_session->cleanup_active);
    CHECK(prep && prep->ready);
    CHECK((prep->scratch_payload != NULL) == (source->ncols > 32));
    CHECK((prep->single != NULL) == (filter_mode == 1 || filter_mode == 4));
    if (filter_cancel && progress == 257) {
        CHECK(!filter_stop_seen++); wl_evaluation_control_request_cancel(c);
    }
}
static unsigned filtered_handoff_seen;
static uint64_t filtered_built, filtered_compared;
static void passing_membership_checkpoint(
    wl_columnar_join_membership_record_t *record, int stage, uint64_t progress)
{
    if (stage == 7 && progress == 1) {
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(
            record->session->cleanup_active);
        CHECK(record->right_filter_slot == 1 && stack->top == 2);
        eval_entry_t *entry = &stack->items[record->right_filter_slot];
        CHECK(entry->owned && entry->rel && entry->rel->nrows == 200);
        CHECK(entry->rel != session_find_rel(record->session, "B"));
        unsigned ones = 0, twos = 0;
        for (uint32_t row = 0; row < entry->rel->nrows; row++) {
            int64_t key = entry->rel->columns[0][row];
            CHECK(key == 1 || key == 2);
            ones += key == 1; twos += key == 2;
        }
        CHECK(ones == 100 && twos == 100);
        filtered_handoff_seen++;
    }
    if (stage == 8) filtered_built = progress;
    if (stage == 11) filtered_compared = progress;
}
static void right_filter_result(const eval_stack_t *stack, bool anti,
    unsigned mode)
{
    const col_rel_t *out = stack->items[0].rel;
    CHECK(out && out->ncols == 2);
    CHECK(out->nrows == (mode >= 3 ? 1u : anti ? 1u : 0u));
    if (out->nrows) {
        CHECK(out->columns[0][0] == (mode >= 3 && anti ? 99 : 1));
        CHECK(out->columns[1][0] == (mode >= 3 && anti ? 9 : 7));
    }
}
static void wl_membership_append(char *text, size_t capacity, size_t *at,
    const char *format, ...)
{
    CHECK(*at < capacity);
    size_t remaining = capacity - *at;
    va_list args;
    va_start(args, format);
    int written = vsnprintf(text + *at, remaining, format, args);
    va_end(args);
    CHECK(written >= 0);
    CHECK((size_t)written < remaining);
    *at += (size_t)written;
}

static void right_filter(bool anti, uint32_t width, unsigned mode, bool stop)
{
    char text[4096]; size_t at = 0;
    wl_membership_append(text, sizeof(text), &at,
        ".decl A(k:int64,v:int64)\n.decl B(");
    for (uint32_t i = 0; i < width; i++)
        wl_membership_append(text, sizeof(text), &at, "%sb%u:int64",
            i ? "," : "", i);
    wl_membership_append(text, sizeof(text), &at,
        ")\n.decl H(v:int64)\nH(v):-A(k,v),B(");
    for (uint32_t i = 0; i < width; i++)
        wl_membership_append(text, sizeof(text), &at, "%sx%u", i ? "," : "", i);
    wl_membership_append(text, sizeof(text), &at, ").\n");
    CHECK(at < sizeof(text));
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    int64_t a[] = {1, 7, 99, 9};
    CHECK(wl_backend_columnar()->session_insert(base, "A", a,
        mode >= 3 ? 2u : 1u, 2) == 0);
    int64_t *data = malloc((size_t)300 * width * sizeof(*data)); CHECK(data);
    for (uint32_t row = 0; row < 300; row++)
        for (uint32_t i = 0; i < width;
            i++) data[(size_t)row * width + i] = i ? row :
                mode >= 3 ? row % 3 == 0 ? 1 : row % 3 == 1 ? 2 : 99 : 1;
    CHECK(wl_backend_columnar()->session_insert(base, "B", data, 300,
        width) == 0);
    free(data);
    uint8_t simple[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0',
                        WL_PLAN_EXPR_CONST_INT, 0xff, 0xff, 0xff, 0x7f, 0, 0, 0,
                        0, WL_PLAN_EXPR_CMP_GT};
    uint8_t compiled[] = {WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0, 0, 0, 0,
                          WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0, 0, 0, 0, 0,
                          WL_PLAN_EXPR_ARITH_DIV};
    uint8_t interpreted[] = {WL_PLAN_EXPR_CONST_INT, 0xff, 0xff, 0xff, 0xff,
                             0xff, 0xff, 0xff, 0x7f,
                             WL_PLAN_EXPR_STR_FN_STRLEN, WL_PLAN_EXPR_CONST_INT,
                             0, 0, 0, 0, 0, 0, 0, 0,
                             WL_PLAN_EXPR_ARITH_DIV};
    uint8_t passing_simple[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0',
                                WL_PLAN_EXPR_CONST_INT, 3, 0, 0, 0, 0, 0, 0, 0,
                                WL_PLAN_EXPR_CMP_LT};
    uint8_t passing_compiled[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0',
                                  WL_PLAN_EXPR_CONST_INT, 3, 0, 0, 0, 0, 0, 0,
                                  0, WL_PLAN_EXPR_ARITH_MOD};
    uint8_t passing_interpreted[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0',
                                     WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0, 0, 0,
                                     0, 0,
                                     WL_PLAN_EXPR_STR_FN_STRLEN,
                                     WL_PLAN_EXPR_CMP_LT};
    if (mode == 5) {
        int64_t id;
        CHECK(wl_intern_put_checked(s->intern, "abc", &id) == 0 && id >= 0);
        CHECK(!strcmp(wl_intern_reverse(s->intern, id), "abc"));
        for (unsigned byte = 0; byte < 8; byte++)
            passing_interpreted[8u + byte] = (uint8_t)((uint64_t)id >>
                (8u * byte));
    }
    const char *keys[] = {"col0"};
    wl_plan_op_t op = {.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN,
                       .right_relation = "B", .key_count = 1, .left_keys = keys,
                       .right_keys = keys, .delta_mode = WL_DELTA_AUTO,
                       .right_filter_expr = {.data = mode ==
                                                 0 ? simple : mode ==
                                                 1 ? compiled : interpreted,
                                             .size = mode ==
                                                 0 ? sizeof(simple) : mode ==
                                                 1 ? sizeof(compiled) :
                                                 sizeof(interpreted)}};
    if (mode >= 3) op.right_filter_expr = (wl_plan_expr_buffer_t){
            .data = mode == 3 ? passing_simple : mode ==
                4 ? passing_compiled : passing_interpreted,
            .size = mode == 3 ? sizeof(passing_simple) : mode ==
                4 ? sizeof(passing_compiled) : sizeof(passing_interpreted)
        };
    col_rel_t *left = session_find_rel(s, "A"),
        *right = session_find_rel(s, "B");
    wl_columnar_eval_stack_cleanup_frame_t *f = frame(s, left);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    filter_session = s; filter_mode = mode;
    filter_rows = filter_gathers = filter_stop_seen = 0;
    filter_cancel = stop;
    filtered_handoff_seen = 0; filtered_built = filtered_compared = 0;
    if (mode >=
        3) wl_columnar_join_membership_test_checkpoint =
            passing_membership_checkpoint;
    wl_columnar_filter_test_controlled_checkpoint = right_filter_checkpoint;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
    int rc = anti ? wl_columnar_antijoin_op(&op, stack,
            s) : wl_columnar_semijoin_op(&op, stack, s);
    if (rc != (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0))
        fprintf(stderr, "right filter anti=%u width=%u mode=%u stop=%u rc=%d\n",
            anti, width, mode, stop, rc);
    CHECK(rc == (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0));
    CHECK(filter_rows == (stop ? 257u : 300u)
        && filter_gathers == filter_rows * width &&
        filter_stop_seen == (unsigned)stop);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == rc);
    wl_columnar_filter_test_controlled_checkpoint = NULL;
    if (!stop) {
        right_filter_result(stack, anti, mode);
        if (mode >= 3) CHECK(filtered_handoff_seen == 1 &&
                filtered_built == 200 && filtered_compared > 0);
    } else if (mode >= 3) {
        eval_entry_t *partial = wl_columnar_eval_stack_cleanup_result(f);
        CHECK(partial->owned && partial->rel && partial->rel->nrows == 171 &&
            partial->rel->ncols == width);
        CHECK(filtered_handoff_seen == 0 && left->nrows == 2 &&
            right->nrows == 300);
        wl_columnar_expr_operator_preparation_t *prep =
            wl_columnar_eval_stack_cleanup_operator_preparation(f);
        CHECK(prep && prep->ready && !prep->single && !prep->current &&
            !prep->scratch_payload);
        const wl_columnar_memory_reservation_t *tokens[] = {
            &prep->scratch, &prep->compile_admission, &prep->retired_expression
        };
        for (unsigned i = 0; i < 3; i++) {
            uint64_t state = atomic_load_explicit(&tokens[i]->state,
                    memory_order_acquire);
            CHECK(state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY ||
                state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED);
        }
        unpinned(partial->rel);
    }
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
    CHECK(!f && !s->cleanup_active && !s->cleanup_pending &&
        !s->cleanup_reserved_bytes);
    unpinned(left); unpinned(right);
    if (stop) {
        CHECK(wl_evaluation_control_reset(c) == 0);
        s->base.evaluation_control = NULL; f = frame(s, left);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        stack = wl_columnar_eval_stack_cleanup_stack(f);
        CHECK((anti ? wl_columnar_antijoin_op(&op, stack, s) :
            wl_columnar_semijoin_op(&op, stack, s)) == 0);
        right_filter_result(stack, anti, mode);
        if (mode >= 3) CHECK(filtered_handoff_seen == 1 &&
                filtered_built == 200 && filtered_compared > 0);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
    }
    s->base.evaluation_control = NULL;
    f = frame(s, left); stack = wl_columnar_eval_stack_cleanup_stack(f);
    CHECK((anti ? wl_columnar_antijoin_op(&op, stack, s) :
        wl_columnar_semijoin_op(&op, stack, s)) == 0);
    right_filter_result(stack, anti, mode);
    wl_columnar_join_membership_test_checkpoint = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
    s->base.evaluation_control = c; destroy(s);
    wl_plan_free(plan); wirelog_program_free(program);
}

/* Independently source-derived literal trajectories: see the retained
 * serial-membership-exact-cost-intake-r2.json, never measured-cost rebasing. */
static uint64_t exact_buckets, exact_left_visits, exact_candidate_visits;
static void exact_checkpoint(wl_columnar_join_membership_record_t *record,
    int stage, uint64_t progress)
{
    CHECK(record && record->ready);
    if (stage == 7) exact_buckets = progress;
    if (stage == 9) exact_left_visits++;
    if (stage == 10 || stage == 11) exact_candidate_visits++;
}
static void exact_trial(bool anti, unsigned boundary)
{
    const uint64_t full = anti ? 1839u +
        sizeof(wl_columnar_consolidate_record_t) + 1041u
        : 2690u + sizeof(wl_columnar_consolidate_record_t) + 1041u
        + sizeof(wl_columnar_memory_reservation_t);
    uint64_t limit = boundary == 0 ? 0 : boundary == 1 ? full :
        boundary == 2 ? full - 1u : full - 564u - 1u;
    uint64_t expected_work = boundary < 2 ? full : boundary == 2 ? full - 64u :
        full - 564u - 173u;
    int expected = boundary < 2 ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl A(k:int64,v:int64)\n.decl B(k:int64)\n.decl H(v:int64)\nH(v):-A(k,v),!B(k).\n",
        &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_col_session_t *s = create(plan, 2);
    CHECK(s->nrels == 2 && !strcmp(s->rels[0]->name,
        "A") && !strcmp(s->rels[1]->name, "B"));
    col_rel_t *left = s->rels[0], *right = s->rels[1];
    CHECK(left->ncols == 2 && right->ncols == 1 && left->nrows == 1 &&
        right->nrows == 1);
    CHECK(!left->column_types && !right->column_types
        && !strcmp(left->col_names[0], "col0") && !strcmp(left->col_names[1],
        "col1")
        && !strcmp(right->col_names[0], "col0"));
    CHECK(col_rel_storage_owner_ensure_initialized(left) == 0
        && col_rel_storage_owner_ensure_initialized(right) == 0);
    CHECK(wl_columnar_filter_next_pow2(2) == 16);
    CHECK((wl_columnar_hash_value(2166136261u, left, 0, 1) & 15u) == 4u);
    CHECK((wl_columnar_hash_value(2166136261u, right, 0, 99) & 15u) == 6u);
    exact_buckets = exact_left_visits = exact_candidate_visits = 0;
    wl_columnar_join_membership_test_checkpoint = exact_checkpoint;
    wl_columnar_eval_stack_cleanup_frame_t *f = frame(s, left);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(limit, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    const char *keys[] = {"col0"};
    wl_plan_op_t op = {.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN,
                       .right_relation = "B", .key_count = 1, .left_keys = keys,
                       .right_keys = keys, .delta_mode = WL_DELTA_AUTO};
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
    int rc = anti ? wl_columnar_antijoin_op(&op, stack,
            s) : wl_columnar_semijoin_op(&op, stack, s);
    CHECK(rc == expected);
    CHECK(exact_buckets == 16 && exact_left_visits == 1 &&
        exact_candidate_visits == 0);
    wl_columnar_join_membership_test_checkpoint = NULL;
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c,
        &report) == 0 && report.result == expected);
    if (report.consumed != expected_work)
        fprintf(stderr,
            "membership exact anti=%u boundary=%u consumed=%llu expected=%llu\n",
            anti, boundary, (unsigned long long)report.consumed,
            (unsigned long long)expected_work);
    CHECK(report.consumed == expected_work);
    col_rel_t *out =
        expected ? wl_columnar_eval_stack_cleanup_result(f)->rel :
        stack->items[0].rel;
    CHECK(out && out->ncols == 2 &&
        out->nrows == (anti && boundary != 3 ? 1u : 0u));
    if (anti &&
        boundary != 3) CHECK(out->columns[0][0] == 1 &&
            out->columns[1][0] == 7);
    if (boundary == 3) CHECK(stack->items[0].rel == left);
    /* Checked disposal preserves the borrowed relation pointer. */
    if (boundary == 2) CHECK(stack->items[0].rel == left &&
            !stack->items[0].owned && !stack->items[0].seg_boundaries &&
            !stack->items[0].seg_count);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
    unpinned(left); unpinned(right);
    if (expected) {
        CHECK(wl_evaluation_control_reset(c) == 0 &&
            wl_evaluation_control_configure(c, 0) == 0);
        s->base.evaluation_control = NULL; f = frame(s, left);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        stack = wl_columnar_eval_stack_cleanup_stack(f);
        CHECK((anti ? wl_columnar_antijoin_op(&op, stack,
            s) : wl_columnar_semijoin_op(&op, stack, s)) == 0);
        CHECK(stack->items[0].rel->nrows == (anti ? 1u : 0u));
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
    }
    s->base.evaluation_control = NULL; f = frame(s, left);
    stack = wl_columnar_eval_stack_cleanup_stack(f);
    CHECK((anti ? wl_columnar_antijoin_op(&op, stack,
        s) : wl_columnar_semijoin_op(&op, stack, s)) == 0);
    CHECK(stack->items[0].rel->nrows == (anti ? 1u : 0u));
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
    s->base.evaluation_control = c; destroy(s);
    wl_plan_free(plan); wirelog_program_free(program);
}

static void edge_trial(bool anti, unsigned mode)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl A(k:int64,v:int64)\n.decl B(k:int64)\n.decl H(v:int64)\nH(v):-A(k,v),!B(k).\n",
        &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    wl_col_session_t *s = create(plan, 2);
    col_rel_t *left = session_find_rel(s, "A"),
        *right = session_find_rel(s, "B");
    const char *keys[] = {"col0"}; uint32_t projection[] = {99};
    wl_plan_op_t op = {.op = anti ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_SEMIJOIN,
                       .right_relation = mode == 0 ||
                           mode == 1 ? "absent" : "B",
                       .key_count = mode == 3 || mode == 6 ? 0u : 1u,
                       .left_keys = keys,
                       .right_keys = keys, .delta_mode = WL_DELTA_AUTO};
    bool owned = mode == 1 || mode == 7;
    if (owned) {
        col_rel_t *copy = NULL;
        CHECK(wl_columnar_relation_new_auto_governed("owned", 2, 64, false,
            s->memory_governor, &copy) == 0);
        copy->nrows = 1; copy->columns[0][0] = 1; copy->columns[1][0] = 7;
        left = copy;
    }
    if (mode == 2) right->nrows = 0; /* Valid empty source, before all leases. */
    if (mode == 4) {
        left->nrows = 0;
        wirelog_column_type_t types[] = {WIRELOG_TYPE_FLOAT,
                                         WIRELOG_TYPE_INT64};
        CHECK(col_rel_set_column_types(left, types, 2) == 0);
        left->nrows = 1;
    }
    if (mode == 5 || mode == 6) {
        op.project_count = 1;
        op.project_indices = mode == 5 ? NULL : projection;
    }
    wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &f) == 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
    CHECK(eval_stack_push(stack, left, owned) == 0);
    if (mode == 1) stack->items[0].is_delta = true;
    wl_columnar_source_access_reader_t reader = {0};
    if (mode == 7) CHECK(col_rel_source_reader_acquire(left, &reader) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    int expected = mode == 4 ||
        (mode == 5 && !anti) ? EINVAL : mode == 7 ? EBUSY : 0;
    int rc = anti ? wl_columnar_antijoin_op(&op, stack,
            s) : wl_columnar_semijoin_op(&op, stack, s);
    if (rc != expected) fprintf(stderr,
            "membership edge anti=%u mode=%u rc=%d expected=%d\n", anti, mode,
            rc, expected);
    CHECK(rc == expected);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
        rc > 0 ? rc : 0, 0) == rc);
    if (mode < 2) CHECK(stack->top == 1 && stack->items[0].rel == left
            && stack->items[0].owned == owned &&
            stack->items[0].is_delta == (mode == 1));
    else if (!expected) {
        col_rel_t *out = stack->items[0].rel;
        uint32_t rows = mode == 2 ? (anti ? 1 : 0) : (mode == 3 ||
            mode == 6) ? (anti ? 0 : 1) : anti ? 1 : 0;
        CHECK(stack->top == 1 && stack->items[0].owned && out->nrows == rows);
        if (rows && !anti && mode == 6) CHECK(out->columns[0][0] == 0);
    }
    if (mode == 7) {
        CHECK(stack->items[0].rel == left && stack->items[0].owned);
        CHECK(wl_columnar_eval_stack_cleanup_result(f)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == EBUSY && !f &&
            s->cleanup_pending);
        CHECK(col_rel_source_reader_release(&reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
    } else CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
    CHECK(!s->cleanup_active && !s->cleanup_pending);
    if (mode == 1) {
        col_rel_t *raw_left = NULL;
        CHECK(wl_columnar_relation_new_auto_governed("owned", 2, 64, false,
            s->memory_governor, &raw_left) == 0);
        raw_left->nrows = 1; raw_left->columns[0][0] = 1;
        raw_left->columns[1][0] = 7;
        s->base.evaluation_control = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &f) == 0);
        stack = wl_columnar_eval_stack_cleanup_stack(f);
        CHECK(eval_stack_push(stack, raw_left, true) == 0);
        stack->items[0].is_delta = true;
        CHECK((anti ? wl_columnar_antijoin_op(&op, stack, s) :
            wl_columnar_semijoin_op(&op, stack, s)) == 0);
        CHECK(stack->top == 1 && stack->items[0].rel == raw_left
            && stack->items[0].owned && stack->items[0].is_delta);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
        s->base.evaluation_control = c;
    }
    /* The legacy raw SEMI projected-schema helper dereferences a NULL
     * projection with positive count; mode5 is controlled malformed-input
     * validation only, not a raw-equivalence claim. */
    if (mode != 1 && mode != 7 && !(mode == 5 && !anti)) {
        s->base.evaluation_control = NULL; f = frame(s, left);
        stack = wl_columnar_eval_stack_cleanup_stack(f);
        CHECK((anti ? wl_columnar_antijoin_op(&op, stack, s) :
            wl_columnar_semijoin_op(&op, stack, s)) == expected);
        if (!expected) {
            uint32_t rows = mode == 0 ? 1 : mode == 2 ? (anti ? 1 : 0) :
                (mode == 3 || mode == 6) ? (anti ? 0 : 1) : anti ? 1 : 0;
            CHECK(stack->items[0].rel->nrows == rows);
            if (rows && !anti &&
                mode == 6) CHECK(stack->items[0].rel->columns[0][0] == 0);
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
        s->base.evaluation_control = c;
    }
    unpinned(right); destroy(s); wl_plan_free(plan);
    wirelog_program_free(program);
}
int main(void)
{
    for (unsigned kind = 0; kind < 3; kind++)
        for (unsigned snapshot = 0; snapshot < 2;
            snapshot++) generated(kind, snapshot != 0);
    for (unsigned anti = 0; anti < 2; anti++) {
        collision_stop(anti != 0, 8); collision_stop(anti != 0, 11);
    }
    authority_fault(0); authority_fault(1); authority_fault(2);
    for (unsigned anti = 0; anti < 2; anti++)
        for (unsigned mode = 0; mode < 6; mode++) {
            right_filter(anti != 0, 1, mode, false);
            right_filter(anti != 0, 33, mode, true);
        }
    for (unsigned anti = 0; anti < 2; anti++)
        for (unsigned mode = 0; mode < 8; mode++) edge_trial(anti != 0, mode);
    /* Original managed pool64MiB + original evaluation arena64MiB,
     * plus1MiB for actual slabs/relations/operator overlap. */
    enforcing_budget = UINT64_C(129) * 1024 * 1024;
    generated(1, true); enforcing_budget = 0;
    for (unsigned anti = 0; anti < 2; anti++) {
        exact_trial(anti != 0, 0); exact_trial(anti != 0, 1);
        exact_trial(anti != 0, 1); exact_trial(anti != 0, 2);
        if (anti) exact_trial(true, 3);
    }
    puts("evaluation_membership: generated initial/dirty STEP/SNAPSHOT OK");
    return 0;
}
