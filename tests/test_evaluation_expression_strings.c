/* #2299 context-bearing string comparisons through genuine session hosts. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"
#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x)                                                               \
        do {                                                                         \
            if (!(x)) {                                                                \
                fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
                exit(1);                                                                 \
            }                                                                          \
        } while (0)
static unsigned evaluations, stop_phase, stop_evaluation_at;
static uint64_t stop_progress;
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
static void witness(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (phase == 83) {
        evaluations++;
        if (control && stop_evaluation_at && evaluations == stop_evaluation_at)
            wl_evaluation_control_request_cancel(control);
    }
    if (control && phase == stop_phase && progress >= stop_progress)
        wl_evaluation_control_request_cancel(control);
}
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session;
    wl_evaluation_control_t *control;
} fixture_t;
static fixture_t create(const char *source, bool passes) {
    fixture_t f = {0};
    wirelog_error_t error;
    f.program = wirelog_parse_string(source, &error);
    if (!f.program)
        fprintf(stderr, "parse failed: %s\n", source);
    CHECK(f.program);
    if (passes) {
        wl_fusion_apply(f.program, NULL);
        wl_jpp_apply(f.program, NULL);
        wl_sip_apply(f.program, NULL);
    }
    CHECK(!wl_plan_from_program(f.program, &f.plan));
    CHECK(!wl_evaluation_control_create(0, &f.control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = f.control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session));
    return f;
}
static void destroy(fixture_t *f) {
    wl_session_destroy(f->session);
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    wl_evaluation_control_release(f->control);
}
static int step_complete(wl_session_t *session) {
    for (unsigned turn = 0; turn < 10000; turn++) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN)
            return rc;
    }
    CHECK(false);
    return EAGAIN;
}
static void add(fixture_t *f, const char *text) {
    int64_t v = wl_intern_put(f->plan->intern, text);
    CHECK(v >= 0);
    CHECK(!wl_session_insert(f->session, "input", &v, 1, 1));
}
static bool accepted(unsigned op, int order) {
    switch (op) {
    case 0:
        return order == 0;
    case 1:
        return order != 0;
    case 2:
        return order < 0;
    case 3:
        return order > 0;
    case 4:
        return order <= 0;
    default:
        return order >= 0;
    }
}
static void session_comparisons(void) {
    const char *ops[] = {"=", "!=", "<", ">", "<=", ">="};
    const char *values[] = {"zz", "aa", "mm"};
    for (unsigned mode = 0; mode < 2; mode++)
        for (unsigned op = 0; op < 6; op++) {
            char source[512];
            snprintf(source, sizeof(source),
                ".decl input(x:string)\n.decl out(x:string)\nout(x) :- "
                "input(x), x %s \"mm\".\n",
                ops[op]);
            fixture_t f = create(source, mode != 0);
            for (unsigned i = 0; i < 3; i++)
                add(&f, values[i]);
            evaluations = 0;
            stop_phase = 0;
            wl_columnar_expr_test_checkpoint = witness;
            int rc = step_complete(f.session);
            wl_columnar_expr_test_checkpoint = NULL;
            if (rc)
                fprintf(stderr, "session rc=%d op=%s passes=%u\n", rc, ops[op],
                    mode);
            CHECK(!rc && evaluations > 0);
            col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
            CHECK(out && out->column_types[0] == WIRELOG_TYPE_STRING);
            bool seen[3] = {0};
            unsigned count = 0;
            for (uint32_t r = 0; r < out->nrows; r++) {
                const char *word =
                    wl_intern_reverse(f.plan->intern, out->columns[0][r]);
                CHECK(word);
                unsigned i;
                for (i = 0; i < 3; i++)
                    if (!strcmp(word, values[i]))
                        break;
                CHECK(i < 3 && !seen[i] && accepted(op, strcmp(word, "mm")));
                seen[i] = true;
                count++;
            }
            unsigned expected = 0;
            for (unsigned i = 0; i < 3; i++) {
                bool want = accepted(op, strcmp(values[i], "mm"));
                CHECK(seen[i] == want);
                expected += want;
            }
            CHECK(count == expected);
            destroy(&f);
        }
}
static const char map_source[] =
    ".decl input(x:string,n:int64)\n.decl out(x:string,n:int64,s:string)\n"
    "out(x,n+1,\"mapped\") :- input(x,n).\n";
static void map_input(fixture_t *f, const char *word, int64_t number) {
    int64_t row[] = {wl_intern_put(f->plan->intern, word), number};
    CHECK(row[0] >= 0);
    CHECK(!wl_session_insert(f->session, "input", row, 1, 2));
}
static void check_map(fixture_t *f, unsigned count) {
    static const char *words[] = {"zz", "aa", "mm"};
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == count && out->ncols == 3);
    CHECK(out->column_types[0] == WIRELOG_TYPE_STRING &&
        out->column_types[1] == WIRELOG_TYPE_INT64 &&
        out->column_types[2] == WIRELOG_TYPE_STRING);
    bool seen[3] = {0};
    for (uint32_t r = 0; r < out->nrows; r++) {
        const char *word = wl_intern_reverse(f->plan->intern,
                out->columns[0][r]);
        CHECK(word);
        unsigned i;
        for (i = 0; i < 3; i++)
            if (!strcmp(word, words[i]))
                break;
        CHECK(i < 3 && !seen[i]);
        seen[i] = true;
        CHECK(out->columns[1][r] == 11 + (int64_t)i);
        CHECK(!strcmp(wl_intern_reverse(f->plan->intern, out->columns[2][r]),
            "mapped"));
    }
    if (count == 3)
        CHECK(seen[0] && seen[1] && seen[2]);
    if (count == 2)
        CHECK(seen[0] && !seen[1] && seen[2]);
    if (count == 1)
        CHECK(seen[0] && !seen[1] && !seen[2]);
}
static void session_maps(void) {
    for (unsigned passes = 0; passes < 2; passes++) {
        fixture_t f = create(map_source, passes != 0);
        map_input(&f, "zz", 10);
        map_input(&f, "aa", 11);
        map_input(&f, "mm", 12);
        uint32_t interns = wl_intern_count(f.plan->intern);
        bool already_mapped = wl_intern_get(f.plan->intern, "mapped") >= 0;
        evaluations = 0;
        stop_phase = 0;
        wl_columnar_expr_test_checkpoint = witness;
        CHECK(!step_complete(f.session));
        wl_columnar_expr_test_checkpoint = NULL;
        CHECK(evaluations >= 3 &&
            wl_intern_count(f.plan->intern) == interns + !already_mapped);
        check_map(&f, 3);
        destroy(&f);
    }
}
static unsigned events, positives, negatives;
static void delta(const char *relation, const int64_t *row, uint32_t cols,
    int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 3 && (diff == 1 || diff == -1));
    CHECK(!strcmp(wl_intern_reverse(f->plan->intern, row[2]), "mapped"));
    const char *word = wl_intern_reverse(f->plan->intern, row[0]);
    CHECK(word);
    CHECK((!strcmp(word, "zz") && row[1] == 11) ||
        (!strcmp(word, "aa") && row[1] == 12) ||
        (!strcmp(word, "mm") && row[1] == 13));
    if (diff < 0)
        CHECK(!strcmp(word, "aa"));
    events++;
    positives += diff == 1;
    negatives += diff == -1;
}
static void session_recovery(void) {
    fixture_t f = create(map_source, false);
    map_input(&f, "zz", 10);
    map_input(&f, "aa", 11);
    map_input(&f, "mm", 12);
    events = positives = negatives = 0;
    wl_session_set_delta_cb(f.session, delta, &f);
    evaluations = 0;
    stop_phase = 0;
    stop_evaluation_at = 3;
    stop_progress = 0;
    wl_columnar_expr_test_checkpoint = witness;
    int stopped = step_complete(f.session);
    if (stopped != WL_EVALUATION_CONTROL_CANCELLED)
        fprintf(stderr, "observed stop rc=%d evaluations=%u events=%u\n",
            stopped,
            evaluations, events);
    CHECK(stopped == WL_EVALUATION_CONTROL_CANCELLED);
    wl_columnar_expr_test_checkpoint = NULL;
    stop_phase = 0;
    stop_evaluation_at = 0;
    CHECK(!events && !wl_evaluation_control_active_attempt_id(f.control));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.control, &report) &&
        report.result == WL_EVALUATION_CONTROL_CANCELLED);
    col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
    CHECK(!out || !out->nrows);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!step_complete(f.session));
    check_map(&f, 3);
    CHECK(events == 3 && positives == 3 && !negatives);
    int64_t removed[] = {wl_intern_get(f.plan->intern, "aa"), 11};
    CHECK(!wl_session_remove(f.session, "input", removed, 1, 2));
    CHECK(!step_complete(f.session));
    check_map(&f, 2);
    CHECK(events == 4 && positives == 3 && negatives == 1);
    CHECK(!step_complete(f.session));
    CHECK(events == 4);
    destroy(&f);
}
static void empty_and_shared_plan(void) {
    fixture_t f = create(map_source, false);
    unsigned interns = wl_intern_count(f.plan->intern);
    evaluations = 0;
    stop_phase = 0;
    wl_columnar_expr_test_checkpoint = witness;
    CHECK(!step_complete(f.session));
    wl_columnar_expr_test_checkpoint = NULL;
    check_map(&f, 0);
    CHECK(!evaluations && wl_intern_count(f.plan->intern) == interns);
    wl_session_t *second = NULL;
    wl_evaluation_control_t *second_control = NULL;
    CHECK(!wl_evaluation_control_create(0, &second_control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = second_control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &second));
    wl_columnar_memory_governor_ref_t *first_gov =
        COL_SESSION(f.session)->memory_governor,
        *second_gov =
        COL_SESSION(second)->memory_governor;
    CHECK(first_gov != second_gov);
    wl_columnar_memory_governor_ref_retain(first_gov);
    wl_columnar_memory_governor_ref_retain(second_gov);
    int64_t row[] = {wl_intern_put(f.plan->intern, "zz"), 10};
    CHECK(!wl_session_insert(second, "input", row, 1, 2));
    CHECK(!step_complete(second));
    col_rel_t *out = session_find_rel(COL_SESSION(second), "out");
    CHECK(out && out->nrows == 1 && out->columns[1][0] == 11);
    CHECK(
        !strcmp(wl_intern_reverse(f.plan->intern, out->columns[2][0]),
        "mapped"));
    wl_session_destroy(second);
    wl_evaluation_control_release(second_control);
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(second_gov)));
    destroy(&f);
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(first_gov)));
    wl_columnar_memory_governor_ref_release(second_gov);
    wl_columnar_memory_governor_ref_release(first_gov);
}

static int direct_owner;
static wl_evaluation_control_t *begin(uint64_t limit) {
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, &direct_owner));
    CHECK(!wl_evaluation_control_begin(c, &direct_owner));
    return c;
}
static void finish(wl_evaluation_control_t *c, int rc) {
    CHECK(wl_evaluation_control_finish(c, &direct_owner, rc > 0 ? rc : 0, 0) ==
        rc);
    CHECK(!wl_evaluation_control_detach(c, &direct_owner));
    wl_evaluation_control_release(c);
}
static size_t variable(uint8_t *bytes, uint8_t tag) {
    const uint8_t prefix[] = {0, 4, 0, 'c', 'o', 'l', '0'};
    memcpy(bytes, prefix, 7);
    bytes[0] = tag;
    return 7;
}
static size_t literal(uint8_t *bytes, const void *text, uint16_t length) {
    bytes[0] = WL_PLAN_EXPR_CONST_STR;
    memcpy(bytes + 1, &length, 2);
    memcpy(bytes + 3, text, length);
    return length + 3;
}
static void direct_comparisons(void) {
    wl_intern_t *intern = wl_intern_create();
    CHECK(intern);
    int64_t row = wl_intern_put(intern, "aa");
    CHECK(row >= 0);
    wl_columnar_expr_context_t context = {.intern = intern};
    for (unsigned tag =
        WL_PLAN_EXPR_CMP_STR_EQ; tag <= WL_PLAN_EXPR_CMP_STR_GTE;
        tag++) {
        uint8_t bytes[128];
        size_t n = variable(bytes, WL_PLAN_EXPR_VAR_STRING);
        n += literal(bytes + n, "mm\0tail", 7);
        bytes[n++] = (uint8_t)tag;
        wl_evaluation_control_t *c = begin(0);
        bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
                bytes, (uint32_t)n, NULL, &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) &&
            supported && program);
        int64_t raw = 99, value = 99;
        bool failed = true;
        wl_columnar_expr_status_t status;
        CHECK(!wl_columnar_expr_eval_run_ctx(bytes, (uint32_t)n, &row, 1, &raw,
            &context, &status));
        /* Compile owns its bytecode and literal spans. */
        memset(bytes, 0, n);
        CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(
                program, &row, 1, &context, &value, &failed, &status, c,
                wl_evaluation_control_active_attempt_id(c)) &&
            !failed && status == WL_COLUMNAR_EXPR_OK && value == raw);
        wirelog_column_type_t type = WIRELOG_TYPE_INT64;
        CHECK(!wl_columnar_expr_scalar_result_type_attempt_controlled(
                program, NULL, 0, &type, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) &&
            supported && type == WIRELOG_TYPE_BOOL);
        wl_columnar_expr_attempt_scalar_free(program);
        finish(c, 0);
    }
    wl_intern_free(intern);
}
static void direct_limits_and_stack(void) {
    uint8_t bytes[2048];
    uint32_t n = (uint32_t)literal(bytes, "abc", 3);
    for (unsigned below = 0; below < 2; below++) {
        /* Literal compile:10 fixed/header regions plus2*size+2 byte-copy
         * regions. size6 gives24*224=5376. */
        wl_evaluation_control_t *c = begin(5376 - below);
        bool supported = true;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        int rc = wl_columnar_expr_compile_scalar_attempt_controlled(
            bytes, n, NULL, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(supported == !below && (program != NULL) == !below);
        wl_columnar_expr_attempt_scalar_free(program);
        finish(c, rc);
        c = begin(0);
        supported = false;
        program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
                bytes, n, NULL, &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) &&
            supported);
        CHECK(!wl_evaluation_control_finish(c, &direct_owner, 0, 0));
        CHECK(!wl_evaluation_control_configure(c, 1792 - below));
        CHECK(!wl_evaluation_control_begin(c, &direct_owner));
        int64_t value = 999;
        bool failed = true;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        /* Null-intern literal evaluation:8 fixed/header regions; payload
         * stays opaque and no temporary or intern allocation occurs. */
        rc = wl_columnar_expr_eval_scalar_attempt_controlled(
            program, NULL, 0, NULL, &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(value == (below ? 999 : 0) && failed == !!below);
        CHECK(status ==
            (below ? WL_COLUMNAR_EXPR_INVALID_RESULT : WL_COLUMNAR_EXPR_OK));
        wl_columnar_expr_attempt_scalar_free(program);
        finish(c, rc);
    }
    wl_intern_t *intern = wl_intern_create();
    CHECK(intern);
    wl_columnar_expr_context_t context = {.intern = intern};
    for (unsigned which = 0; which < 4; which++) {
        n = 0;
        if (which == 0)
            bytes[n++] = WL_PLAN_EXPR_CMP_STR_EQ; /*zero-filled underflow.*/
        else if (which == 1) {
            n = (uint32_t)literal(bytes, "a", 1);
            bytes[n++] = WL_PLAN_EXPR_AGG_SUM;
        } else {
            for (unsigned i = 0; i < COL_FILTER_STACK; i++) {
                bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                int64_t v = i;
                memcpy(bytes + n, &v, 8);
                n += 8;
            }
            if (which == 2)
                n += (uint32_t)literal(bytes + n, "ignored", 7);
            else
                n += (uint32_t)variable(bytes + n, WL_PLAN_EXPR_VAR_STRING);
        }
        wl_evaluation_control_t *c = begin(0);
        bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
                bytes, n, NULL, &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) &&
            supported);
        int64_t row = -1, value = 999, raw = 999;
        bool failed = false;
        wl_columnar_expr_status_t status, raw_status;
        int rawrc = wl_columnar_expr_eval_run_ctx(bytes, n, &row, 1, &raw,
                &context,
                &raw_status);
        CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(
                program, &row, 1, &context, &value, &failed, &status, c,
                wl_evaluation_control_active_attempt_id(c)));
        CHECK(failed == (rawrc != 0) && value == raw && status == raw_status);
        if (which == 2)
            CHECK(value == COL_FILTER_STACK - 1);
        wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
        CHECK(!wl_columnar_expr_scalar_result_type_attempt_controlled(
                program, NULL, 0, &type, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) &&
            supported);
        CHECK(type == (which == 0   ? WIRELOG_TYPE_BOOL
                   : which == 1 ? WIRELOG_TYPE_STRING
                                : WIRELOG_TYPE_INT64));
        wl_columnar_expr_attempt_scalar_free(program);
        finish(c, 0);
    }
    wl_intern_free(intern);
    bytes[0] = 0xff;
    wl_evaluation_control_t *c = begin(0);
    bool supported = true;
    wl_columnar_expr_attempt_scalar_t *program = NULL;
    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
            bytes, 1, NULL, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) &&
        !supported && !program);
    finish(c, 0);
}
static void direct_retained_literal(void) {
    uint8_t bytes[32];
    uint32_t n = (uint32_t)literal(bytes, "fresh", 5);
    wl_intern_t *intern = wl_intern_create();
    CHECK(intern);
    wl_columnar_memory_resolution_t r = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = 1000000,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *string_gov =
        wl_columnar_memory_governor_ref_create(
        &r),
        *owner_gov =
        wl_columnar_memory_governor_ref_create(
        &r);
    CHECK(string_gov && owner_gov);
    CHECK(!wl_intern_attach_memory_governor(intern, string_gov));
    wl_evaluation_control_t *c = begin(0);
    bool supported = false;
    wl_columnar_expr_attempt_scalar_t *program = NULL;
    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
            bytes, n, owner_gov, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) &&
        supported);
    uint64_t owner_bytes = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(owner_gov));
    CHECK(owner_bytes);
    wl_columnar_expr_context_t context = {.intern = intern};
    int64_t value = 999;
    bool failed = true;
    wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
    stop_phase = 86;
    stop_progress = 0;
    wl_columnar_expr_test_checkpoint = witness;
    int rc = wl_columnar_expr_eval_scalar_attempt_controlled(
        program, NULL, 0, &context, &value, &failed, &status, c,
        wl_evaluation_control_active_attempt_id(c));
    wl_columnar_expr_test_checkpoint = NULL;
    stop_phase = 0;
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && value == 999 && failed &&
        status == WL_COLUMNAR_EXPR_INVALID_RESULT);
    CHECK(wl_intern_count(intern) == 1 &&
        !strcmp(wl_intern_reverse(intern, 0), "fresh"));
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner_gov)) == owner_bytes);
    CHECK(wl_evaluation_control_finish(c, &direct_owner, 0, 0) == rc);
    CHECK(!wl_evaluation_control_reset(c));
    CHECK(!wl_evaluation_control_begin(c, &direct_owner));
    CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(
            program, NULL, 0, &context, &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c)) &&
        !failed && value == 0);
    CHECK(wl_intern_count(intern) == 1);
    wl_columnar_expr_attempt_scalar_free(program);
    finish(c, 0);
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner_gov)));
    wl_intern_free(intern);
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(string_gov)));
    wl_columnar_memory_governor_ref_release(owner_gov);
    wl_columnar_memory_governor_ref_release(string_gov);
}

static void scalar_resource_failures(void) {
    uint8_t bytes[32];
    uint32_t n = (uint32_t)literal(bytes, "resource", 8);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = 1000000,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    for (unsigned mode = 0; mode < 4; mode++) {
        wl_columnar_memory_governor_ref_t *g =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(g);
        wl_columnar_memory_governor_t *budget =
            wl_columnar_memory_governor_ref_get(g);
        uint64_t required = wl_columnar_expr_test_scalar_owner_size() + n;
        if (mode < 2)
            atomic_store_explicit(&budget->usable_bytes, required - mode,
                memory_order_relaxed);
        else
            wl_columnar_expr_test_fail_allocation = (int)mode - 2;
        wl_evaluation_control_t *c = begin(0);
        bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        int rc = wl_columnar_expr_compile_scalar_attempt_controlled(
            bytes, n, g, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c));
        wl_columnar_expr_test_fail_allocation = -1;
        CHECK(rc == (mode == 0 ? 0 : mode == 1 ? ENOSPC : ENOMEM));
        CHECK((program != NULL) == (mode == 0) && supported == (mode == 0));
        CHECK(wl_columnar_memory_reserved(budget) ==
            (mode == 0 ? required : 0));
        wl_columnar_expr_attempt_scalar_free(program);
        finish(c, rc);
        CHECK(!wl_columnar_memory_reserved(budget));
        wl_columnar_memory_governor_ref_release(g);
    }
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g);
    wl_intern_t *intern = wl_intern_create();
    CHECK(intern);
    wl_columnar_expr_context_t context = {.intern = intern};
    for (unsigned mode = 0; mode < 2; mode++) {
        wl_evaluation_control_t *c = begin(0);
        bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
                bytes, n, g, &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) &&
            supported);
        uint64_t owner_bytes =
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
        if (mode == 0)
            wl_columnar_expr_test_fail_allocation = 0;
        else
            wl_intern_control_test_fail_allocation = 0;
        int64_t value = 999;
        bool failed = true;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        int rc = wl_columnar_expr_eval_scalar_attempt_controlled(
            program, NULL, 0, &context, &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c));
        wl_columnar_expr_test_fail_allocation = -1;
        wl_intern_control_test_fail_allocation = -1;
        CHECK(rc == ENOMEM && value == 999 && failed &&
            status == WL_COLUMNAR_EXPR_INVALID_RESULT);
        CHECK(!wl_intern_count(intern) &&
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)) ==
            owner_bytes);
        wl_columnar_expr_attempt_scalar_free(program);
        finish(c, rc);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
    }
    wl_intern_free(intern);
    wl_columnar_memory_governor_ref_release(g);
}

static void array_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress) {
    if (phase == 142 && progress == 0)
        wl_evaluation_control_request_cancel(c);
}
static void partial_scalar_array(void) {
    fixture_t f = create(map_source, false);
    map_input(&f, "zz", 10);
    uint32_t strings = wl_intern_count(f.plan->intern);
    wl_columnar_memory_governor_ref_t *g =
        COL_SESSION(f.session)->memory_governor;
    wl_columnar_memory_governor_ref_retain(g);
    wl_columnar_filter_test_attempt_checkpoint = array_stop;
    CHECK(step_complete(f.session) == WL_EVALUATION_CONTROL_CANCELLED);
    wl_columnar_filter_test_attempt_checkpoint = NULL;
    CHECK(wl_intern_count(f.plan->intern) == strings);
    col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
    CHECK(!out || !out->nrows);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!step_complete(f.session));
    check_map(&f, 1);
    destroy(&f);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static bool rebound;
static void rebind_scalar(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress;
    (void)failed;
    if (c && phase == 84 && !rebound) {
        rebound = true;
        CHECK(!wl_evaluation_control_finish(c, &direct_owner, 0, 0));
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &direct_owner));
    }
}
static unsigned intern_rebind_phase;
static void rebind_intern(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress) {
    (void)progress;
    if (phase == intern_rebind_phase && !rebound) {
        rebound = true;
        CHECK(!wl_evaluation_control_finish(c, &direct_owner, 0, 0));
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &direct_owner));
    }
}
static void nested_intern_identity(void) {
    for (unsigned trial = 0; trial < 2; ++trial) {
        wl_intern_t *intern = wl_intern_create();
        CHECK(intern);
        uint8_t bytes[32];
        uint32_t n = (uint32_t)literal(bytes, "nested", 6);
        wl_evaluation_control_t *c = begin(0);
        bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        uint64_t old = wl_evaluation_control_active_attempt_id(c);
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
                bytes, n, NULL, &program, &supported, c, old) && supported);
        wl_columnar_expr_context_t context = { .intern = intern };
        int64_t value = 999;
        bool failed = true;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        rebound = false;
        intern_rebind_phase = trial ? 4 : 2;
        wl_intern_control_test_checkpoint = rebind_intern;
        int rc = wl_columnar_expr_eval_scalar_attempt_controlled(program,
                NULL, 0, &context, &value, &failed, &status, c, old);
        wl_intern_control_test_checkpoint = NULL;
        CHECK(rebound && rc == EINVAL && value == 999 && failed &&
            status == WL_COLUMNAR_EXPR_INVALID_RESULT);
        CHECK(wl_intern_count(intern) == 0);
        uint64_t consumed = 999;
        uint64_t successor = wl_evaluation_control_active_attempt_id(c);
        CHECK(!wl_evaluation_control_test_active_consumed(c, successor,
            &consumed) && consumed == 0);
        CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
            NULL, 0, &context, &value, &failed, &status, c, successor));
        CHECK(!failed && status == WL_COLUMNAR_EXPR_OK &&
            !strcmp(wl_intern_reverse(intern, value), "nested") &&
            wl_intern_count(intern) == 1);
        wl_columnar_expr_attempt_scalar_free(program);
        wl_intern_free(intern);
        finish(c, 0);
    }
}
static void scalar_interruption(void) {
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = 1000000,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g);
    uint8_t bytes[4100];
    char text[4096];
    memset(text, 'a', sizeof(text));
    uint32_t n = (uint32_t)literal(bytes, text, sizeof(text));
    for (unsigned mode = 0; mode < 2; mode++) {
        wl_evaluation_control_t *c = begin(0);
        bool supported = true;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        if (mode == 0) {
            n = (uint32_t)variable(bytes, WL_PLAN_EXPR_VAR_STRING);
            stop_phase = 70;
            stop_progress = 5;
        } else {
            n = (uint32_t)literal(bytes, text, sizeof(text));
            stop_phase = 43;
            stop_progress = 1024;
        }
        wl_columnar_expr_test_checkpoint = witness;
        int rc = wl_columnar_expr_compile_scalar_attempt_controlled(
            bytes, n, g, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c));
        wl_columnar_expr_test_checkpoint = NULL;
        stop_phase = 0;
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && !program && !supported);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        finish(c, rc);
    }
    n = (uint32_t)literal(bytes, "later", 5);
    wl_evaluation_control_t *c = begin(0);
    bool supported = false;
    wl_columnar_expr_attempt_scalar_t *program = NULL;
    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
            bytes, n, g, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) &&
        supported);
    int64_t value = 999;
    bool failed = true;
    wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
    uint64_t old = wl_evaluation_control_active_attempt_id(c);
    rebound = false;
    wl_columnar_expr_test_checkpoint = rebind_scalar;
    int rc = wl_columnar_expr_eval_scalar_attempt_controlled(
        program, NULL, 0, NULL, &value, &failed, &status, c, old);
    wl_columnar_expr_test_checkpoint = NULL;
    CHECK(rebound && rc == EINVAL && value == 999 && failed &&
        status == WL_COLUMNAR_EXPR_INVALID_RESULT);
    uint64_t consumed = 999;
    CHECK(!wl_evaluation_control_test_active_consumed(
            c, wl_evaluation_control_active_attempt_id(c), &consumed) &&
        consumed == 0);
    wl_columnar_expr_attempt_scalar_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    finish(c, 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void utf8_primitives(void) {
    struct vector { const char *text; size_t advance; int64_t cp;
                    unsigned reads; } cases[] = {
        {"", 0, -1, 1}, {"A", 1, 65, 1}, {"\xC2\xA2", 2, 162, 2},
        {"\xE2\x82\xAC", 3, 8364, 3}, {"\xF0\x9F\x98\x80", 4, 128512, 4},
        {"\x80", 1, -1, 1}, {"\xFF", 1, -1, 1}, {"\xC2" "A", 1, -1, 2},
        {"\xE1\x80" "A", 2, -1, 3}, {"\xF1\x80\x80" "A", 3, -1, 4},
        {"\xC0\x80", 2, 0, 2}, {"\xED\xA0\x80", 3, 55296, 3},
        {"\xF4\x90\x80\x80", 4, 1114112, 4},
        {"\xC2", 1, -1, 2}, {"\xE1", 1, -1, 2}, {"\xE1\x80", 2, -1, 3},
        {"\xF1", 1, -1, 2}, {"\xF1\x80", 2, -1, 3}, {"\xF1\x80\x80", 3, -1, 4}
    };
    for (unsigned i = 0; i < sizeof(cases)/sizeof(*cases); ++i) {
        size_t length = strlen(cases[i].text);
        char *exact_storage = malloc(length + 1);
        CHECK(exact_storage); memcpy(exact_storage, cases[i].text, length + 1);
        for (unsigned shortfall = 0; shortfall < 2; ++shortfall) {
            uint64_t limit = 224 * (2 + cases[i].reads) - shortfall;
            wl_evaluation_control_t *c = begin(limit);
            size_t advance = 999; int64_t cp = 999;
            int rc =
                wl_columnar_expr_utf8_next_attempt_controlled(exact_storage,
                    &advance, &cp, c,
                    wl_evaluation_control_active_attempt_id(c));
            if (shortfall) CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                    advance == 999 && cp == 999);
            else CHECK(!rc && advance == cases[i].advance && cp == cases[i].cp);
            finish(c, rc);
        }
        free(exact_storage);
    }
    for (unsigned phase = 90; phase <= 92; ++phase) {
        wl_evaluation_control_t *c = begin(0);
        stop_phase = phase; stop_progress = phase == 91 ? 2 : 0;
        wl_columnar_expr_test_checkpoint = witness;
        size_t advance = 999; int64_t cp = 999;
        int rc = wl_columnar_expr_utf8_next_attempt_controlled("\xE2\x82\xAC",
                &advance, &cp,
                c, wl_evaluation_control_active_attempt_id(c));
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && advance == 999 &&
            cp == 999);
        finish(c, rc);
    }
}
static void direct_readers(void) {
    const uint8_t tags[] = {WL_PLAN_EXPR_STR_FN_STRLEN,
                            WL_PLAN_EXPR_STR_FN_STR_ORD,
                            WL_PLAN_EXPR_STR_FN_CONTAINS,
                            WL_PLAN_EXPR_STR_FN_STR_PREFIX,
                            WL_PLAN_EXPR_STR_FN_STR_SUFFIX};
    const char *strings[] = {"", "a", "aaaaab", "aaab", "\xC2" "A",
                             "\xE1\x80" "A",
                             "\xF1\x80\x80" "A", "\xC0\x80", "\xED\xA0\x80",
                             "\xF4\x90\x80\x80", "\xF1\x80"};
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    int64_t ids[sizeof(strings)/sizeof(*strings)];
    for (unsigned i = 0; i < sizeof(strings)/sizeof(*strings); ++i) {
        ids[i] = wl_intern_put(intern, strings[i]); CHECK(ids[i] >= 0);
    }
    for (unsigned tag = 0; tag < 5; ++tag)
        for (unsigned null_context = 0; null_context < 2; ++null_context)
            for (int a = -2; a < (int)(sizeof(ids)/sizeof(*ids)); ++a)
                for (int b = -1;
                    b < (tag < 2 ? 0 : (int)(sizeof(ids)/sizeof(*ids))); ++b){
                    uint8_t bytes[24]; uint32_t n = 0;
                    int64_t av = a < 0 ? -1 : ids[a], bv = b < 0 ? -1 : ids[b];
                    if (a != -2) {
                        bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                        memcpy(bytes+n, &av, 8); n += 8;
                    }
                    if (tag >= 2 && a != -2) {
                        bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                        memcpy(bytes+n, &bv, 8); n += 8;
                    }
                    bytes[n++] = tags[tag];
                    wl_evaluation_control_t *c = begin(0);
                    wl_columnar_expr_attempt_scalar_t *program = NULL;
                    bool supported = false;
                    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
                            bytes, n, NULL,
                            &program, &supported, c,
                            wl_evaluation_control_active_attempt_id(c)) &&
                        supported);
                    wl_columnar_expr_context_t context =
                    {.intern = null_context ? NULL : intern};
                    int64_t raw = 999, value = 999; bool failed = true;
                    wl_columnar_expr_status_t status, raw_status;
                    int rawrc = wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0,
                            &raw, &context, &raw_status);
                    CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(
                            program, NULL, 0, &context,
                            &value, &failed, &status, c,
                            wl_evaluation_control_active_attempt_id(c)));
                    CHECK(value == raw && failed == (rawrc != 0) &&
                        status == raw_status);
                    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
                    CHECK(
                        !wl_columnar_expr_scalar_result_type_attempt_controlled(
                            program, NULL, 0,
                            &type, &supported, c,
                            wl_evaluation_control_active_attempt_id(c)) &&
                        supported);
                    CHECK(type ==
                        (tag < 2 ? WIRELOG_TYPE_INT64 : WIRELOG_TYPE_BOOL));
                    wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
                }
    wl_intern_free(intern);
}
static unsigned utf8_rebind_phase;
static void utf8_rebind(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (c && phase == utf8_rebind_phase &&
        progress >= (phase == 91 ? 2u : 0u) && !rebound){
        rebound = true;
        CHECK(!wl_evaluation_control_finish(c, &direct_owner, 0, 0));
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &direct_owner));
    }
}
static void reader_interruption(void) {
    for (unsigned phase = 91; phase <= 92; ++phase) {
        wl_evaluation_control_t *c = begin(0);
        uint64_t old = wl_evaluation_control_active_attempt_id(c);
        rebound = false; utf8_rebind_phase = phase;
        wl_columnar_expr_test_checkpoint = utf8_rebind;
        size_t advance = 999; int64_t cp = 999;
        int rc = wl_columnar_expr_utf8_next_attempt_controlled("\xE2\x82\xAC",
                &advance, &cp, c, old);
        wl_columnar_expr_test_checkpoint = NULL;
        CHECK(rebound && rc == EINVAL && advance == 999 && cp == 999);
        uint64_t consumed = 999;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c),
            &consumed) && !consumed);
        finish(c, 0);
    }
    const uint8_t tags[] = {WL_PLAN_EXPR_STR_FN_STRLEN,
                            WL_PLAN_EXPR_STR_FN_STR_ORD,
                            WL_PLAN_EXPR_STR_FN_CONTAINS,
                            WL_PLAN_EXPR_STR_FN_STR_PREFIX,
                            WL_PLAN_EXPR_STR_FN_STR_SUFFIX};
    const unsigned phases[] = {94, 91, 47, 42, 42};
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    int64_t a = wl_intern_put(intern, "\xE2\x82\xAC" "aaaaab"),
        b = wl_intern_put(intern, "aaaab");
    CHECK(a >= 0 && b >= 0);
    for (unsigned tag = 0; tag < 5; ++tag) {
        uint8_t bytes[24]; uint32_t n = 0;
        bytes[n++] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+n, &a, 8); n += 8;
        if (tag >= 2) {
            bytes[n++] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+n, &b, 8); n += 8;
        }
        bytes[n++] = tags[tag];
        wl_evaluation_control_t *c = begin(0);
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        bool supported = false;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
            NULL,
            &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        wl_columnar_expr_context_t context = {.intern = intern};
        stop_phase = phases[tag];
        stop_progress = tag == 0 ? 1 : tag == 1 || tag == 2 ? 1 : 0;
        wl_columnar_expr_test_checkpoint = witness;
        int64_t value = 999; bool failed = true;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        int rc = wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL,
                0, &context,
                &value, &failed, &status, c,
                wl_evaluation_control_active_attempt_id(c));
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && value == 999 && failed &&
            status == WL_COLUMNAR_EXPR_INVALID_RESULT);
        CHECK(wl_evaluation_control_finish(c, &direct_owner, 0, 0) == rc);
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &direct_owner));
        CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
            &context,
            &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c)) && !failed);
        wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
    }
    wl_intern_free(intern);
}
static const char *reader_words[] = {"", "a", "ba", "ab", "bbb"};
static int64_t reader_values(unsigned op, const char *s) {
    switch (op) {
    case 0: return (int64_t)strlen(s);
    case 1: return *s ? (unsigned char)*s : -1;
    case 2: return strchr(s, 'a') != NULL;
    case 3: return *s == 'a';
    default: return *s && s[strlen(s)-1] == 'a';
    }
}
static void reader_delta(const char *relation, const int64_t *row,
    uint32_t cols,
    int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 6 && (diff == 1 || diff == -1));
    const char *s = wl_intern_reverse(f->plan->intern, row[0]); CHECK(s);
    for (unsigned i = 0; i < 5; ++i) CHECK(row[i+1] == reader_values(i, s));
    ++events; positives += diff == 1; negatives += diff == -1;
}
static void check_reader_map(fixture_t *f, unsigned count) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->ncols == 6 && out->nrows == count);
    bool seen[5] = {0};
    for (uint32_t r = 0; r < count; ++r) {
        const char *s = wl_intern_reverse(f->plan->intern, out->columns[0][r]);
        CHECK(s);
        unsigned k = 0; while (k < 5 && strcmp(s, reader_words[k])) ++k;
        CHECK(k < 5 && !seen[k]); seen[k] = true;
        for (unsigned i = 0; i < 5;
            ++i) CHECK(out->columns[i+1][r] == reader_values(i, s));
    }
}
static unsigned reader_filter_op;
static void reader_filter_delta(const char *relation, const int64_t *row,
    uint32_t cols, int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 1 && (diff == 1 || diff == -1));
    const char *word = wl_intern_reverse(f->plan->intern, row[0]);
    CHECK(word && reader_values(reader_filter_op, word) > 0);
    ++events; positives += diff == 1; negatives += diff == -1;
}
static bool reader_exhaust;
static void reader_session_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (c && phase == 94 && progress >= 1) {
        if (reader_exhaust) CHECK(wl_evaluation_control_charge(c,
                UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        else wl_evaluation_control_request_cancel(c);
    }
}
static void session_readers(void) {
    const char *fn[] = {"strlen(x) > 0", "str_ord(x) > 0", "contains(x,\"a\")",
                        "str_prefix(x,\"a\")", "str_suffix(x,\"a\")"};
    for (unsigned op = 0; op < 5; ++op) for (unsigned mode = 0; mode < 2;
            ++mode){
            char source[512]; snprintf(source, sizeof(source),
                ".decl input(x:string)\n.decl out(x:string)\nout(x) :- input(x), %s.\n",
                fn[op]);
            fixture_t f = create(source, mode != 0);
            reader_filter_op = op; events = positives = negatives = 0;
            wl_session_set_delta_cb(f.session, reader_filter_delta, &f);
            CHECK(!step_complete(f.session));
            col_rel_t *empty = session_find_rel(COL_SESSION(f.session), "out");
            CHECK(empty && !empty->nrows);
            for (unsigned i = 0; i < 5; ++i) add(&f, reader_words[i]);
            const unsigned phases[] = {94, 91, 47, 42, 42};
            stop_phase = phases[op]; stop_progress = op == 0 ? 1 : 0;
            wl_columnar_expr_test_checkpoint = witness;
            CHECK(step_complete(f.session) == WL_EVALUATION_CONTROL_CANCELLED);
            wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
            CHECK(!events);
            empty = session_find_rel(COL_SESSION(f.session), "out");
            CHECK(empty && !empty->nrows);
            CHECK(!wl_evaluation_control_reset(f.control));
            evaluations = 0; stop_phase = 0;
            wl_columnar_expr_test_checkpoint = witness;
            CHECK(!step_complete(f.session));
            wl_columnar_expr_test_checkpoint = NULL;
            CHECK(evaluations > 0);
            col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
            CHECK(out);
            bool seen[5] = {0}; unsigned count = 0;
            for (uint32_t r = 0; r < out->nrows; ++r) {
                const char *s = wl_intern_reverse(f.plan->intern,
                        out->columns[0][r]); CHECK(s);
                unsigned k = 0; while (k < 5 && strcmp(s, reader_words[k])) ++k;
                CHECK(k < 5 && !seen[k] && reader_values(op, s) > 0);
                seen[k] = true;
            }
            for (unsigned i = 0; i < 5; ++i) {
                bool want = reader_values(op, reader_words[i]) > 0;
                CHECK(seen[i] == want); count += want;
            }
            if (events != count || positives != count ||
                negatives) fprintf(stderr,
                    "reader filter op=%u mode=%u rows=%u count=%u events=%u pos=%u neg=%u\n",
                    op, mode, out->nrows, count, events, positives, negatives);
            CHECK(out->nrows == count && events == count &&
                positives == count && !negatives);
            unsigned removed = 0;
            while (removed < 5 && !seen[removed]) ++removed;
            CHECK(removed < 5);
            int64_t id = wl_intern_get(f.plan->intern, reader_words[removed]);
            CHECK(!wl_session_remove(f.session, "input", &id, 1, 1));
            CHECK(!step_complete(f.session));
            out = session_find_rel(COL_SESSION(f.session), "out");
            CHECK(out && out->nrows == count - 1 && events == count + 1 &&
                negatives == 1);
            CHECK(!step_complete(f.session) && events == count + 1);
            destroy(&f);
        }
    const char *source =
        ".decl input(x:string)\n.decl out(x:string,l:int64,o:int64,c:int64,p:int64,s:int64)\n"
        "out(x,strlen(x),str_ord(x),contains(x,\"a\"),str_prefix(x,\"a\"),str_suffix(x,\"a\")) :- input(x).\n";
    for (unsigned mode = 0; mode < 2; ++mode) {
        fixture_t f = create(source, mode != 0);
        events = positives = negatives = 0;
        wl_session_set_delta_cb(f.session, reader_delta, &f);
        CHECK(!step_complete(f.session)); check_reader_map(&f, 0);
        for (unsigned i = 0; i < 5; ++i) add(&f, reader_words[i]);
        reader_exhaust = mode != 0; evaluations = 0;
        if (reader_exhaust) CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_MAX - 1));
        wl_columnar_expr_test_checkpoint = reader_session_stop;
        CHECK(step_complete(f.session) ==
            (reader_exhaust ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            WL_EVALUATION_CONTROL_CANCELLED));
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        CHECK(!events); check_reader_map(&f, 0);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!step_complete(f.session));
        check_reader_map(&f, 5);
        CHECK(events == 5 && positives == 5 && !negatives);
        int64_t removed = wl_intern_get(f.plan->intern, "a");
        CHECK(!wl_session_remove(f.session, "input", &removed, 1, 1));
        CHECK(!step_complete(f.session)); check_reader_map(&f, 4);
        CHECK(events == 6 && negatives == 1);
        CHECK(!step_complete(f.session) && events == 6);
        wl_columnar_memory_governor_ref_t *g =
            COL_SESSION(f.session)->memory_governor;
        wl_columnar_memory_governor_ref_retain(g); destroy(&f);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        wl_columnar_memory_governor_ref_release(g);
    }
}

static void decimal_primitives(void) {
    struct number_vector { const char *text; uint64_t reads, digits; int rc;
                           int64_t value; } cases[] = {
        {"", 1, 0, 0, 0}, {"abc", 1, 0, 0, 0}, {"+", 2, 0, 0, 0},
        {"++1", 2, 0, 0, 0},
        {"12", 3, 2, 0, 12}, {"  -12x", 6, 2, 0, -12}, {"0x10", 2, 1, 0, 0},
        {" \t\n\r\v\f+123tail", 11, 3, 0, 123},
        {"9223372036854775807", 20, 19, 0, INT64_MAX},
        {"-9223372036854775808", 21, 19, 0, INT64_MIN},
        {"9223372036854775808", 20, 19, ERANGE, 0},
        {"-9223372036854775809", 21, 19, ERANGE, 0},
        {"9999999999999999999999999999999999999999x", 41, 40, ERANGE, 0}
    };
    for (unsigned i = 0; i < sizeof(cases)/sizeof(*cases); ++i)
        for (unsigned below = 0; below < 2; ++below) {
            wl_evaluation_control_t *c = begin(224 *
                    (2+cases[i].reads+cases[i].digits)-below);
            int64_t value = 999;
            int rc = wl_columnar_expr_decimal_attempt_controlled(cases[i].text,
                    &value,
                    c, wl_evaluation_control_active_attempt_id(c));
            CHECK(rc ==
                (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : cases[i].rc));
            CHECK(value == (rc ? 999 : cases[i].value));
            finish(c, rc);
        }
    for (unsigned phase = 97; phase <= 99; ++phase) {
        wl_evaluation_control_t *c = begin(0);
        stop_phase = phase; stop_progress = phase == 99 ? 0 : 32;
        wl_columnar_expr_test_checkpoint = witness;
        int64_t value = 999;
        int rc = wl_columnar_expr_decimal_attempt_controlled(
            "9999999999999999999999999999999999999999x", &value, c,
            wl_evaluation_control_active_attempt_id(c));
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && value == 999);
        finish(c, rc);
    }
    {
        wl_evaluation_control_t *c = begin(0);
        char text[67]; memset(text, ' ', 64); text[64] = '+'; text[65] = '7';
        text[66] = 0;
        stop_phase = 97; stop_progress = 32;
        wl_columnar_expr_test_checkpoint = witness;
        int64_t value = 999;
        int rc = wl_columnar_expr_decimal_attempt_controlled(text, &value, c,
                wl_evaluation_control_active_attempt_id(c));
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && value == 999);
        CHECK(wl_evaluation_control_finish(c, &direct_owner, 0, 0) == rc);
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &direct_owner));
        CHECK(!wl_columnar_expr_decimal_attempt_controlled(text, &value, c,
            wl_evaluation_control_active_attempt_id(c)) && value == 7);
        finish(c, 0);
    }
    for (unsigned phase = 98; phase <= 99; ++phase) {
        wl_evaluation_control_t *c = begin(0);
        uint64_t old = wl_evaluation_control_active_attempt_id(c);
        rebound = false; utf8_rebind_phase = phase;
        wl_columnar_expr_test_checkpoint = utf8_rebind;
        int64_t value = 999;
        int rc = wl_columnar_expr_decimal_attempt_controlled("12345", &value, c,
                old);
        wl_columnar_expr_test_checkpoint = NULL;
        CHECK(rebound && rc == EINVAL && value == 999);
        uint64_t consumed = 999;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c),
            &consumed) && !consumed);
        finish(c, 0);
    }
}
static void decimal_parity(void) {
    const char *current = setlocale(LC_CTYPE, NULL); CHECK(current);
    char *saved = malloc(strlen(current)+1); CHECK(saved);
    strcpy(saved, current);
    const char *locales[] = {"C", "C.UTF-8", "C.utf8", "en_US.utf8"};
    const char *numbers[] = {"", "not a number", " 12tail", " -3 ", " +", "+-1",
                             "0x10",
                             "9223372036854775807", "-9223372036854775808",
                             "9223372036854775808", "-9223372036854775809"};
    unsigned checked_locales = 0;
    for (unsigned locale = 0; locale < sizeof(locales)/sizeof(*locales);
        ++locale){
        if (!setlocale(LC_CTYPE, locales[locale])) continue;
        ++checked_locales;
        wl_intern_t *intern = wl_intern_create(); CHECK(intern);
        for (unsigned i = 0; i < 256 + sizeof(numbers)/sizeof(*numbers); ++i) {
            char encoded[] = {(char)i, '1', '2', 0};
            const char *text = i < 256 ? encoded : numbers[i-256];
            int64_t operand = wl_intern_put(intern, text); CHECK(operand >= 0);
            uint8_t bytes[10] = {WL_PLAN_EXPR_CONST_INT};
            memcpy(bytes+1, &operand, 8);
            bytes[9] = WL_PLAN_EXPR_STR_FN_TO_NUMBER;
            wl_evaluation_control_t *c = begin(0);
            wl_columnar_expr_attempt_scalar_t *program = NULL;
            bool supported = false;
            CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
                sizeof(bytes), NULL,
                &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) && supported);
            for (unsigned context_case = 0; context_case < 3; ++context_case) {
                wl_columnar_expr_context_t context = {.intern = context_case ==
                                                          1 ? NULL : intern};
                int64_t raw = 999, value = 999; bool failed = false;
                wl_columnar_expr_status_t raw_status, status;
                if (context_case == 2) {
                    operand = -1; memcpy(bytes+1, &operand, 8);
                    wl_columnar_expr_attempt_scalar_free(program);
                    program = NULL;
                    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
                            bytes, sizeof(bytes), NULL,
                            &program, &supported, c,
                            wl_evaluation_control_active_attempt_id(c)) &&
                        supported);
                }
                int rawrc = wl_columnar_expr_eval_run_ctx(bytes, sizeof(bytes),
                        NULL, 0, &raw, &context, &raw_status);
                CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                    NULL, 0, &context,
                    &value, &failed, &status, c,
                    wl_evaluation_control_active_attempt_id(c)));
                CHECK(value == raw && failed == (rawrc != 0) &&
                    status == raw_status);
            }
            wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
            CHECK(!wl_columnar_expr_scalar_result_type_attempt_controlled(
                    program, NULL, 0,
                    &type, &supported, c,
                    wl_evaluation_control_active_attempt_id(c)) && supported &&
                type == WIRELOG_TYPE_INT64);
            wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
        }
        wl_intern_free(intern);
    }
    CHECK(checked_locales && setlocale(LC_CTYPE, saved)); free(saved);
}
static int64_t decimal_expected(const char *word) {
    if (!strcmp(word, "12tail"))return 12;
    if (!strcmp(word, " -3 "))return -3;
    CHECK(!strcmp(word, "nonsense")); return 0;
}
static void decimal_delta(const char *relation, const int64_t *row,
    uint32_t cols,
    int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 3 && (diff == 1 || diff == -1));
    const char *word = wl_intern_reverse(f->plan->intern, row[0]); CHECK(word);
    CHECK(row[1] == decimal_expected(word) && row[2] == row[1]+1);
    ++events; positives += diff == 1; negatives += diff == -1;
}
static void check_decimal_map(fixture_t *f, unsigned count) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == count && out->ncols == 3);
    bool seen[3] = {0};
    const char *words[] = {"12tail", " -3 ", "nonsense"};
    for (uint32_t r = 0; r < count; ++r) {
        const char *word = wl_intern_reverse(f->plan->intern,
                out->columns[0][r]); CHECK(word);
        unsigned k = 0;
        while (k < 3 && strcmp(word, words[k]))
            ++k;
        CHECK(k < 3 && !seen[k]);
        seen[k] = true;
        CHECK(out->columns[1][r] == decimal_expected(word) &&
            out->columns[2][r] == decimal_expected(word)+1);
    }
    if (count == 3) CHECK(seen[0] && seen[1] && seen[2]);
    if (count == 2) CHECK(!seen[0] && seen[1] && seen[2]);
    if (count == 1) CHECK(seen[0] && !seen[1] && !seen[2]);
}
static bool decimal_exhaust;
static void decimal_session_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (c && phase == 97 && progress >= 1) {
        if (decimal_exhaust) CHECK(wl_evaluation_control_charge(c,
                UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        else wl_evaluation_control_request_cancel(c);
    }
}
static void session_decimal(void) {
    const char *source =
        ".decl input(x:string)\n.decl out(x:string,n:int64,m:int64)\n"
        "out(x,to_number(x),to_number(x)+1) :- input(x).\n";
    for (unsigned mode = 0; mode < 2; ++mode) {
        fixture_t f = create(source, mode != 0);
        events = positives = negatives = 0;
        wl_session_set_delta_cb(f.session, decimal_delta, &f);
        CHECK(!step_complete(f.session)); check_decimal_map(&f, 0);
        add(&f, "12tail"); add(&f, " -3 "); add(&f, "nonsense");
        decimal_exhaust = mode != 0;
        if (decimal_exhaust) CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_MAX - 1));
        wl_columnar_expr_test_checkpoint = decimal_session_stop;
        CHECK(step_complete(f.session) ==
            (decimal_exhaust ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            WL_EVALUATION_CONTROL_CANCELLED));
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        CHECK(!events); check_decimal_map(&f, 0);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!step_complete(f.session));
        check_decimal_map(&f, 3);
        CHECK(events == 3 && positives == 3 && !negatives);
        int64_t id = wl_intern_get(f.plan->intern, "12tail");
        CHECK(!wl_session_remove(f.session, "input", &id, 1, 1));
        CHECK(!step_complete(f.session)); check_decimal_map(&f, 2);
        CHECK(events == 4 && negatives == 1);
        CHECK(!step_complete(f.session) && events == 4);
        wl_columnar_memory_governor_ref_t *g =
            COL_SESSION(f.session)->memory_governor;
        wl_columnar_memory_governor_ref_retain(g); destroy(&f);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        wl_columnar_memory_governor_ref_release(g);
    }
    fixture_t f = create(source, false);
    events = positives = negatives = 0;
    wl_session_set_delta_cb(f.session, decimal_delta, &f);
    add(&f, "12tail"); add(&f, "9223372036854775808");
    CHECK(step_complete(f.session) == ERANGE && !events);
    col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
    CHECK(!out || !out->nrows);
    int64_t id = wl_intern_get(f.plan->intern, "9223372036854775808");
    CHECK(!wl_session_remove(f.session, "input", &id, 1, 1));
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!step_complete(f.session)); check_decimal_map(&f, 1);
    CHECK(events == 1); destroy(&f);
    const char *filter =
        ".decl input(x:string)\n.decl out(x:string)\nout(x) :- input(x), to_number(x) >= 0.\n";
    f = create(filter, false); CHECK(!step_complete(f.session));
    add(&f, "12tail"); add(&f, " -3 "); add(&f, "nonsense");
    add(&f, "9223372036854775808");
    CHECK(!step_complete(f.session));
    out = session_find_rel(COL_SESSION(f.session), "out");
    CHECK(out && out->nrows == 2);
    bool a = false, b = false;
    for (uint32_t r = 0; r < out->nrows; ++r) {
        const char *word = wl_intern_reverse(f.plan->intern,
                out->columns[0][r]); CHECK(word);
        if (!strcmp(word, "12tail")) {
            CHECK(!a); a = true;
        } else{
            CHECK(!strcmp(word, "nonsense") && !b); b = true;
        }
    }
    CHECK(a && b); destroy(&f);
}

int main(void) {
    decimal_primitives();
    decimal_parity();
    session_decimal();
    utf8_primitives();
    direct_readers();
    reader_interruption();
    session_readers();
    direct_comparisons();
    direct_limits_and_stack();
    direct_retained_literal();
    scalar_resource_failures();
    scalar_interruption();
    nested_intern_identity();
    session_comparisons();
    session_maps();
    session_recovery();
    partial_scalar_array();
    empty_and_shared_plan();
    puts("controlled string comparisons: all checks passed");
    return 0;
}
