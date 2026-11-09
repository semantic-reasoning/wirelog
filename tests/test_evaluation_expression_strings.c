/* #2299 context-bearing string comparisons through genuine session hosts. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/session.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/crc32.h"
#include <xxhash.h>
#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_MSC_VER) && !defined(__clang__)
#  define TEST_ALIGNAS(n) __declspec(align(n))
#else
#  define TEST_ALIGNAS(n) _Alignas(n)
#endif
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

static void hash_unaligned_spans(void) {
    TEST_ALIGNAS(16) uint8_t bytes[4096 + 15];
    for (size_t i = 0; i < sizeof(bytes); ++i)
        bytes[i] = (uint8_t)(i * 37u + 11u);
    const size_t sizes[] = {0, 1, 63, 64, 65, 240, 241, 255, 256, 257,
                            1024, 4096};
    for (size_t offset = 0; offset < 16; ++offset) {
        for (size_t k = 0; k < sizeof(sizes) / sizeof(*sizes); ++k) {
            size_t n = sizes[k];
            for (unsigned below = 0; below < 2; ++below) {
                wl_evaluation_control_t *c = begin(
                    224 * (4 + (n + 63) / 64) - below);
                int64_t value = 999;
                int rc = wl_columnar_expr_hash_bytes_attempt_controlled(
                    bytes + offset, n, &value, c,
                    wl_evaluation_control_active_attempt_id(c));
                CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
                CHECK(value == (below ? 999 :
                    (int64_t)XXH3_64bits(bytes + offset, n)));
                finish(c, rc);
            }
        }
    }
}

static void hash_crc_primitives(void) {
    uint8_t bytes[4096];
    for (size_t i = 0; i < sizeof(bytes);
        ++i) bytes[i] = (uint8_t)(i * 37u + 11u);
    const size_t sizes[] = {0, 1, 8, 16, 17, 63, 64, 65, 127, 128, 129, 240,
                            241, 255, 256, 257, 1024, 4096};
    for (unsigned k = 0; k < sizeof(sizes)/sizeof(*sizes); ++k) {
        size_t n = sizes[k];
        for (unsigned below = 0; below < 2; ++below) {
            uint64_t chunks = (n + 63) / 64;
            wl_evaluation_control_t *c = begin(224 * (4 + chunks) - below);
            int64_t value = 999;
            int rc =
                wl_columnar_expr_hash_bytes_attempt_controlled(n ? bytes : NULL,
                    n,
                    &value, c, wl_evaluation_control_active_attempt_id(c));
            CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
            CHECK(value == (below ? 999 : (int64_t)XXH3_64bits(bytes, n)));
            if (!below &&
                !n) CHECK((uint64_t)value == UINT64_C(0x2D06800538D394C2));
            finish(c, rc);
            for (unsigned cast = 0; cast < 2; ++cast) {
                c = begin(224 * (2*n + 2) - below);
                uint32_t crc = 999;
                rc =
                    wl_columnar_expr_crc_bytes_attempt_controlled(n ? bytes :
                        NULL, n, cast != 0,
                        &crc, c, wl_evaluation_control_active_attempt_id(c));
                CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
                uint32_t expected = cast ? castagnoli_crc32(bytes,
                        n) : ethernet_crc32(bytes, n);
                CHECK(crc == (below ? 999 : expected)); finish(c, rc);
            }
        }
    }
    for (unsigned cast = 0; cast < 2; ++cast) {
        wl_evaluation_control_t *c = begin(0); uint32_t crc = 0;
        CHECK(!wl_columnar_expr_crc_bytes_attempt_controlled(
                (const uint8_t *)"123456789", 9,
                cast != 0, &crc, c,
                wl_evaluation_control_active_attempt_id(c)));
        CHECK(crc == (cast ? UINT32_C(0xE3069283) : UINT32_C(0xCBF43926)));
        finish(c, 0);
    }
    for (unsigned phase = 114; phase <= 127; ++phase) {
        if (phase == 118 || phase == 119)continue;
        wl_evaluation_control_t *c = begin(0);
        stop_phase = phase;
        stop_progress = (phase == 123 || phase == 124) ? 1024 : 0;
        wl_columnar_expr_test_checkpoint = witness;
        int64_t value = 999; uint32_t crc = 999;
        int rc = phase <
            120 ? wl_columnar_expr_crc_bytes_attempt_controlled(bytes,
                sizeof(bytes), true,
                &crc, c, wl_evaluation_control_active_attempt_id(c)) :
            wl_columnar_expr_hash_bytes_attempt_controlled(bytes, sizeof(bytes),
                &value, c,
                wl_evaluation_control_active_attempt_id(c));
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && value == 999 &&
            crc == 999); finish(c, rc);
        c = begin(0); uint64_t old = wl_evaluation_control_active_attempt_id(c);
        rebound = false; utf8_rebind_phase = phase;
        wl_columnar_expr_test_checkpoint = utf8_rebind;
        rc = phase < 120 ? wl_columnar_expr_crc_bytes_attempt_controlled(bytes,
                sizeof(bytes), true, &crc, c, old) :
            wl_columnar_expr_hash_bytes_attempt_controlled(bytes, sizeof(bytes),
                &value, c, old);
        wl_columnar_expr_test_checkpoint = NULL;
        CHECK(rebound && rc == EINVAL && value == 999 && crc == 999);
        uint64_t consumed = 999;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c),
            &consumed) && !consumed);
        finish(c, 0);
    }
}
static const uint8_t digest_tags[] = {WL_PLAN_EXPR_ARITH_HASH_S,
                                      WL_PLAN_EXPR_ARITH_CRC32_ETH_S,
                                      WL_PLAN_EXPR_ARITH_CRC32_CAST_S};
static int64_t digest_reference(unsigned op, const void *bytes, size_t length) {
    return op == 0 ? (int64_t)XXH3_64bits(bytes, length) : op == 1 ?
           (int64_t)ethernet_crc32(bytes,
               length) : (int64_t)castagnoli_crc32(bytes, length);
}
static void direct_string_digests(void) {
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    char large[1026]; memset(large, 'a', sizeof(large)-1);
    large[sizeof(large)-1] = 0;
    const char *words[] = {"", "a", "hello", "123456789", "\xF1\x80", large};
    int64_t ids[6]; for (unsigned i = 0; i < 6; ++i) {
        ids[i] = wl_intern_put(intern, words[i]); CHECK(ids[i] >= 0);
    }
    int64_t operands[] = {ids[0], ids[1], ids[2], ids[3], ids[4], ids[5], -1,
                          INT64_MIN, INT64_MAX};
    for (unsigned op = 0; op < 3; ++op)for (unsigned context_case = 0;
            context_case < 2; ++context_case)
            for (unsigned i = 0; i < sizeof(operands)/sizeof(*operands)+2;
                ++i){
                uint8_t bytes[700]; uint32_t n = 0;
                if (i < sizeof(operands)/sizeof(*operands)) {
                    bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                    memcpy(bytes+n, &operands[i], 8); n += 8;
                } else if (i == sizeof(operands)/sizeof(*operands)+1) {
                    for (int64_t value = 0; value < COL_FILTER_STACK; ++value) {
                        bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                        memcpy(bytes+n, &value, 8); n += 8;
                    }
                }
                bytes[n++] = digest_tags[op];
                wl_evaluation_control_t *c = begin(0); bool supported = false;
                wl_columnar_expr_attempt_scalar_t *program = NULL;
                CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
                    n, NULL, &program, &supported,
                    c,
                    wl_evaluation_control_active_attempt_id(c)) && supported);
                wl_columnar_expr_context_t context =
                {.intern = context_case ? NULL : intern};
                int64_t value = 999, raw = 999; bool failed = true;
                wl_columnar_expr_status_t status, raw_status;
                CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0, &raw,
                    &context, &raw_status));
                CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                    NULL, 0, &context, &value, &failed, &status,
                    c,
                    wl_evaluation_control_active_attempt_id(c)) && !failed &&
                    value == raw && status == raw_status);
                wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
                CHECK(!wl_columnar_expr_scalar_result_type_attempt_controlled(
                        program, NULL, 0, &type, &supported, c,
                        wl_evaluation_control_active_attempt_id(c)) &&
                    supported &&
                    type == WIRELOG_TYPE_INT64);
                wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
            }
    for (unsigned op = 0; op < 3; ++op) {
        uint8_t bytes[32];
        const uint8_t payload[] = {'a', 0, 'b'};
        uint32_t n = (uint32_t)literal(bytes, payload, sizeof(payload));
        bytes[n++] = digest_tags[op];
        wl_evaluation_control_t *c = begin(0);
        bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
            NULL, &program,
            &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        wl_columnar_expr_context_t context = {.intern = intern};
        int64_t value = 999; bool failed = true;
        wl_columnar_expr_status_t status;
        CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
            &context,
            &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c)) && !failed);
        CHECK(value == digest_reference(op, "a", 1));
        wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
        n = 0; bytes[n++] = WL_PLAN_EXPR_CONST_INT;
        memcpy(bytes + n, &ids[5], 8); n += 8;
        bytes[n++] = digest_tags[op]; c = begin(0); program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
            NULL, &program,
            &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        stop_phase = op ? 116 : 124; stop_progress = op ? 32 : 64;
        value = 999; failed = true; status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        wl_columnar_expr_test_checkpoint = witness;
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
        CHECK(value == digest_reference(op, large, strlen(large)));
        wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
    }
    wl_intern_free(intern);
}
static const char *digest_words[] = {"", "a", "hello", "123456789", "aaaab"};
static void digest_input(fixture_t *f, unsigned i) {
    int64_t row[] = {wl_intern_put(f->plan->intern, digest_words[i]),
                     10+(int64_t)i}; CHECK(row[0] >= 0);
    CHECK(!wl_session_insert(f->session, "input", row, 1, 2));
}
static void check_digest_map(fixture_t *f, unsigned count) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->ncols == 5 && out->nrows == count);
    CHECK(out->column_types[0] == WIRELOG_TYPE_STRING);
    for (unsigned i = 1; i < 5;
        ++i) CHECK(out->column_types[i] == WIRELOG_TYPE_INT64);
    bool seen[5] = {0};
    for (uint32_t r = 0; r < count; ++r) {
        const char *word = wl_intern_reverse(f->plan->intern,
                out->columns[0][r]); CHECK(word);
        unsigned k = 0;
        while (k < 5 && strcmp(word, digest_words[k]))++k;
        CHECK(k < 5 && !seen[k]); seen[k] = true;
        for (unsigned op = 0; op < 3;
            ++op) CHECK(out->columns[op+1][r] == digest_reference(op, word,
                strlen(word)));
        CHECK(out->columns[4][r] == 11+(int64_t)k);
    }
    if (count == 5)for (unsigned i = 0; i < 5; ++i) CHECK(seen[i]);
    if (count == 4)for (unsigned i = 0; i < 5; ++i) CHECK(seen[i] == (i != 1));
    if (count == 1)CHECK(seen[2]);
}
static void digest_delta(const char *relation, const int64_t *row,
    uint32_t cols, int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 5 && (diff == 1 || diff == -1));
    const char *word = wl_intern_reverse(f->plan->intern, row[0]); CHECK(word);
    unsigned k = 0;
    while (k < 5 && strcmp(word, digest_words[k]))
        ++k;
    CHECK(k < 5);
    for (unsigned op = 0; op < 3;
        ++op) CHECK(row[op+1] == digest_reference(op, word, strlen(word)));
    CHECK(row[4] == 11+(int64_t)k); ++events; positives += diff == 1;
    negatives += diff == -1;
}
static void check_digest_generated_tags(const wl_plan_t *plan) {
    unsigned found = 0;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)for (uint32_t r = 0;
            r < plan->strata[s].relation_count; ++r){
            const wl_plan_relation_t *relation = &plan->strata[s].relations[r];
            for (uint32_t o = 0; o < relation->op_count; ++o) {
                const wl_plan_op_t *op = &relation->ops[o];
                if (op->op != WL_PLAN_OP_MAP ||
                    op->map_expr_count != 5) continue;
                for (unsigned i = 0; i < 3; ++i) {
                    const wl_plan_expr_buffer_t *expr = &op->map_exprs[i+1];
                    CHECK(expr->size &&
                        expr->data[expr->size-1] == digest_tags[i]);
                }
                ++found;
            }
        }
    CHECK(found);
}
static void check_digest_filter_tag(const wl_plan_t *plan, unsigned which) {
    unsigned found = 0;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r) {
            const wl_plan_relation_t *relation = &plan->strata[s].relations[r];
            for (uint32_t o = 0; o < relation->op_count; ++o) {
                const wl_plan_op_t *op = &relation->ops[o];
                if (op->op != WL_PLAN_OP_FILTER) continue;
                const wl_plan_expr_buffer_t *expr = &op->filter_expr;
                CHECK(expr->size >= 14 &&
                    (expr->data[0] == WL_PLAN_EXPR_VAR ||
                    expr->data[0] == WL_PLAN_EXPR_VAR_STRING) &&
                    expr->data[7] == digest_tags[which] &&
                    expr->data[expr->size - 2] == digest_tags[which]);
                ++found;
            }
        }
    CHECK(found);
}
static bool digest_exhaust;
static void digest_session_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (c && phase == 116 && progress >= 1) {
        if (digest_exhaust)CHECK(wl_evaluation_control_charge(c,
                UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        else wl_evaluation_control_request_cancel(c);
    }
}
static void session_string_digests(void) {
    const char *source =
        ".decl input(x:string,n:int64)\n.decl out(x:string,h:int64,e:int64,c:int64,n:int64)\n"
        "out(x,hash(x),crc32_ethernet(x),crc32_castagnoli(x),n+1) :- input(x,n).\n";
    for (unsigned mode = 0; mode < 2; ++mode) {
        fixture_t f = create(source, mode != 0);
        check_digest_generated_tags(f.plan);
        events = positives = negatives = 0;
        wl_session_set_delta_cb(f.session, digest_delta, &f);
        CHECK(!step_complete(f.session)); check_digest_map(&f, 0);
        for (unsigned i = 0; i < 5; ++i)digest_input(&f, i);
        uint32_t strings = wl_intern_count(f.plan->intern);
        digest_exhaust = mode != 0;
        if (digest_exhaust)CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_MAX-1));
        wl_columnar_expr_test_checkpoint = digest_session_stop;
        CHECK(step_complete(f.session) ==
            (digest_exhaust ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            WL_EVALUATION_CONTROL_CANCELLED));
        wl_columnar_expr_test_checkpoint = NULL; CHECK(!events);
        check_digest_map(&f, 0);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!step_complete(f.session)); check_digest_map(&f, 5);
        CHECK(events == 5 && positives == 5 && !negatives &&
            wl_intern_count(f.plan->intern) == strings);
        int64_t removed[] = {wl_intern_get(f.plan->intern, "a"), 11};
        CHECK(!wl_session_remove(f.session, "input", removed, 1, 2));
        CHECK(!step_complete(f.session)); check_digest_map(&f, 4);
        CHECK(events == 6 && negatives == 1);
        CHECK(!step_complete(f.session) && events == 6);
        wl_columnar_memory_governor_ref_t *g =
            COL_SESSION(f.session)->memory_governor;
        wl_columnar_memory_governor_ref_retain(g); destroy(&f);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g))); wl_columnar_memory_governor_ref_release(g);
    }
    const char *functions[] = {"hash", "crc32_ethernet", "crc32_castagnoli"};
    for (unsigned op = 0; op < 3; ++op) {
        char text[512];
        snprintf(text, sizeof(text),
            ".decl input(x:string)\n.decl out(x:string)\nout(x) :- input(x), %s(x) = %s(\"a\").\n",
            functions[op], functions[op]);
        fixture_t f = create(text, false); check_digest_filter_tag(f.plan, op);
        CHECK(!step_complete(f.session));
        for (unsigned i = 0; i < 5; ++i)add(&f, digest_words[i]);
        CHECK(!step_complete(f.session));
        col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && out->nrows == 1);
        CHECK(!strcmp(wl_intern_reverse(f.plan->intern, out->columns[0][0]),
            "a")); destroy(&f);
    }
    fixture_t f = create(source, false); CHECK(!step_complete(f.session));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *second = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &second));
    wl_columnar_memory_governor_ref_t *g1 =
        COL_SESSION(f.session)->memory_governor,
        *g2 = COL_SESSION(second)->memory_governor;
    CHECK(g1 != g2); wl_columnar_memory_governor_ref_retain(g1);
    wl_columnar_memory_governor_ref_retain(g2);
    int64_t row[] = {wl_intern_put(f.plan->intern, "hello"), 12};
    CHECK(row[0] >= 0);
    CHECK(!wl_session_insert(second, "input", row, 1, 2));
    CHECK(!step_complete(second));
    fixture_t borrowed = f; borrowed.session = second;
    check_digest_map(&borrowed, 1);
    wl_session_destroy(second); wl_evaluation_control_release(control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            g2))); destroy(&f);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            g1)));
    wl_columnar_memory_governor_ref_release(g1);
    wl_columnar_memory_governor_ref_release(g2);
}

static const uint8_t builder_tags[] = {WL_PLAN_EXPR_STR_FN_CAT,
                                       WL_PLAN_EXPR_STR_FN_TO_UPPER,
                                       WL_PLAN_EXPR_STR_FN_TO_LOWER,
                                       WL_PLAN_EXPR_STR_FN_TO_STRING};
static void builder_primitives(void) {
    uint8_t source[256], out[256];
    for (unsigned i = 0; i < 256; ++i)source[i] = (uint8_t)i;
    for (unsigned upper = 0; upper < 2; ++upper)for (unsigned empty = 0;
            empty < 2;
            ++empty)
            for (unsigned below = 0; below < 2; ++below) {
                size_t n = empty?0:256, copied = 999;
                memset(out, 0xFF, sizeof(out));
                wl_evaluation_control_t *c = begin(224*(3*n+2)-below);
                int rc = wl_columnar_expr_ascii_case_attempt_controlled(out,
                        source, n, upper != 0, &copied,
                        c, wl_evaluation_control_active_attempt_id(c));
                CHECK(rc == (below?WL_EVALUATION_CONTROL_WORK_LIMIT:0) &&
                    copied == n);
                for (size_t i = 0; i < n; ++i) {
                    unsigned char x = source[i];
                    if (upper && x >= 'a' && x <= 'z') x -= 32u;
                    else if (!upper && x >= 'A' && x <= 'Z') x += 32u;
                    CHECK(out[i] == x);
                }
                finish(c, rc);
            }
    for (unsigned phase = 144; phase <= 148; ++phase) {
        wl_evaluation_control_t *c = begin(0); size_t copied = 999;
        stop_phase = phase; stop_progress = phase == 144?0:phase == 148?0:1;
        wl_columnar_expr_test_checkpoint = witness;
        int rc = wl_columnar_expr_ascii_case_attempt_controlled(out, source,
                256, true, &copied,
                c, wl_evaluation_control_active_attempt_id(c));
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(copied == (phase == 144?999:phase == 148?256:1)); finish(c, rc);
    }
    const int64_t numbers[] = {0, 1, -1, INT64_MIN, INT64_MAX};
    const char *strings[] = {"0", "1", "-1", "-9223372036854775808",
                             "9223372036854775807"};
    for (unsigned i = 0; i < 5;
        ++i) for (unsigned below = 0; below < 2; ++below){
            wl_evaluation_control_t *c = begin(448-below); char text[24];
            size_t length = 999;
            int rc = wl_columnar_expr_format_i64_attempt_controlled(numbers[i],
                    text, &length, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(rc == (below?WL_EVALUATION_CONTROL_WORK_LIMIT:0));
            if (below) CHECK(length == 999);
            else CHECK(length == strlen(strings[i]) && !strcmp(text,
                    strings[i]));
            finish(c, rc);
        }
    for (unsigned phase = 139; phase <= 148; ++phase) {
        if (phase == 140 || phase == 143)continue;
        wl_evaluation_control_t *c = begin(0);
        uint64_t old = wl_evaluation_control_active_attempt_id(c);
        rebound = false; utf8_rebind_phase = phase;
        wl_columnar_expr_test_checkpoint = utf8_rebind;
        size_t copied = 999, length = 999; char text[24];
        int rc = phase <
            144?wl_columnar_expr_format_i64_attempt_controlled(INT64_MIN, text,
                &length, c, old):
            wl_columnar_expr_ascii_case_attempt_controlled(out, source, 256,
                true, &copied, c, old);
        wl_columnar_expr_test_checkpoint = NULL;
        CHECK(rebound && rc == EINVAL && length == 999);
        uint64_t consumed = 999;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c),
            &consumed) && !consumed);
        finish(c, 0);
    }
}
static void direct_builders(void) {
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    const char *words[] = {"", "aB", " A z ", "\xE2\x82\xAC" "z", "\xF1\x80"};
    int64_t ids[5];
    for (unsigned i = 0; i < 5; ++i) {
        ids[i] = wl_intern_put(intern, words[i]); CHECK(ids[i] >= 0);
    }
    int64_t operands[] = {ids[0], ids[1], ids[2], ids[3], ids[4], -1, INT64_MIN,
                          INT64_MAX};
    for (unsigned op = 0; op < 4; ++op)for (unsigned context_case =
            0; context_case < 2;
            ++context_case)
            for (unsigned a = 0; a < sizeof(operands)/sizeof(*operands)+1; ++a)
                for (unsigned b = 0;
                    b < (op?1:sizeof(operands)/sizeof(*operands)); ++b){
                    uint8_t bytes[32]; uint32_t n = 0;
                    if (a < sizeof(operands)/sizeof(*operands)) {
                        bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                        memcpy(bytes+n, &operands[a], 8); n += 8;
                        if (!op) {
                            bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                            memcpy(bytes+n, &operands[b], 8); n += 8;
                        }
                    }
                    bytes[n++] = builder_tags[op];
                    wl_evaluation_control_t *c = begin(0);
                    bool supported = false;
                    wl_columnar_expr_attempt_scalar_t *program = NULL;
                    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
                            bytes, n, NULL, &program, &supported, c,
                            wl_evaluation_control_active_attempt_id(c)) &&
                        supported);
                    wl_columnar_expr_context_t context =
                    {.intern = context_case?NULL:intern};
                    int64_t value = 999, raw = 999; bool failed = true;
                    wl_columnar_expr_status_t status, raw_status;
                    CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0,
                        &raw, &context, &raw_status));
                    CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(
                            program, NULL, 0, &context, &value, &failed,
                            &status, c,
                            wl_evaluation_control_active_attempt_id(c)) &&
                        !failed && value == raw && status == raw_status);
                    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
                    CHECK(
                        !wl_columnar_expr_scalar_result_type_attempt_controlled(
                            program, NULL, 0, &type, &supported, c,
                            wl_evaluation_control_active_attempt_id(c)) &&
                        supported && type == WIRELOG_TYPE_STRING);
                    wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
                }
    wl_intern_free(intern);
}
static wl_columnar_memory_governor_ref_t *builder_governor(void) {
    wl_columnar_memory_resolution_t r =
    {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET, .usable_bytes = 1000000,
     .mode =
         WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r); CHECK(g); return g;
}
static void builder_memory(void) {
    for (unsigned shared = 0; shared < 2; ++shared)for (unsigned denied =
            0; denied < 2;
            ++denied){
            wl_intern_t *intern = wl_intern_create(); CHECK(intern);
            int64_t x = wl_intern_put(intern, "x"); CHECK(x >= 0);
            wl_columnar_memory_governor_ref_t *gi = builder_governor(),
                *go = shared?gi:builder_governor();
            CHECK(!wl_intern_attach_memory_governor(intern, gi));
            wl_columnar_memory_governor_t *bi =
                wl_columnar_memory_governor_ref_get(gi),
                *bo = wl_columnar_memory_governor_ref_get(go);
            uint64_t base = wl_columnar_memory_reserved(bi);
            uint8_t bytes[10] = {WL_PLAN_EXPR_CONST_INT};
            memcpy(bytes+1, &x, 8); bytes[9] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
            wl_evaluation_control_t *c = begin(0); bool supported = false;
            wl_columnar_expr_attempt_scalar_t *program = NULL;
            CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
                sizeof(bytes), go, &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) && supported);
            uint64_t owner = wl_columnar_expr_test_scalar_owner_size()+
                sizeof(bytes), scratch = 2;
            uint64_t peak = 2*base+2+(shared?owner+scratch:0);
            if (!shared) atomic_store_explicit(&bo->usable_bytes, owner+scratch,
                    memory_order_relaxed);
            atomic_store_explicit(&bi->usable_bytes, peak-denied,
                memory_order_relaxed);
            wl_columnar_expr_context_t context = {.intern = intern};
            int64_t value = 999; bool failed = true;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
            int rc = wl_columnar_expr_eval_scalar_attempt_controlled(program,
                    NULL, 0, &context, &value, &failed, &status, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(rc == (denied?ENOMEM:0));
            if (denied) {
                CHECK(value == 999 && failed &&
                    status == WL_COLUMNAR_EXPR_INVALID_RESULT &&
                    wl_intern_count(intern) == 1);
                CHECK(wl_columnar_memory_reserved(bi) == base+(shared?owner:0));
                CHECK(wl_evaluation_control_finish(c, &direct_owner, ENOMEM,
                    0) == ENOMEM);
                CHECK(!wl_evaluation_control_reset(c));
                CHECK(!wl_evaluation_control_begin(c, &direct_owner));
                atomic_store_explicit(&bi->usable_bytes, peak,
                    memory_order_relaxed);
                CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                    NULL, 0, &context, &value, &failed, &status, c,
                    wl_evaluation_control_active_attempt_id(c)) && !failed);
            }
            CHECK(!strcmp(wl_intern_reverse(intern, value),
                "X") && wl_intern_count(intern) == 2);
            CHECK(wl_columnar_memory_reserved(bi) == base+2+(shared?owner:0));
            if (!shared) CHECK(wl_columnar_memory_reserved(bo) == owner);
            wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
            wl_intern_free(intern);
            CHECK(!wl_columnar_memory_reserved(bi)); if (!shared) {
                CHECK(!wl_columnar_memory_reserved(bo));
                wl_columnar_memory_governor_ref_release(go);
            }
            wl_columnar_memory_governor_ref_release(gi);
        }
    /* A distinct owner governor can admit the owner but deny its real scratch. */
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    int64_t x = wl_intern_put(intern, "x"); CHECK(x >= 0);
    wl_columnar_memory_governor_ref_t *g = builder_governor();
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(g);
    uint8_t bytes[10] = {WL_PLAN_EXPR_CONST_INT}; memcpy(bytes+1, &x, 8);
    bytes[9] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
    wl_evaluation_control_t *c = begin(0); bool supported = false;
    wl_columnar_expr_attempt_scalar_t *program = NULL;
    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
        sizeof(bytes), g, &program, &supported, c,
        wl_evaluation_control_active_attempt_id(c)) && supported);
    uint64_t owner = wl_columnar_expr_test_scalar_owner_size()+sizeof(bytes);
    atomic_store_explicit(&budget->usable_bytes, owner+1, memory_order_relaxed);
    int64_t value = 999; bool failed = true;
    wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
    wl_columnar_expr_context_t context = {.intern = intern};
    CHECK(wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
        &context, &value, &failed, &status, c,
        wl_evaluation_control_active_attempt_id(c)) == ENOSPC && value == 999 &&
        failed && status == WL_COLUMNAR_EXPR_INVALID_RESULT);
    CHECK(wl_intern_count(intern) == 1 &&
        wl_columnar_memory_reserved(budget) == owner);
    wl_columnar_expr_attempt_scalar_free(program); finish(c, ENOSPC);
    wl_intern_free(intern);
    CHECK(!wl_columnar_memory_reserved(budget));
    wl_columnar_memory_governor_ref_release(g);
}
static void builder_stack_format(void) {
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    CHECK(wl_intern_put(intern, "42") >= 0);
    wl_columnar_memory_governor_ref_t *g = builder_governor();
    uint8_t bytes[10] = {WL_PLAN_EXPR_CONST_INT}; int64_t input = 42;
    memcpy(bytes+1, &input, 8); bytes[9] = WL_PLAN_EXPR_STR_FN_TO_STRING;
    wl_evaluation_control_t *c = begin(0); bool supported = false;
    wl_columnar_expr_attempt_scalar_t *program = NULL;
    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
        sizeof(bytes), g, &program, &supported, c,
        wl_evaluation_control_active_attempt_id(c)) && supported);
    uint64_t owner = wl_columnar_expr_test_scalar_owner_size()+sizeof(bytes);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(g)->usable_bytes,
        owner, memory_order_relaxed);
    wl_columnar_expr_test_fail_allocation = 0;
    int64_t value = 999; bool failed = true;
    wl_columnar_expr_context_t context = {.intern = intern};
    wl_columnar_expr_status_t status;
    CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
        &context, &value, &failed, &status, c,
        wl_evaluation_control_active_attempt_id(c)) && !failed &&
        !strcmp(wl_intern_reverse(intern, value), "42"));
    CHECK(wl_columnar_expr_test_fail_allocation == 0);
    wl_columnar_expr_test_fail_allocation = -1;
    wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
    wl_intern_free(intern);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static uint64_t intern_overflow_before;
static void intern_accounting_overflow(wl_evaluation_control_t *c,
    unsigned phase, uint64_t progress) {
    (void)progress;
    if (phase == 2) {
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c),
            &intern_overflow_before));
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    }
}
static void allocation_late_cancel(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (c && phase == 150 &&
        progress == ENOMEM) wl_evaluation_control_request_cancel(c);
}
static void builder_errors_and_stops(void) {
    for (unsigned mode = 0; mode < 5; ++mode) {
        wl_intern_t *intern = wl_intern_create(); CHECK(intern);
        int64_t id = wl_intern_put(intern, "x"); CHECK(id >= 0);
        wl_columnar_memory_governor_ref_t *g = builder_governor();
        uint8_t bytes[32]; uint32_t n;
        if (mode == 0)n = (uint32_t)literal(bytes, "overflow", 8);
        else {
            bytes[0] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+1, &id, 8);
            bytes[9] = WL_PLAN_EXPR_STR_FN_TO_UPPER; n = 10;
        }
        wl_evaluation_control_t *c = begin(0); bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n, g,
            &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        uint64_t owner =
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
        if (mode <
            2) wl_intern_control_test_checkpoint = intern_accounting_overflow;
        else if (mode == 2)wl_columnar_expr_test_fail_allocation = 0;
        else {
            wl_intern_control_test_fail_allocation = 0;
            if (mode ==
                4) wl_columnar_expr_test_checkpoint = allocation_late_cancel;
        }
        int64_t value = 999; bool failed = true;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        wl_columnar_expr_context_t context = {.intern = intern};
        uint64_t old = wl_evaluation_control_active_attempt_id(c);
        int rc = wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL,
                0, &context, &value, &failed, &status, c, old);
        wl_intern_control_test_checkpoint = NULL;
        wl_columnar_expr_test_checkpoint = NULL;
        wl_columnar_expr_test_fail_allocation = -1;
        wl_intern_control_test_fail_allocation = -1;
        CHECK(rc == (mode < 2?EOVERFLOW:ENOMEM) && value == 999 && failed &&
            status == WL_COLUMNAR_EXPR_INVALID_RESULT);
        CHECK(wl_intern_count(intern) == 1 &&
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)) == owner);
        uint64_t consumed = 0;
        CHECK(!wl_evaluation_control_test_active_consumed(c, old,
            &consumed) && consumed > 0 && consumed < UINT64_MAX);
        if (mode < 2) CHECK(consumed == intern_overflow_before);
        CHECK(wl_evaluation_control_finish(c, &direct_owner, rc, 0) == rc);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(c,
            &report) && report.result == rc && report.execution_status == rc &&
            report.consumed == consumed && !report.cleanup_status);
        if (mode ==
            4) CHECK(report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &direct_owner));
        CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
            &context, &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c)) && !failed);
        CHECK(!strcmp(wl_intern_reverse(intern, value),
            mode == 0?"overflow":"X"));
        wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
        wl_intern_free(intern);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g))); wl_columnar_memory_governor_ref_release(g);
    }
    for (unsigned rebind = 0; rebind < 2; ++rebind)for (unsigned phase = 130;
            phase <= 138; ++phase) {
            if (phase == 132 || phase == 133 || phase == 134) continue;
            wl_intern_t *intern = wl_intern_create(); CHECK(intern);
            int64_t id = wl_intern_put(intern, "x"); CHECK(id >= 0);
            wl_columnar_memory_governor_ref_t *g = builder_governor();
            uint8_t bytes[10] = {WL_PLAN_EXPR_CONST_INT};
            memcpy(bytes+1, &id, 8); bytes[9] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
            wl_evaluation_control_t *c = begin(0); bool supported = false;
            wl_columnar_expr_attempt_scalar_t *program = NULL;
            CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
                sizeof(bytes), g, &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) && supported);
            uint64_t owner =
                wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                        g)), old = wl_evaluation_control_active_attempt_id(c);
            if (rebind) {
                rebound = false; utf8_rebind_phase = phase;
                wl_columnar_expr_test_checkpoint = utf8_rebind;
            }else {
                stop_phase = phase; stop_progress = 0;
                wl_columnar_expr_test_checkpoint = witness;
            }
            int64_t value = 999; bool failed = true;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
            wl_columnar_expr_context_t context = {.intern = intern};
            int rc = wl_columnar_expr_eval_scalar_attempt_controlled(program,
                    NULL, 0, &context, &value, &failed, &status, c, old);
            wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
            CHECK(rc == (rebind?EINVAL:WL_EVALUATION_CONTROL_CANCELLED) &&
                value == 999 && failed &&
                status == WL_COLUMNAR_EXPR_INVALID_RESULT);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)) == owner);
            CHECK(wl_intern_count(intern) == (phase >= 137?2:1));
            if (rebind) {
                uint64_t consumed = 999;
                CHECK(rebound && !wl_evaluation_control_test_active_consumed(c,
                    wl_evaluation_control_active_attempt_id(c),
                    &consumed) && !consumed);
            }else {
                CHECK(wl_evaluation_control_finish(c, &direct_owner, 0,
                    0) == rc); CHECK(!wl_evaluation_control_reset(c));
                CHECK(!wl_evaluation_control_begin(c, &direct_owner));
            }
            CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                NULL, 0, &context, &value, &failed, &status, c,
                wl_evaluation_control_active_attempt_id(c)) && !failed &&
                !strcmp(wl_intern_reverse(intern, value), "X"));
            wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
            wl_intern_free(intern);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g);
        }
}

static const char *builder_words[] = {"", "aB", " Z ", "\xE2\x82\xAC" "z"};
static const int64_t builder_numbers[] = {10, -42, INT64_MIN, INT64_MAX-1};
static const char *builder_results[4][4] = {
    {"!", "aB!", " Z !", "\xE2\x82\xAC" "z!"},
    {"", "AB", " Z ", "\xE2\x82\xAC" "Z"},
    {"", "ab", " z ", "\xE2\x82\xAC" "z"},
    {"10", "-42", "-9223372036854775808", "9223372036854775806"}
};
static const char builder_source[] = ".decl input(x:string,n:int64)\n"
    ".decl out(x:string,c:string,u:string,l:string,s:string,n:int64)\n"
    "out(x,cat(x,\"!\"),to_upper(x),to_lower(x),to_string(n),n+1) :- input(x,n).\n";
static void builder_input(fixture_t *f, unsigned i) {
    int64_t row[] = {wl_intern_put(f->plan->intern, builder_words[i]),
                     builder_numbers[i]}; CHECK(row[0] >= 0);
    CHECK(!wl_session_insert(f->session, "input", row, 1, 2));
}
static unsigned builder_word_index(wl_intern_t *intern, int64_t value) {
    const char *word = wl_intern_reverse(intern, value); CHECK(word);
    unsigned i = 0;
    while (i < 4 && strcmp(word, builder_words[i]))++i;
    CHECK(i < 4); return i;
}
static void check_builder_map(fixture_t *f, unsigned count) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->ncols == 6 && out->nrows == count);
    for (unsigned c = 0; c < 5;
        ++c) CHECK(out->column_types[c] == WIRELOG_TYPE_STRING);
    CHECK(out->column_types[5] == WIRELOG_TYPE_INT64); bool seen[4] = {0};
    for (uint32_t r = 0; r < count; ++r) {
        unsigned k = builder_word_index(f->plan->intern, out->columns[0][r]);
        CHECK(!seen[k]); seen[k] = true;
        for (unsigned op = 0; op < 4; ++op) {
            const char *result = wl_intern_reverse(f->plan->intern,
                    out->columns[op+1][r]);
            CHECK(result && !strcmp(result, builder_results[op][k]));
        }
        CHECK(out->columns[5][r] == builder_numbers[k]+1);
    }
    if (count == 4)for (unsigned i = 0; i < 4; ++i) CHECK(seen[i]);
    if (count == 3)for (unsigned i = 0; i < 4; ++i) CHECK(seen[i] == (i != 1));
    if (count == 1)CHECK(seen[1]);
}
static void builder_delta(const char *relation, const int64_t *row,
    uint32_t cols, int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 6 && (diff == 1 || diff == -1));
    unsigned k = builder_word_index(f->plan->intern, row[0]);
    for (unsigned op = 0; op < 4; ++op) {
        const char *result = wl_intern_reverse(f->plan->intern, row[op+1]);
        CHECK(result && !strcmp(result, builder_results[op][k]));
    }
    CHECK(row[5] == builder_numbers[k]+1); ++events; positives += diff == 1;
    negatives += diff == -1;
}
static unsigned builders_seen[4];
static bool builder_exhaust;
static void builder_session_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (phase == 129)for (unsigned i = 0; i < 4;
            ++i) if (progress == builder_tags[i]) ++builders_seen[i];
    if (c && phase == 147 && progress == 1) {
        if (builder_exhaust)CHECK(wl_evaluation_control_charge(c,
                UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        else wl_evaluation_control_request_cancel(c);
    }
}
static void builder_filter_delta(const char *relation, const int64_t *row,
    uint32_t cols, int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 1 && (diff == 1 || diff == -1));
    CHECK(builder_word_index(f->plan->intern, row[0]) == 1); ++events;
    positives += diff == 1; negatives += diff == -1;
}
static void session_builders(void) {
    for (unsigned mode = 0; mode < 2; ++mode) {
        fixture_t f = create(builder_source, mode != 0);
        events = positives = negatives = 0;
        wl_session_set_delta_cb(f.session, builder_delta, &f);
        memset(builders_seen, 0, sizeof(builders_seen));
        uint32_t strings = wl_intern_count(f.plan->intern);
        wl_columnar_expr_test_checkpoint = builder_session_stop;
        CHECK(!step_complete(f.session));
        wl_columnar_expr_test_checkpoint = NULL; check_builder_map(&f, 0);
        CHECK(wl_intern_count(f.plan->intern) == strings); for (unsigned i = 0;
            i < 4; ++i) CHECK(!builders_seen[i]);
        for (unsigned i = 0; i < 4; ++i)builder_input(&f, i);
        builder_exhaust = mode != 0;
        if (builder_exhaust) CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_MAX-1));
        wl_columnar_expr_test_checkpoint = builder_session_stop;
        CHECK(step_complete(f.session) ==
            (builder_exhaust?WL_EVALUATION_CONTROL_WORK_LIMIT:
            WL_EVALUATION_CONTROL_CANCELLED));
        wl_columnar_expr_test_checkpoint = NULL; CHECK(!events);
        check_builder_map(&f, 0);
        for (unsigned i = 0; i < 4; ++i)CHECK(builders_seen[i]);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!step_complete(f.session)); check_builder_map(&f, 4);
        CHECK(events == 4 && positives == 4 && !negatives);
        int64_t removed[] = {wl_intern_get(f.plan->intern, "aB"), -42};
        CHECK(!wl_session_remove(f.session, "input", removed, 1, 2));
        CHECK(!step_complete(f.session)); check_builder_map(&f, 3);
        CHECK(events == 5 && negatives == 1);
        CHECK(!step_complete(f.session) && events == 5);
        wl_columnar_memory_governor_ref_t *g =
            COL_SESSION(f.session)->memory_governor;
        wl_columnar_memory_governor_ref_retain(g); destroy(&f);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g))); wl_columnar_memory_governor_ref_release(g);
    }
    const char *expressions[] = {"cat(x,\"!\")", "to_upper(x)", "to_lower(x)",
                                 "to_string(n)"};
    for (unsigned op = 0; op < 4; ++op) {
        char source[512];
        snprintf(source, sizeof(source),
            ".decl input(x:string,n:int64)\n.decl out(x:string)\nout(x) :- input(x,n), %s = \"%s\".\n",
            expressions[op], builder_results[op][1]);
        fixture_t f = create(source, false); events = positives = negatives = 0;
        wl_session_set_delta_cb(f.session, builder_filter_delta, &f);
        CHECK(!step_complete(f.session));
        col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && !out->nrows);
        for (unsigned i = 0; i < 4; ++i)builder_input(&f, i);
        stop_phase = 136; stop_progress = 0;
        wl_columnar_expr_test_checkpoint = witness;
        CHECK(step_complete(f.session) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && !out->nrows && !events);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!step_complete(f.session));
        out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && out->nrows == 1 && events == 1 && positives == 1 &&
            !negatives);
        CHECK(builder_word_index(f.plan->intern, out->columns[0][0]) == 1);
        int64_t removed[] = {wl_intern_get(f.plan->intern, "aB"), -42};
        CHECK(!wl_session_remove(f.session, "input", removed, 1, 2));
        CHECK(!step_complete(f.session));
        out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && !out->nrows && events == 2 && negatives == 1);
        CHECK(!step_complete(f.session) && events == 2); destroy(&f);
    }
    fixture_t f = create(builder_source, false);
    CHECK(!step_complete(f.session));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = control;
    wl_session_t *second = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &second));
    wl_columnar_memory_governor_ref_t *g1 =
        COL_SESSION(f.session)->memory_governor,
        *g2 = COL_SESSION(second)->memory_governor;
    CHECK(g1 != g2); wl_columnar_memory_governor_ref_retain(g1);
    wl_columnar_memory_governor_ref_retain(g2);
    fixture_t borrowed = f; borrowed.session = second;
    builder_input(&borrowed, 1); CHECK(!step_complete(second));
    check_builder_map(&borrowed, 1);
    wl_session_destroy(second); wl_evaluation_control_release(control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            g2)));
    destroy(&f);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            g1)));
    wl_columnar_memory_governor_ref_release(g1);
    wl_columnar_memory_governor_ref_release(g2);
}
static void builders_preserve_allocations(void) {
    for (unsigned kind = 0; kind < 5; ++kind) {
        wl_intern_t *intern = wl_intern_create(); CHECK(intern);
        int64_t empty = wl_intern_put(intern, ""),
            upper = wl_intern_put(intern, "A"),
            lower = wl_intern_put(intern, "a");
        CHECK(empty >= 0 && upper >= 0 && lower >= 0);
        int64_t input = kind < 3?empty:kind == 3?upper:lower;
        uint8_t bytes[24] = {WL_PLAN_EXPR_CONST_INT};
        memcpy(bytes+1, &input, 8); uint32_t n = 9;
        if (!kind) {
            bytes[n++] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+n, &empty, 8);
            n += 8;
        }
        bytes[n++] = kind == 0?WL_PLAN_EXPR_STR_FN_CAT:kind == 1 ||
            kind == 3?WL_PLAN_EXPR_STR_FN_TO_UPPER:WL_PLAN_EXPR_STR_FN_TO_LOWER;
        wl_evaluation_control_t *c = begin(0); bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
            NULL, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        wl_columnar_expr_test_fail_allocation = 0;
        int64_t value = 999; bool failed = true;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        wl_columnar_expr_context_t context = {.intern = intern};
        CHECK(wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
            &context, &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c)) == ENOMEM &&
            value == 999 && failed &&
            status == WL_COLUMNAR_EXPR_INVALID_RESULT);
        wl_columnar_expr_test_fail_allocation = -1;
        CHECK(wl_intern_count(intern) == 3);
        wl_columnar_expr_attempt_scalar_free(program); finish(c, ENOMEM);
        wl_intern_free(intern);
    }
    for (unsigned phase = 2; phase <= 4; phase += 2) {
        wl_intern_t *intern = wl_intern_create(); CHECK(intern);
        int64_t input = wl_intern_put(intern, "x"); CHECK(input >= 0);
        wl_columnar_memory_governor_ref_t *g = builder_governor();
        uint8_t bytes[10] = {WL_PLAN_EXPR_CONST_INT};
        memcpy(bytes+1, &input, 8); bytes[9] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
        wl_evaluation_control_t *c = begin(0); bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
            sizeof(bytes), g, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        uint64_t owner =
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)),
            old = wl_evaluation_control_active_attempt_id(c);
        rebound = false; intern_rebind_phase = phase;
        wl_intern_control_test_checkpoint = rebind_intern;
        int64_t value = 999; bool failed = true;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        wl_columnar_expr_context_t context = {.intern = intern};
        CHECK(wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
            &context, &value, &failed, &status, c, old) == EINVAL);
        wl_intern_control_test_checkpoint = NULL;
        CHECK(rebound && value == 999 && failed &&
            status == WL_COLUMNAR_EXPR_INVALID_RESULT &&
            wl_intern_count(intern) == 1);
        uint64_t consumed = 999;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c),
            &consumed) && !consumed);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)) == owner);
        CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
            &context, &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c)) && !failed &&
            !strcmp(wl_intern_reverse(intern, value), "X"));
        wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
        wl_intern_free(intern);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g))); wl_columnar_memory_governor_ref_release(g);
    }
}

static void selector_primitives(void) {
    struct trim_vector {const char *text; size_t offset, length; uint64_t units;
    } trims[] = {
        {"", 0, 0, 6}, {"abc", 0, 3, 10}, {" a ", 1, 1, 12},
        {" \t\n", 3, 0, 12}, {"\ta\tb\n", 1, 3, 14},
        {"\x80" " a \x80", 0, 5, 12}, {" \t\n\r\v\fA \t\n\r\v\f", 6, 1, 32}
    };
    for (unsigned i = 0; i < sizeof(trims)/sizeof(*trims);
        ++i) for (unsigned below = 0;
            below < 2; ++below){
            wl_evaluation_control_t *c = begin(224*trims[i].units-below);
            size_t offset = 999, length = 999;
            int rc =
                wl_columnar_expr_trim_span_attempt_controlled(trims[i].text,
                    &offset, &length, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(rc == (below?WL_EVALUATION_CONTROL_WORK_LIMIT:0));
            CHECK(offset == (below?999:trims[i].offset) &&
                length == (below?999:trims[i].length)); finish(c, rc);
        }
    struct slice_vector {const char *text; int64_t start, length; bool valid;
                         size_t offset, bytes; uint64_t units;} slices[] = {
        {"ab", 0, 0, true, 0, 0, 2}, {"ab", 0, 1, true, 0, 1, 6},
        {"ab", 1, 1, true, 1, 1, 10}, {"ab", 2, 0, true, 2, 0, 10},
        {"ab", 3, 0, false, 999, 999, 14}, {"", 1, 0, false, 999, 999, 6},
        {"", 0, 10, true, 0, 0, 6},
        {"ab", 1, INT64_MAX, true, 1, 1, 14}, {"ab", -1, 1, false, 999, 999, 2},
        {"ab", 0, -1, false, 999, 999, 2},
        {"\xC2" "A", 1, 1, true, 1, 1, 11},
        {"\xE1\x80" "A", 1, 1, true, 2, 1, 12},
        {"\xF1\x80\x80" "A", 1, 1, true, 3, 1, 13},
        {"\xE1\x80", 1, 1, true, 2, 0, 12},
        {"\xE1\x80", 2, 0, false, 999, 999, 12},
        {"\xE2\x82\xAC" "x", 0, 1, true, 0, 3, 8},
        {"\xE2\x82\xAC" "x", 1, 1, true, 3, 1, 12},
        {"\xF4\x90\x80\x80", 0, 1, true, 0, 4, 9}
    };
    for (unsigned i = 0; i < sizeof(slices)/sizeof(*slices);
        ++i) for (unsigned below =
            0;
            below < 2; ++below){
            wl_evaluation_control_t *c = begin(224*slices[i].units-below);
            size_t offset = 999, bytes = 999; bool valid = true;
            int rc =
                wl_columnar_expr_utf8_slice_attempt_controlled(slices[i].text,
                    slices[i].start, slices[i].length,
                    &offset, &bytes, &valid, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(rc == (below?WL_EVALUATION_CONTROL_WORK_LIMIT:0));
            CHECK(offset == (below?999:slices[i].offset) &&
                bytes == (below?999:slices[i].bytes) &&
                valid == (below?true:slices[i].valid)); finish(c, rc);
        }
    for (unsigned rebind = 0; rebind < 2; ++rebind)for (unsigned phase = 151;
            phase <= 158; ++phase){
            wl_evaluation_control_t *c = begin(0);
            uint64_t old = wl_evaluation_control_active_attempt_id(c);
            size_t offset = 999, length = 999; bool valid = true;
            if (rebind) {
                rebound = false; utf8_rebind_phase = phase;
                wl_columnar_expr_test_checkpoint = utf8_rebind;
            }else {
                stop_phase = phase; stop_progress = 0;
                wl_columnar_expr_test_checkpoint = witness;
            }
            int rc = phase <
                155?wl_columnar_expr_trim_span_attempt_controlled(" abc ",
                    &offset, &length, c, old):
                wl_columnar_expr_utf8_slice_attempt_controlled("\xE2\x82\xAC"
                    "abc", 1, 2, &offset, &length, &valid, c, old);
            wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
            CHECK(rc == (rebind?EINVAL:WL_EVALUATION_CONTROL_CANCELLED) &&
                offset == 999 && length == 999 && valid);
            if (rebind) {
                uint64_t consumed = 999;
                CHECK(rebound && !wl_evaluation_control_test_active_consumed(c,
                    wl_evaluation_control_active_attempt_id(c),
                    &consumed) && !consumed); finish(c, 0);
            }else finish(c, rc);
        }
}
static void selector_parity(void) {
    const char *current = setlocale(LC_CTYPE, NULL); CHECK(current);
    char *saved = malloc(strlen(current)+1); CHECK(saved);
    strcpy(saved, current);
    const char *locales[] = {"C", "C.utf8", "en_US.utf8"};
    const char *words[] = {"", "abc", " a b ", " \t\n\r\v\f",
                           "\xE2\x82\xAC" "xy", "\xC2" "A", "\xE1\x80" "A",
                           "\xF1\x80\x80" "A", "\xE1\x80", "\xF4\x90\x80\x80"};
    const int64_t indices[] = {-1, 0, 1, 2, 4, INT64_MAX};
    for (unsigned locale = 0; locale < 3; ++locale) {
        if (!setlocale(LC_CTYPE, locales[locale]))continue;
        wl_intern_t *intern = wl_intern_create(); CHECK(intern);
        int64_t ids[11]; ids[10] = -1;
        for (unsigned i = 0; i < 10; ++i) {
            ids[i] = wl_intern_put(intern, words[i]); CHECK(ids[i] >= 0);
        }
        for (unsigned op = 0; op < 2; ++op)for (unsigned context_case = 0;
                context_case < 2; ++context_case)
                for (unsigned a = 0; a < 11; ++a) for (unsigned b = 0;
                        b < (op?1:6); ++b) for (unsigned d = 0; d < (op?1:6);
                            ++d){
                            uint8_t bytes[32] = {WL_PLAN_EXPR_CONST_INT};
                            memcpy(bytes+1, &ids[a], 8); uint32_t n = 9;
                            if (!op) {
                                bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                                memcpy(bytes+n, &indices[b], 8); n += 8;
                                bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                                memcpy(bytes+n, &indices[d], 8); n += 8;
                            }
                            bytes[n++] =
                                op?WL_PLAN_EXPR_STR_FN_TRIM:
                                WL_PLAN_EXPR_STR_FN_SUBSTR;
                            wl_evaluation_control_t *c = begin(0);
                            bool supported = false;
                            wl_columnar_expr_attempt_scalar_t *program = NULL;
                            CHECK(
                                !
                                wl_columnar_expr_compile_scalar_attempt_controlled
                                    (bytes, n, NULL, &program, &supported, c,
                                wl_evaluation_control_active_attempt_id(c)) &&
                                supported);
                            wl_columnar_expr_context_t context =
                            {.intern = context_case?NULL:intern};
                            int64_t raw = 999, value = 999; bool failed = true;
                            wl_columnar_expr_status_t raw_status, status;
                            CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL,
                                0, &raw, &context, &raw_status));
                            CHECK(
                                !wl_columnar_expr_eval_scalar_attempt_controlled
                                    (program, NULL, 0, &context, &value,
                                &failed, &status, c,
                                wl_evaluation_control_active_attempt_id(c)) &&
                                !failed && value == raw &&
                                status == raw_status);
                            wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
                            CHECK(
                                !
                                wl_columnar_expr_scalar_result_type_attempt_controlled
                                    (program, NULL, 0, &type, &supported, c,
                                wl_evaluation_control_active_attempt_id(c)) &&
                                supported && type == WIRELOG_TYPE_STRING);
                            wl_columnar_expr_attempt_scalar_free(program);
                            finish(c, 0);
                        }
        wl_intern_free(intern);
    }
    CHECK(setlocale(LC_CTYPE, saved)); free(saved);
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    CHECK(wl_intern_put(intern, "") >= 0);
    for (unsigned op = 0; op < 3; ++op) {
        uint8_t bytes[700]; uint32_t n = 0;
        if (op == 2) {
            for (int64_t value = 0; value < COL_FILTER_STACK; ++value) {
                bytes[n++] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+n, &value, 8);
                n += 8;
            }
        }
        bytes[n++] = op ==
            1?WL_PLAN_EXPR_STR_FN_TRIM:WL_PLAN_EXPR_STR_FN_SUBSTR;
        if (op == 2) {
            int64_t marker = 7; double floating = 2.0;
            bytes[n++] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+n, &marker, 8);
            n += 8;
            bytes[n++] = WL_PLAN_EXPR_CONST_FLOAT;
            memcpy(bytes+n, &floating, 8); n += 8;
        }
        wl_evaluation_control_t *c = begin(0); bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
            NULL, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        wl_columnar_expr_context_t context = {.intern = intern};
        int64_t raw = 999, value = 999; bool failed = true;
        wl_columnar_expr_status_t raw_status, status;
        CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0, &raw, &context,
            &raw_status));
        CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
            &context, &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c)) && !failed &&
            value == raw && status == raw_status);
        wirelog_column_type_t type = WIRELOG_TYPE_INT64;
        CHECK(!wl_columnar_expr_scalar_result_type_attempt_controlled(program,
            NULL, 0, &type, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported &&
            type == (op == 2?WIRELOG_TYPE_FLOAT:WIRELOG_TYPE_STRING));
        wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
    }
    wl_intern_free(intern);
}
static const char *selector_words[] = {" ab ", "\xE2\x82\xAC" "xyz",
                                       " \t\n\r\v\f", "\xC2" "A"};
static const int64_t selector_starts[] = {1, 1, 6, -1},
    selector_lengths[] = {2, 2, 0, 1};
static const char *selector_substrings[] = {"ab", "xy", "", NULL};
static const char *selector_trims[] = {"ab", "\xE2\x82\xAC" "xyz", "",
                                       "\xC2" "A"};
static const char selector_source[] =
    ".decl input(x:string,p:int64,n:int64)\n.decl out(x:string,s:string,t:string,k:int64)\n"
    "out(x,substr(x,p,n),trim(x),p+1) :- input(x,p,n).\n";
static void selector_input(fixture_t *f, unsigned i) {
    int64_t row[] = {wl_intern_put(f->plan->intern, selector_words[i]),
                     selector_starts[i], selector_lengths[i]};
    CHECK(row[0] >= 0);
    CHECK(!wl_session_insert(f->session, "input", row, 1, 3));
}
static unsigned selector_index(wl_intern_t *intern, int64_t id) {
    const char *word = wl_intern_reverse(intern, id); CHECK(word);
    unsigned i = 0;
    while (i < 4 && strcmp(word, selector_words[i]))++i;
    CHECK(i < 4); return i;
}
static void selector_values(fixture_t *f, const int64_t *row) {
    unsigned k = selector_index(f->plan->intern, row[0]);
    if (selector_substrings[k]) {
        const char *s = wl_intern_reverse(f->plan->intern, row[1]);
        CHECK(s && !strcmp(s, selector_substrings[k]));
    }else CHECK(row[1] == -1);
    const char *t = wl_intern_reverse(f->plan->intern, row[2]);
    CHECK(t && !strcmp(t, selector_trims[k]));
    CHECK(row[3] == selector_starts[k]+1);
}
static void check_selector_map(fixture_t *f, unsigned count) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->ncols == 4 && out->nrows == count);
    for (unsigned i = 0; i < 3;
        ++i) CHECK(out->column_types[i] == WIRELOG_TYPE_STRING);
    CHECK(out->column_types[3] == WIRELOG_TYPE_INT64); bool seen[4] = {0};
    for (uint32_t r = 0; r < count; ++r) {
        int64_t row[4];
        for (unsigned i = 0; i < 4; ++i) row[i] = out->columns[i][r];
        unsigned k = selector_index(f->plan->intern, row[0]); CHECK(!seen[k]);
        seen[k] = true; selector_values(f, row);
    }
    if (count == 4)for (unsigned i = 0; i < 4; ++i) CHECK(seen[i]);
    if (count == 3)for (unsigned i = 0; i < 4; ++i) CHECK(seen[i] == (i != 0));
    if (count == 1)CHECK(seen[1]);
}
static void selector_delta(const char *relation, const int64_t *row,
    uint32_t cols, int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 4 && (diff == 1 || diff == -1));
    selector_values(f, row);
    ++events; positives += diff == 1; negatives += diff == -1;
}
static bool selector_exhaust;
static unsigned selector_seen[2];
static void selector_session_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (phase == 159) {
        if (progress == WL_PLAN_EXPR_STR_FN_SUBSTR) ++selector_seen[0];
        else if (progress == WL_PLAN_EXPR_STR_FN_TRIM) ++selector_seen[1];
    }
    if (c && phase == 91 && progress == 1 && selector_seen[0] >= 2) {
        if (selector_exhaust)CHECK(wl_evaluation_control_charge(c,
                UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        else wl_evaluation_control_request_cancel(c);
    }
}
static void selector_filter_delta(const char *relation, const int64_t *row,
    uint32_t cols, int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 1 && (diff == 1 || diff == -1));
    CHECK(selector_index(f->plan->intern, row[0]) == 0); ++events;
    positives += diff == 1; negatives += diff == -1;
}
static void session_selectors(void) {
    for (unsigned mode = 0; mode < 2; ++mode) {
        fixture_t f = create(selector_source, mode != 0);
        events = positives = negatives = 0;
        wl_session_set_delta_cb(f.session, selector_delta, &f);
        memset(selector_seen, 0, sizeof(selector_seen));
        wl_columnar_expr_test_checkpoint = selector_session_stop;
        CHECK(!step_complete(f.session));
        wl_columnar_expr_test_checkpoint = NULL;
        check_selector_map(&f, 0);
        CHECK(!selector_seen[0] && !selector_seen[1]); for (unsigned i = 0;
            i < 4; ++i) selector_input(&f, i);
        selector_exhaust = mode != 0;
        if (selector_exhaust) CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_MAX-1));
        wl_columnar_expr_test_checkpoint = selector_session_stop;
        CHECK(step_complete(f.session) ==
            (selector_exhaust?WL_EVALUATION_CONTROL_WORK_LIMIT:
            WL_EVALUATION_CONTROL_CANCELLED));
        wl_columnar_expr_test_checkpoint = NULL;
        CHECK(selector_seen[0] >= 2 && selector_seen[1] >= 1 && !events);
        check_selector_map(&f, 0);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!step_complete(f.session)); check_selector_map(&f, 4);
        CHECK(events == 4 && positives == 4 && !negatives);
        int64_t removed[] = {wl_intern_get(f.plan->intern, selector_words[0]),
                             selector_starts[0], selector_lengths[0]};
        CHECK(!wl_session_remove(f.session, "input", removed, 1, 3));
        CHECK(!step_complete(f.session)); check_selector_map(&f, 3);
        CHECK(events == 5 && negatives == 1);
        CHECK(!step_complete(f.session) && events == 5);
        wl_columnar_memory_governor_ref_t *g =
            COL_SESSION(f.session)->memory_governor;
        wl_columnar_memory_governor_ref_retain(g); destroy(&f);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g))); wl_columnar_memory_governor_ref_release(g);
    }
    const char *functions[] = {"substr(x,p,n)", "trim(x)"};
    for (unsigned op = 0; op < 2; ++op) {
        char source[512];
        snprintf(source, sizeof(source),
            ".decl input(x:string,p:int64,n:int64)\n.decl out(x:string)\nout(x) :- input(x,p,n), %s = \"ab\".\n",
            functions[op]);
        fixture_t f = create(source, false); events = positives = negatives = 0;
        wl_session_set_delta_cb(f.session, selector_filter_delta, &f);
        CHECK(!step_complete(f.session));
        for (unsigned i = 0; i < 4; ++i) selector_input(&f, i);
        stop_phase = 136; stop_progress = 0;
        wl_columnar_expr_test_checkpoint = witness;
        CHECK(step_complete(f.session) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0; CHECK(!events);
        col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && !out->nrows);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!step_complete(f.session));
        out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && out->nrows == 1 && selector_index(f.plan->intern,
            out->columns[0][0]) == 0 && events == 1);
        int64_t removed[] = {wl_intern_get(f.plan->intern, selector_words[0]),
                             selector_starts[0], selector_lengths[0]};
        CHECK(!wl_session_remove(f.session, "input", removed, 1, 3));
        CHECK(!step_complete(f.session));
        out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && !out->nrows && events == 2 && negatives == 1);
        CHECK(!step_complete(f.session) && events == 2); destroy(&f);
    }
}
static void selector_allocations(void) {
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    int64_t input = wl_intern_put(intern, ""); CHECK(input >= 0);
    for (unsigned kind = 0; kind < 4; ++kind) {
        uint8_t bytes[32] = {WL_PLAN_EXPR_CONST_INT};
        memcpy(bytes+1, &input, 8); uint32_t n = 9;
        if (kind != 0) {
            int64_t start = kind == 1?0:kind == 2?-1:1, length = 0;
            bytes[n++] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+n, &start, 8);
            n += 8; bytes[n++] = WL_PLAN_EXPR_CONST_INT;
            memcpy(bytes+n, &length, 8); n += 8;
        }
        bytes[n++] = kind ==
            0?WL_PLAN_EXPR_STR_FN_TRIM:WL_PLAN_EXPR_STR_FN_SUBSTR;
        wl_evaluation_control_t *c = begin(0); bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
            NULL, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        wl_columnar_expr_test_fail_allocation = 0;
        int64_t value = 999; bool failed = true;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        wl_columnar_expr_context_t context = {.intern = intern};
        int rc = wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL,
                0, &context, &value, &failed, &status, c,
                wl_evaluation_control_active_attempt_id(c));
        if (kind < 2)CHECK(rc == ENOMEM && value == 999 && failed &&
                status == WL_COLUMNAR_EXPR_INVALID_RESULT);
        else CHECK(!rc && !failed && value == -1 &&
                wl_columnar_expr_test_fail_allocation == 0);
        wl_columnar_expr_test_fail_allocation = -1;
        wl_columnar_expr_attempt_scalar_free(program); finish(c, rc);
    }
    wl_intern_free(intern);
}

static void selector_extra_resources(void) {
    for (unsigned op = 0; op < 2; ++op)for (unsigned which = 0; which < 2;
            ++which) {
            wl_intern_t *intern = wl_intern_create(); CHECK(intern);
            int64_t input = wl_intern_put(intern, " x "); CHECK(input >= 0);
            wl_columnar_memory_governor_ref_t *g = builder_governor();
            uint8_t bytes[32] = {WL_PLAN_EXPR_CONST_INT};
            memcpy(bytes+1, &input, 8); uint32_t n = 9;
            if (!op) {
                int64_t one = 1; for (unsigned i = 0; i < 2; ++i) {
                    bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                    memcpy(bytes+n, &one, 8); n += 8;
                }
            }
            bytes[n++] = op?WL_PLAN_EXPR_STR_FN_TRIM:WL_PLAN_EXPR_STR_FN_SUBSTR;
            wl_evaluation_control_t *c = begin(0); bool supported = false;
            wl_columnar_expr_attempt_scalar_t *program = NULL;
            CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
                g, &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) && supported);
            uint64_t owner =
                wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                        g)), old = wl_evaluation_control_active_attempt_id(c);
            unsigned phase = which?137:44; rebound = false;
            utf8_rebind_phase = phase;
            wl_columnar_expr_test_checkpoint = utf8_rebind;
            int64_t value = 999; bool failed = true;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
            wl_columnar_expr_context_t context = {.intern = intern};
            CHECK(wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL,
                0, &context, &value, &failed, &status, c, old) == EINVAL);
            wl_columnar_expr_test_checkpoint = NULL;
            CHECK(rebound && value == 999 && failed &&
                status == WL_COLUMNAR_EXPR_INVALID_RESULT);
            CHECK(wl_intern_count(intern) == (which?2:1) &&
                wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                    g)) == owner);
            uint64_t consumed = 999;
            CHECK(!wl_evaluation_control_test_active_consumed(c,
                wl_evaluation_control_active_attempt_id(c),
                &consumed) && !consumed);
            CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                NULL, 0, &context, &value, &failed, &status, c,
                wl_evaluation_control_active_attempt_id(c)) && !failed &&
                !strcmp(wl_intern_reverse(intern, value), "x"));
            wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
            wl_intern_free(intern);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g);
        }
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    for (unsigned op = 0; op < 2; ++op) {
        uint8_t bytes[32]; const uint8_t payload[] = {'a', 0, 'b'};
        uint32_t n = (uint32_t)literal(bytes, payload, sizeof(payload));
        if (!op) {
            int64_t start = 2, length = 1; bytes[n++] = WL_PLAN_EXPR_CONST_INT;
            memcpy(bytes+n, &start, 8); n += 8;
            bytes[n++] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+n, &length, 8);
            n += 8;
        }
        bytes[n++] = op?WL_PLAN_EXPR_STR_FN_TRIM:WL_PLAN_EXPR_STR_FN_SUBSTR;
        wl_evaluation_control_t *c = begin(0); bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
            NULL, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        int64_t raw = 999, value = 999; bool failed = true;
        wl_columnar_expr_status_t raw_status, status;
        wl_columnar_expr_context_t context = {.intern = intern};
        CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0, &raw, &context,
            &raw_status));
        CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program, NULL, 0,
            &context, &value, &failed, &status, c,
            wl_evaluation_control_active_attempt_id(c)) && !failed &&
            value == raw && status == raw_status);
        CHECK(op?value >= 0 && !strcmp(wl_intern_reverse(intern, value),
            "a"):value == -1);
        wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
    }
    wl_intern_free(intern);
    fixture_t f = create(selector_source, false);
    CHECK(!step_complete(f.session));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = control;
    wl_session_t *second = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &second));
    wl_columnar_memory_governor_ref_t *g1 =
        COL_SESSION(f.session)->memory_governor,
        *g2 = COL_SESSION(second)->memory_governor;
    CHECK(g1 != g2); wl_columnar_memory_governor_ref_retain(g1);
    wl_columnar_memory_governor_ref_retain(g2);
    fixture_t borrowed = f; borrowed.session = second;
    selector_input(&borrowed, 1); CHECK(!step_complete(second));
    check_selector_map(&borrowed, 1);
    wl_session_destroy(second); wl_evaluation_control_release(control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            g2)));
    destroy(&f);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            g1)));
    wl_columnar_memory_governor_ref_release(g1);
    wl_columnar_memory_governor_ref_release(g2);
}

static uint32_t replace_bytes(uint8_t *bytes, const int64_t args[3]) {
    uint32_t n = 0;
    for (unsigned i = 0; i < 3; ++i) {
        bytes[n++] = WL_PLAN_EXPR_CONST_INT;
        memcpy(bytes+n, &args[i], 8); n += 8;
    }
    bytes[n++] = WL_PLAN_EXPR_STR_FN_STR_REPLACE;
    return n;
}
static void replace_sizes(void) {
    for (unsigned below = 0; below < 2; ++below) {
        wl_evaluation_control_t *c = begin(448-below);
        size_t size = 999;
        int rc = wl_columnar_expr_replace_size_attempt_controlled(3, 2, 1, 1,
                &size, c, wl_evaluation_control_active_attempt_id(c));
        CHECK(rc == (below?WL_EVALUATION_CONTROL_WORK_LIMIT:0));
        CHECK(size == (below?999:2)); finish(c, rc);
    }
    struct {size_t src, old, replacement, count; int rc; size_t expected;
    } v[] = {
        {4, 2, 0, 2, 0, 0}, {0, 1, SIZE_MAX, 0, 0, 0},
        {1, 1, SIZE_MAX, 1, EOVERFLOW, 999},
        {SIZE_MAX-1, 1, 2, 1, EOVERFLOW, 999}, {3, 2, 1, 2, EINVAL, 999}
    };
    for (unsigned i = 0; i < sizeof(v)/sizeof(*v); ++i) {
        wl_evaluation_control_t *c = begin(0); size_t size = 999;
        int rc = wl_columnar_expr_replace_size_attempt_controlled(v[i].src,
                v[i].old, v[i].replacement, v[i].count, &size, c,
                wl_evaluation_control_active_attempt_id(c));
        CHECK(rc == v[i].rc && size == v[i].expected); finish(c, rc);
    }
}
static void replace_parity(void) {
    const char *v[][4] = {
        {"", "", "x", ""}, {"", "a", "x", ""},
        {"abc", "", "x", "abc"}, {"abc", "d", "x", "abc"},
        {"aaa", "aa", "x", "xa"}, {"aaaa", "aa", "", ""},
        {"aaaaab", "aaab", "x", "aax"},
        {"ababa", "a", "long", "longblongblong"},
        {"aaaa", "aa", "aa", "aaaa"}, {"a", "a", "a", "a"},
        {"\x80\xff\x80", "\x80", "\xc2", "\xc2\xff\xc2"}
    };
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    for (unsigned i = 0; i < sizeof(v)/sizeof(*v)+4; ++i) {
        int64_t args[3];
        for (unsigned j = 0; j < 3; ++j)
            args[j] = wl_intern_put(intern, v[i < sizeof(v)/sizeof(*v)?i:0][j]);
        if (i >= sizeof(v)/sizeof(*v) && i < sizeof(v)/sizeof(*v)+3)
            args[i-sizeof(v)/sizeof(*v)] = -1;
        uint8_t bytes[32]; uint32_t n = replace_bytes(bytes, args);
        wl_evaluation_control_t *c = begin(0); bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
            NULL, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        for (unsigned null = 0; null < 2; ++null) {
            wl_columnar_expr_context_t context = {.intern = null?NULL:intern};
            int64_t raw = 999, value = 999; bool failed = true;
            wl_columnar_expr_status_t a, b;
            CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0, &raw,
                &context, &a));
            CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                NULL, 0, &context, &value, &failed, &b, c,
                wl_evaluation_control_active_attempt_id(c)) && !failed &&
                value == raw && a == b);
            if (!null && i < sizeof(v)/sizeof(*v))
                CHECK(!strcmp(wl_intern_reverse(intern, value), v[i][3]));
        }
        wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
    }
    wl_intern_free(intern);
}
static unsigned replace_pass, replace_target;
static bool replace_rebind;
static void replace_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (phase == 172 || phase == 175) replace_pass = phase;
    if (c && phase == 47 && progress >= 2 && replace_pass == replace_target) {
        if (replace_rebind) utf8_rebind(c, utf8_rebind_phase, 0, false);
        else wl_evaluation_control_request_cancel(c);
    }
}
static void replace_resources(void) {
    for (unsigned pass = 0; pass < 4; ++pass)
        for (unsigned rebind = 0; rebind < 2; ++rebind) {
            wl_intern_t *intern = wl_intern_create(); CHECK(intern);
            int64_t args[] = {wl_intern_put(intern, "aaaaab"),
                              wl_intern_put(intern, "aaab"),
                              wl_intern_put(intern, "x")};
            uint8_t bytes[32]; uint32_t n = replace_bytes(bytes, args);
            wl_columnar_memory_governor_ref_t *g = builder_governor();
            wl_evaluation_control_t *c = begin(0); bool supported = false;
            wl_columnar_expr_attempt_scalar_t *program = NULL;
            uint64_t id = wl_evaluation_control_active_attempt_id(c);
            CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
                n, g, &program, &supported, c, id) && supported);
            uint64_t reserved = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(g));
            replace_pass = 0; replace_target = pass?175:172;
            replace_rebind = rebind != 0; rebound = false;
            utf8_rebind_phase = pass < 2?47:pass == 2?44:137;
            stop_phase = utf8_rebind_phase; stop_progress = 0;
            wl_columnar_expr_test_checkpoint = pass <
                2?replace_stop:rebind?utf8_rebind:witness;
            int64_t value = 999; bool failed = true;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
            wl_columnar_expr_context_t context = {.intern = intern};
            int rc = wl_columnar_expr_eval_scalar_attempt_controlled(program,
                    NULL, 0, &context, &value, &failed, &status, c, id);
            wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
            CHECK(rc == (rebind?EINVAL:WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(value == 999 && failed &&
                status == WL_COLUMNAR_EXPR_INVALID_RESULT);
            CHECK(wl_intern_count(intern) == (pass == 3?4:3) && reserved ==
                wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                    g)));
            if (rebind) {
                uint64_t consumed = 999;
                CHECK(rebound && !wl_evaluation_control_test_active_consumed(c,
                    wl_evaluation_control_active_attempt_id(c),
                    &consumed) && !consumed);
            } else {
                CHECK(wl_evaluation_control_finish(c, &direct_owner, 0,
                    0) == rc);
                CHECK(!wl_evaluation_control_reset(c));
                CHECK(!wl_evaluation_control_begin(c, &direct_owner));
            }
            CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                NULL, 0, &context, &value, &failed, &status, c,
                wl_evaluation_control_active_attempt_id(c)) && !failed &&
                !strcmp(wl_intern_reverse(intern, value), "aax"));
            wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g); wl_intern_free(intern);
        }
    /* The two shortcuts allocate neither scratch nor a speculative intern copy. */
    for (unsigned matched = 0; matched < 3; ++matched) {
        wl_intern_t *intern = wl_intern_create(); CHECK(intern);
        int64_t args[] = {wl_intern_put(intern, "aaa"),
                          wl_intern_put(intern,
                              matched == 0?"":matched == 1?"z":"a"),
                          wl_intern_put(intern, "a")};
        uint8_t bytes[32]; uint32_t n = replace_bytes(bytes, args);
        wl_evaluation_control_t *c = begin(0); bool supported = false;
        wl_columnar_expr_attempt_scalar_t *program = NULL;
        CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
            NULL, &program, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) && supported);
        wl_columnar_expr_test_fail_allocation = 0;
        int64_t value = 999; bool failed = true;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        wl_columnar_expr_context_t context = {.intern = intern};
        int rc = wl_columnar_expr_eval_scalar_attempt_controlled(program,
                NULL, 0, &context, &value, &failed, &status, c,
                wl_evaluation_control_active_attempt_id(c));
        CHECK(matched == 2?rc == ENOMEM && value == 999:
            !rc && !failed && value == args[0] &&
            wl_columnar_expr_test_fail_allocation == 0);
        wl_columnar_expr_test_fail_allocation = -1;
        wl_columnar_expr_attempt_scalar_free(program); finish(c, rc);
        wl_intern_free(intern);
    }
}

static void replace_delta(const char *relation, const int64_t *row,
    uint32_t cols, int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && (cols == 1 || cols == 3));
    const char *x = wl_intern_reverse(f->plan->intern, row[0]);
    CHECK(x && (!strcmp(x, "aaaa") || !strcmp(x, "ba")));
    CHECK(diff == 1 || diff == -1); ++events;
    positives += diff == 1; negatives += diff == -1;
}
static void replace_budget(void) {
    for (unsigned shared = 0; shared < 2; ++shared)
        for (unsigned shortcut = 0; shortcut < 3; ++shortcut) {
            wl_intern_t *intern = wl_intern_create(); CHECK(intern);
            int64_t args[] = {wl_intern_put(intern, "aaa"),
                              wl_intern_put(intern,
                                  shortcut == 0?"":shortcut == 1?"z":"a"),
                              wl_intern_put(intern, "a")};
            wl_columnar_memory_governor_ref_t *gi = builder_governor(),
                *go = shared?gi:builder_governor();
            CHECK(!wl_intern_attach_memory_governor(intern, gi));
            wl_columnar_memory_governor_t *bi =
                wl_columnar_memory_governor_ref_get(gi),
                *bo = wl_columnar_memory_governor_ref_get(go);
            uint8_t bytes[32]; uint32_t n = replace_bytes(bytes, args);
            wl_evaluation_control_t *c = begin(0); bool supported = false;
            wl_columnar_expr_attempt_scalar_t *program = NULL;
            CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
                go,
                &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) && supported);
            uint64_t old_i = wl_columnar_memory_reserved(bi),
                old_o = wl_columnar_memory_reserved(bo);
            atomic_store_explicit(&bi->usable_bytes, old_i,
                memory_order_relaxed);
            atomic_store_explicit(&bo->usable_bytes, old_o,
                memory_order_relaxed);
            int64_t value = 999; bool failed = true;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_INVALID_RESULT;
            wl_columnar_expr_context_t context = {.intern = intern};
            int rc = wl_columnar_expr_eval_scalar_attempt_controlled(program,
                    NULL, 0,
                    &context, &value, &failed, &status, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(shortcut == 2?rc == ENOSPC && value == 999:
                !rc && !failed && value == args[0]);
            CHECK(wl_columnar_memory_reserved(bi) == old_i &&
                wl_columnar_memory_reserved(bo) == old_o);
            wl_columnar_expr_attempt_scalar_free(program); finish(c, rc);
            wl_intern_free(intern); CHECK(!wl_columnar_memory_reserved(bi));
            CHECK(!wl_columnar_memory_reserved(bo));
            if (!shared) wl_columnar_memory_governor_ref_release(go);
            wl_columnar_memory_governor_ref_release(gi);
        }
}
static void session_replace(void) {
    for (unsigned filter = 0; filter < 2; ++filter) {
        const char *source = filter?
            ".decl input(x:string)\n.decl out(x:string)\nout(x) :- input(x), str_replace(x,\"aa\",\"z\") = \"zz\".\n"
            :
            ".decl input(x:string)\n.decl out(x:string,y:string,n:int64)\nout(x,str_replace(x,\"aa\",\"z\"),strlen(x)) :- input(x).\n";
        fixture_t f = create(source, filter != 0);
        events = positives = negatives = 0;
        wl_session_set_delta_cb(f.session, replace_delta, &f);
        CHECK(!step_complete(f.session)); add(&f, "aaaa"); add(&f, "ba");
        stop_phase = 175; stop_progress = 0;
        wl_columnar_expr_test_checkpoint = witness;
        CHECK(step_complete(f.session) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_columnar_expr_test_checkpoint = NULL; stop_phase = 0;
        col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && !out->nrows && !events);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!step_complete(f.session));
        out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && out->nrows == (filter?1:2) &&
            out->column_types[0] == WIRELOG_TYPE_STRING);
        for (uint32_t r = 0; r < out->nrows; ++r) {
            const char *x = wl_intern_reverse(f.plan->intern,
                    out->columns[0][r]);
            CHECK(x && (!strcmp(x, "aaaa") || (!filter && !strcmp(x, "ba"))));
            if (!filter) {
                CHECK(out->column_types[1] == WIRELOG_TYPE_STRING);
                CHECK(!strcmp(wl_intern_reverse(f.plan->intern,
                    out->columns[1][r]),
                    !strcmp(x, "aaaa")?"zz":"ba"));
                CHECK(out->columns[2][r] == (int64_t)strlen(x));
            }
        }
        CHECK(events == (filter?1:2) && positives == events && !negatives);
        int64_t removed = wl_intern_get(f.plan->intern, "aaaa");
        CHECK(!wl_session_remove(f.session, "input", &removed, 1, 1));
        CHECK(!step_complete(f.session));
        out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && out->nrows == (filter?0:1));
        CHECK(events == (filter?2:3) && negatives == 1);
        CHECK(!step_complete(f.session)); destroy(&f);
    }
}

static fixture_t *joined_active;
static unsigned joined_route, joined_ack, joined_forward, joined_mode;
static bool joined_stopped;
static uint64_t joined_successor;
static wl_evaluation_control_report_t joined_report;
static void joined_route_hook(wl_evaluation_control_t *c, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t work) {
    (void)generation; (void)extent; (void)work;
    CHECK(joined_active && c == joined_active->control);
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_EXCHANGED) ++joined_route;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK) ++joined_ack;
    if (joined_stopped && joined_mode >= 2 &&
        (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE ||
        event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK)) ++joined_forward;
}
static void joined_scalar_hook(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress; (void)failed;
    if (!c || joined_stopped || phase != (joined_mode == 2?76:137)) return;
    CHECK(joined_route && joined_ack);
    joined_stopped = true;
    if (joined_mode < 2) {
        if (joined_mode) CHECK(wl_evaluation_control_charge(c, UINT64_MAX) ==
                WL_EVALUATION_CONTROL_WORK_LIMIT);
        else wl_evaluation_control_request_cancel(c);
        return;
    }
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    wl_evaluation_control_attempt_finish_result_t end =
        wl_evaluation_control_finish_attempt(c,
            joined_active->session->operation_admission, id, 0, 0);
    CHECK(!end.protocol && end.report_valid &&
        end.kind == WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED &&
        !end.report.result && end.report.consumed);
    joined_report = end.report;
    CHECK(!wl_evaluation_control_reset(c));
    wl_evaluation_control_attempt_begin_result_t next =
        wl_evaluation_control_begin_attempt(c,
            joined_active->session->operation_admission);
    CHECK(!next.protocol && next.attempt_id != id);
    joined_successor = next.attempt_id;
}
static void joined_array_hook(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress) {
    if (joined_mode == 4 && phase == 142 && !progress)
        joined_scalar_hook(c, 137, 0, false);
}
static void joined_delta(const char *relation, const int64_t *row,
    uint32_t cols, int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(relation, "out") && cols == 3 && (diff == 1 || diff == -1));
    const char *x = wl_intern_reverse(f->plan->intern, row[0]),
        *y = wl_intern_reverse(f->plan->intern, row[1]);
    CHECK(x && y && row[2] == 1);
    CHECK((!strcmp(x, "aaaa") && !strcmp(y, "zz")) ||
        (!strcmp(x, "ba") && !strcmp(y, "ba")));
    ++events; positives += diff == 1; negatives += diff == -1;
}
static void joined_contents(fixture_t *f, unsigned count) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == count && out->ncols == 3 &&
        out->column_types[0] == WIRELOG_TYPE_STRING &&
        out->column_types[1] == WIRELOG_TYPE_STRING &&
        out->column_types[2] == WIRELOG_TYPE_INT64);
    bool seen[2] = {false, false};
    for (uint32_t r = 0; r < out->nrows; ++r) {
        const char *x = wl_intern_reverse(f->plan->intern, out->columns[0][r]),
            *y = wl_intern_reverse(f->plan->intern, out->columns[1][r]);
        CHECK(x && y && out->columns[2][r] == 1);
        unsigned i = !strcmp(x, "aaaa")?0:1;
        CHECK(!seen[i] && !strcmp(x, i?"ba":"aaaa") &&
            !strcmp(y, i?"ba":"zz")); seen[i] = true;
    }
}
static void joined_strings(void) {
    const char *source =
        ".decl a(k:int64,x:string)\n.decl b(k:int64,n:int64)\n"
        ".decl out(x:string,y:string,n:int64)\n"
        "out(x,str_replace(x,\"aa\",\"z\"),n) :- a(k,x),b(k,n),n>0,contains(x,\"a\").\n";
    for (unsigned full = 0; full < 2; ++full)
        for (unsigned mode = 0; mode < 6; ++mode) {
            fixture_t f = create(source, false);
            COL_SESSION(f.session)->join_batch_bytes = 128;
            if (full) for (uint32_t st = 0; st < f.plan->stratum_count; ++st)
                    for (uint32_t r = 0; r < f.plan->strata[st].relation_count;
                        ++r) {
                        const wl_plan_relation_t *rel =
                            &f.plan->strata[st].relations[r];
                        for (uint32_t op = 0; op < rel->op_count; ++op)
                            if (rel->ops[op].op == WL_PLAN_OP_JOIN)
                                ((wl_plan_op_t *)rel->ops)[op].delta_mode =
                                    WL_DELTA_FORCE_FULL;
                    }
            joined_active = &f; joined_route = joined_ack = joined_forward = 0;
            joined_stopped = false; joined_successor = 0; joined_mode = mode;
            events = positives = negatives = 0;
            wl_session_set_delta_cb(f.session, joined_delta, &f);
            wl_columnar_join_pipeline_test_numeric_checkpoint =
                joined_route_hook;
            CHECK(!step_complete(f.session)); joined_contents(&f, 0);
            CHECK(!events);
            const char *words[] = {"aaaa", "ba", "zzzz"};
            for (unsigned i = 0; i < 3; ++i) {
                int64_t row[] = {1, wl_intern_put(f.plan->intern, words[i])};
                CHECK(row[1] >= 0 && !wl_session_insert(f.session, "a", row, 1,
                    2));
            }
            int64_t rhs[] = {1, 1};
            CHECK(!wl_session_insert(f.session, "b", rhs, 1, 2));
            if (full && !mode) {
                /* Raw strict policy forbids this unconsumed string suffix.
                 * Keep its refusal rather than broaden optimization policy. */
                COL_SESSION(f.session)->join_batch_strict = true;
                CHECK(step_complete(f.session) == ENOTSUP && !events);
                joined_contents(&f, 0);
                wl_session_t *raw = NULL;
                CHECK(!wl_session_create(wl_backend_columnar(), f.plan, 1,
                    &raw));
                COL_SESSION(raw)->join_batch_bytes = 128;
                COL_SESSION(raw)->join_batch_strict = true;
                for (unsigned i = 0; i < 3; ++i) {
                    int64_t row[] = {1,
                                     wl_intern_get(f.plan->intern, words[i])};
                    CHECK(!wl_session_insert(raw, "a", row, 1, 2));
                }
                CHECK(!wl_session_insert(raw, "b", rhs, 1, 2));
                CHECK(step_complete(raw) == ENOTSUP);
                wl_session_destroy(raw);
                CHECK(!wl_evaluation_control_reset(f.control));
                COL_SESSION(f.session)->join_batch_strict = false;
            }
            if (mode == 1) CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_MAX-1));
            if (mode == 5) COL_SESSION(f.session)->join_output_limit = 1;
            wl_columnar_filter_test_attempt_checkpoint = mode ==
                4?joined_array_hook:NULL;
            wl_columnar_expr_test_checkpoint = mode ==
                5?NULL:joined_scalar_hook;
            int rc = step_complete(f.session);
            wl_columnar_expr_test_checkpoint = NULL;
            wl_columnar_filter_test_attempt_checkpoint = NULL;
            int expected = mode == 5?EOVERFLOW:mode >= 2?EINVAL:mode?
                WL_EVALUATION_CONTROL_WORK_LIMIT:
                WL_EVALUATION_CONTROL_CANCELLED;
            if (rc != expected || (mode < 5 && !joined_stopped))
                fprintf(stderr,
                    "joined full=%u mode=%u rc=%d expected=%d stopped=%u route=%u ack=%u\n",
                    full, mode, rc, expected, joined_stopped, joined_route,
                    joined_ack);
            CHECK(rc == expected && (mode == 5 || joined_stopped));
            joined_contents(&f, 0); CHECK(!events && !joined_forward);
            CHECK(!COL_SESSION(f.session)->cleanup_active &&
                !COL_SESSION(f.session)->cleanup_pending &&
                !COL_SESSION(f.session)->cleanup_reserved_bytes);
            if (mode >= 2 && mode < 5) {
                uint64_t used = 999;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    joined_successor, &used) && !used);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report));
                CHECK(report.attempt_id == joined_report.attempt_id &&
                    report.consumed == joined_report.consumed &&
                    !report.result);
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(f.control,
                        f.session->operation_admission, joined_successor, 0, 0);
                CHECK(!end.protocol && end.report_valid &&
                    !end.report.consumed);
            }
            CHECK(!wl_evaluation_control_reset(f.control));
            COL_SESSION(f.session)->join_output_limit = 0;
            CHECK(!step_complete(f.session)); joined_contents(&f, 2);
            CHECK(joined_route && joined_ack && events == 2 && positives == 2 &&
                !negatives);
            int64_t removed[] = {1, wl_intern_get(f.plan->intern, "aaaa")};
            CHECK(!wl_session_remove(f.session, "a", removed, 1, 2));
            CHECK(!step_complete(f.session)); joined_contents(&f, 1);
            CHECK(events == 3 && negatives == 1);
            CHECK(!step_complete(f.session) && events == 3);
            wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
            joined_active = NULL;
            wl_columnar_memory_governor_ref_t *g =
                COL_SESSION(f.session)->memory_governor;
            wl_columnar_memory_governor_ref_retain(g); destroy(&f);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g);
        }
}
int main(void) {
    joined_strings();
    replace_sizes(); replace_parity(); replace_resources(); replace_budget();
    session_replace();
    selector_extra_resources();
    selector_primitives();
    selector_parity();
    session_selectors();
    selector_allocations();
    session_builders();
    builders_preserve_allocations();
    builder_primitives();
    direct_builders();
    builder_memory();
    builder_stack_format();
    builder_errors_and_stops();
    hash_unaligned_spans();
    hash_crc_primitives();
    direct_string_digests();
    session_string_digests();
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
