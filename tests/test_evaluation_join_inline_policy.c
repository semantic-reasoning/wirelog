/* Configured no-tail INLINE JOIN: ordinary consumption, strict policy order. */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

static wl_session_t *active;
static unsigned held, produced, successors;
static void
configure(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(value ? !setenv(name, value, 1) : !unsetenv(name));
#endif
}
static int
finish(wl_session_t *s)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(s);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}
static void
host(wl_evaluation_control_t *control, unsigned event, uint64_t gen,
    uint32_t extent, uint64_t work)
{
    (void)control; (void)gen; (void)extent; (void)work;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE) ++produced;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_STRICT_SUCCESSOR) ++successors;
}
static void
gather(wl_evaluation_control_t *control, unsigned phase, uint64_t progress)
{
    (void)control; (void)progress;
    if (phase != 18) return;
    wl_col_session_t *s = COL_SESSION(active);
    wl_columnar_eval_serial_framed_attempt_t *root = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(s->cleanup_active, s, &root));
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(s->cleanup_active,
        &ctx, &root->child));
    const wl_columnar_relation_scalar_source_t *sources[] =
    {&ctx->source, &ctx->join_right};
    for (unsigned i = 0; i < 2; ++i) {
        const wl_columnar_relation_scalar_source_t *r = sources[i];
        CHECK(r->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD
            && r->descriptor && r->columns ==
            (const int64_t *const *)r->descriptor->columns);
        if (r->descriptor->compound_kind == WIRELOG_COMPOUND_KIND_INLINE)
            held |= 1u << i;
    }
}
static void
verify(wl_session_t *s, bool empty)
{
    const col_rel_t *out = session_find_rel(COL_SESSION(s), "out");
    if (empty) {
        CHECK(!out || !out->nrows); return;
    }
    CHECK(out && out->nrows == 2 && out->ncols == 2);
    bool first = false, second = false;
    for (uint32_t i = 0; i < out->nrows; ++i) {
        int64_t x = col_rel_get(out, i, 0), y = col_rel_get(out, i, 1);
        if (x == 7 && y == 8) {
            CHECK(!first); first = true;
        }else {
            CHECK(x == 9 && y == 10 && !second); second = true;
        }
    }
    CHECK(first && second);
}

/* Four natural plans: left, right, both INLINE, scalar control. No constant
 * RHS predicate; the key and projection lie after the INLINE physical lanes. */
static void
run(unsigned kind, bool strict, unsigned bytes, unsigned shape, bool empty)
{
    bool li = kind == 0 || kind == 2, ri = kind == 1 || kind == 2;
    char text[768], budget[32];
    snprintf(text, sizeof(text), ".decl a(%sk:int64,x:int64)\n"
        ".decl b(%sk:int64,y:int64)\n.decl out(x:int64,y:int64)\n"
        "out(x,y):-a(%sk,x),b(%sk,y)%s.\n",
        li ? "p:pair/2 inline," : "", ri ? "p:pair/2 inline," : "",
        li ? "pair(v,w)," : "", ri ? "pair(t,u)," : "",
        shape == 1 ? ",x>y+100" : shape == 2 ? ",x<y" : "");
    snprintf(budget, sizeof(budget), "%u", bytes);
    configure("WIRELOG_JOIN_BATCH_BYTES", budget);
    configure("WIRELOG_JOIN_BATCH_STRICT", strict ? "1" : NULL);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    CHECK(plan->stratum_count == 1 && plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *p = &plan->strata[0].relations[0];
    uint32_t ji = UINT32_MAX;
    for (uint32_t i = 0; i < p->op_count; ++i)
        if (p->ops[i].op == WL_PLAN_OP_JOIN) {
            CHECK(ji == UINT32_MAX); ji = i;
        }
    CHECK(ji != UINT32_MAX && p->ops[ji].key_count == 1
        && !p->ops[ji].right_filter_expr.size && !p->ops[ji].materialized);
    CHECK(!strcmp(p->ops[ji].left_keys[0], li ? "col2" : "col0")
        && !strcmp(p->ops[ji].right_keys[0], ri ? "col2" : "col0"));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *s = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &s));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    uint32_t tail = UINT32_MAX;
    bool streaming = wl_columnar_join_pipeline_preflight(p, ji, COL_SESSION(s),
            &tail) == WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE;
    CHECK(streaming == (shape == 2));
    if (shape == 1) CHECK(p->ops[ji + 1].op == WL_PLAN_OP_FILTER);
    configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
    configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
    const int64_t left[] = {91, 92, 1, 7, 93, 94, 2, 9, 95, 96, 3, 11};
    const int64_t right[] = {81, 82, 1, 8, 83, 84, 2, 10, 85, 86, 4, 12};
    const int64_t ls[] = {1, 7, 2, 9, 3, 11}, rs[] = {1, 8, 2, 10, 4, 12};
    wl_session_t *sessions[] = {s, raw};
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_session_insert(sessions[i], "a", li ? left : ls,
            empty ? 0 : 3, li ? 4 : 2));
        CHECK(!wl_session_insert(sessions[i], "b", ri ? right : rs, 3,
            ri ? 4 : 2));
    }
    active = s; held = produced = successors = 0;
    wl_columnar_join_pipeline_test_numeric_checkpoint = host;
    wl_columnar_join_test_attempt_checkpoint = gather;
    int rc = finish(s);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    int rr = finish(raw);
    int raw_expected = strict && (shape == 1 || (!empty && bytes < 16))
        ? ENOTSUP : 0;
    int expected = shape == 2 && kind != 3 ? ENOTSUP : raw_expected;
    if (rc != expected || rr != raw_expected) fprintf(stderr,
            "kind=%u strict=%u bytes=%u shape=%u empty=%u rc=%d/%d raw=%d/%d\n",
            kind, strict, bytes, shape, empty, rc, expected, rr, raw_expected);
    CHECK(rc == expected && rr == raw_expected);
    verify(s, expected || empty || shape == 1);
    verify(raw, raw_expected || empty || shape == 1);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == expected && report.consumed &&
        !report.cleanup_status);
    if (!expected && !empty && !shape) {
        CHECK(kind == 3 && bytes >= 16 ? produced > 0 : !produced);
        CHECK(held == (kind == 3 ? 0u : kind == 2 ? 3u : kind == 0 ? 1u : 2u));
    }
    if (strict && shape != 2 && !empty) CHECK(successors > 0);
    CHECK(!wl_session_try_destroy(&s) && !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control); wl_plan_free(plan);
    wirelog_program_free(program);
}
int
main(void)
{
    for (unsigned kind = 0; kind < 4; ++kind)
        for (unsigned strict = 0; strict < 2; ++strict) {
            run(kind, strict, 256, 0, false);
            run(kind, strict, 1, 0, false);
            run(kind, strict, 1, 0, true);
            run(kind, strict, 256, 1, false);
            run(kind, strict, 256, 2, false);
        }
    return 0;
}
