/* Configured strict JOIN positives, ordered declines and cache protections. */
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

static unsigned prepared, successor, cache_steps, stop_event, stop_at;
static bool stopped;
static uint64_t attempt, consumed;
static const char *right_name = "b";
enum { NORMAL, RIGHT_FILTER, FILTER_ERROR, DELTA_RIGHT, BAD_TYPE,
       DELTA_BAD_TYPE, FILTER_BAD_TYPE };
static unsigned variant, filter_rows, intrinsic_checks;
static unsigned empty_mask;

static void
join_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)control; (void)progress;
    if (phase == 44) ++filter_rows;
    if (phase == 50) ++intrinsic_checks;
}

static void
configure(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(value ? !setenv(name, value, 1) : !unsetenv(name));
#endif
}

static void
checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t work)
{
    (void)generation; (void)extent; (void)work;
    uint64_t id = wl_evaluation_control_active_attempt_id(control), used = 0;
    CHECK(id && (!attempt || attempt == id));
    CHECK(!wl_evaluation_control_test_active_consumed(control, id, &used)
        && used >= consumed);
    attempt = id; consumed = used;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED) ++prepared;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_STRICT_SUCCESSOR) ++successor;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_STRICT_CACHE) ++cache_steps;
    unsigned hits = event == WL_COLUMNAR_JOIN_PIPELINE_TEST_STRICT_CACHE
        ? cache_steps : successor;
    if (!stopped && event == stop_event && hits == stop_at) {
        stopped = true;
        wl_evaluation_control_request_cancel(control);
    }
}

static int
finish(wl_session_t *session)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}

/* These cache states are legal INTERNAL fixtures. Owning controlled sessions
 * start with no primary index and have no public primary-cache producer. */
enum { COLD, WARM, PINNED, STALE_PIN, GROW_PIN, SPARE_PIN, EVICT_PIN,
       TOMBSTONE_PIN };

static void
verify(wl_session_t *session, unsigned expected)
{
    const col_rel_t *out = session_find_rel((wl_col_session_t *)session,
            "output");
    CHECK(out && out->ncols == 2 && out->nrows == expected);
    bool seen[8][8] = {{false}};
    for (uint32_t r = 0; r < out->nrows; ++r) {
        int64_t x = col_rel_get(out, r, 0), y = col_rel_get(out, r, 1);
        CHECK(x >= 0 && x < 8 && y >= 0 && y < 8 && !seen[x][y]);
        seen[x][y] = true;
    }
}

static void
run(const char *body, unsigned bytes, unsigned mask, int expected,
    unsigned rows, unsigned cache_mode, unsigned event, unsigned hit)
{
    char source[1024], budget[32];
    snprintf(source, sizeof(source),
        ".decl a(k:int64,x:int64)\n.decl %s(k:int64,y:int64)\n"
        ".decl p(k:int64,x:int64)\n.decl q(k:int64,x:int64)\n"
        ".decl r(k:int64,x:int64)\n.decl s(k:int64,x:int64)\n"
        ".decl output(x:int64,y:int64)\noutput(x,y):-%s.\n", right_name, body);
    snprintf(budget, sizeof(budget), "%u", bytes);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    if (variant) {
        /* Explicit internal-plan/error fixtures on an otherwise parsed plan;
         * creation-time strict admission and real host execution are retained. */
        wl_plan_op_t *join = NULL;
        for (uint32_t i = 0; i < plan->strata[0].relations[0].op_count; ++i) {
            wl_plan_op_t *op =
                &((wl_plan_op_t *)plan->strata[0].relations[0].ops)[i];
            if (op->op == WL_PLAN_OP_JOIN) join = op;
        }
        CHECK(join);
        if (variant == RIGHT_FILTER || variant == FILTER_ERROR
            || variant == FILTER_BAD_TYPE) {
            const uint8_t predicate[] = {WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0,
                                         0, 0, 0,
                                         WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0, 0,
                                         0, 0, 0,
                                         WL_PLAN_EXPR_ARITH_DIV};
            size_t size = variant != FILTER_ERROR ? 9 : sizeof(predicate);
            join->right_filter_expr.data = malloc(size);
            CHECK(join->right_filter_expr.data);
            memcpy(join->right_filter_expr.data, predicate, size);
            join->right_filter_expr.size = (uint32_t)size;
        }
        if (variant == DELTA_RIGHT || variant == DELTA_BAD_TYPE)
            join->delta_mode = WL_DELTA_FORCE_DELTA;
    }
    configure("WIRELOG_JOIN_BATCH_BYTES", budget);
    configure("WIRELOG_JOIN_BATCH_STRICT", "1");
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = control;
    wl_session_t *owned = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &owned));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
    configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
    int64_t input[16];
    for (unsigned i = 0; i < 8; ++i) {
        input[2 * i] = 1; input[2 * i + 1] = i;
    }
    wl_session_t *sessions[] = {owned, raw};
    for (unsigned i = 0; i < 2; ++i) {
        if (mask & 1) CHECK(!wl_session_insert(sessions[i], "a", input, 8, 2));
        if (mask & 2) CHECK(!wl_session_insert(sessions[i], right_name, input,
                8, 2));
        if (empty_mask & 1) CHECK(!wl_session_remove(sessions[i], "a", input, 8,
                2));
        if (empty_mask & 2) CHECK(!wl_session_remove(sessions[i], right_name,
                input, 8, 2));
        for (unsigned side = 0; side < 2; ++side) {
            if (!(empty_mask & (1u << side))) continue;
            const col_rel_t *empty =
                session_find_rel((wl_col_session_t *)sessions[i],
                    side ? right_name : "a");
            CHECK(empty && empty->schema_ok && empty->ncols == 2 &&
                !empty->nrows);
        }
        if (variant == BAD_TYPE || variant == DELTA_BAD_TYPE
            || variant == FILTER_BAD_TYPE) {
            col_rel_t *right = session_find_rel((wl_col_session_t *)sessions[i],
                    right_name);
            const wirelog_column_type_t integers[] = {WIRELOG_TYPE_INT64,
                                                      WIRELOG_TYPE_INT64};
            const wirelog_column_type_t types[] = {WIRELOG_TYPE_FLOAT,
                                                   WIRELOG_TYPE_INT64};
            CHECK(right && !col_rel_set_column_types(right, integers, 2));
            CHECK(!col_rel_set_column_types(right, types, 2));
        }
        if (variant == DELTA_RIGHT || variant == DELTA_BAD_TYPE) {
            col_rel_t *delta = col_rel_new_auto("$d$b", 2);
            CHECK(delta);
            if (variant == DELTA_BAD_TYPE) {
                const wirelog_column_type_t types[] = {WIRELOG_TYPE_FLOAT,
                                                       WIRELOG_TYPE_INT64};
                CHECK(!col_rel_set_column_types(delta, types, 2));
            }
            CHECK(!col_rel_append_row(delta, (int64_t[]){1, 0}));
            CHECK(!session_add_rel((wl_col_session_t *)sessions[i], delta));
        }
    }
    col_arrangement_pin_t pin = {0};
    wl_col_session_t *session = (wl_col_session_t *)owned;
    uint32_t key = 0;
    size_t cache_limit = session->arr_cache_limit_bytes;
    if (cache_mode >= GROW_PIN) {
        /* Eight distinct real registry keys fill its initial capacity. */
        const char *names[] = {"a", "p", "q", "r"};
        for (unsigned n = 1; n < 4; ++n)
            CHECK(!wl_session_insert(owned, names[n], input, 8, 2));
        for (unsigned n = 0; n < 4; ++n)
            for (key = 0; key < 2; ++key)
                CHECK(col_session_get_arrangement(owned, names[n], &key, 1));
        key = 0;
        CHECK(session->arr_count == 8 && session->arr_cap == 8);
        CHECK(!col_session_pin_arrangement(owned, "a", &key, 1, &pin));
        if (cache_mode == SPARE_PIN) {
            /* Temporarily release, grow by a real cache insertion, re-pin. */
            col_arrangement_pin_release(&pin);
            CHECK(!wl_session_insert(owned, "s", input, 8, 2));
            CHECK(col_session_get_arrangement(owned, "s", &key, 1));
            CHECK(!col_session_pin_arrangement(owned, "a", &key, 1, &pin));
            CHECK(session->arr_count < session->arr_cap);
        } else if (cache_mode == EVICT_PIN || cache_mode == TOMBSTONE_PIN) {
            /* This is an internal cache-policy fixture, not a public knob. */
            session->arr_cache_limit_bytes = 1;
            if (cache_mode == TOMBSTONE_PIN) {
                CHECK(!wl_session_insert(owned, "s", input, 8, 2));
                CHECK(col_session_get_arrangement(owned, "s", &key, 1));
                session->arr_cache_limit_bytes = cache_limit;
            }
        }
    } else if (cache_mode) {
        CHECK(col_session_get_arrangement(owned, right_name, &key, 1));
        if (cache_mode >= PINNED)
            CHECK(!col_session_pin_arrangement(owned, right_name, &key, 1,
                &pin));
        if (cache_mode == STALE_PIN) col_session_invalidate_arrangements(owned,
                "b");
    }
    prepared = successor = cache_steps = filter_rows = intrinsic_checks = 0;
    attempt = consumed = 0; stopped = false;
    stop_event = event; stop_at = hit;
    wl_columnar_join_pipeline_test_numeric_checkpoint = checkpoint;
    wl_columnar_join_test_attempt_checkpoint = join_checkpoint;
    int rc = finish(owned);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    int wanted = event ? WL_EVALUATION_CONTROL_CANCELLED : expected;
    if (rc != wanted) fprintf(stderr,
            "body=%s cache=%u bytes=%u rc=%d wanted=%d\n",
            body, cache_mode, bytes, rc, wanted);
    CHECK(rc == wanted && stopped == (event != 0));
    if (variant == RIGHT_FILTER || variant == FILTER_ERROR)
        CHECK(filter_rows == 8 && intrinsic_checks == 1);
    if (variant == FILTER_BAD_TYPE) CHECK(filter_rows == 8 &&
            !intrinsic_checks);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control,
        &report) && report.attempt_id == attempt
        && report.result == wanted && report.consumed &&
        !report.cleanup_status);
    if (!wanted) {
        CHECK(!finish(raw)); verify(owned, rows); verify(raw, rows);
        if (rows) CHECK(prepared == 1 && successor);
        if (empty_mask) CHECK(successor && prepared == (strstr(body,
                "x<y") ? 1u : 0u));
        if (empty_mask && !strstr(body, "x<y")) CHECK(!cache_steps);
        if (!cache_mode) CHECK(!session->arr_count);
    } else {
        CHECK(!prepared);
        const col_rel_t *out = session_find_rel(session, "output");
        CHECK(!out || !out->nrows);
        if (!event && !cache_mode) CHECK(finish(raw) == expected);
    }
    col_arrangement_pin_release(&pin);
    session->arr_cache_limit_bytes = cache_limit;
    if (event || cache_mode == STALE_PIN || cache_mode == GROW_PIN) {
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!finish(owned)); verify(owned, rows);
    }
    CHECK(!wl_session_try_destroy(&owned) && !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control);
    wl_plan_free(plan); wirelog_program_free(program);
}

int
main(void)
{
    static const char keyed[] = "a(k,x),b(k,y)";
    run(keyed, 64, 3, 0, 64, COLD, 0, 0);
    run(keyed, 16, 3, 0, 64, COLD, 0, 0);
    run(keyed, 15, 3, ENOTSUP, 0, COLD, 0, 0);
    run(keyed, 1, 0, 0, 0, COLD, 0, 0);
    run(keyed, 1, 1, 0, 0, COLD, 0, 0);
    run(keyed, 1, 2, 0, 0, COLD, 0, 0);
    run("a(ka,x),b(kb,y)", 64, 3, ENOTSUP, 0, COLD, 0, 0);
    run("a(ka,x),b(kb,y)", 64, 1, ENOTSUP, 0, COLD, 0, 0);
    run("a(k,x),b(k,y),x>y+100", 64, 3, ENOTSUP, 0, COLD, 0, 0);
    for (variant = RIGHT_FILTER; variant <= FILTER_BAD_TYPE; ++variant)
        run(keyed, 64, 3, variant >= BAD_TYPE ? EINVAL : ENOTSUP, 0, COLD, 0,
            0);
    variant = NORMAL;
    for (empty_mask = 1; empty_mask < 4; ++empty_mask) {
        run(keyed, 1, 3, 0, 0, COLD, 0, 0);
        run(keyed, 1, 3, 0, 0, STALE_PIN, 0, 0);
        run("a(k,x),b(k,y),x>y+100", 64, 3, ENOTSUP, 0, COLD, 0, 0);
        run("a(k,x),b(k,y),x<y", 64, 3, 0, 0, COLD, 0, 0);
        run("a(k,x),b(k,y),x<y", 64, 3, EBUSY, 0, STALE_PIN, 0, 0);
        run("a(k,x),b(k,y),x<y", 1, 3, ENOTSUP, 0, COLD, 0, 0);
    }
    empty_mask = 0;
    for (unsigned mode = WARM; mode <= TOMBSTONE_PIN; ++mode)
        run(keyed, 64, 3, mode == STALE_PIN || mode == GROW_PIN ? EBUSY : 0,
            64, mode, 0, 0);
    run(keyed, 64, 3, 0, 64, COLD,
        WL_COLUMNAR_JOIN_PIPELINE_TEST_STRICT_SUCCESSOR, 1);
    run(keyed, 64, 3, 0, 64, WARM,
        WL_COLUMNAR_JOIN_PIPELINE_TEST_STRICT_CACHE, 3);
    char long_name[201], long_body[256];
    memset(long_name, 'b', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = '\0';
    snprintf(long_body, sizeof(long_body), "a(k,x),%s(k,y)", long_name);
    right_name = long_name;
    run(long_body, 64, 3, 0, 64, WARM,
        WL_COLUMNAR_JOIN_PIPELINE_TEST_STRICT_CACHE, 32);
    right_name = "b";
    return 0;
}
