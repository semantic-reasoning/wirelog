/* Real generated recursive frontier: finite parent END and signed future input. */
#include "wirelog/columnar/internal.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/session.h"
#include "wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(e) do { if (!(e)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #e); abort(); \
                      } } while (0)

typedef struct { int64_t x, y; } pair_t;
typedef struct { int diff[2][6][6]; unsigned calls[2][6][6]; } events_t;
static const pair_t seed[] = {{1, 2}, {2, 3}, {3, 4}, {1, 3}, {2, 4}, {1, 4}};
static const pair_t inserted[] = {{1, 2}, {2, 3}, {3, 4}, {1, 3}, {2, 4},
                                  {1, 4},
                                  {4, 5}, {3, 5}, {2, 5}, {1, 5}};
static const pair_t removed[] = {{1, 2}, {3, 4}, {4, 5}, {3, 5}};
static const pair_t edge0[] = {{1, 2}, {2, 3}, {3, 4}};
static const pair_t edge1[] = {{1, 2}, {2, 3}, {3, 4}, {4, 5}};
static const pair_t edge2[] = {{1, 2}, {3, 4}, {4, 5}};
static wl_session_t *controlled;
static wl_evaluation_control_t *original;
static unsigned mode, end_actions;
static bool armed, fired;
static uint64_t successor;
static wl_evaluation_control_report_t frozen;

static bool
ended_mode(void)
{
    return mode == 3 || mode == 4 || mode == 7 || mode == 8;
}

static int
stop_result(void)
{
    return mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED
        : mode == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : mode == 7 ? EINVAL : mode == 8 ? ENOMEM : 7;
}

static void
build_checkpoint(wl_col_session_t *session, bool after, int result)
{
    if (&session->base != controlled || !armed || fired || mode != 8)return;
    if (!after) {
        CHECK(!wl_columnar_relation_test_constructor_fail_allocation);
        wl_columnar_relation_test_constructor_fail_allocation = 1;
    } else {
        CHECK(result == ENOMEM &&
            !wl_columnar_relation_test_constructor_fail_allocation);
        fired = true;
        CHECK(wl_evaluation_control_finish(original,
            controlled->operation_admission,
            7, 0) == 7);
        CHECK(!wl_evaluation_control_report(original, &frozen));
    }
}

static bool
contains(const pair_t *rows, size_t count, unsigned x, unsigned y)
{
    for (size_t i = 0; i < count; ++i)
        if (rows[i].x == (int64_t)x && rows[i].y == (int64_t)y)return true;
    return false;
}

static void
capture(const char *name, const int64_t *row, uint32_t width, int32_t sign,
    void *payload)
{
    events_t *events = payload;
    unsigned relation = !strcmp(name, "path") ? 0 : 1;
    CHECK(!strcmp(name, relation ? "plain" : "path") && width == 2
        && (sign == 1 || sign == -1) && row[0] > 0 && row[0] < 6
        && row[1] > 0 && row[1] < 6);
    CHECK(!events->calls[relation][row[0]][row[1]]++);
    events->diff[relation][row[0]][row[1]] += sign;
}

static int
finish(wl_session_t *session)
{
    for (unsigned call = 0; call < 1000000; ++call) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN)return rc;
    }
    CHECK(false); return EIO;
}

static void
content(wl_session_t *session, const char *name, const pair_t *rows,
    size_t count)
{
    const col_rel_t *r = session_find_rel(COL_SESSION(session), name);
    CHECK(r && r->ncols == 2 && r->nrows == count);
    bool seen[6][6] = {{false}};
    for (uint32_t i = 0; i < r->nrows; ++i) {
        int64_t x = col_rel_get(r, i, 0), y = col_rel_get(r, i, 1);
        CHECK(x > 0 && x < 6 && y > 0 && y < 6 && !seen[x][y]
            && contains(rows, count, (unsigned)x, (unsigned)y));
        seen[x][y] = true;
    }
}

static void
events_match(const events_t *events, unsigned relation, const pair_t *before,
    size_t old_count, const pair_t *now, size_t count)
{
    for (unsigned x = 1; x < 6; ++x)for (unsigned y = 1; y < 6; ++y) {
            int diff = (int)contains(now, count, x, y)-(int)contains(before,
                    old_count, x, y);
            CHECK(events->diff[relation][x][y] == diff
                && events->calls[relation][x][y] == (diff ? 1u : 0u));
        }
}

static void
end_checkpoint(const wl_columnar_relation_scalar_source_t *source,
    wl_evaluation_control_t *funding, uint64_t funding_id, unsigned after)
{
    if (!wl_columnar_session_test_frontier_source_current(COL_SESSION(
            controlled),
        source)) return;
    CHECK(funding != original && funding_id);
    CHECK(source->end_attempt_id ==
        wl_evaluation_control_active_attempt_id(original)
        || (fired && ended_mode()));
    ++end_actions;
    if (!armed || fired || after != (mode == 6 ? 1u : 0u))return;
    fired = true;
    wl_session_t *slot = controlled;
    CHECK(wl_session_try_destroy(&slot) == EBUSY && slot == controlled);
    CHECK(controlled->backend->session_step(controlled) == EBUSY);
    if (mode == 1)wl_evaluation_control_request_cancel(original);
    else if (mode == 2) {
        CHECK(wl_evaluation_control_charge_attempt(original,
            source->end_attempt_id,
            UINT64_C(100000001)) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    } else if (mode == 3 || mode == 4 || mode == 7) {
        int result = mode == 7 ? 0 : 7;
        CHECK(wl_evaluation_control_finish(original,
            controlled->operation_admission,
            result, 0) == result);
        CHECK(!wl_evaluation_control_report(original, &frozen));
        if (mode == 4) {
            CHECK(!wl_evaluation_control_begin(original,
                controlled->operation_admission));
            successor = wl_evaluation_control_active_attempt_id(original);
            CHECK(successor && successor != frozen.attempt_id);
        }
    } else wl_evaluation_control_request_cancel(funding);
}

int
main(int argc, char **argv)
{
    CHECK(argc == 2);
    const char *modes[] = {"plain", "cancel", "work", "ended", "successor",
                           "funding-before", "funding-after", "ended-zero",
                           "error-ended"};
    for (; mode < 9 && strcmp(argv[1], modes[mode]); ++mode) {
    }
    CHECK(mode < 9);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
        ".decl plain(x:int64,y:int64)\nplain(x,y):-edge(x,y).\n"
        "path(x,y):-edge(x,y).\npath(x,z):-path(x,y),edge(y,z).\n", &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan)
        && !wl_evaluation_control_create(UINT64_C(100000000), &original));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(128)<<20, .usable_bytes = UINT64_C(128)<<20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = original; options.memory_governor = governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options,
        &controlled));
    options.evaluation_control = NULL;
    wl_session_t *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &raw));
    events_t actual = {0}, expected = {0};
    wl_session_set_delta_cb(controlled, capture, &actual);
    wl_session_set_delta_cb(raw, capture, &expected);
    const int64_t input[] = {1, 2, 2, 3, 3, 4};
    CHECK(!wl_session_insert(controlled, "edge", input, 3, 2)
        && !wl_session_insert(raw, "edge", input, 3, 2));
    const pair_t *path[] = {seed, inserted, removed},
        *edge[] = {edge0, edge1, edge2};
    const size_t path_count[] = {6, 10, 4}, edge_count[] = {3, 4, 3};
    for (unsigned stage = 0; stage < 3; ++stage) {
        CHECK(!finish(raw));
        armed = stage == 1 && mode; unsigned before = end_actions;
        wl_columnar_relation_test_scalar_end_checkpoint = end_checkpoint;
        wl_columnar_session_test_frontier_build_checkpoint = build_checkpoint;
        int rc = finish(controlled);
        if (armed && (mode < 5 || ended_mode())) {
            if (!fired || rc != stop_result())
                fprintf(stderr,
                    "frontier stop mode=%u fired=%u rc=%d actions=%u\n",
                    mode, fired, rc, end_actions-before);
            CHECK(fired && rc == stop_result());
            const events_t empty = {0};
            CHECK(!memcmp(&actual, &empty, sizeof(empty)));
            content(controlled, "path", path[stage-1], path_count[stage-1]);
            content(controlled, "plain", edge[stage-1], edge_count[stage-1]);
            if (ended_mode()) {
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(original, &report)
                    && report.attempt_id == frozen.attempt_id &&
                    report.limit == frozen.limit
                    && report.consumed == frozen.consumed &&
                    report.stop_reason == frozen.stop_reason
                    && report.execution_status == frozen.execution_status
                    && report.cleanup_status == frozen.cleanup_status &&
                    report.result == frozen.result);
                if (successor) {
                    uint64_t used = UINT64_MAX;
                    CHECK(!wl_evaluation_control_test_active_consumed(original,
                        successor,
                        &used) && !used);
                    CHECK(!wl_evaluation_control_finish(original,
                        controlled->operation_admission, 0, 0));
                }
            }
            CHECK(!wl_evaluation_control_reset(original));
            armed = false; rc = finish(controlled);
        }
        wl_columnar_relation_test_scalar_end_checkpoint = NULL;
        wl_columnar_session_test_frontier_build_checkpoint = NULL;
        if (rc) fprintf(stderr, "frontier mode=%u stage=%u rc=%d actions=%u\n",
                mode, stage, rc, end_actions-before);
        CHECK(!rc && end_actions > before && (!armed || fired));
        content(controlled, "path", path[stage], path_count[stage]);
        content(raw, "path", path[stage], path_count[stage]);
        content(controlled, "plain", edge[stage], edge_count[stage]);
        events_match(&actual, 0, stage ? path[stage-1] : NULL,
            stage ? path_count[stage-1] : 0, path[stage], path_count[stage]);
        events_match(&actual, 1, stage ? edge[stage-1] : NULL,
            stage ? edge_count[stage-1] : 0, edge[stage], edge_count[stage]);
        CHECK(!memcmp(&actual, &expected, sizeof(actual)));
        memset(&actual, 0, sizeof(actual));
        memset(&expected, 0, sizeof(expected));
        CHECK(!finish(controlled) && !memcmp(&actual, &(events_t){0},
            sizeof(actual)));
        if (!stage) CHECK(!wl_session_insert(controlled, "edge",
                (int64_t[]){4, 5}, 1, 2)
                && !wl_session_insert(raw, "edge", (int64_t[]){4, 5}, 1, 2));
        if (stage == 1)CHECK(!wl_session_remove(controlled, "edge",
                (int64_t[]){2, 3}, 1, 2)
                && !wl_session_remove(raw, "edge", (int64_t[]){2, 3}, 1, 2));
    }
    CHECK(!wl_session_try_destroy(&controlled) &&
        !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(original); wl_plan_free(plan);
    wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
    puts("frontier parent: literal 6/10/4 and real finite END passed");
    return 0;
}
