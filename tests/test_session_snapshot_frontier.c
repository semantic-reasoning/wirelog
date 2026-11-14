/* Reuse only the genuine fixture/literal helpers; its ordinary STEP main is
 * renamed and never invoked. This file drives the synchronous Snapshot owner. */
#define main wl_snapshot_frontier_unused_ordinary_main
#include "test_session_frontier_parent.c"
#undef main

static unsigned snapshot_mode, snapshot_boundary, snapshot_builds;
static unsigned snapshot_ends, snapshot_drained, snapshot_rows;
static bool snapshot_armed, snapshot_fired, snapshot_source_live;
static bool snapshot_failed_build;
static unsigned snapshot_seen[2][6][6];
static const pair_t *snapshot_expected_path;
static size_t snapshot_expected_count;
static events_t *snapshot_events;

static int
snapshot_error(void)
{
    return snapshot_mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED
        : snapshot_mode == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : snapshot_mode == 4 ? EINVAL : snapshot_mode == 6 ? ENOMEM : 7;
}

static void
snapshot_capture(const char *name, const int64_t *row, uint32_t width,
    void *payload)
{
    (void)payload;
    if (strcmp(name, "path") && strcmp(name, "plain")) return;
    CHECK(!snapshot_source_live && snapshot_drained && width == 2
        && row[0] > 0 && row[0] < 6 && row[1] > 0 && row[1] < 6);
    unsigned which = !strcmp(name, "plain");
    CHECK(!snapshot_seen[which][row[0]][row[1]]++);
    CHECK(contains(which ? edge2 : snapshot_expected_path,
        which ? 3 : snapshot_expected_count, row[0], row[1]));
    ++snapshot_rows;
}

static void
snapshot_build(wl_col_session_t *session, bool after, int result)
{
    if (&session->base != controlled || !snapshot_armed) return;
    if (!after) {
        ++snapshot_builds; snapshot_source_live = true;
        CHECK(!memcmp(snapshot_events, &(events_t){0}, sizeof(events_t)));
        content(controlled, "path", inserted, 10);
        if (snapshot_mode == 6 && !snapshot_fired) {
            CHECK(!wl_columnar_relation_test_constructor_fail_allocation);
            wl_columnar_relation_test_constructor_fail_allocation = 1;
        }
    } else if (snapshot_mode == 6 && !snapshot_fired) {
        CHECK(result == ENOMEM);
        snapshot_failed_build = snapshot_fired = true;
        CHECK(wl_evaluation_control_finish(original,
            controlled->operation_admission, 7, 0) == 7);
        CHECK(!wl_evaluation_control_report(original, &frozen));
    } else CHECK(!result);
}

static void
snapshot_end(const wl_columnar_relation_scalar_source_t *source,
    wl_evaluation_control_t *funding, uint64_t id, unsigned after)
{
    if (!snapshot_armed || !wl_columnar_session_test_frontier_source_current(
            COL_SESSION(controlled), source)) return;
    CHECK(funding != original && id);
    ++snapshot_ends;
    if (after && source->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED) {
        ++snapshot_drained; snapshot_source_live = false;
        CHECK(!source->control && !source->owner && !source->descriptor
            && !source->owner_reader.owner && !source->descriptor_reader.owner);
    }
    if (!snapshot_mode || snapshot_mode == 6 || snapshot_fired) return;
    /* After-action checkpoints observe the completed reader/header step. */
    unsigned cursor = snapshot_boundary == 0 ? (snapshot_mode == 8 ? 1 : 0)
        : snapshot_boundary == 1 ? (snapshot_mode == 8 ? 3 : 2) : 4;
    if (source->end_cursor != cursor
        || after != (snapshot_mode == 8 ? 1u : 0u)
        || (snapshot_mode == 8 && snapshot_boundary == 2
        && source->phase != WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED)) return;
    snapshot_fired = true;
    CHECK(!memcmp(snapshot_events, &(events_t){0}, sizeof(events_t)));
    content(controlled, "path", inserted, 10);
    wl_session_t *slot = controlled;
    CHECK(wl_session_try_destroy(&slot) == EBUSY && slot == controlled);
    CHECK(controlled->backend->session_step(controlled) == EBUSY);
    if (snapshot_mode == 1) wl_evaluation_control_request_cancel(original);
    else if (snapshot_mode == 2) {
        CHECK(wl_evaluation_control_charge_attempt(original,
            source->end_attempt_id, UINT64_C(100000001))
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    } else if (snapshot_mode >= 3 && snapshot_mode <= 5) {
        int result = snapshot_mode == 4 ? 0 : 7;
        CHECK(wl_evaluation_control_finish(original,
            controlled->operation_admission, result, 0) == result);
        CHECK(!wl_evaluation_control_report(original, &frozen));
        if (snapshot_mode == 5) {
            CHECK(!wl_evaluation_control_begin(original,
                controlled->operation_admission));
            successor = wl_evaluation_control_active_attempt_id(original);
            CHECK(successor && successor != frozen.attempt_id);
        }
    } else wl_evaluation_control_request_cancel(funding);
}

static void
snapshot_case(unsigned selected, unsigned boundary)
{
    snapshot_mode = selected; snapshot_boundary = boundary;
    snapshot_builds = snapshot_ends = snapshot_drained = snapshot_rows = 0;
    snapshot_fired = snapshot_source_live = snapshot_failed_build = false;
    snapshot_armed = false; successor = 0;
    memset(&frozen, 0, sizeof(frozen));
    memset(snapshot_seen, 0, sizeof(snapshot_seen));
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
        ".decl plain(x:int64,y:int64)\nplain(x,y):-edge(x,y).\n"
        "path(x,y):-edge(x,y).\npath(x,z):-path(x,y),edge(y,z).\n", &error);
    CHECK(program);
    wl_plan_t *plan = NULL; CHECK(!wl_plan_from_program(program, &plan));
    CHECK(!wl_evaluation_control_create(UINT64_C(100000000), &original));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(128)<<20, .usable_bytes = UINT64_C(128)<<20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution); CHECK(g);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = original; options.memory_governor = g;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &controlled));
    events_t actual = {0}; snapshot_events = &actual;
    wl_session_set_delta_cb(controlled, capture, &actual);
    CHECK(!wl_session_insert(controlled, "edge",
        (int64_t[]){1, 2, 2, 3, 3, 4, 4, 5}, 4, 2));
    CHECK(!finish(controlled)); content(controlled, "path", inserted, 10);
    memset(&actual, 0, sizeof(actual));
    CHECK(!wl_session_remove(controlled, "edge", (int64_t[]){2, 3}, 1, 2));
    snapshot_armed = true;
    wl_columnar_session_test_frontier_build_checkpoint = snapshot_build;
    wl_columnar_relation_test_scalar_end_checkpoint = snapshot_end;
    snapshot_expected_path = removed; snapshot_expected_count = 4;
    int rc = wl_session_snapshot(controlled, snapshot_capture, NULL);
    CHECK(snapshot_builds && snapshot_ends && snapshot_drained
        && !snapshot_source_live);
    bool stopped = selected >= 1 && selected <= 6;
    if (stopped) {
        if (rc != snapshot_error()) fprintf(stderr,
                "snapshot frontier mode=%u boundary=%u rc=%d expected=%d\n",
                selected, boundary, rc, snapshot_error());
        CHECK(snapshot_fired && rc == snapshot_error() && !snapshot_rows
            && !memcmp(&actual, &(events_t){0}, sizeof(actual)));
        content(controlled, "path", inserted, 10);
        if (selected >= 3) {
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(original, &report)
                && !memcmp(&report, &frozen, sizeof(report)));
            if (successor) {
                uint64_t used;
                CHECK(!wl_evaluation_control_test_active_consumed(original,
                    successor, &used) && !used);
                CHECK(!wl_evaluation_control_finish(original,
                    controlled->operation_admission, 0, 0));
            }
        }
        CHECK(selected != 6 || snapshot_failed_build);
        CHECK(!wl_evaluation_control_reset(original));
        snapshot_mode = 0; snapshot_fired = false;
        snapshot_rows = 0; memset(snapshot_seen, 0, sizeof(snapshot_seen));
        rc = wl_session_snapshot(controlled, snapshot_capture, NULL);
    }
    CHECK(!rc && snapshot_rows == 7 &&
        (!selected || stopped || snapshot_fired));
    content(controlled, "path", removed, 4);
    content(controlled, "plain", edge2, 3);
    events_match(&actual, 0, inserted, 10, removed, 4);
    events_match(&actual, 1, edge1, 4, edge2, 3);
    snapshot_armed = false;
    wl_columnar_session_test_frontier_build_checkpoint = NULL;
    wl_columnar_relation_test_scalar_end_checkpoint = NULL;
    memset(&actual, 0, sizeof(actual));
    CHECK(!finish(controlled) && !memcmp(&actual, &(events_t){0},
        sizeof(actual)));
    CHECK(!wl_session_insert(controlled, "edge", (int64_t[]){2, 3}, 1, 2)
        && !finish(controlled));
    content(controlled, "path", inserted, 10);
    events_match(&actual, 0, removed, 4, inserted, 10);
    events_match(&actual, 1, edge2, 3, edge1, 4);
    memset(&actual, 0, sizeof(actual));
    CHECK(!wl_session_remove(controlled, "edge", (int64_t[]){4, 5}, 1, 2)
        && !finish(controlled));
    content(controlled, "path", seed, 6);
    events_match(&actual, 0, inserted, 10, seed, 6);
    events_match(&actual, 1, edge1, 4, edge0, 3);
    CHECK(!wl_session_try_destroy(&controlled));
    wl_evaluation_control_release(original);
    wl_plan_free(plan); wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}

int main(void)
{
    unsigned cases = 0;
    for (unsigned selected = 0; selected < 9; ++selected)
        for (unsigned boundary = 0; boundary <
            (selected == 0 || selected == 6 ? 1u : 3u); ++boundary) {
            snapshot_case(selected, boundary); ++cases;
        }
    CHECK(cases == 23);
    puts("Snapshot frontier: 23 real ownership/stop/retry histories passed");
    return 0;
}
