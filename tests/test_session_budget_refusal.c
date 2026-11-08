/*
 * A real budget refusal, through a public shared governor and with no test hook
 * in the failure path (#2348).  A second session holds the budget down until the
 * refusal, then goes away, which is the way this test gets memory back.
 */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/intern.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/wirelog-internal.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

enum {
    CHAIN_EDGES = 20,
    BALLAST_ROWS = 8192,
    SLACK_LIMIT = 61440,
    SLACK_STEP = 4096
};

/* What fails, and what has to hold afterwards. */
typedef enum {
    /* A first plain step is refused; an edge is removed before the retry. */
    SCENARIO_PLAIN,
    /* A committed session observed by a callback since its creation has
     * its next step refused.  The retry's events complete the stream
     * exactly. */
    SCENARIO_OBSERVED_STEP,
    /* A committed session with a callback has an evaluating snapshot refused.
     * The step that finishes it reports nothing, and the following step's
     * events are exact. */
    SCENARIO_SNAPSHOT,
    SCENARIO_COUNT
} scenario_t;

static const char *const scenario_names[SCENARIO_COUNT] = {
    "plain step", "observed step", "snapshot with a callback"
};

#define DECLS ".decl e(x: int64, y: int64)\n.decl p(x: int64, y: int64)\n" \
        "p(x,y) :- e(x,y).\n"
static const char *const programs[] = {
    DECLS "p(x,w) :- p(x,y), p(y,w).\n",
    DECLS "p(x,w) :- p(x,y), e(y,w).\n",
    /* Rows in the head that come from the program rather than from e. */
    DECLS "p(x,w) :- p(x,y), p(y,w).\np(30,31).\np(1,2).\n"
};
static const char ballast_text[] =
    ".decl bx(x: int64, y: int64)\n.decl by(x: int64, y: int64)\n"
    "by(x,y) :- bx(x,y).\n";

/* A row multiset of p as a count and a commutative hash, so rows may arrive in
 * any order and an event stream can be summed into one. */
typedef struct {
    int64_t count;
    uint64_t row_hash;
} model_t;

static uint64_t
row_hash(const int64_t *row)
{
    return ((uint64_t)row[0] * UINT64_C(0x9E3779B97F4A7C15))
           ^ ((uint64_t)row[1] * UINT64_C(0xC2B2AE3D27D4EB4F));
}

static void
collect_model(const char *relation, const int64_t *row, uint32_t ncols,
    void *user)
{
    model_t *model = user;
    if (strcmp(relation, "p") != 0)
        return;
    CHECK(ncols == 2);
    model->count++;
    model->row_hash += row_hash(row);
}

typedef struct {
    model_t net;
    uint64_t events;
} events_t;

static void
collect_event(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user)
{
    events_t *events = user;
    if (strcmp(relation, "p") != 0)
        return;
    CHECK(ncols == 2 && (diff == 1 || diff == -1));
    events->events++;
    events->net.count += diff;
    events->net.row_hash += diff > 0 ? row_hash(row) : 0u - row_hash(row);
}

static bool
models_equal(model_t a, model_t b)
{
    return a.count == b.count && a.row_hash == b.row_hash;
}

/* The program owns the intern table the plan refers to. */
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
} compiled_t;

static compiled_t
compile(const char *text)
{
    wirelog_error_t error;
    compiled_t compiled = { wirelog_parse_string(text, &error), NULL };
    CHECK(compiled.program
        && !wl_plan_from_program(compiled.program, &compiled.plan));
    return compiled;
}

static void
compiled_free(compiled_t compiled)
{
    wl_plan_free(compiled.plan);
    wirelog_program_free(compiled.program);
}

static void
insert_edges(wl_session_t *session, int64_t first, int64_t last)
{
    for (int64_t i = first; i <= last; i++) {
        const int64_t row[2] = { i, i + 1 };
        CHECK(!wl_session_insert(session, "e", row, 1, 2));
    }
}

static void
remove_edge(wl_session_t *session)
{
    static const int64_t edge[2] = { 10, 11 };
    CHECK(!wl_session_remove(session, "e", edge, 1, 2));
}

/* The model of a fresh session given edges [1, last], less e(10,11) when
 * asked. */
static model_t
reference_model(const compiled_t *compiled, uint32_t workers, int64_t last,
    bool removed)
{
    wl_session_t *session = NULL;
    model_t model = { 0 };
    CHECK(!wl_session_create(wl_backend_columnar(), compiled->plan, workers,
        &session));
    CHECK(!wl_session_load_facts(session, compiled->program));
    insert_edges(session, 1, last);
    if (removed)
        remove_edge(session);
    CHECK(!wl_session_snapshot(session, collect_model, &model));
    wl_session_destroy(session);
    return model;
}

typedef struct {
    model_t plain;      /* edges [1, CHAIN_EDGES] less e(10,11) */
    model_t extended;   /* edges [1, CHAIN_EDGES + 1] */
    model_t followup;   /* edges [1, CHAIN_EDGES + 2] */
} references_t;

typedef struct {
    uint64_t reserved_before_refusal;
    bool refused;
} attempt_t;

/* The governed call this scenario expects to be refused. */
static int
refused_call(wl_session_t *session, scenario_t scenario, model_t *unused)
{
    return scenario == SCENARIO_SNAPSHOT
               ? wl_session_snapshot(session, collect_model, unused)
               : wl_session_step(session);
}

/* With calibrate set, only measure what is reserved before the refused call. */
static attempt_t
attempt(const compiled_t *compiled, scenario_t scenario, uint32_t workers,
    uint64_t budget, bool calibrate, const references_t *expected)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = budget, .usable_bytes = budget,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    attempt_t result = { 0 };
    wl_columnar_memory_governor_ref_t *governor
        = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    compiled_t ballast_compiled = compile(ballast_text);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = governor;
    wl_session_t *session = NULL, *ballast = NULL;
    events_t events = { 0 };
    CHECK(!wl_session_create_with_options(wl_backend_columnar(),
        compiled->plan, workers, &options, &session));
    CHECK(!wl_session_load_facts(session, compiled->program));
    if (scenario != SCENARIO_PLAIN)
        wl_session_set_delta_cb(session, collect_event, &events);
    insert_edges(session, 1, CHAIN_EDGES);
    if (scenario != SCENARIO_PLAIN) {
        /* The commit runs before the ballast takes the budget. */
        if (wl_session_step(session) != 0)
            goto cleanup;
        insert_edges(session, CHAIN_EDGES + 1, CHAIN_EDGES + 1);
    }
    CHECK(!wl_session_create_with_options(wl_backend_columnar(),
        ballast_compiled.plan, 1, &options, &ballast));
    int64_t *rows = malloc(sizeof(*rows) * 2u * BALLAST_ROWS);
    CHECK(rows);
    for (int64_t i = 0; i < (int64_t)2 * BALLAST_ROWS; i++)
        rows[i] = i;
    CHECK(!wl_session_insert(ballast, "bx", rows, BALLAST_ROWS, 2));
    free(rows);
    result.reserved_before_refusal = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(governor));
    if (calibrate)
        goto cleanup;
    model_t ignored = { 0 };
    const events_t before_refusal = events;
    int rc = refused_call(session, scenario, &ignored);
    /* This scan injects no allocation failure, so a refusal here is the
     * budget, and a denial on any thread reports on this call as ENOSPC. */
    CHECK(rc != ENOMEM);
    if (rc == ENOSPC) {
        CHECK(wl_session_budget_denied(session));
        CHECK(wl_facade_session_error_code_with_budget(rc, 0, true)
            == WIRELOG_ERR_MEMORY_BUDGET);
    } else {
        CHECK(rc != 0 || !wl_session_budget_denied(session));
    }
    if (rc == 0)
        goto cleanup;
    result.refused = true;
    CHECK(!wl_session_try_destroy(&ballast));
    if (scenario == SCENARIO_PLAIN)
        remove_edge(session);
    const events_t before_retry = events;
    CHECK(!wl_session_step(session));
    model_t model = { 0 };
    CHECK(!wl_session_snapshot(session, collect_model, &model));
    switch (scenario) {
    case SCENARIO_PLAIN:
        CHECK(models_equal(model, expected->plain));
        break;
    case SCENARIO_OBSERVED_STEP:
        /* The callback saw every step, the refused one included. */
        CHECK(models_equal(model, expected->extended));
        CHECK(models_equal(events.net, expected->extended));
        break;
    case SCENARIO_SNAPSHOT: {
        CHECK(models_equal(model, expected->extended));
        CHECK(before_refusal.events == events.events);
        CHECK(before_retry.events == events.events);
        /* The snapshot is the baseline the next step's events start from. */
        events = (events_t){ 0 };
        insert_edges(session, CHAIN_EDGES + 2, CHAIN_EDGES + 2);
        CHECK(!wl_session_step(session));
        CHECK(events.net.count
            == expected->followup.count - model.count);
        CHECK(events.net.row_hash
            == expected->followup.row_hash - model.row_hash);
        break;
    }
    default:
        CHECK(false);
    }
cleanup:
    if (ballast)
        CHECK(!wl_session_try_destroy(&ballast));
    CHECK(!wl_session_try_destroy(&session));
    CHECK(!wl_intern_detach_memory_governor(compiled->plan->intern, governor));
    CHECK(!wl_intern_detach_memory_governor(ballast_compiled.plan->intern,
        governor));
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor)));
    compiled_free(ballast_compiled);
    wl_columnar_memory_governor_ref_release(governor);
    return result;
}

int
main(void)
{
#ifdef _WIN32
    (void)_putenv_s("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1");
#else
    (void)setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
#endif
    static const uint32_t widths[] = { 1, 2, 8 };
    for (size_t p = 0; p < sizeof(programs) / sizeof(programs[0]); p++) {
        compiled_t compiled = compile(programs[p]);
        for (size_t w = 0; w < sizeof(widths) / sizeof(widths[0]); w++) {
            const uint32_t workers = widths[w];
            const references_t expected = {
                reference_model(&compiled, workers, CHAIN_EDGES, true),
                reference_model(&compiled, workers, CHAIN_EDGES + 1, false),
                reference_model(&compiled, workers, CHAIN_EDGES + 2, false)
            };
            for (int s = 0; s < SCENARIO_COUNT; s++) {
                const scenario_t scenario = (scenario_t)s;
                const uint64_t reserved = attempt(&compiled, scenario, workers,
                        UINT64_C(1) << 32, true, NULL).reserved_before_refusal;
                unsigned refusals = 0;
                for (uint64_t slack = 0; slack <= SLACK_LIMIT;
                    slack += SLACK_STEP)
                    refusals += attempt(&compiled, scenario, workers,
                            reserved + slack, false, &expected).refused;
                printf("program %zu W=%u %s: %u refusals\n", p, workers,
                    scenario_names[s], refusals);
                /* The sizes are not part of any contract, only that this
                 * range reaches the governor at all. */
                CHECK(refusals > 0);
            }
        }
        compiled_free(compiled);
    }
    return 0;
}
