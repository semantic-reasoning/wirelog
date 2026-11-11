/*
 * Controlled sessions keep stepping (#2351).  Every STEP here goes through
 * evaluation control on a W=1 session with an enforcing governor.
 */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/evaluation_control.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

enum { MAX_NODE = 64, MAX_EDGES = 64, SLICE_CAP = 400000 };

static const char *closure_text =
    ".decl edge(x: int64, y: int64)\n.decl path(x: int64, y: int64)\n"
    "path(x,y) :- edge(x,y).\npath(x,z) :- path(x,y), edge(y,z).\n";

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_columnar_memory_governor_ref_t *governor_ref;
    wl_columnar_memory_governor_t *governor;
    wl_evaluation_control_t *control;
    wl_session_t *session;
    int64_t edges[MAX_EDGES][2];
    uint32_t edge_count;
    wl_columnar_memory_reservation_t pressure;
    bool pressure_held;
} fixture_t;

static void
fixture_open(fixture_t *f, const char *text)
{
    memset(f, 0, sizeof(*f));
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program);
    CHECK(!wl_plan_from_program(f->program, &f->plan));
    uint64_t budget = UINT64_C(1) << 30;
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = budget, .usable_bytes = budget,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f->governor_ref = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor_ref);
    f->governor = wl_columnar_memory_governor_ref_get(f->governor_ref);
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000000), &f->control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = f->governor_ref;
    options.evaluation_control = f->control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->session));
}

static void
fixture_close(fixture_t *f)
{
    CHECK(!f->pressure_held);
    CHECK(!wl_session_try_destroy(&f->session));
    wl_evaluation_control_release(f->control);
    CHECK(!wl_intern_detach_memory_governor(f->plan->intern, f->governor_ref));
    CHECK(!wl_columnar_memory_reserved(f->governor));
    wl_columnar_memory_governor_ref_release(f->governor_ref);
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
}

static wl_col_session_t *
col(const fixture_t *f)
{
    return COL_SESSION(f->session);
}

/* Receipt slots that are not FREE, whatever state they are in. */
static unsigned
receipt_slots(const fixture_t *f)
{
    unsigned used = 0;
    for (unsigned i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; ++i)
        used += col(f)->cleanup_receipts[i].state
            != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE;
    return used;
}

static uint32_t
rows(const fixture_t *f, const char *name)
{
    const wl_col_session_t *sess = col(f);
    for (uint32_t i = 0; i < sess->nrels; ++i)
        if (sess->rels[i] && !strcmp(sess->rels[i]->name, name))
            return sess->rels[i]->nrows;
    return 0;
}

static int
insert_edge(fixture_t *f, int64_t x, int64_t y)
{
    int64_t row[2] = { x, y };
    int rc = wl_session_insert(f->session, "edge", row, 1, 2);
    if (!rc) {
        CHECK(f->edge_count < MAX_EDGES);
        f->edges[f->edge_count][0] = x;
        f->edges[f->edge_count][1] = y;
        f->edge_count++;
    }
    return rc;
}

/* Size of the transitive closure of the inserted edges, computed directly. */
static uint32_t
closure_size(const fixture_t *f)
{
    static bool reach[MAX_NODE][MAX_NODE];
    memset(reach, 0, sizeof(reach));
    for (uint32_t i = 0; i < f->edge_count; ++i) {
        CHECK(f->edges[i][0] >= 0 && f->edges[i][0] < MAX_NODE);
        CHECK(f->edges[i][1] >= 0 && f->edges[i][1] < MAX_NODE);
        reach[f->edges[i][0]][f->edges[i][1]] = true;
    }
    for (int k = 0; k < MAX_NODE; ++k)
        for (int i = 0; i < MAX_NODE; ++i)
            if (reach[i][k])
                for (int j = 0; j < MAX_NODE; ++j)
                    if (reach[k][j]) reach[i][j] = true;
    uint32_t count = 0;
    for (int i = 0; i < MAX_NODE; ++i)
        for (int j = 0; j < MAX_NODE; ++j)
            count += reach[i][j];
    return count;
}

/* Hold every unreserved governor byte, so the next admission is denied. */
static void
pressure_hold(fixture_t *f)
{
    CHECK(!f->pressure_held);
    uint64_t reserved = wl_columnar_memory_reserved(f->governor);
    wl_columnar_memory_reservation_init(&f->pressure);
    CHECK(wl_columnar_memory_reserve_checked(f->governor,
        f->governor->budget_bytes - reserved, &f->pressure)
        == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    f->pressure_held = true;
}

static void
pressure_release(fixture_t *f)
{
    if (!f->pressure_held) return;
    CHECK(wl_columnar_memory_rollback(&f->pressure));
    f->pressure_held = false;
}

typedef enum { ACT_NONE, ACT_PRESSURE, ACT_CANCEL } action_t;

/*
 * Drive one controlled STEP to a non-EAGAIN result.  The action is applied
 * before public call number `at` (0 is the first call), pressure is released
 * once the STEP returns, and the control is reset for the next STEP.
 */
static int
drive(fixture_t *f, action_t action, unsigned at, unsigned *calls)
{
    int rc;
    unsigned n = 0;
    do {
        if (n == at && action == ACT_PRESSURE) pressure_hold(f);
        if (n == at && action == ACT_CANCEL)
            wl_evaluation_control_request_cancel(f->control);
        rc = wl_session_step(f->session);
        ++n;
    } while (rc == EAGAIN && n < SLICE_CAP);
    CHECK(rc != EAGAIN);
    pressure_release(f);
    CHECK(!wl_evaluation_control_reset(f->control));
    if (calls) *calls = n;
    return rc;
}

static void
open_chain(fixture_t *f, int64_t length)
{
    fixture_open(f, closure_text);
    for (int64_t i = 1; i <= length; ++i)
        CHECK(!insert_edge(f, i, i + 1));
}

/* C1: a full receipt table is reported as ENOBUFS, not as a missing receipt. */
static void
test_receipt_table_full(void)
{
    fixture_t f;
    open_chain(&f, 3);
    wl_columnar_eval_cleanup_receipt_key_t keys[
        WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES];
    for (unsigned i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; ++i)
        CHECK(!wl_columnar_eval_stack_cleanup_receipt_reserve(col(&f), 777,
            888, &keys[i]));
    CHECK(receipt_slots(&f) == WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES);
    CHECK(drive(&f, ACT_NONE, 0, NULL) == ENOBUFS);
    for (unsigned i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; ++i)
        CHECK(!wl_columnar_eval_stack_cleanup_receipt_release(col(&f),
            &keys[i]));
    CHECK(drive(&f, ACT_NONE, 0, NULL) == 0);
    CHECK(rows(&f, "path") == closure_size(&f));
    fixture_close(&f);
}

/* A program of `rules` independent non-recursive rules over `edge`. */
static char *
copy_rules_text(unsigned rules)
{
    size_t cap = 64 + (size_t)rules * 96;
    char *text = malloc(cap);
    CHECK(text);
    int n = snprintf(text, cap, ".decl edge(x: int64, y: int64)\n");
    CHECK(n >= 0 && (size_t)n < cap);
    size_t used = (size_t)n;
    for (unsigned i = 0; i < rules; ++i) {
        n = snprintf(text + used, cap - used,
                ".decl b%u(x: int64, y: int64)\nb%u(x,y) :- edge(x,y).\n",
                i, i);
        CHECK(n >= 0 && (size_t)n < cap - used);
        used += (size_t)n;
    }
    return text;
}

static unsigned ack_refusals;

static uint64_t
refuse_receipt_ack(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t proposed)
{
    (void)session;
    if (action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK && ack_refusals) {
        ack_refusals--;
        return 1;
    }
    return proposed;
}

/*
 * C2: the plain controlled driver acknowledges each relation's terminal
 * receipt, so the receipt table does not fill across relations or STEPs.
 */
static void
test_plain_receipts(void)
{
    static const unsigned rule_counts[] = { 1, 32, 33 };
    for (unsigned c = 0; c < sizeof(rule_counts) / sizeof(*rule_counts);
        ++c) {
        char *text = copy_rules_text(rule_counts[c]);
        fixture_t f;
        fixture_open(&f, text);
        for (int64_t i = 1; i <= 3; ++i)
            CHECK(!insert_edge(&f, i, i + 1));
        CHECK(drive(&f, ACT_NONE, 0, NULL) == 0);
        CHECK(receipt_slots(&f) == 0);
        for (int64_t k = 0; k < 5; ++k) {
            CHECK(!insert_edge(&f, 10 + k, 11 + k));
            CHECK(drive(&f, ACT_NONE, 0, NULL) == 0);
            CHECK(receipt_slots(&f) == 0);
        }
        CHECK(rows(&f, "b0") == f.edge_count);
        fixture_close(&f);
        free(text);
    }
}

/* C2: a refused acknowledgement grant is retried under the same attempt. */
static void
test_plain_ack_refused(void)
{
    for (unsigned refusals = 1; refusals <= 2; ++refusals) {
        char *text = copy_rules_text(1);
        fixture_t f;
        fixture_open(&f, text);
        for (int64_t i = 1; i <= 3; ++i)
            CHECK(!insert_edge(&f, i, i + 1));
        ack_refusals = refusals;
        wl_columnar_session_test_framed_grant_limit = refuse_receipt_ack;
        int rc = drive(&f, ACT_NONE, 0, NULL);
        wl_columnar_session_test_framed_grant_limit = NULL;
        CHECK(rc == 0 && !ack_refusals && receipt_slots(&f) == 0);
        CHECK(!insert_edge(&f, 10, 11));
        CHECK(drive(&f, ACT_NONE, 0, NULL) == 0 && receipt_slots(&f) == 0);
        CHECK(rows(&f, "b0") == f.edge_count);
        fixture_close(&f);
        free(text);
    }
}

/*
 * C2b: a later non-recursive STEP runs through the observed child, which
 * also retries a refused acknowledgement instead of reporting it.
 */
static void
test_observed_ack_refused(void)
{
    for (unsigned refusals = 1; refusals <= 2; ++refusals) {
        char *text = copy_rules_text(1);
        fixture_t f;
        fixture_open(&f, text);
        for (int64_t i = 1; i <= 3; ++i)
            CHECK(!insert_edge(&f, i, i + 1));
        CHECK(drive(&f, ACT_NONE, 0, NULL) == 0);
        CHECK(!insert_edge(&f, 10, 11));
        ack_refusals = refusals;
        wl_columnar_session_test_framed_grant_limit = refuse_receipt_ack;
        int rc = drive(&f, ACT_NONE, 0, NULL);
        wl_columnar_session_test_framed_grant_limit = NULL;
        CHECK(rc == 0 && !ack_refusals && receipt_slots(&f) == 0);
        CHECK(!insert_edge(&f, 20, 21));
        CHECK(drive(&f, ACT_NONE, 0, NULL) == 0 && receipt_slots(&f) == 0);
        CHECK(rows(&f, "b0") == f.edge_count);
        fixture_close(&f);
        free(text);
    }
}

static fixture_t *hook_fixture;
static unsigned hook_point;
static uint32_t hook_iteration;

static void
pressure_at_checkpoint(wl_evaluation_control_t *control, unsigned point,
    uint32_t iteration)
{
    (void)control;
    if (point == hook_point && iteration == hook_iteration
        && !hook_fixture->pressure_held)
        pressure_hold(hook_fixture);
}

/*
 * C1: a governed refusal of a later body's bootstrap is reported as the
 * denial.  Pressure installed after a merge refuses the next body's frame.
 */
static void
test_bootstrap_refusal(void)
{
    for (uint32_t iteration = 1; iteration <= 2; ++iteration) {
        fixture_t f;
        open_chain(&f, 5);
        CHECK(drive(&f, ACT_NONE, 0, NULL) == 0);
        CHECK(!insert_edge(&f, 6, 7));
        hook_fixture = &f;
        hook_point = WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_MERGE;
        hook_iteration = iteration;
        wl_columnar_session_test_recursive_step_checkpoint =
            pressure_at_checkpoint;
        int rc = drive(&f, ACT_NONE, 0, NULL);
        wl_columnar_session_test_recursive_step_checkpoint = NULL;
        CHECK(rc == ENOSPC);
        fixture_close(&f);
    }
}

int
main(void)
{
    test_receipt_table_full();
    test_bootstrap_refusal();
    test_plain_receipts();
    test_plain_ack_refused();
    test_observed_ack_refused();
    puts("controlled recovery passed");
    return 0;
}
