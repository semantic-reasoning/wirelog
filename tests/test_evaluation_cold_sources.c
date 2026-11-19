/* #2301: cold (spill-backed) sources in controlled single-worker sessions.
 *
 * A controlled session (an evaluation control, an enforcing governor, one
 * worker) and an uncontrolled one are created with the same program, spill
 * store and JOIN caps, get the same rows, and seal the same input or load it
 * through begin/append/finish. The controlled STEP reads the cold binding
 * through the composed cold scan attempt; the test checks for every admitted
 * case that the attempt's checkpoint fired, that the unmetered raw cold scan
 * did not run, and that both sessions hold the same head image after a first
 * and a second STEP.
 *
 * The one admitted shape is VARIABLE over the cold input, a direct MAP/FILTER
 * prefix with at least one MAP, then the rule's final CONSOLIDATE; each case
 * asserts its plan's op order. The parsed corpus has one and two FILTERs
 * before the MAP, an empty input, an input every row of which is filtered
 * out, 70-column rows, with one and with two FILTERs, STRING lanes from
 * program facts, and a second stratum that reads the head. The planner puts
 * every FILTER before the MAP, so a FILTER after a permuting MAP comes from a
 * plan the test edits; its image is also compared with an uncontrolled
 * session that keeps the input resident.
 *
 * Stops: for every cold scan checkpoint k of a calibrated run, a
 * cancellation and a work-limit stop at k on a first STEP, on the seal and
 * on the loading route and for 70-column rows. Each lands at k, returns the
 * stop code with a report naming it, publishes no head, returns the governor
 * to its value before the STEP, and after a reset STEP returns 0 with the
 * uncontrolled image. Some of the cancelled sessions are destroyed without a
 * reset. The scan runs its rows within one public call; parked at its first
 * EAGAIN return after the last row, a session refuses wl_session_try_destroy,
 * and after a cancellation its STEP ends and it is destroyed with no
 * reservation left. A cancellation requested from inside a spill read, an
 * injected read fault and governor denials at the admission points of the
 * attempt are checked the same way, the denials with wl_session_budget_denied.
 * The consumed work of a full STEP is pinned for a fixed input on both routes,
 * and a limit one unit below it stops the STEP. Within the rows no action
 * evaluates more than 64 cells, and no resolution action charges more than
 * 257 units.
 *
 * Refusals: seal and begin return WL_COLUMNAR_SPILL_UNSUPPORTED under
 * control, and WL_COLUMNAR_SPILL_OK uncontrolled, for a cold input read as a
 * JOIN right input or by a self-JOIN, a VARIABLE/JOIN pair without a MAP, a
 * FILTER prefix before a JOIN, a COUNT, a two-EDB single-rule projection, a
 * head that is also an EDB, and a registry that a successor STEP would find
 * larger than three relations. A controlled SNAPSHOT returns ENOTSUP and a
 * STEP with a delta callback set returns ENOTSUP. A STEP after a callback was
 * set and cleared following a completed STEP completes without an attempt,
 * the inputs being frozen; a callback set while an attempt is parked is
 * ignored, as in a resident controlled session.
 *
 * Not checked here: more than one worker; an observed STEP or an observed
 * SNAPSHOT over a cold source, both refused; a JOIN or an aggregate over a
 * cold input, refused; the loading calls under the control, which neither
 * meters nor cancels them. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include "test_tmpdir.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s [%s]\n", \
                                      __FILE__, __LINE__, #x, where); \
                                  abort(); } } while (0)

static const char *where = "";

/* ---- spill fault hooks --------------------------------------------------- */

static unsigned read_fault;
static bool read_cancel;
static wl_evaluation_control_t *read_cancel_control;

bool
wl_columnar_spill_test_allocation(void)
{
    return false;
}

int
wl_columnar_spill_test_io(bool writing, uint64_t offset, size_t *length)
{
    (void)offset; (void)length;
    if (writing) return 0;
    if (read_cancel && read_cancel_control) {
        read_cancel = false;
        wl_evaluation_control_request_cancel(read_cancel_control);
    }
    if (read_fault && --read_fault == 0) return EIO;
    return 0;
}

/* ---- sessions ------------------------------------------------------------ */

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_columnar_memory_governor_ref_t *governor;
    wl_evaluation_control_t *control;
    wl_session_t *s;   /* controlled */
    wl_session_t *raw; /* uncontrolled */
    const wl_plan_op_t *saved_ops; /* The planner's ops of an edited rule. */
    uint32_t saved_count;
} fixture_t;

/* Set before fixture(): edit the first rule from VARIABLE, FILTER,
 * MAP[2,0], CONSOLIDATE to VARIABLE, MAP[1,2,0], FILTER, MAP[1,2],
 * CONSOLIDATE. The FILTER's column col2 then names the source's col0. */
static bool plan_edit;
static wl_plan_op_t edited_ops[5];
static uint32_t edit_permute[3] = {1, 2, 0}, edit_project[2] = {1, 2};

#define BUDGET (UINT64_C(4) * 1024 * 1024)

static const char *
spill_dir(void)
{
    const char *d = getenv("TMPDIR");
    return d && *d ? d : test_tmpdir();
}

static wl_session_t *
spill_session(fixture_t *f, wl_evaluation_control_t *control)
{
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    options.memory_governor = f->governor;
    options.spill_directory = spill_dir();
    options.spill_quota_bytes = UINT64_C(4) * 1024 * 1024;
    options.spill_max_row_bytes = 1024;
    options.spill_join_batch_bytes = 96;
    options.spill_join_index_bytes =
        sizeof(wl_columnar_memory_reservation_t) + 16;
    wl_session_t *s = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &s));
    CHECK(!wl_session_load_facts(s, f->program));
    return s;
}

static void
fixture(fixture_t *f, const char *text)
{
    memset(f, 0, sizeof(*f));
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = BUDGET, .usable_bytes = BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000), &f->control));
    if (plan_edit) {
        /* The plan is the test's own: edit it before any session reads it. */
        wl_plan_relation_t *rp =
            (wl_plan_relation_t *)&f->plan->strata[0].relations[0];
        CHECK(rp->op_count == 4 && rp->ops[1].op == WL_PLAN_OP_FILTER &&
            rp->ops[2].op == WL_PLAN_OP_MAP &&
            rp->ops[2].project_count == 2);
        f->saved_ops = rp->ops;
        f->saved_count = rp->op_count;
        edited_ops[0] = rp->ops[0];
        edited_ops[1] = (wl_plan_op_t){.op = WL_PLAN_OP_MAP,
                                       .project_count = 3,
                                       .project_indices = edit_permute};
        edited_ops[2] = rp->ops[1];
        edited_ops[3] = (wl_plan_op_t){.op = WL_PLAN_OP_MAP,
                                       .project_count = 2,
                                       .project_indices = edit_project};
        edited_ops[4] = rp->ops[3];
        rp->ops = edited_ops;
        rp->op_count = 5;
        plan_edit = false;
    }
    f->s = spill_session(f, f->control);
    f->raw = spill_session(f, NULL);
}

static uint64_t
reserved(const fixture_t *f)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f->governor));
}

static void
fixture_destroy(fixture_t *f)
{
    CHECK(!wl_session_try_destroy(&f->s));
    wl_session_destroy(f->raw);
    wl_evaluation_control_release(f->control);
    if (f->saved_ops) {
        wl_plan_relation_t *rp =
            (wl_plan_relation_t *)&f->plan->strata[0].relations[0];
        rp->ops = f->saved_ops;
        rp->op_count = f->saved_count;
    }
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    CHECK(reserved(f) == 0);
    wl_columnar_memory_governor_ref_release(f->governor);
}

/* ---- images -------------------------------------------------------------- */

#define MAX_ROWS 64
#define MAX_COLS 72

typedef struct {
    uint32_t count, width;
    int64_t rows[MAX_ROWS][MAX_COLS];
} image_t;

static int
row_cmp(const void *a, const void *b)
{
    return memcmp(a, b, MAX_COLS * sizeof(int64_t));
}

/* Absent and empty heads both read as zero rows. */
static void
image_of(wl_session_t *s, const char *head, image_t *m)
{
    memset(m, 0, sizeof(*m));
    const col_rel_t *r = session_find_rel((wl_col_session_t *)s, head);
    if (!r) return;
    CHECK(r->nrows <= MAX_ROWS && r->ncols <= MAX_COLS);
    m->count = r->nrows;
    m->width = r->nrows ? r->ncols : 0;
    for (uint32_t i = 0; i < r->nrows; i++)
        for (uint32_t c = 0; c < r->ncols; c++)
            m->rows[i][c] = col_rel_get(r, i, c);
    qsort(m->rows, m->count, sizeof(m->rows[0]), row_cmp);
}

static bool
image_equal(const image_t *a, const image_t *b)
{
    return a->count == b->count && a->width == b->width &&
           !memcmp(a->rows, b->rows, a->count * sizeof(a->rows[0]));
}

/* ---- hooks --------------------------------------------------------------- */

static uint64_t checkpoints;
static uint64_t stop_at; /* 1-based checkpoint ordinal; 0 disables. */
static int stop_mode;    /* 1 cancel, 2 work limit. */
static bool stop_hit;
static wl_columnar_cold_scan_attempt_phase_t last_phase;

static uint64_t cell_consumed, cell_run;

/* Within ROWS, the cell checkpoints of one action see the same consumed
 * work, its prepaid cells, and so does the next action's opening checkpoint:
 * a run never exceeds one action's cell bound plus that one. */
static void
cell_bound(wl_evaluation_control_t *control,
    const wl_columnar_cold_scan_attempt_t *a)
{
    uint64_t consumed = 0;
    if (a->phase != WL_COLUMNAR_COLD_SCAN_ATTEMPT_ROWS ||
        wl_evaluation_control_test_active_consumed(control, a->attempt_id,
        &consumed)) {
        cell_run = 0;
        return;
    }
    cell_run = consumed == cell_consumed ? cell_run + 1 : 1;
    cell_consumed = consumed;
    /* 64 is the documented bound, independent of the macro under test. */
    CHECK(WL_COLUMNAR_COLD_SCAN_ACTION_CELLS == 64u && cell_run <= 65u);
}

static uint64_t resolve_consumed;
static bool resolve_seen;

/* Each resolution action, a name byte run, a sizing step or a prepayment,
 * charges at most one quantum plus the sizing step's own unit: consecutive
 * RESOLVE checkpoints differ by no more than 257. */
static void
resolve_bound(wl_evaluation_control_t *control,
    const wl_columnar_cold_scan_attempt_t *a)
{
    uint64_t consumed = 0;
    if (a->phase != WL_COLUMNAR_COLD_SCAN_ATTEMPT_RESOLVE ||
        wl_evaluation_control_test_active_consumed(control, a->attempt_id,
        &consumed)) {
        resolve_seen = false;
        return;
    }
    if (resolve_seen) CHECK(consumed - resolve_consumed <= 257u);
    resolve_consumed = consumed;
    resolve_seen = true;
}

static void
cold_checkpoint(wl_evaluation_control_t *control,
    const wl_columnar_cold_scan_attempt_t *a)
{
    last_phase = a->phase;
    if (!stop_mode) {
        cell_bound(control, a);
        resolve_bound(control, a);
    }
    if (++checkpoints != stop_at || !stop_mode) return;
    stop_hit = true;
    if (stop_mode == 1) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge(control, UINT64_MAX) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
}

static void
hooks_clear(void)
{
    checkpoints = 0;
    stop_at = 0;
    stop_mode = 0;
    stop_hit = false;
    last_phase = WL_COLUMNAR_COLD_SCAN_ATTEMPT_INERT;
    resolve_seen = false;
}

/* The bound turns a livelock into a prompt CHECK failure. */
static int
until_done(wl_session_t *s)
{
    for (unsigned i = 0; i < 200000; i++) {
        int rc = wl_session_step(s);
        if (rc != EAGAIN)
            return rc;
    }
    CHECK(false);
    return EIO;
}

/* ---- inputs -------------------------------------------------------------- */

typedef enum { VIA_SEAL, VIA_LOAD } via_t;

typedef struct {
    const char *name;
    const char *text;
    const char *cold;     /* The input sealed or loaded. */
    uint32_t width;       /* Its declared width; 0 seals program facts. */
    uint32_t rows;        /* Generated rows. */
    const char *ops;      /* Op order of the cold rule: VFMC... */
    bool edit;            /* Move the FILTER after a permuting MAP. */
    const char *head;
    const char *head2; /* A second head to compare, or NULL. */
} qcase_t;

static int64_t
cell(uint32_t row, uint32_t col)
{
    return (int64_t)((row * 7u + col * 3u) % 13u);
}

static void
fill(fixture_t *f, const char *name, uint32_t width, uint32_t rows,
    bool both)
{
    int64_t row[MAX_COLS];
    for (uint32_t i = 0; i < rows; i++) {
        for (uint32_t c = 0; c < width; c++)
            row[c] = cell(i, c);
        CHECK(!wl_session_insert(f->s, name, row, 1, width));
        if (both) CHECK(!wl_session_insert(f->raw, name, row, 1, width));
    }
}

static wl_columnar_spill_status_t
load(wl_session_t *s, const char *name, uint32_t width, uint32_t rows)
{
    wl_columnar_spill_status_t st = wl_columnar_session_begin_cold_input(s,
            name);
    if (st != WL_COLUMNAR_SPILL_OK) return st;
    int64_t chunk[3 * MAX_COLS];
    for (uint32_t i = 0; i < rows; i += 3) {
        uint32_t n = rows - i < 3 ? rows - i : 3;
        for (uint32_t r = 0; r < n; r++)
            for (uint32_t c = 0; c < width; c++)
                chunk[r * width + c] = cell(i + r, c);
        st = wl_columnar_session_append_cold_input(s, name, chunk, n, width);
        if (st != WL_COLUMNAR_SPILL_OK) return st;
    }
    return wl_columnar_session_finish_cold_input(s, name);
}

/* Sealing clears the session's lazy name hash and the next STEP admits it
 * again. Rebuild it before a test takes its governor baseline, so that the
 * baseline holds the registry and a STEP adds only what it still owns. */
static void
registry_hash(fixture_t *f)
{
    (void)session_find_rel((wl_col_session_t *)f->s, "");
    (void)session_find_rel((wl_col_session_t *)f->raw, "");
}

/* Fill both sessions and seal or load the cold input in both. */
static void
prepare(fixture_t *f, const qcase_t *c, via_t via)
{
    if (via == VIA_SEAL) {
        fill(f, c->cold, c->width, c->rows, true);
        CHECK(wl_columnar_session_seal_join_input(f->raw, c->cold, NULL,
            NULL) == WL_COLUMNAR_SPILL_OK);
        CHECK(wl_columnar_session_seal_join_input(f->s, c->cold, NULL,
            NULL) == WL_COLUMNAR_SPILL_OK);
    } else {
        CHECK(load(f->raw, c->cold, c->width, c->rows) ==
            WL_COLUMNAR_SPILL_OK);
        CHECK(load(f->s, c->cold, c->width, c->rows) ==
            WL_COLUMNAR_SPILL_OK);
    }
    registry_hash(f);
}

static void
expect_heads(fixture_t *f, const qcase_t *c)
{
    image_t *a = malloc(sizeof(*a)), *b = malloc(sizeof(*b));
    CHECK(a && b);
    const char *heads[2] = {c->head, c->head2};
    for (int h = 0; h < 2; h++) {
        if (!heads[h]) continue;
        image_of(f->s, heads[h], a);
        image_of(f->raw, heads[h], b);
        if (!image_equal(a, b))
            fprintf(stderr, "%s: %s rows %u vs %u\n", where, heads[h],
                a->count, b->count);
        CHECK(image_equal(a, b));
    }
    free(a);
    free(b);
}

static uint32_t
head_rows(wl_session_t *s, const char *head)
{
    const col_rel_t *r = session_find_rel((wl_col_session_t *)s, head);
    return r ? r->nrows : 0;
}

/* ---- corpus -------------------------------------------------------------- */

static char wide_text[2][8192];

static void
append_text(char **p, size_t *left, const char *format, unsigned value,
    bool comma)
{
    int n = snprintf(*p, *left, "%s", comma ? "," : "");
    CHECK(n >= 0 && (size_t)n < *left);
    *p += n; *left -= (size_t)n;
    n = snprintf(*p, *left, format, value);
    CHECK(n >= 0 && (size_t)n < *left);
    *p += n; *left -= (size_t)n;
}

/* 70 columns: a projected row is more cells than one action evaluates, and
 * with two FILTERs the prefix decode costs more than one quantum. */
static const char *
wide_program(int filters)
{
    char *p = wide_text[filters - 1];
    size_t left = sizeof(wide_text[0]);
    const char *parts[] = {".decl c(", ")\n.decl out(", ")\nout(", ") :- c(",
                           filters == 1 ? "), c0 > 2.\n"
                           : "), c0 > 2, c1 < 9.\n"};
    for (int part = 0; part < 4; part++) {
        append_text(&p, &left, parts[part], 0, false);
        for (unsigned i = 0; i < 70; i++)
            append_text(&p, &left, part < 2 ? "c%u:int64" : "c%u",
                part == 2 ? 69 - i : i, i != 0);
    }
    append_text(&p, &left, parts[4], 0, false);
    return wide_text[filters - 1];
}

#define PROJECTION ".decl c(x:int64,y:int64)\n" \
        ".decl out(y:int64,x:int64)\n" \
        "out(y,x) :- c(x,y), x > 1.\n"

#define THREE ".decl c(x:int64,y:int64,z:int64)\n.decl out(z:int64,x:int64)\n"

static qcase_t qcases[] = {
    {"projection", PROJECTION, "c", 2, 20, "VFMC", false, "out", NULL},
    {"two_filters", THREE "out(z,x) :- c(x,y,z), z < 9, x != 4.\n", "c", 3,
     20, "VFFMC", false, "out", NULL},
    {"filter_after_map", THREE "out(z,x) :- c(x,y,z), z < 9.\n", "c", 3, 20,
     "VMFMC", true, "out", NULL},
    {"empty", PROJECTION, "c", 2, 0, "VFMC", false, "out", NULL},
    {"all_filtered", ".decl c(x:int64,y:int64)\n.decl out(y:int64)\n"
     "out(y) :- c(x,y), x > 100.\n", "c", 2, 20, "VFMC", false, "out", NULL},
    {"wide", NULL, "c", 70, 4, "VFMC", false, "out", NULL},
    {"wide_two_filters", NULL, "c", 70, 4, "VFFMC", false, "out", NULL},
    {"strings", ".decl c(x:int64,s:symbol)\n.decl out(s:symbol,x:int64)\n"
     "c(1,\"a\"). c(2,\"b\"). c(3,\"c\").\n"
     "out(s,x) :- c(x,s), x > 1.\n", "c", 0, 0, "VFMC", false, "out", NULL},
    {"second_stratum", ".decl c(x:int64,y:int64)\n"
     ".decl out(y:int64,x:int64)\n.decl top(x:int64)\n"
     "out(y,x) :- c(x,y), x > 1.\ntop(x) :- out(y,x).\n", "c", 2,
     20, "VFMC", false, "out", "top"},
};

#define NQCASES (sizeof(qcases) / sizeof(qcases[0]))

static const qcase_t *
find_qcase(const char *name)
{
    for (size_t i = 0; i < NQCASES; i++)
        if (!strcmp(qcases[i].name, name)) return &qcases[i];
    CHECK(false);
    return NULL;
}

static const char *
case_text(const qcase_t *c)
{
    plan_edit = c->edit;
    return c->text ? c->text
           : wide_program(strcmp(c->name, "wide") ? 2 : 1);
}

/* The cold rule's op order, as asserted by each case. */
static void
expect_ops(const fixture_t *f, const qcase_t *c)
{
    const wl_plan_relation_t *rp = &f->plan->strata[0].relations[0];
    char got[16] = {0};
    for (uint32_t i = 0; i < rp->op_count && i < sizeof(got) - 1; i++) {
        switch (rp->ops[i].op) {
        case WL_PLAN_OP_VARIABLE: got[i] = 'V'; break;
        case WL_PLAN_OP_MAP: got[i] = 'M'; break;
        case WL_PLAN_OP_FILTER: got[i] = 'F'; break;
        case WL_PLAN_OP_CONSOLIDATE: got[i] = 'C'; break;
        default: got[i] = '?'; break;
        }
    }
    if (strcmp(got, c->ops))
        fprintf(stderr, "%s: ops %s, want %s\n", where, got, c->ops);
    CHECK(!strcmp(got, c->ops));
}

static char where_buf[128];

static void
set_where(const qcase_t *c, via_t via, const char *what)
{
    snprintf(where_buf, sizeof(where_buf), "%s/%s/%s", c->name,
        via == VIA_SEAL ? "seal" : "load", what);
    where = where_buf;
}

/* Admitted case: a first and a second STEP, both sessions equal. */
static void
run_qcase(const qcase_t *c, via_t via)
{
    if (!c->width && via == VIA_LOAD) return; /* Facts load resident. */
    if (!c->rows && c->width && via == VIA_SEAL) return; /* Seal needs rows. */
    set_where(c, via, "run");
    fixture_t f;
    fixture(&f, case_text(c));
    expect_ops(&f, c);
    if (!c->width) {
        CHECK(wl_columnar_session_seal_join_input(f.raw, c->cold, NULL,
            NULL) == WL_COLUMNAR_SPILL_OK);
        CHECK(wl_columnar_session_seal_join_input(f.s, c->cold, NULL,
            NULL) == WL_COLUMNAR_SPILL_OK);
    } else {
        prepare(&f, c, via);
    }
    CHECK(!until_done(f.raw));
    for (int round = 0; round < 2; round++) {
        uint64_t raw = wl_columnar_ops_test_raw_cold_scans;
        hooks_clear();
        CHECK(!until_done(f.s));
        CHECK(round || checkpoints > 0);
        CHECK(wl_columnar_ops_test_raw_cold_scans == raw);
        if (round) CHECK(!until_done(f.raw));
        expect_heads(&f, c);
    }
    if (c->edit) {
        /* The edited plan over a resident input, uncontrolled. */
        fixture_t g;
        fixture(&g, case_text(c));
        fill(&g, c->cold, c->width, c->rows, true);
        CHECK(!until_done(g.raw));
        image_t *a = malloc(sizeof(*a)), *b = malloc(sizeof(*b));
        CHECK(a && b);
        image_of(f.s, c->head, a);
        image_of(g.raw, c->head, b);
        CHECK(a->count > 0 && image_equal(a, b));
        free(a);
        free(b);
        fixture_destroy(&g);
    }
    fixture_destroy(&f);
}

/* ---- stops --------------------------------------------------------------- */

static uint64_t
calibrate(const qcase_t *c, via_t via)
{
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, via);
    hooks_clear();
    CHECK(!until_done(f.s));
    uint64_t n = checkpoints;
    fixture_destroy(&f);
    return n;
}

static void
stop_case(const qcase_t *c, via_t via, int mode)
{
    set_where(c, via, mode == 1 ? "cancel" : "work");
    uint64_t n = calibrate(c, via);
    CHECK(n > 0);
    for (uint64_t k = 1; k <= n; k++) {
        fixture_t f;
        fixture(&f, case_text(c));
        prepare(&f, c, via);
        CHECK(!until_done(f.raw));
        uint64_t before = reserved(&f);
        hooks_clear();
        stop_at = k;
        stop_mode = mode;
        int rc = until_done(f.s);
        CHECK(stop_hit);
        /* The control is polled at the checkpoint itself: none follows. */
        CHECK(checkpoints == k);
        int code = mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED
            : WL_EVALUATION_CONTROL_WORK_LIMIT;
        if (rc != code)
            fprintf(stderr, "%s: k=%llu rc=%d\n", where,
                (unsigned long long)k, rc);
        CHECK(rc == code);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report));
        CHECK(report.result == code && (int)report.stop_reason == code);
        CHECK(head_rows(f.s, c->head) == 0);
        if (reserved(&f) != before)
            fprintf(stderr, "%s: k=%llu phase %d reserved %llu before %llu\n",
                where, (unsigned long long)k, (int)last_phase,
                (unsigned long long)reserved(&f), (unsigned long long)before);
        CHECK(reserved(&f) == before);
        CHECK(!wl_session_budget_denied(f.s));
        if (mode == 1 && k % 5 == 0) {
            /* Destroy after the cancellation, without a reset. A resident
             * controlled session refuses that after a work-limit stop. */
            fixture_destroy(&f);
            continue;
        }
        hooks_clear();
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!until_done(f.s));
        expect_heads(&f, c);
        fixture_destroy(&f);
    }
}

/* The scan runs its rows within one public call; its first EAGAIN return
 * after the last row is the latest park a caller sees. There the session
 * refuses checked destruction; after a cancellation the STEP ends and the
 * session is destroyed with every reservation returned. */
static void
parked_destroy_case(void)
{
    const qcase_t *c = find_qcase("wide");
    set_where(c, VIA_LOAD, "parked_destroy");
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_LOAD);
    hooks_clear();
    int rc = EAGAIN;
    for (unsigned i = 0; rc == EAGAIN && !checkpoints; i++) {
        CHECK(i < 200000);
        rc = wl_session_step(f.s);
    }
    CHECK(rc == EAGAIN && checkpoints &&
        last_phase == WL_COLUMNAR_COLD_SCAN_ATTEMPT_WRITER_END);
    wl_session_t *s = f.s;
    CHECK(wl_session_try_destroy(&s) == EBUSY && s == f.s);
    wl_evaluation_control_request_cancel(f.control);
    rc = until_done(f.s);
    CHECK(rc == 0 || rc == WL_EVALUATION_CONTROL_CANCELLED);
    fixture_destroy(&f);
}

/* A cancellation the control first sees inside a spill read. */
static void
read_cancel_case(void)
{
    const qcase_t *c = find_qcase("projection");
    set_where(c, VIA_LOAD, "read_cancel");
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_LOAD);
    CHECK(!until_done(f.raw));
    uint64_t before = reserved(&f);
    hooks_clear();
    read_cancel_control = f.control;
    read_cancel = true;
    int rc = until_done(f.s);
    read_cancel_control = NULL;
    CHECK(!read_cancel);
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(head_rows(f.s, c->head) == 0 && reserved(&f) == before);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);
}

static void
read_fault_case(void)
{
    const qcase_t *c = find_qcase("projection");
    set_where(c, VIA_LOAD, "read_fault");
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_LOAD);
    CHECK(!until_done(f.raw));
    uint64_t before = reserved(&f);
    read_fault = 1;
    int rc = until_done(f.s);
    CHECK(!read_fault);
    CHECK(rc == EIO);
    CHECK(head_rows(f.s, c->head) == 0 && reserved(&f) == before);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);
}

/* Governor denials: a blocker leaves `room` bytes free before the STEP. */
static void
denial_case(const qcase_t *c, via_t via)
{
    set_where(c, via, "denial");
    unsigned phases = 0, denials = 0, passes = 0;
    for (uint64_t room = 0; room <= UINT64_C(256) * 1024 && !passes;
        room += 64) {
        fixture_t f;
        fixture(&f, case_text(c));
        prepare(&f, c, via);
        CHECK(!until_done(f.raw));
        uint64_t before = reserved(&f);
        CHECK(before + room <= BUDGET);
        wl_columnar_memory_reservation_t blocker;
        wl_columnar_memory_reservation_init(&blocker);
        CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
                f.governor), BUDGET - before - room, &blocker));
        hooks_clear();
        int rc = until_done(f.s);
        if (!rc) {
            passes++;
            CHECK(!wl_session_budget_denied(f.s));
            CHECK(wl_columnar_memory_release(&blocker));
            expect_heads(&f, c);
            fixture_destroy(&f);
            continue;
        }
        if (rc != ENOSPC)
            fprintf(stderr, "%s: room %llu rc %d\n", where,
                (unsigned long long)room, rc);
        CHECK(rc == ENOSPC);
        CHECK(wl_session_budget_denied(f.s));
        CHECK(head_rows(f.s, c->head) == 0);
        CHECK(reserved(&f) == BUDGET - room);
        if (checkpoints) {
            denials++;
            phases |= 1u << (unsigned)last_phase;
        }
        CHECK(wl_columnar_memory_release(&blocker));
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!until_done(f.s));
        expect_heads(&f, c);
        fixture_destroy(&f);
    }
    CHECK(passes == 1);
    /* The last checkpoint before each denial names the action it stopped:
     * scratch, output, reader, the writer's drain after WRITER_END, and on
     * the loading route a row read that loads a segment. */
    unsigned want = 1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_SCRATCH |
        1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_OUTPUT_CREATE |
        1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_READER |
        1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_WRITER_END |
        (via == VIA_LOAD ? 1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_ROWS : 0u);
    if ((phases & want) != want)
        fprintf(stderr, "%s: denials %u phases %#x\n", where, denials,
            phases);
    CHECK(denials && (phases & want) == want);
}

/* A full STEP consumes the same work on both routes, and one unit less
 * stops it.  The total itself differs between toolchains (582040 with gcc
 * and clang on Linux, 568600 with MSVC), so it is not pinned. */
static uint64_t
exact_case(via_t via)
{
    const qcase_t *c = find_qcase("projection");
    set_where(c, via, "exact");
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, via);
    CHECK(!until_done(f.s));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.control, &report));
    uint64_t expected = report.consumed;
    CHECK(expected > 0);
    fixture_destroy(&f);

    fixture(&f, case_text(c));
    prepare(&f, c, via);
    CHECK(!wl_evaluation_control_configure(f.control, expected - 1));
    CHECK(until_done(f.s) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_configure(f.control, expected));
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!until_done(f.s));
    fixture_destroy(&f);
    return expected;
}

/* ---- refusals ------------------------------------------------------------ */

typedef struct {
    const char *name;
    const char *text;
    const char *cold;
    uint32_t width;
    bool side_compound; /* Make a side compound before sealing. */
} refusal_t;

static const refusal_t refusals[] = {
    {"right_input", ".decl a(x:int64,y:int64)\n.decl c(y:int64,z:int64)\n"
     ".decl out(x:int64,z:int64)\nout(x,z) :- a(x,y), c(y,z).\n", "c", 2,
     false},
    {"self_join", ".decl c(x:int64,y:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y), c(y,z).\n", "c", 2, false},
    {"pair_without_map", ".decl c(x:int64,y:int64)\n.decl r(y:int64,z:int64)"
     "\n.decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- c(x,y), r(y,z).\n", "c", 2, false},
    {"filter_prefix_join", ".decl c(x:int64,y:int64,w:int64)\n"
     ".decl r(y:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y,w), w = 7, r(y,z).\n", "c", 3, false},
    {"count", ".decl c(x:int64,y:int64)\n.decl out(x:int64,n:int64)\n"
     "out(x,count(y)) :- c(x,y).\n", "c", 2, false},
    {"seed_shape", ".decl c(x:int64,y:int64)\n.decl d(x:int64)\n"
     ".decl out(y:int64,x:int64)\nout(y,x) :- c(x,y), x > 1.\n", "c", 2,
     false},
    {"head_is_edb", ".decl c(x:int64,y:int64)\n.decl d(y:int64,x:int64)\n"
     "d(9,9).\nd(y,x) :- c(x,y), x > 1, x < 90.\n", "c", 2, false},
    {"registry", ".decl c(x:int64,y:int64)\n.decl r(x:int64)\n"
     ".decl out(y:int64,x:int64)\n.decl out2(x:int64)\n"
     "out(y,x) :- c(x,y), x > 1.\nout2(x) :- r(x).\n", "c", 2, false},
    {"registry_side", ".decl c(x:int64,y:int64)\n"
     ".decl e(i:int64,h:pair/2 side)\n"
     ".decl out(y:int64,x:int64)\n"
     "out(y,x) :- c(x,y), x > 1, x < 90.\n", "c", 2, true},
};

#define NREFUSALS (sizeof(refusals) / sizeof(refusals[0]))

static void
refusal_case(const refusal_t *r, via_t via, bool compound)
{
    snprintf(where_buf, sizeof(where_buf), "refusal/%s/%s%s", r->name,
        via == VIA_SEAL ? "seal" : "load", compound ? "" : "/control");
    where = where_buf;
    fixture_t f;
    fixture(&f, r->text);
    if (compound) {
        wirelog_compound_arg_t args[2] = {
            {WIRELOG_TYPE_INT64, 1}, {WIRELOG_TYPE_INT64, 2}
        };
        uint64_t h[2];
        CHECK(!wl_session_make_compound(f.s, "pair", 2, args, &h[0]));
        CHECK(!wl_session_make_compound(f.raw, "pair", 2, args, &h[1]));
    }
    bool refuse = !r->side_compound || compound;
    wl_columnar_spill_status_t want = refuse
        ? WL_COLUMNAR_SPILL_UNSUPPORTED : WL_COLUMNAR_SPILL_OK;
    if (via == VIA_SEAL) {
        fill(&f, r->cold, r->width, 4, true);
        CHECK(wl_columnar_session_seal_join_input(f.raw, r->cold, NULL,
            NULL) == WL_COLUMNAR_SPILL_OK);
        CHECK(wl_columnar_session_seal_join_input(f.s, r->cold, NULL,
            NULL) == want);
    } else {
        CHECK(load(f.raw, r->cold, r->width, 4) == WL_COLUMNAR_SPILL_OK);
        CHECK(load(f.s, r->cold, r->width, 4) == want);
    }
    /* A refused source stays resident: the STEP returns what it returns in
     * a session that never tried the seal. An admitted one runs the attempt. */
    hooks_clear();
    int rc = until_done(f.s);
    CHECK(refuse ? checkpoints == 0 : checkpoints > 0 && !rc);
    if (refuse) {
        fixture_t g;
        fixture(&g, r->text);
        if (compound) {
            wirelog_compound_arg_t args[2] = {
                {WIRELOG_TYPE_INT64, 1}, {WIRELOG_TYPE_INT64, 2}
            };
            uint64_t h;
            CHECK(!wl_session_make_compound(g.s, "pair", 2, args, &h));
        }
        if (via == VIA_SEAL) fill(&g, r->cold, r->width, 4, false);
        int expected = until_done(g.s);
        if (rc != expected)
            fprintf(stderr, "%s: rc %d, unsealed %d\n", where, rc, expected);
        CHECK(rc == expected);
        fixture_destroy(&g);
    }
    fixture_destroy(&f);
}

static void
on_delta(const char *name, const int64_t *v, uint32_t width, int diff,
    void *data)
{
    (void)name; (void)v; (void)width; (void)diff; (void)data;
}

static void
on_tuple(const char *name, const int64_t *v, uint32_t width, void *data)
{
    (void)name; (void)v; (void)width; (void)data;
}

static void
mode_refusals(void)
{
    const qcase_t *c = find_qcase("projection");
    fixture_t f;

    where = "mode/snapshot";
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(wl_session_snapshot(f.s, on_tuple, NULL) == ENOTSUP);
    CHECK(head_rows(f.s, c->head) == 0);
    fixture_destroy(&f);

    where = "mode/callback_first";
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(!until_done(f.raw));
    wl_session_set_delta_cb(f.s, on_delta, NULL);
    CHECK(wl_session_step(f.s) == ENOTSUP);
    wl_session_set_delta_cb(f.s, NULL, NULL);
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);

    where = "mode/callback_changed";
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(!until_done(f.raw));
    CHECK(!until_done(f.s));
    uint64_t raw = wl_columnar_ops_test_raw_cold_scans;
    wl_session_set_delta_cb(f.s, on_delta, NULL);
    wl_session_set_delta_cb(f.s, NULL, NULL);
    uint64_t before = reserved(&f);
    /* Inputs are frozen after the seal: the STEP completes without an
     * attempt, so the changed callback selects no route. */
    hooks_clear();
    CHECK(!until_done(f.s));
    CHECK(checkpoints == 0);
    CHECK(wl_columnar_ops_test_raw_cold_scans == raw);
    CHECK(reserved(&f) == before);
    expect_heads(&f, c);
    fixture_destroy(&f);
}

/* A callback set on the first EAGAIN return of an attempt is ignored, as in
 * a resident controlled session: with a stop later in the same attempt, the
 * attempt still stops and cleans up; without one it completes, and neither
 * session delivers an event. */
static unsigned delta_events;

static void
count_delta(const char *name, const int64_t *v, uint32_t width, int diff,
    void *data)
{
    (void)name; (void)v; (void)width; (void)diff; (void)data;
    delta_events++;
}

static int
parked_run(wl_session_t *s, bool *parked)
{
    int rc = wl_session_step(s);
    *parked = rc == EAGAIN;
    if (rc == EAGAIN) wl_session_set_delta_cb(s, count_delta, NULL);
    /* The session ignores the call while the attempt is parked. */
    CHECK(!((wl_col_session_t *)s)->delta_cb);
    for (unsigned i = 0; rc == EAGAIN; i++) {
        CHECK(i < 200000);
        rc = wl_session_step(s);
    }
    wl_session_set_delta_cb(s, NULL, NULL);
    return rc;
}

static void
parked_callback(void)
{
    const qcase_t *c = find_qcase("projection");
    where = "mode/parked_callback/stop";
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(!until_done(f.raw));
    uint64_t before = reserved(&f);
    hooks_clear();
    stop_at = 40;
    stop_mode = 1;
    bool parked = false;
    CHECK(parked_run(f.s, &parked) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(parked && stop_hit);
    CHECK(head_rows(f.s, c->head) == 0 && reserved(&f) == before);
    CHECK(!wl_evaluation_control_reset(f.control));
    hooks_clear();
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);

    where = "mode/parked_callback/complete";
    unsigned events[2];
    for (int cold = 0; cold < 2; cold++) {
        fixture(&f, case_text(c));
        if (cold) prepare(&f, c, VIA_SEAL);
        else fill(&f, c->cold, c->width, c->rows, true);
        CHECK(!until_done(f.raw));
        hooks_clear();
        delta_events = 0;
        CHECK(!parked_run(f.s, &parked));
        CHECK(parked && (cold ? checkpoints > 0 : checkpoints == 0));
        events[cold] = delta_events;
        expect_heads(&f, c);
        fixture_destroy(&f);
    }
    if (events[0] != events[1])
        fprintf(stderr, "%s: events resident %u cold %u\n", where,
            events[0], events[1]);
    CHECK(events[0] == 0 && events[1] == 0);
}

int
main(void)
{
    wl_columnar_ops_test_cold_scan_checkpoint = cold_checkpoint;
    for (size_t i = 0; i < NQCASES; i++) {
        run_qcase(&qcases[i], VIA_SEAL);
        run_qcase(&qcases[i], VIA_LOAD);
    }
    for (int mode = 1; mode <= 2; mode++) {
        stop_case(find_qcase("projection"), VIA_SEAL, mode);
        stop_case(find_qcase("projection"), VIA_LOAD, mode);
        stop_case(find_qcase("wide"), VIA_SEAL, mode);
    }
    parked_destroy_case();
    read_cancel_case();
    read_fault_case();
    denial_case(find_qcase("projection"), VIA_SEAL);
    denial_case(find_qcase("projection"), VIA_LOAD);
    uint64_t sealed = exact_case(VIA_SEAL);
    uint64_t loaded = exact_case(VIA_LOAD);
    if (sealed != loaded)
        fprintf(stderr, "exact: sealed %llu loaded %llu\n",
            (unsigned long long)sealed, (unsigned long long)loaded);
    CHECK(sealed == loaded);
    for (size_t i = 0; i < NREFUSALS; i++) {
        refusal_case(&refusals[i], VIA_SEAL, refusals[i].side_compound);
        refusal_case(&refusals[i], VIA_LOAD, refusals[i].side_compound);
        if (refusals[i].side_compound) {
            refusal_case(&refusals[i], VIA_SEAL, false);
            refusal_case(&refusals[i], VIA_LOAD, false);
        }
    }
    mode_refusals();
    parked_callback();
    printf("evaluation_cold_sources: ok\n");
    return 0;
}
