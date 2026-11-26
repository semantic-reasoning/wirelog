/* #2301: cold (spill-backed) sources in controlled single-worker sessions.
 *
 * A controlled session (an evaluation control, an enforcing governor, one
 * worker) and an uncontrolled one are created with the same program, spill
 * store and JOIN caps, get the same rows, and seal the same input or load it
 * through begin/append/finish. The controlled STEP reads the cold binding
 * through the composed cold scan attempt; the test checks for every admitted
 * case that the attempt's checkpoint fired, that neither the unmetered raw
 * cold scan, the raw cold aggregate, the raw cold-left JOIN, the raw cold JOIN
 * aggregate nor the raw JOIN of a cold right input ran, and that both sessions
 * hold the same head image, row values and column types, after a first and a
 * second STEP. The cold right input cases run as their own test,
 * evaluation_cold_right_sources, which passes this executable the argument
 * "right".
 *
 * Five shapes are admitted, each case asserting its plan's op order and
 * whether the cold input is the left input or the JOIN's right one. VARIABLE
 * over the cold input, a direct MAP/FILTER prefix with at least one MAP, then
 * the rule's final CONSOLIDATE: the corpus has one and two FILTERs before the
 * MAP, an empty input, an input every row of which is filtered out, 70-column
 * rows, with one and with two FILTERs, STRING lanes from program facts, and a
 * second stratum that reads the head. The planner puts every FILTER before the
 * MAP, so a FILTER after a permuting MAP comes from a plan the test edits; its
 * image is also compared with an uncontrolled session that keeps the input
 * resident. VARIABLE, a direct MAP/FILTER prefix, possibly empty, then a
 * REDUCE that the uncontrolled cold aggregate admits and the final
 * CONSOLIDATE; the scan's output then feeds the controlled REDUCE, whose
 * checkpoint the test also requires. The corpus has a grouped COUNT and MIN, a
 * MAX in the head's first column, a scalar COUNT over 6, 20 and 200 rows, a
 * scalar SUM over an empty input, a scalar COUNT over an input every row of
 * which is filtered out, a FILTER before a grouped SUM over 6 and 20 rows and
 * before a grouped COUNT, a STRING group key from program facts, and, from an
 * edited plan, a permuting MAP before a grouped SUM, compared also with the
 * resident input. And VARIABLE, a direct MAP/FILTER prefix, possibly empty,
 * then a JOIN with a resident right input and the operators after it that the
 * uncontrolled cold JOIN admits; the scan's output feeds the controlled JOIN,
 * whose checkpoint the test requires, and the uncontrolled session is checked
 * to have read the input through the raw cold-left JOIN or the raw scan. The
 * corpus has a JOIN right after the VARIABLE over 6, 20 and 200 rows and with
 * an empty right input, an equality FILTER before the JOIN over 6 and 20 rows
 * and with every row filtered out, a STRING key from program facts, and, from
 * an edited plan, a permuting MAP before the JOIN, compared also with the
 * resident input. Each JOIN case with right rows, other than the filtered-out
 * one, publishes rows. And the JOIN shape followed by a COUNT, MIN or MAX that
 * the uncontrolled cold JOIN aggregate admits, as the rule's next to last op:
 * the controlled REDUCE then runs over the controlled JOIN's output, both
 * checkpoints are required, and the uncontrolled session is checked to have
 * run the raw cold JOIN aggregate. The corpus has a grouped COUNT right after
 * the VARIABLE over 6 and 20 rows and over a four-column input, a grouped
 * COUNT whose right input holds three rows per key, checked to count every
 * pair of the JOIN, a MIN after an equality FILTER before the JOIN, a MAX
 * after a FILTER that follows the JOIN, a STRING JOIN key and group key from
 * program facts, and, with a right input declared but never inserted, a scalar
 * COUNT, that MIN, that MAX and a COUNT grouped by a STRING column of the
 * right input, each of which publishes no row. These cases are compared also
 * with an uncontrolled and a controlled session that keep the input resident.
 * And a cold input read as the right input of a JOIN at the rule's second op,
 * after a VARIABLE over a resident input, with no REDUCE: the cold scan copies
 * the input whole, the controlled JOIN reads the copy, the scan's and the
 * JOIN's checkpoints are required, the scan's with its right-input flag, and
 * the uncontrolled session is checked to have run the raw JOIN of a cold right
 * input. The corpus has the JOIN over 6, 20 and 200 cold rows, with the head
 * keeping every column and with a projection, over a four-column cold input,
 * with a left input declared but never inserted, and with a STRING key from
 * program facts. These cases other than the one from facts are compared also
 * with an uncontrolled and a controlled session that keep both inputs
 * resident. The JOIN that reads a cold right input may instead be the rule's
 * first JOIN after an equality FILTER over the resident input, which then
 * joins the FILTER's output with the copy. The corpus has it over 6 and 20
 * cold rows and over 10 cold rows of four columns, followed by a second,
 * one-key JOIN of the resident input, followed by a grouped COUNT and with two
 * keys, the resident rows from program facts, and, from an edited plan, after
 * a permuting MAP over the resident input, which the copy's columns do not
 * follow; each of these cases is compared also with an uncontrolled and a
 * controlled session that keep both inputs resident. The JOIN that reads a
 * cold right input may also come after one or more JOINs of the resident input
 * with itself, with no REDUCE; the copy is then scanned when the rule reaches
 * that JOIN, which the test checks from the JOIN checkpoints that come before
 * the scan's first. The corpus has it after one such JOIN over 6 and 20 cold
 * rows, with the key from the earlier JOIN's left or right input, after an
 * equality FILTER before the earlier JOIN, after two such JOINs, followed by a
 * further JOIN of the resident input, with two keys over 6 and 20 cold rows,
 * with two keys followed by a FILTER, and with a STRING key from program
 * facts; each of these cases with generated rows is compared also with an
 * uncontrolled and a controlled session that keep both inputs resident. The
 * JOIN that reads the cold input, as its left or its right input, may have two
 * keys, and so may a JOIN after it; in the generated cases the rows of the
 * resident input match the cold rows on both keys or on the first only. Where
 * only the JOIN that reads the cold input has two keys, the first column of
 * every head row is checked to be that of a row that matches on both. The
 * corpus has, with the cold input on the left, the JOIN right after the
 * VARIABLE over 6 and 20 rows, with an empty right input, followed by a FILTER
 * and followed by a second, one-key JOIN of the right input, the JOIN after an
 * equality FILTER, a grouped COUNT after the JOIN over 6 and 20 rows, a MAX
 * after the equality FILTER and, from program facts, a JOIN and a COUNT whose
 * INT64 and STRING keys sit at other positions in the two inputs; and with the
 * cold input on the right, the JOIN over 6 and 20 cold rows, followed by a
 * FILTER, with a left input declared but never inserted, followed by a second,
 * one-key JOIN of the left input, a grouped COUNT, a MAX and, from program
 * facts, a JOIN with keys at other positions. Where a later JOIN has two keys,
 * each head row is checked to hold two equal values or, in the cases with a
 * one-key JOIN of a cold right input, an even row index of the resident input,
 * which a later JOIN on its first or its second key alone does not give in the
 * generated cases with 20 rows in both inputs. The corpus has, with the cold
 * input on the left, a one-key JOIN followed by a two-key JOIN of the right
 * input over 6 and 20 cold rows, with an empty right input and after an
 * equality FILTER, a two-key JOIN followed by another, and, from program
 * facts, two two-key JOINs whose INT64 and STRING keys sit at other positions
 * in the two inputs and a one-key JOIN followed by a two-key JOIN whose first
 * key is the cold input's second column; and with the cold input on the right,
 * a one-key JOIN followed by a two-key JOIN of the left input over 6 and 20
 * cold rows and with a left input declared but never inserted, a two-key JOIN
 * followed by another and, from program facts, a two-key JOIN followed by
 * another whose second key comes from the cold input's last column. Each of
 * these cases asserts that its last JOIN comes after the one that reads the
 * cold input and has two keys. Each of these cases other than those from
 * program facts without an aggregate is compared also with an uncontrolled and
 * a controlled session that keep the inputs resident.
 *
 * Stops: for every cold scan checkpoint k of a calibrated run, a cancellation
 * and a work-limit stop at k on a first STEP: for the projection on the seal
 * and on the loading route and for 70-column rows, and for a filtered grouped
 * SUM on the seal route. For every checkpoint of the controlled REDUCE that
 * follows the scan, the same stops for a filtered grouped SUM over six rows on
 * the seal route and a scalar COUNT over six rows on the loading route. For
 * every checkpoint of the scan, the same stops for the filtered JOIN over six
 * rows on the seal route; for every checkpoint of the controlled JOIN that
 * follows it, a work-limit stop for that JOIN on the seal route and a
 * cancellation for the JOIN right after the VARIABLE over six rows on the
 * loading route. For every checkpoint of the controlled REDUCE after a JOIN, a
 * cancellation for the grouped COUNT right after the VARIABLE over six rows on
 * the seal route. For every checkpoint of the scan of a cold right input over
 * six rows, both stops on both routes; for every checkpoint of the JOIN that
 * reads its copy, a work-limit stop on the seal route and a cancellation on
 * the loading route; the same after an equality FILTER over the resident
 * input, and a cancellation at every checkpoint of the REDUCE of a grouped
 * COUNT after such a JOIN over 20 cold rows on the seal route; and the same
 * after a JOIN of the resident input with itself, the JOIN checkpoints then
 * counting those of both JOINs, and a cancellation at every JOIN checkpoint
 * with two keys at the later JOIN on the loading route. For a JOIN with two
 * keys over six cold rows, a work-limit stop at every JOIN checkpoint with the
 * cold input on the left on the seal route and a cancellation at every one
 * with it on the right on the loading route, and a cancellation at every
 * REDUCE checkpoint of a COUNT after such a JOIN with the cold input on the
 * left on the seal route. For a one-key JOIN followed by a two-key JOIN over
 * six cold rows, a work-limit stop at every checkpoint of either JOIN with the
 * cold input on the left on the seal route and a cancellation at every one
 * with it on the right on the loading route. Each lands at k, returns the stop
 * code with a report naming it, publishes no head, returns the governor to its
 * value before the STEP, and after a reset STEP returns 0 with the
 * uncontrolled image. Some of the cancelled sessions are destroyed without a
 * reset. The scan runs its rows within one public call; parked at its first
 * EAGAIN return after the last row, a session refuses wl_session_try_destroy,
 * and after a cancellation its STEP ends and it is destroyed with no
 * reservation left. A resident controlled JOIN, which the cold one reuses, and
 * the CONSOLIDATE after it run across many public calls: a cancellation
 * requested at every EAGAIN return of such a STEP ends it as cancelled with
 * every reservation returned, or, once requested after its last poll, lets it
 * complete; a cancelled session is destroyed at once or, after a reset, STEPs
 * to the uncontrolled image. Parked inside the JOIN that reads the copy of a
 * cold right input, with and without an equality FILTER before it, a session
 * refuses wl_session_try_destroy, and after a cancellation its STEP ends with
 * every reservation returned; with the FILTER, and after a JOIN of the
 * resident input with itself, parked at the STEP's first return, before the
 * scan began, it also refuses wl_session_try_destroy, and its STEP either ends
 * cancelled with every reservation returned or resumes and completes; parked
 * inside that earlier JOIN, before the scan began, it refuses
 * wl_session_try_destroy and its STEP ends cancelled with every reservation
 * returned; an allocation that JOIN cannot make, with one key, with two and
 * before a later JOIN with two keys, with one and with two keys after the
 * FILTER, and with one and with two keys after a JOIN of the resident input
 * with itself, fails the STEP with ENOMEM, with every reservation returned, as
 * does an allocation that earlier JOIN cannot make, before the scan. A
 * cancellation requested from inside a spill read, an injected read fault and
 * governor denials at the admission points of the attempt are checked the same
 * way, the denials with wl_session_budget_denied, for the projection, the SUM,
 * the filtered JOIN, the grouped COUNT after a JOIN and the JOIN of a cold
 * right input, the cancellation and the fault also after the FILTER and after
 * a JOIN of the resident input with itself, the denials of the filtered JOIN
 * over six rows and those of the COUNT over the four-column input on the seal
 * route only, those of the JOIN of a four-column cold right input on both
 * routes, after the FILTER on the seal route, where some come before the scan
 * began and in the case run none at the scan's scratch, and after a JOIN of
 * the resident input with itself on both routes, where likewise some come
 * before the scan began, inside the earlier JOIN, and are not counted as the
 * scan's or the later JOIN's; some SUM and JOIN denials come after the REDUCE
 * or the JOIN started, some COUNT denials inside the REDUCE after the JOIN,
 * and for those a denial after WRITER_END is not told apart from them. For
 * every admitted case the first STEP prepays exactly the resolution work that
 * a closed form over the plan's prefix and REDUCE gives, none for a cold right
 * input, which the scan reads whole. A full STEP of the projection, the SUM,
 * the filtered JOIN, the grouped COUNT after a JOIN, the JOIN of a cold right
 * input, with and without the FILTER before it and after a JOIN of the
 * resident input with itself, the JOINs with two keys of a cold left and a
 * cold right input over 20 rows and, with either, a one-key JOIN followed by a
 * two-key JOIN over 20 rows consumes the same work on the seal and the loading
 * route, and a limit one unit below it stops the STEP. Within the rows no
 * action evaluates more than 64 cells, and no resolution action charges more
 * than 257 units.
 *
 * Aggregates also: a SUM that overflows returns ERANGE on both sessions on
 * both routes, publishes nothing and returns the governor to its value before
 * the STEP. The controlled route reads every row before its REDUCE starts, so
 * a read fault injected after the reads of the uncontrolled route, which stops
 * at the overflowing row, returns EIO controlled while the uncontrolled STEP
 * returns ERANGE. The least free memory at which a STEP succeeds is larger
 * controlled than uncontrolled for a scalar COUNT over 200 rows and for a JOIN
 * over 200 rows: the scan's output is held beside the REDUCE's or the JOIN's
 * state.
 *
 * JOINs also: a right input with rows from program facts and from inserts made
 * before the seal or load is joined whole on both routes; a STEP before the
 * seal or load makes either one return WL_COLUMNAR_SPILL_BUSY, and after it an
 * insert into the right input returns ENOTSUP.
 *
 * Refusals: seal and begin return WL_COLUMNAR_SPILL_UNSUPPORTED under control,
 * and WL_COLUMNAR_SPILL_OK uncontrolled, for a cold input read by a self-JOIN,
 * a JOIN over a source with an INLINE compound column and a COUNT over one, a
 * cold right input read by a second JOIN, a cold right input read again by a
 * later JOIN, after an equality FILTER over the resident input and with no
 * prefix, a cold right input read by the rule's second JOIN after a JOIN of a
 * third input, whose registry the controlled bound refuses, a two-EDB
 * single-rule projection, a head that is also an EDB, with a projection and
 * with a COUNT, and a registry that a successor STEP would find larger than
 * three relations. Both sessions refuse a SUM over a JOIN, a cold right input
 * after an equality FILTER on its JOIN key, a COUNT after a later JOIN that
 * reads a cold right input, a JOIN with no key, a JOIN with no key after a
 * one-key JOIN with the cold input on the left and after a two-key JOIN with
 * it on the right, and a COUNT after a two-key JOIN that follows a one-key
 * JOIN, with the cold input on either side. With a right filter and a cold
 * right input they also refuse the rule with the filter after a prefix, with
 * the filter on the JOIN that reads the cold input, with two keys at the
 * earlier JOIN or at the cold one and with the cold input at the first JOIN,
 * and with no prefix and one key a comparison that the planner emits as a
 * FILTER after the JOINs. Of two cold inputs of one JOIN, the second seal is
 * refused under control in either order, and the STEP gives what an
 * uncontrolled session with only the first sealed gives; of the resident input
 * of a JOIN with itself and the cold right input of a later JOIN, the former
 * is refused under control in either order, and the STEP gives what an
 * uncontrolled session with only the latter sealed gives. A later JOIN of a
 * cold right input whose left key is a STRING column, against the cold input's
 * INT64 key, is admitted at seal and begin uncontrolled and fails its STEP
 * with ENOTSUP, and is refused at seal and begin under control; the parser
 * refuses such a rule, so the key comes from an edited plan. Over a resident
 * input declared but never inserted, the earlier JOIN's output has no column,
 * and the STEP returns ENOTSUP on both sessions, the controlled one before the
 * scan with every reservation returned. A controlled SNAPSHOT returns ENOTSUP
 * and a STEP with a delta callback set returns ENOTSUP. A STEP after a
 * callback was set and cleared following a completed STEP completes without an
 * attempt, the inputs being frozen; a callback set while an attempt is parked
 * is ignored, as in a resident controlled session.
 *
 * Right filters: with no prefix and one key at every JOIN, a JOIN of the
 * resident input before or after the one that reads a cold right input may
 * carry a right filter, a constant in its right input. The corpus has an INT64
 * constant at the earlier JOIN over 6 and 20 cold rows, one that no resident
 * row matches, two filtered JOINs before the cold one, a filtered JOIN after
 * it and a STRING constant from program facts, each compared with the
 * uncontrolled session and, with generated rows, with the sessions that keep
 * both inputs resident. With the earlier JOIN filtered, both stops at every
 * scan checkpoint on both routes, a work-limit stop at every JOIN checkpoint
 * on the seal route and a cancellation at every one on the loading route, the
 * parks in the earlier JOIN and in the one that reads the copy, an allocation
 * that a JOIN cannot make, also with the filtered JOIN after the cold one, the
 * read cancellation, the read fault and the denials on both routes, on the
 * seal route also with the filtered JOIN after the cold one, behave as without
 * the filter; the full STEP consumes the same work on both routes, the STRING
 * left key from an edited plan is refused under control, and over a resident
 * input never inserted the STEP returns ENOTSUP on both sessions.
 *
 * Twice, run with the argument "twice": two JOINs of a rule or more may read a
 * cold right input, which is scanned once, every such JOIN reading the copy.
 * The corpus has two such JOINs with no prefix over 8 and 20 cold rows, after
 * the resident input's prefix, after a JOIN of the resident input with itself
 * and with one between them, and three such JOINs; two keys at the first and
 * at the second; a right filter at an earlier JOIN of the resident input; a
 * second JOIN that no row matches; and STRING keys from program facts, each
 * compared with the uncontrolled session and, with generated rows, with the
 * sessions that keep both inputs resident. Both stops at every scan checkpoint
 * on both routes, a work-limit stop at every JOIN checkpoint on the seal route
 * and a cancellation at every one on the loading route, parks after the scan
 * and in the first and a later JOIN of the copy, an allocation that the first
 * or a later one cannot make, the read cancellation, the read fault and the
 * denials on both routes behave as with one such JOIN; the full STEP consumes
 * the same work on both routes; a STRING left key at the later JOIN from an
 * edited plan is refused under control; and over a first input never inserted
 * the controlled session refuses the seal or load, the uncontrolled STEP
 * returning ENOTSUP or no head row.
 *
 * Not checked here: more than one worker; an observed STEP or an observed
 * SNAPSHOT over a cold source, both refused; a MAP whose lanes, or a REDUCE
 * whose group keys and operand, exceed one quantum of resolution lanes; a
 * work-limit stop requested between public calls, which the control does not
 * accept while an attempt is active; the loading calls under the control,
 * which neither meters nor cancels them; the least free memory at which an
 * aggregate after a JOIN succeeds; a work-limit stop at every checkpoint of
 * such an aggregate's REDUCE; stops at the scan and JOIN checkpoints before
 * it, which are not run for this shape; a FLOAT or unsigned column carried
 * through the JOIN to the aggregate; the least free memory at which the JOIN
 * of a cold right input succeeds, whose copy is held until the attempt
 * releases it after the final CONSOLIDATE; and, for a JOIN with two keys, a
 * MAP before it, a FLOAT or unsigned key, an allocation failure inside a later
 * one, the read fault, the cancellation inside a spill read, the parked STEP
 * and the governor denials; and, for a cold right input after the resident
 * input's prefix, a MAP that the planner emits before the JOIN, a REDUCE other
 * than a grouped COUNT, the governor denials on the loading route, the least
 * free memory at which a STEP succeeds and the amount of work that the scan's
 * type lanes charge, which only the parity of the two routes and a stop one
 * unit lower check; and, for a cold right input after a JOIN of the resident
 * input with itself, a REDUCE, a MAP that the planner emits between the JOINs,
 * an earlier JOIN with a projection over a resident input never inserted, a
 * key type other than INT64 and STRING declared on the resident input, which
 * carries no column types once inserted in the cases tried, a work-limit stop
 * at every JOIN checkpoint on the loading route and a cancellation at every
 * one on the seal route, the least free memory at which a STEP succeeds and an
 * insert into the resident input after the seal. */
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
static uint64_t read_ios; /* Spill reads seen, faulted or not. */
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
    ++read_ios;
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
static int plan_edit; /* 1 FILTER after MAP, 2 MAP before REDUCE, 3 MAP
                      * before JOIN, 4 a STRING key at a later JOIN. */
static wl_plan_op_t edited_ops[5];
static uint32_t edit_permute[3] = {1, 2, 0}, edit_project[2] = {1, 2};
static uint32_t edit_swap[2] = {1, 0}, edit_group[1] = {1};
static uint8_t reduce_expr[7]; /* VAR, length 4, "col0". */
static const char *const edit_left_keys[1] = {"col0"};
static const char *const edit_string_keys[1] = {"col2"};
static uint32_t edit_join_project[3] = {1, 0, 3};

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
    if (plan_edit == 2) {
        /* VARIABLE, REDUCE(group col0, sum col1), CONSOLIDATE becomes
         * VARIABLE, MAP[1,0], REDUCE(group col1, sum col0), CONSOLIDATE. */
        wl_plan_relation_t *rp =
            (wl_plan_relation_t *)&f->plan->strata[0].relations[0];
        CHECK(rp->op_count == 3 && rp->ops[1].op == WL_PLAN_OP_REDUCE &&
            rp->ops[1].group_by_count == 1 &&
            rp->ops[1].group_by_indices[0] == 0 &&
            rp->ops[1].agg_expr.size == sizeof(reduce_expr) &&
            !memcmp(rp->ops[1].agg_expr.data + 3, "col1", 4));
        f->saved_ops = rp->ops;
        f->saved_count = rp->op_count;
        memcpy(reduce_expr, rp->ops[1].agg_expr.data, sizeof(reduce_expr));
        reduce_expr[6] = '0';
        edited_ops[0] = rp->ops[0];
        edited_ops[1] = (wl_plan_op_t){.op = WL_PLAN_OP_MAP,
                                       .project_count = 2,
                                       .project_indices = edit_swap};
        edited_ops[2] = rp->ops[1];
        edited_ops[2].group_by_indices = edit_group;
        edited_ops[2].agg_expr.data = reduce_expr;
        edited_ops[3] = rp->ops[2];
        rp->ops = edited_ops;
        rp->op_count = 4;
        plan_edit = 0;
    }
    if (plan_edit == 3) {
        /* VARIABLE, JOIN(col1 = col0, project 0,1,3), CONSOLIDATE becomes
         * VARIABLE, MAP[1,0], JOIN(col0 = col0, project 1,0,3),
         * CONSOLIDATE. */
        wl_plan_relation_t *rp =
            (wl_plan_relation_t *)&f->plan->strata[0].relations[0];
        CHECK(rp->op_count == 3 && rp->ops[1].op == WL_PLAN_OP_JOIN &&
            rp->ops[1].key_count == 1 &&
            !strcmp(rp->ops[1].left_keys[0], "col1") &&
            rp->ops[1].project_count == 3 &&
            rp->ops[1].project_indices[0] == 0 &&
            rp->ops[1].project_indices[1] == 1 &&
            rp->ops[1].project_indices[2] == 3);
        f->saved_ops = rp->ops;
        f->saved_count = rp->op_count;
        edited_ops[0] = rp->ops[0];
        edited_ops[1] = (wl_plan_op_t){.op = WL_PLAN_OP_MAP,
                                       .project_count = 2,
                                       .project_indices = edit_swap};
        edited_ops[2] = rp->ops[1];
        edited_ops[2].left_keys = edit_left_keys;
        edited_ops[2].project_indices = edit_join_project;
        edited_ops[3] = rp->ops[2];
        rp->ops = edited_ops;
        rp->op_count = 4;
        plan_edit = 0;
    }
    if (plan_edit == 4) {
        /* VARIABLE, JOIN, JOIN(col4 = col0), CONSOLIDATE: the later JOIN's
         * left key becomes col2, the first input's STRING column, against
         * the cold input's INT64 key. */
        wl_plan_relation_t *rp =
            (wl_plan_relation_t *)&f->plan->strata[0].relations[0];
        CHECK(rp->op_count == 4 && rp->ops[1].op == WL_PLAN_OP_JOIN &&
            !rp->ops[1].project_count && rp->ops[2].op == WL_PLAN_OP_JOIN &&
            rp->ops[2].key_count == 1 &&
            !strcmp(rp->ops[2].left_keys[0], "col4"));
        f->saved_ops = rp->ops;
        f->saved_count = rp->op_count;
        for (uint32_t i = 0; i < 4; i++) edited_ops[i] = rp->ops[i];
        edited_ops[2].left_keys = edit_string_keys;
        rp->ops = edited_ops;
        plan_edit = 0;
    }
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
        plan_edit = 0;
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
    wirelog_column_type_t types[MAX_COLS];
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
    for (uint32_t c = 0; r->nrows && r->column_types && c < r->ncols; c++)
        m->types[c] = r->column_types[c];
    for (uint32_t i = 0; i < r->nrows; i++)
        for (uint32_t c = 0; c < r->ncols; c++)
            m->rows[i][c] = col_rel_get(r, i, c);
    qsort(m->rows, m->count, sizeof(m->rows[0]), row_cmp);
}

static bool
image_equal(const image_t *a, const image_t *b)
{
    return a->count == b->count && a->width == b->width &&
           !memcmp(a->types, b->types, sizeof(a->types)) &&
           !memcmp(a->rows, b->rows, a->count * sizeof(a->rows[0]));
}

/* ---- hooks --------------------------------------------------------------- */

static uint64_t checkpoints;
static uint64_t stop_at; /* 1-based checkpoint ordinal; 0 disables. */
static int stop_mode;    /* 1 cancel, 2 work limit. */
static int stop_child;   /* Count a child's checkpoints instead: 1 REDUCE,
                          * 2 JOIN. */
static bool stop_hit;
static uint64_t reduce_checkpoints, join_checkpoints;
/* JOIN checkpoints before the scan of a cold right input began. */
static uint64_t joins_before_right;
/* JOIN attempt allocations made before the scan of a cold right input
 * began, latched at its first checkpoint while counting down from a bound
 * that no run reaches. */
#define JOIN_ALLOCATION_BOUND 1000000000
static int allocations_before_right = -1;

static wl_columnar_cold_scan_attempt_phase_t last_phase;
static bool right_scan_seen; /* A checkpoint of a JOIN right input's scan. */
static uint64_t right_scans; /* Scans of a JOIN right input that ended. */
/* JOIN attempts started; at the start of number `join_latch`, the JOIN
 * checkpoints and the JOIN attempt allocations made before it. */
static uint64_t join_starts, join_latch, joins_before_latch;
static int allocations_before_latch = -1;

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
static uint64_t decode_work; /* Prepaid resolution work, seen at SCRATCH. */

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
    if (a->right && !right_scan_seen) {
        joins_before_right = join_checkpoints;
        if (wl_columnar_join_test_attempt_fail_allocation >= 0)
            allocations_before_right = JOIN_ALLOCATION_BOUND -
                wl_columnar_join_test_attempt_fail_allocation;
    }
    right_scan_seen |= a->right;
    right_scans += a->right &&
        a->phase == WL_COLUMNAR_COLD_SCAN_ATTEMPT_WRITER_END;
    if (!stop_mode) {
        cell_bound(control, a);
        resolve_bound(control, a);
    }
    if (a->phase == WL_COLUMNAR_COLD_SCAN_ATTEMPT_SCRATCH)
        decode_work = a->decode_work;
    if (++checkpoints != stop_at || !stop_mode || stop_child) return;
    stop_hit = true;
    if (stop_mode == 1) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge(control, UINT64_MAX) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
}

/* The controlled REDUCE that consumes the scan's output (#2301). */
static void
reduce_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)phase;
    (void)progress;
    if (++reduce_checkpoints != stop_at || !stop_mode || stop_child != 1)
        return;
    stop_hit = true;
    if (stop_mode == 1) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge(control, UINT64_MAX) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
}

/* The controlled JOIN that consumes the scan's output (#2301). Phase 3 is
 * passed once by each JOIN attempt, as it starts. */
static void
join_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase == 3 && ++join_starts == join_latch) {
        joins_before_latch = join_checkpoints;
        if (wl_columnar_join_test_attempt_fail_allocation >= 0)
            allocations_before_latch = JOIN_ALLOCATION_BOUND -
                wl_columnar_join_test_attempt_fail_allocation;
    }
    if (++join_checkpoints != stop_at || !stop_mode || stop_child != 2)
        return;
    stop_hit = true;
    if (stop_mode == 1) wl_evaluation_control_request_cancel(control);
    else CHECK(wl_evaluation_control_charge(control, UINT64_MAX) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
}

static void
hooks_clear(void)
{
    checkpoints = 0;
    reduce_checkpoints = 0;
    join_checkpoints = 0;
    joins_before_right = 0;
    decode_work = 0;
    stop_at = 0;
    stop_mode = 0;
    stop_hit = false;
    last_phase = WL_COLUMNAR_COLD_SCAN_ATTEMPT_INERT;
    resolve_seen = false;
    right_scan_seen = false;
    right_scans = 0;
    join_starts = 0;
    joins_before_latch = 0;
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
    int edit;             /* 1 FILTER after a permuting MAP, 2 a permuting
                           * MAP before the REDUCE, 3 before the JOIN. */
    const char *head;
    const char *head2; /* A second head to compare, or NULL. */
    const char *right; /* A resident input filled in both sessions. */
    uint32_t right_width, right_rows;
    uint32_t right_fan; /* Nonzero: right row i is (i % fan, i). */
    bool cold_right; /* A JOIN of the rule, after the left input's
                      * MAP/FILTER prefix if any, reads the cold input as
                      * its right input; `right`, if set, is then the
                      * rule's resident left. */
    uint32_t cold_fan; /* Nonzero: each JOIN key of the left input matches
                        * this many cold rows, and the COUNT counts them. */
    bool skew; /* Odd rows of `right` match the cold rows on the first JOIN
                * key only; see fill_fan. */
    int later; /* The rule's last JOIN, after the one that reads the cold
                * input, has two keys; each head row's second column is
                * then even (1) or equal to its first (2). */
    bool second; /* The JOIN that reads the cold right input comes after
                  * one or more JOINs of the resident left input with
                  * itself. */
    bool twice; /* Two JOINs of the rule or more read the cold input as
                 * their right input. */
} qcase_t;

static int64_t
cell(uint32_t row, uint32_t col)
{
    return (int64_t)((row * 7u + col * 3u) % 13u);
}

/* With skew, row i holds cell(i, 0), then cell(i, 1) + i % 2 modulo 13,
 * then i in every further column. A generated cold row j holds a, a + 3,
 * a + 6 and so on modulo 13, with a = cell(j, 0), so an even row i matches
 * the cold rows with its first column on two keys, an odd one on the first
 * key only. */
static void
fill_fan(fixture_t *f, const char *name, uint32_t width, uint32_t rows,
    uint32_t fan, bool skew, bool both)
{
    int64_t row[MAX_COLS];
    for (uint32_t i = 0; i < rows; i++) {
        for (uint32_t c = 0; c < width; c++)
            row[c] = fan ? (c ? (int64_t)i : (int64_t)(i % fan)) :
                !skew || !c ? cell(i, c) :
                c == 1 ? (cell(i, 1) + (int64_t)(i % 2)) % 13 : (int64_t)i;
        CHECK(!wl_session_insert(f->s, name, row, 1, width));
        if (both) CHECK(!wl_session_insert(f->raw, name, row, 1, width));
    }
}

static void
fill(fixture_t *f, const char *name, uint32_t width, uint32_t rows,
    bool both)
{
    fill_fan(f, name, width, rows, 0, false, both);
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
    if (c->right)
        fill_fan(f, c->right, c->right_width, c->right_rows, c->right_fan,
            c->skew, true);
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
    {"projection", PROJECTION, "c", 2, 20, "VFMC", 0, "out", NULL},
    {"two_filters", THREE "out(z,x) :- c(x,y,z), z < 9, x != 4.\n", "c", 3,
     20, "VFFMC", 0, "out", NULL},
    {"filter_after_map", THREE "out(z,x) :- c(x,y,z), z < 9.\n", "c", 3, 20,
     "VMFMC", 1, "out", NULL},
    {"empty", PROJECTION, "c", 2, 0, "VFMC", 0, "out", NULL},
    {"all_filtered", ".decl c(x:int64,y:int64)\n.decl out(y:int64)\n"
     "out(y) :- c(x,y), x > 100.\n", "c", 2, 20, "VFMC", 0, "out", NULL},
    {"wide", NULL, "c", 70, 4, "VFMC", 0, "out", NULL},
    {"wide_two_filters", NULL, "c", 70, 4, "VFFMC", 0, "out", NULL},
    {"strings", ".decl c(x:int64,s:symbol)\n.decl out(s:symbol,x:int64)\n"
     "c(1,\"a\"). c(2,\"b\"). c(3,\"c\").\n"
     "out(s,x) :- c(x,s), x > 1.\n", "c", 0, 0, "VFMC", 0, "out", NULL},
    {"second_stratum", ".decl c(x:int64,y:int64)\n"
     ".decl out(y:int64,x:int64)\n.decl top(x:int64)\n"
     "out(y,x) :- c(x,y), x > 1.\ntop(x) :- out(y,x).\n", "c", 2,
     20, "VFMC", 0, "out", "top"},
#define AGG ".decl c(x:int64,y:int64)\n"
    {"count_grouped", AGG ".decl out(x:int64,n:int64)\n"
     "out(x,count(y)) :- c(x,y).\n", "c", 2, 20, "VRC", 0, "out", NULL},
    {"sum_filtered", AGG ".decl out(x:int64,n:int64)\n"
     "out(x,sum(y)) :- c(x,y), x > 1.\n", "c", 2, 20, "VFRC", 0, "out", NULL},
    {"min_grouped", AGG ".decl out(x:int64,n:int64)\n"
     "out(x,min(y)) :- c(x,y).\n", "c", 2, 20, "VRC", 0, "out", NULL},
    {"max_swapped", AGG ".decl out(n:int64,y:int64)\n"
     "out(max(x),y) :- c(x,y).\n", "c", 2, 20, "VRC", 0, "out", NULL},
    {"count_scalar", AGG ".decl out(n:int64)\n"
     "out(count(y)) :- c(x,y).\n", "c", 2, 20, "VRC", 0, "out", NULL},
    {"sum_scalar_empty", AGG ".decl out(n:int64)\n"
     "out(sum(y)) :- c(x,y).\n", "c", 2, 0, "VRC", 0, "out", NULL},
    {"sum_few", AGG ".decl out(x:int64,n:int64)\n"
     "out(x,sum(y)) :- c(x,y), x > 1.\n", "c", 2, 6, "VFRC", 0, "out", NULL},
    {"count_few", AGG ".decl out(n:int64)\n"
     "out(count(y)) :- c(x,y).\n", "c", 2, 6, "VRC", 0, "out", NULL},
    {"count_many", AGG ".decl out(n:int64)\n"
     "out(count(y)) :- c(x,y).\n", "c", 2, 200, "VRC", 0, "out", NULL},
    {"string_key", ".decl c(s:symbol,y:int64)\n.decl out(s:symbol,n:int64)\n"
     "c(\"a\",1). c(\"b\",2). c(\"a\",3).\n"
     "out(s,sum(y)) :- c(s,y).\n", "c", 0, 0, "VRC", 0, "out", NULL},
    {"filter_reduce", THREE "out(z,count(x)) :- c(x,y,z), y > 2.\n",
     "c", 3, 20, "VFRC", 0, "out", NULL},
    {"map_before_reduce", AGG ".decl out(x:int64,n:int64)\n"
     "out(x,sum(y)) :- c(x,y).\n", "c", 2, 20, "VMRC", 2, "out", NULL},
    {"count_all_filtered", AGG ".decl out(n:int64)\n"
     "out(count(y)) :- c(x,y), x > 100.\n", "c", 2, 20, "VFRC", 0, "out",
     NULL},
#define JOIN2 ".decl c(x:int64,y:int64)\n.decl r(y:int64,z:int64)\n"
#define JOIN3 ".decl c(x:int64,y:int64,w:int64)\n.decl r(y:int64,z:int64)\n"
    {"pair_join", JOIN2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- c(x,y), r(y,z).\n", "c", 2, 20, "VJC", 0, "out", NULL,
     "r", 2, 20},
    {"filter_join", JOIN3 ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y,w), w = 7, r(y,z).\n", "c", 3, 20, "VFJFMC", 0, "out",
     NULL, "r", 2, 20},
    {"string_join", ".decl c(x:int64,s:symbol)\n.decl r(s:symbol,z:int64)\n"
     ".decl out(x:int64,z:int64)\n"
     "c(1,\"a\"). c(2,\"b\"). c(3,\"a\"). c(4,\"d\").\n"
     "r(\"a\",10). r(\"b\",20). r(\"a\",30).\n"
     "out(x,z) :- c(x,s), r(s,z).\n", "c", 0, 0, "VJC", 0, "out", NULL},
    {"empty_right", JOIN2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- c(x,y), r(y,z).\n", "c", 2, 20, "VJC", 0, "out", NULL,
     "r", 2, 0},
    {"all_filtered_join", JOIN3 ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y,w), w = 100, r(y,z).\n", "c", 3, 20, "VFJFMC", 0,
     "out", NULL, "r", 2, 20},
    {"pair_few", JOIN2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- c(x,y), r(y,z).\n", "c", 2, 6, "VJC", 0, "out", NULL,
     "r", 2, 13},
    {"filter_few", JOIN3 ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y,w), w = 7, r(y,z).\n", "c", 3, 6, "VFJFMC", 0, "out",
     NULL, "r", 2, 13},
    {"map_join", JOIN2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- c(x,y), r(y,z).\n", "c", 2, 20, "VMJC", 3, "out", NULL,
     "r", 2, 20},
    {"pair_many", JOIN2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- c(x,y), r(y,z).\n", "c", 2, 200, "VJC", 0, "out", NULL,
     "r", 2, 2},
    {"count_join", JOIN2 ".decl out(x:int64,n:int64)\n"
     "out(x,count(z)) :- c(x,y), r(y,z).\n", "c", 2, 20, "VJRC", 0, "out",
     NULL, "r", 2, 20},
    {"count_join_few", JOIN2 ".decl out(x:int64,n:int64)\n"
     "out(x,count(z)) :- c(x,y), r(y,z).\n", "c", 2, 6, "VJRC", 0, "out",
     NULL, "r", 2, 13},
    {"fanout_count", JOIN2 ".decl out(x:int64,n:int64)\n"
     "out(x,count(y)) :- c(x,y), r(y,z).\n", "c", 2, 20, "VJRC", 0, "out",
     NULL, "r", 2, 39, 13},
    {"min_join_filtered", JOIN3 ".decl out(x:int64,n:int64)\n"
     "out(x,min(z)) :- c(x,y,w), w = 7, r(y,z).\n", "c", 3, 20, "VFJFRC", 0,
     "out", NULL, "r", 2, 20},
    {"count_wide_join", ".decl c(x:int64,y:int64,a:int64,b:int64)\n"
     ".decl r(y:int64,z:int64)\n.decl out(x:int64,n:int64)\n"
     "out(x,count(z)) :- c(x,y,a,b), r(y,z).\n", "c", 4, 10, "VJRC", 0,
     "out", NULL, "r", 2, 20},
    {"max_join_post_filter", JOIN2 ".decl out(x:int64,n:int64)\n"
     "out(x,max(z)) :- c(x,y), r(y,z), z > 3.\n", "c", 2, 20, "VJFRC", 0,
     "out", NULL, "r", 2, 20},
    {"count_scalar_empty_right", JOIN2 ".decl out(n:int64)\n"
     "out(count(z)) :- c(x,y), r(y,z).\n", "c", 2, 20, "VJRC", 0, "out",
     NULL, "r", 2, 0},
    {"max_post_filter_absent", JOIN2 ".decl out(x:int64,n:int64)\n"
     "out(x,max(z)) :- c(x,y), r(y,z), z > 3.\n", "c", 2, 20, "VJFRC", 0,
     "out", NULL, "r", 2, 0},
    {"min_join_absent", JOIN3 ".decl out(x:int64,n:int64)\n"
     "out(x,min(z)) :- c(x,y,w), w = 7, r(y,z).\n", "c", 3, 20, "VFJFRC", 0,
     "out", NULL, "r", 2, 0},
    {"string_group_absent", ".decl c(x:int64,y:int64)\n"
     ".decl r(y:int64,s:symbol)\n.decl out(s:symbol,n:int64)\n"
     "out(s,count(x)) :- c(x,y), r(y,s).\n", "c", 2, 20, "VJRC", 0, "out",
     NULL, "r", 2, 0},
    {"string_group_join", ".decl c(x:int64,s:symbol)\n"
     ".decl r(s:symbol,z:int64)\n.decl out(s:symbol,n:int64)\n"
     "c(1,\"a\"). c(2,\"b\"). c(3,\"a\"). c(4,\"d\").\n"
     "r(\"a\",10). r(\"b\",20). r(\"a\",30).\n"
     "out(s,count(z)) :- c(x,s), r(s,z).\n", "c", 0, 0, "VJRC", 0, "out",
     NULL},
#define KEYS2 ".decl c(x:int64,y:int64)\n.decl r(x:int64,y:int64,z:int64)\n"
#define KEYS3 ".decl c(x:int64,y:int64,w:int64)\n" \
        ".decl r(x:int64,y:int64,z:int64)\n"
    {"composite_join", KEYS2 ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y), r(x,y,z).\n", "c", 2, 20, "VJC", 0, "out", NULL,
     "r", 3, 20, .skew = true},
    {"composite_few", KEYS2 ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y), r(x,y,z).\n", "c", 2, 6, "VJC", 0, "out", NULL,
     "r", 3, 13, .skew = true},
    {"composite_filter_after", KEYS2 ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y), r(x,y,z), z > 3.\n", "c", 2, 20, "VJFMC", 0,
     "out", NULL, "r", 3, 20, .skew = true},
    {"composite_scan_prefix", KEYS3 ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y,w), w = 7, r(x,y,z).\n", "c", 3, 20, "VFJFMC", 0,
     "out", NULL, "r", 3, 20, .skew = true},
    {"composite_count", KEYS2 ".decl out(x:int64,n:int64)\n"
     "out(x,count(z)) :- c(x,y), r(x,y,z).\n", "c", 2, 20, "VJRC", 0, "out",
     NULL, "r", 3, 20, .skew = true},
    {"composite_count_few", KEYS2 ".decl out(x:int64,n:int64)\n"
     "out(x,count(z)) :- c(x,y), r(x,y,z).\n", "c", 2, 6, "VJRC", 0, "out",
     NULL, "r", 3, 13, .skew = true},
    {"composite_max_prefix", KEYS3 ".decl out(x:int64,n:int64)\n"
     "out(x,max(z)) :- c(x,y,w), w = 7, r(x,y,z).\n", "c", 3, 20, "VFJFRC",
     0, "out", NULL, "r", 3, 20, .skew = true},
    {"composite_empty_right", KEYS2 ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y), r(x,y,z).\n", "c", 2, 20, "VJC", 0, "out", NULL,
     "r", 3, 0, .skew = true},
    {"composite_then_join", KEYS2 ".decl out(x:int64,u:int64)\n"
     "out(x,u) :- c(x,y), r(x,y,v), r(v,z,u).\n", "c", 2, 20, "VJJC", 0,
     "out", NULL, "r", 3, 20, .skew = true},
    /* A later JOIN with two keys. The JOIN on x alone or on its second
     * key alone would also pair a row of `r` with a row of the other
     * parity, whose last column differs. */
    {"later_keys_join", KEYS2 ".decl out(v:int64,u:int64)\n"
     "out(v,u) :- c(x,y), r(x,w,v), r(x,w,u).\n", "c", 2, 20, "VJJC", 0,
     "out", NULL, "r", 3, 20, .skew = true, .later = 2},
    {"later_keys_few", KEYS2 ".decl out(v:int64,u:int64)\n"
     "out(v,u) :- c(x,y), r(x,w,v), r(x,w,u).\n", "c", 2, 6, "VJJC", 0,
     "out", NULL, "r", 3, 13, .skew = true, .later = 2},
    {"later_keys_scan_prefix", KEYS3 ".decl out(v:int64,u:int64)\n"
     "out(v,u) :- c(x,y,w), w = 7, r(x,p,v), r(x,p,u).\n", "c", 3, 20,
     "VFJJFMC", 0, "out", NULL, "r", 3, 20, .skew = true, .later = 2},
    {"later_keys_empty_right", KEYS2 ".decl out(v:int64,u:int64)\n"
     "out(v,u) :- c(x,y), r(x,w,v), r(x,w,u).\n", "c", 2, 20, "VJJC", 0,
     "out", NULL, "r", 3, 0, .skew = true, .later = 2},
    {"composite_later_keys", KEYS2 ".decl out(v:int64,u:int64)\n"
     "out(v,u) :- c(x,y), r(x,y,v), r(x,y,u).\n", "c", 2, 20, "VJJC", 0,
     "out", NULL, "r", 3, 20, .skew = true, .later = 2},
    /* The later JOIN's first key is the cold input's second column. */
    {"later_cold_key", ".decl c(x:int64,y:int64)\n"
     ".decl r(a:int64,b:int64,c:int64)\n.decl out(v:int64,u:int64)\n"
     "c(1,2). c(3,5).\n"
     "r(2,5,7). r(5,2,7). r(5,3,8). r(6,2,9). r(5,4,6).\n"
     "out(v,u) :- c(x,y), r(y,w,v), r(w,y,u).\n", "c", 0, 0, "VJJC", 0,
     "out", NULL, .later = 2},
#define PERMUTED \
        ".decl c(x:int64,s:symbol)\n.decl r(s:symbol,x:int64,z:int64)\n" \
        "c(1,\"a\"). c(1,\"b\"). c(2,\"a\"). c(3,\"c\").\n" \
        "r(\"a\",1,10). r(\"b\",2,20). r(\"a\",2,30). r(\"c\",3,40).\n" \
        "r(\"b\",1,50). r(\"c\",1,60).\n"
    {"composite_permuted", PERMUTED ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,s), r(s,x,z).\n", "c", 0, 0, "VJC", 0, "out", NULL},
    {"composite_permuted_count", PERMUTED ".decl out(s:symbol,n:int64)\n"
     "out(s,count(z)) :- c(x,s), r(s,x,z).\n", "c", 0, 0, "VJRC", 0, "out",
     NULL},
    {"composite_permuted_later", PERMUTED ".decl out(z:int64,u:int64)\n"
     "out(z,u) :- c(x,s), r(s,x,z), r(s,x,u).\n", "c", 0, 0, "VJJC", 0,
     "out", NULL, .later = 2},
#define RIGHT2 ".decl a(x:int64,y:int64)\n.decl c(y:int64,z:int64)\n"
    {"right_pair", RIGHT2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- a(x,y), c(y,z).\n", "c", 2, 20, "VJC", 0, "out", NULL,
     "a", 2, 20, 0, true},
    {"right_project", RIGHT2 ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y), c(y,z).\n", "c", 2, 20, "VJC", 0, "out", NULL,
     "a", 2, 20, 0, true},
    {"right_few", RIGHT2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- a(x,y), c(y,z).\n", "c", 2, 6, "VJC", 0, "out", NULL,
     "a", 2, 13, 0, true},
    {"right_many", RIGHT2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- a(x,y), c(y,z).\n", "c", 2, 200, "VJC", 0, "out", NULL,
     "a", 2, 20, 0, true},
    {"right_wide", ".decl a(x:int64,y:int64)\n"
     ".decl c(y:int64,p:int64,q:int64,z:int64)\n"
     ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y), c(y,p,q,z).\n", "c", 4, 10, "VJC", 0, "out", NULL,
     "a", 2, 20, 0, true},
    {"right_left_absent", RIGHT2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- a(x,y), c(y,z).\n", "c", 2, 20, "VJC", 0, "out", NULL,
     "a", 2, 0, 0, true},
    {"right_string", ".decl a(x:int64,s:symbol)\n"
     ".decl c(s:symbol,z:int64)\n.decl out(x:int64,z:int64)\n"
     "a(1,\"a\"). a(2,\"b\"). a(3,\"a\"). a(4,\"d\").\n"
     "c(\"a\",10). c(\"b\",20). c(\"a\",30).\n"
     "out(x,z) :- a(x,s), c(s,z).\n", "c", 0, 0, "VJC", 0, "out", NULL,
     NULL, 0, 0, 0, true},
#define RIGHT_AGG RIGHT2 ".decl out(x:int64,n:int64)\n"
    {"right_count", RIGHT_AGG "out(x,count(z)) :- a(x,y), c(y,z).\n", "c",
     2, 20, "VJRC", 0, "out", NULL, "a", 2, 20, 0, true},
    {"right_count_few", RIGHT_AGG "out(x,count(z)) :- a(x,y), c(y,z).\n",
     "c", 2, 6, "VJRC", 0, "out", NULL, "a", 2, 13, 0, true},
    {"right_count_scalar", RIGHT2 ".decl out(n:int64)\n"
     "out(count(z)) :- a(x,y), c(y,z).\n", "c", 2, 20, "VJRC", 0, "out",
     NULL, "a", 2, 20, 0, true},
    {"right_min", RIGHT_AGG "out(x,min(z)) :- a(x,y), c(y,z).\n", "c", 2,
     20, "VJRC", 0, "out", NULL, "a", 2, 20, 0, true},
    {"right_max_filter", RIGHT_AGG
     "out(x,max(z)) :- a(x,y), c(y,z), z > 3.\n", "c", 2, 20, "VJFRC", 0,
     "out", NULL, "a", 2, 20, 0, true},
    {"right_fanout", RIGHT_AGG "out(x,count(z)) :- a(x,y), c(y,z).\n", "c",
     2, 39, "VJRC", 0, "out", NULL, "a", 2, 20, 0, true, 3},
    {"right_count_absent_left", RIGHT_AGG
     "out(x,count(z)) :- a(x,y), c(y,z).\n", "c", 2, 20, "VJRC", 0, "out",
     NULL, "a", 2, 0, 0, true},
    {"right_max_absent_left", RIGHT_AGG
     "out(x,max(z)) :- a(x,y), c(y,z).\n", "c", 2, 20, "VJRC", 0, "out",
     NULL, "a", 2, 0, 0, true},
    {"right_string_group", ".decl a(x:int64,k:int64)\n"
     ".decl c(k:int64,s:symbol)\n.decl out(s:symbol,n:int64)\n"
     "a(1,1). a(2,2). a(3,1). a(4,4).\n"
     "c(1,\"p\"). c(2,\"q\"). c(1,\"r\"). c(5,\"p\").\n"
     "out(s,count(x)) :- a(x,k), c(k,s).\n", "c", 0, 0, "VJRC", 0, "out",
     NULL, NULL, 0, 0, 0, true},
#define RIGHT_KEYS ".decl a(x:int64,y:int64)\n" \
        ".decl c(x:int64,y:int64,z:int64)\n"
    {"right_composite", RIGHT_KEYS ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y), c(x,y,z).\n", "c", 3, 20, "VJC", 0, "out", NULL,
     "a", 2, 20, 0, true, .skew = true},
    {"right_composite_few", RIGHT_KEYS ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y), c(x,y,z).\n", "c", 3, 6, "VJC", 0, "out", NULL,
     "a", 2, 13, 0, true, .skew = true},
    {"right_composite_filter_after", RIGHT_KEYS ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y), c(x,y,z), z > 3.\n", "c", 3, 20, "VJFMC", 0,
     "out", NULL, "a", 2, 20, 0, true, .skew = true},
    {"right_composite_count", RIGHT_KEYS ".decl out(x:int64,n:int64)\n"
     "out(x,count(z)) :- a(x,y), c(x,y,z).\n", "c", 3, 20, "VJRC", 0, "out",
     NULL, "a", 2, 20, 0, true, .skew = true},
    {"right_composite_max", RIGHT_KEYS ".decl out(x:int64,n:int64)\n"
     "out(x,max(z)) :- a(x,y), c(x,y,z).\n", "c", 3, 20, "VJRC", 0, "out",
     NULL, "a", 2, 20, 0, true, .skew = true},
    {"right_composite_left_absent", RIGHT_KEYS ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y), c(x,y,z).\n", "c", 3, 20, "VJC", 0, "out", NULL,
     "a", 2, 0, 0, true, .skew = true},
    {"right_composite_then_join", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(x:int64,y:int64,z:int64)\n.decl out(x:int64,u:int64)\n"
     "out(x,u) :- a(x,y,w), c(x,y,z), a(z,v,u).\n", "c", 3, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .skew = true},
#define RIGHT_LATER ".decl a(x:int64,y:int64,w:int64)\n" \
        ".decl c(x:int64,y:int64,z:int64)\n.decl out(x:int64,u:int64)\n"
    /* A later JOIN with two keys: the cold rows give p, which only the
     * even rows of `a` hold beside x. */
    {"right_later_keys", RIGHT_LATER
     "out(x,u) :- a(x,y,w), c(x,p,q), a(x,p,u).\n", "c", 3, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .skew = true, .later = 1},
    {"right_later_keys_few", RIGHT_LATER
     "out(x,u) :- a(x,y,w), c(x,p,q), a(x,p,u).\n", "c", 3, 6, "VJJC", 0,
     "out", NULL, "a", 3, 13, 0, true, .skew = true, .later = 1},
    {"right_later_keys_left_absent", RIGHT_LATER
     "out(x,u) :- a(x,y,w), c(x,p,q), a(x,p,u).\n", "c", 3, 20, "VJJC", 0,
     "out", NULL, "a", 3, 0, 0, true, .skew = true, .later = 1},
    {"right_composite_later_keys", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(x:int64,y:int64,z:int64)\n.decl out(w:int64,u:int64)\n"
     "out(w,u) :- a(x,y,w), c(x,y,z), a(x,y,u).\n", "c", 3, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .skew = true, .later = 2},
    /* The later JOIN's second key comes from the cold input's last column. */
    {"right_composite_cold_key", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(x:int64,y:int64,z:int64)\n.decl out(w:int64,u:int64)\n"
     "a(1,2,7). a(7,2,7). a(7,4,8). a(9,2,9).\n"
     "c(1,2,7).\n"
     "out(w,u) :- a(x,y,w), c(x,y,z), a(z,y,u).\n", "c", 0, 0, "VJJC", 0,
     "out", NULL, NULL, 0, 0, 0, true, .later = 2},
    {"right_composite_permuted", ".decl a(x:int64,s:symbol)\n"
     ".decl c(s:symbol,x:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "a(1,\"a\"). a(1,\"b\"). a(2,\"a\"). a(3,\"c\").\n"
     "c(\"a\",1,10). c(\"b\",2,20). c(\"a\",2,30). c(\"c\",3,40).\n"
     "c(\"b\",1,50). c(\"c\",1,60).\n"
     "out(x,z) :- a(x,s), c(s,x,z).\n", "c", 0, 0, "VJC", 0, "out", NULL,
     NULL, 0, 0, 0, true},
    /* A cold right input after the left input's FILTER. The generated
     * cold row j is (7j % 13, (7j + 3) % 13), and the left rows that pass
     * the FILTER have keys that 2, 1, 0 and 1 cold rows hold, or none. */
#define RIGHT_PREFIX ".decl a(x:int64,y:int64,w:int64)\n" \
        ".decl c(y:int64,z:int64)\n" \
        "a(1,0,7). a(2,7,7). a(3,5,7). a(4,1,8). a(5,12,7). a(6,99,7).\n"
    {"right_after_filter_join", RIGHT_PREFIX ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), w = 7, c(y,z).\n", "c", 2, 20, "VFJFMC", 0,
     "out", NULL, NULL, 0, 0, 0, true},
    {"right_after_filter_few", RIGHT_PREFIX ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), w = 7, c(y,z).\n", "c", 2, 6, "VFJFMC", 0,
     "out", NULL, NULL, 0, 0, 0, true},
    {"right_after_filter_wide", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(y:int64,p:int64,q:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "a(1,0,7). a(2,7,7). a(3,5,7). a(4,1,8).\n"
     "out(x,z) :- a(x,y,w), w = 7, c(y,p,q,z).\n", "c", 4, 10, "VFJFMC", 0,
     "out", NULL, NULL, 0, 0, 0, true},
    {"right_after_filter_count", RIGHT_PREFIX ".decl out(x:int64,n:int64)\n"
     "out(x,count(z)) :- a(x,y,w), w = 7, c(y,z).\n", "c", 2, 20, "VFJFRC",
     0, "out", NULL, NULL, 0, 0, 0, true},
    {"right_after_filter_then_join", RIGHT_PREFIX
     ".decl out(x:int64,u:int64)\n"
     "out(x,u) :- a(x,y,w), w = 7, c(y,z), a(z,v,u).\n", "c", 2, 20,
     "VFJJFMC", 0, "out", NULL, NULL, 0, 0, 0, true},
    /* A permuting MAP before the JOIN, from an edited plan: the copy keeps
     * the cold input's columns in their order. */
    {"right_after_map", RIGHT2 ".decl out(x:int64,y:int64,z:int64)\n"
     "out(x,y,z) :- a(x,y), c(y,z).\n", "c", 2, 20, "VMJC", 3, "out", NULL,
     "a", 2, 20, 0, true},
    /* Two keys after the FILTER: the cold row j holds (7j % 13,
     * (7j + 3) % 13, (7j + 6) % 13), and a(4,9,7) matches the cold row
     * (4,7,10) on its first key only. */
    {"right_composite_after_filter", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(x:int64,y:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "a(1,8,7). a(7,10,7). a(4,9,7). a(2,9,8).\n"
     "out(x,z) :- a(x,y,w), w = 7, c(x,y,z).\n", "c", 3, 20, "VFJFMC", 0,
     "out", NULL, NULL, 0, 0, 0, true},
    /* A cold right input read by a JOIN after one of the resident input
     * with itself. A generated resident row i is (7i % 13, (7i + 3) % 13,
     * (7i + 6) % 13), so over 13 rows or more every key of the first JOIN
     * finds a row, and every key of the cold JOIN one of the cold rows. */
#define RIGHT_SECOND ".decl a(x:int64,y:int64,w:int64)\n" \
        ".decl c(v:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
    {"right_second_self", RIGHT_SECOND
     "out(x,z) :- a(x,y,w), a(y,v,u), c(v,z).\n", "c", 2, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .second = true},
    {"right_second_few", RIGHT_SECOND
     "out(x,z) :- a(x,y,w), a(y,v,u), c(v,z).\n", "c", 2, 6, "VJJC", 0,
     "out", NULL, "a", 3, 13, 0, true, .second = true},
    /* The cold JOIN's key from the first JOIN's left input. */
    {"right_second_left_key", RIGHT_SECOND
     "out(x,z) :- a(x,y,w), a(y,v,u), c(w,z).\n", "c", 2, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .second = true},
    /* Resident row 2 passes the FILTER, row 8 extends it and cold row 1
     * matches. */
    {"right_second_after_filter", RIGHT_SECOND
     "out(x,z) :- a(x,y,w), w = 7, a(y,v,u), c(v,z).\n", "c", 2, 20, "VFJJFMC",
     0, "out", NULL, "a", 3, 20, 0, true, .second = true},
    {"right_second_third", RIGHT_SECOND
     "out(x,z) :- a(x,y,w), a(y,v,u), a(v,p,q), c(p,z).\n", "c", 2, 20,
     "VJJJC", 0, "out", NULL, "a", 3, 20, 0, true, .second = true},
    {"right_second_then_join", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,z:int64)\n.decl out(x:int64,q:int64)\n"
     "out(x,q) :- a(x,y,w), a(y,v,u), c(v,z), a(z,p,q).\n", "c", 2, 20,
     "VJJJC",
     0, "out", NULL, "a", 3, 20, 0, true, .second = true},
    /* Two keys at the cold JOIN: the cold row j holds (7j % 13,
    * (7j + 3) % 13, (7j + 6) % 13), as do the resident rows. */
    {"right_second_composite", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,u:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), a(y,v,u), c(v,u,z).\n", "c", 3, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .second = true},
    {"right_second_composite_few", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,u:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), a(y,v,u), c(v,u,z).\n", "c", 3, 6, "VJJC", 0,
     "out", NULL, "a", 3, 13, 0, true, .second = true},
    {"right_second_composite_filter_after",
     ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,u:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), a(y,v,u), c(v,u,z), z > 3.\n", "c", 3, 20,
     "VJJFMC",
     0, "out", NULL, "a", 3, 20, 0, true, .second = true},
    {"right_second_string", ".decl a(x:int64,y:int64,s:symbol)\n"
     ".decl c(s:symbol,z:int64)\n.decl out(x:int64,z:int64)\n"
     "a(1,2,\"p\"). a(2,3,\"q\"). a(3,1,\"p\"). a(2,5,\"r\").\n"
     "c(\"q\",10). c(\"p\",20). c(\"r\",30). c(\"p\",40).\n"
     "out(x,z) :- a(x,y,s), a(y,v,t), c(t,z).\n", "c", 0, 0, "VJJC", 0, "out",
     NULL, NULL, 0, 0, 0, true, .second = true},
    /* A constant in a resident JOIN's right input, which the planner emits
     * as a right filter of that JOIN. Only resident row 9, (11, 1, 4), has
     * a first JOIN partner (1, 4, 7) whose second column is 4, and cold row
     * 1, (7, 10), extends it. */
    {"right_second_filter", RIGHT_SECOND
     "out(x,z) :- a(x,y,w), a(y,4,u), c(u,z).\n", "c", 2, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .second = true},
    {"right_second_filter_few", RIGHT_SECOND
     "out(x,z) :- a(x,y,w), a(y,4,u), c(u,z).\n", "c", 2, 6, "VJJC", 0,
     "out", NULL, "a", 3, 13, 0, true, .second = true},
    /* No resident row has 99 in its second column: the earlier JOIN's
     * output is empty, with its columns. */
    {"right_second_all_filtered", RIGHT_SECOND
     "out(x,z) :- a(x,y,w), a(y,99,u), c(u,z).\n", "c", 2, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .second = true},
    /* Two filtered JOINs: (11, 1, 4), (1, 4, 7), (7, 10, 0), cold (0, 3). */
    {"right_second_filter_third", RIGHT_SECOND
     "out(x,z) :- a(x,y,w), a(y,4,u), a(u,10,t), c(t,z).\n", "c", 2, 20,
     "VJJJC", 0, "out", NULL, "a", 3, 20, 0, true, .second = true},
    /* A filtered JOIN after the one that reads the cold input. */
    {"right_second_filter_then_join", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,z:int64)\n.decl out(x:int64,t:int64)\n"
     "out(x,t) :- a(x,y,w), a(y,v,u), c(v,z), a(z,3,t).\n", "c", 2, 20,
     "VJJJC", 0, "out", NULL, "a", 3, 20, 0, true, .second = true},
    /* A STRING constant: a(2,3,"q") fails the filter, so (1,10) is not a
     * head row. */
    {"right_second_filter_string", ".decl a(x:int64,y:int64,s:symbol)\n"
     ".decl c(v:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "a(1,2,\"p\"). a(2,3,\"q\"). a(2,4,\"p\"). a(3,1,\"p\").\n"
     "c(3,10). c(4,20). c(1,30).\n"
     "out(x,z) :- a(x,y,s), a(y,v,\"p\"), c(v,z).\n", "c", 0, 0, "VJJC", 0,
     "out", NULL, NULL, 0, 0, 0, true, .second = true},
    /* A cold right input read by two JOINs or more, which read one copy. A
     * generated row with first column r is (r, r + 3) in a two-column
     * input and (r, r + 3, r + 6) in a three-column one, modulo 13, so over
     * 13 rows or more every key finds a row. */
#define RIGHT_TWICE ".decl a(x:int64,y:int64)\n.decl c(y:int64,z:int64)\n" \
        ".decl out(x:int64,u:int64)\n"
#define RIGHT_TWICE3 ".decl a(x:int64,y:int64,w:int64)\n" \
        ".decl c(y:int64,z:int64)\n.decl out(x:int64,u:int64)\n"
    {"right_twice", RIGHT_TWICE "out(x,u) :- a(x,y), c(y,z), c(z,u).\n", "c",
     2, 20, "VJJC", 0, "out", NULL, "a", 2, 20, 0, true, .twice = true},
    /* Over 8 cold rows, cold row 0, (0, 3), has the partner (3, 6). */
    {"right_twice_few", RIGHT_TWICE "out(x,u) :- a(x,y), c(y,z), c(z,u).\n",
     "c", 2, 8, "VJJC", 0, "out", NULL, "a", 2, 13, 0, true, .twice = true},
    /* Resident row 2, (1, 4, 7), passes the FILTER. */
    {"right_twice_after_filter", RIGHT_TWICE3
     "out(x,u) :- a(x,y,w), w = 7, c(y,z), c(z,u).\n", "c", 2, 20,
     "VFJJFMC", 0, "out", NULL, "a", 3, 20, 0, true, .twice = true},
    {"right_twice_second", RIGHT_TWICE3
     "out(x,u) :- a(x,y,w), a(y,v,t), c(v,z), c(z,u).\n", "c", 2, 20,
     "VJJJC", 0, "out", NULL, "a", 3, 20, 0, true, .second = true,
     .twice = true},
    /* A JOIN of the resident input between the two that read the copy. */
    {"right_twice_between", RIGHT_TWICE3
     "out(x,u) :- a(x,y,w), c(y,z), a(z,v,t), c(v,u).\n", "c", 2, 20,
     "VJJJC", 0, "out", NULL, "a", 3, 20, 0, true, .twice = true},
    {"right_thrice", RIGHT_TWICE
     "out(x,u) :- a(x,y), c(y,z), c(z,w), c(w,u).\n", "c", 2, 20, "VJJJC", 0,
     "out", NULL, "a", 2, 20, 0, true, .twice = true},
    /* Two keys at the first JOIN that reads the copy, and at the second. */
    {"right_twice_composite", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(y:int64,z:int64,v:int64)\n.decl out(x:int64,u:int64)\n"
     "out(x,u) :- a(x,y,w), c(y,w,v), c(v,z,u).\n", "c", 3, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .twice = true},
    {"right_twice_keys_second", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(y:int64,z:int64,v:int64)\n.decl out(x:int64,u:int64)\n"
     "out(x,u) :- a(x,y,w), c(y,z,v), c(z,v,u).\n", "c", 3, 20, "VJJC", 0,
     "out", NULL, "a", 3, 20, 0, true, .twice = true},
    /* Resident row 9, (11, 1, 4), has the filtered partner (1, 4, 7). */
    {"right_second_filter_twice", RIGHT_TWICE3
     "out(x,u) :- a(x,y,w), a(y,4,t), c(t,z), c(z,u).\n", "c", 2, 20,
     "VJJJC", 0, "out", NULL, "a", 3, 20, 0, true, .second = true,
     .twice = true},
    /* The second JOIN's two keys: no cold row (z, y) follows a cold row
     * (y, z), since 3 + 3 is not 0 modulo 13. */
    {"right_twice_all_filtered", ".decl a(x:int64,y:int64)\n"
     ".decl c(y:int64,z:int64)\n.decl out(x:int64,y:int64)\n"
     "out(x,y) :- a(x,y), c(y,z), c(z,y).\n", "c", 2, 20, "VJJC", 0, "out",
     NULL, "a", 2, 20, 0, true, .twice = true},
    /* STRING keys at both JOINs: 1 reaches "r" and 2 reaches "p". */
    {"right_twice_string", ".decl a(x:int64,s:symbol)\n"
     ".decl c(s:symbol,t:symbol)\n.decl out(x:int64,u:symbol)\n"
     "a(1,\"p\"). a(2,\"q\").\n"
     "c(\"p\",\"q\"). c(\"q\",\"r\"). c(\"r\",\"p\").\n"
     "out(x,u) :- a(x,s), c(s,t), c(t,u).\n", "c", 0, 0, "VJJC", 0, "out",
     NULL, NULL, 0, 0, 0, true, .twice = true},
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

/* The cold rule's op order: VARIABLE, MAP, FILTER, CONSOLIDATE, REDUCE and
 * JOIN as VMFCRJ, anything else as '?'. */
static void
ops_of(const fixture_t *f, char got[16])
{
    const wl_plan_relation_t *rp = &f->plan->strata[0].relations[0];
    memset(got, 0, 16);
    for (uint32_t i = 0; i < rp->op_count && i < 15; i++) {
        switch (rp->ops[i].op) {
        case WL_PLAN_OP_VARIABLE: got[i] = 'V'; break;
        case WL_PLAN_OP_MAP: got[i] = 'M'; break;
        case WL_PLAN_OP_FILTER: got[i] = 'F'; break;
        case WL_PLAN_OP_CONSOLIDATE: got[i] = 'C'; break;
        case WL_PLAN_OP_REDUCE: got[i] = 'R'; break;
        case WL_PLAN_OP_JOIN: got[i] = 'J'; break;
        default: got[i] = '?'; break;
        }
    }
}

/* The cold rule's op order, as asserted by each case. */
static void
expect_ops(const fixture_t *f, const qcase_t *c)
{
    char got[16];
    ops_of(f, got);
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

/* The resolution prepayment, from the plan, with u = end + 1: each prefix
 * op costs u plus, for a MAP, u and its expression bytes per lane; before a
 * REDUCE every prefix op is counted twice, and the REDUCE costs u, u per
 * group key, u for the operand, and its expression bytes. */
static uint64_t
expected_decode_work(const fixture_t *f, const qcase_t *c)
{
    /* A right input is read whole: its scan decodes no prefix. */
    if (c->cold_right) return 0;
    const wl_plan_relation_t *rp = &f->plan->strata[0].relations[0];
    uint32_t end = 1;
    while (end < rp->op_count && (rp->ops[end].op == WL_PLAN_OP_MAP ||
        rp->ops[end].op == WL_PLAN_OP_FILTER)) end++;
    bool reduce = rp->ops[end].op == WL_PLAN_OP_REDUCE;
    uint64_t unit = 1u + end, total = 0;
    for (uint32_t i = 1; i <= end; i++) {
        const wl_plan_op_t *op = &rp->ops[i];
        if (i == end && !reduce) break;
        uint64_t cost = unit;
        if (op->op == WL_PLAN_OP_MAP) {
            for (uint32_t l = 0; l < op->project_count; l++)
                cost += unit + (l < op->map_expr_count ?
                    op->map_exprs[l].size : 0u);
        } else if (op->op == WL_PLAN_OP_REDUCE) {
            cost += unit * (op->group_by_count + 1u) + op->agg_expr.size;
        }
        total += i == end ? cost : reduce ? 2u * cost : cost;
    }
    return total;
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
    /* The planner's orientation: the cold input is the JOIN's right input
     * exactly in the cases that say so. In the second cases that JOIN
     * follows JOINs of the resident input with itself. */
    const wl_plan_relation_t *rp = &f.plan->strata[0].relations[0];
    uint32_t join = 0;
    while (join < rp->op_count && rp->ops[join].op != WL_PLAN_OP_JOIN) join++;
    for (; c->second && join < rp->op_count &&
        (rp->ops[join].op != WL_PLAN_OP_JOIN ||
        strcmp(rp->ops[join].right_relation, c->cold)); join++)
        CHECK(rp->ops[join].op != WL_PLAN_OP_JOIN ||
            !strcmp(rp->ops[join].right_relation, rp->ops[0].relation_name));
    CHECK(!c->second || join < rp->op_count);
    /* Exactly the right filter cases carry a right filter, on a JOIN of the
     * resident input with itself. */
    bool right_filter = false;
    for (uint32_t i = 0; i < rp->op_count; i++)
        right_filter |= rp->ops[i].op == WL_PLAN_OP_JOIN &&
            rp->ops[i].right_filter_expr.size &&
            !strcmp(rp->ops[i].right_relation, rp->ops[0].relation_name);
    CHECK(right_filter == (!strncmp(c->name, "right_second_filter", 19) ||
        !strcmp(c->name, "right_second_all_filtered")));
    CHECK(join == rp->op_count ||
        (c->cold_right == !strcmp(rp->ops[join].right_relation, c->cold) &&
        c->cold_right == !!strcmp(rp->ops[0].relation_name, c->cold)));
    /* Exactly the twice cases read the cold input at two JOINs or more. */
    uint32_t cold_joins = 0, joins = 0;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        joins += rp->ops[i].op == WL_PLAN_OP_JOIN;
        cold_joins += rp->ops[i].op == WL_PLAN_OP_JOIN &&
            !strcmp(rp->ops[i].right_relation, c->cold);
    }
    CHECK((cold_joins > 1) == c->twice);
    /* The JOIN that reads the cold input has two keys exactly in the
     * composite cases, and the permuted ones pair each left key with a
     * right column of another position. */
    bool composite = strstr(c->name, "composite") != NULL;
    CHECK(join == rp->op_count ||
        (rp->ops[join].key_count == 2) == composite);
    for (uint32_t k = 0; strstr(c->name, "permuted") && k < 2; k++)
        CHECK(strcmp(rp->ops[join].left_keys[k],
            rp->ops[join].right_keys[k]) != 0);
    /* A later case's last JOIN comes after that one and has two keys; its
     * generated rows are skewed so that the second key discriminates. */
    uint32_t last = rp->op_count;
    while (last && rp->ops[last - 1].op != WL_PLAN_OP_JOIN) last--;
    CHECK(!c->later || (last > join + 1 && rp->ops[last - 1].key_count == 2 &&
        (c->skew || !c->width)));
    if (!c->width) {
        CHECK(wl_columnar_session_seal_join_input(f.raw, c->cold, NULL,
            NULL) == WL_COLUMNAR_SPILL_OK);
        CHECK(wl_columnar_session_seal_join_input(f.s, c->cold, NULL,
            NULL) == WL_COLUMNAR_SPILL_OK);
    } else {
        prepare(&f, c, via);
    }
    bool join_reduce = strchr(c->ops, 'J') && strchr(c->ops, 'R');
    uint64_t raw_joins = wl_columnar_join_test_raw_cold_joins;
    uint64_t raw_scans = wl_columnar_ops_test_raw_cold_scans;
    uint64_t raw_join_aggregates =
        wl_columnar_join_test_raw_cold_join_aggregates;
    uint64_t raw_right_joins = wl_columnar_join_test_raw_cold_right_joins;
    CHECK(!until_done(f.raw));
    /* The uncontrolled oracle reads the cold input before its JOIN through
     * the raw cold-left JOIN, or after a prefix through the raw scan, as a
     * right input through the raw JOIN of a cold right input, and an
     * aggregate after the JOIN through the raw cold JOIN aggregate. */
    CHECK(!strchr(c->ops, 'J') || c->cold_right ||
        wl_columnar_join_test_raw_cold_joins +
        wl_columnar_ops_test_raw_cold_scans > raw_joins + raw_scans);
    CHECK((c->cold_right && !join_reduce) ==
        (wl_columnar_join_test_raw_cold_right_joins > raw_right_joins));
    CHECK(!join_reduce || wl_columnar_join_test_raw_cold_join_aggregates >
        raw_join_aggregates);
    for (int round = 0; round < 2; round++) {
        uint64_t raw = wl_columnar_ops_test_raw_cold_scans;
        uint64_t raw_aggregates = wl_columnar_ops_test_raw_cold_aggregates;
        raw_joins = wl_columnar_join_test_raw_cold_joins;
        raw_join_aggregates = wl_columnar_join_test_raw_cold_join_aggregates;
        raw_right_joins = wl_columnar_join_test_raw_cold_right_joins;
        hooks_clear();
        CHECK(!until_done(f.s));
        CHECK(round || checkpoints > 0);
        CHECK(round || right_scan_seen == c->cold_right);
        /* The copy is scanned at the JOIN that reads it, which then runs. */
        CHECK(round || (joins_before_right > 0) == c->second);
        CHECK(round || !c->cold_right ||
            join_checkpoints > joins_before_right);
        /* The input is scanned once, and every JOIN of the rule runs; in
         * the twice cases the later ones read the same copy. */
        CHECK(round || !c->cold_right || right_scans == 1);
        CHECK(round || !c->twice || join_starts == joins);
        CHECK(wl_columnar_join_test_raw_cold_right_joins == raw_right_joins);
        CHECK(round || !strchr(c->ops, 'R') || reduce_checkpoints > 0);
        CHECK(round || !strchr(c->ops, 'J') || join_checkpoints > 0);
        /* A JOIN case with right rows and no all-rejecting FILTER yields. */
        CHECK(!strchr(c->ops, 'J') || !c->right_rows ||
            strstr(c->name, "all_filtered") || head_rows(f.s, c->head) > 0);
        /* Program facts give no generated rows to check: the STRING right
         * filter case keeps (1, 20) and (2, 30) and drops (1, 10), and the
         * STRING twice case gives (1, "r") and (2, "p"). */
        CHECK((strcmp(c->name, "right_second_filter_string") &&
            strcmp(c->name, "right_twice_string")) ||
            head_rows(f.s, c->head) == 2);
        /* Each left row joins right_rows / right_fan right rows; inserted
         * duplicates stay, and the COUNT counts every pair. */
        const col_rel_t *out = session_find_rel((wl_col_session_t *)f.s,
                c->head);
        for (uint32_t i = 0; c->right_fan && out && i < out->nrows; i++) {
            int64_t left = 0;
            for (uint32_t k = 0; k < c->rows; k++)
                left += cell(k, 0) == col_rel_get(out, i, 0);
            CHECK(col_rel_get(out, i, 1) ==
                left * (int64_t)(c->right_rows / c->right_fan));
        }
        CHECK(!c->right_fan || (out && out->nrows > 0));
        /* Each left row's key matches cold_fan cold rows, which repeat
         * every 13 rows; the COUNT counts every pair. */
        for (uint32_t i = 0; c->cold_fan && out && i < out->nrows; i++) {
            int64_t left = 0;
            for (uint32_t k = 0; k < c->right_rows; k++)
                left += cell(k, 0) == col_rel_get(out, i, 0);
            CHECK(col_rel_get(out, i, 1) == left * (int64_t)c->cold_fan);
        }
        CHECK(!c->cold_fan || (out && out->nrows > 0));
        /* With skew only the even rows of `right` match on both keys, and
         * the odd ones bring first columns the even ones lack, so every
         * head row's first column is that of an even row. */
        for (uint32_t i = 0; c->skew && !c->later && out && i < out->nrows;
            i++) {
            bool even = false;
            for (uint32_t k = 0; k < c->right_rows; k += 2)
                even |= cell(k, 0) == col_rel_get(out, i, 0);
            CHECK(even);
        }
        CHECK(!c->skew || !c->right_rows || (out && out->nrows > 0));
        /* After a later JOIN with two keys every head row holds an even
         * index of `right` (1), or two equal values that a JOIN on one of
         * the keys alone would pair with other ones (2). */
        for (uint32_t i = 0; c->later && out && i < out->nrows; i++)
            CHECK(c->later == 1 ? col_rel_get(out, i, 1) % 2 == 0 :
                col_rel_get(out, i, 0) == col_rel_get(out, i, 1));
        CHECK(!c->later || c->width || (out && out->nrows > 0));
        /* An aggregate over an empty right input yields no row. */
        CHECK(!join_reduce || !c->right || c->right_rows ||
            head_rows(f.s, c->head) == 0);
        CHECK(wl_columnar_join_test_raw_cold_joins == raw_joins);
        CHECK(wl_columnar_join_test_raw_cold_join_aggregates ==
            raw_join_aggregates);
        if (!round && decode_work != expected_decode_work(&f, c))
            fprintf(stderr, "%s: decode work %llu, want %llu\n", where,
                (unsigned long long)decode_work,
                (unsigned long long)expected_decode_work(&f, c));
        CHECK(round || decode_work == expected_decode_work(&f, c));
        CHECK(wl_columnar_ops_test_raw_cold_scans == raw);
        CHECK(wl_columnar_ops_test_raw_cold_aggregates == raw_aggregates);
        if (round) CHECK(!until_done(f.raw));
        expect_heads(&f, c);
    }
    if (c->edit || join_reduce ||
        ((c->cold_right || composite || c->later) && c->width)) {
        /* The plan over a resident input, uncontrolled; after a JOIN and
         * an aggregate, for a cold right input and for a JOIN with two
         * keys, also controlled, which reaches the same JOIN and REDUCE
         * attempts without the cold scan. */
        fixture_t g;
        fixture(&g, case_text(c));
        if (c->right)
            fill_fan(&g, c->right, c->right_width, c->right_rows,
                c->right_fan, c->skew, true);
        fill(&g, c->cold, c->width, c->rows, true);
        CHECK(!until_done(g.raw));
        image_t *a = malloc(sizeof(*a)), *b = malloc(sizeof(*b));
        CHECK(a && b);
        image_of(f.s, c->head, a);
        image_of(g.raw, c->head, b);
        CHECK((a->count > 0 || (c->right && !c->right_rows) ||
            strstr(c->name, "all_filtered")) && image_equal(a, b));
        if (join_reduce || c->cold_right || composite || c->later) {
            hooks_clear();
            CHECK(!until_done(g.s));
            CHECK((!join_reduce || reduce_checkpoints > 0) &&
                join_checkpoints > 0 && !checkpoints);
            image_of(g.s, c->head, a);
            CHECK(image_equal(a, b));
        }
        free(a);
        free(b);
        fixture_destroy(&g);
    }
    fixture_destroy(&f);
}

/* ---- stops --------------------------------------------------------------- */

static uint64_t
child_checkpoints(int child)
{
    return child == 1 ? reduce_checkpoints : child == 2 ? join_checkpoints
           : checkpoints;
}

static uint64_t
calibrate(const qcase_t *c, via_t via, int child)
{
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, via);
    hooks_clear();
    CHECK(!until_done(f.s));
    uint64_t n = child_checkpoints(child);
    fixture_destroy(&f);
    return n;
}

/* Stops at every checkpoint of the scan (child 0), of the REDUCE (1) or of
 * the JOIN (2) that consumes its output. */
static void
stop_case(const qcase_t *c, via_t via, int mode, int child)
{
    static const char *names[3][2] = {
        {"cancel", "work"}, {"reduce_cancel", "reduce_work"},
        {"join_cancel", "join_work"}
    };
    set_where(c, via, names[child][mode - 1]);
    uint64_t n = calibrate(c, via, child);
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
        stop_child = child;
        int rc = until_done(f.s);
        stop_child = 0;
        CHECK(stop_hit);
        /* The control is polled at the checkpoint itself: none follows. */
        CHECK(child_checkpoints(child) == k);
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
parked_destroy_case(const qcase_t *c)
{
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

/* Parked inside the controlled JOIN that reads the copy of a cold right input
 * (#2301), after any JOIN before it: the session refuses checked destruction
 * while the copy is held; after a cancellation the STEP ends with every
 * reservation returned, and a later STEP gives the uncontrolled heads. With
 * reduce, the park is the first return after the REDUCE after the JOIN
 * started, before the RELEASE that drops the copy; as after a scan's last row,
 * the cancellation may come too late to stop the STEP. */
static void
parked_join_case(const qcase_t *c, bool reduce)
{
    set_where(c, VIA_SEAL, reduce ? "parked_reduce" : "parked_join");
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(!until_done(f.raw));
    uint64_t before = reserved(&f);
    hooks_clear();
    int rc = EAGAIN;
    for (unsigned i = 0; rc == EAGAIN && !(reduce ? reduce_checkpoints :
        right_scan_seen && join_checkpoints > joins_before_right); i++) {
        CHECK(i < 200000);
        rc = wl_session_step(f.s);
    }
    CHECK(rc == EAGAIN && join_checkpoints && right_scan_seen);
    CHECK(!reduce || reduce_checkpoints);
    wl_session_t *s = f.s;
    CHECK(wl_session_try_destroy(&s) == EBUSY && s == f.s);
    wl_evaluation_control_request_cancel(f.control);
    rc = until_done(f.s);
    if (rc != WL_EVALUATION_CONTROL_CANCELLED && (!reduce || rc))
        fprintf(stderr, "%s: rc %d\n", where, rc);
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED || (reduce && !rc));
    /* A STEP the cancellation missed holds its published heads. */
    CHECK(!rc || (head_rows(f.s, c->head) == 0 && reserved(&f) == before));
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);
}

/* Parked at the STEP's first return, before the scan of a cold right input
 * after the left input's prefix began: the session refuses checked
 * destruction; after a cancellation the STEP ends with every reservation
 * returned, or, with resume, the STEP resumes and completes, and either way
 * the session then gives the uncontrolled heads. */
static void
parked_prefix_case(const qcase_t *c, bool resume)
{
    set_where(c, VIA_SEAL, resume ? "parked_prefix_resume" : "parked_prefix");
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(!until_done(f.raw));
    uint64_t before = reserved(&f);
    hooks_clear();
    int rc = wl_session_step(f.s);
    CHECK(rc == EAGAIN && !checkpoints && !right_scan_seen &&
        !join_checkpoints);
    wl_session_t *s = f.s;
    CHECK(wl_session_try_destroy(&s) == EBUSY && s == f.s);
    if (resume) {
        CHECK(!until_done(f.s) && right_scan_seen && join_checkpoints);
        expect_heads(&f, c);
        fixture_destroy(&f);
        return;
    }
    wl_evaluation_control_request_cancel(f.control);
    rc = until_done(f.s);
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(head_rows(f.s, c->head) == 0 && reserved(&f) == before);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);
}

/* An allocation the controlled JOIN cannot make while the copy of its cold
 * right input is held: the STEP fails, the copy is dropped, and a later
 * STEP gives the uncontrolled heads. With `before_scan`, the failing
 * allocation is the first one of an earlier JOIN, before the scan; without
 * it, the first one of the JOIN that reads the copy. The hook fails every
 * allocation after the first that it fails. */
static void
join_fault_at(const qcase_t *c, bool before_scan)
{
    set_where(c, VIA_SEAL, before_scan ? "join_fault_first" : "join_fault");
    int skip = 0;
    if (c->second && !before_scan) {
        fixture_t f;
        fixture(&f, case_text(c));
        prepare(&f, c, VIA_SEAL);
        hooks_clear();
        allocations_before_right = -1;
        wl_columnar_join_test_attempt_fail_allocation = JOIN_ALLOCATION_BOUND;
        CHECK(!until_done(f.s));
        wl_columnar_join_test_attempt_fail_allocation = -1;
        CHECK(allocations_before_right > 0);
        skip = allocations_before_right;
        fixture_destroy(&f);
    }
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(!until_done(f.raw));
    uint64_t before = reserved(&f);
    hooks_clear();
    wl_columnar_join_test_attempt_fail_allocation = skip;
    int rc = until_done(f.s);
    wl_columnar_join_test_attempt_fail_allocation = -1;
    CHECK(rc == ENOMEM && right_scan_seen == !before_scan);
    CHECK(head_rows(f.s, c->head) == 0 && reserved(&f) == before);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);
}

static void
join_fault_case(const qcase_t *c)
{
    join_fault_at(c, false);
    if (c->second) join_fault_at(c, true);
}

/* Two cold inputs in one rule: whichever is sealed first is admitted, the
 * second is refused under control and stays resident, and the STEP gives
 * what an uncontrolled session with only the first sealed gives. */
static void
cold_both_case(bool left_first)
{
    snprintf(where_buf, sizeof(where_buf), "cold_both/%s",
        left_first ? "left_first" : "right_first");
    where = where_buf;
    const char *text = ".decl a(x:int64,y:int64)\n.decl c(y:int64,z:int64)\n"
        ".decl out(x:int64,z:int64)\nout(x,z) :- a(x,y), c(y,z).\n";
    const char *first = left_first ? "a" : "c";
    const char *second = left_first ? "c" : "a";
    fixture_t f;
    fixture(&f, text);
    fill(&f, "a", 2, 20, true);
    fill(&f, "c", 2, 20, true);
    CHECK(wl_columnar_session_seal_join_input(f.raw, first, NULL, NULL) ==
        WL_COLUMNAR_SPILL_OK);
    CHECK(wl_columnar_session_seal_join_input(f.s, first, NULL, NULL) ==
        WL_COLUMNAR_SPILL_OK);
    CHECK(wl_columnar_session_seal_join_input(f.s, second, NULL, NULL) ==
        WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(!until_done(f.raw));
    hooks_clear();
    CHECK(!until_done(f.s));
    CHECK(checkpoints > 0 && right_scan_seen == !left_first);
    image_t *a = malloc(sizeof(*a)), *b = malloc(sizeof(*b));
    CHECK(a && b);
    image_of(f.s, "out", a);
    image_of(f.raw, "out", b);
    CHECK(a->count > 0 && image_equal(a, b));
    free(a);
    free(b);
    fixture_destroy(&f);
}

#define SECOND_TEXT ".decl a(x:int64,y:int64,w:int64)\n" \
        ".decl c(v:int64,z:int64)\n.decl out(x:int64,z:int64)\n" \
        "out(x,z) :- a(x,y,w), a(y,v,u), c(v,z).\n"

/* Two cold inputs of a rule whose later JOIN reads one: the resident input
 * of the earlier JOIN with itself is refused under control in either
 * order, and the STEP gives what an uncontrolled session with only the
 * other sealed gives. */
static void
cold_both_later_case(bool left_first)
{
    snprintf(where_buf, sizeof(where_buf), "cold_both_later/%s",
        left_first ? "left_first" : "right_first");
    where = where_buf;
    fixture_t f;
    fixture(&f, SECOND_TEXT);
    fill(&f, "a", 3, 20, true);
    fill(&f, "c", 2, 20, true);
    CHECK(wl_columnar_session_seal_join_input(f.raw, "c", NULL, NULL) ==
        WL_COLUMNAR_SPILL_OK);
    if (left_first)
        CHECK(wl_columnar_session_seal_join_input(f.s, "a", NULL, NULL) ==
            WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(wl_columnar_session_seal_join_input(f.s, "c", NULL, NULL) ==
        WL_COLUMNAR_SPILL_OK);
    if (!left_first)
        CHECK(wl_columnar_session_seal_join_input(f.s, "a", NULL, NULL) ==
            WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(!until_done(f.raw));
    hooks_clear();
    CHECK(!until_done(f.s));
    CHECK(right_scan_seen && joins_before_right > 0);
    image_t *a = malloc(sizeof(*a)), *b = malloc(sizeof(*b));
    CHECK(a && b);
    image_of(f.s, "out", a);
    image_of(f.raw, "out", b);
    CHECK(a->count > 0 && image_equal(a, b));
    free(a);
    free(b);
    fixture_destroy(&f);
}

/* A later JOIN of a cold right input whose STRING left key meets the cold
 * input's INT64 key: the uncontrolled session admits the seal or load and
 * its STEP returns ENOTSUP; the controlled one refuses it, and its STEP
 * returns what it returns in a session that never tried. The parser
 * refuses a variable over a STRING and an INT64 column, so the key comes
 * from an edited plan. Kind 0 has a JOIN of the resident input before that
 * one, kind 1 with a right filter, and in kind 2 the earlier JOIN reads
 * the cold input too. */
static void
key_type_case(via_t via, int kind)
{
    static const char *const kinds[3] = {"", "/filtered", "/twice"};
    snprintf(where_buf, sizeof(where_buf), "key_type/%s%s",
        via == VIA_SEAL ? "seal" : "load", kinds[kind]);
    where = where_buf;
    static const char *const texts[3] = {
        ".decl a(x:int64,y:int64,s:symbol)\n"
        ".decl c(v:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
        "a(1,2,\"p\"). a(2,3,\"q\"). a(3,1,\"p\").\n"
        "out(x,z) :- a(x,y,s), a(y,v,u), c(v,z).\n",
        ".decl a(x:int64,y:int64,s:symbol)\n"
        ".decl c(v:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
        "a(1,2,\"p\"). a(2,3,\"q\"). a(3,1,\"p\").\n"
        "out(x,z) :- a(x,y,s), a(y,v,\"p\"), c(v,z).\n",
        ".decl a(x:int64,y:int64,s:symbol)\n"
        ".decl c(v:int64,z:int64)\n.decl out(x:int64,u:int64)\n"
        "a(1,2,\"p\"). a(2,3,\"q\"). a(3,1,\"p\").\n"
        "out(x,u) :- a(x,y,s), c(y,z), c(z,u).\n"
    };
    const char *text = texts[kind];
    fixture_t f;
    plan_edit = 4;
    fixture(&f, text);
    char got[16];
    ops_of(&f, got);
    CHECK(!strcmp(got, "VJJC"));
    const wl_plan_relation_t *rp = &f.plan->strata[0].relations[0];
    CHECK(!rp->ops[1].right_filter_expr.size == (kind != 1));
    CHECK(!strcmp(rp->ops[1].right_relation, kind == 2 ? "c" : "a"));
    if (via == VIA_SEAL) {
        fill(&f, "c", 2, 4, true);
        CHECK(wl_columnar_session_seal_join_input(f.raw, "c", NULL, NULL) ==
            WL_COLUMNAR_SPILL_OK);
        CHECK(wl_columnar_session_seal_join_input(f.s, "c", NULL, NULL) ==
            WL_COLUMNAR_SPILL_UNSUPPORTED);
    } else {
        CHECK(load(f.raw, "c", 2, 4) == WL_COLUMNAR_SPILL_OK);
        CHECK(load(f.s, "c", 2, 4) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    }
    CHECK(until_done(f.raw) == ENOTSUP);
    hooks_clear();
    int rc = until_done(f.s);
    CHECK(!checkpoints);
    fixture_t g;
    plan_edit = 4;
    fixture(&g, text);
    if (via == VIA_SEAL) fill(&g, "c", 2, 4, false);
    int expected = until_done(g.s);
    if (rc != expected)
        fprintf(stderr, "%s: rc %d, unsealed %d\n", where, rc, expected);
    CHECK(rc == expected);
    fixture_destroy(&g);
    fixture_destroy(&f);
}

/* A later JOIN of a cold right input over a resident input declared but
 * never inserted: the earlier JOIN's output has no column, which the
 * uncontrolled JOIN of the cold input refuses at its STEP with ENOTSUP;
 * the controlled STEP returns ENOTSUP too, before the scan, with every
 * reservation returned. */
static void
second_absent_case(via_t via, const char *name)
{
    snprintf(where_buf, sizeof(where_buf), "second_absent/%s/%s", name,
        via == VIA_SEAL ? "seal" : "load");
    where = where_buf;
    const qcase_t *c = find_qcase(name);
    qcase_t absent = *c;
    absent.right_rows = 0;
    fixture_t f;
    fixture(&f, case_text(&absent));
    prepare(&f, &absent, via);
    CHECK(until_done(f.raw) == ENOTSUP);
    uint64_t before = reserved(&f);
    hooks_clear();
    CHECK(until_done(f.s) == ENOTSUP);
    CHECK(!checkpoints && join_checkpoints > 0);
    CHECK(head_rows(f.s, "out") == 0 && reserved(&f) == before);
    fixture_destroy(&f);
}

/* A cold right input read twice over a resident first input declared but
 * never inserted. The uncontrolled session admits the seal or load; its
 * STEP checks the left input of each JOIN of the cold input, and returns
 * ENOTSUP where a later one's left key is not a column of the earlier
 * JOINs' output, completing with no head row otherwise. Over such an input
 * the output of a controlled JOIN has none of its columns, so a later JOIN
 * of the copy would resolve its keys against other columns than the
 * uncontrolled one: the controlled session refuses the seal or load, and
 * its STEP returns what it returns in a session that never tried. */
static void
twice_absent_case(via_t via, const char *name, int raw_rc)
{
    snprintf(where_buf, sizeof(where_buf), "twice_absent/%s/%s", name,
        via == VIA_SEAL ? "seal" : "load");
    where = where_buf;
    const qcase_t *c = find_qcase(name);
    fixture_t f;
    fixture(&f, case_text(c));
    if (via == VIA_SEAL) {
        fill(&f, c->cold, c->width, c->rows, true);
        CHECK(wl_columnar_session_seal_join_input(f.raw, c->cold, NULL,
            NULL) == WL_COLUMNAR_SPILL_OK);
        CHECK(wl_columnar_session_seal_join_input(f.s, c->cold, NULL,
            NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    } else {
        CHECK(load(f.raw, c->cold, c->width, c->rows) ==
            WL_COLUMNAR_SPILL_OK);
        CHECK(load(f.s, c->cold, c->width, c->rows) ==
            WL_COLUMNAR_SPILL_UNSUPPORTED);
    }
    CHECK(until_done(f.raw) == raw_rc && head_rows(f.raw, "out") == 0);
    hooks_clear();
    int rc = until_done(f.s);
    CHECK(!checkpoints);
    fixture_t g;
    fixture(&g, case_text(c));
    if (via == VIA_SEAL) fill(&g, c->cold, c->width, c->rows, false);
    int expected = until_done(g.s);
    if (rc != expected)
        fprintf(stderr, "%s: rc %d, unsealed %d\n", where, rc, expected);
    CHECK(rc == expected);
    fixture_destroy(&g);
    fixture_destroy(&f);
}

/* An allocation that the nth JOIN attempt of the rule cannot make, its
 * first one, after an earlier JOIN read the copy of the cold right input:
 * the STEP fails, the copy is dropped, and a later STEP gives the
 * uncontrolled heads. */
static void
join_fault_nth(const qcase_t *c, uint64_t n)
{
    char what[32];
    snprintf(what, sizeof(what), "join_fault_%llu", (unsigned long long)n);
    set_where(c, VIA_SEAL, what);
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    hooks_clear();
    join_latch = n;
    allocations_before_latch = -1;
    wl_columnar_join_test_attempt_fail_allocation = JOIN_ALLOCATION_BOUND;
    CHECK(!until_done(f.s));
    wl_columnar_join_test_attempt_fail_allocation = -1;
    CHECK(join_starts >= n && allocations_before_latch > 0);
    int skip = allocations_before_latch;
    fixture_destroy(&f);
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(!until_done(f.raw));
    uint64_t before = reserved(&f);
    hooks_clear();
    wl_columnar_join_test_attempt_fail_allocation = skip;
    int rc = until_done(f.s);
    wl_columnar_join_test_attempt_fail_allocation = -1;
    join_latch = 0;
    CHECK(rc == ENOMEM && right_scans == 1 && join_starts == n);
    CHECK(head_rows(f.s, c->head) == 0 && reserved(&f) == before);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);
}

/* Parked inside the nth JOIN attempt of the rule, after a checkpoint of
 * it, with the copy of the cold right input held for an earlier JOIN: the
 * session refuses checked destruction; after a cancellation the STEP ends
 * with every reservation returned, and a later STEP gives the uncontrolled
 * heads. */
static void
parked_later_join_case(const qcase_t *c, uint64_t n)
{
    char what[32];
    snprintf(what, sizeof(what), "parked_join_%llu", (unsigned long long)n);
    set_where(c, VIA_SEAL, what);
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(!until_done(f.raw));
    uint64_t before = reserved(&f);
    hooks_clear();
    join_latch = n;
    int rc = EAGAIN;
    for (unsigned i = 0; rc == EAGAIN && !(join_starts >= n &&
        join_checkpoints > joins_before_latch + 1); i++) {
        CHECK(i < 200000);
        rc = wl_session_step(f.s);
    }
    join_latch = 0;
    CHECK(rc == EAGAIN && right_scans == 1 && join_starts == n);
    wl_session_t *s = f.s;
    CHECK(wl_session_try_destroy(&s) == EBUSY && s == f.s);
    wl_evaluation_control_request_cancel(f.control);
    rc = until_done(f.s);
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(head_rows(f.s, c->head) == 0 && reserved(&f) == before);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);
}

/* Parked inside a JOIN before the one that reads the copy of a cold right
 * input, before the scan began: the session refuses checked destruction;
 * after a cancellation the STEP ends with every reservation returned, and a
 * later STEP gives the uncontrolled heads. */
static void
parked_first_join_case(const qcase_t *c)
{
    set_where(c, VIA_SEAL, "parked_first_join");
    fixture_t f;
    fixture(&f, case_text(c));
    prepare(&f, c, VIA_SEAL);
    CHECK(!until_done(f.raw));
    uint64_t before = reserved(&f);
    hooks_clear();
    int rc = EAGAIN;
    for (unsigned i = 0; rc == EAGAIN && !join_checkpoints; i++) {
        CHECK(i < 200000);
        rc = wl_session_step(f.s);
    }
    CHECK(rc == EAGAIN && join_checkpoints && !right_scan_seen);
    wl_session_t *s = f.s;
    CHECK(wl_session_try_destroy(&s) == EBUSY && s == f.s);
    wl_evaluation_control_request_cancel(f.control);
    rc = until_done(f.s);
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && !right_scan_seen);
    CHECK(head_rows(f.s, c->head) == 0 && reserved(&f) == before);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!until_done(f.s));
    expect_heads(&f, c);
    fixture_destroy(&f);
}

/* A cancellation the control first sees inside a spill read. */
static void
read_cancel_case(const qcase_t *c)
{
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
read_fault_case(const qcase_t *c)
{
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
    unsigned phases = 0, denials = 0, passes = 0, child_denials = 0;
    unsigned reduce_denials = 0, prefix_denials = 0;
    bool child = strchr(c->ops, 'R') || strchr(c->ops, 'J');
    /* A cold right input after the left input's prefix or after an
     * earlier JOIN. */
    bool prefixed = c->cold_right && (c->ops[1] != 'J' || c->second);
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
        /* A denial after the scan, at its output's install, in the REDUCE
         * or JOIN or in the CONSOLIDATE, leaves the scan's last checkpoint
         * behind. */
        if (checkpoints && !child) {
            denials++;
            phases |= 1u << (unsigned)last_phase;
        } else if (checkpoints && !reduce_checkpoints &&
            join_checkpoints == joins_before_right &&
            last_phase != WL_COLUMNAR_COLD_SCAN_ATTEMPT_WRITER_END) {
            denials++;
            phases |= 1u << (unsigned)last_phase;
        }
        if (!checkpoints) prefix_denials++;
        /* Denials in an earlier JOIN, before the scan, are not the child's. */
        if (reduce_checkpoints ||
            (checkpoints && join_checkpoints > joins_before_right))
            child_denials++;
        if (reduce_checkpoints) reduce_denials++;
        CHECK(wl_columnar_memory_release(&blocker));
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!until_done(f.s));
        expect_heads(&f, c);
        fixture_destroy(&f);
    }
    CHECK(passes == 1);
    /* The last checkpoint before each denial names the action it stopped:
     * scratch, output, reader, the writer's drain after WRITER_END, and on
     * the loading route a row read that loads a segment. Before a REDUCE or
     * a JOIN a denial after WRITER_END is not told apart from the later
     * ones, so WRITER_END is not required there. After the left input's
     * prefix, some denials come before the scan began, and in the case run
     * here none at the scratch: the room that the prefix needs also holds
     * the scan's scratch, so the scratch is not required there. */
    CHECK(!prefixed || prefix_denials);
    unsigned want = (prefixed ? 0u :
        1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_SCRATCH) |
        1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_OUTPUT_CREATE |
        1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_READER |
        (child ? 0u : 1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_WRITER_END) |
        (via == VIA_LOAD ? 1u << WL_COLUMNAR_COLD_SCAN_ATTEMPT_ROWS : 0u);
    if ((phases & want) != want)
        fprintf(stderr, "%s: denials %u phases %#x\n", where, denials,
            phases);
    CHECK(denials && (phases & want) == want);
    /* Before a REDUCE or a JOIN some denials come after that child
     * started. */
    CHECK(!child || child_denials);
    /* After a JOIN some denials come inside the REDUCE over its output. */
    CHECK(!(strchr(c->ops, 'J') && strchr(c->ops, 'R')) || reduce_denials);
}

/* A full STEP consumes the same work on both routes, and one unit less
 * stops it.  The total itself differs between toolchains (582040 with gcc
 * and clang on Linux, 568600 with MSVC), so it is not pinned. */
static uint64_t
exact_case(const qcase_t *c, via_t via)
{
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

/* ---- aggregates ---------------------------------------------------------- */

/* Rows of the overflow case: a group whose SUM overflows at its second row,
 * then rows of another group. */
#define OVERFLOW_ROWS 12u

static void
overflow_row(uint32_t i, int64_t row[2])
{
    row[0] = i < 2 ? 1 : 2;
    row[1] = i == 0 ? INT64_MAX : i == 1 ? 1 : (int64_t)i;
}

static wl_columnar_spill_status_t
overflow_load(wl_session_t *s)
{
    wl_columnar_spill_status_t st = wl_columnar_session_begin_cold_input(s,
            "c");
    for (uint32_t i = 0; st == WL_COLUMNAR_SPILL_OK && i < OVERFLOW_ROWS;
        i += 3) {
        int64_t chunk[6];
        for (uint32_t r = 0; r < 3; r++)
            overflow_row(i + r, &chunk[2 * r]);
        st = wl_columnar_session_append_cold_input(s, "c", chunk, 3, 2);
    }
    return st == WL_COLUMNAR_SPILL_OK ?
           wl_columnar_session_finish_cold_input(s, "c") : st;
}

/* SUM overflow returns ERANGE on both sessions and publishes nothing. The
 * controlled route reads every row before its REDUCE starts, the
 * uncontrolled one stops at the overflowing row: a read fault after that
 * row returns EIO only on the controlled route. */
static void
overflow_case(via_t via, bool fault)
{
    snprintf(where_buf, sizeof(where_buf), "overflow/%s%s",
        via == VIA_SEAL ? "seal" : "load", fault ? "/fault" : "");
    where = where_buf;
    fixture_t f;
    fixture(&f, AGG ".decl out(x:int64,n:int64)\n"
        "out(x,sum(y)) :- c(x,y).\n");
    if (via == VIA_SEAL) {
        for (uint32_t i = 0; i < OVERFLOW_ROWS; i++) {
            int64_t row[2];
            overflow_row(i, row);
            CHECK(!wl_session_insert(f.s, "c", row, 1, 2));
            CHECK(!wl_session_insert(f.raw, "c", row, 1, 2));
        }
        CHECK(wl_columnar_session_seal_join_input(f.raw, "c", NULL, NULL) ==
            WL_COLUMNAR_SPILL_OK);
        CHECK(wl_columnar_session_seal_join_input(f.s, "c", NULL, NULL) ==
            WL_COLUMNAR_SPILL_OK);
    } else {
        CHECK(overflow_load(f.raw) == WL_COLUMNAR_SPILL_OK);
        CHECK(overflow_load(f.s) == WL_COLUMNAR_SPILL_OK);
    }
    registry_hash(&f);
    uint64_t before = reserved(&f);
    read_ios = 0;
    CHECK(until_done(f.raw) == ERANGE);
    uint64_t raw_reads = read_ios;
    CHECK(head_rows(f.raw, "out") == 0);
    if (fault) read_fault = (unsigned)raw_reads + 1;
    hooks_clear();
    int rc = until_done(f.s);
    if (fault) {
        CHECK(!read_fault);
        CHECK(rc == EIO);
    } else {
        CHECK(rc == ERANGE && reduce_checkpoints > 0);
    }
    CHECK(head_rows(f.s, "out") == 0 && reserved(&f) == before);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(until_done(f.s) == ERANGE);
    fixture_destroy(&f);
}

/* The minimum free bytes at which a STEP of one session succeeds. */
static uint64_t
least_room(const qcase_t *c, bool controlled, uint64_t step)
{
    for (uint64_t room = 0; room <= UINT64_C(256) * 1024; room += step) {
        fixture_t f;
        fixture(&f, case_text(c));
        prepare(&f, c, VIA_SEAL);
        uint64_t before = reserved(&f);
        wl_columnar_memory_reservation_t blocker;
        wl_columnar_memory_reservation_init(&blocker);
        CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
                f.governor), BUDGET - before - room, &blocker));
        wl_session_t *s = controlled ? f.s : f.raw;
        int rc = until_done(s);
        CHECK(!rc || rc == ENOSPC);
        CHECK(rc ? wl_session_budget_denied(s) && head_rows(s, c->head) == 0
            : !wl_session_budget_denied(s));
        CHECK(wl_columnar_memory_release(&blocker));
        if (rc && controlled) {
            /* As after a work-limit stop, destruction needs a reset. */
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!until_done(f.s));
        }
        fixture_destroy(&f);
        if (!rc) return room;
    }
    CHECK(false);
    return 0;
}

/* The controlled route holds the scan's output, sized from the source rows,
 * beside the REDUCE or JOIN state; the uncontrolled cold aggregate streams
 * the rows into its groups, and the uncontrolled cold JOIN reads them in
 * batches. A budget that admits the uncontrolled STEP can deny the
 * controlled one. */
static void
divergence_case(const char *name)
{
    const qcase_t *c = find_qcase(name);
    set_where(c, VIA_SEAL, "divergence");
    uint64_t raw = least_room(c, false, 256);
    uint64_t controlled = least_room(c, true, 256);
    if (controlled <= raw)
        fprintf(stderr, "%s: raw %llu controlled %llu\n", where,
            (unsigned long long)raw, (unsigned long long)controlled);
    CHECK(controlled > raw);
}

/* ---- later STEPs --------------------------------------------------------- */

/* The controlled JOIN may read a delta of its right input in place of the
 * whole input; the uncontrolled cold JOIN reads the whole input. Rows of
 * the right input can come from program facts and from inserts made before
 * the cold input is sealed or loaded, and the first STEP joins all of them
 * on both routes. A STEP before the seal or load makes either one busy, and
 * after it every insert is refused, so no later STEP sees a new delta. */
static void
right_insert_case(const qcase_t *c, via_t via)
{
    set_where(c, via, "right_insert");
    static const char *text = ".decl c(x:int64,y:int64)\n"
        ".decl r(y:int64,z:int64)\n.decl out(x:int64,y:int64,z:int64)\n"
        "r(3,50). r(10,51). r(4,52).\nout(x,y,z) :- c(x,y), r(y,z).\n";
    fixture_t f;
    fixture(&f, text);
    prepare(&f, c, via);
    CHECK(!until_done(f.raw));
    hooks_clear();
    CHECK(!until_done(f.s));
    CHECK(join_checkpoints > 0 && head_rows(f.s, c->head) > 0);
    expect_heads(&f, c);
    int64_t row[2] = {1, 2};
    CHECK(wl_session_insert(f.s, c->right, row, 1, 2) == ENOTSUP);
    fixture_destroy(&f);

    fixture(&f, text);
    fill(&f, c->right, c->right_width, c->right_rows, true);
    if (via == VIA_SEAL) fill(&f, c->cold, c->width, c->rows, true);
    CHECK(!until_done(f.s));
    CHECK((via == VIA_SEAL ? wl_columnar_session_seal_join_input(f.s,
        c->cold, NULL, NULL) : load(f.s, c->cold, c->width, c->rows)) ==
        WL_COLUMNAR_SPILL_BUSY);
    fixture_destroy(&f);
}

/* ---- parked stops -------------------------------------------------------- */

/* A resident controlled JOIN and the CONSOLIDATE after it run across many
 * public calls; the cold JOIN reuses both. A cancellation requested between
 * two calls, with the relation in flight, ends the STEP as cancelled and
 * returns every reservation, unless it comes after the STEP's last poll.
 * The session is then destroyed at once on odd parks; on even ones a reset
 * and a new STEP match the uncontrolled session. A work limit is reached
 * only inside a charge, which the stop sweeps cover. */
#define PARKED_JOIN ".decl c(x:int64,y:int64)\n.decl r(y:int64,z:int64)\n" \
        ".decl out(x:int64,y:int64,z:int64)\n" \
        "out(x,y,z) :- c(x,y), r(y,z).\n"

static void
parked_cancel_case(void)
{
    const qcase_t c = {"parked", PARKED_JOIN, "c", 2, 20, "VJC", 0, "out",
                       NULL, "r", 2, 20};
    where = "parked_cancel";
    unsigned stopped = 0, completed = 0;
    for (unsigned park = 1;; park++) {
        CHECK(park < 100000);
        fixture_t f;
        fixture(&f, PARKED_JOIN);
        fill(&f, "r", 2, 20, true);
        fill(&f, "c", 2, 20, true);
        CHECK(!until_done(f.raw));
        uint64_t before = reserved(&f);
        hooks_clear();
        int rc = EAGAIN;
        for (unsigned i = 0; i < park && rc == EAGAIN; i++)
            rc = wl_session_step(f.s);
        if (rc != EAGAIN) {
            /* The STEP completed before this park: every park was tried. */
            CHECK(!rc);
            expect_heads(&f, &c);
            fixture_destroy(&f);
            break;
        }
        const int code = WL_EVALUATION_CONTROL_CANCELLED;
        wl_evaluation_control_request_cancel(f.control);
        rc = until_done(f.s);
        if (rc && rc != code)
            fprintf(stderr, "%s: park %u rc %d\n", where, park, rc);
        /* A cancellation requested after the STEP's last poll lets it
         * complete, and so does every later one. */
        CHECK(!rc || rc == code);
        if (!rc) {
            completed++;
            expect_heads(&f, &c);
            fixture_destroy(&f);
            continue;
        }
        CHECK(!completed);
        stopped += join_checkpoints > 0;
        CHECK(head_rows(f.s, c.head) == 0 && reserved(&f) == before);
        if (park % 2) {
            fixture_destroy(&f);
            continue;
        }
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!until_done(f.s));
        expect_heads(&f, &c);
        fixture_destroy(&f);
    }
    /* Some stops came after the JOIN had started. */
    CHECK(stopped > 0);
}

/* ---- refusals ------------------------------------------------------------ */

typedef struct {
    const char *name;
    const char *text;
    const char *cold;
    uint32_t width;
    bool side_compound; /* Make a side compound before sealing. */
    bool raw_refuses;   /* The uncontrolled session refuses it too. */
    const char *ops;    /* If set, the rule's op order, and */
    uint32_t keys;      /* the key count of its last JOIN. */
} refusal_t;

static const refusal_t refusals[] = {
    {"right_sum", ".decl a(x:int64,y:int64)\n.decl c(y:int64,z:int64)\n"
     ".decl out(x:int64,n:int64)\nout(x,sum(z)) :- a(x,y), c(y,z).\n",
     "c", 2, false, true},
    {"right_count_after_filter", ".decl a(x:int64,y:int64)\n"
     ".decl c(y:int64,z:int64)\n.decl out(x:int64,n:int64)\n"
     "out(x,count(z)) :- a(x,y), y = 3, c(y,z).\n", "c", 2, false, true},
    {"right_after_filter", ".decl a(x:int64,y:int64)\n"
     ".decl c(y:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y), y = 3, c(y,z).\n", "c", 2, false, true},
    {"right_inline", ".decl a(x:int64,y:int64)\n"
     ".decl c(y:int64,p:pair/2 inline)\n.decl out(x:int64,u:int64)\n"
     "out(x,u) :- a(x,y), c(y,pair(u,v)).\n", "c", 3, false},
    {"right_inline_count", ".decl a(x:int64,y:int64)\n"
     ".decl c(y:int64,p:pair/2 inline)\n.decl out(x:int64,n:int64)\n"
     "out(x,count(u)) :- a(x,y), c(y,pair(u,v)).\n", "c", 3, false},
    /* Three inputs and a head exceed the controlled registry bound. */
    {"right_second_join", ".decl a(x:int64,y:int64)\n"
     ".decl r(y:int64,w:int64)\n.decl c(w:int64,z:int64)\n"
     ".decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y), r(y,w), c(w,z).\n", "c", 2, false},
    {"self_join", ".decl c(x:int64,y:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- c(x,y), c(y,z).\n", "c", 2, false},
    /* A cold right input read twice, refused uncontrolled too: a constant
     * in the second JOIN's cold input, over two columns of it and over
     * three, a comparison between the JOINs, a COUNT after them, and two
     * reads on the same key, which the planner emits in another shape. */
    {"right_twice_filter_on_cold", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(y:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), c(y,z), c(z,4).\n", "c", 2, false, true, "VJJC",
     1},
    {"right_twice_filter_cold_keys", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(y:int64,z:int64,v:int64)\n.decl out(x:int64,u:int64)\n"
     "out(x,u) :- a(x,y,w), c(y,z,v), c(z,4,u).\n", "c", 3, false, true,
     "VJJC", 1},
    {"right_twice_compare", ".decl a(x:int64,y:int64)\n"
     ".decl c(y:int64,z:int64)\n.decl out(x:int64,u:int64)\n"
     "out(x,u) :- a(x,y), c(y,z), z < 9, c(z,u).\n", "c", 2, false, true,
     "VJJFMC", 1},
    {"right_twice_count", ".decl a(x:int64,y:int64)\n"
     ".decl c(y:int64,z:int64)\n.decl out(x:int64,n:int64)\n"
     "out(x,count(u)) :- a(x,y), c(y,z), c(z,u).\n", "c", 2, false, true,
     "VJJRC", 1},
    {"right_twice_same_key", ".decl a(x:int64,y:int64)\n"
     ".decl c(y:int64,z:int64)\n.decl out(x:int64,u:int64)\n"
     "out(x,u) :- a(x,y), c(y,z), c(y,u).\n", "c", 2, false, true},
    /* A COUNT after a later JOIN that reads a cold right input. */
    {"right_second_count", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,z:int64)\n.decl out(x:int64,n:int64)\n"
     "out(x,count(z)) :- a(x,y,w), a(y,v,u), c(v,z).\n", "c", 2, false,
     true, "VJJRC", 1},
    /* A right filter and a cold right input, refused uncontrolled too: the
     * filter after a prefix, on the JOIN of the cold input, with two keys at
     * the earlier JOIN or at the cold one, and with the cold input at the
     * first JOIN. A comparison that the planner emits here as a FILTER after
     * the JOINs, with no prefix and one key, is refused as well. */
    {"right_filter_after_prefix", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), w = 7, a(y,3,u), c(u,z).\n", "c", 2, false,
     true, "VFJJFMC", 1},
    {"right_filter_on_cold", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,z:int64)\n.decl out(x:int64,v:int64)\n"
     "out(x,v) :- a(x,y,w), a(y,v,u), c(u,3).\n", "c", 2, false, true,
     "VJJC", 1},
    {"right_filter_keys", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), a(y,w,3), c(y,z).\n", "c", 2, false, true,
     "VJJC", 1},
    {"right_filter_cold_keys", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,x:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), a(y,3,u), c(u,x,z).\n", "c", 3, false, true,
     "VJJC", 2},
    {"right_filter_first_cold", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), c(y,z), a(z,3,u).\n", "c", 2, false, true,
     "VJJC", 1},
    {"right_compare_after", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(v:int64,z:int64)\n.decl out(x:int64,z:int64)\n"
     "out(x,z) :- a(x,y,w), a(y,v,u), u < 9, c(v,z).\n", "c", 2, false,
     true, "VJJFMC", 1},
    /* A later JOIN with no key, and one with two keys before a COUNT. */
    {"later_cross_join", ".decl c(x:int64,y:int64)\n"
     ".decl r(a:int64,b:int64,c:int64)\n.decl out(x:int64,u:int64)\n"
     "out(x,u) :- c(x,y), r(y,w,v), r(p,q,u).\n", "c", 2, false, true,
     "VJJC", 0},
    {"right_later_cross_join", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(x:int64,y:int64,z:int64)\n.decl out(x:int64,u:int64)\n"
     "out(x,u) :- a(x,y,w), c(x,y,z), a(p,q,u).\n", "c", 3, false, true,
     "VJJC", 0},
    {"later_keys_count", ".decl c(x:int64,y:int64)\n"
     ".decl r(a:int64,b:int64,c:int64)\n.decl out(v:int64,n:int64)\n"
     "out(v,count(u)) :- c(x,y), r(x,w,v), r(x,w,u).\n", "c", 2, false,
     true, "VJJRC", 2},
    {"right_later_keys_count", ".decl a(x:int64,y:int64,w:int64)\n"
     ".decl c(x:int64,y:int64,z:int64)\n.decl out(x:int64,n:int64)\n"
     "out(x,count(u)) :- a(x,y,w), c(x,p,q), a(x,p,u).\n", "c", 3, false,
     true, "VJJRC", 2},
    {"cross_join", ".decl c(x:int64,y:int64)\n.decl r(w:int64,z:int64)\n"
     ".decl out(x:int64,z:int64)\nout(x,z) :- c(x,y), r(w,z).\n", "c", 2,
     false, true, "VJC", 0},
    {"join_sum", ".decl c(x:int64,y:int64)\n.decl r(y:int64,z:int64)\n"
     ".decl out(x:int64,n:int64)\nout(x,sum(z)) :- c(x,y), r(y,z).\n",
     "c", 2, false, true},
    {"inline_join_count", ".decl c(i:int64,p:pair/2 inline)\n"
     ".decl r(x:int64,z:int64)\n.decl out(i:int64,n:int64)\n"
     "out(i,count(z)) :- c(i,pair(x,y)), r(x,z).\n", "c", 3, false},
    {"inline_join", ".decl c(i:int64,p:pair/2 inline)\n"
     ".decl r(x:int64,z:int64)\n.decl out(i:int64,z:int64)\n"
     "out(i,z) :- c(i,pair(x,y)), r(x,z).\n", "c", 3, false},
    {"head_is_edb_count", ".decl c(x:int64,y:int64)\n"
     ".decl d(x:int64,n:int64)\nd(9,9).\nd(x,count(y)) :- c(x,y).\n", "c",
     2, false},
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
    if (r->ops) {
        char got[16];
        ops_of(&f, got);
        const wl_plan_relation_t *rp = &f.plan->strata[0].relations[0];
        uint32_t join = rp->op_count;
        while (join && rp->ops[join - 1].op != WL_PLAN_OP_JOIN) join--;
        CHECK(!strcmp(got, r->ops) && join &&
            rp->ops[join - 1].key_count == r->keys);
    }
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
    wl_columnar_spill_status_t raw_want = r->raw_refuses
        ? WL_COLUMNAR_SPILL_UNSUPPORTED : WL_COLUMNAR_SPILL_OK;
    if (via == VIA_SEAL) {
        fill(&f, r->cold, r->width, 4, true);
        CHECK(wl_columnar_session_seal_join_input(f.raw, r->cold, NULL,
            NULL) == raw_want);
        CHECK(wl_columnar_session_seal_join_input(f.s, r->cold, NULL,
            NULL) == want);
    } else {
        CHECK(load(f.raw, r->cold, r->width, 4) == raw_want);
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

/* The cold right input cases (#2301), run as their own test so that each
 * run stays within the sanitizer legs' time. */
static void
right_cases(void)
{
    for (size_t i = 0; i < NQCASES; i++) {
        if (!qcases[i].cold_right || qcases[i].twice) continue;
        run_qcase(&qcases[i], VIA_SEAL);
        run_qcase(&qcases[i], VIA_LOAD);
    }
    /* Stops in the scan on both routes, and in the JOIN that reads its copy
     * with each stop mode on one route. */
    for (int mode = 1; mode <= 2; mode++) {
        stop_case(find_qcase("right_few"), VIA_SEAL, mode, 0);
        stop_case(find_qcase("right_few"), VIA_LOAD, mode, 0);
    }
    stop_case(find_qcase("right_few"), VIA_SEAL, 2, 2);
    stop_case(find_qcase("right_few"), VIA_LOAD, 1, 2);
    /* After the JOIN, the REDUCE's checkpoints with each stop mode on one
     * route. */
    stop_case(find_qcase("right_count_few"), VIA_SEAL, 1, 1);
    stop_case(find_qcase("right_count_few"), VIA_LOAD, 2, 1);
    parked_destroy_case(find_qcase("right_pair"));
    parked_join_case(find_qcase("right_pair"), false);
    parked_join_case(find_qcase("right_count"), true);
    /* A cancellation at every checkpoint of a JOIN with two keys that
     * reads the copy, and an allocation that JOIN cannot make. */
    stop_case(find_qcase("right_composite_few"), VIA_LOAD, 1, 2);
    stop_case(find_qcase("right_later_keys_few"), VIA_LOAD, 1, 2);
    join_fault_case(find_qcase("right_composite"));
    join_fault_case(find_qcase("right_later_keys"));
    join_fault_case(find_qcase("right_pair"));
    read_cancel_case(find_qcase("right_pair"));
    read_fault_case(find_qcase("right_pair"));
    denial_case(find_qcase("right_wide"), VIA_SEAL);
    denial_case(find_qcase("right_wide"), VIA_LOAD);
    /* After the left input's FILTER: stops in the scan with each mode on
     * both routes, in the JOIN that reads the copy with each mode on one
     * route, and in the REDUCE after it; parks before the scan and in the
     * JOIN or the REDUCE; an allocation the JOIN cannot make, with one and
     * with two keys; a read cancellation, a read fault and the denials. */
    for (int mode = 1; mode <= 2; mode++) {
        stop_case(find_qcase("right_after_filter_few"), VIA_SEAL, mode, 0);
        stop_case(find_qcase("right_after_filter_few"), VIA_LOAD, mode, 0);
    }
    stop_case(find_qcase("right_after_filter_few"), VIA_SEAL, 2, 2);
    stop_case(find_qcase("right_after_filter_few"), VIA_LOAD, 1, 2);
    stop_case(find_qcase("right_after_filter_count"), VIA_SEAL, 1, 1);
    parked_prefix_case(find_qcase("right_after_filter_join"), false);
    parked_prefix_case(find_qcase("right_after_filter_join"), true);
    parked_join_case(find_qcase("right_after_filter_join"), false);
    parked_join_case(find_qcase("right_after_filter_count"), true);
    join_fault_case(find_qcase("right_after_filter_join"));
    join_fault_case(find_qcase("right_composite_after_filter"));
    read_cancel_case(find_qcase("right_after_filter_join"));
    read_fault_case(find_qcase("right_after_filter_join"));
    denial_case(find_qcase("right_after_filter_wide"), VIA_SEAL);
    const char *exact[] = {"right_pair", "right_composite",
                           "right_later_keys", "right_after_filter_join",
                           "right_second_self", "right_second_filter"};
    for (size_t i = 0; i < sizeof(exact) / sizeof(exact[0]); i++) {
        uint64_t sealed = exact_case(find_qcase(exact[i]), VIA_SEAL);
        uint64_t loaded = exact_case(find_qcase(exact[i]), VIA_LOAD);
        if (sealed != loaded)
            fprintf(stderr, "exact %s: sealed %llu loaded %llu\n", exact[i],
                (unsigned long long)sealed, (unsigned long long)loaded);
        CHECK(sealed == loaded);
    }
    cold_both_case(true);
    cold_both_case(false);
    /* After a JOIN of the resident input with itself: stops in the scan
     * with each mode on both routes, in either JOIN with each mode on one
     * route, also with two keys; parks before the scan, in the earlier
     * JOIN and in the one that reads the copy; an allocation that either
     * JOIN cannot make, with one and with two keys; a read cancellation, a
     * read fault and the denials on both routes. */
    for (int mode = 1; mode <= 2; mode++) {
        stop_case(find_qcase("right_second_few"), VIA_SEAL, mode, 0);
        stop_case(find_qcase("right_second_few"), VIA_LOAD, mode, 0);
    }
    stop_case(find_qcase("right_second_few"), VIA_SEAL, 2, 2);
    stop_case(find_qcase("right_second_few"), VIA_LOAD, 1, 2);
    stop_case(find_qcase("right_second_composite_few"), VIA_LOAD, 1, 2);
    parked_destroy_case(find_qcase("right_second_self"));
    parked_prefix_case(find_qcase("right_second_self"), false);
    parked_prefix_case(find_qcase("right_second_self"), true);
    parked_first_join_case(find_qcase("right_second_self"));
    parked_join_case(find_qcase("right_second_self"), false);
    join_fault_case(find_qcase("right_second_self"));
    join_fault_case(find_qcase("right_second_composite"));
    read_cancel_case(find_qcase("right_second_self"));
    read_fault_case(find_qcase("right_second_self"));
    denial_case(find_qcase("right_second_self"), VIA_SEAL);
    denial_case(find_qcase("right_second_self"), VIA_LOAD);
    for (int v = 0; v < 2; v++) {
        via_t via = v ? VIA_LOAD : VIA_SEAL;
        key_type_case(via, 0);
        key_type_case(via, 1);
        second_absent_case(via, "right_second_self");
        second_absent_case(via, "right_second_filter");
    }
    /* With a right filter on a resident JOIN before or after the one that
     * reads the cold input: stops in the scan and in either JOIN, parks in
     * the earlier JOIN and in the one that reads the copy, faults, a read
     * cancellation and fault, and the denials on both routes. */
    for (int mode = 1; mode <= 2; mode++) {
        stop_case(find_qcase("right_second_filter_few"), VIA_SEAL, mode, 0);
        stop_case(find_qcase("right_second_filter_few"), VIA_LOAD, mode, 0);
    }
    stop_case(find_qcase("right_second_filter_few"), VIA_SEAL, 2, 2);
    stop_case(find_qcase("right_second_filter_few"), VIA_LOAD, 1, 2);
    parked_destroy_case(find_qcase("right_second_filter"));
    parked_first_join_case(find_qcase("right_second_filter"));
    parked_join_case(find_qcase("right_second_filter"), false);
    join_fault_case(find_qcase("right_second_filter"));
    join_fault_case(find_qcase("right_second_filter_then_join"));
    read_cancel_case(find_qcase("right_second_filter"));
    read_fault_case(find_qcase("right_second_filter"));
    denial_case(find_qcase("right_second_filter"), VIA_SEAL);
    denial_case(find_qcase("right_second_filter"), VIA_LOAD);
    denial_case(find_qcase("right_second_filter_then_join"), VIA_SEAL);
    cold_both_later_case(true);
    cold_both_later_case(false);
}

/* A cold right input read by two JOINs of the rule or more (#2301). */
static void
twice_cases(void)
{
    for (size_t i = 0; i < NQCASES; i++) {
        if (!qcases[i].twice) continue;
        run_qcase(&qcases[i], VIA_SEAL);
        run_qcase(&qcases[i], VIA_LOAD);
    }
    /* Stops in the scan with each mode on both routes, and at every JOIN
     * checkpoint, those of the JOIN that reads the copy again included,
     * with each mode on one route. */
    for (int mode = 1; mode <= 2; mode++) {
        stop_case(find_qcase("right_twice_few"), VIA_SEAL, mode, 0);
        stop_case(find_qcase("right_twice_few"), VIA_LOAD, mode, 0);
    }
    stop_case(find_qcase("right_twice_few"), VIA_SEAL, 2, 2);
    stop_case(find_qcase("right_twice_few"), VIA_LOAD, 1, 2);
    /* Parks after the scan, in the first JOIN that reads the copy and in a
     * later one; allocation faults in a later one. */
    parked_destroy_case(find_qcase("right_twice"));
    parked_join_case(find_qcase("right_twice"), false);
    parked_later_join_case(find_qcase("right_twice"), 2);
    parked_later_join_case(find_qcase("right_twice_between"), 3);
    parked_later_join_case(find_qcase("right_thrice"), 3);
    join_fault_case(find_qcase("right_twice"));
    join_fault_nth(find_qcase("right_twice"), 2);
    join_fault_nth(find_qcase("right_twice_between"), 3);
    join_fault_nth(find_qcase("right_thrice"), 3);
    join_fault_nth(find_qcase("right_twice_composite"), 2);
    join_fault_nth(find_qcase("right_twice_keys_second"), 2);
    join_fault_nth(find_qcase("right_twice_second"), 3);
    read_cancel_case(find_qcase("right_twice"));
    read_fault_case(find_qcase("right_twice"));
    denial_case(find_qcase("right_twice_second"), VIA_SEAL);
    denial_case(find_qcase("right_twice_second"), VIA_LOAD);
    const char *exact[] = {"right_twice", "right_twice_between"};
    for (size_t i = 0; i < sizeof(exact) / sizeof(exact[0]); i++) {
        uint64_t sealed = exact_case(find_qcase(exact[i]), VIA_SEAL);
        uint64_t loaded = exact_case(find_qcase(exact[i]), VIA_LOAD);
        if (sealed != loaded)
            fprintf(stderr, "exact %s: sealed %llu loaded %llu\n", exact[i],
                (unsigned long long)sealed, (unsigned long long)loaded);
        CHECK(sealed == loaded);
    }
    for (int v = 0; v < 2; v++) {
        via_t via = v ? VIA_LOAD : VIA_SEAL;
        key_type_case(via, 2);
        twice_absent_case(via, "right_twice", 0);
        twice_absent_case(via, "right_twice_after_filter", 0);
        twice_absent_case(via, "right_thrice", 0);
        twice_absent_case(via, "right_twice_composite", 0);
        twice_absent_case(via, "right_twice_between", ENOTSUP);
        twice_absent_case(via, "right_twice_second", ENOTSUP);
        twice_absent_case(via, "right_second_filter_twice", ENOTSUP);
    }
}

int
main(int argc, char **argv)
{
    wl_columnar_ops_test_cold_scan_checkpoint = cold_checkpoint;
    wl_columnar_ops_test_reduce_attempt_checkpoint = reduce_checkpoint;
    wl_columnar_join_test_attempt_checkpoint = join_checkpoint;
    if (argc > 1 && !strcmp(argv[1], "right")) {
        right_cases();
        wl_columnar_join_test_attempt_checkpoint = NULL;
        printf("evaluation_cold_sources right: ok\n");
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "twice")) {
        twice_cases();
        wl_columnar_join_test_attempt_checkpoint = NULL;
        printf("evaluation_cold_sources twice: ok\n");
        return 0;
    }
    for (size_t i = 0; i < NQCASES; i++) {
        if (qcases[i].cold_right) continue;
        run_qcase(&qcases[i], VIA_SEAL);
        run_qcase(&qcases[i], VIA_LOAD);
    }
    for (int mode = 1; mode <= 2; mode++) {
        stop_case(find_qcase("projection"), VIA_SEAL, mode, 0);
        stop_case(find_qcase("projection"), VIA_LOAD, mode, 0);
        stop_case(find_qcase("wide"), VIA_SEAL, mode, 0);
        stop_case(find_qcase("sum_filtered"), VIA_SEAL, mode, 0);
        stop_case(find_qcase("sum_few"), VIA_SEAL, mode, 1);
        stop_case(find_qcase("count_few"), VIA_LOAD, mode, 1);
        stop_case(find_qcase("filter_few"), VIA_SEAL, mode, 0);
    }
    /* The JOIN's sweeps are long; each stop mode runs on one route. */
    stop_case(find_qcase("filter_few"), VIA_SEAL, 2, 2);
    stop_case(find_qcase("pair_few"), VIA_LOAD, 1, 2);
    /* After a JOIN only the REDUCE's checkpoints are swept, with a
     * cancellation; stops at the scan and JOIN checkpoints before it are
     * not run for this shape. */
    stop_case(find_qcase("count_join_few"), VIA_SEAL, 1, 1);
    /* A JOIN with two keys: a work-limit stop at every JOIN checkpoint, and
     * a cancellation at every REDUCE checkpoint after one. */
    stop_case(find_qcase("composite_few"), VIA_SEAL, 2, 2);
    stop_case(find_qcase("composite_count_few"), VIA_SEAL, 1, 1);
    stop_case(find_qcase("later_keys_few"), VIA_SEAL, 2, 2);
    parked_destroy_case(find_qcase("wide"));
    parked_destroy_case(find_qcase("sum_filtered"));
    parked_destroy_case(find_qcase("filter_join"));
    parked_destroy_case(find_qcase("count_join"));
    overflow_case(VIA_SEAL, false);
    overflow_case(VIA_LOAD, false);
    overflow_case(VIA_LOAD, true);
    divergence_case("count_many");
    divergence_case("pair_many");
    read_cancel_case(find_qcase("projection"));
    read_cancel_case(find_qcase("sum_filtered"));
    read_cancel_case(find_qcase("filter_join"));
    read_cancel_case(find_qcase("count_join"));
    read_fault_case(find_qcase("projection"));
    read_fault_case(find_qcase("sum_filtered"));
    read_fault_case(find_qcase("filter_join"));
    read_fault_case(find_qcase("count_join"));
    denial_case(find_qcase("projection"), VIA_SEAL);
    denial_case(find_qcase("projection"), VIA_LOAD);
    denial_case(find_qcase("sum_filtered"), VIA_SEAL);
    denial_case(find_qcase("sum_filtered"), VIA_LOAD);
    denial_case(find_qcase("filter_few"), VIA_SEAL);
    denial_case(find_qcase("count_wide_join"), VIA_SEAL);
    const char *exact[] = {"projection", "sum_filtered", "filter_join",
                           "count_join", "composite_join", "later_keys_join"};
    for (int i = 0; i < 6; i++) {
        uint64_t sealed = exact_case(find_qcase(exact[i]), VIA_SEAL);
        uint64_t loaded = exact_case(find_qcase(exact[i]), VIA_LOAD);
        if (sealed != loaded)
            fprintf(stderr, "exact %s: sealed %llu loaded %llu\n", exact[i],
                (unsigned long long)sealed, (unsigned long long)loaded);
        CHECK(sealed == loaded);
    }
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
    parked_cancel_case();
    right_insert_case(find_qcase("pair_join"), VIA_SEAL);
    right_insert_case(find_qcase("pair_join"), VIA_LOAD);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    printf("evaluation_cold_sources: ok\n");
    return 0;
}
