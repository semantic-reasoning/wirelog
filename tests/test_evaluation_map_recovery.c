/* #2299: generic hot scalar MAP failures retain public correction/retry. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/session.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); exit(1); } } while (0)
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session;
    wl_evaluation_control_t *control;
    unsigned events, positive, negative;
    int balance[2];
    bool snapshot_seen[2];
    unsigned snapshot_count;
} fixture_t;
static int complete(wl_session_t *session) {
    for (unsigned i = 0; i < 10000; ++i) {
        int rc = wl_session_step(session); if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EAGAIN;
}
static void capture(const char *name, const int64_t *row, uint32_t columns,
    int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(name, "out") && columns == 2 && (diff == 1 || diff == -1));
    const char *text = wl_intern_reverse(f->plan->intern, row[0]); CHECK(text);
    unsigned index = !strcmp(text, "12tail")?0:1;
    CHECK(!strcmp(text, index?" -3 ":"12tail") && row[1] == (index?-3:12));
    f->balance[index] += diff;
    CHECK(f->balance[index] >= 0 && f->balance[index] <= 1);
    ++f->events; f->positive += diff == 1; f->negative += diff == -1;
}
static fixture_t create_expression(unsigned nested) {
    fixture_t f = {0}; wirelog_error_t error;
    f.program = wirelog_parse_string(nested == 3 ?
            ".decl input(x:string)\n.decl out(x:string)\n"
            "out(x) :- input(x),hash(x)=hash(\"a\").\n" : nested == 2 ?
            ".decl input(x:string)\n.decl out(x:string,n:int64)\n"
            "out(x,to_number(x)) :- input(x),strlen(x)>0.\n" : nested ?
            ".decl input(x:string)\n.decl out(x:string,n:int64)\n"
            "out(x,to_number(cat(x,\" \"))) :- input(x).\n" :
            ".decl input(x:string)\n.decl out(x:string,n:int64)\n"
            "out(x,to_number(x)) :- input(x).\n", &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    CHECK(!wl_evaluation_control_create(0, &f.control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = f.control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session));
    return f;
}
static fixture_t create(void) {
    return create_expression(false);
}
static int64_t insert(fixture_t *f, const char *text) {
    int64_t id = wl_intern_put(f->plan->intern, text); CHECK(id >= 0);
    CHECK(!wl_session_insert(f->session, "input", &id, 1, 1)); return id;
}
static void contents(fixture_t *f, unsigned expected) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == expected && out->ncols == 2 &&
        out->column_types[0] == WIRELOG_TYPE_STRING &&
        out->column_types[1] == WIRELOG_TYPE_INT64);
    bool seen[2] = {false, false};
    for (uint32_t r = 0; r < out->nrows; ++r) {
        const char *text = wl_intern_reverse(f->plan->intern,
                out->columns[0][r]); CHECK(text);
        unsigned i = !strcmp(text, "12tail")?0:1;
        CHECK(!seen[i] && !strcmp(text,
            i?" -3 ":"12tail") && out->columns[1][r] == (i?-3:12));
        seen[i] = true;
    }
}
static void destroy(fixture_t *f) {
    wl_columnar_memory_governor_ref_t *g =
        COL_SESSION(f->session)->memory_governor;
    wl_columnar_memory_governor_ref_retain(g);
    wl_session_destroy(f->session); wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    wl_evaluation_control_release(f->control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
static unsigned conversions;
static void overflow(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress; (void)failed;
    if (phase == 100)++conversions;
    if (c && phase == 101 && conversions == 2) {
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
        wl_evaluation_control_request_cancel(c);
    }
}
static void same_report(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b) {
    CHECK(a->attempt_id == b->attempt_id && a->limit == b->limit &&
        a->consumed == b->consumed &&
        a->stop_reason == b->stop_reason &&
        a->execution_status == b->execution_status &&
        a->cleanup_status == b->cleanup_status && a->result == b->result);
}
static void snapshot(const char *name, const int64_t *row, uint32_t columns,
    void *arg) {
    fixture_t *f = arg; CHECK(!strcmp(name, "out") && columns == 2);
    const char *text = wl_intern_reverse(f->plan->intern, row[0]); CHECK(text);
    unsigned i = !strcmp(text, "12tail")?0:1;
    CHECK(!f->snapshot_seen[i] && !strcmp(text,
        i?" -3 ":"12tail") && row[1] == (i?-3:12));
    f->snapshot_seen[i] = true; ++f->snapshot_count;
}
static void terminal(void) {
    for (unsigned prior = 0; prior < 2; ++prior)for (unsigned error = 0;
            error < 3;
            ++error){
            fixture_t f = create();
            wl_session_set_delta_cb(f.session, capture, &f);
            int64_t good = prior?insert(&f, "12tail"):-1;
            CHECK(!complete(f.session)); contents(&f, prior);
            col_rel_t *committed = session_find_rel(COL_SESSION(f.session),
                    "out");
            col_relation_snapshot_t generation =
                wl_columnar_relation_snapshot(committed);
            unsigned before = f.events;
            if (!prior) good = insert(&f, "12tail");
            int64_t other = insert(&f, " -3 ");
            int64_t bad = error == 1?-1:insert(&f, "9223372036854775808");
            if (error == 1) wl_columnar_expr_test_fail_allocation = 0;
            if (error == 2) {
                conversions = 0; wl_columnar_expr_test_checkpoint = overflow;
            }
            int expected = error == 0?ERANGE:error == 1?ENOMEM:EOVERFLOW;
            CHECK(complete(f.session) == expected);
            wl_columnar_expr_test_fail_allocation = -1;
            wl_columnar_expr_test_checkpoint = NULL;
            contents(&f, prior); CHECK(f.events == before);
            CHECK(session_find_rel(COL_SESSION(f.session),
                "out") == committed &&
                wl_columnar_relation_snapshot_equal(generation,
                wl_columnar_relation_snapshot(committed)));
            CHECK(!COL_SESSION(f.session)->cleanup_active &&
                !COL_SESSION(f.session)->cleanup_pending &&
                !COL_SESSION(f.session)->cleanup_reserved_bytes);
            wl_evaluation_control_report_t frozen, report;
            CHECK(!wl_evaluation_control_report(f.control,
                &frozen) && frozen.result == expected &&
                frozen.execution_status == expected);
            /* A retry without a correction must not pretend the old image is a
             * newly successful result. The permanently invalid input still fails. */
            if (error == 0) {
                CHECK(!wl_evaluation_control_reset(f.control));
                CHECK(complete(f.session) == ERANGE); contents(&f, prior);
                CHECK(f.events == before);
                CHECK(!wl_evaluation_control_report(f.control,
                    &report) && report.result == ERANGE &&
                    report.attempt_id > frozen.attempt_id); frozen = report;
            }
            CHECK(wl_session_remove(f.session, "missing", &good, 1,
                1) == ENOTSUP);
            int64_t wrong_width[2] = {good, 0};
            CHECK(wl_session_remove(f.session, "input", wrong_width, 1,
                2) == ENOTSUP);
            CHECK(!wl_session_remove(f.session, "input", &good, 0, 1));
            CHECK(wl_session_snapshot(f.session, snapshot,
                &f) == EBUSY && !f.snapshot_count);
            CHECK(!wl_evaluation_control_report(f.control, &report));
            same_report(&frozen, &report);
            if (prior && error == 0) {
                /* An unrelated live generation grants no correction or replay.
                 * Closing it does not erase the earlier authentic terminal receipt. */
                CHECK(!wl_evaluation_control_reset(f.control));
                wl_evaluation_control_attempt_begin_result_t next =
                    wl_evaluation_control_begin_attempt(
                    f.control, f.session->operation_admission);
                CHECK(!next.protocol && next.attempt_id);
                CHECK(wl_session_remove(f.session, "input", &bad, 1,
                    1) == EBUSY);
                CHECK(complete(f.session) == EBUSY);
                uint64_t used = 999;
                CHECK(wl_evaluation_control_active_attempt_id(f.control) ==
                    next.attempt_id &&
                    !wl_evaluation_control_test_active_consumed(f.control,
                    next.attempt_id, &used) && !used);
                CHECK(!wl_evaluation_control_report(f.control, &report));
                same_report(&frozen, &report);
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.session->operation_admission, next.attempt_id,
                    0, 0);
                CHECK(!end.protocol && end.report_valid && !end.report.result &&
                    !end.report.consumed);
                frozen = end.report;
            }
            int rc;
            if (error == 1) {
                /* Successful remove/reinsert cancels logically, but both real
                 * generations remain authorized and the next image is rebuilt. */
                CHECK(!wl_session_remove(f.session, "input", &good, 1, 1));
                CHECK(!wl_session_insert(f.session, "input", &good, 1, 1));
                rc = 0;
            } else rc = wl_session_remove(f.session, "input", &bad, 1, 1);
            if (rc) fprintf(stderr,
                    "terminal prior=%u error=%u correction=%d\n", prior, error,
                    rc);
            CHECK(!rc);
            CHECK(!wl_evaluation_control_report(f.control, &report));
            same_report(&frozen, &report);
            CHECK(!wl_evaluation_control_reset(f.control));
            if (prior && error == 0) {
                CHECK(!wl_session_snapshot(f.session, snapshot, &f));
                CHECK(f.snapshot_count == 2 && f.snapshot_seen[0] &&
                    f.snapshot_seen[1]);
            } else CHECK(!complete(f.session));
            contents(&f, 2);
            CHECK(f.events == 2 && f.positive == 2 && !f.negative);
            CHECK(!complete(f.session) && f.events == 2);
            CHECK(!wl_session_remove(f.session, "input", &other, 1, 1));
            CHECK(!complete(f.session)); contents(&f, 1);
            CHECK(f.events == 3 && f.negative == 1 && f.balance[0] == 1 &&
                !f.balance[1]);
            CHECK(!complete(f.session) && f.events == 3);
            CHECK(!wl_session_remove(f.session, "input", &good, 1, 1));
            CHECK(!complete(f.session)); contents(&f, 0);
            CHECK(f.events == 4 && f.negative == 2 && !f.balance[0]);
            destroy(&f);
        }
}
static fixture_t *active;
static unsigned rebind_calls;
static bool ended, start_successor;
static uint64_t successor;
static unsigned target_phase, target_domain, finance_refusals, finance_hits;
static bool accounting_error, expect_filter;
static int expected_error;
static wl_evaluation_control_report_t original_report;
static uint32_t owned_receipt(const wl_col_session_t *sess, uint64_t id) {
    for (uint32_t i = 0; i < 32; ++i)
        if (sess->cleanup_receipts[i].state !=
            WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE
            && sess->cleanup_receipts[i].key.operation_attempt_id ==
            id) return i;
    return UINT32_MAX;
}
static void rebind(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress; (void)failed;
    if (phase == 100) ++rebind_calls;
    if (ended && c) CHECK(false); /* Native selection never resumes expression work. */
    if (!c || ended || target_domain || phase != target_phase
        || (target_phase == 101 && rebind_calls < 2)) return;
    if (expect_filter) {
        wl_col_session_t *sess = COL_SESSION(active->session);
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        CHECK(!wl_columnar_eval_stack_framed_context(sess->cleanup_active, sess,
            &ctx)
            && ctx->child.cursor < ctx->plan->op_count
            && ctx->plan->ops[ctx->child.cursor].op == WL_PLAN_OP_FILTER);
    }
    wl_col_session_t *owned = COL_SESSION(active->session);
    CHECK(owned_receipt(owned,
        wl_evaluation_control_active_attempt_id(c)) < 32);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    if (accounting_error) CHECK(wl_evaluation_control_charge_attempt(c, id,
            UINT64_MAX) == EOVERFLOW);
    wl_evaluation_control_attempt_finish_result_t finish =
        wl_evaluation_control_finish_attempt(c,
            active->session->operation_admission, id, 0, 0);
    CHECK(!finish.protocol && finish.report_valid
        && finish.report.result == (accounting_error ? EOVERFLOW : 0)
        && finish.report.consumed);
    original_report = finish.report; ended = true;
    if (start_successor) {
        CHECK(!wl_evaluation_control_reset(c));
        wl_evaluation_control_attempt_begin_result_t next =
            wl_evaluation_control_begin_attempt(c,
                active->session->operation_admission);
        CHECK(!next.protocol && next.attempt_id != id);
        successor = next.attempt_id;
    }
}
static void other_rebind(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress) {
    if (!c || ended || !target_domain || phase != target_phase
        || (target_domain == 1 && phase == 142 && progress)) return;
    unsigned saved_domain = target_domain, saved_phase = target_phase;
    target_domain = 0; target_phase = 999;
    rebind(c, 999, progress, false);
    target_domain = saved_domain; target_phase = saved_phase;
}
static uint64_t finance_limit(const wl_col_session_t *sess,
    wl_columnar_session_metadata_purpose_t purpose, uint64_t limit) {
    (void)sess;
    if (purpose != WL_COLUMNAR_SESSION_METADATA_UNARY_CLEANUP ||
        !ended) return limit;
    ++finance_hits;
    return finance_hits <= finance_refusals ? 1 : limit;
}
#if defined(WL_HAVE_C11_THREADS)
static int foreign_step(void *arg) {
    return wl_session_step(arg);
}
#endif
static void displaced_receipt(fixture_t *f) {
    wl_col_session_t *sess = COL_SESSION(f->session);
    uint32_t slot = owned_receipt(sess, original_report.attempt_id);
    CHECK(slot < 31);
    wl_columnar_eval_cleanup_receipt_t original = sess->cleanup_receipts[slot];
    CHECK(original.state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE
        && sess->cleanup_receipts[31].state ==
        WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE);
    /* Test-only corruption: generic find could reach the matching key at31.
     * The confined owner must refuse and protect its debt without scanning. */
    sess->cleanup_receipts[31] = original;
    sess->cleanup_receipts[slot] = (wl_columnar_eval_cleanup_receipt_t){0};
    CHECK(wl_session_step(f->session) == EINVAL);
    int64_t input = wl_intern_get(f->plan->intern, "12tail");
    CHECK(wl_session_remove(f->session, "input", &input, 1, 1) == EBUSY
        && !f->events && sess->cleanup_receipts[31].state == original.state
        && sess->cleanup_receipts[31].key.receipt_id ==
        original.key.receipt_id);
    sess->cleanup_receipts[slot] = original;
    sess->cleanup_receipts[31] = (wl_columnar_eval_cleanup_receipt_t){0};
}
static void refused_proofs(fixture_t *f) {
    wl_col_session_t *sess = COL_SESSION(f->session);
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    CHECK(sess->cleanup_active && !wl_columnar_eval_stack_framed_context(
            sess->cleanup_active, sess, &ctx));
    int cause = 999;
    CHECK(wl_columnar_session_unary_cleanup_prove(ctx, f->control,
        successor ? successor : original_report.attempt_id, NULL,
        &cause) == EINVAL);
    /* A copy is never the registered owner even with identical field values.
     * Its rejection must leave the actual frame's debt protected. */
    wl_columnar_eval_serial_framed_attempt_t copy = *ctx;
    wl_evaluation_control_t *funding = NULL;
    CHECK(!wl_evaluation_control_create(8192, &funding)
        && !wl_evaluation_control_attach(funding, &copy)
        && !wl_evaluation_control_begin(funding, &copy));
    CHECK(wl_columnar_session_unary_cleanup_prove(&copy, funding,
        wl_evaluation_control_active_attempt_id(funding), NULL,
        &cause) == EINVAL);
    CHECK(!wl_evaluation_control_finish(funding, &copy, 0, 0)
        && !wl_evaluation_control_detach(funding, &copy));
    wl_evaluation_control_release(funding);
#if defined(WL_HAVE_C11_THREADS)
    thrd_t thread; int result;
    CHECK(thrd_create(&thread, foreign_step, f->session) == thrd_success
        && thrd_join(thread, &result) == thrd_success && result == EBUSY);
#endif
    int64_t input = wl_intern_get(f->plan->intern, "12tail");
    CHECK(wl_session_remove(f->session, "input", &input, 1, 1) == EBUSY);
    CHECK(wl_session_snapshot(f->session, snapshot, f) == EBUSY
        && !f->snapshot_count && !f->events);
    CHECK(sess->cleanup_active);
    displaced_receipt(f);
}
static int native_complete(fixture_t *f) {
    bool probed = false, terminal_probed = false;
    for (unsigned i = 0; i < 10000; ++i) {
        int rc = wl_session_step(f->session);
        if (ended && !probed && finance_hits >= finance_refusals
            && COL_SESSION(f->session)->cleanup_active) {
            refused_proofs(f); probed = true;
        }
        uint32_t slot = owned_receipt(COL_SESSION(f->session),
                original_report.attempt_id);
        if (ended && !terminal_probed && slot < 32 &&
            !COL_SESSION(f->session)->cleanup_active
            && COL_SESSION(f->session)->cleanup_receipts[slot].state
            == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED){
            displaced_receipt(f); terminal_probed = true;
        }
        if (rc != EAGAIN) {
            CHECK(terminal_probed);
            if (!probed) fprintf(stderr,
                    "no native probe phase=%u domain=%u ended=%u rc=%d finance=%u/%u\n",
                    target_phase, target_domain, ended, rc, finance_hits,
                    finance_refusals);
            CHECK(probed); return rc;
        }
    }
    CHECK(false); return EAGAIN;
}
static void native_rebind(void) {
    const unsigned phases[] = {101, 4, 5, 6, 137, 141, 142, 2, 4, 83};
    const unsigned domains[] = {0, 0, 0, 0, 0, 1, 1, 2, 2, 0};
    for (unsigned cut = 0; cut < sizeof(phases)/sizeof(phases[0]); ++cut)
        for (unsigned action = 0; action < 6; ++action) {
            fixture_t f = create_expression(cut == 9 ? 2 : cut != 0);
            active = &f;
            wl_session_set_delta_cb(f.session, capture, &f);
            CHECK(!complete(f.session)); contents(&f, 0);
            col_rel_t *prior = session_find_rel(COL_SESSION(f.session), "out");
            col_relation_snapshot_t generation =
                wl_columnar_relation_snapshot(prior);
            insert(&f, "12tail"); insert(&f, " -3 ");
            start_successor = action & 1; ended = false; successor = 0;
            rebind_calls = 0;
            target_phase = phases[cut]; target_domain = domains[cut];
            expect_filter = cut == 9;
            accounting_error = action == 2 || action == 3;
            expected_error = accounting_error ? EOVERFLOW : EINVAL;
            finance_refusals = action == 3 ? 2 : 0; finance_hits = 0;
            wl_columnar_session_test_metadata_limit = finance_limit;
            wl_columnar_expr_test_checkpoint = rebind;
            wl_columnar_filter_test_attempt_checkpoint = other_rebind;
            wl_intern_control_test_checkpoint = target_domain ==
                2 ? other_rebind : NULL;
            int rc = action >= 4 ? wl_session_snapshot(f.session, snapshot,
                    &f) : native_complete(&f);
            wl_columnar_expr_test_checkpoint = NULL;
            wl_columnar_filter_test_attempt_checkpoint = NULL;
            wl_intern_control_test_checkpoint = NULL;
            wl_columnar_session_test_metadata_limit = NULL;
            if (!ended || rc != expected_error) fprintf(stderr,
                    "cut=%u phase=%u domain=%u action=%u ended=%u rc=%d expected=%d\n",
                    cut, target_phase, target_domain, action, ended, rc,
                    expected_error);
            CHECK(ended && rc == expected_error &&
                finance_hits > finance_refusals);
            CHECK(cut || rebind_calls == 2);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report));
            same_report(&original_report, &report);
            if (successor) {
                uint64_t used = 999;
                CHECK(wl_evaluation_control_active_attempt_id(f.control) ==
                    successor
                    && !wl_evaluation_control_test_active_consumed(f.control,
                    successor, &used)
                    && !used);
            }
            CHECK(!f.events && !COL_SESSION(f.session)->cleanup_active
                && !COL_SESSION(f.session)->cleanup_pending
                && !COL_SESSION(f.session)->cleanup_reserved_bytes);
            CHECK(session_find_rel(COL_SESSION(f.session), "out") == prior
                && wl_columnar_relation_snapshot_equal(generation,
                wl_columnar_relation_snapshot(prior)));
            if (successor) {
                wl_evaluation_control_attempt_finish_result_t finish =
                    wl_evaluation_control_finish_attempt(f.control,
                        f.session->operation_admission, successor, 0, 0);
                CHECK(!finish.protocol && finish.report_valid &&
                    !finish.report.consumed);
            }
            CHECK(!wl_evaluation_control_reset(f.control));
            if (action >= 4) {
                CHECK(!wl_session_snapshot(f.session, snapshot, &f)
                    && f.snapshot_count == 2 && f.snapshot_seen[0] &&
                    f.snapshot_seen[1]);
            } else CHECK(!complete(f.session));
            contents(&f, 2);
            CHECK(f.events == 2 && f.positive == 2 && !f.negative);
            CHECK(!complete(f.session) && f.events == 2);
            int64_t other = wl_intern_get(f.plan->intern, " -3 ");
            CHECK(!wl_session_remove(f.session, "input", &other, 1, 1)
                && !complete(f.session)); contents(&f, 1);
            CHECK(f.events == 3 && f.negative == 1 && f.positive == 2);
            CHECK(!wl_session_insert(f.session, "input", &other, 1, 1)
                && !complete(f.session)); contents(&f, 2);
            CHECK(f.events == 4 && f.negative == 1 && f.positive == 3
                && !complete(f.session) && f.events == 4);
            destroy(&f);
        }
}
static void indexed_delta(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(name, "out") && width == 1 && (diff == 1 || diff == -1)
        && !strcmp(wl_intern_reverse(f->plan->intern, row[0]), "a"));
    ++f->events; f->positive += diff == 1; f->negative += diff == -1;
}
static void indexed_contents(fixture_t *f, unsigned rows) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == rows && out->ncols == 1 &&
        out->column_types[0] == WIRELOG_TYPE_STRING);
    if (rows)CHECK(!strcmp(wl_intern_reverse(f->plan->intern,
            out->columns[0][0]), "a"));
}
static void indexed_history(void) {
    fixture_t f = create_expression(3); active = &f;
    CHECK(!complete(f.session)); indexed_contents(&f, 0);
    wl_col_session_t *sess = COL_SESSION(f.session);
    wl_columnar_eval_cleanup_receipt_t historical = sess->cleanup_receipts[0];
    CHECK(historical.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
        && historical.committed && !historical.result);
    wl_session_set_delta_cb(f.session, indexed_delta, &f);
    int64_t rows[] = {wl_intern_put(f.plan->intern, "a"),
                      wl_intern_put(f.plan->intern, "zzz")};
    CHECK(rows[0] >= 0 && rows[1] >= 0 && !wl_session_insert(f.session, "input",
        rows, 2, 1));
    ended = false; start_successor = true; successor = 0; rebind_calls = 0;
    target_phase = 83; target_domain = 0; expect_filter = true;
    accounting_error = false;
    finance_hits = finance_refusals = 0;
    wl_columnar_expr_test_checkpoint = rebind;
    wl_columnar_session_test_metadata_limit = finance_limit;
    CHECK(native_complete(&f) == EINVAL && ended && successor);
    wl_columnar_expr_test_checkpoint = NULL;
    wl_columnar_session_test_metadata_limit = NULL;
    CHECK(!memcmp(&historical, &sess->cleanup_receipts[0], sizeof(historical))
        && !sess->cleanup_active && !sess->cleanup_pending &&
        !sess->cleanup_reserved_bytes);
    uint64_t used = 999;
    CHECK(!wl_evaluation_control_test_active_consumed(f.control, successor,
        &used) && !used);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.control, &report));
    same_report(&original_report, &report);
    wl_evaluation_control_attempt_finish_result_t finish =
        wl_evaluation_control_finish_attempt(
        f.control, f.session->operation_admission, successor, 0, 0);
    CHECK(!finish.protocol && finish.report_valid && !finish.report.consumed);
    CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session));
    indexed_contents(&f, 1);
    CHECK(f.events == 1 && f.positive == 1 && !f.negative
        && !memcmp(&historical, &sess->cleanup_receipts[0],
        sizeof(historical)));
    CHECK(!wl_session_remove(f.session, "input", rows, 1,
        1) && !complete(f.session));
    indexed_contents(&f, 0);
    CHECK(f.events == 2 && f.negative == 1 && !complete(f.session) &&
        f.events == 2);
    destroy(&f);
}
static uint64_t registration_cap, registration_used;
static bool registration_injected, registration_refused;
static unsigned registration_rows;
static uint64_t registration_limit(const wl_col_session_t *sess,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t limit) {
    if (!registration_injected && !sess->cleanup_active
        && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE) {
        registration_injected = true; return registration_cap;
    }
    return limit;
}
static void registration_finished(const wl_col_session_t *sess,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *grant, int protocol) {
    (void)action; (void)protocol;
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant, &report));
    if (report.limit != registration_cap) return;
    CHECK(!registration_refused &&
        report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && report.consumed == registration_used && !report.execution_status
        && !report.cleanup_status && !sess->cleanup_active
        && !sess->cleanup_pending && !sess->cleanup_reserved_bytes);
    registration_refused = true;
}
static void registration_forward(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)c; (void)progress; (void)failed;
    if (phase == 100) {
        CHECK(registration_refused); ++registration_rows;
    }
}
static void registration_denials(void) {
    /* Outer384, lookup128/192/128, inner384; fresh registration now has
     * ENTRY,threeOP,ROOT,BINDING,KEY,INDEX,PUBLISH at256 each, including
     * saved-Original observation. Existing lookup/bootstrap prices unchanged. */
    const uint64_t caps[] = {511, 703, 831, 1215, 1471, 1727, 1983, 2239, 2495,
                             2751, 3007, 3263, 3519};
    const uint64_t consumed[] = {384, 512, 704, 832, 1216, 1472, 1728, 1984,
                                 2240, 2496, 2752, 3008, 3264};
    for (unsigned i = 0; i < sizeof(caps)/sizeof(caps[0]); ++i) {
        fixture_t f = create(); wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session)); insert(&f, "12tail"); insert(&f, " -3 ");
        registration_cap = caps[i]; registration_used = consumed[i];
        registration_injected = registration_refused = false;
        registration_rows = 0;
        wl_columnar_session_test_framed_grant_limit = registration_limit;
        wl_columnar_session_test_framed_grant_finished = registration_finished;
        wl_columnar_expr_test_checkpoint = registration_forward;
        CHECK(!complete(f.session));
        wl_columnar_session_test_framed_grant_limit = NULL;
        wl_columnar_session_test_framed_grant_finished = NULL;
        wl_columnar_expr_test_checkpoint = NULL;
        CHECK(registration_injected && registration_refused &&
            registration_rows == 2
            && f.events == 2 && f.positive == 2 && !f.negative);
        contents(&f, 2);
        for (unsigned slot = 0; slot < 32; ++slot)
            CHECK(COL_SESSION(f.session)->cleanup_receipts[slot].state
                == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE);
        destroy(&f);
    }
}
static int lookup_owner;
static unsigned lookup_phase, lookup_mode;
static uint32_t lookup_cut;
static bool lookup_stopped;
static uint64_t lookup_successor;
static void lookup_stop(wl_evaluation_control_t *c, unsigned phase,
    uint32_t index) {
    if (lookup_stopped || phase != lookup_phase ||
        (phase == 1 && index != lookup_cut)) return;
    lookup_stopped = true;
    if (!lookup_mode) {
        wl_evaluation_control_request_cancel(c); return;
    }
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    wl_evaluation_control_attempt_finish_result_t finish =
        wl_evaluation_control_finish_attempt(c, &lookup_owner, id, 0, 0);
    CHECK(!finish.protocol && finish.report_valid && !finish.report.result);
    CHECK(!wl_evaluation_control_reset(c));
    wl_evaluation_control_attempt_begin_result_t begin =
        wl_evaluation_control_begin_attempt(c, &lookup_owner);
    CHECK(!begin.protocol && begin.attempt_id != id);
    lookup_successor = begin.attempt_id;
}
static void lookup_case(unsigned preceding, uint64_t limit, int expected,
    unsigned stop_phase, unsigned mode) {
    fixture_t f = create(); wl_col_session_t *sess = COL_SESSION(f.session);
    wl_evaluation_control_attempt_begin_result_t operation =
        wl_evaluation_control_begin_attempt(
        f.control, f.session->operation_admission);
    CHECK(!operation.protocol && operation.attempt_id);
    uint64_t identity = (uint64_t)(uintptr_t)&f;
    wl_columnar_eval_cleanup_receipt_key_t prior[31], actual;
    for (unsigned i = 0; i < preceding;
        ++i) CHECK(!wl_columnar_eval_stack_cleanup_receipt_reserve(
                sess, operation.attempt_id, identity, &prior[i]));
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(!wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
            sess, &frame,
            f.control, operation.attempt_id, identity, &actual));
    CHECK(sess->cleanup_receipts[preceding].state ==
        WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_RESERVED);
    uint64_t before = 0, after = 0;
    CHECK(!wl_evaluation_control_test_active_consumed(f.control,
        operation.attempt_id, &before));
    wl_evaluation_control_t *funding = NULL;
    CHECK(!wl_evaluation_control_create(limit,
        &funding) && !wl_evaluation_control_attach(funding, &lookup_owner)
        && !wl_evaluation_control_begin(funding, &lookup_owner));
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    wl_columnar_eval_cleanup_receipt_key_t output, sentinel;
    memset(&sentinel, 0x5a, sizeof(sentinel)); output = sentinel;
    uint32_t index = UINT32_MAX;
    wl_columnar_eval_cleanup_receipt_t saved =
        sess->cleanup_receipts[preceding];
    if (expected ==
        ENOENT) sess->cleanup_receipts[preceding] =
            (wl_columnar_eval_cleanup_receipt_t){0};
    lookup_stopped = false; lookup_successor = 0; lookup_phase = stop_phase;
    lookup_cut = preceding; lookup_mode = mode;
    wl_columnar_eval_stack_test_receipt_lookup_checkpoint = stop_phase <
        3?lookup_stop:NULL;
    int rc = wl_columnar_eval_stack_cleanup_receipt_find_frame_attempt(frame,
            &index, &output, funding, id);
    wl_columnar_eval_stack_test_receipt_lookup_checkpoint = NULL;
    if (expected == ENOENT)sess->cleanup_receipts[preceding] = saved;
    CHECK(rc == expected &&
        !wl_evaluation_control_test_active_consumed(f.control,
        operation.attempt_id, &after)
        && after == before && sess->cleanup_active == frame);
    if (!rc)CHECK(index == preceding && !memcmp(&actual, &output,
            sizeof(actual)));
    else CHECK(index == UINT32_MAX && !memcmp(&output, &sentinel,
            sizeof(output)));
    if (stop_phase < 3)CHECK(lookup_stopped);
    if (lookup_successor) {
        uint64_t used = 999;
        CHECK(!wl_evaluation_control_test_active_consumed(funding,
            lookup_successor, &used) && !used);
    }
    int financial = wl_evaluation_control_finish(funding, &lookup_owner, 0, 0);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report));
    CHECK(report.result == financial && !report.execution_status &&
        !report.cleanup_status);
    if (!rc)CHECK(!financial && report.consumed == limit);
    else if (stop_phase >=
        3) CHECK(financial ==
            (expected == WL_EVALUATION_CONTROL_WORK_LIMIT?expected:0)
            && report.consumed == (expected == ENOENT?6272:limit-127));
    CHECK(!wl_evaluation_control_detach(funding, &lookup_owner));
    wl_evaluation_control_release(funding);
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_cancel_begin(frame, &actual)
        && !wl_columnar_eval_stack_cleanup_finish(&frame) && !frame);
    for (unsigned i = 0; i < preceding;
        ++i) CHECK(!wl_columnar_eval_stack_cleanup_receipt_release(sess,
            &prior[i]));
    CHECK(!sess->cleanup_active && !sess->cleanup_pending &&
        !sess->cleanup_reserved_bytes);
    wl_evaluation_control_attempt_finish_result_t finish =
        wl_evaluation_control_finish_attempt(
        f.control, f.session->operation_admission, operation.attempt_id, 0, 0);
    CHECK(!finish.protocol && finish.report_valid && !finish.report.result);
    destroy(&f);
}
static void indexed_lookup(void) {
    const unsigned positions[] = {0, 1, 31};
    const uint64_t fees[] = {448, 640, 6400};
    for (unsigned i = 0; i < 3; ++i) {
        lookup_case(positions[i], fees[i], 0, 3, 0);
        lookup_case(positions[i], fees[i]-1, WL_EVALUATION_CONTROL_WORK_LIMIT,
            3, 0);
        lookup_case(positions[i], 6272, ENOENT, 3, 0);
        for (unsigned phase = 0; phase < 3; ++phase)for (unsigned mode = 0;
                mode < 2;
                ++mode)
                lookup_case(positions[i], 0,
                    mode?EINVAL:WL_EVALUATION_CONTROL_CANCELLED, phase, mode);
    }
}
static unsigned boundary_cut;
static void boundary_finished(const wl_col_session_t *read,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *funding, int protocol) {
    (void)funding; (void)protocol;
    if (ended)return;
    wl_col_session_t *sess = (wl_col_session_t *)read;
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    bool select = false;
    if (sess->cleanup_active &&
        !wl_columnar_eval_stack_framed_context(sess->cleanup_active, sess, &ctx)
        && ctx->identity == ctx) {
        select = boundary_cut == 0?ctx->phase ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP
            :boundary_cut == 1?ctx->phase ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
            && ctx->pending_action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN
            :boundary_cut == 2?ctx->phase ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
            && ctx->pending_action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD
            && ctx->child.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
            && ctx->child.pending_action ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
            :boundary_cut == 3?ctx->phase ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
            && ctx->pending_action ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_ACK:false;
    } else if (!sess->cleanup_active) {
        uint64_t id = wl_evaluation_control_active_attempt_id(active->control);
        uint32_t slot = owned_receipt(sess, id);
        select = boundary_cut == 4?action ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE && slot < 32
            && sess->cleanup_receipts[slot].state ==
            WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
            :boundary_cut == 5 &&
            action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK;
    }
    if (!select)return;
    uint64_t id = wl_evaluation_control_active_attempt_id(active->control);
    wl_evaluation_control_attempt_finish_result_t finish =
        wl_evaluation_control_finish_attempt(
        active->control, active->session->operation_admission, id, 0, 0);
    CHECK(!finish.protocol && finish.report_valid && !finish.report.result);
    original_report = finish.report; ended = true;
    CHECK(!wl_evaluation_control_reset(active->control));
    wl_evaluation_control_attempt_begin_result_t begin =
        wl_evaluation_control_begin_attempt(
        active->control, active->session->operation_admission);
    CHECK(!begin.protocol && begin.attempt_id != id);
    successor = begin.attempt_id;
}
static void grant_boundaries(void) {
    for (unsigned snapshot_entry = 0; snapshot_entry < 2; ++snapshot_entry)
        for (boundary_cut = 0; boundary_cut < 6; ++boundary_cut) {
            fixture_t f = create(); active = &f;
            wl_session_set_delta_cb(f.session, capture, &f);
            CHECK(!complete(f.session)); insert(&f, "12tail");
            insert(&f, " -3 ");
            ended = false; successor = 0;
            wl_columnar_session_test_framed_grant_finished = boundary_finished;
            int rc = snapshot_entry?wl_session_snapshot(f.session, snapshot,
                    &f):complete(f.session);
            wl_columnar_session_test_framed_grant_finished = NULL;
            if (rc != EINVAL || !ended) fprintf(stderr,
                    "boundary=%u ended=%u rc=%d\n", boundary_cut, ended, rc);
            CHECK(ended && rc == EINVAL && !f.events &&
                !COL_SESSION(f.session)->cleanup_active
                && !COL_SESSION(f.session)->cleanup_pending &&
                !COL_SESSION(f.session)->cleanup_reserved_bytes);
            CHECK(f.session->backend->session_controlled_step_state(
                    f.session) ==
                (snapshot_entry?WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE:
                WL_CONTROLLED_STEP_IDLE));
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report));
            same_report(&original_report, &report);
            uint64_t used = 999;
            CHECK(successor &&
                !wl_evaluation_control_test_active_consumed(f.control,
                successor, &used) && !used);
            wl_evaluation_control_attempt_finish_result_t finish =
                wl_evaluation_control_finish_attempt(
                f.control, f.session->operation_admission, successor, 0, 0);
            CHECK(!finish.protocol && finish.report_valid &&
                !finish.report.consumed);
            CHECK(!wl_evaluation_control_reset(f.control));
            if (snapshot_entry) CHECK(!wl_session_snapshot(f.session, snapshot,
                    &f) && f.snapshot_count == 2);
            else CHECK(!complete(f.session));
            contents(&f, 2);
            CHECK(f.events == 2 && !complete(f.session) && f.events == 2);
            destroy(&f);
        }
}
static uint64_t ack_cap, ack_consumed;
static bool ack_injected, ack_refused;
static unsigned ack_successes;
static uint64_t ack_limit(const wl_col_session_t *sess,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t limit) {
    (void)sess;
    if (!ack_injected && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK) {
        ack_injected = true; return ack_cap;
    }
    return limit;
}
static void ack_finished(const wl_col_session_t *sess,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *funding, int protocol) {
    (void)protocol;
    if (action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK)return;
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report));
    if (report.limit == ack_cap) {
        CHECK(!ack_refused && report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
            && report.consumed == ack_consumed && !report.execution_status &&
            !report.cleanup_status
            && !active->events && !sess->cleanup_active
            && sess->cleanup_receipts[0].state ==
            WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED);
        ack_refused = true;
    } else if (!report.result)++ack_successes;
}
static void ack_denials(void) {
    for (unsigned stopped = 0; stopped < 2; ++stopped)for (unsigned cut = 0;
            cut < 2;
            ++cut) {
            fixture_t f = create(); active = &f;
            wl_session_set_delta_cb(f.session, capture, &f);
            CHECK(!complete(f.session)); insert(&f, "12tail");
            insert(&f, " -3 ");
            ack_cap = cut?575:191; ack_consumed = cut?192:0;
            ack_injected = ack_refused = false; ack_successes = 0;
            ended = false; successor = 0; rebind_calls = 0;
            start_successor = true;
            target_phase = 101; target_domain = 0; expect_filter = false;
            accounting_error = false;
            wl_columnar_session_test_framed_grant_limit = ack_limit;
            wl_columnar_session_test_framed_grant_finished = ack_finished;
            wl_columnar_expr_test_checkpoint = stopped?rebind:NULL;
            int rc = complete(f.session);
            wl_columnar_expr_test_checkpoint = NULL;
            wl_columnar_session_test_framed_grant_limit = NULL;
            wl_columnar_session_test_framed_grant_finished = NULL;
            CHECK(ack_injected && ack_refused && ack_successes == 1 &&
                rc == (stopped?EINVAL:0));
            if (stopped) {
                CHECK(ended && !f.events &&
                    !COL_SESSION(f.session)->cleanup_active
                    && !COL_SESSION(f.session)->cleanup_pending &&
                    !COL_SESSION(f.session)->cleanup_reserved_bytes);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report));
                same_report(&original_report, &report); uint64_t used = 999;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    successor, &used) && !used);
                wl_evaluation_control_attempt_finish_result_t finish =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.session->operation_admission, successor, 0, 0);
                CHECK(!finish.protocol && finish.report_valid &&
                    !finish.report.consumed);
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !complete(f.session));
            }
            contents(&f, 2);
            CHECK(f.events == 2 && !complete(f.session) && f.events == 2);
            destroy(&f);
        }
}
static uint64_t acked_cap, acked_consumed;
static bool acked_injected, acked_refused;
static unsigned acked_releases;
static uint64_t acked_limit(const wl_col_session_t *sess,
    wl_columnar_session_metadata_purpose_t purpose, uint64_t limit) {
    (void)sess;
    if (ended && !acked_injected &&
        purpose == WL_COLUMNAR_SESSION_METADATA_UNARY_CLEANUP) {
        acked_injected = true; return acked_cap;
    }
    return limit;
}
static void acked_source_finished(const wl_col_session_t *sess,
    wl_columnar_session_metadata_purpose_t purpose,
    wl_columnar_session_metadata_event_t event, int protocol,
    wl_evaluation_control_report_t report) {
    (void)sess; (void)event; (void)protocol;
    if (purpose != WL_COLUMNAR_SESSION_METADATA_UNARY_CLEANUP ||
        report.limit != acked_cap) return;
    CHECK(!acked_refused && report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
        && report.consumed == acked_consumed && !report.execution_status &&
        !report.cleanup_status);
    acked_refused = true;
}
static void acked_grant_finished(const wl_col_session_t *sess,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *funding, int protocol) {
    if (action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK &&
        !protocol) ++acked_releases;
    boundary_finished(sess, action, funding, protocol);
}
static void acked_source_denials(void) {
    const uint64_t caps[] = {411, 603, 795, 987, 1115};
    const uint64_t consumed[] = {220, 412, 604, 796, 988};
    for (unsigned cut = 0; cut < 5; ++cut) {
        fixture_t f = create(); active = &f;
        wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session)); insert(&f, "12tail"); insert(&f, " -3 ");
        boundary_cut = 5; ended = false; successor = 0; acked_releases = 0;
        acked_cap = caps[cut]; acked_consumed = consumed[cut];
        acked_injected = acked_refused = false;
        wl_columnar_session_test_framed_grant_finished = acked_grant_finished;
        wl_columnar_session_test_metadata_limit = acked_limit;
        wl_columnar_session_test_metadata_finished = acked_source_finished;
        CHECK(complete(f.session) == EINVAL);
        wl_columnar_session_test_framed_grant_finished = NULL;
        wl_columnar_session_test_metadata_limit = NULL;
        wl_columnar_session_test_metadata_finished = NULL;
        CHECK(ended && acked_injected && acked_refused && acked_releases == 1 &&
            !f.events
            && !COL_SESSION(f.session)->cleanup_active &&
            !COL_SESSION(f.session)->cleanup_pending
            && !COL_SESSION(f.session)->cleanup_reserved_bytes);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report));
        same_report(&original_report, &report); uint64_t used = 999;
        CHECK(!wl_evaluation_control_test_active_consumed(f.control, successor,
            &used) && !used);
        wl_evaluation_control_attempt_finish_result_t finish =
            wl_evaluation_control_finish_attempt(
            f.control, f.session->operation_admission, successor, 0, 0);
        CHECK(!finish.protocol && finish.report_valid &&
            !finish.report.consumed);
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session));
        contents(&f, 2);
        CHECK(f.events == 2 && !complete(f.session) && f.events == 2);
        destroy(&f);
    }
}
/* Component finance is genuinely attached/begun at its final lexical address.
 * It grants no native permission: the receipt is produced and retired by the
 * real frame API and native384 is paid by a separate actual control. Bank-issued
 * public finance is independently observed by unary_source_tails below. */
static void gateway_source_probe(wl_col_session_t *sess,
    const wl_plan_relation_t *plan,
    const wl_columnar_eval_cleanup_receipt_key_t *key,
    uint64_t limit, wl_columnar_eval_serial_framed_host_action_t action,
    unsigned mode) {
    wl_columnar_eval_cleanup_receipt_t before, after;
    CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(sess, key, &before));
    uint64_t original_id = wl_evaluation_control_active_attempt_id(
        sess->base.evaluation_control), original_before = 0, original_after = 0;
    CHECK(original_id && !wl_evaluation_control_test_active_consumed(
            sess->base.evaluation_control, original_id, &original_before));
    int owner;
    wl_evaluation_control_t *native = NULL;
    CHECK(!wl_evaluation_control_create(mode == 7 ? 383 : 384, &native)
        && !wl_evaluation_control_attach(native, &owner)
        && !wl_evaluation_control_begin(native, &owner));
    uint64_t native_id = wl_evaluation_control_active_attempt_id(native);
    wl_session_source_finance_t source = {0};
    source.limit = limit;
    CHECK(!wl_evaluation_control_create(limit, &source.control)
        && !wl_evaluation_control_attach(source.control, &source)
        && !wl_evaluation_control_begin(source.control, &source));
    source.attached = source.active = true;
    source.id = wl_evaluation_control_active_attempt_id(source.control);
    uint64_t first = source.id;
    wl_columnar_eval_serial_framed_host_reply_t reply = {0};
    wl_columnar_eval_serial_framed_host_request_t request = {
        .action = action, .operation = sess->base.evaluation_control,
        .operation_id = key->operation_attempt_id,
        .operation_identity = key->operation_id,
        .funding = native, .funding_id = native_id,
        .receipt_key = *key, .reply = &reply
    };
    if (mode == 1) ++request.receipt_key.receipt_id;
    if (mode == 2) request.funding = NULL;
    if (mode == 3) ++request.funding_id;
    if (mode == 4) CHECK(!wl_evaluation_control_finish(native, &owner, 0, 0));
    if (mode == 5) ++request.receipt_key.plan_epoch;
    if (mode == 6) ++request.receipt_key.session_generation;
    int expected = limit < 256 ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : mode == 7 ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : mode == 1 || mode == 5 || mode == 6 ? ENOENT : mode ? EINVAL : 0;
    int rc = wl_columnar_eval_stack_cleanup_receipt_host_source(plan, sess,
            false, &request, &source);
    CHECK(rc == expected && !source.active && !source.id);
    wl_evaluation_control_report_t financial;
    CHECK(!wl_evaluation_control_report(source.control, &financial)
        && financial.attempt_id == first + 1
        && financial.result ==
        (limit < 256 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0)
        && financial.consumed == (limit == 223 ? 0 : limit == 255 ? 224 : 256)
        && !financial.execution_status && !financial.cleanup_status);
    CHECK(!wl_session_source_finance_dispose(&source));
    uint64_t native_used = 0;
    if (wl_evaluation_control_active_attempt_id(native)) {
        CHECK(!wl_evaluation_control_test_active_consumed(native, native_id,
            &native_used));
        CHECK(wl_evaluation_control_finish(native, &owner, 0, 0)
            == (mode == 7 &&
            limit == 256 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
    }
    CHECK(native_used == (!mode && limit == 256 ? 384 : 0));
    CHECK(!wl_evaluation_control_detach(native, &owner));
    wl_evaluation_control_release(native);
    if (!mode && limit == 256 &&
        action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK)
        CHECK(wl_columnar_eval_stack_cleanup_receipt_read(sess, key,
            &after) == ENOENT);
    else CHECK(!wl_columnar_eval_stack_cleanup_receipt_read(sess, key, &after)
            && !memcmp(&before, &after, sizeof(before)));
    CHECK(!wl_evaluation_control_test_active_consumed(
            sess->base.evaluation_control,
            original_id, &original_after) && original_after == original_before);
    if (!expected) CHECK(reply.execution_result == before.result
            && reply.committed == before.committed
            && reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL);
}
static void gateway_sources(void) {
    const uint64_t limits[] = {223, 255, 256};
    const wl_columnar_eval_serial_framed_host_action_t actions[] = {
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY,
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK
    };
    for (unsigned a = 0; a < 4; ++a)
        for (unsigned mode = 0; mode < 8; ++mode)
            for (unsigned l = 0; l < 3; ++l) {
                fixture_t f = create();
                wl_col_session_t *sess = COL_SESSION(f.session);
                wl_evaluation_control_attempt_begin_result_t op =
                    wl_evaluation_control_begin_attempt(f.control,
                        f.session->operation_admission);
                CHECK(!op.protocol && op.attempt_id);
                wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
                wl_columnar_eval_cleanup_receipt_key_t key;
                CHECK(
                    !
                    wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled
                    (
                        sess, &frame, f.control, op.attempt_id,
                        (uint64_t)(uintptr_t)&f, &key));
                CHECK(!wl_columnar_eval_stack_cleanup_receipt_prepare(frame,
                    &key, 37, true));
                wl_columnar_eval_cleanup_outcome_t outcome;
                CHECK(!wl_columnar_eval_stack_cleanup_finish_receipted(&frame,
                    &key, &outcome)
                    && !frame &&
                    outcome == WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_FREED);
                gateway_source_probe(sess, &f.plan->strata[0].relations[0],
                    &key,
                    limits[l], actions[a], mode);
                if (!(!mode && limits[l] == 256
                    && actions[a] ==
                    WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK))
                    CHECK(!wl_columnar_eval_stack_cleanup_receipt_ack(sess,
                        &key));
                wl_evaluation_control_attempt_finish_result_t finish =
                    wl_evaluation_control_finish_attempt(f.control,
                        f.session->operation_admission, op.attempt_id, 0, 0);
                CHECK(!finish.protocol && finish.report_valid &&
                    !finish.report.result);
                destroy(&f);
            }
}
static bool indexed_gateway_probed;

extern void (*wl_columnar_session_test_source_tail)(const wl_col_session_t *,
    const wl_session_source_finance_t *);
static unsigned unary_tail_reads;
static bool end_in_source_tail;
static void observe_unary_source_tail(const wl_col_session_t *sess,
    const wl_session_source_finance_t *source) {
    CHECK(source->active && source->control != sess->base.evaluation_control
        && source->id >= 4 && (source->charged == 72 || source->charged == 96));
    uint64_t consumed = UINT64_MAX;
    CHECK(!wl_evaluation_control_test_active_consumed(source->control,
        source->id, &consumed) && consumed == source->charged);
    wl_evaluation_control_report_t prior;
    CHECK(!wl_evaluation_control_report(source->control, &prior)
        && prior.attempt_id + 1 == source->id && !prior.result
        && !prior.execution_status && !prior.cleanup_status
        && (prior.consumed == 128 || prior.consumed == 237
        || prior.consumed == 363 || prior.consumed == 407));
    if (source->charged != 96) return;
    ++unary_tail_reads;
    if (!indexed_gateway_probed && !end_in_source_tail &&
        !sess->cleanup_active) {
        uint64_t id = wl_evaluation_control_active_attempt_id(active->control);
        uint32_t slot = owned_receipt(sess, id);
        if (slot < 32 &&
            sess->cleanup_receipts[slot].state ==
            WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED) {
            indexed_gateway_probed = true;
            wl_columnar_eval_cleanup_receipt_key_t key =
                sess->cleanup_receipts[slot].key;
            const uint64_t limits[] = {223, 255, 256};
            const unsigned modes[] = {0, 1, 2, 5, 6, 7};
            for (unsigned l = 0; l < 3; ++l)
                for (unsigned m = 0; m < 6; ++m)
                    gateway_source_probe((wl_col_session_t *)sess,
                        &active->plan->strata[0].relations[0], &key, limits[l],
                        WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS, modes[m]);
        }
    }
    if (!end_in_source_tail || ended) return;
    uint64_t id = wl_evaluation_control_active_attempt_id(active->control);
    CHECK(id);
    wl_evaluation_control_attempt_finish_result_t finish =
        wl_evaluation_control_finish_attempt(active->control,
            active->session->operation_admission, id, 0, 0);
    CHECK(!finish.protocol && finish.report_valid && !finish.report.result);
    original_report = finish.report; ended = true;
    CHECK(!wl_evaluation_control_reset(active->control));
    wl_evaluation_control_attempt_begin_result_t next =
        wl_evaluation_control_begin_attempt(active->control,
            active->session->operation_admission);
    CHECK(!next.protocol && next.attempt_id != id);
    successor = next.attempt_id;
}
static void unary_source_tails(void) {
    for (unsigned stop = 0; stop < 2; ++stop) {
        fixture_t f = create(); active = &f;
        wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session)); insert(&f, "12tail"); insert(&f, " -3 ");
        unary_tail_reads = 0; end_in_source_tail = stop != 0;
        indexed_gateway_probed = false;
        ended = false; successor = 0;
        wl_columnar_session_test_source_tail = observe_unary_source_tail;
        int rc = complete(f.session);
        wl_columnar_session_test_source_tail = NULL;
        CHECK(unary_tail_reads && rc == (stop ? EINVAL : 0));
        if (!stop) CHECK(indexed_gateway_probed);
        CHECK(!COL_SESSION(f.session)->cleanup_active
            && !COL_SESSION(f.session)->cleanup_pending
            && !COL_SESSION(f.session)->cleanup_reserved_bytes);
        if (stop) {
            CHECK(ended && !f.events && successor);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report));
            same_report(&original_report, &report);
            uint64_t used = UINT64_MAX;
            CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                successor, &used) && !used);
            wl_evaluation_control_attempt_finish_result_t finish =
                wl_evaluation_control_finish_attempt(f.control,
                    f.session->operation_admission, successor, 0, 0);
            CHECK(!finish.protocol && finish.report_valid &&
                !finish.report.consumed);
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !complete(f.session));
        }
        contents(&f, 2);
        CHECK(f.events == 2 && !complete(f.session) && f.events == 2);
        destroy(&f);
    }
}

extern unsigned wl_columnar_session_test_cleanup_owners(
    const wl_col_session_t *);
extern int wl_columnar_session_test_unary_birth_match(wl_col_session_t *,
    wl_session_source_finance_t *);
static unsigned mixed_mode, mixed_reads, mixed_probes, mixed_events;
static bool mixed_string, mixed_lftj;
static unsigned mixed_join_reads;
static void mixed_capture(const char *name, const int64_t *row,
    uint32_t columns, int32_t diff, void *arg) {
    fixture_t *f = arg;
    if (!strcmp(name, "side")) {
        CHECK(columns == 1 && (row[0] == 12 || row[0] == 13) && diff == 1);
        return;
    }
    CHECK(!strcmp(name, "out") && columns == 2 && diff == 1);
    int64_t x = row[0];
    if (mixed_string) {
        const char *text = wl_intern_reverse(f->plan->intern, row[0]);
        CHECK(text && (!strcmp(text, "12") || !strcmp(text, "13")));
        x = !strcmp(text, "12") ? 12 : 13;
    }
    CHECK((x == 12 || x == 13) && row[1] == x + (mixed_string ? 0 : 1));
    ++mixed_events;
}
static void mixed_compare_probe(wl_col_session_t *sess) {
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    if (!sess->cleanup_active || wl_columnar_eval_stack_framed_context(
            sess->cleanup_active, sess, &ctx)) return;
    /* Real native owner; this independent component finance is no permit. */
    for (unsigned mode = 0; mode < 5; ++mode) {
        wl_session_source_finance_t source = {.limit = mode ? 160 : 159};
        CHECK(!wl_evaluation_control_create(source.limit, &source.control)
            && !wl_evaluation_control_attach(source.control, &source)
            && !wl_evaluation_control_begin(source.control, &source));
        source.active = source.attached = true;
        source.id = wl_evaluation_control_active_attempt_id(source.control);
        bool marker = ctx->unary_native_cleanup;
        col_rel_t **capture_out = ctx->capture_out;
        wl_columnar_eval_cleanup_receipt_key_t key = ctx->cleanup_receipt_key;
        if (mode == 2) ctx->unary_native_cleanup = false;
        if (mode == 3) ++ctx->cleanup_receipt_key.receipt_id;
        if (mode == 4) ctx->capture_out = NULL;
        int rc = wl_columnar_session_test_unary_birth_match(sess, &source);
        ctx->unary_native_cleanup = marker; ctx->cleanup_receipt_key = key;
        ctx->capture_out = capture_out;
        CHECK(rc == (!mode ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            mode == 1 ? 0 : EINVAL));
        uint64_t used = UINT64_MAX;
        CHECK(!wl_evaluation_control_test_active_consumed(source.control,
            source.id, &used) && used == (mode ? 160 : 0));
        CHECK(wl_columnar_session_test_cleanup_owners(sess) == 2);
        CHECK(wl_session_source_finance_finish(&source) ==
            (!mode ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(!wl_session_source_finance_dispose(&source));
    }
    ++mixed_probes;
}
static void mixed_tail(const wl_col_session_t *sess,
    const wl_session_source_finance_t *source) {
    unsigned owners = wl_columnar_session_test_cleanup_owners(sess);
    CHECK(owners != 3);
    if (mixed_lftj) {
        if (source->charged != 72 || owners != 1) return;
        ++mixed_join_reads;
    } else {
        if (source->charged != 96 || owners != 2) return;
        ++mixed_reads;
        wl_evaluation_control_report_t prior;
        CHECK(!wl_evaluation_control_report(source->control, &prior));
        CHECK(prior.attempt_id + 1 == source->id);
        if (prior.consumed == 240 && !mixed_probes)
            mixed_compare_probe((wl_col_session_t *)sess);
    }
    if (!mixed_mode || ended) return;
    uint64_t id = wl_evaluation_control_active_attempt_id(active->control);
    CHECK(id);
    if (mixed_mode == 3) {
        wl_evaluation_control_request_cancel(active->control);
        ended = true; return;
    }
    wl_evaluation_control_attempt_finish_result_t r =
        wl_evaluation_control_finish_attempt(active->control,
            active->session->operation_admission, id, 0, 0);
    CHECK(!r.protocol && r.report_valid); original_report = r.report;
    ended = true;
    if (mixed_mode == 2) {
        CHECK(!wl_evaluation_control_reset(active->control));
        wl_evaluation_control_attempt_begin_result_t b =
            wl_evaluation_control_begin_attempt(active->control,
                active->session->operation_admission);
        CHECK(!b.protocol && b.attempt_id != id); successor = b.attempt_id;
    }
}
static void mixed_owners(void) {
    for (unsigned text = 0; text < 2; ++text)
        for (mixed_mode = 0; mixed_mode < 4; ++mixed_mode) {
            mixed_string = text != 0; mixed_lftj = false;
            fixture_t f = {0}; wirelog_error_t error;
            f.program = wirelog_parse_string(mixed_string ?
                    ".decl input(x:string)\n.decl out(x:string,n:int64)\n"
                    ".decl a(x:int64)\n.decl b(x:int64)\n.decl c(x:int64)\n"
                    ".decl side(x:int64)\nout(x,to_number(x)) :- input(x).\n"
                    "side(x) :- a(x),b(x),c(x).\n" :
                    ".decl input(x:int64)\n.decl out(x:int64,n:int64)\n"
                    ".decl a(x:int64)\n.decl b(x:int64)\n.decl c(x:int64)\n"
                    ".decl side(x:int64)\nout(x,x+1) :- input(x).\n"
                    "side(x) :- a(x),b(x),c(x).\n", &error);
            CHECK(f.program && !wl_plan_from_program(f.program, &f.plan)
                && !wl_evaluation_control_create(0, &f.control));
            wl_session_options_t options; wl_session_options_init(&options);
            options.evaluation_control = f.control;
            CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan,
                1, &options, &f.session));
            wl_session_set_delta_cb(f.session, mixed_capture, &f);
            const char *names[] = {"input", "a", "b", "c"}; int64_t value = 12;
            for (unsigned i = 0; i < 4; ++i) {
                value = !i && mixed_string ? wl_intern_put(f.plan->intern,
                        "12") : 12;
                CHECK(!wl_session_insert(f.session, names[i], &value, 1, 1));
            }
            CHECK(!complete(f.session) && f.session->source_c1_admission);
            value = mixed_string ? wl_intern_put(f.plan->intern, "13") : 13;
            CHECK(!wl_session_insert(f.session, "input", &value, 1, 1));
            mixed_events = 0;
            active = &f; ended = false; successor = 0;
            mixed_reads = mixed_probes = 0;
            wl_columnar_session_test_source_tail = mixed_tail;
            int rc = complete(f.session);
            wl_columnar_session_test_source_tail = NULL;
            CHECK(mixed_reads && mixed_probes && rc == (!mixed_mode ? 0 :
                mixed_mode == 3 ? WL_EVALUATION_CONTROL_CANCELLED : EINVAL));
            CHECK(!COL_SESSION(f.session)->cleanup_active &&
                !COL_SESSION(f.session)->cleanup_pending &&
                !COL_SESSION(f.session)->cleanup_reserved_bytes);
            if (mixed_mode) {
                CHECK(!mixed_events);
                if (mixed_mode != 3) {
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report));
                    same_report(&original_report, &report);
                }
                if (successor) {
                    uint64_t used = UINT64_MAX;
                    CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                        successor, &used) && !used);
                    wl_evaluation_control_attempt_finish_result_t r =
                        wl_evaluation_control_finish_attempt(f.control,
                            f.session->operation_admission, successor, 0, 0);
                    CHECK(!r.protocol && r.report_valid && !r.report.consumed);
                }
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !complete(f.session));
            }
            col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
            col_rel_t *side = session_find_rel(COL_SESSION(f.session), "side");
            CHECK(out && out->nrows == 2 && out->ncols == 2 &&
                out->column_types[0] ==
                (mixed_string ? WIRELOG_TYPE_STRING : WIRELOG_TYPE_INT64) &&
                out->column_types[1] == WIRELOG_TYPE_INT64 && side &&
                side->nrows == 1 &&
                side->column_types[0] == WIRELOG_TYPE_INT64 &&
                side->columns[0][0] == 12);
            bool seen[2] = {false, false};
            for (uint32_t r = 0; r < 2; ++r) {
                int64_t x = out->columns[0][r];
                if (mixed_string) {
                    const char *v = wl_intern_reverse(f.plan->intern, x);
                    CHECK(v && (!strcmp(v, "12") || !strcmp(v, "13")));
                    x = !strcmp(v, "12") ? 12 : 13;
                }
                CHECK(x == 12 || x == 13);
                CHECK(!seen[x-12] &&
                    out->columns[1][r] == x + (mixed_string ? 0 : 1));
                seen[x-12] = true;
            }
            CHECK(mixed_events == 1 && !complete(f.session) &&
                mixed_events == 1);
            /* The other head must retain real JOIN/LFTJ ownership, including
            * an independently ended hot operation after unary retirement. */
            unsigned saved_mode = mixed_mode;
            mixed_mode = saved_mode ? 0 : 1;
            mixed_lftj = true; mixed_join_reads = 0; ended = false;
            value = 13;
            for (unsigned i = 1; i < 4; ++i)
                CHECK(!wl_session_insert(f.session, names[i], &value, 1, 1));
            wl_columnar_session_test_source_tail = mixed_tail;
            rc = complete(f.session);
            wl_columnar_session_test_source_tail = NULL;
            CHECK(mixed_join_reads && rc == (mixed_mode ? EINVAL : 0));
            CHECK(!COL_SESSION(f.session)->cleanup_active &&
                !COL_SESSION(f.session)->cleanup_pending &&
                !COL_SESSION(f.session)->cleanup_reserved_bytes);
            if (mixed_mode) {
                CHECK(ended && !wl_evaluation_control_reset(f.control));
                CHECK(!complete(f.session));
            }
            side = session_find_rel(COL_SESSION(f.session), "side");
            CHECK(side && side->nrows == 2 && mixed_events == 1);
            CHECK((side->columns[0][0] == 12 && side->columns[0][1] == 13) ||
                (side->columns[0][0] == 13 && side->columns[0][1] == 12));
            mixed_mode = saved_mode; destroy(&f);
        }
}

int main(void) {
    terminal(); native_rebind(); registration_denials(); indexed_history();
    indexed_lookup(); grant_boundaries(); ack_denials(); acked_source_denials();
    gateway_sources(); unary_source_tails(); mixed_owners();
    puts("hot scalar MAP recovery: all checks passed"); return 0;
}
