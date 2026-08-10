/*
 * test_mixed_insert_remove_mask.c - Affected-strata mask for a step that
 * both inserts and removes (Issue #1031)
 *
 * col_session_step() derives one affected-strata bitmask per step and skips
 * every stratum whose bit is clear.  The mask has to cover the union of the
 * strata reachable from the inserted relation and those reachable from the
 * removed relation; a step that touches two relations in disjoint strata
 * otherwise evaluates neither.  The same mask must stay UINT64_MAX whenever
 * pending_full_input_eval is set, because that flag means "this step cannot
 * be incremental".
 *
 * Every case asserts its own premise -- that the seed step really derived
 * the baseline tuples -- before asserting the step under test.  Without that
 * the interesting assertions would hold vacuously on a session that derives
 * nothing at all, which is exactly the shape of the bug: the broken step
 * emits no deltas, so any assertion phrased as "no wrong tuple appeared"
 * passes on the broken build.  T2 is the one case phrased that way on
 * purpose, and it does pass on the broken build; it is a regression guard on
 * the step's cleanup, not a detector for this bug.  See its own comment.
 *
 * Assertions are exact multiset equality over (relation, row, diff).  A
 * count-only check would pass under a mask that ran the wrong strata but the
 * right number of them.  No mask literals appear here, because which bit a
 * stratum gets depends on how the program stratifies, and these cases should
 * survive any restratification of SRC_MIXED.  The bit layout that
 * col_compute_affected_strata() produces is covered separately by
 * tests/test_affected_strata.c, which hand-builds wl_plan_t values on the
 * stack and never parses a program.
 *
 * Deltas alone cannot observe the mask -- see T6, which does.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog-easy.h"
#include "../wirelog/wirelog.h"

/* ======================================================================== */
/* TEST HARNESS MACROS                                                       */
/* ======================================================================== */

#define TEST(name)                       \
        do {                                 \
            printf("  [TEST] %-62s ", name); \
            fflush(stdout);                  \
        } while (0)

#define PASS              \
        do {                  \
            printf("PASS\n"); \
            tests_passed++;   \
        } while (0)

#define FAIL(msg)                  \
        do {                           \
            printf("FAIL: %s\n", msg); \
            tests_failed++;            \
        } while (0)

static int tests_passed = 0;
static int tests_failed = 0;

/* ======================================================================== */
/* DATALOG PROGRAM                                                           */
/* ======================================================================== */

/*
 * Three independent EDB relations feeding three independent derivation
 * chains.  A reaches two strata (P then P2), B and C one each, and no
 * stratum is reachable from more than one EDB, so an intersected mask is
 * empty and a union mask is not.
 */
static const char *const SRC_MIXED =
    ".decl A(x: int64, y: int64)\n"
    ".decl B(x: int64, y: int64)\n"
    ".decl C(x: int64, y: int64)\n"
    ".decl P(x: int64, y: int64)\n"
    ".decl P2(x: int64, y: int64)\n"
    ".decl Q(x: int64, y: int64)\n"
    ".decl R(x: int64, y: int64)\n"
    ".output P\n"
    ".output P2\n"
    ".output Q\n"
    ".output R\n"
    "P(x,y) :- A(x,y).\n"
    "P2(x,y) :- P(x,y).\n"
    "Q(x,y) :- B(x,y).\n"
    "R(x,y) :- C(x,y).\n";

/* ======================================================================== */
/* DELTA COLLECTION                                                          */
/* ======================================================================== */

#define MAX_DELTAS 64
#define MAX_RELNAME 16

typedef struct {
    char rel[MAX_RELNAME];
    int64_t x;
    int64_t y;
    int32_t diff;
} delta_ent_t;

typedef struct {
    delta_ent_t ent[MAX_DELTAS];
    uint32_t count;
    uint32_t overflow;  /* deltas dropped for want of room                 */
    uint32_t malformed; /* deltas with an unexpected arity or relation name */
} delta_log_t;

static void
mixed_delta_cb(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    delta_log_t *log = (delta_log_t *)user_data;

    if (!log)
        return;
    if (!relation || !row || ncols != 2
        || strlen(relation) >= MAX_RELNAME) {
        log->malformed++;
        return;
    }
    if (log->count == MAX_DELTAS) {
        log->overflow++;
        return;
    }
    snprintf(log->ent[log->count].rel, MAX_RELNAME, "%s", relation);
    log->ent[log->count].x = row[0];
    log->ent[log->count].y = row[1];
    log->ent[log->count].diff = diff;
    log->count++;
}

/* Exact multiset equality between the collected deltas and @want. */
static bool
delta_log_equals(const delta_log_t *log, const delta_ent_t *want,
    uint32_t nwant)
{
    bool used[MAX_DELTAS];

    if (log->overflow != 0 || log->malformed != 0)
        return false;
    if (log->count != nwant)
        return false;
    memset(used, 0, sizeof(used));
    for (uint32_t i = 0; i < nwant; i++) {
        bool found = false;
        for (uint32_t j = 0; j < log->count; j++) {
            if (used[j])
                continue;
            if (strcmp(log->ent[j].rel, want[i].rel) == 0
                && log->ent[j].x == want[i].x && log->ent[j].y == want[i].y
                && log->ent[j].diff == want[i].diff) {
                used[j] = true;
                found = true;
                break;
            }
        }
        if (!found)
            return false;
    }
    return true;
}

/* Render the collected deltas into @buf for a FAIL message. */
static void
delta_log_describe(const delta_log_t *log, char *buf, size_t buflen)
{
    size_t off = 0;
    int n = snprintf(buf, buflen, "got {");

    if (n < 0)
        return;
    off = (size_t)n;
    for (uint32_t i = 0; i < log->count && off < buflen; i++) {
        n = snprintf(buf + off, buflen - off, "%s%c%s(%lld,%lld)",
                i == 0 ? "" : " ", log->ent[i].diff < 0 ? '-' : '+',
                log->ent[i].rel, (long long)log->ent[i].x,
                (long long)log->ent[i].y);
        if (n < 0)
            return;
        off += (size_t)n;
    }
    if (off < buflen)
        snprintf(buf + off, buflen - off, "}");
}

/* ======================================================================== */
/* FIXTURE                                                                   */
/* ======================================================================== */

static const int64_t ROW_A12[2] = { 1, 2 };
static const int64_t ROW_A34[2] = { 3, 4 };
static const int64_t ROW_B78[2] = { 7, 8 };
static const int64_t ROW_C99[2] = { 9, 9 };

/* What the seed step below must emit; every case checks this first. */
static const delta_ent_t SEED_EXPECT[] = {
    { "P", 1, 2, +1 },
    { "P2", 1, 2, +1 },
    { "Q", 7, 8, +1 },
};
#define SEED_EXPECT_N \
        ((uint32_t)(sizeof(SEED_EXPECT) / sizeof(SEED_EXPECT[0])))

/*
 * open_seeded - Open a delta-callback session holding A(1,2) and B(7,8) and
 * run one step, leaving @log holding exactly that step's deltas.
 *
 * Returns the session, or NULL if any wirelog call failed.
 */
static wirelog_easy_session_t *
open_seeded(delta_log_t *log)
{
    wirelog_easy_session_t *s = NULL;

    memset(log, 0, sizeof(*log));
    if (wirelog_easy_open(SRC_MIXED, &s) != WIRELOG_OK || !s)
        return NULL;
    if (wirelog_easy_set_delta_cb(s, mixed_delta_cb, log) != WIRELOG_OK
        || wirelog_easy_insert(s, "A", ROW_A12, 2) != WIRELOG_OK
        || wirelog_easy_insert(s, "B", ROW_B78, 2) != WIRELOG_OK
        || wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        return NULL;
    }
    return s;
}

/* ======================================================================== */
/* T1: mixed insert + remove in one step                                     */
/* ======================================================================== */

static void
test_mixed_insert_and_remove(void)
{
    TEST("mixed: insert A + remove B in one step emits both sides");

    delta_log_t log;
    char buf[256];
    wirelog_easy_session_t *s = open_seeded(&log);

    if (!s) {
        FAIL("session setup failed");
        return;
    }
    if (!delta_log_equals(&log, SEED_EXPECT, SEED_EXPECT_N)) {
        wirelog_easy_close(s);
        FAIL("premise: seed step did not derive the baseline tuples");
        return;
    }

    memset(&log, 0, sizeof(log));
    if (wirelog_easy_insert(s, "A", ROW_A34, 2) != WIRELOG_OK
        || wirelog_easy_remove(s, "B", ROW_B78, 2) != WIRELOG_OK
        || wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("mixed insert/remove step failed");
        return;
    }

    static const delta_ent_t want[] = {
        { "P", 3, 4, +1 },
        { "P2", 3, 4, +1 },
        { "Q", 7, 8, -1 },
    };
    bool ok = delta_log_equals(&log, want,
            (uint32_t)(sizeof(want) / sizeof(want[0])));
    if (!ok)
        delta_log_describe(&log, buf, sizeof(buf));
    wirelog_easy_close(s);
    if (!ok) {
        FAIL(buf);
        return;
    }
    PASS;
}

/* ======================================================================== */
/* T2: the step after the mixed step is silent                               */
/* ======================================================================== */

/*
 * This passes on the pre-#1031 build too, and is not a detector for #1031.
 * col_session_step() clears last_inserted_relation, last_removed_relation and
 * pending_input_change on its way out, so in delta-callback mode the next
 * step with no input change hits the early return and evaluates nothing at
 * all: with or without the fix, a delta arriving one step late is
 * structurally impossible rather than merely unobserved.  What this case pins
 * is that cleanup/early-return pair, so that a future change leaving either
 * pointer set -- and therefore replaying a step's deltas -- is caught.
 */
static void
test_mixed_step_has_no_late_arrival(void)
{
    TEST("mixed: the following idle step emits nothing");

    delta_log_t log;
    char buf[256];
    wirelog_easy_session_t *s = open_seeded(&log);

    if (!s) {
        FAIL("session setup failed");
        return;
    }
    if (!delta_log_equals(&log, SEED_EXPECT, SEED_EXPECT_N)) {
        wirelog_easy_close(s);
        FAIL("premise: seed step did not derive the baseline tuples");
        return;
    }

    if (wirelog_easy_insert(s, "A", ROW_A34, 2) != WIRELOG_OK
        || wirelog_easy_remove(s, "B", ROW_B78, 2) != WIRELOG_OK
        || wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("mixed insert/remove step failed");
        return;
    }

    /* No input change: a correct engine has nothing left to say. */
    memset(&log, 0, sizeof(log));
    if (wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("idle step failed");
        return;
    }

    bool ok = delta_log_equals(&log, NULL, 0);
    if (!ok)
        delta_log_describe(&log, buf, sizeof(buf));
    wirelog_easy_close(s);
    if (!ok) {
        FAIL(buf);
        return;
    }
    PASS;
}

/* ======================================================================== */
/* T3: control -- insert only                                                */
/* ======================================================================== */

static void
test_insert_only_control(void)
{
    TEST("control: insert A alone emits exactly A's deltas");

    delta_log_t log;
    char buf[256];
    wirelog_easy_session_t *s = open_seeded(&log);

    if (!s) {
        FAIL("session setup failed");
        return;
    }
    if (!delta_log_equals(&log, SEED_EXPECT, SEED_EXPECT_N)) {
        wirelog_easy_close(s);
        FAIL("premise: seed step did not derive the baseline tuples");
        return;
    }

    memset(&log, 0, sizeof(log));
    if (wirelog_easy_insert(s, "A", ROW_A34, 2) != WIRELOG_OK
        || wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("insert step failed");
        return;
    }

    static const delta_ent_t want[] = {
        { "P", 3, 4, +1 },
        { "P2", 3, 4, +1 },
    };
    bool ok = delta_log_equals(&log, want,
            (uint32_t)(sizeof(want) / sizeof(want[0])));
    if (!ok)
        delta_log_describe(&log, buf, sizeof(buf));
    wirelog_easy_close(s);
    if (!ok) {
        FAIL(buf);
        return;
    }
    PASS;
}

/* ======================================================================== */
/* T4: control -- remove only                                                */
/* ======================================================================== */

static void
test_remove_only_control(void)
{
    TEST("control: remove B alone emits exactly B's deltas");

    delta_log_t log;
    char buf[256];
    wirelog_easy_session_t *s = open_seeded(&log);

    if (!s) {
        FAIL("session setup failed");
        return;
    }
    if (!delta_log_equals(&log, SEED_EXPECT, SEED_EXPECT_N)) {
        wirelog_easy_close(s);
        FAIL("premise: seed step did not derive the baseline tuples");
        return;
    }

    memset(&log, 0, sizeof(log));
    if (wirelog_easy_remove(s, "B", ROW_B78, 2) != WIRELOG_OK
        || wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("remove step failed");
        return;
    }

    static const delta_ent_t want[] = {
        { "Q", 7, 8, -1 },
    };
    bool ok = delta_log_equals(&log, want,
            (uint32_t)(sizeof(want) / sizeof(want[0])));
    if (!ok)
        delta_log_describe(&log, buf, sizeof(buf));
    wirelog_easy_close(s);
    if (!ok) {
        FAIL(buf);
        return;
    }
    PASS;
}

/* ======================================================================== */
/* T5: a removal must not narrow a full-input-eval step                      */
/* ======================================================================== */

static void
test_removal_does_not_narrow_full_eval(void)
{
    TEST("full eval: removal alongside two-relation insert stays complete");

    delta_log_t log;
    char buf[256];
    wirelog_easy_session_t *s = open_seeded(&log);

    if (!s) {
        FAIL("session setup failed");
        return;
    }
    if (!delta_log_equals(&log, SEED_EXPECT, SEED_EXPECT_N)) {
        wirelog_easy_close(s);
        FAIL("premise: seed step did not derive the baseline tuples");
        return;
    }

    /* Inserting into two different relations before the step escalates the
     * session to pending_full_input_eval, which the removal must not narrow.
     */
    memset(&log, 0, sizeof(log));
    if (wirelog_easy_insert(s, "A", ROW_A34, 2) != WIRELOG_OK
        || wirelog_easy_insert(s, "C", ROW_C99, 2) != WIRELOG_OK
        || wirelog_easy_remove(s, "B", ROW_B78, 2) != WIRELOG_OK
        || wirelog_easy_step(s) != WIRELOG_OK) {
        wirelog_easy_close(s);
        FAIL("full-eval step failed");
        return;
    }

    static const delta_ent_t want[] = {
        { "P", 3, 4, +1 },
        { "P2", 3, 4, +1 },
        { "R", 9, 9, +1 },
        { "Q", 7, 8, -1 },
    };
    bool ok = delta_log_equals(&log, want,
            (uint32_t)(sizeof(want) / sizeof(want[0])));
    if (!ok)
        delta_log_describe(&log, buf, sizeof(buf));
    wirelog_easy_close(s);
    if (!ok) {
        FAIL(buf);
        return;
    }
    PASS;
}

/* ======================================================================== */
/* T6: the mask itself, read through the Issue #264 diff-operator guard      */
/* ======================================================================== */

/*
 * T1-T5 assert deltas, and deltas cannot observe the mask.
 * col_stratum_step_with_delta() set-diffs a pre-step snapshot against a
 * re-evaluation, so every mask that covers the affected strata yields the
 * same deltas: forcing affected_mask = UINT64_MAX unconditionally passes all
 * five, and the whole rest of the suite with them.  This case asserts on the
 * mask instead, via the observable the engine already keeps --
 * wl_col_session_t.diff_operators_active, which col_session_step() assigns
 * from col_should_activate_diff() = diff_enabled && mask != UINT64_MAX &&
 * mask != 0 (Issue #264), and which is still readable after the step returns.
 * diff_enabled defaults to true, so the flag reads back as "this step ran on
 * a narrowed, non-empty mask".
 *
 * Expected, and what each expectation rules out:
 *
 *   mixed (insert A + remove B)   true   -- false on the pre-#1031 build,
 *                                           where M_A & M_B == 0
 *   insert only                   true   -- false if the mask were forced to
 *                                           UINT64_MAX
 *   remove only                   true   -- likewise
 *   insert A + insert C + remove B
 *     (pending_full_input_eval)   false  -- true on the pre-#1031 build,
 *                                           which narrowed the full eval to
 *                                           the removal's strata
 *
 * Three limits, all deliberate:
 *   - this couples the case to col_should_activate_diff()'s definition.  A
 *     change to the Issue #264 guard changes what these four booleans mean,
 *     and this case has to be re-derived rather than merely re-baselined.
 *   - a recursive stratum is not read-only here: eval.c overrides
 *     diff_operators_active per sub-pass and restores the saved value on the
 *     way out (Issue #282), so the post-step value survives one only because
 *     that restore is correct.  SRC_MIXED has no recursive rule, so this
 *     case does not lean on it either way.
 *   - the flag cannot tell M_A|M_B from any other non-empty, non-full mask.
 *     Pinning the exact strata is what T1-T5's exact deltas do.
 */

typedef enum {
    SCEN_MIXED,
    SCEN_INSERT_ONLY,
    SCEN_REMOVE_ONLY,
    SCEN_FULL_EVAL
} mask_scenario_t;

/*
 * run_mask_scenario - Build a raw wl_session_t on SRC_MIXED, seed it exactly
 * as open_seeded() does, apply @scen, step, and report the resulting
 * diff_operators_active in @out.
 *
 * The wirelog_easy struct is private, so this cannot reuse open_seeded().
 *
 * Returns false if any wirelog call failed, in which case @out is untouched.
 */
static bool
run_mask_scenario(mask_scenario_t scen, bool *out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(SRC_MIXED, &err);

    if (!prog)
        return false;
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return false;
    }

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return false;
    }

    bool ok = wl_session_load_facts(sess, prog) == 0;

    /* The delta callback must be installed before the first insert: it is
     * what routes wl_session_insert/remove onto the incremental paths that
     * set last_inserted_relation and last_removed_relation. */
    delta_log_t log;
    memset(&log, 0, sizeof(log));
    if (ok)
        wl_session_set_delta_cb(sess, mixed_delta_cb, &log);
    ok = ok && wl_session_insert(sess, "A", ROW_A12, 1, 2) == 0
        && wl_session_insert(sess, "B", ROW_B78, 1, 2) == 0
        && wl_session_step(sess) == 0;
    /* Same premise as T1-T5: the seed step really did derive the baseline. */
    ok = ok && delta_log_equals(&log, SEED_EXPECT, SEED_EXPECT_N);

    if (ok) {
        switch (scen) {
        case SCEN_MIXED:
            ok = wl_session_insert(sess, "A", ROW_A34, 1, 2) == 0
                && wl_session_remove(sess, "B", ROW_B78, 1, 2) == 0;
            break;
        case SCEN_INSERT_ONLY:
            ok = wl_session_insert(sess, "A", ROW_A34, 1, 2) == 0;
            break;
        case SCEN_REMOVE_ONLY:
            ok = wl_session_remove(sess, "B", ROW_B78, 1, 2) == 0;
            break;
        case SCEN_FULL_EVAL:
            /* Two distinct insert targets in one step set
             * pending_full_input_eval. */
            ok = wl_session_insert(sess, "A", ROW_A34, 1, 2) == 0
                && wl_session_insert(sess, "C", ROW_C99, 1, 2) == 0
                && wl_session_remove(sess, "B", ROW_B78, 1, 2) == 0;
            break;
        default:
            ok = false;
            break;
        }
    }

    memset(&log, 0, sizeof(log));
    ok = ok && wl_session_step(sess) == 0;
    if (ok)
        *out = COL_SESSION(sess)->diff_operators_active;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return ok;
}

static void
test_mask_narrowing_observable(void)
{
    TEST("mask: diff guard sees a narrowed mask on every incremental step");

    static const struct {
        mask_scenario_t scen;
        bool want;
        const char *what;
    } cases[] = {
        { SCEN_MIXED, true, "mixed insert+remove" },
        { SCEN_INSERT_ONLY, true, "insert only" },
        { SCEN_REMOVE_ONLY, true, "remove only" },
        { SCEN_FULL_EVAL, false, "pending full input eval" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        bool active = false;
        char buf[128];

        if (!run_mask_scenario(cases[i].scen, &active)) {
            snprintf(buf, sizeof(buf), "%s: session setup or step failed",
                cases[i].what);
            FAIL(buf);
            return;
        }
        if (active != cases[i].want) {
            snprintf(buf, sizeof(buf),
                "%s: diff_operators_active=%d, expected %d",
                cases[i].what, (int)active, (int)cases[i].want);
            FAIL(buf);
            return;
        }
    }
    PASS;
}

/* ======================================================================== */
/* MAIN                                                                      */
/* ======================================================================== */

int
main(void)
{
    printf("=== Mixed insert/remove affected-strata mask (#1031) ===\n");

    test_mixed_insert_and_remove();
    test_mixed_step_has_no_late_arrival();
    test_insert_only_control();
    test_remove_only_control();
    test_removal_does_not_narrow_full_eval();
    test_mask_narrowing_observable();

    printf("\n  passed: %d, failed: %d\n", tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
