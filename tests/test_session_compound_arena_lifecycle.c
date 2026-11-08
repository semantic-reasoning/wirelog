/*
 * test_session_compound_arena_lifecycle.c - Issue #559 lifecycle gateway test
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies that wl_col_session_t owns a heap-allocated wl_compound_arena_t
 * across its lifecycle:
 *   - col_session_create allocates the arena (sess->compound_arena != NULL).
 *   - The arena is usable: a small wl_compound_arena_alloc returns a non-NULL
 *     handle that round-trips through the 20-bit session-seed field.
 *   - col_session_destroy frees the arena (no leaks under ASan).
 *
 * This is the Phase 0 gateway for the side-relation tier; downstream issues
 * replace the placeholder seed and wire the arena into operator paths.
 */

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"
#include "../wirelog/thread.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)

#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
            return;                           \
        } while (0)

#define ASSERT(cond, msg)     \
        do {                      \
            if (!(cond)) {        \
                FAIL(msg);        \
            }                     \
        } while (0)

/* ======================================================================== */
/* Helper: build a minimal columnar plan                                    */
/* ======================================================================== */

static wl_plan_t *
build_plan(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }
    plan_fixture_hold(prog);
    return plan;
}

/* ======================================================================== */
/* Test                                                                     */
/* ======================================================================== */

static void
test_session_compound_arena_lifecycle(void)
{
    TEST("session.compound_arena: create -> alloc -> destroy");

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    ASSERT(plan != NULL, "could not build plan");

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
    }

    wl_col_session_t *cs = COL_SESSION(session);
    ASSERT(cs->compound_arena != NULL,
        "compound_arena not allocated by col_session_create");

    /* Exercise one allocation so we know the arena is wired correctly. */
    uint64_t handle = wl_compound_arena_alloc(cs->compound_arena, 64u);
    if (handle == WL_COMPOUND_HANDLE_NULL) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("wl_compound_arena_alloc returned WL_COMPOUND_HANDLE_NULL");
    }

    /* Session seed propagation: low 20 bits of 0x53455353 ('SESS'). */
    if (wl_compound_handle_session(handle) != (0x53455353u & 0xFFFFFu)) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("handle session seed does not match placeholder constant");
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

typedef struct {
    wl_arena_compound_arena_gc_hold_t hold;
    uint64_t handle;
    int errors;
} detached_reader_t;

static void *
read_detached_arena(void *arg)
{
    detached_reader_t *reader = arg;
    for (unsigned i = 0; i < 10000; i++) {
        wl_compound_arena_borrow_t borrow = { 0 };
        if (!wl_arena_compound_arena_gc_hold_borrow(&reader->hold, &borrow)) {
            reader->errors++;
            break;
        }
        const void *data = wl_compound_arena_lookup_borrowed(&borrow,
                reader->handle, NULL);
        if (!data || memcmp(data, "detached", 8) != 0)
            reader->errors++;
        if (!wl_compound_arena_borrow_release(&borrow))
            reader->errors++;
    }
    if (wl_arena_compound_arena_gc_hold_release(&reader->hold) != 0)
        reader->errors++;
    return NULL;
}

static void
test_detached_session_arena(void)
{
    TEST("detached compound readers survive real session destruction");
    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\nr(x) :- a(x).\n");
    ASSERT(plan != NULL, "plan");
    wl_session_t *session = NULL;
    ASSERT(wl_session_create(wl_backend_columnar(), plan, 1, &session) == 0,
        "session");
    wl_col_session_t *cs = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *owner = cs->memory_governor;
    wl_columnar_memory_governor_ref_retain(owner);
    wl_columnar_memory_governor_t *governor
        = wl_columnar_memory_governor_ref_get(owner);
    uint64_t handle = wl_compound_arena_alloc(cs->compound_arena, 8);
    ASSERT(handle != 0, "handle");
    memcpy((void *)wl_compound_arena_lookup(cs->compound_arena, handle, NULL),
        "detached", 8);
    detached_reader_t readers[2] = { 0 };
    wl_thread_t threads[2];
    bool started[2];
    wl_arena_compound_arena_gc_hold_t last = { 0 };
    ASSERT(wl_arena_compound_arena_gc_hold_acquire(cs->compound_arena,
        &last) == 0, "last hold");
    for (unsigned i = 0; i < 2; i++) {
        readers[i].handle = handle;
        ASSERT(wl_arena_compound_arena_gc_hold_acquire(cs->compound_arena,
            &readers[i].hold) == 0, "reader hold");
        started[i] = wl_thread_create(&threads[i], read_detached_arena,
                &readers[i]) == 0;
    }
    wl_session_destroy(session);
    wl_plan_free(plan);
    plan_fixture_release();
    for (unsigned i = 0; i < 2; i++) {
        if (started[i])
            wl_thread_join(&threads[i]);
        else
            read_detached_arena(&readers[i]);
        ASSERT(readers[i].errors == 0, "concurrent retained reads");
    }
    ASSERT(wl_columnar_memory_reserved(governor) > 0, "arena remains charged");
    wl_compound_arena_borrow_t borrow = { 0 };
    ASSERT(wl_arena_compound_arena_gc_hold_borrow(&last, &borrow),
        "borrow after destruction");
    ASSERT(wl_arena_compound_arena_gc_hold_release(&last) == 0, "last hold");
    const void *data = wl_compound_arena_lookup_borrowed(&borrow, handle, NULL);
    ASSERT(data && memcmp(data, "detached", 8) == 0, "borrow outlives hold");
    ASSERT(wl_compound_arena_borrow_release(&borrow), "final borrow");
    ASSERT(wl_columnar_memory_reserved(governor) == 0, "complete refunds");
    ASSERT(wl_columnar_memory_governor_ref_is_sole(owner), "governor released");
    wl_columnar_memory_governor_ref_release(owner);
    PASS();
}

/* Component oracles use a real owning Session arena. They do not stand in
 * for the selected Root/observer caller and its separate finance journal. */
static void
retirement_require(bool value)
{
    if (!value) abort();
}

static wl_evaluation_control_t *retirement_funding;
static bool retirement_cancel_on_entry;

static int
arena_hold_turn(wl_col_session_t *cs, wl_arena_compound_arena_gc_hold_t *hold,
    wl_arena_compound_arena_hold_release_t *receipt, uint64_t limit,
    bool *done, uint64_t *used)
{
    wl_evaluation_control_t *control = NULL;
    retirement_require(!wl_evaluation_control_create(limit, &control));
    retirement_require(!wl_evaluation_control_attach(control, cs));
    retirement_require(!wl_evaluation_control_begin(control, cs));
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    retirement_funding = control;
    int rc = wl_arena_compound_arena_gc_hold_release_step_owned(hold,
            cs->compound_arena, receipt, cs, control, id, done);
    retirement_funding = NULL;
    int ended = wl_evaluation_control_finish(control, cs, 0, 0);
    wl_evaluation_control_report_t report;
    retirement_require(!wl_evaluation_control_report(control, &report));
    retirement_require(report.result == ended && report.attempt_id == id);
    *used = report.consumed;
    retirement_require(!wl_evaluation_control_detach(control, cs));
    wl_evaluation_control_release(control);
    return rc;
}

static int
arena_gc_turn(wl_col_session_t *cs, wl_arena_compound_arena_gc_step_t *receipt,
    uint64_t limit, bool exhausted, bool *done, uint32_t *result,
    uint64_t *used)
{
    wl_evaluation_control_t *control = NULL;
    retirement_require(!wl_evaluation_control_create(limit, &control));
    retirement_require(!wl_evaluation_control_attach(control, cs));
    retirement_require(!wl_evaluation_control_begin(control, cs));
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    retirement_funding = control;
    if (exhausted) retirement_require(!wl_evaluation_control_charge_attempt(
                control, id, 1));
    if (retirement_cancel_on_entry) {
        retirement_cancel_on_entry = false;
        wl_evaluation_control_request_cancel(control);
    }
    int rc = wl_arena_compound_arena_gc_epoch_step_owned(cs->compound_arena,
            receipt, cs, control, id, done, result);
    retirement_funding = NULL;
    int ended = wl_evaluation_control_finish(control, cs, 0, 0);
    wl_evaluation_control_report_t report;
    retirement_require(!wl_evaluation_control_report(control, &report));
    retirement_require(report.result == ended && report.attempt_id == id);
    *used = report.consumed;
    retirement_require(!wl_evaluation_control_detach(control, cs));
    wl_evaluation_control_release(control);
    return rc;
}

typedef struct {
    wl_col_session_t *session;
    wl_arena_compound_arena_gc_hold_t *hold;
    wl_arena_compound_arena_hold_release_t *receipt;
    int result;
    bool done;
    uint64_t used;
} arena_foreign_turn_t;

static void *
arena_foreign_turn(void *payload)
{
    arena_foreign_turn_t *p = payload;
    p->result = arena_hold_turn(p->session, p->hold, p->receipt, 128, &p->done,
            &p->used);
    return NULL;
}

static void
arena_stale_turn(wl_col_session_t *cs, wl_arena_compound_arena_gc_hold_t *hold,
    wl_arena_compound_arena_hold_release_t *receipt)
{
    wl_evaluation_control_t *control = NULL;
    retirement_require(!wl_evaluation_control_create(128, &control));
    retirement_require(!wl_evaluation_control_attach(control, cs));
    retirement_require(!wl_evaluation_control_begin(control, cs));
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    bool done = false;
    retirement_require(wl_arena_compound_arena_gc_hold_release_step_owned(hold,
        cs->compound_arena, receipt, cs, control, id + 1,
        &done) == EINVAL && !done);
    retirement_require(!wl_evaluation_control_finish(control, cs, 0, 0));
    wl_evaluation_control_report_t report;
    retirement_require(!wl_evaluation_control_report(control, &report)
        && !report.result && !report.consumed && report.attempt_id == id);
    retirement_require(!wl_evaluation_control_detach(control, cs));
    wl_evaluation_control_release(control);
}

typedef struct {
    wl_col_session_t *session;
    wl_arena_compound_arena_gc_step_t *receipt;
    int result;
    bool done;
    uint64_t used;
} arena_gc_foreign_turn_t;

static void *
arena_gc_foreign_turn(void *payload)
{
    arena_gc_foreign_turn_t *p = payload;
    uint32_t result = UINT32_MAX;
    p->result = arena_gc_turn(p->session, p->receipt, 128, false, &p->done,
            &result, &p->used);
    return NULL;
}

static void
arena_gc_stale_turn(wl_col_session_t *cs,
    wl_arena_compound_arena_gc_step_t *receipt)
{
    wl_evaluation_control_t *control = NULL;
    retirement_require(!wl_evaluation_control_create(128, &control));
    retirement_require(!wl_evaluation_control_attach(control, cs));
    retirement_require(!wl_evaluation_control_begin(control, cs));
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    bool done = false; uint32_t result = UINT32_MAX;
    retirement_require(wl_arena_compound_arena_gc_epoch_step_owned(
            cs->compound_arena,
            receipt, cs, control, id + 1, &done, &result) == EINVAL && !done);
    retirement_require(!wl_evaluation_control_finish(control, cs, 0, 0));
    wl_evaluation_control_report_t report;
    retirement_require(!wl_evaluation_control_report(control, &report)
        && !report.result && !report.consumed && report.attempt_id == id);
    retirement_require(!wl_evaluation_control_detach(control, cs));
    wl_evaluation_control_release(control);
}

static wl_arena_compound_arena_gc_hold_t retirement_peer_hold;
static wl_compound_arena_borrow_t retirement_peer_borrow;
static unsigned retirement_peer_event;

static void
retirement_peer(wl_compound_arena_t *arena, unsigned event)
{
    retirement_require(event == retirement_peer_event);
    wl_arena_compound_arena_test_retirement_peer = NULL;
    if (event) retirement_require(!wl_arena_compound_arena_gc_hold_acquire(
                arena, &retirement_peer_hold));
    else retirement_require(wl_compound_arena_borrow(arena,
            &retirement_peer_borrow));
}

static void
retirement_cancel_peer(wl_compound_arena_t *arena, unsigned event)
{
    (void)arena;
    retirement_require(retirement_funding && event == retirement_peer_event);
    wl_arena_compound_arena_test_retirement_peer = NULL;
    wl_evaluation_control_request_cancel(retirement_funding);
}

static uint32_t
arena_gc_complete(wl_col_session_t *cs,
    wl_arena_compound_arena_gc_step_t *receipt,
    bool try_short)
{
    bool done = false;
    uint32_t result = UINT32_MAX;
    uint64_t used = 0;
    for (unsigned calls = 0; !done; ++calls) {
        retirement_require(calls < 10000);
        bool scan = receipt->phase == WL_ARENA_COMPOUND_ARENA_GC_SCAN;
        unsigned phase = receipt->phase;
        uint32_t epoch = cs->compound_arena->current_epoch,
            index = receipt->index;
        bool gate = receipt->gate_owned;
        if (gate && receipt->phase == WL_ARENA_COMPOUND_ARENA_GC_SNAPSHOT) {
            wl_arena_compound_arena_gc_step_t copied = *receipt;
            retirement_require(arena_gc_turn(cs, &copied, 128, false, &done,
                &result, &used)
                == EBUSY && !done && !used && receipt->gate_owned);
            uint64_t refs =
                atomic_load_explicit(&cs->compound_arena->lifetime_state,
                    memory_order_acquire);
            uint32_t entries = epoch < cs->compound_arena->max_epochs
                ? cs->compound_arena->gens[epoch].entry_count : 0;
            arena_gc_foreign_turn_t foreign = {cs, receipt, 0, false,
                                               UINT64_MAX};
            wl_thread_t thread;
            retirement_require(!wl_thread_create(&thread, arena_gc_foreign_turn,
                &foreign));
            retirement_require(!wl_thread_join(&thread) &&
                foreign.result == EBUSY
                && !foreign.done && !foreign.used);
            arena_gc_stale_turn(cs, receipt);
            retirement_cancel_on_entry = true;
            retirement_require(arena_gc_turn(cs, receipt, 128, false, &done,
                &result, &used)
                == WL_EVALUATION_CONTROL_CANCELLED && !done && !used);
            retirement_require(receipt->gate_owned && receipt->phase == phase
                && receipt->index == index &&
                cs->compound_arena->current_epoch == epoch
                && atomic_load_explicit(&cs->compound_arena->lifetime_state,
                memory_order_acquire) == refs
                && (epoch >= cs->compound_arena->max_epochs
                || cs->compound_arena->gens[epoch].entry_count == entries));
        }
        if (try_short) {
            int rc = arena_gc_turn(cs, receipt, scan ? 1 : 127, scan, &done,
                    &result, &used);
            retirement_require(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !done);
            retirement_require(used == (scan ? 1u : 0u) &&
                receipt->phase == phase
                && receipt->index == index && receipt->gate_owned == gate
                && cs->compound_arena->current_epoch == epoch);
        }
        int rc = arena_gc_turn(cs, receipt, scan ? 1 : 128, false, &done,
                &result, &used);
        retirement_require((done ? rc == 0 : rc == EAGAIN) &&
            used == (scan ? 1u : 128u));
        if (scan) retirement_require(receipt->index == index + 1);
        if (receipt->gate_owned)
            retirement_require(wl_compound_arena_alloc(cs->compound_arena,
                8) == WL_COMPOUND_HANDLE_NULL);
    }
    retirement_require(!receipt->gate_owned);
    return result;
}

static void
test_owned_finite_arena_retirement(void)
{
    TEST("finite arena ownership, one-shot contention and exact/short grants");
    wl_plan_t *plan =
        build_plan(".decl a(x: int32)\n.decl r(x: int32)\nr(x) :- a(x).\n");
    ASSERT(plan, "plan");
    wl_session_t *session = NULL;
    ASSERT(!wl_session_create(wl_backend_columnar(), plan, 1, &session),
        "session");
    wl_col_session_t *cs = COL_SESSION(session);
    wl_compound_arena_t *arena = cs->compound_arena;
    wl_columnar_memory_governor_ref_t *governor = cs->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    uint64_t lifetime = atomic_load_explicit(&arena->lifetime_state,
            memory_order_acquire);
    wl_arena_compound_arena_gc_hold_t hold = {0};
    wl_arena_compound_arena_hold_release_t release = {0};
    ASSERT(!wl_arena_compound_arena_gc_hold_acquire(arena, &hold),
        "actual hold");
    uint64_t used = UINT64_MAX;
    bool done = false;
    ASSERT(arena_hold_turn(cs, &hold, &release, 127, &done, &used)
        == WL_EVALUATION_CONTROL_WORK_LIMIT && !done && !used
        && release.phase == WL_ARENA_COMPOUND_ARENA_HOLD_INERT &&
        hold.arena == arena,
        "one-short initial grant must not transfer hold");
    ASSERT(arena_hold_turn(cs, &hold, &release, 128, &done, &used) == EAGAIN
        && used == 128 && !done, "funded receipt");
    wl_arena_compound_arena_hold_release_t copied = release;
    ASSERT(arena_hold_turn(cs, &hold, &copied, 128, &done, &used) == EBUSY
        && !used && hold.arena == arena, "copied receipt");
    arena_foreign_turn_t foreign = {cs, &hold, &release, 0, false, UINT64_MAX};
    wl_thread_t foreign_thread;
    ASSERT(!wl_thread_create(&foreign_thread, arena_foreign_turn, &foreign),
        "foreign caller");
    ASSERT(!wl_thread_join(&foreign_thread) && foreign.result == EBUSY &&
        !foreign.used
        && !foreign.done && hold.arena == arena,
        "foreign receipt preserves original owner");
    arena_stale_turn(cs, &hold, &release);
    ASSERT(release.phase == WL_ARENA_COMPOUND_ARENA_HOLD_COUNT &&
        hold.arena == arena,
        "stale financial generation preserves hold");
    for (unsigned phase = WL_ARENA_COMPOUND_ARENA_HOLD_COUNT;
        phase <= WL_ARENA_COMPOUND_ARENA_HOLD_REFERENCE; ++phase){
        uint64_t refs = atomic_load_explicit(&arena->lifetime_state,
                memory_order_acquire);
        uint64_t holds = atomic_load_explicit(&arena->gc_hold_count,
                memory_order_acquire);
        retirement_peer_event = phase;
        wl_arena_compound_arena_test_retirement_peer = retirement_cancel_peer;
        ASSERT(arena_hold_turn(cs, &hold, &release, 128, &done, &used)
            == WL_EVALUATION_CONTROL_CANCELLED && !done && used == 128
            && release.phase == phase
            && atomic_load_explicit(&arena->lifetime_state,
            memory_order_acquire) == refs
            && atomic_load_explicit(&arena->gc_hold_count,
            memory_order_acquire) == holds,
            "post-peer cancellation must not execute count/reference CAS");
        wl_arena_compound_arena_test_retirement_peer = retirement_peer;
        ASSERT(arena_hold_turn(cs, &hold, &release, 128, &done, &used) == EAGAIN
            && !done && used == 128 && release.phase == phase,
            "genuine peer CAS contention");
        ASSERT(!wl_arena_compound_arena_gc_hold_release(&retirement_peer_hold),
            "peer hold release");
        ASSERT(arena_hold_turn(cs, &hold, &release, 127, &done, &used)
            == WL_EVALUATION_CONTROL_WORK_LIMIT && !done && !used
            && release.phase == phase, "refused remaining transition");
        int rc = arena_hold_turn(cs, &hold, &release, 128, &done, &used);
        ASSERT(used == 128 && (phase == WL_ARENA_COMPOUND_ARENA_HOLD_COUNT
            ? rc == EAGAIN && !done && release.reference_owed && !hold.arena
            : !rc && done && !release.reference_owed),
            "exact count/reference grants");
    }
    ASSERT(atomic_load_explicit(&arena->lifetime_state,
        memory_order_acquire) == lifetime
        && !atomic_load_explicit(&arena->gc_hold_count, memory_order_acquire),
        "exact hold/ref baseline");
    ASSERT(!arena_hold_turn(cs, &hold, &release, 1, &done,
        &used) && done && !used,
        "terminal acknowledgement cannot decrement again");

    wl_compound_arena_t *reference =
        wl_compound_arena_create(arena->session_seed, 1024, arena->max_epochs);
    ASSERT(reference, "raw oracle arena");
    for (unsigned i = 0; i < 4; ++i) {
        uint64_t h = wl_compound_arena_alloc(arena, 8),
            r = wl_compound_arena_alloc(reference, 8);
        ASSERT(h && r, "genuine entries");
        if (i == 1 || i == 3)
            ASSERT(!wl_compound_arena_retain(arena, h, -1)
                && !wl_compound_arena_retain(reference, r, -1),
                "real multiplicity transition");
    }
    wl_arena_compound_arena_gc_step_t gc = {0};
    uint32_t result = UINT32_MAX;
    ASSERT(arena_gc_turn(cs, &gc, 128, false, &done, &result, &used) == EAGAIN,
        "GC receipt");
    retirement_peer_event = 0;
    wl_arena_compound_arena_test_retirement_peer = retirement_cancel_peer;
    ASSERT(arena_gc_turn(cs, &gc, 128, false, &done, &result, &used)
        == WL_EVALUATION_CONTROL_CANCELLED && !done && used == 128 &&
        !gc.gate_owned
        && gc.phase == WL_ARENA_COMPOUND_ARENA_GC_GATE,
        "post-peer cancellation must not acquire writer gate");
    wl_arena_compound_arena_test_retirement_peer = retirement_peer;
    ASSERT(arena_gc_turn(cs, &gc, 128, false, &done, &result, &used) == EAGAIN
        && !gc.gate_owned, "actual borrow prevents writer CAS");
    ASSERT(wl_compound_arena_borrow_release(&retirement_peer_borrow),
        "peer borrow release");
    ASSERT(arena_gc_complete(cs, &gc, true) == 2
        && wl_compound_arena_gc_epoch_boundary(reference) == 2
        && arena->current_epoch == reference->current_epoch &&
        arena->live_handles == 2,
        "one-entry sweep matches independent multiplicity oracle");
    ASSERT(!arena_gc_turn(cs, &gc, 1, false, &done, &result, &used)
        && done && !used && result == 2, "GC DONE cannot sweep/reset again");
    memset(&gc, 0, sizeof(gc));
    wl_compound_arena_freeze(arena); wl_compound_arena_freeze(reference);
    ASSERT(arena_gc_complete(cs, &gc,
        true) == wl_compound_arena_gc_epoch_boundary(reference)
        && gc.disposition == WL_ARENA_COMPOUND_ARENA_GC_FROZEN
        && arena->current_epoch == reference->current_epoch, "frozen skip");
    wl_compound_arena_unfreeze(arena); wl_compound_arena_unfreeze(reference);
    ASSERT(!wl_arena_compound_arena_gc_hold_acquire(arena, &hold),
        "held skip source");
    memset(&gc, 0, sizeof(gc));
    uint32_t epoch = arena->current_epoch;
    ASSERT(arena_gc_complete(cs, &gc,
        true) == epoch && arena->current_epoch == epoch
        && gc.disposition == WL_ARENA_COMPOUND_ARENA_GC_HELD,
        "real hold skips sweep");
    ASSERT(!wl_arena_compound_arena_gc_hold_release(&hold),
        "held skip release");
    while (arena->current_epoch <
        arena->max_epochs) wl_compound_arena_gc_epoch_boundary(arena);
    memset(&gc, 0, sizeof(gc));
    ASSERT(arena_gc_complete(cs, &gc, true) == 0
        && gc.disposition == WL_ARENA_COMPOUND_ARENA_GC_SATURATED
        && arena->current_epoch == arena->max_epochs,
        "saturated skip without generation access");
    wl_compound_arena_free(reference);
    ASSERT(atomic_load_explicit(&arena->lifetime_state,
        memory_order_acquire) == lifetime,
        "writer protocol must not mint lifetime refs");
    wl_session_destroy(session); wl_plan_free(plan); plan_fixture_release();
    ASSERT(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor))
        && wl_columnar_memory_governor_ref_is_sole(governor),
        "complete session/governor baseline");
    wl_columnar_memory_governor_ref_release(governor);
    PASS();
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_session_compound_arena_lifecycle (Issue #559)\n");
    test_session_compound_arena_lifecycle();
    test_detached_session_arena();
    test_owned_finite_arena_retirement();
    printf("\n%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
