/*
 * test_session.c - Persistent session delta tests via wl_session_* API
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests the persistent columnar session: create, insert, step, delta callback.
 * Uses the wl_session_* wrapper API (not raw FFI), exercising the full
 * backend vtable dispatch.
 */

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/columnar/memory_governor.h"
#include "../wirelog/arena/compound_arena.h"
#include "../wirelog/intern.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#endif

/* ======================================================================== */
/* Portability Macros                                                       */
/* ======================================================================== */

/* MSVC does not support __attribute__((unused)); use a portable macro */
#ifdef _MSC_VER
#define UNUSED
#else
#define UNUSED __attribute__((unused))
#endif

/* ======================================================================== */
/* Test Helpers                                                             */
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
        } while (0)

static void
test_session_hash_overflow_rejected(void)
{
    TEST("session hash rejects overflowing relation counts");

    wl_col_session_t sess = { 0 };
    sess.nrels = UINT32_MAX / 2u + 2u;

    int rc = session_rel_build_hash(&sess);

    if (rc != ENOMEM || sess.rel_hash_nbuckets != 0
        || sess.rel_hash_head != NULL) {
        FAIL("overflowing relation count changed hash state");
        return;
    }
    PASS();
}

static void
test_retained_relation_admission(void)
{
    const uint64_t initial_bytes = 64u * sizeof(int64_t)
        + 64u * sizeof(col_delta_timestamp_t);
    const uint64_t grown_bytes = 128u * sizeof(int64_t)
        + 128u * sizeof(col_delta_timestamp_t);
    const uint64_t exact_budget = initial_bytes + grown_bytes;
    wl_columnar_memory_resolution_t resolution = { 0 };
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *rel = NULL;
    int64_t row = 7;
    int rc;

    TEST("retained relation growth admits peak and releases on destroy");
    resolution.budget_bytes = exact_budget;
    resolution.usable_bytes = exact_budget;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    if (!ref || col_rel_alloc(&rel, "retained") != 0
        || col_rel_attach_memory_governor(rel, ref) != 0) {
        if (rel)
            col_rel_destroy(rel);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        FAIL("retained relation setup failed");
        return;
    }
    rc = col_rel_set_schema(rel, 1, NULL);
    if (rc == 0)
        rc = col_rel_enable_timestamps(rel);
    for (uint32_t i = 0; rc == 0 && i < 64u; i++)
        rc = col_rel_append_row(rel, &row);
    if (rc == 0)
        rc = col_rel_append_row(rel, &row);
    if (rc != 0 || rel->capacity != 128u || rel->nrows != 65u
        || rel->timestamps[0].iteration != 0
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != grown_bytes) {
        col_rel_destroy(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("exact-fit retained growth was not committed");
        return;
    }
    rel->nrows = 0;
    col_rel_compact(rel);
    if (rel->capacity != 0u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0u
        || col_rel_append_row(rel, &row) != 0
        || rel->capacity != 64u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref))
        != 64u * sizeof(int64_t)) {
        col_rel_destroy(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("empty compaction left a stale retained admission");
        return;
    }
    col_rel_destroy(rel);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0) {
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("retained reservation leaked after destroy");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    TEST("retained relation denial preserves old rows and capacity");
    resolution.usable_bytes = exact_budget - 1u;
    resolution.budget_bytes = exact_budget - 1u;
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    rel = NULL;
    if (!ref || col_rel_alloc(&rel, "retained-denied") != 0
        || col_rel_attach_memory_governor(rel, ref) != 0) {
        if (rel)
            col_rel_destroy(rel);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        FAIL("denial setup failed");
        return;
    }
    rc = col_rel_set_schema(rel, 1, NULL);
    if (rc == 0)
        rc = col_rel_enable_timestamps(rel);
    for (uint32_t i = 0; rc == 0 && i < 64u; i++)
        rc = col_rel_append_row(rel, &row);
    rc = rc == 0 ? col_rel_append_row(rel, &row) : rc;
    if (rc != ENOMEM || rel->capacity != 64u || rel->nrows != 64u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != initial_bytes) {
        col_rel_destroy(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("denied retained growth changed relation state");
        return;
    }
    col_rel_destroy(rel);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0) {
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("denied relation reservation leaked after destroy");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();
}

/* ======================================================================== */
/* Delta Collector                                                          */
/* ======================================================================== */

#define MAX_DELTAS 256
#define MAX_COLS 16

typedef struct {
    int count;
    char relations[MAX_DELTAS][64];
    int64_t rows[MAX_DELTAS][MAX_COLS];
    uint32_t ncols[MAX_DELTAS];
    int32_t diffs[MAX_DELTAS];
} delta_collector_t;

static void
collect_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    delta_collector_t *c = (delta_collector_t *)user_data;
    if (c->count >= MAX_DELTAS)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 63);
    c->relations[idx][63] = '\0';
    c->ncols[idx] = ncols;
    c->diffs[idx] = diff;
    for (uint32_t i = 0; i < ncols && i < MAX_COLS; i++)
        c->rows[idx][i] = row[i];
}

/* ======================================================================== */
/* Tuple Collector (snapshot - no diff)                                    */
/* ======================================================================== */

typedef struct {
    int count;
    char relations[MAX_DELTAS][64];
    int64_t rows[MAX_DELTAS][MAX_COLS];
    uint32_t ncols[MAX_DELTAS];
} tuple_collector_t;

static void
collect_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    tuple_collector_t *c = (tuple_collector_t *)user_data;
    if (c->count >= MAX_DELTAS)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 63);
    c->relations[idx][63] = '\0';
    c->ncols[idx] = ncols;
    for (uint32_t i = 0; i < ncols && i < MAX_COLS; i++)
        c->rows[idx][i] = row[i];
}

/* ======================================================================== */
/* Delta Query Helpers                                                      */
/* ======================================================================== */

static int
count_deltas(const delta_collector_t *c, const char *relation, int32_t diff)
{
    int n = 0;
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) == 0 && c->diffs[i] == diff)
            n++;
    }
    return n;
}

static bool
has_delta(const delta_collector_t *c, const char *relation,
    const int64_t *expected, uint32_t ncols, int32_t diff)
{
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) != 0)
            continue;
        if (c->ncols[i] != ncols || c->diffs[i] != diff)
            continue;
        bool match = true;
        for (uint32_t j = 0; j < ncols; j++) {
            if (c->rows[i][j] != expected[j]) {
                match = false;
                break;
            }
        }
        if (match)
            return true;
    }
    return false;
}

static bool
has_tuple(const tuple_collector_t *c, const char *relation,
    const int64_t *expected, uint32_t ncols)
{
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) != 0)
            continue;
        if (c->ncols[i] != ncols)
            continue;
        bool match = true;
        for (uint32_t j = 0; j < ncols; j++) {
            if (c->rows[i][j] != expected[j]) {
                match = false;
                break;
            }
        }
        if (match)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Helper: Build plan from Datalog source                                   */
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
/* Tests                                                                    */
/* ======================================================================== */

/* Issue #1431: the program's intern table is charged to the governor of the
 * first session created from its plan, a second session from the same plan
 * tolerates that the table is already owned (EBUSY), destroying either
 * session leaves the program-owned reservation in place, and only
 * wirelog_program_free() releases it. */
static void
test_intern_reservation_program_lifetime(void)
{
    TEST("session: intern reservation is program-owned and survives destroy");
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        "edge(1, 2).\n"
        "path(x, y) :- edge(x, y).\n", &err);
    wl_plan_t *plan = NULL;
    wl_session_t *first = NULL;
    wl_session_t *second = NULL;
    wl_columnar_memory_governor_ref_t *owner = NULL;
    uint64_t with_first;
    uint64_t retained;

    if (!prog || wl_plan_from_program(prog, &plan) != 0 || !plan) {
        if (prog)
            wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }
    if (wl_session_create(wl_backend_columnar(), plan, 1, &first) != 0
        || !first) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("first session create failed");
        return;
    }
    owner = COL_SESSION(first)->memory_governor;
    wl_columnar_memory_governor_ref_retain(owner);
    with_first = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(owner));

    /* The second session gets its own governor; the intern stays with the
     * first one and is neither re-charged nor stolen. */
    if (wl_session_create(wl_backend_columnar(), plan, 1, &second) != 0
        || !second
        || COL_SESSION(second)->memory_governor == owner
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner)) != with_first) {
        if (second)
            wl_session_destroy(second);
        wl_session_destroy(first);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(owner);
        FAIL("second session did not tolerate the owned intern table");
        return;
    }
    wl_session_destroy(second);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner)) != with_first) {
        wl_session_destroy(first);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(owner);
        FAIL("destroying the second session changed the owner's reservation");
        return;
    }
    wl_session_destroy(first);
    retained = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(owner));
    wl_plan_free(plan);
    if (retained == 0 || retained > with_first
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner)) != retained) {
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(owner);
        FAIL("session destroy released or kept the wrong intern bytes");
        return;
    }
    wirelog_program_free(prog);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(owner)) != 0) {
        wl_columnar_memory_governor_ref_release(owner);
        FAIL("wirelog_program_free did not release the intern reservation");
        return;
    }
    wl_columnar_memory_governor_ref_release(owner);
    PASS();
}

/*
 * Test: create a session and destroy it without crash.
 */
static void
test_session_create_destroy_impl(const wl_compute_backend_t *backend)
{
    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(backend, ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

static void
test_session_create_destroy(void)
{
    TEST("session(columnar): create and destroy");
    test_session_create_destroy_impl(wl_backend_columnar());
}

static void
test_session_create_destroy_columnar(void)
{
    TEST("session(columnar): create and destroy (duplicate check)");
    test_session_create_destroy_impl(wl_backend_columnar());
}

static void
test_session_create_with_options(void)
{
    TEST("session: internal host options are accepted");
    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    wl_session_options_t options;
    wl_session_t *session = NULL;
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }
    wl_session_options_init(&options);
    /* Invalid on every host, but safe: providers must fall back to advisory. */
    options.windows_job_handle = (void *)(uintptr_t)1;
    int rc = wl_session_create_with_options(wl_backend_columnar(), ffi, 1,
            &options, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session options creation failed");
        return;
    }
    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* Issue #1473: injected governor and create-time admission                 */
/* ======================================================================== */

static wl_columnar_memory_governor_ref_t *
enforcing_governor(uint64_t usable)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = usable;
    resolution.usable_bytes = usable;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static uint64_t
reserved_on(wl_columnar_memory_governor_ref_t *ref)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
}

/* Bytes a session admits for the program's intern table when it attaches
 * the table (#1431), measured with a throwaway governor and detached again
 * so the table is free for the session under test. */
static int
probe_intern_bytes(wl_intern_t *intern, uint64_t *out)
{
    wl_columnar_memory_governor_ref_t *probe
        = enforcing_governor(UINT64_MAX / 4u);
    int rc;
    if (!probe)
        return -1;
    rc = wl_intern_attach_memory_governor(intern, probe);
    if (rc == 0) {
        *out = reserved_on(probe);
        rc = wl_intern_detach_memory_governor(intern, probe);
        if (rc == 0 && reserved_on(probe) != 0)
            rc = -1;
    }
    wl_columnar_memory_governor_ref_release(probe);
    return rc;
}

/* Bytes the session's fixed compound arena admits at creation, using the
 * same shape col_session_create_internal builds (seed, default_gen_cap and
 * the library-default epoch count; like the budget tests this assumes
 * WIRELOG_COMPOUND_MAX_EPOCHS is unset in the test environment). */
static int
probe_compound_bytes(uint64_t *out)
{
    wl_columnar_memory_governor_ref_t *probe
        = enforcing_governor(UINT64_MAX / 4u);
    wl_compound_arena_t *arena;
    int rc = 0;
    if (!probe)
        return -1;
    arena = wl_compound_arena_create_managed(0x53455353u, 4096u, 0u,
            wl_columnar_memory_governor_ref_get(probe));
    if (!arena) {
        rc = -1;
    } else {
        *out = reserved_on(probe);
        wl_compound_arena_free(arena);
        if (reserved_on(probe) != 0)
            rc = -1;
    }
    wl_columnar_memory_governor_ref_release(probe);
    return rc;
}

/* A fact-free program: nothing but the create-time floor (intern table plus
 * fixed compound arena; the delta pool and eval arena degrade to malloc) is
 * admitted at session creation. */
static const char *INJECTED_GOVERNOR_SRC
    = ".decl edge(x: int64, y: int64)\n"
    ".decl path(x: int64, y: int64)\n"
    "path(x, y) :- edge(x, y).\n";

static void
test_session_injected_governor_admission(void)
{
    wirelog_error_t err;
    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    wl_session_options_t options;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    wl_columnar_memory_reservation_t token;
    wl_mem_ledger_snapshot_t ledger;
    uint64_t intern_bytes = 0;
    uint64_t compound_bytes = 0;
    int rc;

    TEST("session(#1473): injected governor denies below the intern floor");
    prog = wirelog_parse_string(INJECTED_GOVERNOR_SRC, &err);
    if (!prog || wl_plan_from_program(prog, &plan) != 0 || !plan) {
        if (prog)
            wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }
    /* The overflow case below needs more than 16 intern bytes so that the
     * pre-reserved total overflows instead of being merely denied. */
    if (probe_intern_bytes(plan->intern, &intern_bytes) != 0
        || intern_bytes <= 16u || probe_compound_bytes(&compound_bytes) != 0
        || compound_bytes == 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("could not measure the create-time floor");
        return;
    }

    /* The whole intern table is admitted transactionally; one byte short
     * fails creation before anything else is charged and leaves the
     * caller's reference untouched (LSan proves the refcount). */
    ref = enforcing_governor(intern_bytes - 1u);
    wl_session_options_init(&options);
    options.memory_governor = ref;
    session = NULL;
    rc = ref ? wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session) : -1;
    if (!ref || rc != ENOMEM || session != NULL || reserved_on(ref) != 0) {
        if (session)
            wl_session_destroy(session);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("intern-floor denial did not fail creation with ENOMEM");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    /* The intern table fits but the compound arena does not: creation fails
     * and the oom path detaches the table it attached. */
    TEST("session(#1473): injected governor denies below the compound floor");
    ref = enforcing_governor(intern_bytes + compound_bytes - 1u);
    options.memory_governor = ref;
    session = NULL;
    rc = ref ? wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session) : -1;
    if (!ref || rc != ENOMEM || session != NULL || reserved_on(ref) != 0) {
        if (session)
            wl_session_destroy(session);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("compound-floor denial did not fail creation with ENOMEM");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    /* Exact fit: creation succeeds on the injected governor itself, charges
     * exactly the floor, and destroy releases everything but the
     * program-owned intern reservation. */
    TEST("session(#1473): injected governor admits the exact floor");
    ref = enforcing_governor(intern_bytes + compound_bytes);
    options.memory_governor = ref;
    session = NULL;
    rc = ref ? wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session) : -1;
    if (rc == 0 && session)
        wl_mem_ledger_snapshot(&COL_SESSION(session)->mem_ledger, &ledger);
    if (!ref || rc != 0 || !session
        || COL_SESSION(session)->memory_governor != ref
        || wl_session_memory_governor(session)
        != wl_columnar_memory_governor_ref_get(ref)
        || reserved_on(ref) != intern_bytes + compound_bytes
        || ledger.total_budget != intern_bytes + compound_bytes) {
        if (session)
            wl_session_destroy(session);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("exact-fit creation did not use the injected governor");
        return;
    }
    wl_session_destroy(session);
    if (reserved_on(ref) != intern_bytes) {
        wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("destroy did not leave exactly the intern reservation");
        return;
    }
    wl_plan_free(plan);
    wirelog_program_free(prog);
    prog = NULL;
    plan = NULL;
    if (reserved_on(ref) != 0) {
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("program free did not release the intern reservation");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    /* Arithmetic overflow of the governor total is EOVERFLOW, not ENOMEM. */
    TEST("session(#1473): injected governor overflow fails with EOVERFLOW");
    prog = wirelog_parse_string(INJECTED_GOVERNOR_SRC, &err);
    if (!prog || wl_plan_from_program(prog, &plan) != 0 || !plan) {
        if (prog)
            wirelog_program_free(prog);
        FAIL("plan generation failed");
        return;
    }
    ref = enforcing_governor(UINT64_MAX);
    wl_columnar_memory_reservation_init(&token);
    if (!ref || wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(ref), UINT64_MAX - 16u,
            &token) != WL_COLUMNAR_MEMORY_ADMISSION_OK) {
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("overflow fixture setup failed");
        return;
    }
    options.memory_governor = ref;
    session = NULL;
    rc = wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session);
    if (rc != EOVERFLOW || session != NULL
        || reserved_on(ref) != UINT64_MAX - 16u) {
        if (session)
            wl_session_destroy(session);
        (void)wl_columnar_memory_release(&token);
        wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("governor overflow did not fail creation with EOVERFLOW");
        return;
    }
    (void)wl_columnar_memory_release(&token);
    wl_columnar_memory_governor_ref_release(ref);
    PASS();

    /* The test-only per-thread default stands in for options on the
     * option-less creators used by the public facades. */
    TEST("session(#1473): thread default options reach option-less create");
    ref = enforcing_governor(intern_bytes - 1u);
    options.memory_governor = ref;
    wl_session_testhook_set_default_options(&options);
    session = NULL;
    rc = ref ? wl_session_create(wl_backend_columnar(), plan, 1, &session)
        : -1;
    wl_session_testhook_set_default_options(NULL);
    if (!ref || rc != ENOMEM || session != NULL || reserved_on(ref) != 0
        || wl_session_testhook_default_options() != NULL) {
        if (session)
            wl_session_destroy(session);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("thread default options were not applied");
        return;
    }
    wl_columnar_memory_governor_ref_release(ref);
    session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("clearing the thread default did not restore resolution");
        return;
    }
    wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

#ifdef _WIN32
/*
 * The provider and session plumbing must be tested with a real Job Object,
 * not only with injected resolver fields.  The session borrows the handle
 * during creation, so closing the caller's handle before destruction also
 * verifies that the session does not retain or close it.
 */
static void
test_session_windows_job_options(void)
{
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits;
    HANDLE job = NULL;
    char *saved_budget = NULL;
    size_t saved_budget_len = 0;
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    wl_session_options_t options;
    wl_columnar_memory_governor_t *governor;
    const uint64_t expected_budget = UINT64_C(512) * 1024 * 1024;
    int ok = 0;

    TEST("session: real Windows Job Object memory source");
    if (_dupenv_s(&saved_budget, &saved_budget_len,
        "WIRELOG_MEMORY_BUDGET") != 0)
        saved_budget = NULL;
    /* An explicit environment budget intentionally has precedence. */
    if (_putenv_s("WIRELOG_MEMORY_BUDGET", "") != 0) {
        FAIL("could not clear WIRELOG_MEMORY_BUDGET");
        goto cleanup;
    }

    job = CreateJobObjectW(NULL, NULL);
    if (!job) {
        FAIL("CreateJobObjectW failed");
        goto cleanup;
    }
    memset(&limits, 0, sizeof(limits));
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_PROCESS_MEMORY;
    limits.ProcessMemoryLimit = (SIZE_T)expected_budget;
    if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation,
        &limits, sizeof(limits))) {
        FAIL("SetInformationJobObject failed");
        goto cleanup;
    }

    plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate Job Object test plan");
        goto cleanup;
    }
    wl_session_options_init(&options);
    options.windows_job_handle = (void *)job;
    if (wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) != 0 || !session) {
        FAIL("session creation with Job Object failed");
        goto cleanup;
    }

    /* The session must have completed its synchronous probe by now. */
    CloseHandle(job);
    job = NULL;
    governor = wl_session_memory_governor(session);
    if (!governor
        || governor->source != WL_COLUMNAR_MEMORY_SOURCE_WINDOWS_JOB
        || governor->mode != WL_COLUMNAR_MEMORY_MODE_ENFORCING
        || governor->budget_bytes != expected_budget) {
        FAIL("session did not enforce the Job Object memory source");
        goto cleanup;
    }
    ok = 1;

cleanup:
    if (job)
        CloseHandle(job);
    if (session)
        wl_session_destroy(session);
    if (plan)
        wl_plan_free(plan);
    if (saved_budget)
        _putenv_s("WIRELOG_MEMORY_BUDGET", saved_budget);
    else
        _putenv_s("WIRELOG_MEMORY_BUDGET", "");
    free(saved_budget);
    if (ok)
        PASS();
}
#endif

/*
 * Test: num_workers > 1 is rejected.
 */
static void
test_session_create_multi_worker_rejected(void)
{
    TEST("session: multi-worker accepted");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 2, &session);
    wl_plan_free(ffi);

    if (rc == 0 && session) {
        wl_session_destroy(session);
        PASS();
    } else {
        FAIL("multi-worker session creation should succeed");
    }
}

/*
 * Test: initial step produces all results as diff=+1.
 * Datalog: r(X,Y,Z) :- a(X,Y), b(Y,Z).
 * EDB: a={(1,2)}, b={(2,3)}
 * Expected delta: r(1,2,3) with diff=+1
 */
static void
test_session_step_initial_delta(void)
{
    TEST("session: initial step produces diff=+1 deltas");

    wl_plan_t *ffi = build_plan(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, z: int32)\n"
            ".decl r(x: int32, y: int32, z: int32)\n"
            "r(x, y, z) :- a(x, y), b(y, z).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    /* Set delta callback */
    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert EDB facts */
    int64_t a_data[] = { 1, 2 };
    rc = wl_session_insert(session, "a", a_data, 1, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert a failed");
        return;
    }

    int64_t b_data[] = { 2, 3 };
    rc = wl_session_insert(session, "b", b_data, 1, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert b failed");
        return;
    }

    /* Step: should produce delta r(1,2,3) with diff=+1 */
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("session_step failed");
        return;
    }

    int64_t expected[] = { 1, 2, 3 };
    if (!has_delta(&deltas, "r", expected, 3, +1)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected r(1,2,3) diff=+1, got %d deltas total",
            deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 positive delta, got %d",
            count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: incremental insert + step produces only new derivations.
 * After initial step (r(1,2,3)), insert a=(1,4), b=(4,5).
 * Second step should produce exactly r(1,4,5) with diff=+1.
 */
static void
test_session_step_incremental_delta(void)
{
    TEST("session: incremental step produces only new deltas");

    wl_plan_t *ffi = build_plan(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, z: int32)\n"
            ".decl r(x: int32, y: int32, z: int32)\n"
            "r(x, y, z) :- a(x, y), b(y, z).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Initial data */
    int64_t a1[] = { 1, 2 };
    int64_t b1[] = { 2, 3 };
    wl_session_insert(session, "a", a1, 1, 2);
    wl_session_insert(session, "b", b1, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Reset collector for second step */
    memset(&deltas, 0, sizeof(deltas));

    /* Insert new facts */
    int64_t a2[] = { 1, 4 };
    int64_t b2[] = { 4, 5 };
    wl_session_insert(session, "a", a2, 1, 2);
    wl_session_insert(session, "b", b2, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    /* Should have exactly r(1,4,5) with diff=+1 */
    int64_t expected[] = { 1, 4, 5 };
    if (!has_delta(&deltas, "r", expected, 3, +1)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected r(1,4,5) diff=+1, got %d deltas total",
            deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 positive delta, got %d",
            count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: step with no changes produces zero delta callbacks.
 */
static void
test_session_step_no_change(void)
{
    TEST("session: step with no changes produces no deltas");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert and step once */
    int64_t a_data[] = { 10 };
    wl_session_insert(session, "a", a_data, 1, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Reset collector, step again with no changes */
    memset(&deltas, 0, sizeof(deltas));
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    if (deltas.count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 deltas, got %d", deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* RED Tests: remove and snapshot (require TASK 1-3 for GREEN)             */
/* ======================================================================== */

/*
 * Test: remove a tuple and step produces diff=-1 delta.
 *
 * RED: currently fails because dd_session_remove returns -1 (not implemented).
 * GREEN: after TASK 2+3 wire wl_dd_session_remove through the vtable.
 */
static void
test_session_remove_single_delta(void)
{
    TEST("session: remove tuple produces diff=-1 delta");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert a=(1) and step: establishes r(1) */
    int64_t a_data[] = { 1 };
    rc = wl_session_insert(session, "a", a_data, 1, 1);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert failed");
        return;
    }
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Reset and remove a=(1) */
    memset(&deltas, 0, sizeof(deltas));
    rc = wl_session_remove(session, "a", a_data, 1, 1);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remove returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    /* Step: should produce r(1) diff=-1 */
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    int64_t expected[] = { 1 };
    if (!has_delta(&deltas, "r", expected, 1, -1)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected r(1) diff=-1, got %d total deltas",
            deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", -1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 retraction, got %d",
            count_deltas(&deltas, "r", -1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: remove of non-existent tuple is a no-op (no crash, rc=0).
 *
 * RED: currently fails because dd_session_remove returns -1.
 * GREEN: after TASK 2+3 remove returns 0 and produces no phantom deltas.
 */
static void
test_session_remove_nonexistent(void)
{
    TEST("session: remove non-existent tuple is no-op");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    /* Remove a=(99) which was never inserted */
    int64_t a_data[] = { 99 };
    rc = wl_session_remove(session, "a", a_data, 1, 1);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remove returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: snapshot on empty session returns 0 tuples with rc=0.
 */
static void
test_session_snapshot_empty(void)
{
    TEST("session: snapshot of empty session returns 0 tuples");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    tuple_collector_t tuples;
    memset(&tuples, 0, sizeof(tuples));
    rc = wl_session_snapshot(session, collect_tuple, &tuples);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "snapshot returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (tuples.count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 tuples, got %d", tuples.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: snapshot after insert+step reflects current derived state.
 *
 * RED: currently fails because dd_session_snapshot uses empty batch worker.
 * GREEN: after TASK 2+3 snapshot reads from persistent session state.
 */
static void
test_session_snapshot_after_insert(void)
{
    TEST("session: snapshot reflects current derived state");

    wl_plan_t *ffi = build_plan(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, z: int32)\n"
            ".decl r(x: int32, y: int32, z: int32)\n"
            "r(x, y, z) :- a(x, y), b(y, z).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    /* Insert a=(1,2), b=(2,3) and step */
    int64_t a_data[] = { 1, 2 };
    int64_t b_data[] = { 2, 3 };
    wl_session_insert(session, "a", a_data, 1, 2);
    wl_session_insert(session, "b", b_data, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("step failed");
        return;
    }

    /* Snapshot: should contain r(1,2,3) */
    tuple_collector_t tuples;
    memset(&tuples, 0, sizeof(tuples));
    rc = wl_session_snapshot(session, collect_tuple, &tuples);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "snapshot returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    int64_t expected[] = { 1, 2, 3 };
    if (!has_tuple(&tuples, "r", expected, 3)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected r(1,2,3) in snapshot, got %d tuples total",
            tuples.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (tuples.count != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected exactly 1 tuple, got %d",
            tuples.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: duplicate insert of the same tuple produces exactly one positive delta.
 * DD set semantics via consolidate should not double-count.
 */
static void
test_session_duplicate_insert(void)
{
    TEST("session: duplicate insert produces single positive delta");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert a=(5) twice before stepping */
    int64_t a_data[] = { 5 };
    wl_session_insert(session, "a", a_data, 1, 1);
    wl_session_insert(session, "a", a_data, 1, 1);

    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("step failed");
        return;
    }

    int64_t expected[] = { 5 };
    if (!has_delta(&deltas, "r", expected, 1, +1)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected r(5) diff=+1, got %d deltas total",
            deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected exactly 1 positive delta (set semantics), got %d",
            count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* TEST: Transitive Closure Insert (RED phase - will FAIL)                 */
/* ======================================================================== */

static void
test_session_tc_insert(void)
{
    TEST("session: transitive closure insert test (recursive iterate)");

    /* Program:
       tc(X, Y) :- edge(X, Y).
       tc(X, Z) :- edge(X, Y), tc(Y, Z).
     */
    wl_plan_t *ffi = build_plan(".decl edge(x: int32, y: int32)\n"
            ".decl tc(x: int32, y: int32)\n"
            "tc(X, Y) :- edge(X, Y).\n"
            "tc(X, Z) :- edge(X, Y), tc(Y, Z).\n");

    if (!ffi) {
        FAIL("failed to parse TC program");
        return;
    }

    /* Create persistent session */
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0) {
        wl_plan_free(ffi);
        FAIL("session creation failed");
        return;
    }

    /* Setup delta collector */
    delta_collector_t deltas = { 0 };
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Step 1: Insert edge(1,2), edge(2,3) */
    int64_t edge_12[] = { 1, 2 };
    int64_t edge_23[] = { 2, 3 };
    wl_session_insert(session, "edge", edge_12, 1, 2);
    wl_session_insert(session, "edge", edge_23, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Should have TC: (1,2), (2,3), (1,3) */
    if (!has_delta(&deltas, "tc", (int64_t[]){ 1, 2 }, 2, +1)) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("expected tc(1,2) with diff=+1");
        return;
    }
    if (!has_delta(&deltas, "tc", (int64_t[]){ 1, 3 }, 2, +1)) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("expected tc(1,3) with diff=+1 (recursive)");
        return;
    }

    /* Reset for step 2 */
    memset(&deltas, 0, sizeof(deltas));

    /* Step 2: Insert edge(3,4) - should derive tc(1,4), tc(2,4), tc(3,4) */
    int64_t edge_34[] = { 3, 4 };
    wl_session_insert(session, "edge", edge_34, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    /* Verify exactly 3 new TC tuples */
    int new_tc_count = count_deltas(&deltas, "tc", +1);
    if (new_tc_count != 3) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 3 new TC deltas, got %d",
            new_tc_count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (!has_delta(&deltas, "tc", (int64_t[]){ 1, 4 }, 2, +1)
        || !has_delta(&deltas, "tc", (int64_t[]){ 2, 4 }, 2, +1)
        || !has_delta(&deltas, "tc", (int64_t[]){ 3, 4 }, 2, +1)) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("expected tc(1,4), tc(2,4), tc(3,4) deltas");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_session: persistent columnar session delta tests\n");

    test_session_hash_overflow_rejected();
    test_retained_relation_admission();
    test_intern_reservation_program_lifetime();
    test_session_create_destroy();
    test_session_create_destroy_columnar();
    test_session_create_with_options();
    test_session_injected_governor_admission();
#ifdef _WIN32
    test_session_windows_job_options();
#endif
    test_session_create_multi_worker_rejected();
    test_session_step_initial_delta();
    test_session_step_incremental_delta();
    test_session_step_no_change();

    /* GREEN: diff=-1 retraction deltas now implemented */
    test_session_remove_single_delta();
    test_session_remove_nonexistent();
    /* GREEN: col_session_remove returns 0 for uninitialized schema */
    /* test_session_snapshot_empty(); */
    /* test_session_snapshot_after_insert(); */
    /* test_session_duplicate_insert(); */

    /* RED: the following tests require TASK 4 (Variable collections + iterate) */
    /* Disabled until wl_session_iterate is implemented in columnar backend */
    /* test_session_tc_insert(); */

    printf("\n  %d tests: %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
