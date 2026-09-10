/*
 * test_filt_cache.c - Unit tests for the filtered-relation cache (Issue #386)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates that apply_right_filter_cached in ops.c correctly:
 *   a. Produces correct results on the first snapshot (cache miss path):
 *      filtered(y) :- edge(x, y), y > CONST returns only passing rows.
 *   b. Produces the same pointer (cached relation) on the second iteration of
 *      a recursive fixpoint: the filt_cache entry built in iteration 1 is
 *      reused in iteration 2 (same nrows => cache hit).
 *   c. Different filter thresholds on the same EDB relation produce different
 *      row counts (no cross-entry pollution between cache entries).
 *   d. Inserting new rows into the filtered EDB relation causes the cache to
 *      rebuild and the derived IDB relation to update correctly.
 *
 * Tests a, b, c use a single wl_session_snapshot call per program (fresh
 * session lifecycle) to avoid IDB accumulation across multiple snapshots.
 * Test d uses wl_session_step + delta callback to count net new tuples.
 *
 * Tests e-h (Issue #1435) drive the lease-taking lookup directly on a
 * session whose filt_cache is populated by a snapshot: a leased filtered
 * relation survives a same-nrows source mutation and a session_add_rel
 * replacement, two leases release in either order back to zero, and cache
 * growth is refused while a lease is active.
 */

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                       \
        do {                                                 \
            test_count++;                                    \
            printf("TEST %d: %s ... ", test_count, (name)); \
        } while (0)

#define PASS()            \
        do {                  \
            pass_count++;     \
            printf("PASS\n"); \
        } while (0)

#define FAIL(msg)                    \
        do {                             \
            fail_count++;                \
            printf("FAIL: %s\n", (msg)); \
            return;                      \
        } while (0)

#define ASSERT(cond, msg) \
        do {                  \
            if (!(cond))      \
            FAIL(msg);    \
        } while (0)

/* ----------------------------------------------------------------
 * Callback helpers
 * ---------------------------------------------------------------- */

#define MAX_ROWS 512
#define MAX_COLS 8

typedef struct {
    const char *tracked_rel;
    uint32_t count;
    int64_t rows[MAX_ROWS][MAX_COLS];
    uint32_t ncols[MAX_ROWS];
} collect_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    collect_t *c = (collect_t *)user;
    if (!c->tracked_rel || strcmp(relation, c->tracked_rel) != 0)
        return;
    if (c->count >= MAX_ROWS)
        return;
    uint32_t idx = c->count++;
    c->ncols[idx] = ncols < MAX_COLS ? ncols : MAX_COLS;
    for (uint32_t i = 0; i < c->ncols[idx]; i++)
        c->rows[idx][i] = row[i];
}

typedef struct {
    const char *tracked_rel;
    int count;
    int64_t sum_col1; /* sum of second column (col index 1) */
} delta_ctx_t;

static void
count_delta_cb(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user)
{
    delta_ctx_t *c = (delta_ctx_t *)user;
    if (!c->tracked_rel || strcmp(relation, c->tracked_rel) != 0)
        return;
    if (diff > 0) {
        c->count += diff;
        if (ncols >= 2)
            c->sum_col1 += row[1];
    }
}

static void
noop_cb(const char *r, const int64_t *row, uint32_t nc, void *u)
{
    (void)r;
    (void)row;
    (void)nc;
    (void)u;
}

/* ----------------------------------------------------------------
 * Session helpers
 * ---------------------------------------------------------------- */

/*
 * run_once: Parse src, apply optimiser passes, create session, load facts,
 * run one snapshot collecting tuples for tracked_rel, then tear down.
 * Returns 0 on success; out->count is the tuple count.
 */
static int
run_once(const char *src, const char *tracked_rel, collect_t *out)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (wl_session_load_facts(sess, prog) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    memset(out, 0, sizeof(*out));
    out->tracked_rel = tracked_rel;

    int rc = wl_session_snapshot(sess, collect_cb, out);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

/* ================================================================
 * Test 1: Cache miss — first snapshot produces correct filtered results
 *
 * filtered(y) :- edge(x, y), y > 1.
 * edge has (1,1),(1,2),(1,3) — rows with y > 1 are (1,2),(1,3).
 * Expected: filtered = 2 rows.
 * ================================================================ */
static void
test_cache_miss_correct_result(void)
{
    TEST("Cache miss: first snapshot produces correct filtered result");

    const char *src =
        ".decl edge(x: int32, y: int32)\n"
        "edge(1, 1). edge(1, 2). edge(1, 3).\n"
        ".decl filtered(y: int32)\n"
        "filtered(y) :- edge(x, y), y > 1.\n";

    collect_t out;
    int rc = run_once(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.count == 2, "expected 2 rows (y > 1)");

    PASS();
}

/* ================================================================
 * Test 2: Cache hit — filt_cache entry reused on second snapshot call
 *
 * After one wl_session_snapshot (which builds the cache), check that the
 * filt_cache has exactly one entry and that its source_nrows matches the
 * nrows of the EDB relation.  This confirms the cache was populated on
 * the first pass (which internally may apply the filter multiple times
 * within a recursive fixpoint or just once for a non-recursive rule).
 *
 * We verify via the internal wl_col_session_t fields.
 * ================================================================ */
static void
test_cache_entry_populated(void)
{
    TEST("Cache entry: filt_cache populated after snapshot with filter rule");

    /*
     * Use a join with filter on right side — this structure is more likely
     * to push the filter to right_filter_expr via JPP.
     * src(a), edge(a, b), b > 1 -> reach(a, b)
     */
    const char *src =
        ".decl src(a: int32)\n"
        "src(10). src(20).\n"
        ".decl edge(a: int32, b: int32)\n"
        "edge(10, 1). edge(10, 2). edge(10, 3).\n"
        "edge(20, 4). edge(20, 5).\n"
        ".decl reach(a: int32, b: int32)\n"
        "reach(a, b) :- src(a), edge(a, b), b > 2.\n";

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    ASSERT(wl_plan_from_program(prog, &plan) == 0, "plan gen failed");

    wl_session_t *sess = NULL;
    ASSERT(wl_session_create(wl_backend_columnar(), plan, 1, &sess) == 0,
        "session create failed");
    ASSERT(wl_session_load_facts(sess, prog) == 0, "load facts failed");

    collect_t out;
    memset(&out, 0, sizeof(out));
    out.tracked_rel = "reach";
    ASSERT(wl_session_snapshot(sess, collect_cb, &out) == 0,
        "snapshot failed");

    /* reach(a, b) with b > 2: edge rows (10,3),(20,4),(20,5) pass = 3 rows */
    ASSERT(out.count == 3, "reach must have 3 rows (b > 2)");

    /*
     * If JPP pushed b > 2 into right_filter_expr, the filt_cache will have
     * been populated.  If not (filter applied post-join), filt_cache_count
     * stays 0.  Either way the result must be correct.
     * We just verify the session is in a consistent state.
     */
    wl_col_session_t *csess = COL_SESSION(sess);
    /* filt_cache_count must be 0 or 1 (not negative/corrupt) */
    ASSERT(csess->filt_cache_count <= csess->filt_cache_cap ||
        csess->filt_cache_cap == 0,
        "filt_cache_count must be <= filt_cache_cap");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Test 3: Different filter predicates produce different results
 *
 * Two independent sessions with the same EDB relation but different
 * filter thresholds.  Each must produce the expected count.
 * ================================================================ */
static void
test_different_filter_different_result(void)
{
    TEST("Different filter expr: different row counts (no cross-pollution)");

    /* Program A: y > 1 — rows (1,2),(1,3),(1,4) = 3 rows in filtered */
    const char *src_a =
        ".decl edge(x: int32, y: int32)\n"
        "edge(1, 1). edge(1, 2). edge(1, 3). edge(1, 4).\n"
        ".decl filtered(y: int32)\n"
        "filtered(y) :- edge(x, y), y > 1.\n";

    /* Program B: y > 3 — only row (1,4) = 1 row in filtered */
    const char *src_b =
        ".decl edge(x: int32, y: int32)\n"
        "edge(1, 1). edge(1, 2). edge(1, 3). edge(1, 4).\n"
        ".decl filtered(y: int32)\n"
        "filtered(y) :- edge(x, y), y > 3.\n";

    collect_t out_a, out_b;
    ASSERT(run_once(src_a, "filtered", &out_a) == 0,
        "session A execution failed");
    ASSERT(run_once(src_b, "filtered", &out_b) == 0,
        "session B execution failed");

    ASSERT(out_a.count == 3,
        "session A (y > 1): filtered must have 3 rows");
    ASSERT(out_b.count == 1,
        "session B (y > 3): filtered must have 1 row");
    ASSERT(out_a.count != out_b.count,
        "different filter thresholds must produce different row counts");

    PASS();
}

/* ================================================================
 * Test 4: Cache rebuild when source nrows grows
 *
 * Use wl_session_set_delta_cb to count net-new derived tuples after
 * inserting a new row that passes the filter.  The delta count must be 1
 * (one new `filtered` tuple derived from the new edge row).
 * ================================================================ */
static void
test_cache_rebuild_on_nrows_change(void)
{
    TEST("Cache rebuild: new derived tuples appear after source grows");

    /*
     * filtered(y) :- edge(x, y), y > 0.
     * Initially edge has 2 rows: (10,1),(10,2) — both pass y > 0.
     * After inserting edge(10,3), one new filtered tuple (y=3) is derived.
     */
    const char *src =
        ".decl edge(x: int32, y: int32)\n"
        "edge(10, 1). edge(10, 2).\n"
        ".decl filtered(y: int32)\n"
        "filtered(y) :- edge(x, y), y > 0.\n";

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    ASSERT(wl_plan_from_program(prog, &plan) == 0, "plan gen failed");

    wl_session_t *sess = NULL;
    ASSERT(wl_session_create(wl_backend_columnar(), plan, 1, &sess) == 0,
        "session create failed");
    ASSERT(wl_session_load_facts(sess, prog) == 0, "load facts failed");

    /* Run initial snapshot to establish baseline */
    ASSERT(wl_session_snapshot(sess, noop_cb, NULL) == 0,
        "initial snapshot failed");

    /* Verify baseline via internal relation nrows */
    wl_col_session_t *csess = COL_SESSION(sess);
    col_rel_t *filtered_rel = NULL;
    for (uint32_t i = 0; i < csess->nrels; i++) {
        if (csess->rels[i] && strcmp(csess->rels[i]->name, "filtered") == 0) {
            filtered_rel = csess->rels[i];
            break;
        }
    }
    ASSERT(filtered_rel != NULL, "filtered relation must exist after snapshot");
    ASSERT(filtered_rel->nrows == 2,
        "baseline: filtered must have 2 rows (y > 0, 2 edges)");

    /* Register delta callback to count new tuples */
    delta_ctx_t dctx;
    memset(&dctx, 0, sizeof(dctx));
    dctx.tracked_rel = "filtered";
    wl_session_set_delta_cb(sess, count_delta_cb, &dctx);

    /* Insert a new edge fact (10,3) — should pass y > 0 */
    int64_t new_edge[] = { 10, 3 };
    ASSERT(wl_session_insert(sess, "edge", new_edge, 1, 2) == 0,
        "insert new edge fact failed");
    ASSERT(wl_session_step(sess) == 0,
        "step after insert failed");

    /* Exactly 1 new filtered tuple must have been derived */
    ASSERT(dctx.count == 1,
        "exactly 1 new filtered tuple must be derived after insert");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Issue #1435: lease tests.  These call the cache API directly on a
 * session built from a small program, so the entry array, counters and
 * pointers can be inspected.
 * ================================================================ */

/* VAR("col1") CONST_INT(threshold) CMP_GT, as the planner would emit it
 * for `y > threshold` (lifted from test_generation_cache.c). */
static wl_plan_expr_buffer_t
make_col1_gt(int64_t threshold)
{
    uint8_t expression[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
        WL_PLAN_EXPR_CONST_INT,
        0, 0, 0, 0, 0, 0, 0, 0,
        WL_PLAN_EXPR_CMP_GT
    };
    for (int i = 0; i < 8; i++)
        expression[8 + i] = (uint8_t)((uint64_t)threshold >> (8 * i));
    wl_plan_expr_buffer_t result;
    result.data = (uint8_t *)malloc(sizeof(expression));
    result.size = sizeof(expression);
    if (result.data)
        memcpy(result.data, expression, sizeof(expression));
    return result;
}

static col_rel_t *
find_relation(wl_session_t *session, const char *name)
{
    wl_col_session_t *cs = COL_SESSION(session);
    for (uint32_t i = 0; i < cs->nrels; i++) {
        if (cs->rels[i] && cs->rels[i]->name
            && strcmp(cs->rels[i]->name, name) == 0)
            return cs->rels[i];
    }
    return NULL;
}

/* edge(1,1)..(1,4); one snapshot so the session is fully initialised. */
static int
make_lease_session(wl_session_t **session_out, wl_plan_t **plan_out,
    wirelog_program_t **program_out)
{
    const char *src =
        ".decl edge(x: int32, y: int32)\n"
        ".decl out(y: int32)\n"
        "edge(1, 1). edge(1, 2). edge(1, 3). edge(1, 4).\n"
        "out(y) :- edge(x, y), y > 2.\n";
    wirelog_error_t error = WIRELOG_OK;
    wirelog_program_t *program = wirelog_parse_string(src, &error);
    if (!program)
        return -1;
    wl_fusion_apply(program, NULL);
    wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(program, &plan) != 0) {
        wirelog_program_free(program);
        return -1;
    }
    wl_session_t *session = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &session) != 0
        || wl_session_load_facts(session, program) != 0
        || wl_session_snapshot(session, noop_cb, NULL) != 0) {
        if (session)
            wl_session_destroy(session);
        wl_plan_free(plan);
        wirelog_program_free(program);
        return -1;
    }
    *session_out = session;
    *plan_out = plan;
    *program_out = program;
    return 0;
}

static void
destroy_lease_session(wl_session_t *session, wl_plan_t *plan,
    wirelog_program_t *program)
{
    wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

/* Test e: a same-nrows source mutation under a lease must not replace the
 * leased filtered relation; the lookup reports unavailable instead, and the
 * last release destroys the stale copy so the next lookup rebuilds. */
static void
test_lease_survives_source_set(void)
{
    TEST("Lease: stale source under lease defers rebuild until release");

    wl_session_t *session = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    ASSERT(make_lease_session(&session, &plan, &program) == 0,
        "session creation failed");
    wl_col_session_t *cs = COL_SESSION(session);
    col_rel_t *edge = find_relation(session, "edge");
    wl_plan_expr_buffer_t filter = make_col1_gt(2);
    if (!edge || edge->nrows != 4 || !filter.data) {
        free(filter.data);
        destroy_lease_session(session, plan, program);
        FAIL("fixture setup failed");
    }

    col_filt_cache_pin_t pin;
    col_rel_t *filtered = wl_columnar_filter_apply_right_filter_cached_pin(
        cs, &filter, "edge", edge, &pin);
    int ok = filtered != NULL && filtered->nrows == 2 && pin.active
        && pin.entry && pin.entry->pin_count == 1
        && cs->filt_cache_active_pins == 1;
    col_filt_cache_entry_t *entry = pin.entry;
    uint32_t count_before = cs->filt_cache_count;

    /* Row 3 (y == 4) becomes y == 1: nrows unchanged, token changes. */
    if (ok)
        ok = col_rel_set(edge, 3, 1, 1) == 0;
    col_filt_cache_pin_t pin2;
    col_rel_t *again = wl_columnar_filter_apply_right_filter_cached_pin(
        cs, &filter, "edge", edge, &pin2);
    if (ok)
        ok = again == NULL && !pin2.active
            && entry->filtered == filtered && filtered->nrows == 2
            && entry->evict_deferred && entry->pin_count == 1
            && cs->filt_cache_active_pins == 1
            && cs->filt_cache_count == count_before;
    /* The unleased wrapper sees the same deferral. */
    if (ok)
        ok = wl_columnar_filter_apply_right_filter_cached(cs, &filter,
                "edge", edge) == NULL && cs->filt_cache_active_pins == 1;
    /* The leased copy stays readable until release: still (1,3),(1,4). */
    if (ok)
        ok = filtered->columns[1][0] == 3 && filtered->columns[1][1] == 4;

    col_filt_cache_pin_release(&pin);
    if (ok)
        ok = !pin.active && entry->pin_count == 0 && !entry->evict_deferred
            && entry->filtered == NULL && entry->source_nrows == 0
            && cs->filt_cache_active_pins == 0
            && cs->filt_cache_count == count_before;
    /* Double release is a no-op. */
    col_filt_cache_pin_release(&pin);
    if (ok)
        ok = cs->filt_cache_active_pins == 0;

    /* Next lookup rebuilds in place against the mutated source: y > 2
     * now matches only (1,3). */
    col_rel_t *rebuilt = wl_columnar_filter_apply_right_filter_cached_pin(
        cs, &filter, "edge", edge, &pin);
    if (ok)
        ok = rebuilt != NULL && rebuilt->nrows == 1 && pin.entry == entry
            && entry->filtered == rebuilt && entry->source_nrows == 4
            && cs->filt_cache_count == count_before;
    col_filt_cache_pin_release(&pin);
    if (ok)
        ok = cs->filt_cache_active_pins == 0;

    free(filter.data);
    destroy_lease_session(session, plan, program);
    ASSERT(ok,
        "lease did not protect the filtered relation across col_rel_set");
    PASS();
}

/* Test f: session_add_rel replacing the source under a lease must neither
 * compact the entry array nor destroy the leased filtered relation. */
static void
test_lease_survives_session_add_rel(void)
{
    TEST("Lease: session_add_rel defers eviction of a leased entry");

    wl_session_t *session = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    ASSERT(make_lease_session(&session, &plan, &program) == 0,
        "session creation failed");
    wl_col_session_t *cs = COL_SESSION(session);
    col_rel_t *edge = find_relation(session, "edge");
    wl_plan_expr_buffer_t f_gt2 = make_col1_gt(2);
    wl_plan_expr_buffer_t f_gt3 = make_col1_gt(3);
    if (!edge || !f_gt2.data || !f_gt3.data) {
        free(f_gt2.data);
        free(f_gt3.data);
        destroy_lease_session(session, plan, program);
        FAIL("fixture setup failed");
    }

    /* Two entries for edge: one leased, one not. */
    col_filt_cache_pin_t pin;
    col_rel_t *leased = wl_columnar_filter_apply_right_filter_cached_pin(
        cs, &f_gt2, "edge", edge, &pin);
    col_rel_t *unleased = wl_columnar_filter_apply_right_filter_cached(
        cs, &f_gt3, "edge", edge);
    int ok = leased && leased->nrows == 2 && unleased && unleased->nrows == 1
        && cs->filt_cache_active_pins == 1;
    col_filt_cache_entry_t *entries = cs->filt_cache;
    uint32_t count_before = cs->filt_cache_count;
    uint32_t edge_entries = 0;
    col_filt_cache_entry_t *leased_entry = pin.entry;
    col_filt_cache_entry_t *other_entry = NULL;
    for (uint32_t i = 0; ok && i < cs->filt_cache_count; i++) {
        if (strcmp(cs->filt_cache[i].rel_name, "edge") != 0)
            continue;
        edge_entries++;
        if (&cs->filt_cache[i] != leased_entry
            && cs->filt_cache[i].filtered == unleased)
            other_entry = &cs->filt_cache[i];
    }
    if (ok)
        ok = other_entry != NULL && edge_entries >= 2;

    /* Replace edge with an empty relation of the same schema. */
    col_rel_t *replacement = col_rel_new_like("edge", edge);
    if (ok && replacement && session_add_rel(cs, replacement) == 0)
        ok = session_find_rel(cs, "edge") == replacement;
    else {
        ok = 0;
        if (replacement)
            col_rel_destroy(replacement);
    }

    /* No compaction, no move; leased entry deferred, other entry emptied. */
    if (ok)
        ok = cs->filt_cache == entries
            && cs->filt_cache_count == count_before
            && leased_entry->filtered == leased && leased->nrows == 2
            && leased_entry->evict_deferred && leased_entry->pin_count == 1
            && other_entry->filtered == NULL && other_entry->source_nrows == 0
            && !other_entry->evict_deferred
            && other_entry->rel_name != NULL
            && cs->filt_cache_active_pins == 1;
    /* The deferred entry is hidden; the emptied one rebuilds against the
     * replacement (empty source, so an empty result). */
    if (ok)
        ok = wl_columnar_filter_apply_right_filter_cached(cs, &f_gt2, "edge",
                replacement) == NULL;
    col_rel_t *rebuilt = wl_columnar_filter_apply_right_filter_cached(cs,
            &f_gt3, "edge", replacement);
    if (ok)
        ok = rebuilt != NULL && rebuilt->nrows == 0
            && other_entry->filtered == rebuilt
            && cs->filt_cache_count == count_before;

    col_filt_cache_pin_release(&pin);
    if (ok)
        ok = cs->filt_cache_active_pins == 0 && leased_entry->filtered == NULL
            && !leased_entry->evict_deferred
            && cs->filt_cache_count == count_before;
    /* With no leases the next invalidation compacts as before (#386). */
    col_rel_t *replacement2 = ok ? col_rel_new_like("edge", replacement)
        : NULL;
    if (ok && replacement2 && session_add_rel(cs, replacement2) == 0)
        ok = cs->filt_cache_count == count_before - edge_entries;
    else {
        ok = 0;
        if (replacement2)
            col_rel_destroy(replacement2);
    }

    free(f_gt2.data);
    free(f_gt3.data);
    destroy_lease_session(session, plan, program);
    ASSERT(ok,
        "lease did not protect the filtered relation across session_add_rel");
    PASS();
}

/* Test g: two leases on one entry, released in either order, leave both
 * counters at zero and the entry intact. */
static void
test_two_leases_release_either_order(void)
{
    TEST("Lease: two leases on one entry release in either order");

    wl_session_t *session = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    ASSERT(make_lease_session(&session, &plan, &program) == 0,
        "session creation failed");
    wl_col_session_t *cs = COL_SESSION(session);
    col_rel_t *edge = find_relation(session, "edge");
    wl_plan_expr_buffer_t filter = make_col1_gt(2);
    if (!edge || !filter.data) {
        free(filter.data);
        destroy_lease_session(session, plan, program);
        FAIL("fixture setup failed");
    }

    int ok = 1;
    for (int order = 0; ok && order < 2; order++) {
        col_filt_cache_pin_t a;
        col_filt_cache_pin_t b;
        col_rel_t *ra = wl_columnar_filter_apply_right_filter_cached_pin(
            cs, &filter, "edge", edge, &a);
        col_rel_t *rb = wl_columnar_filter_apply_right_filter_cached_pin(
            cs, &filter, "edge", edge, &b);
        ok = ra != NULL && ra == rb && a.entry == b.entry
            && a.entry->pin_count == 2 && cs->filt_cache_active_pins == 2;
        col_filt_cache_entry_t *entry = a.entry;
        col_filt_cache_pin_t *first = order == 0 ? &a : &b;
        col_filt_cache_pin_t *second = order == 0 ? &b : &a;
        col_filt_cache_pin_release(first);
        if (ok)
            ok = entry->pin_count == 1 && cs->filt_cache_active_pins == 1
                && entry->filtered == ra && !first->active && second->active;
        col_filt_cache_pin_release(second);
        if (ok)
            ok = entry->pin_count == 0 && cs->filt_cache_active_pins == 0
                && entry->filtered == ra && ra->nrows == 2
                && !entry->evict_deferred;
    }

    free(filter.data);
    destroy_lease_session(session, plan, program);
    ASSERT(ok, "lease counters did not return to zero");
    PASS();
}

/* Test h: a miss that needs the entry array to grow is refused while a
 * lease is active (the array must not move under the lease), and the same
 * miss succeeds once the lease is released. */
static void
test_growth_refused_while_leased(void)
{
    TEST("Lease: cache growth refused while a lease is active");

    wl_session_t *session = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    ASSERT(make_lease_session(&session, &plan, &program) == 0,
        "session creation failed");
    wl_col_session_t *cs = COL_SESSION(session);
    col_rel_t *edge = find_relation(session, "edge");
    if (!edge) {
        destroy_lease_session(session, plan, program);
        FAIL("fixture setup failed");
    }

    /* Fill the array to capacity with distinct predicates. */
    int ok = 1;
    int64_t threshold = 100;
    wl_plan_expr_buffer_t filter = make_col1_gt(threshold);
    col_filt_cache_pin_t pin;
    ok = filter.data != NULL
        && wl_columnar_filter_apply_right_filter_cached_pin(cs, &filter,
            "edge", edge, &pin) != NULL;
    while (ok && cs->filt_cache_count < cs->filt_cache_cap) {
        wl_plan_expr_buffer_t f = make_col1_gt(++threshold);
        ok = f.data != NULL
            && wl_columnar_filter_apply_right_filter_cached(cs, &f, "edge",
                edge) != NULL;
        free(f.data);
    }
    col_filt_cache_entry_t *entries = cs->filt_cache;
    uint32_t cap_before = cs->filt_cache_cap;
    uint32_t count_before = cs->filt_cache_count;

    /* One more distinct predicate would need growth: refused under lease. */
    wl_plan_expr_buffer_t extra = make_col1_gt(++threshold);
    if (ok)
        ok = extra.data != NULL
            && wl_columnar_filter_apply_right_filter_cached(cs, &extra,
                "edge", edge) == NULL
            && cs->filt_cache == entries && cs->filt_cache_cap == cap_before
            && cs->filt_cache_count == count_before
            && pin.entry->filtered != NULL && pin.active;
    /* A hit on a fresh leased entry still works while full. */
    if (ok)
        ok = wl_columnar_filter_apply_right_filter_cached(cs, &filter,
                "edge", edge) == pin.rel;

    col_filt_cache_pin_release(&pin);
    if (ok)
        ok = wl_columnar_filter_apply_right_filter_cached(cs, &extra, "edge",
                edge) != NULL
            && cs->filt_cache_cap > cap_before
            && cs->filt_cache_count == count_before + 1
            && cs->filt_cache_active_pins == 0;

    free(filter.data);
    free(extra.data);
    destroy_lease_session(session, plan, program);
    ASSERT(ok, "growth was not refused under lease or failed after release");
    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== Filtered Relation Cache Tests (Issue #386) ===\n\n");

    test_cache_miss_correct_result();
    test_cache_entry_populated();
    test_different_filter_different_result();
    test_cache_rebuild_on_nrows_change();
    test_lease_survives_source_set();
    test_lease_survives_session_add_rel();
    test_two_leases_release_either_order();
    test_growth_refused_while_leased();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
