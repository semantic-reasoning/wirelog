/*
 * bench_incremental_frontier.c - Performance benchmark for Phase 4 frontier persistence
 *
 * Measures the actual time saved by incremental re-evaluation when frontier
 * persists across session_step calls. Compares:
 *   - Full re-evaluation (baseline): creates new session, inserts all facts, evaluates
 *   - Incremental evaluation: inserts initial facts, evaluates, adds new facts
 *     without resetting, evaluates again
 *
 * Expected result: Incremental should be significantly faster than full for large graphs.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _POSIX_C_SOURCE 200112L

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _MSC_VER
#include <time.h>
#else
#include <windows.h>
#endif

#include "../wirelog/backend/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

static uint64_t
now_ns(void)
{
#ifndef _MSC_VER
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
#else
    /* MSVC: Use GetTickCount64 (returns ms since system start) */
    return (uint64_t)GetTickCount64() * 1000000ULL;
#endif
}

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
    wirelog_program_free(prog);
    if (rc != 0)
        return NULL;
    return plan;
}

/* Generate a chain graph: 1->2->3->...->N */
static void
generate_chain(int64_t *edges, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) {
        edges[i * 2] = (int64_t)(i + 1);
        edges[i * 2 + 1] = (int64_t)(i + 2);
    }
}

int
main(void)
{
    printf("\n=== Phase 4 Frontier Persistence Performance Benchmark ===\n\n");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl path(x: int32, y: int32)\n"
                      "path(x, y) :- edge(x, y).\n"
                      "path(x, z) :- path(x, y), edge(y, z).\n";

    wl_plan_t *plan = build_plan(src);
    if (!plan) {
        fprintf(stderr, "Failed to build plan\n");
        return 1;
    }

    /* Benchmark parameters */
    uint32_t initial_edges = 100;
    uint32_t batch_size = 50;
    uint32_t num_batches = 5;

    printf("Configuration:\n");
    printf("  Initial edges: %u\n", initial_edges);
    printf("  Batch size: %u\n", batch_size);
    printf("  Number of batches: %u\n\n", num_batches);

    /* ===== FULL RE-EVALUATION (Baseline) ===== */
    printf("BASELINE: Full re-evaluation (all facts at once)\n");
    uint64_t full_start = now_ns();

    wl_session_t *full_sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &full_sess);
    if (rc != 0 || !full_sess) {
        fprintf(stderr, "Failed to create full session\n");
        wl_plan_free(plan);
        return 1;
    }

    uint32_t total_edges = initial_edges + (batch_size * num_batches);
    int64_t *all_edges = (int64_t *)malloc(sizeof(int64_t) * total_edges * 2);
    if (!all_edges) {
        fprintf(stderr, "Allocation failed\n");
        wl_session_destroy(full_sess);
        wl_plan_free(plan);
        return 1;
    }
    generate_chain(all_edges, total_edges);

    rc = wl_session_insert(full_sess, "edge", all_edges, total_edges, 2);
    if (rc != 0) {
        fprintf(stderr, "Insert failed: %d\n", rc);
        free(all_edges);
        wl_session_destroy(full_sess);
        wl_plan_free(plan);
        return 1;
    }

    rc = wl_session_step(full_sess);
    if (rc != 0) {
        fprintf(stderr, "Session step failed: %d\n", rc);
        free(all_edges);
        wl_session_destroy(full_sess);
        wl_plan_free(plan);
        return 1;
    }

    uint64_t full_end = now_ns();
    uint64_t full_time_ns = full_end - full_start;

    wl_session_destroy(full_sess);
    printf("  Time: %.2f ms\n\n", (double)full_time_ns / 1000000.0);

    /* ===== INCREMENTAL EVALUATION ===== */
    printf("INCREMENTAL: Batched fact insertion with frontier persistence\n");
    uint64_t incr_start = now_ns();

    wl_session_t *incr_sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &incr_sess);
    if (rc != 0 || !incr_sess) {
        fprintf(stderr, "Failed to create incremental session\n");
        free(all_edges);
        wl_plan_free(plan);
        return 1;
    }

    /* Insert initial facts */
    rc = wl_session_insert(incr_sess, "edge", all_edges, initial_edges, 2);
    if (rc != 0) {
        fprintf(stderr, "Initial insert failed: %d\n", rc);
        free(all_edges);
        wl_session_destroy(incr_sess);
        wl_plan_free(plan);
        return 1;
    }

    rc = wl_session_step(incr_sess);
    if (rc != 0) {
        fprintf(stderr, "Initial step failed: %d\n", rc);
        free(all_edges);
        wl_session_destroy(incr_sess);
        wl_plan_free(plan);
        return 1;
    }

    /* Incremental batches */
    for (uint32_t b = 0; b < num_batches; b++) {
        uint32_t offset = initial_edges + (b * batch_size);
        rc = col_session_insert_incremental(
            incr_sess, "edge", all_edges + offset * 2, batch_size, 2);
        if (rc != 0) {
            fprintf(stderr, "Incremental insert batch %u failed: %d\n", b, rc);
            free(all_edges);
            wl_session_destroy(incr_sess);
            wl_plan_free(plan);
            return 1;
        }

        rc = wl_session_step(incr_sess);
        if (rc != 0) {
            fprintf(stderr, "Step batch %u failed: %d\n", b, rc);
            free(all_edges);
            wl_session_destroy(incr_sess);
            wl_plan_free(plan);
            return 1;
        }
    }

    uint64_t incr_end = now_ns();
    uint64_t incr_time_ns = incr_end - incr_start;

    wl_session_destroy(incr_sess);
    printf("  Time: %.2f ms\n\n", (double)incr_time_ns / 1000000.0);

    /* Results */
    printf("RESULTS:\n");
    printf("  Baseline (full eval):      %.2f ms\n",
           (double)full_time_ns / 1000000.0);
    printf("  Incremental (batched):     %.2f ms\n",
           (double)incr_time_ns / 1000000.0);

    double speedup = (double)full_time_ns / (double)incr_time_ns;
    printf("  Speedup:                   %.2fx\n\n", speedup);

    if (incr_time_ns < full_time_ns) {
        printf("✓ PASS: Incremental evaluation is faster\n");
    } else {
        printf("✗ FAIL: Incremental should be faster than full\n");
    }

    free(all_edges);
    wl_plan_free(plan);
    return incr_time_ns < full_time_ns ? 0 : 1;
}
