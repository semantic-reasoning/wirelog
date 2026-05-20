/*
 * tests/test_recursive_agg_conformance.c - Recursive aggregation conformance
 *
 * Covers issue #692: recursive MIN/MAX aggregate materialization must not
 * retain dominated aggregate rows after fixed-point evaluation.
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *relation;
    uint32_t ncols;
    int64_t rows[64][4];
    uint32_t count;
} row_set_t;

static void
collect_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    row_set_t *set = (row_set_t *)user_data;
    if (!relation || strcmp(relation, set->relation) != 0)
        return;
    if (ncols > 4 || set->count >= 64)
        abort();
    set->ncols = ncols;
    for (uint32_t c = 0; c < ncols; c++)
        set->rows[set->count][c] = row[c];
    set->count++;
}

static int
row_set_contains(const row_set_t *set, const int64_t *row, uint32_t ncols)
{
    for (uint32_t r = 0; r < set->count; r++) {
        int match = 1;
        for (uint32_t c = 0; c < ncols; c++) {
            if (set->rows[r][c] != row[c]) {
                match = 0;
                break;
            }
        }
        if (match)
            return 1;
    }
    return 0;
}

static int
run_program(const char *src, const char *relation, uint32_t workers,
    row_set_t *out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return rc;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, workers, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return rc;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc == 0) {
        memset(out, 0, sizeof(*out));
        out->relation = relation;
        rc = wl_session_snapshot(sess, collect_rows, out);
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

static int
expect_rows(const row_set_t *set, const int64_t expected[][2],
    uint32_t expected_count)
{
    if (set->count != expected_count)
        return 0;
    if (set->ncols != 2)
        return 0;
    for (uint32_t i = 0; i < expected_count; i++) {
        if (!row_set_contains(set, expected[i], 2))
            return 0;
    }
    return 1;
}

static int
test_cc_min(uint32_t workers)
{
    static const char *src =
        ".decl Edge(x: int32, y: int32)\n"
        ".decl Label(x: int32, l: int32)\n"
        "Edge(1,2). Edge(2,1). Edge(2,3). Edge(3,2).\n"
        "Edge(4,5). Edge(5,4).\n"
        "Label(x, min(x)) :- Edge(x, y).\n"
        "Label(y, min(y)) :- Edge(x, y).\n"
        "Label(x, min(l)) :- Label(y, l), Edge(y, x).\n";
    static const int64_t expected[][2] = {
        {1, 1}, {2, 1}, {3, 1}, {4, 4}, {5, 4},
    };
    row_set_t rows;
    return run_program(src, "Label", workers, &rows) == 0
           && expect_rows(&rows, expected, 5);
}

static int
test_sssp_max(uint32_t workers)
{
    static const char *src =
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl Dist(x: int32, d: int32)\n"
        "Edge(1,2,5). Edge(1,3,1). Edge(2,3,7).\n"
        "Dist(1, max(0)) :- Edge(1, y, w).\n"
        "Dist(y, max(d + w)) :- Dist(x, d), Edge(x, y, w).\n";
    static const int64_t expected[][2] = {
        {1, 0}, {2, 5}, {3, 12},
    };
    row_set_t rows;
    return run_program(src, "Dist", workers, &rows) == 0
           && expect_rows(&rows, expected, 3);
}

static int
test_count_stratified(uint32_t workers)
{
    static const char *src =
        ".decl Edge(x: int32, y: int32)\n"
        ".decl EdgeCount(x: int32, c: int32)\n"
        "Edge(1,2). Edge(1,3). Edge(1,4). Edge(2,3). Edge(2,4). Edge(3,4).\n"
        "EdgeCount(x, count(y)) :- Edge(x, y).\n";
    static const int64_t expected[][2] = {
        {1, 3}, {2, 2}, {3, 1},
    };
    row_set_t rows;
    return run_program(src, "EdgeCount", workers, &rows) == 0
           && expect_rows(&rows, expected, 3);
}

int
main(void)
{
    static const uint32_t worker_counts[] = {1, 4, 8, 16};
    int failures = 0;

    for (uint32_t i = 0; i < sizeof(worker_counts) / sizeof(worker_counts[0]);
        i++) {
        uint32_t workers = worker_counts[i];
        if (!test_cc_min(workers)) {
            printf("FAIL cc-min workers=%u\n", workers);
            failures++;
        }
        if (!test_sssp_max(workers)) {
            printf("FAIL sssp-max workers=%u\n", workers);
            failures++;
        }
        if (!test_count_stratified(workers)) {
            printf("FAIL count-stratified workers=%u\n", workers);
            failures++;
        }
    }

    if (failures == 0) {
        printf("PASS recursive aggregation conformance\n");
        return 0;
    }
    return 1;
}
