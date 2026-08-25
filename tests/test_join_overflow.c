#define _GNU_SOURCE

/*
 * test_join_overflow.c - join overflow failure semantics
 *
 * Verifies that join output limit overflow fails closed instead of publishing
 * partial join results.
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef _WIN32
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, (value && *value) ? value : "1");
}

static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}

#  define setenv   wl_test_setenv_
#  define unsetenv wl_test_unsetenv_
#endif

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
            return 1;                         \
        } while (0)

static void
noop_tuple_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user_data;
}

static int
create_session_from_source(const char *src, uint32_t workers,
    wirelog_program_t **out_prog, wl_plan_t **out_plan, wl_session_t **out_sess)
{
    *out_prog = NULL;
    *out_plan = NULL;
    *out_sess = NULL;

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

    *out_prog = prog;
    *out_plan = plan;
    *out_sess = sess;
    return 0;
}

static int
run_snapshot_with_limit(const char *src, uint32_t workers, const char *limit,
    bool diff_enabled)
{
    setenv("WIRELOG_JOIN_OUTPUT_LIMIT", limit, 1);

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;
    int rc = create_session_from_source(src, workers, &prog, &plan, &sess);
    if (rc == 0)
        ((wl_col_session_t *)sess)->diff_enabled = diff_enabled;
    if (rc == 0)
        rc = wl_session_load_facts(sess, prog);
    if (rc == 0)
        rc = wl_session_snapshot(sess, noop_tuple_cb, NULL);

    if (sess)
        wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
    return rc;
}

static int
run_incremental_snapshot_with_limit(const char *src, const char *limit,
    const char *relation, const int64_t *rows, uint32_t num_rows,
    uint32_t num_cols)
{
    setenv("WIRELOG_JOIN_OUTPUT_LIMIT", limit, 1);

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;
    int rc = create_session_from_source(src, 1, &prog, &plan, &sess);
    if (rc == 0)
        ((wl_col_session_t *)sess)->diff_enabled = true;
    if (rc == 0)
        rc = wl_session_snapshot(sess, noop_tuple_cb, NULL);
    if (rc == 0)
        rc = col_session_insert_incremental(sess, relation, rows, num_rows,
                num_cols);
    if (rc == 0)
        rc = wl_session_snapshot(sess, noop_tuple_cb, NULL);

    if (sess)
        wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
    return rc;
}

static int
test_keyed_join_overflow(void)
{
    TEST("keyed join overflow returns EOVERFLOW");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(x: int32, z: int32)\n"
        ".decl r(x: int32, z: int32)\n"
        "a(1, 100). a(1, 101).\n"
        "b(1, 10). b(1, 11).\n"
        "r(x, z) :- a(x, _), b(x, z).\n";

    int rc = run_snapshot_with_limit(src, 1, "1", false);
    if (rc != EOVERFLOW) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected EOVERFLOW, got %d", rc);
        FAIL(msg);
    }
    PASS();
    return 0;
}

static int
test_filter_next_pow2_overflow_contract(void)
{
    TEST("filter next_pow2 returns zero only on overflow");

    if (wl_columnar_filter_next_pow2(15) != 16
        || wl_columnar_filter_next_pow2(16) != 16
        || wl_columnar_filter_next_pow2(0x80000000u) != 0x80000000u
        || wl_columnar_filter_next_pow2(0x80000001u) != 0
        || wl_columnar_filter_next_pow2(UINT32_MAX) != 0) {
        FAIL("unexpected next_pow2 boundary result");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_unary_join_overflow(void)
{
    TEST("unary join overflow returns EOVERFLOW");

    const char *src = ".decl a(x: int32)\n"
        ".decl b(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "a(1).\n"
        "b(1, 10). b(1, 11).\n"
        "r(x, y) :- b(x, y), a(x).\n";

    int rc = run_snapshot_with_limit(src, 1, "1", false);
    if (rc != EOVERFLOW) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected EOVERFLOW, got %d", rc);
        FAIL(msg);
    }
    PASS();
    return 0;
}

static int
test_diff_join_overflow(void)
{
    TEST("differential join overflow returns EOVERFLOW");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";
    int64_t rows[] = { 1, 2, 2, 3, 3, 4 };

    int rc = run_incremental_snapshot_with_limit(src, "1", "edge", rows, 3, 2);
    if (rc != EOVERFLOW) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected EOVERFLOW, got %d", rc);
        FAIL(msg);
    }
    PASS();
    return 0;
}

static int
test_k_fusion_worker_limit_overflow(void)
{
    TEST("K-fusion worker limit overflow returns EOVERFLOW");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 1). r(1, 2). r(2, 1). r(2, 2).\n"
        "r(a, e) :- r(a, b), r(b, c), r(c, d), r(d, e).\n";

    int rc = run_snapshot_with_limit(src, 8, "64", false);
    if (rc != EOVERFLOW) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected EOVERFLOW, got %d", rc);
        FAIL(msg);
    }
    PASS();
    return 0;
}

int
main(void)
{
    printf("=== test_join_overflow ===\n");

    test_keyed_join_overflow();
    test_filter_next_pow2_overflow_contract();
    test_unary_join_overflow();
    test_diff_join_overflow();
    test_k_fusion_worker_limit_overflow();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
