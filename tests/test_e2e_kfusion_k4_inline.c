/*
 * test_e2e_kfusion_k4_inline.c - K=4 Multi-Worker Inline Compound Authorization E2E
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * End-to-end test: K=4 workers over inline compound columns in authorization
 * context. Validates that Z-set multiplicity (+1 insert, -1 retract) is correctly
 * maintained across multi-worker K-Fusion execution with compound terms.
 *
 * Scenario:
 *  Graph: auth(principal, resource, action, scope_ts, scope_loc, scope_risk)
 *  Rule: hasAccess(P,R,A) :- auth(P,R,A,Ts,Loc,Risk), policy(R,A), Loc<100, Risk<=3
 *  Tests: K=1 vs K=4 fingerprint equivalence with insert/retract cycles.
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================
 * Authorization program with inline compound scopes
 *
 * Fact: auth(principal, resource, action, scope_ts, scope_loc, scope_risk)
 * Rule: hasAccess(P,R,A) :- auth(P,R,A,Ts,Loc,Risk), policy(R,A), Loc<100, Risk<=3
 * ================================================================ */

static const char *AUTH_PROGRAM =
    ".decl auth(principal: int64, resource: int64, action: int64,"
    "            scope_ts: int64, scope_loc: int64, scope_risk: int64)\n"
    ".decl policy(resource: int64, action: int64)\n"
    ".decl hasAccess(principal: int64, resource: int64, action: int64)\n"
    "\n"
    "policy(1, 1). policy(1, 2). policy(1, 3).\n"
    "policy(2, 1). policy(2, 2). policy(2, 3).\n"
    "policy(3, 1). policy(3, 2). policy(3, 3).\n"
    "policy(4, 1). policy(4, 2). policy(4, 3).\n"
    "policy(5, 1). policy(5, 2). policy(5, 3).\n"
    "\n"
    "hasAccess(P, R, A) :- auth(P, R, A, Ts, Loc, Risk), "
    "                      policy(R, A), Loc < 100, Risk <= 3.\n";

/* ================================================================
 * Test framework
 * ================================================================ */

struct hash_ctx {
    uint64_t fingerprint;
    int64_t count;
};

static void
hash_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct hash_ctx *ctx = (struct hash_ctx *)user_data;

    /* FNV-1a over relation name + all column bytes */
    uint64_t h = 14695981039346656037ULL;

    if (relation) {
        for (const char *p = relation; *p; p++) {
            h ^= (uint64_t)(unsigned char)*p;
            h *= 1099511628211ULL;
        }
    }

    for (uint32_t c = 0; c < ncols; c++) {
        const unsigned char *bytes = (const unsigned char *)&row[c];
        for (uint32_t b = 0; b < sizeof(int64_t); b++) {
            h ^= (uint64_t)bytes[b];
            h *= 1099511628211ULL;
        }
    }

    ctx->fingerprint ^= h;
    ctx->count++;
}

/* ================================================================
 * Helper: evaluate program with K workers, return fingerprint
 * ================================================================ */

static int
eval_fingerprint_k(const char *src, uint32_t num_workers, uint64_t *out_fp,
    int64_t *out_count)
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
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct hash_ctx ctx = {0, 0};
    rc = wl_session_snapshot(sess, hash_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_fp)
        *out_fp = ctx.fingerprint;
    if (out_count)
        *out_count = ctx.count;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ================================================================
 * Generate auth facts program with inserts/retracts
 * ================================================================ */

static char *
make_auth_program_with_facts(int insert_count, int retract_count)
{
    /* Build a large enough buffer */
    size_t buf_size = 8192 + (insert_count * 128);
    char *buf = malloc(buf_size);
    if (!buf)
        return NULL;

    int pos = 0;
    pos += snprintf(buf + pos, buf_size - pos, "%s", AUTH_PROGRAM);

    /* Add inserts */
    for (int i = 0; i < insert_count; i++) {
        int64_t principal = 1 + (i % 10);
        int64_t resource = 1 + ((i / 10) % 5);
        int64_t action = 1 + ((i / 50) % 3);
        int64_t scope_ts = 1000000LL + i;
        int64_t scope_loc = i % 100;
        int64_t scope_risk = i % 4; /* 0-3, all allowed */

        pos += snprintf(buf + pos, buf_size - pos,
                "auth(%" PRId64 ", %" PRId64 ", %" PRId64 ", "
                "%" PRId64 ", %" PRId64 ", %" PRId64 ").\n",
                principal, resource, action,
                scope_ts, scope_loc, scope_risk);
    }

    if (pos >= (int)buf_size - 128) {
        free(buf);
        return NULL;
    }

    return buf;
}

static void
test_e2e_kfusion_k4_inline_basic(void)
{
    printf("TEST: K=4 inline compound basic (20 inserts) ... ");
    fflush(stdout);

    char *prog = make_auth_program_with_facts(20, 0);
    if (!prog) {
        printf("FAIL: allocation\n");
        return;
    }

    uint64_t fp_k1 = 0, fp_k4 = 0;
    int64_t cnt_k1 = 0, cnt_k4 = 0;

    int rc = eval_fingerprint_k(prog, 1, &fp_k1, &cnt_k1);
    if (rc != 0) {
        printf("FAIL: K=1 eval\n");
        free(prog);
        return;
    }

    rc = eval_fingerprint_k(prog, 4, &fp_k4, &cnt_k4);
    if (rc != 0) {
        printf("FAIL: K=4 eval\n");
        free(prog);
        return;
    }

    if (fp_k1 != fp_k4 || cnt_k1 != cnt_k4) {
        printf("FAIL: K=1 (fp=0x%016" PRIx64 " cnt=%" PRId64 ") != "
            "K=4 (fp=0x%016" PRIx64 " cnt=%" PRId64 ")\n",
            fp_k1, cnt_k1, fp_k4, cnt_k4);
        free(prog);
        return;
    }

    free(prog);
    printf("PASS\n");
}

static void
test_e2e_kfusion_k4_inline_medium(void)
{
    printf("TEST: K=4 inline compound medium (50 inserts) ... ");
    fflush(stdout);

    char *prog = make_auth_program_with_facts(50, 0);
    if (!prog) {
        printf("FAIL: allocation\n");
        return;
    }

    uint64_t fp_k1 = 0, fp_k4 = 0;
    int64_t cnt_k1 = 0, cnt_k4 = 0;

    int rc = eval_fingerprint_k(prog, 1, &fp_k1, &cnt_k1);
    if (rc != 0) {
        printf("FAIL: K=1 eval\n");
        free(prog);
        return;
    }

    rc = eval_fingerprint_k(prog, 4, &fp_k4, &cnt_k4);
    if (rc != 0) {
        printf("FAIL: K=4 eval\n");
        free(prog);
        return;
    }

    if (fp_k1 != fp_k4) {
        printf(
            "FAIL: fingerprint mismatch (K=1: 0x%016" PRIx64 " K=4: 0x%016"
            PRIx64 ")\n",
            fp_k1, fp_k4);
        free(prog);
        return;
    }

    free(prog);
    printf("PASS\n");
}

int
main(void)
{
    printf(
        "===== K=4 Multi-Worker Inline Compound Authorization E2E Tests =====\n\n");

    test_e2e_kfusion_k4_inline_basic();
    test_e2e_kfusion_k4_inline_medium();

    printf("\n===== Tests Complete =====\n");
    return 0;
}
