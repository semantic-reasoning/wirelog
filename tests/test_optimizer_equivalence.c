/*
 * test_optimizer_equivalence.c - Optimizer pass equivalence matrix (#700).
 *
 * The matrix treats Subsumption as canonicalization and toggles the four
 * optimizer axes documented in docs/SEMANTICS.md: Magic Sets, SIP, Logic
 * Fusion, and JPP.
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/ir/stratify.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/magic_sets.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/passes/subsumption.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define MAX_ROWS 128
#define MAX_COLS 4

typedef struct {
    const char *relation;
    uint32_t ncols;
    int64_t rows[MAX_ROWS][MAX_COLS];
    uint32_t count;
    bool overflow;
} row_set_t;

typedef struct {
    bool magic_sets;
    bool sip;
    bool fusion;
    bool jpp;
} opt_flags_t;

typedef struct {
    bool expect_fusion_activity;
    bool expect_sip_activity;
    bool expect_jpp_activity;
    bool expect_magic_active;
    bool expect_magic_noop;
} matrix_expectation_t;

static void
collect_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    row_set_t *set = (row_set_t *)user_data;
    if (!relation || strcmp(relation, set->relation) != 0)
        return;
    if (ncols > MAX_COLS || set->count >= MAX_ROWS) {
        set->overflow = true;
        return;
    }
    set->ncols = ncols;
    for (uint32_t i = 0; i < ncols; i++)
        set->rows[set->count][i] = row[i];
    set->count++;
}

static bool
row_sets_equal(const row_set_t *a, const row_set_t *b)
{
    if (a->overflow || b->overflow || a->count != b->count
        || a->ncols != b->ncols)
        return false;

    bool matched[MAX_ROWS] = { false };
    for (uint32_t a_row = 0; a_row < a->count; a_row++) {
        bool found = false;
        for (uint32_t b_row = 0; b_row < b->count; b_row++) {
            if (matched[b_row])
                continue;
            bool match = true;
            for (uint32_t c = 0; c < a->ncols; c++) {
                if (a->rows[a_row][c] != b->rows[b_row][c]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                matched[b_row] = true;
                found = true;
                break;
            }
        }
        if (!found)
            return false;
    }
    return true;
}

static bool
apply_optimizer_matrix_with_stats(wirelog_program_t *prog, opt_flags_t flags,
    wl_fusion_stats_t *fusion, wl_sip_stats_t *sip, wl_jpp_stats_t *jpp,
    wl_magic_sets_stats_t *magic, bool *magic_applied)
{
    if (fusion)
        memset(fusion, 0, sizeof(*fusion));
    if (sip)
        memset(sip, 0, sizeof(*sip));
    if (jpp)
        memset(jpp, 0, sizeof(*jpp));
    if (magic)
        memset(magic, 0, sizeof(*magic));
    if (magic_applied)
        *magic_applied = false;

    if (wl_subsumption_apply(prog, NULL) != 0)
        return false;
    if (flags.fusion && wl_fusion_apply(prog, fusion) != 0)
        return false;
    if (flags.jpp && wl_jpp_apply(prog, jpp) != 0)
        return false;
    if (flags.sip && wl_sip_apply(prog, sip) != 0)
        return false;
    if (flags.magic_sets) {
        int rc;
        if (prog->demand_count > 0) {
            rc = wl_magic_sets_apply_with_demands(prog, prog->demands,
                    prog->demand_count, magic);
        } else {
            rc = wl_magic_sets_apply(prog, magic);
        }
        if (rc != 0)
            return false;
        if (magic_applied)
            *magic_applied = prog->magic_sets_applied;
        if (prog->magic_sets_applied) {
            if (wl_ir_program_rebuild_relation_irs(prog) != 0)
                return false;
            wl_ir_program_free_strata(prog);
            if (wl_ir_stratify_program(prog) != 0)
                return false;
        }
    }
    return true;
}

static bool
apply_optimizer_matrix(wirelog_program_t *prog, opt_flags_t flags)
{
    return apply_optimizer_matrix_with_stats(prog, flags, NULL, NULL, NULL,
               NULL,
               NULL);
}

static bool
run_program(const char *src, const char *relation, opt_flags_t flags,
    row_set_t *out)
{
    memset(out, 0, sizeof(*out));
    out->relation = relation;

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        return false;
    }
    if (!apply_optimizer_matrix(prog, flags)) {
        wirelog_program_free(prog);
        return false;
    }

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return false;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc == 0)
        rc = wl_session_load_facts(session, prog);
    if (rc == 0)
        rc = wl_session_snapshot(session, collect_rows, out);

    if (session)
        wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc == 0 && !out->overflow;
}

static bool
run_program_with_stats(const char *src, const char *relation, opt_flags_t flags,
    row_set_t *out, wl_fusion_stats_t *fusion, wl_sip_stats_t *sip,
    wl_jpp_stats_t *jpp, wl_magic_sets_stats_t *magic,
    bool *magic_applied)
{
    memset(out, 0, sizeof(*out));
    out->relation = relation;

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        return false;
    }

    if (!apply_optimizer_matrix_with_stats(prog, flags, fusion, sip, jpp, magic,
        magic_applied)) {
        wirelog_program_free(prog);
        return false;
    }

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return false;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc == 0)
        rc = wl_session_load_facts(session, prog);
    if (rc == 0)
        rc = wl_session_snapshot(session, collect_rows, out);
    if (rc != 0) {
        wirelog_program_free(prog);
        if (session)
            wl_session_destroy(session);
        wl_plan_free(plan);
        return false;
    }

    if (session)
        wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc == 0 && !out->overflow;
}

static int
check_matrix_case(const char *name, const char *src, const char *relation,
    matrix_expectation_t expect)
{
    opt_flags_t baseline_flags = {
        .magic_sets = true,
        .sip = true,
        .fusion = true,
        .jpp = true,
    };
    row_set_t baseline;
    wl_fusion_stats_t fusion = { 0 };
    wl_sip_stats_t sip = { 0 };
    wl_jpp_stats_t jpp = { 0 };
    wl_magic_sets_stats_t magic = { 0 };
    bool magic_applied = false;

    if (!run_program_with_stats(src, relation, baseline_flags, &baseline,
        &fusion,
        &sip, &jpp, &magic, &magic_applied)) {
        printf("%s baseline ... FAIL\n", name);
        return 1;
    }

    if (expect.expect_fusion_activity && fusion.fusions_applied == 0) {
        printf("%s baseline ... FAIL (fusion not active)\n", name);
        return 1;
    }
    if (expect.expect_sip_activity && sip.semijoins_inserted == 0) {
        printf("%s baseline ... FAIL (SIP not active)\n", name);
        return 1;
    }
    if (expect.expect_jpp_activity
        && jpp.joins_reordered == 0
        && jpp.projections_inserted == 0) {
        printf("%s baseline ... FAIL (JPP not active)\n", name);
        return 1;
    }
    if (expect.expect_magic_active
        && (!magic_applied || magic.adorned_predicates == 0
        || magic.magic_rules_generated == 0)) {
        printf("%s baseline ... FAIL (Magic Sets not active)\n", name);
        return 1;
    }
    if (expect.expect_magic_noop
        && (magic_applied || magic.adorned_predicates != 0
        || magic.magic_rules_generated != 0
        || magic.original_rules_modified != 0)) {
        printf("%s baseline ... FAIL (Magic Sets should be all-free/no-op)\n",
            name);
        return 1;
    }

    for (uint32_t mask = 0; mask < 16; mask++) {
        opt_flags_t flags = {
            .magic_sets = (mask & 0x1) != 0,
            .sip = (mask & 0x2) != 0,
            .fusion = (mask & 0x4) != 0,
            .jpp = (mask & 0x8) != 0,
        };
        row_set_t actual;
        if (!run_program(src, relation, flags, &actual)
            || !row_sets_equal(&baseline, &actual)) {
            printf("%s mask=0x%x ... FAIL\n", name, mask);
            return 1;
        }
    }

    printf("%s ... PASS\n", name);
    return 0;
}

static int
test_row_set_multiset_equality(void)
{
    row_set_t expected = { 0 };
    row_set_t reordered = { 0 };
    row_set_t mismatch = { 0 };

    expected.relation = "r";
    reordered.relation = "r";
    mismatch.relation = "r";
    expected.ncols = 2;
    reordered.ncols = 2;
    mismatch.ncols = 2;

    expected.rows[0][0] = 1;
    expected.rows[0][1] = 2;
    expected.rows[1][0] = 1;
    expected.rows[1][1] = 2;
    expected.rows[2][0] = 3;
    expected.rows[2][1] = 4;
    expected.count = 3;

    reordered.rows[0][0] = 1;
    reordered.rows[0][1] = 2;
    reordered.rows[1][0] = 3;
    reordered.rows[1][1] = 4;
    reordered.rows[2][0] = 1;
    reordered.rows[2][1] = 2;
    reordered.count = 3;

    mismatch.rows[0][0] = 1;
    mismatch.rows[0][1] = 2;
    mismatch.rows[1][0] = 3;
    mismatch.rows[1][1] = 4;
    mismatch.count = 2;

    if (!row_sets_equal(&expected, &reordered)) {
        printf("multiset row compare ... FAIL\n");
        return 1;
    }

    if (row_sets_equal(&expected, &mismatch)) {
        printf("multiset row compare ... FAIL (duplicates ignored)\n");
        return 1;
    }

    printf("multiset row compare ... PASS\n");
    return 0;
}

static wirelog_program_t *
parse_aggregate_program(void)
{
    static const char *src =
        ".decl val(g: int32, v: int32)\n"
        ".decl minv(g: int32, v: int32)\n"
        ".output minv\n"
        ".query minv(b, f) .\n"
        "val(1, 5).\n"
        "val(1, 3).\n"
        "val(2, 7).\n"
        "minv(g, min(v)) :- val(g, v).\n";
    wirelog_error_t err = WIRELOG_OK;
    return wirelog_parse_string(src, &err);
}

static int
test_aggregate_skip_stats(void)
{
    wirelog_program_t *prog = parse_aggregate_program();
    if (!prog)
        return 1;
    wl_fusion_stats_t fusion = { 0 };
    int failed = wl_fusion_apply(prog, &fusion) != 0
        || fusion.skipped_aggregate == 0;
    wirelog_program_free(prog);
    if (failed) {
        printf("aggregate skip: fusion ... FAIL\n");
        return 1;
    }

    prog = parse_aggregate_program();
    if (!prog)
        return 1;
    wl_jpp_stats_t jpp = { 0 };
    failed = wl_jpp_apply(prog, &jpp) != 0 || jpp.skipped_aggregate == 0;
    wirelog_program_free(prog);
    if (failed) {
        printf("aggregate skip: jpp ... FAIL\n");
        return 1;
    }

    prog = parse_aggregate_program();
    if (!prog)
        return 1;
    wl_sip_stats_t sip = { 0 };
    failed = wl_sip_apply(prog, &sip) != 0 || sip.skipped_aggregate == 0;
    wirelog_program_free(prog);
    if (failed) {
        printf("aggregate skip: sip ... FAIL\n");
        return 1;
    }

    prog = parse_aggregate_program();
    if (!prog)
        return 1;
    uint32_t relation_count_before = prog->relation_count;
    wl_magic_sets_stats_t magic = { 0 };
    failed = wl_magic_sets_apply_with_demands(prog, prog->demands,
            prog->demand_count, &magic)
        != 0
        || magic.skipped_aggregate == 0 || prog->magic_sets_applied
        || prog->relation_count != relation_count_before;
    wirelog_program_free(prog);
    if (failed) {
        printf("aggregate skip: magic_sets ... FAIL\n");
        return 1;
    }

    printf("aggregate skip stats ... PASS\n");
    return 0;
}

int
main(void)
{
    static const char *join_src =
        ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl out(x: int32, z: int32)\n"
        ".output out\n"
        "a(1, 10).\n"
        "a(2, 20).\n"
        "b(10, 100).\n"
        "b(20, 200).\n"
        "b(30, 300).\n"
        "out(x, z) :- a(x, y), b(y, z).\n";
    static const char *recursive_src =
        ".decl edge(x: int32, y: int32)\n"
        ".decl path(x: int32, y: int32)\n"
        ".output path\n"
        "edge(1, 2).\n"
        "edge(2, 3).\n"
        "edge(3, 4).\n"
        "path(x, y) :- edge(x, y).\n"
        "path(x, y) :- edge(x, z), path(z, y).\n";
    static const char *aggregate_src =
        ".decl val(g: int32, v: int32)\n"
        ".decl minv(g: int32, v: int32)\n"
        ".output minv\n"
        "val(1, 5).\n"
        "val(1, 3).\n"
        "val(2, 7).\n"
        "minv(g, min(v)) :- val(g, v).\n";
    static const char *fusion_src =
        ".decl a(x: int32, y: int32)\n"
        ".decl out(x: int32)\n"
        ".output out\n"
        "a(1, 10).\n"
        "a(2, 20).\n"
        "out(x) :- a(x, y), y > 0.\n";
    static const char *sip_src =
        ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl c(z: int32, w: int32)\n"
        ".decl out(x: int32, w: int32)\n"
        ".output out\n"
        "a(1, 2).\n"
        "a(2, 3).\n"
        "a(3, 4).\n"
        "b(2, 5).\n"
        "b(3, 6).\n"
        "b(4, 7).\n"
        "c(5, 10).\n"
        "c(6, 11).\n"
        "c(7, 12).\n"
        "out(x, w) :- a(x, y), b(y, z), c(z, w).\n";
    static const char *jpp_src =
        ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl c(w: int32, z: int32)\n"
        ".decl out(x: int32, z: int32)\n"
        ".output out\n"
        "a(1, 2).\n"
        "a(1, 3).\n"
        "b(2, 4).\n"
        "b(3, 5).\n"
        "b(4, 6).\n"
        "c(6, 7).\n"
        "c(7, 8).\n"
        "out(x, z) :- a(x, y), c(w, z), b(y, w).\n";
    static const char *bounded_magic_src =
        ".decl p(x: int32, y: int32)\n"
        ".decl edge(x: int32, y: int32)\n"
        ".decl q(x: int32, y: int32)\n"
        ".output p\n"
        ".query q(b, f) .\n"
        "p(1, 10).\n"
        "p(2, 20).\n"
        "edge(1, 2).\n"
        "edge(2, 3).\n"
        "edge(3, 4).\n"
        "q(x, y) :- edge(x, y).\n"
        "q(x, y) :- edge(x, z), q(z, y).\n";
    static const char *all_free_magic_src =
        ".decl af(x: int32, y: int32)\n"
        ".decl out(x: int32, y: int32)\n"
        ".output out\n"
        ".query af(f, f) .\n"
        "out(x, y) :- af(x, y).\n"
        "af(1, 10).\n"
        "af(2, 20).\n"
        "af(2, 20).\n";

    int failed = 0;
    matrix_expectation_t expect_none = { 0, 0, 0, 0, 1 };
    matrix_expectation_t expect_fusion = { 1, 0, 0, 0, 0 };
    matrix_expectation_t expect_sip = { 0, 1, 0, 0, 0 };
    matrix_expectation_t expect_jpp = { 0, 0, 1, 0, 0 };
    matrix_expectation_t expect_noop_magic = { 0, 0, 0, 0, 1 };
    matrix_expectation_t expect_magic = { 0, 0, 0, 1, 0 };

    failed += check_matrix_case("optimizer matrix: join", join_src, "out",
            expect_none);
    failed += check_matrix_case("optimizer matrix: recursive", recursive_src,
            "path", expect_none);
    failed += check_matrix_case("optimizer matrix: aggregate", aggregate_src,
            "minv", expect_none);
    failed += check_matrix_case("optimizer matrix: fusion", fusion_src, "out",
            expect_fusion);
    failed += check_matrix_case("optimizer matrix: sip", sip_src, "out",
            expect_sip);
    failed += check_matrix_case("optimizer matrix: jpp", jpp_src, "out",
            expect_jpp);
    failed += check_matrix_case("optimizer matrix: bounded magic",
            bounded_magic_src,
            "p", expect_magic);
    failed += check_matrix_case("optimizer matrix: all-free magic skip",
            all_free_magic_src, "out", expect_noop_magic);
    failed += test_row_set_multiset_equality();
    failed += test_aggregate_skip_stats();
    return failed == 0 ? 0 : 1;
}
