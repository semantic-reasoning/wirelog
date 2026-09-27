/*
 * test_tdd_delta_driver_plan.c - input binding proof for unfused TDD plans
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef WL_TEST_ALLOC_WRAP
void *__real_calloc(size_t count, size_t size);
static int fail_calloc_after = -1;
void *
__wrap_calloc(size_t count, size_t size)
{
    if (fail_calloc_after >= 0 && fail_calloc_after-- == 0)
        return NULL;
    return __real_calloc(count, size);
}
#endif

#define CHECK(c) do { if (!(c)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #c); return 1; \
                      } } while (0)

static uint32_t
relation_index(const wl_plan_stratum_t *sp, const char *name)
{
    for (uint32_t r = 0; r < sp->relation_count; r++) {
        if (strcmp(sp->relations[r].name, name) == 0)
            return r;
    }
    return UINT32_MAX;
}

static int
empty_result(int rc, wl_columnar_eval_tdd_plan_manifest_t *manifest)
{
    CHECK(rc != 0);
    CHECK(!manifest->slices && !manifest->reads);
    CHECK(!manifest->slice_count && !manifest->read_count);
    CHECK(!manifest->alternative_count);
    wl_columnar_eval_tdd_plan_bindings_free(manifest);
    return 0;
}

/* An independent epoch fixture. Negative weights here are INPUT coverage,
 * not a claim about derived-output differential/retraction semantics. Full
 * generations deliberately differ from delta and include cross-owner rows. */
typedef struct {
    int64_t x, y, weight;
} row_t;

typedef struct {
    const char *name;
    const row_t *full;
    size_t full_count;
    const row_t *delta;
    size_t delta_count;
} epoch_relation_t;

/* Interpret only the three reads of one lowered valueAlias alternative.
 * Fixed expected tuples below are independent of the manifest. This catches
 * partitioning a FULL repeated predicate or the SEMIJOIN witness, without
 * claiming to validate the engine's SCC/differential evaluation protocol. */
static int
cross_owner_oracle(const wl_columnar_eval_tdd_plan_manifest_t *manifest,
    const epoch_relation_t epoch[2])
{
    static const row_t expected[3][2] = {
        {{31, 19, -2}, {1, 31, 24}},
        {{19, 31, -4}, {1, 31, 8}},
        {{19, 31, -6}, {1, 31, -4}},
    };
    const uint32_t widths[] = {1, 2, 8};
    uint32_t checked = 0;
    for (uint32_t s = 0; s < manifest->slice_count; s++) {
        const wl_columnar_eval_tdd_plan_slice_t *slice = &manifest->slices[s];
        if (slice->inactive || slice->seed || slice->ordinal != 1)
            continue;
        CHECK(slice->alternative >= 2 && slice->alternative <= 4);
        const wl_columnar_eval_tdd_plan_read_t *bindings[4] = {0};
        for (uint32_t i = 0; i < slice->read_count; i++) {
            const wl_columnar_eval_tdd_plan_read_t *read =
                &manifest->reads[slice->read_start + i];
            CHECK(read->source_index >= 2 && read->source_index <= 5);
            bindings[read->source_index - 2] = read;
        }
        const row_t *rows[4];
        size_t counts[4];
        for (uint32_t i = 0; i < 4; i++) {
            CHECK(bindings[i]);
            const epoch_relation_t *rel = NULL;
            for (uint32_t j = 0; j < 2; j++) {
                if (strcmp(bindings[i]->relation_name, epoch[j].name) == 0)
                    rel = &epoch[j];
            }
            CHECK(rel);
            bool delta = bindings[i]->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA;
            rows[i] = delta ? rel->delta : rel->full;
            counts[i] = delta ? rel->delta_count : rel->full_count;
        }
        for (uint32_t wi = 0; wi < 3; wi++) {
            int64_t weights[2] = {0};
            for (uint32_t w = 0; w < widths[wi]; w++) {
                for (size_t a = 0; a < counts[0]; a++) {
                    if (bindings[0]->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
                        && (uint64_t)rows[0][a].x % widths[wi] != w)
                        continue;
                    for (size_t b = 0; b < counts[1]; b++) {
                        if (rows[0][a].x != rows[1][b].x
                            || (bindings[1]->kind ==
                            WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
                            && (uint64_t)rows[1][b].x % widths[wi] != w))
                            continue;
                        bool witness = false;
                        for (size_t p = 0; p < counts[2]; p++)
                            witness |= rows[2][p].x == rows[1][b].y &&
                                rows[2][p].weight > 0;
                        if (!witness)
                            continue;
                        for (size_t c = 0; c < counts[3]; c++) {
                            if (rows[1][b].y != rows[3][c].x
                                || (bindings[3]->kind ==
                                WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
                                && (uint64_t)rows[3][c].x % widths[wi] != w))
                                continue;
                            const row_t *golden = expected[slice->alternative -
                                    2];
                            uint32_t found = 2;
                            for (uint32_t e = 0; e < 2; e++) {
                                if (golden[e].x == rows[0][a].y &&
                                    golden[e].y == rows[3][c].y)
                                    found = e;
                            }
                            CHECK(found < 2);
                            weights[found] += rows[0][a].weight *
                                rows[1][b].weight * rows[3][c].weight;
                        }
                    }
                }
            }
            for (uint32_t e = 0; e < 2; e++)
                CHECK(weights[e] == expected[slice->alternative - 2][e].weight);
        }
        checked++;
    }
    CHECK(checked == 3);
    return 0;
}

static int
input_coverage(const wl_columnar_eval_tdd_plan_manifest_t *manifest)
{
    static const row_t full_flow[] = {{1, 8, 2}, {8, 19, 1}, {19, 31, 4},
                                      {31, 1, 1}};
    static const row_t delta_flow[] = {{1, 8, 1}, {19, 31, -2}, {31, 1, 3}};
    static const row_t full_memory[] = {{8, 19, 3}, {19, 8, 1}, {31, 19, 2}};
    static const row_t delta_memory[] = {{8, 19, -1}, {31, 19, 2}};
    const epoch_relation_t epoch[] = {
        {"valueFlow", full_flow, 4, delta_flow, 3},
        {"memoryAlias", full_memory, 3, delta_memory, 2},
    };
    const uint32_t widths[] = {1, 2, 8};
    uint32_t driver_slices = 0, full_same_predicate = 0, prefilters = 0;
    for (uint32_t s = 0; s < manifest->slice_count; s++) {
        const wl_columnar_eval_tdd_plan_slice_t *slice = &manifest->slices[s];
        if (slice->inactive || slice->seed)
            continue;
        driver_slices++;
        const char *driver_name = NULL;
        for (uint32_t j = 0; j < slice->read_count; j++) {
            const wl_columnar_eval_tdd_plan_read_t *read =
                &manifest->reads[slice->read_start + j];
            if (read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA)
                driver_name = read->relation_name;
        }
        CHECK(driver_name);
        for (uint32_t j = 0; j < slice->read_count; j++) {
            const wl_columnar_eval_tdd_plan_read_t *read =
                &manifest->reads[slice->read_start + j];
            const epoch_relation_t *rel = NULL;
            for (size_t r = 0; r < 2; r++) {
                if (strcmp(read->relation_name, epoch[r].name) == 0)
                    rel = &epoch[r];
            }
            CHECK(rel);
            bool driver = read->op_index == slice->driver;
            CHECK(driver == (read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA));
            if (!driver && strcmp(read->relation_name, driver_name) == 0)
                full_same_predicate++;
            if (read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER) {
                prefilters++;
                CHECK(!driver && rel == &epoch[0]);
                /* Witness key 8 exists only in FULL, and owner 0 differs
                 * from the key 19 driver owner 1 at W=2 and W=8. */
                CHECK(rel->full[1].x == 8 && rel->full[1].y == 19);
                for (size_t n = 0; n < rel->delta_count; n++)
                    CHECK(rel->delta[n].x != 8);
            }
            for (size_t wi = 0; wi < 3; wi++) {
                uint32_t wcount = widths[wi];
                unsigned seen[4] = {0};
                int64_t weights[4] = {0};
                for (uint32_t w = 0; w < wcount; w++) {
                    const row_t *input = driver ? rel->delta : rel->full;
                    size_t count = driver ? rel->delta_count : rel->full_count;
                    for (size_t n = 0; n < count; n++) {
                        if (driver && (uint64_t)input[n].x % wcount != w)
                            continue;
                        /* Exact relation-qualified tuples and signed input
                         * weights, not counts/fingerprints alone. */
                        const row_t *expected =
                            driver ? &rel->delta[n] : &rel->full[n];
                        CHECK(input[n].x == expected->x &&
                            input[n].y == expected->y);
                        CHECK(input[n].weight == expected->weight);
                        seen[n]++;
                        weights[n] += input[n].weight;
                    }
                }
                for (size_t n = 0;
                    n < (driver ? rel->delta_count : rel->full_count); n++) {
                    CHECK(seen[n] == (driver ? 1u : wcount));
                    CHECK(weights[n] == (driver ? rel->delta[n].weight
                        : rel->full[n].weight * wcount));
                }
            }
        }
    }
    CHECK(driver_slices == 5 && full_same_predicate > 0 && prefilters == 3);
    CHECK(cross_owner_oracle(manifest, epoch) == 0);
    return 0;
}

static int
check_cspa(const wl_plan_stratum_t *sp)
{
    CHECK(sp->relation_count == 3);
    uint32_t flow = relation_index(sp, "valueFlow");
    uint32_t alias = relation_index(sp, "valueAlias");
    uint32_t memory = relation_index(sp, "memoryAlias");
    CHECK(flow != UINT32_MAX && alias != UINT32_MAX && memory != UINT32_MAX);
    const uint32_t expected_flow[] = {8, 25, 44};
    const uint32_t expected_alias[] = {0, 10, 20, 30, 41};
    const uint32_t indices[] = {flow, alias};
    for (uint32_t r = 0; r < 2; r++) {
        wl_columnar_eval_tdd_plan_manifest_t manifest;
        CHECK(wl_columnar_eval_tdd_plan_bindings(sp, indices[r],
            &manifest) == 0);
        CHECK(manifest.alternative_count == (r == 0 ? 3u : 5u));
        CHECK(manifest.block_size == (r == 0 ? 16u : 9u));
        CHECK(manifest.slice_count == (r == 0 ? 15u : 10u));
        uint32_t active = 0, seeds = 0, inactive = 0, prefilters = 0;
        for (uint32_t s = 0; s < manifest.slice_count; s++) {
            const wl_columnar_eval_tdd_plan_slice_t *slice =
                &manifest.slices[s];
            CHECK(slice->alternative < manifest.alternative_count);
            CHECK(slice->start / manifest.block_size == slice->alternative);
            if (slice->seed) {
                seeds++;
                CHECK(slice->driver == UINT32_MAX && !slice->inactive);
            } else if (slice->inactive) {
                inactive++;
                CHECK(slice->driver == UINT32_MAX && slice->read_count == 0);
            } else {
                const uint32_t *expected = r ==
                    0 ? expected_flow : expected_alias;
                CHECK(slice->driver == expected[active++]);
                uint32_t delta_reads = 0;
                for (uint32_t j = 0; j < slice->read_count; j++) {
                    const wl_columnar_eval_tdd_plan_read_t *read =
                        &manifest.reads[slice->read_start + j];
                    const wl_plan_op_t *op =
                        &sp->relations[indices[r]].ops[read->op_index];
                    CHECK(read->source_index ==
                        read->op_index % manifest.block_size);
                    CHECK(read->right_operand ==
                        (op->op != WL_PLAN_OP_VARIABLE));
                    CHECK(read->relation_name ==
                        (read->right_operand ? op->right_relation :
                        op->relation_name));
                    delta_reads += read->kind ==
                        WL_COLUMNAR_EVAL_TDD_PLAN_DELTA;
                    if (read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER) {
                        CHECK(r == 1 && read->source_index == 4);
                        CHECK(op->op == WL_PLAN_OP_SEMIJOIN &&
                            op->delta_mode == WL_DELTA_AUTO);
                        prefilters++;
                    }
                }
                CHECK(delta_reads == 1);
            }
        }
        CHECK(active == (r == 0 ? 3u : 5u));
        CHECK(seeds == (r == 0 ? 9u : 0u));
        CHECK(inactive == (r == 0 ? 3u : 5u));
        CHECK(prefilters == (r == 0 ? 0u : 3u));
        if (r == 1)
            CHECK(input_coverage(&manifest) == 0);
        wl_columnar_eval_tdd_plan_bindings_free(&manifest);
        CHECK(!manifest.slices && !manifest.reads && !manifest.slice_count);
    }
    wl_columnar_eval_tdd_plan_manifest_t manifest;
    CHECK(wl_columnar_eval_tdd_plan_bindings(sp, memory, &manifest) == ENOTSUP);
    CHECK(empty_result(ENOTSUP, &manifest) == 0);
    CHECK(!tdd_stratum_global_read_candidate(sp));
    return 0;
}

static int
check_mutations(const wl_plan_stratum_t *original)
{
    wl_plan_relation_t relations[3];
    memcpy(relations, original->relations, sizeof(relations));
    wl_plan_stratum_t sp = *original;
    sp.relations = relations;
    uint32_t ri = relation_index(&sp, "valueAlias");
    wl_plan_relation_t *rel = &relations[ri];
    const wl_plan_op_t *source = rel->ops;
    uint32_t count = rel->op_count;
    wl_plan_op_t *ops = malloc((size_t)count * sizeof(*ops));
    CHECK(ops);
    rel->ops = ops;
    const char *bad_key[] = {"col99"};
    const uint32_t bad_projection[] = {0, 2};
    uint8_t bad_filter[] = {0};
    for (uint32_t mutation = 0; mutation < 14; mutation++) {
        memcpy(ops, source, (size_t)count * sizeof(*ops));
        rel->op_count = count;
        switch (mutation) {
        case 0: /* Drop a complete alternative: divisibility still holds. */
            memmove(ops, ops + 9, (size_t)(count - 9) * sizeof(*ops));
            rel->op_count -= 9;
            break;
        case 1: memcpy(ops + 9, ops, 9 * sizeof(*ops)); break;
        case 2: ops[10].right_keys = bad_key; break;
        case 3: ops[10].project_indices = bad_projection; break;
        case 4: ops[10].right_filter_expr = (wl_plan_expr_buffer_t){bad_filter,
                                                                    1}; break;
        case 5: ops[8].op = WL_PLAN_OP_CONSOLIDATE; break;
        case 6: ops[6].op = WL_PLAN_OP_MAP; break;
        case 7: ops[count - 1].opaque_data = NULL; break;
        case 8: ops[count - 2].op = WL_PLAN_OP_CONCAT; break;
        case 9: ops[2].delta_mode = WL_DELTA_FORCE_FULL; break;
        case 10: ops[4].delta_mode = WL_DELTA_FORCE_DELTA; break;
        case 11: /* All copies agree, but prefilter is not the supported form. */
            for (uint32_t b = 0; b < 5; b++)
                ops[b * 9 + 4].left_keys = bad_key;
            break;
        case 12: /* Unsupported operator must not become a seed/ignored op. */
            for (uint32_t b = 0; b < 5; b++)
                ops[b * 9 + 4].op = WL_PLAN_OP_ANTIJOIN;
            break;
        case 13: ops[10].delta_mode = WL_DELTA_FORCE_FULL; break;
        }
        wl_columnar_eval_tdd_plan_manifest_t manifest;
        memset(&manifest, 0xa5, sizeof(manifest));
        int rc = wl_columnar_eval_tdd_plan_bindings(&sp, ri, &manifest);
        if (!rc)
            fprintf(stderr, "unexpectedly accepted mutation %u\n", mutation);
        CHECK(empty_result(rc, &manifest) == 0);
    }
    free(ops);
    wl_columnar_eval_tdd_plan_manifest_t manifest;
    CHECK(empty_result(wl_columnar_eval_tdd_plan_bindings(NULL, 0, &manifest),
        &manifest) == 0);
    CHECK(empty_result(wl_columnar_eval_tdd_plan_bindings(original, 3,
        &manifest), &manifest) == 0);
#ifdef WL_TEST_ALLOC_WRAP
    for (int failure = 0; failure < 2; failure++) {
        fail_calloc_after = failure;
        int rc = wl_columnar_eval_tdd_plan_bindings(original, ri, &manifest);
        fail_calloc_after = -1;
        CHECK(rc == ENOMEM);
        CHECK(empty_result(rc, &manifest) == 0);
        CHECK(wl_columnar_eval_tdd_plan_bindings(original, ri, &manifest) == 0);
        wl_columnar_eval_tdd_plan_bindings_free(&manifest);
    }
#endif
    return 0;
}

int
main(void)
{
    const char *path = getenv("WIRELOG_TEST_CSPA_PLAN");
    CHECK(path);
    FILE *file = fopen(path, "rb");
    CHECK(file);
    char source[8192];
    size_t count = fread(source, 1, sizeof(source) - 1, file);
    CHECK(!ferror(file) && feof(file));
    fclose(file);
    source[count] = '\0';
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_fusion_apply(program, NULL);
    wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    wl_plan_t *plan = NULL;
    CHECK(wl_plan_from_program(program, &plan) == 0);
    CHECK(plan->stratum_count == 1 && plan->strata[0].is_recursive);
    CHECK(check_cspa(&plan->strata[0]) == 0);
    CHECK(check_mutations(&plan->strata[0]) == 0);
    wl_plan_free(plan);
    wirelog_program_free(program);
    puts(
        "TDD occurrence bindings: CSPA, input coverage, malformed plans and retry PASS");
    return 0;
}
