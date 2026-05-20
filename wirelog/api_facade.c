/*
 * api_facade.c - legacy umbrella public API facade.
 *
 * Copyright (C) CleverPlant
 * SPDX-License-Identifier: LGPL-3.0-or-later
 */

#include "wirelog/wirelog.h"

#include "backend.h"
#include "exec_plan_gen.h"
#include "io/csv_reader.h"
#include "ir/program.h"
#include "ir/stratify.h"
#include "passes/fusion.h"
#include "passes/jpp.h"
#include "passes/magic_sets.h"
#include "passes/sip.h"
#include "passes/subsumption.h"
#include "session.h"
#include "session_facts.h"
#include "wirelog-config.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define WIRELOG_EXECUTOR_MAGIC UINT64_C(0x574c455845433831)

struct wirelog_executor {
    uint64_t magic;
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session;
};

typedef struct {
    char *name;
    int64_t *data;
    uint64_t rows;
    uint32_t cols;
    uint64_t capacity_rows;
} wl_result_relation_t;

struct wirelog_result {
    wl_result_relation_t *relations;
    uint32_t count;
    uint32_t capacity;
};

static void
set_error(wirelog_error_t *error, wirelog_error_t value)
{
    if (error)
        *error = value;
}

static char *
wl_strdup(const char *s)
{
    if (!s)
        return NULL;
    size_t len = strlen(s) + 1;
    char *copy = (char *)malloc(len);
    if (copy)
        memcpy(copy, s, len);
    return copy;
}

static const wl_ir_relation_info_t *
find_relation(const wirelog_program_t *program, const char *name)
{
    if (!program || !name)
        return NULL;
    for (uint32_t i = 0; i < program->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &program->relations[i];
        if (rel->name && strcmp(rel->name, name) == 0)
            return rel;
    }
    return NULL;
}

static wirelog_executor_t *
checked_executor(void *worker)
{
    wirelog_executor_t *executor = (wirelog_executor_t *)worker;
    if (!executor || executor->magic != WIRELOG_EXECUTOR_MAGIC)
        return NULL;
    return executor;
}

static uint32_t
count_ir_nodes(const wirelog_program_t *program)
{
    if (!program || !program->relation_irs)
        return 0;
    uint32_t total = 0;
    for (uint32_t i = 0; i < program->relation_count; i++)
        total += wl_fusion_count_nodes(program->relation_irs[i]);
    return total;
}

static bool
run_optimizer_pass(wirelog_program_t *program, wirelog_opt_pass_t pass,
    wirelog_error_t *error, uint32_t *passes_applied,
    wirelog_opt_stats_t *stats)
{
    int rc = 0;

    switch (pass) {
    case WIRELOG_OPT_LOGIC_FUSION: {
        wl_fusion_stats_t fusion = { 0 };
        rc = wl_fusion_apply(program, &fusion);
        if (stats)
            stats->fusion_count += fusion.fusions_applied;
        break;
    }
    case WIRELOG_OPT_JOIN_PROJECT_PLAN: {
        wl_jpp_stats_t jpp = { 0 };
        rc = wl_jpp_apply(program, &jpp);
        if (stats)
            stats->join_reorders += jpp.joins_reordered;
        break;
    }
    case WIRELOG_OPT_SEMIJOIN:
        rc = wl_sip_apply(program, NULL);
        break;
    case WIRELOG_OPT_SUBPLAN_SHARING:
    case WIRELOG_OPT_BOOLEAN_SPEC:
        rc = 0;
        break;
    default:
        set_error(error, WIRELOG_ERR_INVALID_IR);
        return false;
    }

    if (rc != 0) {
        set_error(error,
            rc == -1 ? WIRELOG_ERR_MEMORY : WIRELOG_ERR_INVALID_IR);
        return false;
    }

    if (passes_applied)
        (*passes_applied)++;
    return true;
}

static bool
rebuild_after_magic_sets(wirelog_program_t *program, wirelog_error_t *error)
{
    if (!program || !program->magic_sets_applied)
        return true;
    if (wl_ir_program_rebuild_relation_irs(program) != 0) {
        set_error(error, WIRELOG_ERR_MEMORY);
        return false;
    }
    wl_ir_program_free_strata(program);
    int rc = wl_ir_stratify_program(program);
    if (rc != 0) {
        set_error(error, rc == -1 ? WIRELOG_ERR_MEMORY : WIRELOG_ERR_PARSE);
        return false;
    }
    return true;
}

static void
free_result_relation(wl_result_relation_t *rel)
{
    if (!rel)
        return;
    free(rel->name);
    free(rel->data);
}

static wl_result_relation_t *
find_result_relation(const wirelog_result_t *result, const char *relation_name)
{
    if (!result || !relation_name)
        return NULL;
    for (uint32_t i = 0; i < result->count; i++) {
        wl_result_relation_t *rel = &result->relations[i];
        if (rel->name && strcmp(rel->name, relation_name) == 0)
            return rel;
    }
    return NULL;
}

static wl_result_relation_t *
ensure_result_relation(wirelog_result_t *result, const char *relation,
    uint32_t cols)
{
    wl_result_relation_t *existing = find_result_relation(result, relation);
    if (existing)
        return existing->cols == cols ? existing : NULL;

    if (result->count == result->capacity) {
        uint32_t next = result->capacity ? result->capacity * 2 : 8;
        wl_result_relation_t *rels = (wl_result_relation_t *)realloc(
            result->relations, next * sizeof(wl_result_relation_t));
        if (!rels)
            return NULL;
        memset(rels + result->capacity, 0,
            (next - result->capacity) * sizeof(wl_result_relation_t));
        result->relations = rels;
        result->capacity = next;
    }

    wl_result_relation_t *rel = &result->relations[result->count++];
    rel->name = wl_strdup(relation);
    if (!rel->name)
        return NULL;
    rel->cols = cols;
    return rel;
}

static bool
append_result_row(wl_result_relation_t *rel, const int64_t *row)
{
    if (rel->rows == rel->capacity_rows) {
        uint64_t next = rel->capacity_rows ? rel->capacity_rows * 2 : 16;
        if (rel->cols != 0 && next > UINT64_MAX / rel->cols)
            return false;
        int64_t *data = (int64_t *)realloc(
            rel->data, next * rel->cols * sizeof(int64_t));
        if (!data)
            return false;
        rel->data = data;
        rel->capacity_rows = next;
    }
    memcpy(rel->data + rel->rows * rel->cols, row, rel->cols * sizeof(int64_t));
    rel->rows++;
    return true;
}

typedef struct {
    wirelog_result_t *result;
    bool failed;
} wl_collect_ctx_t;

static void
collect_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    wl_collect_ctx_t *ctx = (wl_collect_ctx_t *)user_data;
    if (!ctx || ctx->failed)
        return;
    wl_result_relation_t *rel = ensure_result_relation(ctx->result, relation,
            ncols);
    if (!rel || !append_result_row(rel, row))
        ctx->failed = true;
}

wirelog_opt_config_t
wirelog_optimizer_get_default_config(void)
{
    return (wirelog_opt_config_t) {
               .enable_logic_fusion = true,
               .enable_join_ordering = true,
               .enable_semijoin = true,
               .enable_subplan_sharing = true,
               .enable_boolean_spec = true,
               .max_join_search_space = 1024,
               .debug_trace = false,
    };
}

bool
wirelog_optimize_with_config(wirelog_program_t *program,
    const wirelog_opt_config_t *config, wirelog_error_t *error)
{
    if (!program) {
        set_error(error, WIRELOG_ERR_INVALID_IR);
        return false;
    }

    wirelog_opt_config_t default_config;
    if (!config) {
        default_config = wirelog_optimizer_get_default_config();
        config = &default_config;
    }

    wirelog_opt_stats_t stats = {
        .original_node_count = count_ir_nodes(program),
    };
    uint32_t applied = 0;

    int rc = wl_subsumption_apply(program, NULL);
    if (rc != 0) {
        set_error(error, WIRELOG_ERR_INVALID_IR);
        return false;
    }

    if (config->enable_logic_fusion
        && !run_optimizer_pass(program, WIRELOG_OPT_LOGIC_FUSION, error,
        &applied, &stats))
        return false;
    if (config->enable_join_ordering
        && !run_optimizer_pass(program, WIRELOG_OPT_JOIN_PROJECT_PLAN, error,
        &applied, &stats))
        return false;
    if (config->enable_semijoin
        && !run_optimizer_pass(program, WIRELOG_OPT_SEMIJOIN, error, &applied,
        &stats))
        return false;
    if (config->enable_subplan_sharing
        && !run_optimizer_pass(program, WIRELOG_OPT_SUBPLAN_SHARING, error,
        &applied, &stats))
        return false;
    if (config->enable_boolean_spec
        && !run_optimizer_pass(program, WIRELOG_OPT_BOOLEAN_SPEC, error,
        &applied, &stats))
        return false;

    rc = wl_magic_sets_apply_with_demands(
        program, program->demands, program->demand_count, NULL);
    if (rc != 0) {
        set_error(error,
            rc == -1 ? WIRELOG_ERR_MEMORY : WIRELOG_ERR_INVALID_IR);
        return false;
    }
    if (!rebuild_after_magic_sets(program, error))
        return false;

    stats.passes_applied = applied;
    stats.optimized_node_count = count_ir_nodes(program);
    program->optimizer_stats_valid = true;
    program->optimizer_original_node_count = stats.original_node_count;
    program->optimizer_optimized_node_count = stats.optimized_node_count;
    program->optimizer_passes_applied = stats.passes_applied;
    program->optimizer_fusion_count = stats.fusion_count;
    program->optimizer_join_reorders = stats.join_reorders;
    program->optimizer_time_ms = stats.optimization_time_ms;
    set_error(error, WIRELOG_OK);
    return true;
}

bool
wirelog_optimize(wirelog_program_t *program, wirelog_error_t *error)
{
    return wirelog_optimize_with_config(program, NULL, error);
}

bool
wirelog_optimize_apply_pass(wirelog_program_t *program, wirelog_opt_pass_t pass,
    wirelog_error_t *error)
{
    if (!program) {
        set_error(error, WIRELOG_ERR_INVALID_IR);
        return false;
    }
    uint32_t applied = 0;
    bool ok = run_optimizer_pass(program, pass, error, &applied, NULL);
    if (ok)
        set_error(error, WIRELOG_OK);
    return ok;
}

bool
wirelog_optimizer_get_stats(const wirelog_program_t *program,
    wirelog_opt_stats_t *stats)
{
    if (!program || !stats)
        return false;
    if (!program->optimizer_stats_valid)
        return false;
    memset(stats, 0, sizeof(*stats));
    stats->original_node_count = program->optimizer_original_node_count;
    stats->optimized_node_count = program->optimizer_optimized_node_count;
    stats->passes_applied = program->optimizer_passes_applied;
    stats->fusion_count = program->optimizer_fusion_count;
    stats->join_reorders = program->optimizer_join_reorders;
    stats->optimization_time_ms = program->optimizer_time_ms;
    return true;
}

void
wirelog_optimizer_debug_print(const wirelog_program_t *program)
{
    if (!program) {
        fprintf(stderr, "wirelog optimizer: NULL program\n");
        return;
    }

    fprintf(stderr, "wirelog optimizer: relations=%u rules=%u strata=%u\n",
        program->relation_count, program->rule_count, program->stratum_count);
    if (!program->relation_irs)
        return;
    for (uint32_t i = 0; i < program->relation_count; i++) {
        if (!program->relation_irs[i])
            continue;
        fprintf(stderr, "relation %s:\n",
            program->relations[i].name ? program->relations[i].name : "(null)");
        wirelog_ir_node_print(program->relation_irs[i], 2);
    }
}

void
wirelog_optimizer_debug(const wirelog_program_t *program)
{
    wirelog_optimizer_debug_print(program);
}

uint64_t
wirelog_optimizer_cost_estimate(const wirelog_program_t *program)
{
    return count_ir_nodes(program);
}

int
wirelog_load_all_facts(const wirelog_program_t *prog, void *worker)
{
    wirelog_executor_t *executor = checked_executor(worker);
    if (!prog || !executor || executor->program != prog)
        return -1;
    return wl_session_load_facts(executor->session, prog);
}

int
wirelog_load_input_files(const wirelog_program_t *prog, void *worker)
{
    wirelog_executor_t *executor = checked_executor(worker);
    if (!prog || !executor || executor->program != prog)
        return -1;
    return wl_session_load_input_files(executor->session, prog);
}

wirelog_executor_t *
wirelog_executor_create(wirelog_program_t *program, wirelog_error_t *error)
{
    if (!program) {
        set_error(error, WIRELOG_ERR_INVALID_IR);
        return NULL;
    }

    wirelog_executor_t *executor
        = (wirelog_executor_t *)calloc(1, sizeof(wirelog_executor_t));
    if (!executor) {
        set_error(error, WIRELOG_ERR_MEMORY);
        return NULL;
    }
    executor->magic = WIRELOG_EXECUTOR_MAGIC;
    executor->program = program;

    if (wl_plan_from_program(program, &executor->plan) != 0) {
        wirelog_executor_free(executor);
        set_error(error, WIRELOG_ERR_INVALID_IR);
        return NULL;
    }
    if (wl_session_create(wl_backend_columnar(), executor->plan, 1,
        &executor->session) != 0) {
        wirelog_executor_free(executor);
        set_error(error, WIRELOG_ERR_EXEC);
        return NULL;
    }
    if (wl_session_load_facts(executor->session, program) != 0
        || wl_session_load_input_files(executor->session, program) != 0) {
        wirelog_executor_free(executor);
        set_error(error, WIRELOG_ERR_IO);
        return NULL;
    }

    set_error(error, WIRELOG_OK);
    return executor;
}

void
wirelog_executor_free(wirelog_executor_t *executor)
{
    if (!executor)
        return;
    executor->magic = 0;
    wl_session_destroy(executor->session);
    wl_plan_free(executor->plan);
    free(executor);
}

bool
wirelog_load_facts_from_csv(wirelog_executor_t *executor,
    const char *relation_name, const char *csv_file, wirelog_error_t *error)
{
    if (!executor || !relation_name || !csv_file) {
        set_error(error, WIRELOG_ERR_EXEC);
        return false;
    }
    const wl_ir_relation_info_t *rel
        = find_relation(executor->program, relation_name);
    if (!rel) {
        set_error(error, WIRELOG_ERR_INVALID_IR);
        return false;
    }

    int64_t *data = NULL;
    uint32_t nrows = 0;
    uint32_t ncols = 0;
    wirelog_column_type_t *types = NULL;
    if (rel->column_count > 0) {
        types = (wirelog_column_type_t *)malloc(
            rel->column_count * sizeof(wirelog_column_type_t));
        if (!types) {
            set_error(error, WIRELOG_ERR_MEMORY);
            return false;
        }
        for (uint32_t i = 0; i < rel->column_count; i++)
            types[i] = rel->columns[i].type;
    }

    int rc = wl_csv_read_file_ex(csv_file, ',', types, rel->column_count,
            &data, &nrows, &ncols, executor->program->intern);
    free(types);
    if (rc != 0) {
        set_error(error, rc == -3 ? WIRELOG_ERR_MEMORY : WIRELOG_ERR_IO);
        return false;
    }

    rc = wl_session_insert(executor->session, relation_name, data, nrows,
            ncols);
    free(data);
    if (rc != 0) {
        set_error(error, WIRELOG_ERR_EXEC);
        return false;
    }
    set_error(error, WIRELOG_OK);
    return true;
}

wirelog_result_t *
wirelog_evaluate(wirelog_executor_t *executor, wirelog_error_t *error)
{
    if (!executor || !executor->session) {
        set_error(error, WIRELOG_ERR_EXEC);
        return NULL;
    }

    wirelog_result_t *result
        = (wirelog_result_t *)calloc(1, sizeof(wirelog_result_t));
    if (!result) {
        set_error(error, WIRELOG_ERR_MEMORY);
        return NULL;
    }

    wl_collect_ctx_t ctx = {
        .result = result,
        .failed = false,
    };
    if (wl_session_snapshot(executor->session, collect_tuple, &ctx) != 0
        || ctx.failed) {
        wirelog_result_free(result);
        set_error(error, ctx.failed ? WIRELOG_ERR_MEMORY : WIRELOG_ERR_EXEC);
        return NULL;
    }

    set_error(error, WIRELOG_OK);
    return result;
}

const void *
wirelog_result_get_relation(const wirelog_result_t *result,
    const char *relation_name)
{
    wl_result_relation_t *rel = find_result_relation(result, relation_name);
    if (!rel || rel->rows == 0)
        return NULL;
    return rel->data;
}

uint64_t
wirelog_result_relation_cardinality(const wirelog_result_t *result,
    const char *relation_name)
{
    wl_result_relation_t *rel = find_result_relation(result, relation_name);
    return rel ? rel->rows : 0;
}

bool
wirelog_result_write_csv(const wirelog_result_t *result,
    const char *relation_name, const char *output_file, wirelog_error_t *error)
{
    wl_result_relation_t *rel = find_result_relation(result, relation_name);
    if (!rel || !output_file) {
        set_error(error, WIRELOG_ERR_EXEC);
        return false;
    }

    FILE *out = fopen(output_file, "w");
    if (!out) {
        set_error(error, WIRELOG_ERR_IO);
        return false;
    }

    for (uint64_t r = 0; r < rel->rows; r++) {
        for (uint32_t c = 0; c < rel->cols; c++) {
            if (c > 0)
                fputc(',', out);
            fprintf(out, "%" PRId64, rel->data[r * rel->cols + c]);
        }
        fputc('\n', out);
    }

    if (fclose(out) != 0) {
        set_error(error, WIRELOG_ERR_IO);
        return false;
    }
    set_error(error, WIRELOG_OK);
    return true;
}

void
wirelog_result_free(wirelog_result_t *result)
{
    if (!result)
        return;
    for (uint32_t i = 0; i < result->count; i++)
        free_result_relation(&result->relations[i]);
    free(result->relations);
    free(result);
}

const char *
wirelog_version_string(void)
{
    return WIRELOG_VERSION_STRING;
}

const char *
wirelog_error_string(wirelog_error_t error)
{
    switch (error) {
    case WIRELOG_OK:
        return "ok";
    case WIRELOG_ERR_PARSE:
        return "parse error";
    case WIRELOG_ERR_INVALID_IR:
        return "invalid IR";
    case WIRELOG_ERR_EXEC:
        return "execution error";
    case WIRELOG_ERR_MEMORY:
        return "out of memory";
    case WIRELOG_ERR_IO:
        return "I/O error";
    case WIRELOG_ERR_COMPOUND_SATURATED:
        return "compound arena saturated";
    case WIRELOG_ERR_COMPOUND_BUSY:
        return "compound arena busy";
    case WIRELOG_ERR_UNKNOWN:
    default:
        return "unknown error";
    }
}

bool
wirelog_config_embedded(void)
{
#ifdef WIRELOG_EMBEDDED
    return true;
#else
    return false;
#endif
}

bool
wirelog_config_ipc(void)
{
#ifdef WIRELOG_IPC
    return true;
#else
    return false;
#endif
}

bool
wirelog_config_threads(void)
{
#ifdef WIRELOG_THREADS
    return true;
#else
    return false;
#endif
}
