/*
 * test_wirelog_public_api.c - public umbrella API regression tests (#841).
 *
 * Copyright (C) CleverPlant
 * SPDX-License-Identifier: LGPL-3.0-or-later
 */

#include "wirelog/wirelog.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int metadata_probe_reads;

static int
metadata_probe_read(wirelog_io_ctx_t *ctx, int64_t **out_data,
    uint32_t *out_nrows, void *user_data)
{
    (void)ctx;
    (void)out_data;
    (void)out_nrows;
    (void)user_data;
    metadata_probe_reads++;
    return -1;
}

static int
test_utility_api(void)
{
    if (!wirelog_version_string() || wirelog_version_string()[0] == '\0') {
        fprintf(stderr, "version string is empty\n");
        return 1;
    }
    if (!wirelog_error_string(WIRELOG_OK)
        || !wirelog_error_string(WIRELOG_ERR_EXEC)) {
        fprintf(stderr, "error strings are missing\n");
        return 1;
    }
    if (!wirelog_config_threads()) {
        fprintf(stderr, "threads config should be enabled in this build\n");
        return 1;
    }
    (void)wirelog_config_embedded();
    (void)wirelog_config_ipc();
    return 0;
}

static int
test_program_input_metadata_api(void)
{
    static const char source[] =
        "# .input Commented(filename=\"ignored.csv\")\n"
        ".decl NoInput(x: int32)\n"
        ".decl Text(s: string)\n"
        "Text(\".input Fake(...)\").\n"
        ".input Declared(IO=\"file\", filename=\"/definitely/missing/.input\")\n"
        ".input Declared(IO=\"metadata_probe\", filename=\"relative.csv\")\n"
        ".input Undeclared(IO=\"metadata_probe\", filename=\"/definitely/missing/undeclared.csv\")\n"
        ".input session_state(IO=\"metadata_probe\", filename=\"/definitely/missing/session.csv\")\n"
        ".input session_state(IO=\"metadata_probe\", filename=\"/definitely/missing/session2.csv\")\n"
        ".input CamelCase(IO=\"metadata_probe\", filename=\"camel.csv\")\n"
        ".input camelcase(IO=\"metadata_probe\", filename=\"lower.csv\")\n";
    wirelog_io_adapter_t adapter = {
        .abi_version = WIRELOG_IO_ABI_VERSION,
        .scheme = "metadata_probe",
        .read = metadata_probe_read,
    };

    if (wirelog_io_register_adapter(&adapter) != 0) {
        fprintf(stderr, "metadata probe adapter registration failed: %s\n",
            wirelog_io_last_error());
        return 1;
    }

    wirelog_error_t err = WIRELOG_ERR_UNKNOWN;
    wirelog_program_t *program = wirelog_parse_string(source, &err);
    int failures = 0;
    if (!program || err != WIRELOG_OK) {
        fprintf(stderr, "input metadata parse failed err=%d\n", err);
        failures = 1;
        goto cleanup;
    }

    const struct {
        const char *name;
        bool expected;
    } cases[] = {
        { "Declared", true },
        { "Undeclared", true },
        { "session_state", true },
        { "CamelCase", true },
        { "camelcase", true },
        { "CAMELCASE", false },
        { "Commented", false },
        { "literal .input", false },
        { "Fake", false },
        { "Unknown", false },
        { "NoInput", false },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        if (wirelog_program_relation_has_input(program, cases[i].name)
            != cases[i].expected) {
            fprintf(stderr, "unexpected .input metadata for '%s'\n",
                cases[i].name);
            failures = 1;
        }
    }
    if (wirelog_program_relation_has_input(program, NULL)) {
        fprintf(stderr, "NULL relation name should return false\n");
        failures = 1;
    }
    if (wirelog_program_relation_has_input(NULL, "Declared")) {
        fprintf(stderr, "NULL program should return false\n");
        failures = 1;
    }
    if (metadata_probe_reads != 0) {
        fprintf(stderr, "metadata query opened an adapter (%d reads)\n",
            metadata_probe_reads);
        failures = 1;
    }

cleanup:
    wirelog_program_free(program);
    if (wirelog_io_unregister_adapter("metadata_probe") != 0) {
        fprintf(stderr, "metadata probe adapter unregister failed\n");
        failures = 1;
    }
    return failures;
}

static int
test_optimizer_api(void)
{
    const char *src =
        ".decl edge(x:int32,y:int32)\n"
        ".decl path(x:int32,y:int32)\n"
        "edge(1,2).\n"
        "path(X,Y) :- edge(X,Y).\n";
    wirelog_error_t err = WIRELOG_ERR_UNKNOWN;
    wirelog_program_t *program = wirelog_parse_string(src, &err);
    if (!program || err != WIRELOG_OK) {
        fprintf(stderr, "parse failed err=%d\n", err);
        return 1;
    }

    wirelog_opt_config_t config = wirelog_optimizer_get_default_config();
    if (!config.enable_logic_fusion || !config.enable_join_ordering
        || !config.enable_semijoin) {
        fprintf(stderr, "default optimizer config is incomplete\n");
        wirelog_program_free(program);
        return 1;
    }
    if (!wirelog_optimize(program, &err) || err != WIRELOG_OK) {
        fprintf(stderr, "optimize failed err=%d\n", err);
        wirelog_program_free(program);
        return 1;
    }
    wirelog_opt_stats_t stats = { 0 };
    if (!wirelog_optimizer_get_stats(program, &stats)
        || stats.optimized_node_count == 0 || stats.passes_applied == 0) {
        fprintf(stderr, "optimizer stats were not recorded\n");
        wirelog_program_free(program);
        return 1;
    }
    if (wirelog_optimizer_cost_estimate(program) == 0) {
        fprintf(stderr, "cost estimate should be non-zero\n");
        wirelog_program_free(program);
        return 1;
    }

    wirelog_program_free(program);
    return 0;
}

static int
test_optimizer_config_disable_passes(void)
{
    const char *src =
        ".decl edge(x:int32,y:int32)\n"
        ".decl path(x:int32,y:int32)\n"
        "edge(1,2).\n"
        "path(X,Y) :- edge(X,Y).\n";
    wirelog_error_t err = WIRELOG_ERR_UNKNOWN;
    wirelog_program_t *program = wirelog_parse_string(src, &err);
    if (!program || err != WIRELOG_OK) {
        fprintf(stderr, "parse failed err=%d\n", err);
        return 1;
    }

    wirelog_opt_config_t config = wirelog_optimizer_get_default_config();
    config.enable_logic_fusion = false;
    config.enable_join_ordering = false;
    config.enable_semijoin = false;
    config.enable_subplan_sharing = false;
    config.enable_boolean_spec = false;

    if (!wirelog_optimize_with_config(program, &config, &err)
        || err != WIRELOG_OK) {
        fprintf(stderr, "optimize_with_config failed err=%d\n", err);
        wirelog_program_free(program);
        return 1;
    }

    wirelog_opt_stats_t stats = { 0 };
    if (!wirelog_optimizer_get_stats(program, &stats)
        || stats.passes_applied != 0) {
        fprintf(stderr, "disabled optimizer config applied %" PRIu32
            " pass(es)\n",
            stats.passes_applied);
        wirelog_program_free(program);
        return 1;
    }

    wirelog_program_free(program);
    return 0;
}

static int
test_bound_query_without_seed_preserves_answers(void)
{
    const char *src =
        ".decl edge(x:int32,y:int32)\n"
        ".decl path(x:int32,y:int32)\n"
        ".output path\n"
        ".query path(b,f) .\n"
        "edge(1,2).\n"
        "edge(2,3).\n"
        "edge(3,4).\n"
        "path(x,y) :- edge(x,y).\n"
        "path(x,y) :- edge(x,z), path(z,y).\n";
    wirelog_error_t err = WIRELOG_ERR_UNKNOWN;
    wirelog_program_t *program = wirelog_parse_string(src, &err);
    if (!program || err != WIRELOG_OK) {
        fprintf(stderr, "bound query parse failed err=%d\n", err);
        return 1;
    }

    if (!wirelog_optimize(program, &err) || err != WIRELOG_OK) {
        fprintf(stderr, "bound query optimize failed err=%d\n", err);
        wirelog_program_free(program);
        return 1;
    }

    wirelog_executor_t *executor = wirelog_executor_create(program, &err);
    if (!executor || err != WIRELOG_OK) {
        fprintf(stderr, "bound query executor create failed err=%d\n", err);
        wirelog_program_free(program);
        return 1;
    }

    wirelog_result_t *result = wirelog_evaluate(executor, &err);
    uint64_t rows = result
        ? wirelog_result_relation_cardinality(result, "path") : 0;
    if (!result || err != WIRELOG_OK || rows != 6) {
        fprintf(stderr,
            "bound query changed unseeded result: rows=%" PRIu64 " err=%d\n",
            rows, err);
        wirelog_result_free(result);
        wirelog_executor_free(executor);
        wirelog_program_free(program);
        return 1;
    }

    wirelog_result_free(result);
    wirelog_executor_free(executor);
    wirelog_program_free(program);
    return 0;
}

static int
test_executor_result_api(void)
{
    const char *src =
        ".decl edge(x:int32,y:int32)\n"
        ".decl path(x:int32,y:int32)\n"
        "edge(1,2).\n"
        "path(X,Y) :- edge(X,Y).\n";
    wirelog_error_t err = WIRELOG_ERR_UNKNOWN;
    wirelog_program_t *program = wirelog_parse_string(src, &err);
    if (!program || err != WIRELOG_OK) {
        fprintf(stderr, "parse failed err=%d\n", err);
        return 1;
    }

    wirelog_executor_t *executor = wirelog_executor_create(program, &err);
    if (!executor || err != WIRELOG_OK) {
        fprintf(stderr, "executor create failed err=%d\n", err);
        wirelog_program_free(program);
        return 1;
    }
    if (wirelog_load_all_facts(program, program) != -1
        || wirelog_load_input_files(program, program) != -1) {
        fprintf(stderr, "legacy loaders accepted a non-executor worker\n");
        wirelog_executor_free(executor);
        wirelog_program_free(program);
        return 1;
    }

    wirelog_result_t *result = wirelog_evaluate(executor, &err);
    if (!result || err != WIRELOG_OK) {
        fprintf(stderr, "evaluate failed err=%d\n", err);
        wirelog_executor_free(executor);
        wirelog_program_free(program);
        return 1;
    }

    uint64_t rows = wirelog_result_relation_cardinality(result, "path");
    const int64_t *path
        = (const int64_t *)wirelog_result_get_relation(result, "path");
    if (rows != 1 || !path || path[0] != 1 || path[1] != 2) {
        fprintf(stderr, "unexpected path result rows=%" PRIu64 "\n", rows);
        wirelog_result_free(result);
        wirelog_executor_free(executor);
        wirelog_program_free(program);
        return 1;
    }

    wirelog_result_free(result);
    wirelog_executor_free(executor);
    wirelog_program_free(program);
    return 0;
}

int
main(void)
{
    int failures = 0;
    failures += test_utility_api();
    failures += test_program_input_metadata_api();
    failures += test_optimizer_api();
    failures += test_optimizer_config_disable_passes();
    failures += test_bound_query_without_seed_preserves_answers();
    failures += test_executor_result_api();
    if (failures == 0)
        printf("test_wirelog_public_api: OK\n");
    return failures == 0 ? 0 : 1;
}
