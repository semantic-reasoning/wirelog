/*
 * driver.c - wirelog CLI Driver Utilities
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements CLI driver utilities: file reading, pipeline execution,
 * and output formatting.
 */

#include "driver.h"

#include "../backend.h"
#include "../exec_plan_gen.h"
#include "../intern.h"
#include "../ir/program.h"
#include "../passes/fusion.h"
#include "../passes/jpp.h"
#include "../passes/sip.h"
#include "../session.h"
#include "../session_facts.h"
#include "../wirelog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

char *
wl_read_file(const char *path)
{
    if (!path)
        return NULL;

    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;

    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }

    long len = ftell(f);
    if (len < 0) {
        fclose(f);
        return NULL;
    }

    rewind(f);

    char *buf = (char *)malloc((size_t)len + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }

    size_t read = fread(buf, 1, (size_t)len, f);
    fclose(f);

    buf[read] = '\0';
    return buf;
}

void
wl_print_tuple(const char *relation, const int64_t *row, uint32_t ncols,
               FILE *out)
{
    fprintf(out, "%s(", relation);
    for (uint32_t i = 0; i < ncols; i++) {
        if (i > 0)
            fprintf(out, ", ");
        fprintf(out, "%" PRId64, row[i]);
    }
    fprintf(out, ")\n");
}

/* ======================================================================== */
/* Type-aware output callback                                               */
/* ======================================================================== */

/*
 * Per-relation file accumulator for .output(filename=...) support.
 * Holds the open FILE* and relation name so the snapshot callback can write
 * matching tuples directly to the file in CSV format.
 */
typedef struct {
    const char *relation; /* borrowed pointer to relation name */
    FILE *file;           /* open for writing, NULL if open failed */
} wl_output_file_entry_t;

typedef struct {
    FILE *out;
    const wirelog_program_t *prog;
    const wl_intern_t *intern;
    bool has_any_output; /* true if any relation has .output */
    /* Per-relation output files (for .output(filename=...)) */
    wl_output_file_entry_t *output_files;
    uint32_t output_file_count;
} wl_output_ctx_t;

/* Find the relation info for a given relation name */
static const wl_ir_relation_info_t *
find_relation(const wirelog_program_t *prog, const char *name)
{
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, name) == 0)
            return &prog->relations[i];
    }
    return NULL;
}

/* Write a single tuple as a CSV row to the given file */
static void
write_tuple_csv(FILE *f, const wl_ir_relation_info_t *rel,
                const wl_intern_t *intern, const int64_t *row, uint32_t ncols)
{
    for (uint32_t i = 0; i < ncols; i++) {
        if (i > 0)
            fprintf(f, ",");

        if (rel && i < rel->column_count
            && rel->columns[i].type == WIRELOG_TYPE_STRING && intern) {
            const char *str = wl_intern_reverse(intern, row[i]);
            if (str)
                fprintf(f, "%s", str);
            else
                fprintf(f, "%" PRId64, row[i]);
        } else {
            fprintf(f, "%" PRId64, row[i]);
        }
    }
    fprintf(f, "\n");
}

/* Callback adapter: type-aware output with string reverse-mapping */
static void
print_tuple_cb(const char *relation, const int64_t *row, uint32_t ncols,
               void *user_data)
{
    wl_output_ctx_t *ctx = (wl_output_ctx_t *)user_data;
    const wl_ir_relation_info_t *rel = NULL;

    if (ctx->prog)
        rel = find_relation(ctx->prog, relation);

    /* Filter: if any relation has .output, only print those */
    if (ctx->has_any_output && rel && !rel->has_output)
        return;

    /* Write to per-relation output file if configured */
    if (rel && rel->output_file) {
        for (uint32_t i = 0; i < ctx->output_file_count; i++) {
            if (ctx->output_files[i].relation
                && strcmp(ctx->output_files[i].relation, relation) == 0
                && ctx->output_files[i].file) {
                write_tuple_csv(ctx->output_files[i].file, rel, ctx->intern,
                                row, ncols);
                break;
            }
        }
        /* Relations with output_file do not print to stdout */
        return;
    }

    fprintf(ctx->out, "%s(", relation);
    for (uint32_t i = 0; i < ncols; i++) {
        if (i > 0)
            fprintf(ctx->out, ", ");

        if (rel && i < rel->column_count
            && rel->columns[i].type == WIRELOG_TYPE_STRING && ctx->intern) {
            const char *str = wl_intern_reverse(ctx->intern, row[i]);
            if (str)
                fprintf(ctx->out, "\"%s\"", str);
            else
                fprintf(ctx->out, "%" PRId64, row[i]);
        } else {
            fprintf(ctx->out, "%" PRId64, row[i]);
        }
    }
    fprintf(ctx->out, ")\n");
}

/* ======================================================================== */
/* Delta callback for delta-query mode                                      */
/* ======================================================================== */

static void
delta_tuple_cb(const char *relation, const int64_t *row, uint32_t ncols,
               int32_t diff, void *user_data)
{
    wl_output_ctx_t *ctx = (wl_output_ctx_t *)user_data;
    const wl_ir_relation_info_t *rel = NULL;

    if (ctx->prog)
        rel = find_relation(ctx->prog, relation);

    /* Print delta prefix: + for insertion (diff > 0), - for retraction (diff < 0) */
    fprintf(ctx->out, "%s%s(", diff > 0 ? "+" : "-", relation);
    for (uint32_t i = 0; i < ncols; i++) {
        if (i > 0)
            fprintf(ctx->out, ", ");

        if (rel && i < rel->column_count
            && rel->columns[i].type == WIRELOG_TYPE_STRING && ctx->intern) {
            const char *str = wl_intern_reverse(ctx->intern, row[i]);
            if (str)
                fprintf(ctx->out, "\"%s\"", str);
            else
                fprintf(ctx->out, "%" PRId64, row[i]);
        } else {
            fprintf(ctx->out, "%" PRId64, row[i]);
        }
    }
    fprintf(ctx->out, ")\n");
}

int
wl_run_pipeline(const char *source, uint32_t num_workers, bool delta_mode,
                FILE *out)
{
    if (!source || !out)
        return -1;

    /* 1. Parse */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog) {
        fprintf(stderr, "Parse error\n");
        return -1;
    }

    /* 2. Optimize */
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    /* 3. Generate execution plan */
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        fprintf(stderr, "Plan generation failed: rc=%d\n", rc);
        wirelog_program_free(prog);
        return -1;
    }

    /* 4. Create columnar session */
    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    /* 4a. Load inline facts */
    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    /* 4b. Load external CSV files from .input directives */
    rc = wl_session_load_input_files(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    /* 5. Check if any relation has .output directive */
    bool has_any_output = false;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].has_output) {
            has_any_output = true;
            break;
        }
    }

    /* 5a. Open per-relation output files for .output(filename=...) */
    wl_output_file_entry_t *output_files = NULL;
    uint32_t output_file_count = 0;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].has_output && prog->relations[i].output_file)
            output_file_count++;
    }

    if (output_file_count > 0) {
        output_files = (wl_output_file_entry_t *)calloc(
            output_file_count, sizeof(wl_output_file_entry_t));
        if (!output_files) {
            wl_session_destroy(sess);
            wl_plan_free(plan);
            wirelog_program_free(prog);
            return -1;
        }
        uint32_t fi = 0;
        for (uint32_t i = 0; i < prog->relation_count; i++) {
            wl_ir_relation_info_t *rel = &prog->relations[i];
            if (!rel->has_output || !rel->output_file)
                continue;
            output_files[fi].relation = rel->name;
            output_files[fi].file = fopen(rel->output_file, "w");
            if (!output_files[fi].file) {
                fprintf(stderr,
                        "error: cannot open output file '%s' for '%s'\n",
                        rel->output_file, rel->name);
                /* Close already-opened files and fail */
                for (uint32_t j = 0; j < fi; j++) {
                    if (output_files[j].file)
                        fclose(output_files[j].file);
                }
                free(output_files);
                wl_session_destroy(sess);
                wl_plan_free(plan);
                wirelog_program_free(prog);
                return -1;
            }
            fi++;
        }
    }

    /* 6. Execute with type-aware output callback */
    wl_output_ctx_t ctx = {
        .out = out,
        .prog = prog,
        .intern = prog->intern,
        .has_any_output = has_any_output,
        .output_files = output_files,
        .output_file_count = output_file_count,
    };

    if (delta_mode) {
        /* Delta mode: register delta callback and step through incremental evaluation */
        wl_session_set_delta_cb(sess, delta_tuple_cb, &ctx);
        rc = wl_session_step(sess);
    } else {
        /* Standard mode: snapshot entire state */
        rc = wl_session_snapshot(sess, print_tuple_cb, &ctx);
    }

    /* 7. Close per-relation output files */
    for (uint32_t i = 0; i < output_file_count; i++) {
        if (output_files[i].file)
            fclose(output_files[i].file);
    }
    free(output_files);

    /* 8. Cleanup */
    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    return rc;
}
