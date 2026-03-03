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

#include "../ffi/dd_ffi.h"
#include "../intern.h"
#include "../ir/program.h"
#include "../passes/fusion.h"
#include "../passes/jpp.h"
#include "../passes/sip.h"
#include "../wirelog.h"

#include <inttypes.h>
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

typedef struct {
    FILE *out;
    const wirelog_program_t *prog;
    const wl_intern_t *intern;
    bool has_any_output; /* true if any relation has .output */
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

int
wl_run_pipeline(const char *source, uint32_t num_workers, FILE *out)
{
    if (!source || !out)
        return -1;

    /* 1. Parse */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return -1;

    /* 2. Optimize */
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    /* 3. Generate DD plan */
    wl_ffi_dd_plan_t *dd_plan = NULL;
    int rc = wl_ffi_dd_plan_generate(prog, &dd_plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    /* 3. Marshal to FFI */
    wl_ffi_plan_t *ffi = NULL;
    rc = wl_dd_marshal_plan(dd_plan, &ffi);
    if (rc != 0) {
        wl_ffi_dd_plan_free(dd_plan);
        wirelog_program_free(prog);
        return -1;
    }

    /* 4. Create worker and load inline facts */
    wl_dd_worker_t *w = wl_dd_worker_create(num_workers);
    if (!w) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wirelog_load_all_facts(prog, w);
    if (rc != 0) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        wirelog_program_free(prog);
        return -1;
    }

    /* 4b. Load external CSV files from .input directives */
    rc = wirelog_load_input_files(prog, w);
    if (rc != 0) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
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

    /* 6. Execute with type-aware output callback */
    wl_output_ctx_t ctx = {
        .out = out,
        .prog = prog,
        .intern = prog->intern,
        .has_any_output = has_any_output,
    };
    rc = wl_dd_execute_cb(ffi, w, print_tuple_cb, &ctx);

    /* 6. Cleanup */
    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(dd_plan);
    wirelog_program_free(prog);

    return rc;
}
