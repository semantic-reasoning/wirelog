/*
 * session_facts.c - wirelog Fact-Loading Session Helpers
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Backend-agnostic fact-loading helpers that replace the deleted
 * DD-specific wirelog_load_all_facts() and wirelog_load_input_files().
 *
 * As of #458 the .input path delegates to the I/O adapter registry
 * instead of hard-coding CSV logic.  session_facts.c is now
 * scheme-agnostic; CSV-specific concerns live in csv_adapter.c.
 */

#include "session_facts.h"

#include "io/io_adapter.h"
#include "io/io_ctx_internal.h"
#include "ir/program.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int
wl_session_load_facts(wl_session_t *sess, const struct wirelog_program *prog)
{
    if (!sess || !prog)
        return -1;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &prog->relations[i];
        if (!rel->name || rel->fact_count == 0 || !rel->fact_data)
            continue;

        /* Physical width, not rel->column_count (#985): fact_data is packed
         * one slot per written argument, and validate_fact_arities() has
         * already required that count to be the physical width.  An inline
         * compound column occupies compound_arity slots, so the logical
         * count would insert at a shorter stride than the buffer holds. */
        int rc = wl_session_insert(sess, rel->name, rel->fact_data,
                rel->fact_count, wl_ir_relation_physical_width(rel));
        if (rc != 0) {
            fprintf(stderr, "error: failed to load facts for '%s'\n",
                rel->name);
            return -1;
        }
    }

    return 0;
}

int
wl_session_load_input_files(wl_session_t *sess,
    const struct wirelog_program *prog)
{
    if (!sess || !prog)
        return -1;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &prog->relations[i];
        if (!rel->name || !rel->has_input)
            continue;

        /* Determine I/O scheme: explicit io="..." or default "csv" */
        const char *scheme = rel->input_io_scheme ? rel->input_io_scheme
                                                  : "csv";

        const wirelog_io_adapter_t *adapter = wirelog_io_find_adapter(scheme);
        if (!adapter) {
            fprintf(stderr,
                "error: no I/O adapter registered for scheme '%s' "
                "(relation '%s')\n",
                scheme, rel->name);
            return -1;
        }

        wirelog_io_ctx_t *ctx =
            wirelog_io_ctx_create_for_relation(rel, prog->intern);
        if (!ctx) {
            fprintf(stderr,
                "error: failed to create I/O context for '%s'\n",
                rel->name);
            return -1;
        }

        /* Optional validation pass */
        if (adapter->validate) {
            char errbuf[512];
            errbuf[0] = '\0';
            int vrc = adapter->validate(ctx, errbuf, sizeof errbuf,
                    adapter->user_data);
            if (vrc != 0) {
                fprintf(stderr,
                    "error: validation failed for '%s': %s\n",
                    rel->name, errbuf);
                wirelog_io_ctx_destroy(ctx);
                return -1;
            }
        }

        /* Delegate to adapter's read callback */
        int64_t *data = NULL;
        uint32_t nrows = 0;

        int rc = adapter->read(ctx, &data, &nrows, adapter->user_data);
        wirelog_io_ctx_destroy(ctx);

        if (rc != 0) {
            fprintf(stderr,
                "error: adapter '%s' failed to read data for '%s'\n",
                scheme, rel->name);
            free(data);
            return -1;
        }

        if (nrows > 0 && data) {
            /* Physical width, matching wirelog_io_ctx_num_cols() (#985):
             * the adapter contract sizes this buffer at
             * nrows * wirelog_io_ctx_num_cols(ctx), and io_ctx.c sets that
             * from the physical width. */
            rc = wl_session_insert(sess, rel->name, data, nrows,
                    wl_ir_relation_physical_width(rel));
            free(data);
            if (rc != 0) {
                fprintf(stderr,
                    "error: failed to insert data for '%s'\n",
                    rel->name);
                return -1;
            }
        } else {
            free(data);
        }
    }

    return 0;
}
