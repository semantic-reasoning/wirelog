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
#include "io/csv_adapter_internal.h"
#include "io/io_ctx_internal.h"
#include "ir/program.h"
#include "intern.h"
#include "columnar/memory_governor.h"
#include "wirelog-internal.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define WL_SESSION_INPUT_BATCH_ROWS 1024u

typedef struct {
    wl_session_t *session;
    int64_t *rows;
    uint32_t nrows;
    uint32_t capacity;
    uint32_t ncols;
    wl_columnar_memory_reservation_t reservations[2];
    unsigned active_reservation;
    bool reserved;
    int error;
} wl_input_batch_sink_t;

static int
wl_session_input_batch_cb(void *opaque, const int64_t *rows,
    uint32_t nrows, uint32_t ncols)
{
    wl_input_batch_sink_t *sink = (wl_input_batch_sink_t *)opaque;
    if (!sink || !sink->session || !rows || !nrows || !ncols
        || (sink->ncols && sink->ncols != ncols))
        return sink ? (sink->error = WL_CSV_ERR_ARGS) : WL_CSV_ERR_ARGS;
    if (nrows > UINT32_MAX - sink->nrows) {
        sink->error = WL_CSV_ERR_OVERFLOW;
        return sink->error;
    }
    uint32_t needed = sink->nrows + nrows;
    if (needed > sink->capacity) {
        uint32_t capacity = sink->capacity ? sink->capacity
            : WL_SESSION_INPUT_BATCH_ROWS;
        while (capacity < needed) {
            if (capacity > UINT32_MAX / 2u) {
                capacity = needed;
                break;
            }
            capacity *= 2u;
        }
        uint64_t cells, bytes;
        if (!wl_columnar_memory_size_mul(capacity, ncols, &cells)
            || !wl_columnar_memory_size_mul(cells, sizeof(int64_t), &bytes)
            || bytes > SIZE_MAX) {
            sink->error = WL_CSV_ERR_OVERFLOW;
            return sink->error;
        }
        unsigned replacement_slot = sink->active_reservation ^ 1u;
        wl_columnar_memory_reservation_t *replacement
            = &sink->reservations[replacement_slot];
        wl_columnar_memory_reservation_init(replacement);
        wl_columnar_memory_governor_t *governor
            = wl_session_memory_governor(sink->session);
        if (governor) {
            wl_columnar_memory_admission_status_t status
                = wl_columnar_memory_reserve_checked(governor, bytes,
                    replacement);
            if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                sink->error = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                    ? WL_CSV_ERR_BUDGET : status ==
                    WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                    ? WL_CSV_ERR_OVERFLOW
                    : WL_CSV_ERR_ARGS;
                return sink->error;
            }
        }
        int64_t *grown = realloc(sink->rows, (size_t)bytes);
        if (!grown) {
            if (governor && !wl_columnar_memory_release(replacement))
                abort();
            sink->error = WL_CSV_ERR_MEMORY;
            return sink->error;
        }
        if (sink->reserved
            && !wl_columnar_memory_release(
                &sink->reservations[sink->active_reservation]))
            abort();
        sink->rows = grown;
        sink->capacity = capacity;
        sink->ncols = ncols;
        sink->active_reservation = replacement_slot;
        sink->reserved = governor != NULL;
    }
    memcpy(sink->rows + (size_t)sink->nrows * ncols, rows,
        (size_t)nrows * ncols * sizeof(*rows));
    sink->nrows = needed;
    return 0;
}

static int
wl_session_input_load_fail(wl_session_t *sess, int rc)
{
    if (sess)
        sess->input_load_failed = true;
    if (rc == WL_ERR_MEMORY_BUDGET)
        return rc;
    return rc > 0 ? rc : -1;
}

int
wl_session_load_facts(wl_session_t *sess, const struct wirelog_program *prog)
{
    if (!sess || !prog)
        return -1;
    if (sess->input_load_failed)
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
            return wl_session_input_load_fail(sess, rc);
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
    if (sess->input_load_failed)
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
            return wl_session_input_load_fail(sess, -1);
        }

        wirelog_io_ctx_t *ctx =
            wirelog_io_ctx_create_for_relation(rel, prog->intern);
        if (!ctx) {
            fprintf(stderr,
                "error: failed to create I/O context for '%s'\n",
                rel->name);
            return wl_session_input_load_fail(sess, -1);
        }
        wirelog_io_ctx_set_memory_governor(ctx,
            wl_session_memory_governor(sess));

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
                return wl_session_input_load_fail(sess, -1);
            }
        }

        if (adapter == &wl_csv_adapter) {
            wl_input_batch_sink_t sink = {
                .session = sess,
            };
            wl_columnar_memory_reservation_init(&sink.reservations[0]);
            wl_columnar_memory_reservation_init(&sink.reservations[1]);
            int src = wl_csv_adapter_stream_read(ctx,
                    WL_SESSION_INPUT_BATCH_ROWS,
                    wl_session_input_batch_cb, &sink);
            int load_rc = 0;
            if (src == 0 && sink.nrows != 0)
                load_rc = wl_session_insert(sess, rel->name, sink.rows,
                        sink.nrows, sink.ncols);
            wirelog_io_ctx_destroy(ctx);
            if (src != 0 || load_rc != 0) {
                if (src != 0) {
                    int csv_rc = sink.error ? sink.error : src;
                    if (csv_rc == WL_CSV_ERR_BUDGET)
                        load_rc = WL_ERR_MEMORY_BUDGET;
                    else if (csv_rc == WL_CSV_ERR_MEMORY)
                        load_rc = ENOMEM;
                    else if (csv_rc == WL_CSV_ERR_OVERFLOW)
                        load_rc = EOVERFLOW;
                    else
                        load_rc = -1;
                } else if (load_rc == WL_INTERN_ERR_MEMORY_BUDGET) {
                    load_rc = WL_ERR_MEMORY_BUDGET;
                }
                free(sink.rows);
                if (sink.reserved
                    && !wl_columnar_memory_release(
                        &sink.reservations[sink.active_reservation]))
                    abort();
                return wl_session_input_load_fail(sess, load_rc);
            }
            free(sink.rows);
            if (sink.reserved
                && !wl_columnar_memory_release(
                    &sink.reservations[sink.active_reservation]))
                abort();
            continue;
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
            return wl_session_input_load_fail(sess, rc == ENOMEM
                ? ENOMEM : -1);
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
                return wl_session_input_load_fail(sess, rc);
            }
        } else {
            free(data);
        }
    }

    return 0;
}
