/*
 * wirelog-easy.c - wirelog convenience facade (Issue #441)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */

#include "wirelog/wirelog-easy.h"

#include "wirelog/backend.h"
#include "wirelog/exec_plan.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/intern.h"
#include "wirelog/ir/program.h"
#include "wirelog/session.h"
#include "wirelog/session_facts.h"
#include "wirelog/wirelog-parser.h"
#include "wirelog/wirelog-types.h"

#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define WIRELOG_EASY_MAX_COLS 16

struct wirelog_easy_session {
    wirelog_program_t *prog; /* owns */
    wl_plan_t *plan;         /* owns, lazy-built */
    wl_session_t *session;   /* owns, lazy-built */
    /* Non-owning alias of prog's intern table.  See delta_demo.c:93-97
     * for the rationale: wirelog_program_get_intern() returns const, but
     * wl_intern_put() needs a mutable pointer; the program owns the
     * intern's lifetime so the cast is safe. */
    wl_intern_t *intern_mut;
    uint32_t num_workers;
    bool plan_built; /* true once first wirelog_easy_step-class call ran */
    wirelog_extension_snapshot_t *extension_snapshot; /* owns one reference */
};

static bool
easy_relation_has_float(const wirelog_easy_session_t *s, const char *relation)
{
    if (!s || !s->prog || !relation)
        return false;
    for (uint32_t i = 0; i < s->prog->relation_count; i++) {
        const wl_ir_relation_info_t *info = &s->prog->relations[i];
        if (!info->name || strcmp(info->name, relation) != 0)
            continue;
        if (info->has_float_compound_slots)
            return true;
        for (uint32_t c = 0; c < info->column_count; c++)
            if (info->columns[c].type == WIRELOG_TYPE_FLOAT)
                return true;
        return false;
    }
    return false;
}

static bool
easy_program_has_float(const wirelog_easy_session_t *s)
{
    if (!s || !s->prog)
        return false;
    for (uint32_t i = 0; i < s->prog->relation_count; i++) {
        const wl_ir_relation_info_t *info = &s->prog->relations[i];
        if (info->has_float_compound_slots)
            return true;
        for (uint32_t c = 0; c < info->column_count; c++)
            if (info->columns[c].type == WIRELOG_TYPE_FLOAT)
                return true;
    }
    return false;
}

/* ======================================================================== */
/* Internal: lazy plan/session build                                        */
/* ======================================================================== */

static wirelog_error_t
ensure_plan_built(wirelog_easy_session_t *s, uint32_t num_workers)
{
    if (!s)
        return WIRELOG_ERR_EXEC;
    if (s->plan_built)
        return WIRELOG_OK;

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(s->prog, &plan);
    if (rc != 0 || !plan) {
        if (plan)
            wl_plan_free(plan);
        return WIRELOG_ERR_INVALID_IR;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create_with_snapshot(wl_backend_columnar(), plan,
            num_workers > 0 ? num_workers : 1, s->extension_snapshot,
            &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        return (rc == ENOMEM) ? WIRELOG_ERR_MEMORY : WIRELOG_ERR_EXEC;
    }

    /* Issue #718: seed inline `.dl` facts into the freshly built session
     * so snapshots and IDB derivations observe them.  This mirrors the
     * fact-load step of the CLI driver's session-build sequence.  The
     * optimizer pipeline is shared with the CLI via wirelog_optimize().
     * The load runs exactly once on the lazy
     * plan/session boundary, before any host delta callback can be
     * installed, so col_session_insert takes the non-incremental path:
     * no spurious static deltas on the first step() and no outer-epoch
     * advance. */
    rc = wl_session_load_facts(session, s->prog);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        return WIRELOG_ERR_EXEC;
    }

    s->plan = plan;
    s->session = session;
    s->plan_built = true;
    return WIRELOG_OK;
}

/* ======================================================================== */
/* Lifecycle                                                                */
/* ======================================================================== */

wirelog_error_t
wirelog_easy_open_opts(const char *dl_src, const wirelog_easy_open_opts_t *opts,
    wirelog_easy_session_t **out)
{
    if (!dl_src || !out)
        return WIRELOG_ERR_EXEC;
    *out = NULL;

    const size_t old_opts_size = offsetof(wirelog_easy_open_opts_t,
            extension_snapshot);
    const size_t snapshot_end = old_opts_size
        + sizeof(((wirelog_easy_open_opts_t *)0)->extension_snapshot);
    if (opts && opts->size < old_opts_size)
        return WIRELOG_ERR_EXEC;
    if (opts && opts->_reserved)
        return WIRELOG_ERR_EXEC;

    wirelog_extension_snapshot_t *snapshot = NULL;
    if (opts && opts->size >= snapshot_end)
        snapshot = opts->extension_snapshot;

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(dl_src, &err);
    if (!prog)
        return (err != WIRELOG_OK) ? err : WIRELOG_ERR_PARSE;

    err = WIRELOG_OK;
    if (!wirelog_optimize(prog, &err)) {
        fprintf(stderr, "wirelog_easy_open: optimize failed (err=%d)\n", err);
        wirelog_program_free(prog);
        return (err != WIRELOG_OK) ? err : WIRELOG_ERR_EXEC;
    }

    wirelog_easy_session_t *s
        = (wirelog_easy_session_t *)calloc(1, sizeof(wirelog_easy_session_t));
    if (!s) {
        wirelog_program_free(prog);
        return WIRELOG_ERR_MEMORY;
    }
    s->prog = prog;
    s->intern_mut = (wl_intern_t *)wirelog_program_get_intern(prog);
    s->plan = NULL;
    s->session = NULL;
    s->num_workers = opts ? opts->num_workers : 0;
    s->plan_built = false;
    s->extension_snapshot = snapshot;
    if (s->extension_snapshot)
        wirelog_extension_snapshot_retain(s->extension_snapshot);

    if (opts && opts->eager_build) {
        err = ensure_plan_built(s, s->num_workers);
        if (err != WIRELOG_OK) {
            if (s->session)
                wl_session_destroy(s->session);
            if (s->plan)
                wl_plan_free(s->plan);
            wirelog_program_free(prog);
            wirelog_extension_snapshot_release(s->extension_snapshot);
            free(s);
            return err;
        }
    }

    *out = s;
    return WIRELOG_OK;
}

wirelog_error_t
wirelog_easy_open(const char *dl_src, wirelog_easy_session_t **out)
{
    return wirelog_easy_open_opts(dl_src, NULL, out);
}

void
wirelog_easy_close(wirelog_easy_session_t *s)
{
    if (!s)
        return;
    if (s->session)
        wl_session_destroy(s->session);
    if (s->plan)
        wl_plan_free(s->plan);
    if (s->prog)
        wirelog_program_free(s->prog);
    wirelog_extension_snapshot_release(s->extension_snapshot);
    free(s);
}

/* ======================================================================== */
/* Intern                                                                   */
/* ======================================================================== */

int64_t
wirelog_easy_intern(wirelog_easy_session_t *s, const char *sym)
{
    if (!s || !sym || !s->intern_mut)
        return -1;
    /* The program's intern table is aliased all the way through plan and
     * session (see exec_plan_gen.c: plan->intern = prog->intern and
     * columnar/session.c: sess->intern = plan->intern), so a newly interned
     * symbol is immediately visible to any already-built session.  Interning
     * at any time is therefore safe; wl_intern_put() returns the existing id
     * for a repeat symbol, so ids remain stable across the run. */
    return wl_intern_put(s->intern_mut, sym);
}

wirelog_error_t
wirelog_easy_make_compound(wirelog_easy_session_t *s, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args, uint64_t *handle_out)
{
    if (handle_out)
        *handle_out = WIRELOG_COMPOUND_HANDLE_NULL;
    if (!s || !functor || !args || arity == 0 || !handle_out)
        return WIRELOG_ERR_EXEC;
    for (uint32_t i = 0; i < arity; i++)
        if (args[i].type == WIRELOG_TYPE_FLOAT)
            return WIRELOG_ERR_EXEC;

    wirelog_error_t err = ensure_plan_built(s, s->num_workers);
    if (err != WIRELOG_OK)
        return err;

    int rc = wl_session_make_compound(s->session, functor, arity, args,
            handle_out);
    if (rc == 0)
        return WIRELOG_OK;
    if (rc == ENOSPC)
        return WIRELOG_ERR_COMPOUND_SATURATED;
    if (rc == EBUSY)
        return WIRELOG_ERR_COMPOUND_BUSY;
    if (rc == ENOMEM)
        return WIRELOG_ERR_MEMORY;
    if (handle_out)
        *handle_out = 0;
    return WIRELOG_ERR_EXEC;
}

/* ======================================================================== */
/* Insert / Remove                                                          */
/* ======================================================================== */

wirelog_error_t
wirelog_easy_insert(wirelog_easy_session_t *s, const char *relation,
    const int64_t *row,
    uint32_t ncols)
{
    if (!s || !relation || !row)
        return WIRELOG_ERR_EXEC;
    if (easy_relation_has_float(s, relation))
        return WIRELOG_ERR_EXEC;
    wirelog_error_t err = ensure_plan_built(s, s->num_workers);
    if (err != WIRELOG_OK)
        return err;
    int rc = wl_session_insert(s->session, relation, row, 1, ncols);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wirelog_easy_remove(wirelog_easy_session_t *s, const char *relation,
    const int64_t *row,
    uint32_t ncols)
{
    if (!s || !relation || !row)
        return WIRELOG_ERR_EXEC;
    if (easy_relation_has_float(s, relation))
        return WIRELOG_ERR_EXEC;
    wirelog_error_t err = ensure_plan_built(s, s->num_workers);
    if (err != WIRELOG_OK)
        return err;
    int rc = wl_session_remove(s->session, relation, row, 1, ncols);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

/* ======================================================================== */
/* Variadic symbol helpers                                                  */
/* ======================================================================== */

static wirelog_error_t
collect_sym_row(wirelog_easy_session_t *s, va_list ap, int64_t *row,
    uint32_t *ncols)
{
    uint32_t n = 0;
    while (n < WIRELOG_EASY_MAX_COLS) {
        const char *sym = va_arg(ap, const char *);
        if (sym == NULL)
            break;
        /* Interning a never-seen symbol after plan build is rejected by
         * wirelog_easy_intern, but if the caller pre-interned the symbol via
         * wirelog_easy_intern() before the first step, wl_intern_put() simply
         * returns the existing id. */
        int64_t id = wl_intern_put(s->intern_mut, sym);
        if (id < 0)
            return WIRELOG_ERR_EXEC;
        row[n++] = id;
    }
    *ncols = n;
    return WIRELOG_OK;
}

wirelog_error_t
wirelog_easy_insert_sym(wirelog_easy_session_t *s, const char *relation, ...)
{
    if (!s || !relation)
        return WIRELOG_ERR_EXEC;
    int64_t row[WIRELOG_EASY_MAX_COLS];
    uint32_t ncols = 0;
    va_list ap;
    va_start(ap, relation);
    wirelog_error_t err = collect_sym_row(s, ap, row, &ncols);
    va_end(ap);
    if (err != WIRELOG_OK)
        return err;
    return wirelog_easy_insert(s, relation, row, ncols);
}

wirelog_error_t
wirelog_easy_remove_sym(wirelog_easy_session_t *s, const char *relation, ...)
{
    if (!s || !relation)
        return WIRELOG_ERR_EXEC;
    int64_t row[WIRELOG_EASY_MAX_COLS];
    uint32_t ncols = 0;
    va_list ap;
    va_start(ap, relation);
    wirelog_error_t err = collect_sym_row(s, ap, row, &ncols);
    va_end(ap);
    if (err != WIRELOG_OK)
        return err;
    return wirelog_easy_remove(s, relation, row, ncols);
}

/* ======================================================================== */
/* Step / set_delta_cb                                                      */
/* ======================================================================== */

wirelog_error_t
wirelog_easy_step(wirelog_easy_session_t *s)
{
    if (!s)
        return WIRELOG_ERR_EXEC;
    wirelog_error_t err = ensure_plan_built(s, s->num_workers);
    if (err != WIRELOG_OK)
        return err;
    int rc = wl_session_step(s->session);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wirelog_easy_set_delta_cb(wirelog_easy_session_t *s, wirelog_on_delta_fn cb,
    void *user_data)
{
    if (!s)
        return WIRELOG_ERR_EXEC;
    if (cb && easy_program_has_float(s))
        return WIRELOG_ERR_EXEC;
    wirelog_error_t err = ensure_plan_built(s, s->num_workers);
    if (err != WIRELOG_OK)
        return err;
    wl_session_set_delta_cb(s->session, cb, user_data);
    return WIRELOG_OK;
}

/* ======================================================================== */
/* Print delta                                                              */
/* ======================================================================== */

static bool
column_is_string(wirelog_column_type_t t)
{
    return t == WIRELOG_TYPE_STRING;
}

void
wirelog_easy_print_delta(const char *relation, const int64_t *row,
    uint32_t ncols,
    int32_t diff, void *user_data)
{
    wirelog_easy_session_t *s = (wirelog_easy_session_t *)user_data;
    if (!s || !relation || !row) {
        fprintf(stderr, "wirelog_easy_print_delta: NULL argument\n");
        abort();
    }

    char sign = (diff > 0) ? '+' : '-';

    /* Schema lookup may fail (unknown relation or arity mismatch).  The
     * abort-on-missed-reverse-intern policy applies ONLY when the schema
     * explicitly declares a column as STRING — otherwise we have no
     * warrant to treat an integer id as a pointer into the intern table.
     * In the schema-unavailable branch, default to integer rendering so a
     * synthetic delta call (or a schema-less relation) does not trip the
     * abort.
     *
     * The ncols equality is a *logical* schema against a *physical* row
     * width.  The two agree for every relation that declares no `inline`
     * compound column; where one is declared they can differ, and then the
     * equality fails: `.decl pred(id: int64, payload: f/2 inline)` reports
     * column_count 2 while the row carries 3 slots (#985).  (An `inline`
     * compound of arity 1 is one declared column and one slot, so the
     * equality still holds there -- an inline compound column is necessary
     * for the mismatch, not sufficient.)  A relation whose widths do differ
     * therefore always renders as raw integers here, including any genuine
     * `symbol` column beside the compound.  That is the safe direction of
     * the degradation -- no abort, no id misread as a pointer -- and it is
     * left deliberate rather than accidental: rendering per column would
     * need the logical-to-physical slot map, which this printer does not
     * have.  Not to be "fixed" by comparing against a physical width; that
     * would index schema->columns[] physically, which is wrong. */
    const wirelog_schema_t *schema
        = wirelog_program_get_schema(s->prog, relation);
    bool have_schema = (schema != NULL && schema->columns != NULL
        && schema->column_count == ncols);

    printf("%c %s(", sign, relation);
    for (uint32_t i = 0; i < ncols; i++) {
        if (i > 0)
            printf(", ");

        if (have_schema && column_is_string(schema->columns[i].type)) {
            const char *str = wl_intern_reverse(s->intern_mut, row[i]);
            if (!str) {
                fprintf(stderr,
                    "wirelog_easy_print_delta: missed reverse-intern for "
                    "relation '%s' column %u id %" PRId64 "\n",
                    relation, i, row[i]);
                abort();
            }
            printf("\"%s\"", str);
        } else {
            /* Either the schema declares this column as a non-string
             * (int/bool) type, or no schema was available at all.  In
             * both cases, render the raw int64 value. */
            printf("%" PRId64, row[i]);
        }
    }
    printf(")\n");
}

/* ======================================================================== */
/* Banner                                                                   */
/* ======================================================================== */

void
wirelog_easy_banner(const char *label)
{
    printf("\n=== %s ===\n", label ? label : "");
}

/* ======================================================================== */
/* Snapshot with relation filter                                            */
/* ======================================================================== */

typedef struct {
    const char *wanted;
    wirelog_on_tuple_fn user_cb;
    void *user_data;
} wirelog_easy_snapshot_filter_t;

static void
snapshot_trampoline(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    wirelog_easy_snapshot_filter_t *f =
        (wirelog_easy_snapshot_filter_t *)user_data;
    if (!relation || !f || !f->wanted)
        return;
    if (strcmp(relation, f->wanted) != 0)
        return;
    if (f->user_cb)
        f->user_cb(relation, row, ncols, f->user_data);
}

wirelog_error_t
wirelog_easy_snapshot(wirelog_easy_session_t *s, const char *relation,
    wirelog_on_tuple_fn cb,
    void *user_data)
{
    if (!s || !relation || !cb)
        return WIRELOG_ERR_EXEC;
    if (easy_relation_has_float(s, relation))
        return WIRELOG_ERR_EXEC;
    wirelog_error_t err = ensure_plan_built(s, s->num_workers);
    if (err != WIRELOG_OK)
        return err;
    wirelog_easy_snapshot_filter_t filter
        = { .wanted = relation, .user_cb = cb, .user_data = user_data };
    int rc = wl_session_snapshot(s->session, snapshot_trampoline, &filter);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}
