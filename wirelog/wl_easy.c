/*
 * wl_easy.c - wirelog convenience facade (Issue #441)
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

#include "wirelog/wl_easy.h"

#include "wirelog/backend.h"
#include "wirelog/exec_plan.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/intern.h"
#include "wirelog/passes/fusion.h"
#include "wirelog/passes/jpp.h"
#include "wirelog/passes/sip.h"
#include "wirelog/session.h"
#include "wirelog/wirelog-parser.h"
#include "wirelog/wirelog-types.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define WL_EASY_MAX_COLS 16

struct wl_easy_session {
    wirelog_program_t *prog; /* owns */
    wl_plan_t *plan;         /* owns, lazy-built */
    wl_session_t *session;   /* owns, lazy-built */
    /* Non-owning alias of prog's intern table.  See delta_demo.c:93-97
     * for the rationale: wirelog_program_get_intern() returns const, but
     * wl_intern_put() needs a mutable pointer; the program owns the
     * intern's lifetime so the cast is safe. */
    wl_intern_t *intern_mut;
    bool plan_built; /* true once first wl_easy_step-class call ran */
};

/* ======================================================================== */
/* Internal: lazy plan/session build                                        */
/* ======================================================================== */

static wirelog_error_t
ensure_plan_built(wl_easy_session_t *s)
{
    if (!s)
        return WIRELOG_ERR_EXEC;
    if (s->plan_built)
        return WIRELOG_OK;

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(s->prog, &plan);
    if (rc != 0 || !plan)
        return WIRELOG_ERR_EXEC;

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
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
wl_easy_open(const char *dl_src, wl_easy_session_t **out)
{
    if (!dl_src || !out)
        return WIRELOG_ERR_EXEC;
    *out = NULL;

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(dl_src, &err);
    if (!prog)
        return (err != WIRELOG_OK) ? err : WIRELOG_ERR_PARSE;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_easy_session_t *s
        = (wl_easy_session_t *)calloc(1, sizeof(wl_easy_session_t));
    if (!s) {
        wirelog_program_free(prog);
        return WIRELOG_ERR_MEMORY;
    }
    s->prog = prog;
    s->intern_mut = (wl_intern_t *)wirelog_program_get_intern(prog);
    s->plan = NULL;
    s->session = NULL;
    s->plan_built = false;

    *out = s;
    return WIRELOG_OK;
}

void
wl_easy_close(wl_easy_session_t *s)
{
    if (!s)
        return;
    if (s->session)
        wl_session_destroy(s->session);
    if (s->plan)
        wl_plan_free(s->plan);
    if (s->prog)
        wirelog_program_free(s->prog);
    free(s);
}

/* ======================================================================== */
/* Intern                                                                   */
/* ======================================================================== */

int64_t
wl_easy_intern(wl_easy_session_t *s, const char *sym)
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

/* ======================================================================== */
/* Insert / Remove                                                          */
/* ======================================================================== */

wirelog_error_t
wl_easy_insert(wl_easy_session_t *s, const char *relation, const int64_t *row,
    uint32_t ncols)
{
    if (!s || !relation || !row)
        return WIRELOG_ERR_EXEC;
    wirelog_error_t err = ensure_plan_built(s);
    if (err != WIRELOG_OK)
        return err;
    int rc = wl_session_insert(s->session, relation, row, 1, ncols);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wl_easy_remove(wl_easy_session_t *s, const char *relation, const int64_t *row,
    uint32_t ncols)
{
    if (!s || !relation || !row)
        return WIRELOG_ERR_EXEC;
    wirelog_error_t err = ensure_plan_built(s);
    if (err != WIRELOG_OK)
        return err;
    int rc = wl_session_remove(s->session, relation, row, 1, ncols);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

/* ======================================================================== */
/* Variadic symbol helpers                                                  */
/* ======================================================================== */

static wirelog_error_t
collect_sym_row(wl_easy_session_t *s, va_list ap, int64_t *row, uint32_t *ncols)
{
    uint32_t n = 0;
    while (n < WL_EASY_MAX_COLS) {
        const char *sym = va_arg(ap, const char *);
        if (sym == NULL)
            break;
        /* Interning a never-seen symbol after plan build is rejected by
         * wl_easy_intern, but if the caller pre-interned the symbol via
         * wl_easy_intern() before the first step, wl_intern_put() simply
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
wl_easy_insert_sym(wl_easy_session_t *s, const char *relation, ...)
{
    if (!s || !relation)
        return WIRELOG_ERR_EXEC;
    int64_t row[WL_EASY_MAX_COLS];
    uint32_t ncols = 0;
    va_list ap;
    va_start(ap, relation);
    wirelog_error_t err = collect_sym_row(s, ap, row, &ncols);
    va_end(ap);
    if (err != WIRELOG_OK)
        return err;
    return wl_easy_insert(s, relation, row, ncols);
}

wirelog_error_t
wl_easy_remove_sym(wl_easy_session_t *s, const char *relation, ...)
{
    if (!s || !relation)
        return WIRELOG_ERR_EXEC;
    int64_t row[WL_EASY_MAX_COLS];
    uint32_t ncols = 0;
    va_list ap;
    va_start(ap, relation);
    wirelog_error_t err = collect_sym_row(s, ap, row, &ncols);
    va_end(ap);
    if (err != WIRELOG_OK)
        return err;
    return wl_easy_remove(s, relation, row, ncols);
}

/* ======================================================================== */
/* Step / set_delta_cb                                                      */
/* ======================================================================== */

wirelog_error_t
wl_easy_step(wl_easy_session_t *s)
{
    if (!s)
        return WIRELOG_ERR_EXEC;
    wirelog_error_t err = ensure_plan_built(s);
    if (err != WIRELOG_OK)
        return err;
    int rc = wl_session_step(s->session);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

void
wl_easy_set_delta_cb(wl_easy_session_t *s, wl_on_delta_fn cb, void *user_data)
{
    if (!s)
        return;
    if (ensure_plan_built(s) != WIRELOG_OK)
        return;
    wl_session_set_delta_cb(s->session, cb, user_data);
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
wl_easy_print_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    wl_easy_session_t *s = (wl_easy_session_t *)user_data;
    if (!s || !relation || !row) {
        fprintf(stderr, "wl_easy_print_delta: NULL argument\n");
        abort();
    }

    char sign = (diff > 0) ? '+' : '-';

    /* Try schema-aware path first.  If the schema is unavailable for any
     * reason (NULL program, unknown relation), fall back to all-symbol mode
     * which still aborts on missed reverse-intern. */
    const wirelog_schema_t *schema
        = wirelog_program_get_schema(s->prog, relation);
    bool have_schema = (schema != NULL && schema->columns != NULL
        && schema->column_count == ncols);

    printf("%c %s(", sign, relation);
    for (uint32_t i = 0; i < ncols; i++) {
        if (i > 0)
            printf(", ");

        bool as_string;
        if (have_schema)
            as_string = column_is_string(schema->columns[i].type);
        else
            as_string = true; /* fall back to symbol mode */

        if (as_string) {
            const char *str = wl_intern_reverse(s->intern_mut, row[i]);
            if (!str) {
                fprintf(stderr,
                    "wl_easy_print_delta: missed reverse-intern for "
                    "relation '%s' column %u id %" PRId64 "\n",
                    relation, i, row[i]);
                abort();
            }
            printf("\"%s\"", str);
        } else {
            printf("%" PRId64, row[i]);
        }
    }
    printf(")\n");
}

/* ======================================================================== */
/* Banner                                                                   */
/* ======================================================================== */

void
wl_easy_banner(const char *label)
{
    printf("\n=== %s ===\n", label ? label : "");
}

/* ======================================================================== */
/* Snapshot with relation filter                                            */
/* ======================================================================== */

typedef struct {
    const char *wanted;
    wl_on_tuple_fn user_cb;
    void *user_data;
} wl_easy_snapshot_filter_t;

static void
snapshot_trampoline(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    wl_easy_snapshot_filter_t *f = (wl_easy_snapshot_filter_t *)user_data;
    if (!relation || !f || !f->wanted)
        return;
    if (strcmp(relation, f->wanted) != 0)
        return;
    if (f->user_cb)
        f->user_cb(relation, row, ncols, f->user_data);
}

wirelog_error_t
wl_easy_snapshot(wl_easy_session_t *s, const char *relation, wl_on_tuple_fn cb,
    void *user_data)
{
    if (!s || !relation || !cb)
        return WIRELOG_ERR_EXEC;
    wirelog_error_t err = ensure_plan_built(s);
    if (err != WIRELOG_OK)
        return err;
    wl_easy_snapshot_filter_t filter
        = { .wanted = relation, .user_cb = cb, .user_data = user_data };
    int rc = wl_session_snapshot(s->session, snapshot_trampoline, &filter);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}
