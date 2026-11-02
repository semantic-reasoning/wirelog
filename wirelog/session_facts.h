/*
 * session_facts.h - wirelog Fact-Loading Session Helpers
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Backend-agnostic fact-loading helpers that use wl_session_insert()
 * instead of the deleted DD-specific loading functions.
 */

#ifndef WL_SESSION_FACTS_H
#define WL_SESSION_FACTS_H

#include "session.h"
#include "intern.h"

/* Forward declaration (opaque in public header) */
struct wirelog_program;

/**
 * wl_session_load_facts:
 * @sess: Active execution session.
 * @prog: Parsed program with inline facts.
 *
 * Load all inline facts from the program into the session.
 * Iterates over all relations with fact_count > 0 and calls
 * wl_session_insert() for each.
 *
 * Returns:
 *    0 on success.
 *   -1 on error (NULL args or insert failure).
 */
int
wl_session_load_facts(wl_session_t *sess, const struct wirelog_program *prog);

/**
 * wl_session_load_input_files:
 * @sess: Active execution session.
 * @prog: Parsed program with .input directives.
 *
 * Load CSV files for all relations with .input directives.
 * Reads CSV via wl_csv_read_file() and inserts via wl_session_insert().
 *
 * Returns:
 *    0 on success.
 *   -1 on error (NULL args, missing file, parse error, or insert failure).
 */
int
wl_session_load_input_files(wl_session_t *sess,
    const struct wirelog_program *prog);

/*
 * Host source origin certification (Issue #2321).
 *
 * A facade captures, from the same program whose inline facts it loads and
 * before it loads them, the names of the user-declared relations that carry
 * no inline facts.  Only those relations' source rows are attributable to
 * host calls alone.  The set is bounded by the relation count, owns copies of
 * the names, and is never widened: later edits to a borrowed program cannot
 * certify a relation, and wl_source_origin_revoke() withdraws a relation
 * whose stored rows a failed host batch may have changed in ways no host
 * call requested.
 */
typedef struct wl_source_origin {
    char **names; /* owned; NULL entries are revoked */
    uint32_t count;
} wl_source_origin_t;

/* Returns 0, or ENOMEM leaving *origin empty. */
int
wl_source_origin_capture(const struct wirelog_program *prog,
    wl_source_origin_t *origin);

void
wl_source_origin_free(wl_source_origin_t *origin);

void
wl_source_origin_revoke(wl_source_origin_t *origin, const char *relation);

/*
 * Validate @relation for inspection against the program metadata, in the
 * issue's precedence: identity (UNKNOWN_RELATION for a missing, private or
 * generated name), full schema (UNSUPPORTED unless every column is a scalar
 * INT64 or STRING and the width is positive), origin (UNSUPPORTED unless the
 * relation is certified in @origin and still has no inline facts), then
 * arity (INVALID_ARGUMENT when @expected_ncols is nonzero and differs; pass
 * 0 to skip), then value domains: each of the first @nlanes entries of
 * @lanes that falls in a STRING column must be an ID present in @intern
 * (INVALID_ARGUMENT otherwise).  On OK *width_out is the physical width.
 * Reads metadata only; never allocates or interns.
 */
wirelog_source_query_status_t
wl_source_query_resolve(const struct wirelog_program *prog,
    const wl_source_origin_t *origin, const wl_intern_t *intern,
    const char *relation, uint32_t expected_ncols, const int64_t *lanes,
    uint32_t nlanes, uint32_t *width_out);

/*
 * Run one inspection request for a facade.  With a @session: admission
 * (wl_session_inspection_begin), the backend ADMIT check, then -- unless
 * q->mode is ADMIT -- wl_source_query_resolve() and the backend query, then
 * release.  With no @session (an easy session not yet built): resolve only,
 * answering zero / false, since no host call has been accepted.  On OK the
 * caller publishes @out; on failure @out is zero / false.
 */
wirelog_source_query_status_t
wl_source_query_run(wl_session_t *session, const struct wirelog_program *prog,
    const wl_source_origin_t *origin, const wl_intern_t *intern,
    wl_source_query_t *q, uint32_t expected_ncols, uint32_t nlanes,
    wl_source_query_result_t *out);

/* Admission as wl_source_query_run() in ADMIT mode, then a read-only
 * wl_intern_get() while the reservation is still held.  The caller resets
 * *out_id to -1 and *out_found to false first; they are set only when the
 * symbol is found. */
wirelog_source_query_status_t
wl_source_lookup_symbol(wl_session_t *session, const wl_intern_t *intern,
    const char *symbol, int64_t *out_id, bool *out_found);

#endif /* WL_SESSION_FACTS_H */
