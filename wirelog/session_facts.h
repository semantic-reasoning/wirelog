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
 *   -1 on invalid input or an untyped failure.
 *   A positive wl_session_insert() error is preserved, so callers can
 *   distinguish budget denial from allocator and execution failures.
 */
int
wl_session_load_facts(wl_session_t *sess, const struct wirelog_program *prog);

/**
 * wl_session_load_input_files:
 * @sess: Active execution session.
 * @prog: Parsed program with .input directives.
 *
 * Load external inputs for all relations with .input directives through
 * their registered adapters, then insert via wl_session_insert().
 *
 * Returns:
 *    0 on success.
 *   -1 on invalid input, I/O, parse, or untyped adapter failure.
 *   A positive row-insertion error or ENOMEM from an adapter is preserved.
 */
int
wl_session_load_input_files(wl_session_t *sess,
    const struct wirelog_program *prog);

#endif /* WL_SESSION_FACTS_H */
