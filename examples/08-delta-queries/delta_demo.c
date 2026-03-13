/*
 * delta_demo.c - Example 08: Delta Queries with Callback API
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Demonstrates wl_session_set_delta_cb() for access control rules.
 * As can(user, permission) tuples are inserted, delta callbacks fire
 * for each newly derived granted() fact, printed in human-readable
 * form via wl_intern_reverse().
 *
 * Note: The --delta CLI flag is not yet implemented. This C API example
 * shows the same capability programmatically.
 *
 * Build: meson compile -C build delta_demo
 * Run:   ./build/examples/08-delta-queries/delta_demo
 */

#include "../../wirelog/exec_plan_gen.h"
#include "../../wirelog/intern.h"
#include "../../wirelog/passes/fusion.h"
#include "../../wirelog/passes/jpp.h"
#include "../../wirelog/passes/sip.h"
#include "../../wirelog/session.h"
#include "../../wirelog/wirelog-parser.h"
#include "../../wirelog/wirelog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Datalog Program                                                          */
/* ======================================================================== */

/*
 * Access control rules:
 *   can(user, perm)     -- base EDB (inserted via C API)
 *   granted(user, perm) -- derived IDB: mirrors can, extensible for roles
 */
static const char *ACCESS_CONTROL_SRC
    = ".decl can(user: symbol, perm: symbol)\n"
      ".decl granted(user: symbol, perm: symbol)\n"
      "granted(U, P) :- can(U, P).\n";

/* ======================================================================== */
/* Delta Callback                                                           */
/* ======================================================================== */

static void
on_delta(const char *relation, const int64_t *row, uint32_t ncols, int32_t diff,
         void *user_data)
{
    const wl_intern_t *intern = (const wl_intern_t *)user_data;
    char sign = (diff > 0) ? '+' : '-';

    if (ncols == 2) {
        const char *col0 = wl_intern_reverse(intern, row[0]);
        const char *col1 = wl_intern_reverse(intern, row[1]);
        printf("%c %s(\"%s\", \"%s\")\n", sign, relation, col0 ? col0 : "?",
               col1 ? col1 : "?");
    } else if (ncols == 1) {
        const char *col0 = wl_intern_reverse(intern, row[0]);
        printf("%c %s(\"%s\")\n", sign, relation, col0 ? col0 : "?");
    }
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("Example 08: Delta Queries with Callback API\n");
    printf("============================================\n\n");

    /* Parse and optimize the access control program */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(ACCESS_CONTROL_SRC, &err);
    if (!prog) {
        fprintf(stderr, "Parse error: %d\n", (int)err);
        return 1;
    }

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    /*
     * Intern the symbols we will insert as facts.
     * wirelog_program_get_intern() returns const, but wl_intern_put()
     * requires a mutable pointer — safe here because we own the program
     * and hold it alive for the duration of the session.
     */
    wl_intern_t *intern = (wl_intern_t *)wirelog_program_get_intern(prog);

    int64_t id_alice = wl_intern_put(intern, "alice");
    int64_t id_bob = wl_intern_put(intern, "bob");
    int64_t id_carol = wl_intern_put(intern, "carol");
    int64_t id_read = wl_intern_put(intern, "read");
    int64_t id_write = wl_intern_put(intern, "write");
    int64_t id_admin = wl_intern_put(intern, "admin");

    /* Compile the plan after all symbols are interned */
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        fprintf(stderr, "Plan generation failed\n");
        wirelog_program_free(prog);
        return 1;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        fprintf(stderr, "Session creation failed\n");
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return 1;
    }

    /* Register the delta callback; pass intern for reverse symbol lookup */
    wl_session_set_delta_cb(session, on_delta, intern);

    /* -------------------------------------------------------------------- */
    /* Insert all access control facts then step once.                       */
    /*                                                                       */
    /* Five grants covering three users and three permissions:               */
    /*   alice: read, write                                                  */
    /*   bob:   read, admin                                                  */
    /*   carol: read                                                         */
    /* -------------------------------------------------------------------- */
    printf("Inserting access control facts...\n\n");

    int64_t facts[] = {
        id_alice, id_read, id_alice, id_write, id_bob,
        id_read,  id_bob,  id_admin, id_carol, id_read,
    };
    wl_session_insert(session, "can", facts, 5, 2);

    printf("Delta output from wl_session_step():\n");
    rc = wl_session_step(session);
    if (rc != 0) {
        fprintf(stderr, "session_step failed\n");
        goto cleanup;
    }

    printf("\nDone.\n");

cleanup:
    wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}
