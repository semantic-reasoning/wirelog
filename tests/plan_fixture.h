/* plan_fixture.h - keep parsed programs alive for the plans built from them
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * wl_plan_t and every session created from it borrow the program's intern
 * table (see the borrow note in exec_plan_gen.c and the executor contract in
 * wirelog.h).  Test helpers used to free the program right after
 * wl_plan_from_program(); that was latent while nothing touched the intern
 * through the plan, and became a use-after-free once session creation
 * attaches the memory governor to plan->intern (Issue #1431).
 *
 * Usage inside a build_plan() helper:
 *
 *     int rc = wl_plan_from_program(prog, &plan);
 *     if (rc != 0) {
 *         wirelog_program_free(prog);
 *         return NULL;
 *     }
 *     plan_fixture_hold(prog);
 *     return plan;
 *
 * plan_fixture_hold() registers plan_fixture_release() with atexit() on first
 * use, so every exit path of the test binary frees the held programs after
 * main() returns and after every session and plan has been destroyed.
 * plan_fixture_release() may also be called explicitly; it is idempotent.
 *
 * The pattern is enforced by scripts/ci/check-program-lifetime.py (meson
 * test program_lifetime, suite abi, Issue #1471): an unconditional
 * wirelog_program_free() after wl_plan_from_program() in a helper under
 * tests/ or bench/ whose plan is still live (returned, stored, or handed to
 * a session) fails CI unless the helper calls plan_fixture_hold().
 *
 * Header-only, no dependencies beyond wirelog.h.
 */
#ifndef WIRELOG_TESTS_PLAN_FIXTURE_H
#define WIRELOG_TESTS_PLAN_FIXTURE_H

#include <stdio.h>
#include <stdlib.h>

#include "../wirelog/wirelog.h"

static wirelog_program_t **plan_fixture_programs;
static size_t plan_fixture_count;
static size_t plan_fixture_capacity;
static int plan_fixture_atexit_registered;

static void
plan_fixture_release(void)
{
    while (plan_fixture_count > 0)
        wirelog_program_free(plan_fixture_programs[--plan_fixture_count]);
    free(plan_fixture_programs);
    plan_fixture_programs = NULL;
    plan_fixture_capacity = 0;
}

/* Take ownership of @prog until process exit.  Never fails silently: an
 * allocation failure while growing the table aborts the test binary, because
 * freeing the program here would recreate the dangling borrow. */
static void
plan_fixture_hold(wirelog_program_t *prog)
{
    if (!prog)
        return;
    if (plan_fixture_count == plan_fixture_capacity) {
        size_t new_capacity = plan_fixture_capacity ? plan_fixture_capacity *
            2 : 16;
        wirelog_program_t **grown = realloc(plan_fixture_programs,
                new_capacity * sizeof(*grown));
        if (!grown) {
            fprintf(stderr, "plan_fixture_hold: out of memory\n");
            abort();
        }
        plan_fixture_programs = grown;
        plan_fixture_capacity = new_capacity;
    }
    plan_fixture_programs[plan_fixture_count++] = prog;
    if (!plan_fixture_atexit_registered) {
        plan_fixture_atexit_registered = 1;
        atexit(plan_fixture_release);
    }
}

#endif /* WIRELOG_TESTS_PLAN_FIXTURE_H */
