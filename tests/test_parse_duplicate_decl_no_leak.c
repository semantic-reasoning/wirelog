/*
 * test_parse_duplicate_decl_no_leak.c
 *
 * Regression test for issue #724.
 *
 * When a program text contains the same `.decl` for a given relation more
 * than once (e.g. produced by concatenating templates that share a
 * declaration), `collect_decl` re-runs against an existing relation entry
 * and overwrites `rel->columns` without freeing the previously allocated
 * column array and per-column `name` strings. The same hazard applies to
 * `collect_input`: a duplicate `.input` overwrites `input_io_scheme`,
 * `input_param_names`, and `input_param_values`.
 *
 * Build hosts that load multi-file template bundles (notably wyrelog,
 * which concatenates `bootstrap.dl` and `decision.dl` and ships duplicate
 * declarations such as `login_skip_mfa_authz`) trip this leak on every
 * `wirelog_parse_string` call. ASAN-instrumented CI then aborts the
 * downstream test binary even though the input program is well-formed.
 *
 * The body below exercises duplicate `.decl` and duplicate `.input` and
 * destroys the program. Under ASAN/LSan the leak surfaces as a hard
 * test failure; without sanitizers the test passes trivially as long
 * as parsing succeeds, which is still useful coverage.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <stdio.h>
#include <string.h>

static int
expect_parse_ok(const char *src, const char *case_name)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog || err != WIRELOG_OK) {
        fprintf(stderr, "%s: parse failed (err=%d)\n", case_name, err);
        if (prog)
            wirelog_program_free(prog);
        return 1;
    }
    wirelog_program_free(prog);
    return 0;
}

int
main(void)
{
    int failures = 0;

    /* T1: same .decl for the same relation twice in one program.
     * This is the wyrelog template-bundle pattern. */
    failures += expect_parse_ok(
        ".decl r(a: int64, b: int64)\n"
        ".decl r(a: int64, b: int64)\n",
        "T1 duplicate .decl");

    /* T2: same .decl with a divergent column shape. The parser today
     * accepts and overwrites ("second .decl wins"); the only contract
     * the test asserts is that the previously allocated columns array
     * and the column-name strings are reclaimed. */
    failures += expect_parse_ok(
        ".decl r(a: int64)\n"
        ".decl r(x: int64, y: int64, z: int64)\n",
        "T2 duplicate .decl with divergent shape");

    /* T3: same .input for the same relation twice, with io scheme +
     * extra parameters. The duplicate must reclaim the previously
     * allocated input_io_scheme / input_param_names / input_param_values. */
    failures += expect_parse_ok(
        ".decl r(a: symbol)\n"
        ".input r(io=\"csv\", filename=\"a.csv\")\n"
        ".input r(io=\"csv\", filename=\"b.csv\")\n",
        "T3 duplicate .input");

    if (failures != 0) {
        printf("test_parse_duplicate_decl_no_leak: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_parse_duplicate_decl_no_leak: OK\n");
    return 0;
}
