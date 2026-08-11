/*
 * test_cli_physical_columns.c - CLI renders by physical layout (Issue #1036)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * The CLI used to pick a column's render type by indexing the relation's
 * declared columns[] with the row's *physical* position.  Those two indexings
 * agree only until the first `inline` compound column, which occupies
 * compound_arity slots rather than one -- after it every type is shifted, and
 * on the last column the index runs off the end of columns[] entirely.
 *
 * The visible result is not a crash but a plausible-looking wrong line.  For
 *
 *     .decl inp(id: int64, p: pair/2 inline, s: symbol)
 *
 * a row whose physical values are [1, 7, 0, intern("zz")] printed as
 *
 *     inp(1, 7, "pair", 1)
 *
 * -- the compound's second slot reverse-interned into the *functor name*,
 * because slot 2 was rendered with `s`'s symbol type, and the genuine symbol
 * printed as its raw intern id, because slot 3 had no declared column at all.
 *
 * So these tests assert the rendered text, not a row count or an exit status:
 * both the buggy and the correct output are one well-formed line with four
 * fields, and the wrong one is the more plausible-looking of the two.
 *
 * Each case pairs the compound relation with a compound-free control in the
 * same program.  Without the control, a "fix" that stopped reverse-interning
 * altogether -- printing every symbol as a raw id -- would pass every
 * positive assertion here.
 */

#include "../wirelog/cli/driver.h"

#include "test_tmpdir.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)

#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

/*
 * Run the full pipeline on @src, capturing printed tuples into @outpath.
 * On success (*out_text != NULL) the caller must free(*out_text).
 */
static int
run_capture(const char *src, const char *outpath, char **out_text)
{
    *out_text = NULL;

    FILE *f = fopen(outpath, "w");
    if (!f)
        return -999;

    int rc = wl_run_pipeline(src, 1, false, false, 0, f);
    fclose(f);

    *out_text = wl_read_file(outpath);
    return rc;
}

/* ---------------------------------------------------------------------- */

/*
 * An inline compound sits between an int64 and a symbol, so every physical
 * slot after the compound is misaligned by one against columns[].
 *
 * The control relation `plain` carries the same symbol in the same trailing
 * position with no compound before it; it rendered correctly before the fix
 * and must still.
 */
static const char *const k_inline_then_symbol_src =
    ".decl src(a: int64, b: int64, c: int64, d: symbol)\n"
    ".decl inp(id: int64, p: pair/2 inline, s: symbol)\n"
    ".decl plain(id: int64, s: symbol)\n"
    ".output inp\n"
    ".output plain\n"
    "src(1, 7, 0, \"zz\").\n"
    "inp(x, y, z, w) :- src(x, y, z, w).\n"
    "plain(x, w) :- src(x, y, z, w).\n";

static void
test_inline_compound_render(void)
{
    TEST("stdout: a symbol after an inline compound renders as itself");

    char path[512];
    test_tmppath(path, sizeof(path), "wl_1036_stdout.txt");

    char *text = NULL;
    int rc = run_capture(k_inline_then_symbol_src, path, &text);
    if (rc != 0 || !text) {
        free(text);
        remove(path);
        FAIL("pipeline did not run");
        return;
    }

    /*
     * The compound's slots are raw payload and print as integers; the
     * trailing symbol reverse-interns.  Pre-fix this was
     * inp(1, 7, "pair", 1).
     */
    bool ok = strstr(text, "inp(1, 7, 0, \"zz\")") != NULL;
    bool no_functor = strstr(text, "\"pair\"") == NULL;
    /* Control: unaffected by the compound, correct before and after. */
    bool control = strstr(text, "plain(1, \"zz\")") != NULL;

    free(text);
    remove(path);

    if (!ok)
        FAIL("expected inp(1, 7, 0, \"zz\")");
    else if (!no_functor)
        FAIL("a compound slot was reverse-interned to the functor name");
    else if (!control)
        FAIL("control relation lost its symbol rendering");
    else
        PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * The CSV writer behind .output(filename=...) had the identical indexing, and
 * is a separate code path from the stdout printer -- a fix applied to only
 * one of them passes the case above and fails here.
 */
static void
test_inline_compound_csv(void)
{
    TEST("csv: .output(filename=) writes physical columns");

    char csv[512], cap[512];
    test_tmppath(csv, sizeof(csv), "wl_1036_out.csv");
    test_tmppath(cap, sizeof(cap), "wl_1036_csvcap.txt");
    remove(csv);

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl src(a: int64, b: int64, c: int64, d: symbol)\n"
        ".decl inp(id: int64, p: pair/2 inline, s: symbol)\n"
        ".output inp(filename=\"%s\")\n"
        "src(1, 7, 0, \"zz\").\n"
        "inp(x, y, z, w) :- src(x, y, z, w).\n",
        csv);

    char *ignored = NULL;
    int rc = run_capture(src, cap, &ignored);
    free(ignored);
    remove(cap);

    if (rc != 0) {
        remove(csv);
        FAIL("pipeline did not run");
        return;
    }

    char *written = wl_read_file(csv);
    remove(csv);
    if (!written) {
        FAIL("no CSV file was written");
        return;
    }

    bool ok = strstr(written, "1,7,0,zz") != NULL;
    bool no_functor = strstr(written, "pair") == NULL;
    free(written);

    if (!ok)
        FAIL("expected the row 1,7,0,zz");
    else if (!no_functor)
        FAIL("a compound slot was reverse-interned to the functor name");
    else
        PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * A relation whose declared width already equals its physical width must be
 * byte-identical to before.  This is the case that fails if the new helper
 * miscounts a non-compound column, or treats a `side` compound -- which
 * occupies one slot, not compound_arity -- as if it were inline.
 */
static void
test_side_compound_and_plain_unchanged(void)
{
    TEST("control: side compound and plain relations are unchanged");

    char path[512];
    test_tmppath(path, sizeof(path), "wl_1036_side.txt");

    /*
     * sq's compound is `side`, which occupies one handle slot rather than
     * compound_arity, so its declared and physical widths already agree and
     * the trailing symbol must still reverse-intern.  A helper that treated
     * every compound as inline would widen the handle to two slots, push the
     * symbol to a physical index the walk types as raw payload, and print
     * 99's neighbour as an integer.
     */
    static const char *const src =
        ".decl e(a: int64, b: int64, c: symbol)\n"
        ".decl sq(id: int64, h: pair/2 side, s: symbol)\n"
        ".output sq\n"
        "e(4, 99, \"qq\").\n"
        "sq(x, y, z) :- e(x, y, z).\n";

    char *text = NULL;
    int rc = run_capture(src, path, &text);
    if (rc != 0 || !text) {
        free(text);
        remove(path);
        FAIL("pipeline did not run");
        return;
    }

    bool ok = strstr(text, "sq(4, 99, \"qq\")") != NULL;
    free(text);
    remove(path);

    if (!ok)
        FAIL("expected sq(4, 99, \"qq\")");
    else
        PASS();
}

/* ---------------------------------------------------------------------- */

int
main(void)
{
    printf("=== CLI physical column rendering (#1036) ===\n\n");

    test_inline_compound_render();
    test_inline_compound_csv();
    test_side_compound_and_plain_unchanged();

    printf("\n=== Results: %d run, %d passed, %d failed ===\n",
        tests_run, tests_passed, tests_failed);

    return tests_failed == 0 ? 0 : 1;
}
