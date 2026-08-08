/*
 * test_csv_wide_columns.c - wide-relation CSV loading (Issue #997)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * The string-aware CSV line parser used to write values[col] for every
 * column of the caller's declared width with no capacity check, into a
 * fixed int64_t row_values[256] frame buffer in
 * wl_csv_read_file_via_ctx().  A relation with 257 or more columns, at
 * least one of them a symbol, therefore overflowed the caller's stack
 * frame -- an ASan stack-buffer-overflow WRITE of size 8.  The boundary
 * was exact: 256 columns clean, 257 over.  The integer-only parser had
 * always taken a max_cols capacity and returned -2 once it was
 * exceeded; the string path had simply lost that check.
 *
 * The fix heap-allocates the row buffer at the relation's real width and
 * threads an explicit capacity through to the write site, which removes
 * the 256-column ceiling rather than merely enforcing it.  These tests
 * pin both halves: nothing is written past the buffer, and relations
 * that used to be impossible now load with byte-exact contents.
 *
 * Coverage is split by what each entry point can actually observe:
 *
 *   - wl_csv_read_file_via_ctx() is asserted cell for cell at 256 (the
 *     positive control), 257 (the boundary), 300 (the ceiling), and 260
 *     mixed int/symbol.  A row-count assertion would pass on the
 *     overflowing build, so every cell is compared to the source text.
 *
 *   - wl_run_pipeline() covers the .input directive path, which is how
 *     the overflow was first reached (csv_read -> csv_parse_line_via_ctx
 *     under wl_session_load_input_files).  Loaded EDB rows are not
 *     observable from there -- wl_session_snapshot emits IDB tuples only,
 *     and a projection rule wide enough to expose them would run into
 *     the evaluator's separate 32-column COL_STACK_MAX limit -- so these
 *     cases assert a clean load plus ASan cleanliness and leave the
 *     content assertions to the direct reader above.
 *
 *   - wirelog_load_facts_from_csv() is the public API an embedder
 *     reaches the same parser through, with no Datalog at all.
 */

#include "../wirelog/cli/driver.h"
#include "../wirelog/io/csv_reader.h"
#include "../wirelog/wirelog.h"

#include "test_tmpdir.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

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
 * The .decl for a 300-column relation is a few KiB.  The field values
 * are kept short so both fixtures fit these static buffers; the reader
 * no longer imposes a line-length limit of its own (#953), and the
 * interaction between a wide relation and a line long enough to need
 * the growable buffer is covered in tests/test_csv_limits.c.
 */
#define WIDE_SRC_BUF 32768
#define WIDE_CSV_BUF 16384

static int
write_text_file(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    if (text && text[0] != '\0')
        fputs(text, f);
    fclose(f);
    return 0;
}

/*
 * Run the full pipeline on @src, capturing printed tuples into @outpath.
 * On success (*out_text != NULL) the caller must free(*out_text).
 * Returns the wl_run_pipeline return code, or -999 if the capture file
 * could not be opened.
 */
static int
run_pipeline_capture(const char *src, const char *outpath, char **out_text)
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

/* Column @col is a symbol unless @mixed and @col is even. */
static int
col_is_symbol(int col, int mixed)
{
    return !mixed || (col % 2) != 0;
}

/* Append to @buf at *@off, returning 0 on success and -1 on overflow. */
static int
appendf(char *buf, size_t bufsz, size_t *off, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf + *off, bufsz - *off, fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= bufsz - *off)
        return -1;
    *off += (size_t)n;
    return 0;
}

/* The symbol cell at (@row, @col): "r<row>c<col>", unique per cell. */
static void
cell_symbol(char *buf, size_t bufsz, int row, int col)
{
    snprintf(buf, bufsz, "r%dc%d", row, col);
}

/* The integer cell at (@row, @col), distinct across the widths used. */
static int64_t
cell_int(int row, int col)
{
    return (int64_t)row * 100000 + col;
}

/* ".decl rel(c0: symbol, c1: int32, ...)\n" */
static int
build_wide_decl(char *buf, size_t bufsz, size_t *off, const char *rel,
    int ncols, int mixed)
{
    if (appendf(buf, bufsz, off, ".decl %s(", rel) != 0)
        return -1;
    for (int i = 0; i < ncols; i++) {
        if (appendf(buf, bufsz, off, "%sc%d: %s", i ? ", " : "", i,
            col_is_symbol(i, mixed) ? "symbol" : "int32")
            != 0)
            return -1;
    }
    return appendf(buf, bufsz, off, ")\n");
}

static int
build_wide_csv(char *buf, size_t bufsz, int ncols, int mixed, int nrows)
{
    size_t off = 0;
    for (int r = 0; r < nrows; r++) {
        for (int c = 0; c < ncols; c++) {
            const char *sep = c ? "," : "";
            int rc;
            if (col_is_symbol(c, mixed)) {
                char cell[64];
                cell_symbol(cell, sizeof(cell), r, c);
                rc = appendf(buf, bufsz, &off, "%s%s", sep, cell);
            } else {
                rc = appendf(buf, bufsz, &off, "%s%lld", sep,
                        (long long)cell_int(r, c));
            }
            if (rc != 0)
                return -1;
        }
        if (appendf(buf, bufsz, &off, "\n") != 0)
            return -1;
    }
    return 0;
}

/* ======================================================================== */
/* Cases 1-4: wl_csv_read_file_via_ctx() asserted cell for cell             */
/* ======================================================================== */

/* Interns through a real table so the loaded ids can be resolved back to
 * the exact source text, rather than only counted. */
static int64_t
intern_cb(void *opaque, const char *str)
{
    return wl_intern_put((wl_intern_t *)opaque, str);
}

static void
check_direct_read(const char *label, int ncols, int mixed)
{
    TEST(label);

    static char csv[WIDE_CSV_BUF];
    const int nrows = 3;
    char path[512];
    char fixture[128];
    char msg[192];

    snprintf(fixture, sizeof(fixture), "wl997_direct_%s%d.csv",
        mixed ? "mix" : "", ncols);
    test_tmppath(path, sizeof(path), fixture);

    if (build_wide_csv(csv, sizeof(csv), ncols, mixed, nrows) != 0) {
        FAIL("csv fixture buffer too small");
        return;
    }
    if (write_text_file(path, csv) != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    wirelog_column_type_t *types
        = (wirelog_column_type_t *)malloc((size_t)ncols * sizeof(*types));
    wl_intern_t *intern = wl_intern_create();
    if (!types || !intern) {
        remove(path);
        free(types);
        if (intern)
            wl_intern_free(intern);
        FAIL("allocation failed");
        return;
    }
    for (int i = 0; i < ncols; i++) {
        types[i] = col_is_symbol(i, mixed) ? WIRELOG_TYPE_STRING
                                           : WIRELOG_TYPE_INT32;
    }

    int64_t *data = NULL;
    uint32_t out_nrows = 0, out_ncols = 0;
    int rc = wl_csv_read_file_via_ctx(path, ',', types, (uint32_t)ncols, &data,
            &out_nrows, &out_ncols, intern_cb, intern);

    remove(path);
    free(types);

    if (rc != 0) {
        snprintf(msg, sizeof(msg), "returned %d for a %d-column file", rc,
            ncols);
        goto fail;
    }
    if (out_nrows != (uint32_t)nrows || out_ncols != (uint32_t)ncols) {
        snprintf(msg, sizeof(msg), "expected %dx%d, got %ux%u", nrows, ncols,
            out_nrows, out_ncols);
        goto fail;
    }

    /*
     * Every cell, not just the row count: an overflowing build still
     * reports the right shape.
     */
    for (int r = 0; r < nrows; r++) {
        for (int c = 0; c < ncols; c++) {
            int64_t got = data[(size_t)r * ncols + c];
            if (col_is_symbol(c, mixed)) {
                char want[64];
                cell_symbol(want, sizeof(want), r, c);
                const char *s = wl_intern_reverse(intern, got);
                if (!s || strcmp(s, want) != 0) {
                    snprintf(msg, sizeof(msg),
                        "cell [%d][%d] is \"%s\", want \"%s\"", r, c,
                        s ? s : "(unresolved)", want);
                    goto fail;
                }
            } else if (got != cell_int(r, c)) {
                snprintf(msg, sizeof(msg), "cell [%d][%d] is %lld, want %lld",
                    r, c, (long long)got, (long long)cell_int(r, c));
                goto fail;
            }
        }
    }

    free(data);
    wl_intern_free(intern);
    PASS();
    return;

fail:
    free(data);
    wl_intern_free(intern);
    FAIL(msg);
}

/* 256 is the last width the old fixed row_values[] could hold: the
 * positive control that must keep working. */
static void
test_direct_256(void)
{
    check_direct_read("read_file_via_ctx: 256 columns round-trip intact", 256,
        0);
}

/* 257 is the exact overflow boundary -- one int64_t past the buffer. */
static void
test_direct_257(void)
{
    check_direct_read("read_file_via_ctx: 257 columns round-trip intact", 257,
        0);
}

/* Proof the ceiling is removed rather than moved. */
static void
test_direct_300(void)
{
    check_direct_read("read_file_via_ctx: 300 columns round-trip intact", 300,
        0);
}

/* The string-aware parser is selected when *any* column is a symbol, so
 * a mostly-integer wide relation takes the same path. */
static void
test_direct_260_mixed(void)
{
    check_direct_read(
        "read_file_via_ctx: 260 mixed int/symbol columns round-trip intact",
        260, 1);
}

/* ======================================================================== */
/* Cases 5-6: the .input directive path through the real pipeline           */
/* ======================================================================== */

/*
 * This is the path the overflow was first reported on: csv_read ->
 * wl_csv_read_file_via_ctx -> csv_parse_line_via_ctx, beneath
 * wl_session_load_input_files.  Before the fix the 257- and 300-column
 * cases abort here under ASan; after it they must load cleanly.
 */
static void
check_pipeline_load(const char *label, int ncols, int mixed)
{
    TEST(label);

    static char src[WIDE_SRC_BUF];
    static char csv[WIDE_CSV_BUF];
    char csv_path[512];
    char out_path[512];
    char name[64];
    char fixture[128];
    size_t off = 0;

    snprintf(name, sizeof(name), "wide%s%d", mixed ? "mix" : "", ncols);
    snprintf(fixture, sizeof(fixture), "wl997_%s.csv", name);
    test_tmppath(csv_path, sizeof(csv_path), fixture);
    snprintf(fixture, sizeof(fixture), "wl997_%s_out.txt", name);
    test_tmppath(out_path, sizeof(out_path), fixture);

    if (build_wide_csv(csv, sizeof(csv), ncols, mixed, 2) != 0) {
        FAIL("csv fixture buffer too small");
        return;
    }
    if (build_wide_decl(src, sizeof(src), &off, name, ncols, mixed) != 0
        || appendf(src, sizeof(src), &off,
        ".input %s(filename=\"%s\", delimiter=\",\")\n", name, csv_path)
        != 0) {
        FAIL("source buffer too small");
        return;
    }
    if (write_text_file(csv_path, csv) != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);
    free(output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d for %d columns",
            rc, ncols);
        FAIL(msg);
        return;
    }

    PASS();
}

static void
test_pipeline_257(void)
{
    check_pipeline_load(".input: 257 symbol columns load without overflow",
        257, 0);
}

static void
test_pipeline_300(void)
{
    check_pipeline_load(".input: 300 symbol columns load", 300, 0);
}

/* ======================================================================== */
/* Case 7: the public API entry -- wirelog_load_facts_from_csv()            */
/* ======================================================================== */

/*
 * api_facade.c forwards rel->column_count into wl_csv_read_file_ex()
 * unchecked, so an embedder reaches the same overflow with no .input
 * directive and no Datalog rules involved.  The relation is declared
 * without .input so the load under test is the explicit public call and
 * not the executor's eager seeding.
 */
static void
test_public_api_wide_csv(void)
{
    TEST("wirelog_load_facts_from_csv: 260-column relation");

    static char src[WIDE_SRC_BUF];
    static char csv[WIDE_CSV_BUF];
    const int ncols = 260;
    const int nrows = 4;
    char csv_path[512];
    size_t off = 0;

    test_tmppath(csv_path, sizeof(csv_path), "wl997_public_api.csv");

    if (build_wide_csv(csv, sizeof(csv), ncols, 0, nrows) != 0) {
        FAIL("csv fixture buffer too small");
        return;
    }
    if (build_wide_decl(src, sizeof(src), &off, "wideapi", ncols, 0) != 0) {
        FAIL("source buffer too small");
        return;
    }
    if (write_text_file(csv_path, csv) != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        remove(csv_path);
        FAIL("wirelog_parse_string failed");
        return;
    }

    wirelog_executor_t *exec = wirelog_executor_create(prog, &err);
    if (!exec) {
        wirelog_program_free(prog);
        remove(csv_path);
        FAIL("wirelog_executor_create failed");
        return;
    }

    bool ok = wirelog_load_facts_from_csv(exec, "wideapi", csv_path, &err);
    remove(csv_path);

    wirelog_executor_free(exec);
    wirelog_program_free(prog);

    if (!ok) {
        FAIL("wirelog_load_facts_from_csv returned false");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* Case 8: the capacity parameter itself rejects an undersized buffer       */
/* ======================================================================== */

/*
 * Direct cover for the bound that replaced the silent overflow: asking
 * for more columns than the output buffer holds must be refused, exactly
 * as wl_csv_parse_line() has always refused it with -2.
 */
static void
test_parse_line_ex_capacity_guard(void)
{
    TEST("parse_line_ex: num_cols beyond the buffer capacity is rejected");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("intern create failed");
        return;
    }

    wirelog_column_type_t types[4] = { WIRELOG_TYPE_STRING,
                                       WIRELOG_TYPE_STRING, WIRELOG_TYPE_STRING,
                                       WIRELOG_TYPE_STRING };
    int64_t values[2];
    uint32_t count = 0;

    int rc = wl_csv_parse_line_ex("a,b,c,d", ',', types, 4, values, 2, &count,
            intern);
    if (rc != -2) {
        char msg[96];
        snprintf(msg, sizeof(msg), "expected -2 for capacity overrun, got %d",
            rc);
        wl_intern_free(intern);
        FAIL(msg);
        return;
    }

    /* An exactly-fitting width must still succeed. */
    rc = wl_csv_parse_line_ex("a,b", ',', types, 2, values, 2, &count, intern);
    if (rc != 0 || count != 2) {
        wl_intern_free(intern);
        FAIL("exact-capacity parse should succeed");
        return;
    }

    wl_intern_free(intern);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    /* Unbuffered: an ASan abort mid-suite must not swallow the progress
     * printed so far, which is what identifies the failing case. */
    setvbuf(stdout, NULL, _IONBF, 0);

    printf("=== test_csv_wide_columns (Issue #997) ===\n");

    test_direct_256();
    test_direct_257();
    test_direct_300();
    test_direct_260_mixed();
    test_pipeline_257();
    test_pipeline_300();
    test_public_api_wide_csv();
    test_parse_line_ex_capacity_guard();

    printf("\n=== Results: %d run, %d passed, %d failed ===\n", tests_run,
        tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
