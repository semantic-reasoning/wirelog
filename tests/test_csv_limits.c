/*
 * test_csv_limits.c - CSV reader line/field length limits (Issue #953)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * The built-in CSV reader used to pull each line through a fixed
 * `char line[4096]` with fgets(), so a physical line longer than 4095
 * bytes was delivered in pieces and every piece was parsed as its own
 * tuple.  Truncation therefore did not merely lose data, it *fabricated
 * rows*: one 8000-byte line became two tuples and the run still exited 0.
 *
 * The arity check added for #977 cannot cover this class, because the
 * reader never assembles the whole line.  With delimiters at byte
 * offsets 2000 and 6000 of an 8000-byte line, chunk [0,4095) holds one
 * delimiter and chunk [4095,8000) holds the other, so *both* chunks
 * present exactly two fields and the width check is satisfied twice.
 *
 * A second, independent 4096-byte cap lived on the string field buffer
 * (`char strbuf[4096]`).  It was unreachable through the file readers --
 * a 4095-byte chunk can never overflow a guard that admits 4095 -- but
 * it is reachable through wl_csv_parse_line_ex() directly, and growing
 * only the line buffer would have turned row fabrication into field
 * truncation.  Both buffers are covered here.
 *
 * The pipeline-driven cases go through wl_run_pipeline (parse -> plan ->
 * load .input -> evaluate) like tests/test_input_arity.c, so the real
 * adapter, reader and columnar insert path are all exercised.
 *
 * Case 8 covers the *interaction* with #997.  Assembling whole lines
 * removes an accidental protection: while a long line arrived in
 * 4095-byte fragments, a wide relation's row write stayed under the
 * fixed 256-entry frame buffer by luck of the fragmentation.  Handing
 * the parser the whole line makes the declared width the only thing
 * bounding the write, so #997's capacity bound has to survive this
 * change.  A >256-column symbol relation with a line long enough to
 * spill is the shape that proves it.
 */

#include "../wirelog/cli/driver.h"
#include "../wirelog/io/csv_reader.h"

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

static int
write_bytes_file(const char *path, const char *bytes, size_t len)
{
    FILE *f = fopen(path, "wb");
    if (!f)
        return -1;
    if (len > 0 && fwrite(bytes, 1, len, f) != len) {
        fclose(f);
        return -1;
    }
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

/* Number of non-empty lines in @text. */
static int
count_lines(const char *text)
{
    int n = 0;
    if (!text)
        return 0;
    for (const char *p = text; *p; ) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        if (len > 0)
            n++;
        if (!nl)
            break;
        p = nl + 1;
    }
    return n;
}

/*
 * Length of the @idx'th double-quoted run in @text (0-based), or
 * (size_t)-1 if there are fewer than @idx+1 of them.
 */
static size_t
quoted_field_len(const char *text, int idx)
{
    if (!text)
        return (size_t)-1;
    const char *p = text;
    for (int i = 0; ; i++) {
        const char *open = strchr(p, '"');
        if (!open)
            return (size_t)-1;
        const char *close = strchr(open + 1, '"');
        if (!close)
            return (size_t)-1;
        if (i == idx)
            return (size_t)(close - open - 1);
        p = close + 1;
    }
}

/*
 * Build the #953 counterexample line: 8000 bytes with delimiters at
 * offsets 2000 and 6000, so the fields are 2000 / 3999 / 1999 bytes and
 * parse as the integers 1 / 2 / 3.  Chunk [0,4095) and chunk [4095,8000)
 * each contain exactly one delimiter, so a split read produces two
 * two-field rows and satisfies a two-column .decl twice over.
 *
 * Returns a malloc'd NUL-terminated string of exactly 8001 bytes
 * (8000 content bytes plus a trailing newline), or NULL.
 */
#define SPLIT_LINE_BYTES 8000

static char *
make_split_line(size_t *out_len, char delim)
{
    char *s = (char *)malloc(SPLIT_LINE_BYTES + 2);
    if (!s)
        return NULL;
    memset(s, '0', SPLIT_LINE_BYTES);
    s[1999] = '1';
    s[2000] = delim;
    s[5999] = '2';
    s[6000] = delim;
    s[7999] = '3';
    s[SPLIT_LINE_BYTES] = '\n';
    s[SPLIT_LINE_BYTES + 1] = '\0';
    *out_len = SPLIT_LINE_BYTES + 1;
    return s;
}

/* ======================================================================== */
/* Case 1: the counterexample #977's width check cannot cover (integers)    */
/* ======================================================================== */

/*
 * One 8000-byte line, delimiters at 2000 and 6000, declared arity 2.
 *
 * Before the fix: two fabricated rows, `q(1, 0)` and `q(2, 3)`, exit 0.
 * Neither chunk trips the #977 width check because both present two
 * fields.
 *
 * After the fix the line is assembled whole and carries three fields,
 * which disagrees with the two-column .decl, so the load must fail.
 * The load-bearing property is that one physical line can no longer
 * become two tuples.
 */
static void
test_split_line_int_not_fabricated(void)
{
    TEST("8000-byte line split across chunks fabricates no integer rows");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_csv953_split_int.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_csv953_split_int_out.txt");

    size_t len = 0;
    char *line = make_split_line(&len, ',');
    if (!line || write_bytes_file(csv_path, line, len) != 0) {
        free(line);
        FAIL("cannot write CSV fixture");
        return;
    }
    free(line);

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl t(a: int32, b: int32)\n"
        ".input t(filename=\"%s\", delimiter=\",\")\n"
        ".decl q(a: int32, b: int32)\n"
        "q(A, B) :- t(A, B).\n"
        ".output q\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc == 0) {
        char msg[512];
        snprintf(msg, sizeof(msg),
            "expected non-zero rc for a 3-field 8000-byte line vs a "
            "2-column .decl; got 0 with output:\n%.300s",
            output ? output : "(null)");
        free(output);
        FAIL(msg);
        return;
    }

    /* Belt and braces: the fabricated tuples must not have been printed. */
    if (output && (strstr(output, "q(1, 0)") || strstr(output, "q(2, 3)"))) {
        free(output);
        FAIL("fabricated chunk tuples q(1, 0) / q(2, 3) were emitted");
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 1b: the same line read at its true width recovers the true values   */
/* ======================================================================== */

/*
 * Same 8000-byte line, now declared with its true arity of 3.  This is
 * the positive half of case 1: assembling the line must yield exactly
 * one row carrying the true field values (1, 2, 3), not chunk debris.
 *
 * Before the fix this failed the load, because chunk [0,4095) presents
 * two fields and the auto-detected width disagreed with the 3-column
 * .decl -- #977 masking the truncation rather than fixing it.
 */
static void
test_split_line_int_true_values(void)
{
    TEST("8000-byte line at its true arity yields one row of true values");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_csv953_split_int3.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_csv953_split_int3_out.txt");

    size_t len = 0;
    char *line = make_split_line(&len, ',');
    if (!line || write_bytes_file(csv_path, line, len) != 0) {
        free(line);
        FAIL("cannot write CSV fixture");
        return;
    }
    free(line);

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl t(a: int32, b: int32, c: int32)\n"
        ".input t(filename=\"%s\", delimiter=\",\")\n"
        ".decl q(a: int32, b: int32, c: int32)\n"
        "q(A, B, C) :- t(A, B, C).\n"
        ".output q\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0 || !output) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    int rows = count_lines(output);
    if (rows != 1) {
        char msg[512];
        snprintf(msg, sizeof(msg),
            "expected exactly 1 row, got %d; output:\n%.300s", rows, output);
        free(output);
        FAIL(msg);
        return;
    }

    if (!strstr(output, "q(1, 2, 3)")) {
        char msg[512];
        snprintf(msg, sizeof(msg),
            "expected q(1, 2, 3); output:\n%.300s", output);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 1c: the same counterexample on the string path                      */
/* ======================================================================== */

/*
 * The string reader parses exactly the declared number of fields and
 * ignores any trailing ones (#982, out of scope here), so the same
 * 8000-byte line at arity 2 must produce exactly one row whose two
 * fields are the true 2000-byte and 3999-byte prefixes of the line.
 *
 * Before the fix: two rows, from the two chunks, exit 0.
 */
static void
test_split_line_str_not_fabricated(void)
{
    TEST("8000-byte line split across chunks fabricates no string rows");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_csv953_split_str.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_csv953_split_str_out.txt");

    size_t len = 0;
    char *line = make_split_line(&len, ',');
    if (!line || write_bytes_file(csv_path, line, len) != 0) {
        free(line);
        FAIL("cannot write CSV fixture");
        return;
    }
    free(line);

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl t(a: symbol, b: symbol)\n"
        ".input t(filename=\"%s\", delimiter=\",\")\n"
        ".decl q(a: symbol, b: symbol)\n"
        "q(A, B) :- t(A, B).\n"
        ".output q\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0 || !output) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    int rows = count_lines(output);
    if (rows != 1) {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "expected exactly 1 row, got %d (chunk rows fabricated)", rows);
        free(output);
        FAIL(msg);
        return;
    }

    size_t f0 = quoted_field_len(output, 0);
    size_t f1 = quoted_field_len(output, 1);
    if (f0 != 2000 || f1 != 3999) {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "expected field lengths 2000/3999, got %zu/%zu", f0, f1);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 2: a 100,000-byte single field is one row, not 25                   */
/* ======================================================================== */

/*
 * Before the fix this emitted 25 rows of *exactly* 4095 characters plus
 * a 1720-character remainder -- chunk-sized, never guard-clipped, which
 * is the proof that the 4096-byte strbuf cap was unreachable from here.
 */
static void
test_huge_single_field(void)
{
    TEST("100,000-byte single field loads as one row");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_csv953_huge.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_csv953_huge_out.txt");

    const size_t n = 100000;
    char *buf = (char *)malloc(n + 2);
    if (!buf) {
        FAIL("out of memory building fixture");
        return;
    }
    memset(buf, 'x', n);
    buf[n] = '\n';
    if (write_bytes_file(csv_path, buf, n + 1) != 0) {
        free(buf);
        FAIL("cannot write CSV fixture");
        return;
    }
    free(buf);

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl t(a: symbol)\n"
        ".input t(filename=\"%s\", delimiter=\",\")\n"
        ".decl q(a: symbol)\n"
        "q(A) :- t(A).\n"
        ".output q\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0 || !output) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    int rows = count_lines(output);
    size_t flen = quoted_field_len(output, 0);
    if (rows != 1 || flen != n) {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "expected 1 row of %zu chars, got %d row(s), first field %zu chars",
            n, rows, flen);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 3: boundary pins, at the old fgets limit and the new chunk size     */
/* ======================================================================== */

/*
 * A single symbol field of exactly @n bytes must load as exactly one row
 * of exactly @n characters.  With @newline == 0 the file ends without a
 * line terminator, which is the case that makes the spill buffer's
 * reserved NUL slot load-bearing: nothing was consumed to make room for
 * it, so an under-reservation writes one byte past the allocation.
 */
static void
run_single_field_case_nl(size_t n, int newline, const char *label)
{
    TEST(label);

    char csv_path[512];
    char out_path[512];
    char fname[80];
    snprintf(fname, sizeof(fname), "wl_csv953_b%zu%s.csv", n,
        newline ? "" : "n");
    test_tmppath(csv_path, sizeof(csv_path), fname);
    snprintf(fname, sizeof(fname), "wl_csv953_b%zu%s_out.txt", n,
        newline ? "" : "n");
    test_tmppath(out_path, sizeof(out_path), fname);

    char *buf = (char *)malloc(n + 2);
    if (!buf) {
        FAIL("out of memory building fixture");
        return;
    }
    memset(buf, 'x', n);
    buf[n] = '\n';
    if (write_bytes_file(csv_path, buf, n + (newline ? 1 : 0)) != 0) {
        free(buf);
        FAIL("cannot write CSV fixture");
        return;
    }
    free(buf);

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl t(a: symbol)\n"
        ".input t(filename=\"%s\", delimiter=\",\")\n"
        ".decl q(a: symbol)\n"
        "q(A) :- t(A).\n"
        ".output q\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0 || !output) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    int rows = count_lines(output);
    size_t flen = quoted_field_len(output, 0);
    if (rows != 1 || flen != n) {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "expected 1 row of %zu chars, got %d row(s), "
            "first field %zu chars", n, rows, flen);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

static void
run_single_field_case(size_t n, const char *label)
{
    run_single_field_case_nl(n, 1, label);
}

/*
 * fgets(line, 4096, f) delivered at most 4095 content bytes, so 4094 and
 * 4095 fitted while 4096 and 4097 split into two rows.  All four must
 * now be a single row of exactly the written length.
 */
static void
test_line_length_boundary(void)
{
    static const size_t lengths[] = { 4094, 4095, 4096, 4097 };

    for (size_t i = 0; i < sizeof(lengths) / sizeof(lengths[0]); i++) {
        char name[80];
        snprintf(name, sizeof(name), "line of %zu bytes is one row",
            lengths[i]);
        run_single_field_case(lengths[i], name);
    }
}

/*
 * The 4096-byte pins above are historical: they are the *old* fgets
 * boundary, and nothing about the rewritten reader makes them special.
 * The boundary that exists now is WL_CSV_READ_CHUNK, where a line stops
 * fitting in the staging buffer and has to be copied into the growable
 * spill buffer instead of being parsed in place.  Pin it at +/-1, and
 * again at twice the chunk size so the spill buffer's own growth step is
 * covered.
 *
 * These lengths are computed from the macro rather than written out, so
 * retuning the chunk size moves the coverage with it instead of quietly
 * leaving it pinned to a number that is no longer a boundary.
 */
static void
test_chunk_boundary(void)
{
    const size_t k = WL_CSV_READ_CHUNK;
    const size_t lengths[] = { k - 1, k, k + 1, 2 * k - 1, 2 * k, 2 * k + 1 };

    for (size_t i = 0; i < sizeof(lengths) / sizeof(lengths[0]); i++) {
        char name[96];
        snprintf(name, sizeof(name),
            "line of %zu bytes (chunk %zu) is one row", lengths[i], k);
        run_single_field_case(lengths[i], name);
    }

    /*
     * The same boundary with no trailing newline.  When a line ends at
     * EOF there is no consumed terminator to overwrite, so the spill
     * buffer has to have reserved the NUL slot itself; at exactly the
     * chunk size the reservation is the only thing forcing a realloc.
     */
    for (size_t i = 0; i < 3; i++) {
        char name[128];
        snprintf(name, sizeof(name),
            "line of %zu bytes with no trailing newline is one row",
            lengths[i]);
        run_single_field_case_nl(lengths[i], 0, name);
    }
}

/* ======================================================================== */
/* Case 4: the growth ceiling is enforced                                   */
/* ======================================================================== */

/*
 * A 20 MiB single line exceeds WL_CSV_MAX_LINE (16 MiB).  The reader
 * must refuse it outright rather than growing without bound or, worse,
 * silently splitting it into rows again.
 */
static void
test_line_ceiling_enforced(void)
{
    TEST("20 MiB single line is rejected, not split or grown without bound");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_csv953_ceiling.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_csv953_ceiling_out.txt");

    const size_t n = (size_t)20 * 1024 * 1024;
    const size_t block = 64 * 1024;
    char *buf = (char *)malloc(block);
    if (!buf) {
        FAIL("out of memory building fixture");
        return;
    }
    memset(buf, 'x', block);

    FILE *f = fopen(csv_path, "wb");
    if (!f) {
        free(buf);
        FAIL("cannot write CSV fixture");
        return;
    }
    for (size_t written = 0; written < n; written += block) {
        if (fwrite(buf, 1, block, f) != block) {
            fclose(f);
            free(buf);
            remove(csv_path);
            FAIL("short write building fixture");
            return;
        }
    }
    fputc('\n', f);
    fclose(f);
    free(buf);

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl t(a: symbol)\n"
        ".input t(filename=\"%s\", delimiter=\",\")\n"
        ".decl q(a: symbol)\n"
        "q(A) :- t(A).\n"
        ".output q\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc == 0) {
        int rows = count_lines(output);
        char msg[256];
        snprintf(msg, sizeof(msg),
            "expected non-zero rc for a 20 MiB line; got 0 with %d row(s)",
            rows);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 5: an embedded NUL is an error, not a silent truncation             */
/* ======================================================================== */

/*
 * strlen() on the fgets buffer stopped at the NUL, so everything after
 * it on the line was dropped and the run still exited 0.  A NUL cannot
 * be represented in an interned symbol anyway (wl_intern_put takes a
 * NUL-terminated string), so the only honest answer is to refuse.
 */
static void
test_embedded_nul_rejected(void)
{
    TEST("embedded NUL byte is rejected");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_csv953_nul.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_csv953_nul_out.txt");

    static const char bytes[] = { 'a', 'b', '\0', 'c', 'd', '\t', 'z', 'z',
                                  '\n' };
    if (write_bytes_file(csv_path, bytes, sizeof(bytes)) != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl t(a: symbol, b: symbol)\n"
        ".input t(filename=\"%s\", delimiter=\"\\t\")\n"
        ".decl q(a: symbol, b: symbol)\n"
        "q(A, B) :- t(A, B).\n"
        ".output q\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc == 0) {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "expected non-zero rc for a line containing NUL; got 0 with "
            "output:\n%.200s", output ? output : "(null)");
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 6: the field buffer cap on the direct parse entry point             */
/* ======================================================================== */

/*
 * wl_csv_parse_line_ex() is reachable without the file reader, and its
 * `char strbuf[4096]` clipped every string field at 4095 bytes while
 * still returning success.  A 10,000-byte field must intern whole.
 */
static void
test_parse_line_ex_long_field(void)
{
    TEST("wl_csv_parse_line_ex interns a 10,000-byte field whole");

    const size_t n = 10000;
    char *line = (char *)malloc(n + 1);
    if (!line) {
        FAIL("out of memory building fixture");
        return;
    }
    memset(line, 'x', n);
    line[n] = '\0';

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        free(line);
        FAIL("wl_intern_create failed");
        return;
    }

    wirelog_column_type_t types[1] = { WIRELOG_TYPE_STRING };
    int64_t values[1] = { -1 };
    uint32_t count = 0;

    int rc = wl_csv_parse_line_ex(line, ',', types, 1, values, 1, &count,
            intern);
    free(line);

    if (rc != 0 || count != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "wl_csv_parse_line_ex returned %d with count %u", rc, count);
        wl_intern_free(intern);
        FAIL(msg);
        return;
    }

    const char *got = wl_intern_reverse(intern, values[0]);
    size_t got_len = got ? strlen(got) : (size_t)-1;
    if (got_len != n) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected an interned field of %zu bytes, got %zu", n, got_len);
        wl_intern_free(intern);
        FAIL(msg);
        return;
    }

    wl_intern_free(intern);
    PASS();
}

/* ======================================================================== */
/* Case 7: regression guards for the rewritten line reader                  */
/* ======================================================================== */

/*
 * The line reader was rewritten from fgets() to fread() + memchr(), so
 * pin the surrounding line-framing behaviour it has to preserve: CRLF
 * endings, a missing final newline, blank lines, and an empty file.
 */
static void
run_framing_case(const char *label, const char *bytes, size_t len,
    int expect_rc_zero, int expect_rows)
{
    TEST(label);

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_csv953_framing.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_csv953_framing_out.txt");

    if (write_bytes_file(csv_path, bytes, len) != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl t(a: int32, b: int32)\n"
        ".input t(filename=\"%s\", delimiter=\",\")\n"
        ".decl q(a: int32, b: int32)\n"
        "q(A, B) :- t(A, B).\n"
        ".output q\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if ((rc == 0) != (expect_rc_zero != 0)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    if (expect_rc_zero) {
        int rows = count_lines(output);
        if (rows != expect_rows) {
            char msg[256];
            snprintf(msg, sizeof(msg),
                "expected %d row(s), got %d; output:\n%.200s",
                expect_rows, rows, output ? output : "(null)");
            free(output);
            FAIL(msg);
            return;
        }
    }

    free(output);
    PASS();
}

static void
test_line_framing_preserved(void)
{
    run_framing_case("CRLF line endings still frame rows",
        "1,2\r\n3,4\r\n", 10, 1, 2);
    /*
     * A blank CRLF line is the case that actually pins the terminator
     * strip: the field parsers tolerate a stray '\r' at the end of a
     * value on their own, so only a line that is *nothing but* "\r\n"
     * distinguishes stripping "\r\n" from stripping "\n".  Left as a
     * one-byte "\r" line it is not empty, and the integer parser
     * reports zero columns, failing the whole load.
     */
    run_framing_case("blank CRLF line is skipped, not parsed as a row",
        "1,2\r\n\r\n3,4\r\n", 12, 1, 2);
    run_framing_case("final line without a newline is still read",
        "1,2\n3,4", 7, 1, 2);
    run_framing_case("blank lines between rows are skipped",
        "1,2\n\n\n3,4\n", 10, 1, 2);
    run_framing_case("empty input file loads nothing and succeeds",
        "", 0, 1, 0);
}

/* ======================================================================== */
/* Case 7b: a read error is reported, not treated as end of input           */
/* ======================================================================== */

/*
 * fgets() signalled EOF and a read error identically, so a failed read
 * ended the load quietly with whatever had been parsed so far -- the
 * same failure mode as silently keeping a prefix of a line.  fread()
 * plus ferror() can tell them apart, so the reader now reports it.
 *
 * On glibc, fopen() on a directory succeeds and the first read fails
 * with EISDIR, which is a read error the test can actually provoke
 * without special privileges.  Other platforms differ (fopen may fail
 * outright, or the read may succeed), so the case is Linux-only; the
 * assertion there is only that the load does not silently succeed.
 */
static void
test_read_error_reported(void)
{
#if defined(__linux__)
    TEST("a read error is reported, not silently treated as end of input");

    char out_path[512];
    test_tmppath(out_path, sizeof(out_path), "wl_csv953_readerr_out.txt");

    /* A directory: openable, but not readable as a stream on glibc. */
    const char *dir_path = test_tmpdir();
    FILE *probe = fopen(dir_path, "r");
    if (!probe) {
        PASS(); /* platform refuses the open; nothing to distinguish */
        return;
    }
    char one;
    size_t got = fread(&one, 1, 1, probe);
    int is_err = ferror(probe);
    fclose(probe);
    if (got != 0 || !is_err) {
        PASS(); /* reading a directory works here; no error to report */
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl t(a: symbol, b: symbol)\n"
        ".input t(filename=\"%s\", delimiter=\",\")\n"
        ".decl q(a: symbol, b: symbol)\n"
        "q(A, B) :- t(A, B).\n"
        ".output q\n",
        dir_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);
    free(output);
    remove(out_path);

    if (rc == 0) {
        FAIL("expected non-zero rc when the input stream cannot be read");
        return;
    }

    PASS();
#endif
}

/* ======================================================================== */
/* Case 8: wide relation x long line -- the #953 / #997 interaction         */
/* ======================================================================== */

/*
 * #997 was a stack-buffer-overflow WRITE: the string-aware parser wrote
 * values[col] for every column of the caller's declared width into a
 * fixed int64_t row_values[256], so 257+ columns overflowed.  The fix
 * heap-allocates that buffer at the real width and passes the width to
 * the parser as an explicit @max_cols capacity.
 *
 * #953 removes the accident that used to keep this from firing on long
 * lines: while fgets() delivered a long line in 4095-byte fragments, a
 * wide relation's rows were assembled from fragments and the write count
 * per call stayed low.  Assembling the whole line makes the declared
 * width the only bound on the write.  A 300-column symbol relation whose
 * lines are ~90 KB -- long enough to straddle the staging chunk and take
 * the spill path -- is therefore the exact shape that regresses if the
 * capacity bound is dropped while collapsing the two parser bodies into
 * one.  It must load cleanly and byte-exactly, and be ASan-clean.
 */

#define WIDE_NCOLS 300
#define WIDE_NROWS 2
/* 300 cols x 320 bytes + separators ~= 96 KB, i.e. > WL_CSV_READ_CHUNK. */
#define WIDE_CELL_LEN 320

/* The cell at (@row, @col): "r<row>c<col>" padded to WIDE_CELL_LEN with a
 * digit that varies per column, so a mis-strided read is visible. */
static void
wide_cell(char *buf, int row, int col)
{
    int n = snprintf(buf, WIDE_CELL_LEN + 1, "r%dc%d", row, col);
    if (n < 0)
        n = 0;
    for (int i = n; i < WIDE_CELL_LEN; i++)
        buf[i] = (char)('0' + (col % 10));
    buf[WIDE_CELL_LEN] = '\0';
}

/* Returns a malloc'd CSV image of WIDE_NROWS x WIDE_NCOLS, or NULL. */
static char *
build_wide_long_csv(size_t *out_len)
{
    size_t line_len = (size_t)WIDE_NCOLS * (WIDE_CELL_LEN + 1); /* + sep/NL */
    size_t total = line_len * WIDE_NROWS;
    char *buf = (char *)malloc(total + 1);
    if (!buf)
        return NULL;

    char cell[WIDE_CELL_LEN + 1];
    size_t off = 0;
    for (int r = 0; r < WIDE_NROWS; r++) {
        for (int c = 0; c < WIDE_NCOLS; c++) {
            if (c > 0)
                buf[off++] = ',';
            wide_cell(cell, r, c);
            memcpy(buf + off, cell, WIDE_CELL_LEN);
            off += WIDE_CELL_LEN;
        }
        buf[off++] = '\n';
    }
    buf[off] = '\0';
    *out_len = off;
    return buf;
}

static int64_t
wide_intern_cb(void *opaque, const char *str)
{
    return wl_intern_put((wl_intern_t *)opaque, str);
}

/*
 * Asserted cell for cell through wl_csv_read_file_via_ctx(): an
 * overflowing build still reports the right shape, so only the contents
 * distinguish it -- and under ASan it aborts outright.
 */
static void
test_wide_long_line_direct(void)
{
    TEST("300 symbol columns on a ~96 KB line round-trip intact");

    char csv_path[512];
    char msg[256];
    test_tmppath(csv_path, sizeof(csv_path), "wl_csv953_wide_long.csv");

    size_t len = 0;
    char *csv = build_wide_long_csv(&len);
    if (!csv || write_bytes_file(csv_path, csv, len) != 0) {
        free(csv);
        FAIL("cannot write CSV fixture");
        return;
    }
    free(csv);

    wirelog_column_type_t *types = (wirelog_column_type_t *)malloc(
        (size_t)WIDE_NCOLS * sizeof(*types));
    wl_intern_t *intern = wl_intern_create();
    if (!types || !intern) {
        remove(csv_path);
        free(types);
        if (intern)
            wl_intern_free(intern);
        FAIL("allocation failed");
        return;
    }
    for (int i = 0; i < WIDE_NCOLS; i++)
        types[i] = WIRELOG_TYPE_STRING;

    int64_t *data = NULL;
    uint32_t nrows = 0, ncols = 0;
    int rc = wl_csv_read_file_via_ctx(csv_path, ',', types, WIDE_NCOLS, &data,
            &nrows, &ncols, wide_intern_cb, intern);

    remove(csv_path);
    free(types);

    if (rc != 0) {
        snprintf(msg, sizeof(msg), "read_file_via_ctx returned %d", rc);
        goto fail;
    }
    if (nrows != WIDE_NROWS || ncols != WIDE_NCOLS) {
        snprintf(msg, sizeof(msg), "expected %dx%d, got %ux%u", WIDE_NROWS,
            WIDE_NCOLS, nrows, ncols);
        goto fail;
    }

    for (int r = 0; r < WIDE_NROWS; r++) {
        for (int c = 0; c < WIDE_NCOLS; c++) {
            char want[WIDE_CELL_LEN + 1];
            wide_cell(want, r, c);
            const char *got
                = wl_intern_reverse(intern, data[(size_t)r * WIDE_NCOLS + c]);
            if (!got || strcmp(got, want) != 0) {
                snprintf(msg, sizeof(msg),
                    "cell [%d][%d] is \"%.32s...\", want \"%.32s...\"", r, c,
                    got ? got : "(unresolved)", want);
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

/*
 * The same fixture through the .input directive, which is the path #997
 * was first reported on (csv_read -> wl_csv_read_file_via_ctx under
 * wl_session_load_input_files).  Loaded EDB rows are not observable from
 * here, so this asserts a clean load and relies on ASan for the write
 * bound; the cell-exact assertions are in the direct case above.
 */
static void
test_wide_long_line_pipeline(void)
{
    TEST(".input: 300 symbol columns on a ~96 KB line load cleanly");

    static char src[32768];
    char csv_path[512];
    char out_path[512];
    size_t off = 0;

    test_tmppath(csv_path, sizeof(csv_path), "wl_csv953_wide_long_pipe.csv");
    test_tmppath(out_path, sizeof(out_path),
        "wl_csv953_wide_long_pipe_out.txt");

    size_t len = 0;
    char *csv = build_wide_long_csv(&len);
    if (!csv || write_bytes_file(csv_path, csv, len) != 0) {
        free(csv);
        FAIL("cannot write CSV fixture");
        return;
    }
    free(csv);

    if (appendf(src, sizeof(src), &off, ".decl wide(") != 0) {
        remove(csv_path);
        FAIL("source buffer too small");
        return;
    }
    for (int i = 0; i < WIDE_NCOLS; i++) {
        if (appendf(src, sizeof(src), &off, "%sc%d: symbol", i ? ", " : "", i)
            != 0) {
            remove(csv_path);
            FAIL("source buffer too small");
            return;
        }
    }
    if (appendf(src, sizeof(src), &off,
        ")\n.input wide(filename=\"%s\", delimiter=\",\")\n", csv_path) != 0) {
        remove(csv_path);
        FAIL("source buffer too small");
        return;
    }

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);
    free(output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        FAIL(msg);
        return;
    }

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

    printf("CSV reader line/field limit tests (Issue #953)\n");
    printf("==============================================\n");

    test_split_line_int_not_fabricated();
    test_split_line_int_true_values();
    test_split_line_str_not_fabricated();
    test_huge_single_field();
    test_line_length_boundary();
    test_chunk_boundary();
    test_line_ceiling_enforced();
    test_embedded_nul_rejected();
    test_parse_line_ex_long_field();
    test_line_framing_preserved();
    test_read_error_reported();
    test_wide_long_line_direct();
    test_wide_long_line_pipeline();

    printf("\n%d run, %d passed, %d failed\n",
        tests_run, tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
