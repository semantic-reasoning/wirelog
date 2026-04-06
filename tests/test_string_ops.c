/*
 * test_string_ops.c - String Operations Unit Tests (Issue #143)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Unit tests for string_ops.c: UTF-8 utilities and all 13 string functions
 * operating on interned string IDs.
 */

#include "../wirelog/string_ops.h"
#include "../wirelog/intern.h"

#include <inttypes.h>
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

/* ======================================================================== */
/* UTF-8 Utilities: utf8_strlen                                            */
/* ======================================================================== */

static void
test_utf8_strlen_null(void)
{
    TEST("utf8_strlen: NULL returns 0");
    if (string_ops_utf8_strlen(NULL) != 0) {
        FAIL("expected 0 for NULL input");
        return;
    }
    PASS();
}

static void
test_utf8_strlen_empty(void)
{
    TEST("utf8_strlen: empty string returns 0");
    if (string_ops_utf8_strlen("") != 0) {
        FAIL("expected 0 for empty string");
        return;
    }
    PASS();
}

static void
test_utf8_strlen_ascii(void)
{
    TEST("utf8_strlen: ASCII-only returns byte count");
    if (string_ops_utf8_strlen("hello") != 5) {
        FAIL("expected 5 codepoints for \"hello\"");
        return;
    }
    PASS();
}

static void
test_utf8_strlen_2byte(void)
{
    /* e-acute U+00E9 = 0xC3 0xA9 (2 bytes, 1 codepoint) */
    TEST("utf8_strlen: 2-byte codepoint counts as 1");
    if (string_ops_utf8_strlen("\xC3\xA9") != 1) {
        FAIL("expected 1 codepoint for 2-byte UTF-8");
        return;
    }
    PASS();
}

static void
test_utf8_strlen_3byte(void)
{
    /* Korean 'han' U+D55C = 0xED 0x95 0x9C (3 bytes, 1 codepoint) */
    TEST("utf8_strlen: 3-byte codepoint counts as 1");
    if (string_ops_utf8_strlen("\xED\x95\x9C") != 1) {
        FAIL("expected 1 codepoint for 3-byte UTF-8");
        return;
    }
    PASS();
}

static void
test_utf8_strlen_4byte(void)
{
    /* U+1F600 = 0xF0 0x9F 0x98 0x80 (4 bytes, 1 codepoint) */
    TEST("utf8_strlen: 4-byte codepoint counts as 1");
    if (string_ops_utf8_strlen("\xF0\x9F\x98\x80") != 1) {
        FAIL("expected 1 codepoint for 4-byte UTF-8");
        return;
    }
    PASS();
}

static void
test_utf8_strlen_mixed(void)
{
    /* 'a' (1) + U+00E9 (2) + U+D55C (3) + U+1F600 (4) = 4 codepoints */
    TEST("utf8_strlen: mixed ASCII and multibyte returns codepoint count");
    if (string_ops_utf8_strlen("a\xC3\xA9\xED\x95\x9C\xF0\x9F\x98\x80") != 4) {
        FAIL("expected 4 codepoints");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* UTF-8 Utilities: utf8_next                                              */
/* ======================================================================== */

static void
test_utf8_next_ascii(void)
{
    TEST("utf8_next: ASCII codepoint decoded correctly");
    int64_t cp = 0;
    const char *next = string_ops_utf8_next("A", &cp);
    if (!next || cp != 65) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected cp=65, got %" PRId64, cp);
        FAIL(buf);
        return;
    }
    if (*next != '\0') {
        FAIL("expected next to point to NUL after single char");
        return;
    }
    PASS();
}

static void
test_utf8_next_2byte(void)
{
    /* U+00E9 = 233 */
    TEST("utf8_next: 2-byte codepoint (U+00E9) decoded correctly");
    int64_t cp = 0;
    const char *next = string_ops_utf8_next("\xC3\xA9", &cp);
    if (!next || cp != 0xE9) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0xE9 (233), got 0x%" PRIx64, cp);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_utf8_next_3byte(void)
{
    /* U+D55C = 54620 */
    TEST("utf8_next: 3-byte codepoint (U+D55C) decoded correctly");
    int64_t cp = 0;
    const char *next = string_ops_utf8_next("\xED\x95\x9C", &cp);
    if (!next || cp != 0xD55C) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0xD55C (54620), got 0x%" PRIx64,
            cp);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_utf8_next_4byte(void)
{
    /* U+1F600 = 128512 */
    TEST("utf8_next: 4-byte codepoint (U+1F600) decoded correctly");
    int64_t cp = 0;
    const char *next = string_ops_utf8_next("\xF0\x9F\x98\x80", &cp);
    if (!next || cp != 0x1F600) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0x1F600 (128512), got 0x%" PRIx64,
            cp);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_utf8_next_at_nul(void)
{
    TEST("utf8_next: returns NULL at NUL terminator");
    const char *next = string_ops_utf8_next("", NULL);
    if (next != NULL) {
        FAIL("expected NULL at NUL terminator");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* strlen                                                                   */
/* ======================================================================== */

static void
test_strlen_empty(void)
{
    TEST("strlen: empty string returns 0");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "");
    int64_t len = string_ops_strlen(id, intern);
    wl_intern_free(intern);
    if (len != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0, got %" PRId64, len);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_strlen_ascii(void)
{
    TEST("strlen: ASCII string returns codepoint count equal to byte count");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    int64_t len = string_ops_strlen(id, intern);
    wl_intern_free(intern);
    if (len != 5) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 5, got %" PRId64, len);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_strlen_multibyte(void)
{
    /* "a" + U+00E9 + U+D55C = 3 codepoints, 6 bytes */
    TEST("strlen: multibyte string returns codepoint count not byte count");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "a\xC3\xA9\xED\x95\x9C");
    int64_t len = string_ops_strlen(id, intern);
    wl_intern_free(intern);
    if (len != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 codepoints, got %" PRId64, len);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_strlen_null_intern(void)
{
    TEST("strlen: NULL intern returns -1");
    if (string_ops_strlen(0, NULL) != -1) {
        FAIL("expected -1 for NULL intern");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* cat                                                                      */
/* ======================================================================== */

static void
test_cat_two_nonempty(void)
{
    TEST("cat: concatenates two nonempty strings");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id1 = wl_intern_put(intern, "hello");
    int64_t id2 = wl_intern_put(intern, " world");
    int64_t result = string_ops_cat(id1, id2, intern);
    if (result < 0) {
        FAIL("cat returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "hello world") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"hello world\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_cat_empty_plus_nonempty(void)
{
    TEST("cat: empty + nonempty returns nonempty string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_empty = wl_intern_put(intern, "");
    int64_t id_str = wl_intern_put(intern, "hello");
    int64_t result = string_ops_cat(id_empty, id_str, intern);
    if (result < 0) {
        FAIL("cat returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "hello") != 0) {
        FAIL("expected \"hello\"");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_cat_both_empty(void)
{
    TEST("cat: empty + empty returns empty string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_empty = wl_intern_put(intern, "");
    int64_t result = string_ops_cat(id_empty, id_empty, intern);
    if (result < 0) {
        FAIL("cat returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "") != 0) {
        FAIL("expected empty string");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_cat_result_length(void)
{
    TEST("cat: result length equals sum of input lengths");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id1 = wl_intern_put(intern, "abc");
    int64_t id2 = wl_intern_put(intern, "de");
    int64_t result = string_ops_cat(id1, id2, intern);
    if (result < 0) {
        FAIL("cat returned error");
        wl_intern_free(intern);
        return;
    }
    int64_t len = string_ops_strlen(result, intern);
    wl_intern_free(intern);
    if (len != 5) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected length 5, got %" PRId64, len);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_cat_null_intern(void)
{
    TEST("cat: NULL intern returns -1");
    if (string_ops_cat(0, 0, NULL) != -1) {
        FAIL("expected -1 for NULL intern");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* substr                                                                   */
/* ======================================================================== */

static void
test_substr_full_string(void)
{
    TEST("substr: start=0 len=full returns entire string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    int64_t result = string_ops_substr(id, 0, 5, intern);
    if (result < 0) {
        FAIL("substr returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "hello") != 0) {
        FAIL("expected \"hello\"");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_substr_partial(void)
{
    TEST("substr: partial extraction at codepoint range");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello world");
    int64_t result = string_ops_substr(id, 6, 5, intern);
    if (result < 0) {
        FAIL("substr returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "world") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"world\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_substr_zero_length(void)
{
    TEST("substr: zero length returns empty string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    int64_t result = string_ops_substr(id, 0, 0, intern);
    if (result < 0) {
        FAIL("substr returned error for zero length");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "") != 0) {
        FAIL("expected empty string for zero length");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_substr_out_of_bounds_start(void)
{
    TEST("substr: start beyond string length returns -1");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hi");
    int64_t result = string_ops_substr(id, 10, 1, intern);
    wl_intern_free(intern);
    if (result != -1) {
        FAIL("expected -1 for out-of-bounds start");
        return;
    }
    PASS();
}

static void
test_substr_negative_start(void)
{
    TEST("substr: negative start returns -1");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    int64_t result = string_ops_substr(id, -1, 2, intern);
    wl_intern_free(intern);
    if (result != -1) {
        FAIL("expected -1 for negative start");
        return;
    }
    PASS();
}

static void
test_substr_length_exceeds_remaining(void)
{
    TEST("substr: length exceeding remaining codepoints truncates at end");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    /* start=3, len=100: from 'l' to end = "lo" */
    int64_t result = string_ops_substr(id, 3, 100, intern);
    if (result < 0) {
        FAIL("substr returned error for length exceeding remaining");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "lo") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"lo\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_substr_multibyte_codepoint_boundary(void)
{
    /* "a" + U+00E9 + U+D55C = codepoints at 0, 1, 2 */
    TEST("substr: respects codepoint boundaries in multibyte string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "a\xC3\xA9\xED\x95\x9C");
    /* Extract codepoint 1 (U+00E9), length 1 */
    int64_t result = string_ops_substr(id, 1, 1, intern);
    if (result < 0) {
        FAIL("substr returned error on multibyte boundary");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "\xC3\xA9") != 0) {
        FAIL("expected UTF-8 bytes for U+00E9 (e-acute)");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

/* ======================================================================== */
/* contains                                                                 */
/* ======================================================================== */

static void
test_contains_found(void)
{
    TEST("contains: returns true when substring is present");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_hay = wl_intern_put(intern, "hello world");
    int64_t id_needle = wl_intern_put(intern, "world");
    bool found = string_ops_contains(id_hay, id_needle, intern);
    wl_intern_free(intern);
    if (!found) {
        FAIL("expected true: \"hello world\" contains \"world\"");
        return;
    }
    PASS();
}

static void
test_contains_not_found(void)
{
    TEST("contains: returns false when substring is absent");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_hay = wl_intern_put(intern, "hello");
    int64_t id_needle = wl_intern_put(intern, "xyz");
    bool found = string_ops_contains(id_hay, id_needle, intern);
    wl_intern_free(intern);
    if (found) {
        FAIL("expected false: \"hello\" does not contain \"xyz\"");
        return;
    }
    PASS();
}

static void
test_contains_empty_needle(void)
{
    TEST("contains: empty needle matches any string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_hay = wl_intern_put(intern, "hello");
    int64_t id_empty = wl_intern_put(intern, "");
    bool found = string_ops_contains(id_hay, id_empty, intern);
    wl_intern_free(intern);
    if (!found) {
        FAIL("expected true: empty needle matches any string");
        return;
    }
    PASS();
}

static void
test_contains_empty_haystack(void)
{
    TEST("contains: nonempty needle in empty haystack returns false");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_empty = wl_intern_put(intern, "");
    int64_t id_needle = wl_intern_put(intern, "x");
    bool found = string_ops_contains(id_empty, id_needle, intern);
    wl_intern_free(intern);
    if (found) {
        FAIL("expected false: nonempty needle not in empty haystack");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* str_prefix                                                               */
/* ======================================================================== */

static void
test_str_prefix_match(void)
{
    TEST("str_prefix: returns true when string starts with prefix");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_str = wl_intern_put(intern, "hello world");
    int64_t id_prefix = wl_intern_put(intern, "hello");
    bool result = string_ops_str_prefix(id_str, id_prefix, intern);
    wl_intern_free(intern);
    if (!result) {
        FAIL("expected true: \"hello world\" starts with \"hello\"");
        return;
    }
    PASS();
}

static void
test_str_prefix_no_match(void)
{
    TEST("str_prefix: returns false when prefix does not match");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_str = wl_intern_put(intern, "hello");
    int64_t id_prefix = wl_intern_put(intern, "world");
    bool result = string_ops_str_prefix(id_str, id_prefix, intern);
    wl_intern_free(intern);
    if (result) {
        FAIL("expected false: \"hello\" does not start with \"world\"");
        return;
    }
    PASS();
}

static void
test_str_prefix_empty_pattern(void)
{
    TEST("str_prefix: empty prefix matches any string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_str = wl_intern_put(intern, "hello");
    int64_t id_empty = wl_intern_put(intern, "");
    bool result = string_ops_str_prefix(id_str, id_empty, intern);
    wl_intern_free(intern);
    if (!result) {
        FAIL("expected true: empty prefix matches any string");
        return;
    }
    PASS();
}

static void
test_str_prefix_full_match(void)
{
    TEST("str_prefix: prefix equal to full string returns true");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    bool result = string_ops_str_prefix(id, id, intern);
    wl_intern_free(intern);
    if (!result) {
        FAIL("expected true: string is prefix of itself");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* str_suffix                                                               */
/* ======================================================================== */

static void
test_str_suffix_match(void)
{
    TEST("str_suffix: returns true when string ends with suffix");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_str = wl_intern_put(intern, "hello world");
    int64_t id_suffix = wl_intern_put(intern, "world");
    bool result = string_ops_str_suffix(id_str, id_suffix, intern);
    wl_intern_free(intern);
    if (!result) {
        FAIL("expected true: \"hello world\" ends with \"world\"");
        return;
    }
    PASS();
}

static void
test_str_suffix_no_match(void)
{
    TEST("str_suffix: returns false when suffix does not match");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_str = wl_intern_put(intern, "hello");
    int64_t id_suffix = wl_intern_put(intern, "xyz");
    bool result = string_ops_str_suffix(id_str, id_suffix, intern);
    wl_intern_free(intern);
    if (result) {
        FAIL("expected false: \"hello\" does not end with \"xyz\"");
        return;
    }
    PASS();
}

static void
test_str_suffix_empty_pattern(void)
{
    TEST("str_suffix: empty suffix matches any string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_str = wl_intern_put(intern, "hello");
    int64_t id_empty = wl_intern_put(intern, "");
    bool result = string_ops_str_suffix(id_str, id_empty, intern);
    wl_intern_free(intern);
    if (!result) {
        FAIL("expected true: empty suffix matches any string");
        return;
    }
    PASS();
}

static void
test_str_suffix_longer_than_string(void)
{
    TEST("str_suffix: suffix longer than haystack returns false");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id_str = wl_intern_put(intern, "hi");
    int64_t id_suffix = wl_intern_put(intern, "hello");
    bool result = string_ops_str_suffix(id_str, id_suffix, intern);
    wl_intern_free(intern);
    if (result) {
        FAIL("expected false: suffix is longer than haystack");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* str_ord                                                                  */
/* ======================================================================== */

static void
test_str_ord_ascii(void)
{
    TEST("str_ord: ASCII 'A' returns codepoint 65");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "A");
    int64_t cp = string_ops_str_ord(id, intern);
    wl_intern_free(intern);
    if (cp != 65) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 65, got %" PRId64, cp);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_str_ord_2byte(void)
{
    /* U+00E9 = 233 */
    TEST("str_ord: 2-byte codepoint (U+00E9) returns 233");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "\xC3\xA9");
    int64_t cp = string_ops_str_ord(id, intern);
    wl_intern_free(intern);
    if (cp != 0xE9) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0xE9 (233), got 0x%" PRIx64, cp);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_str_ord_3byte(void)
{
    /* U+D55C = 54620 */
    TEST("str_ord: 3-byte codepoint (U+D55C) returns 54620");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "\xED\x95\x9C");
    int64_t cp = string_ops_str_ord(id, intern);
    wl_intern_free(intern);
    if (cp != 0xD55C) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0xD55C (54620), got 0x%" PRIx64,
            cp);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_str_ord_4byte(void)
{
    /* U+1F600 = 128512 */
    TEST("str_ord: 4-byte codepoint (U+1F600) returns 128512");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "\xF0\x9F\x98\x80");
    int64_t cp = string_ops_str_ord(id, intern);
    wl_intern_free(intern);
    if (cp != 0x1F600) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0x1F600 (128512), got 0x%" PRIx64,
            cp);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_str_ord_empty(void)
{
    TEST("str_ord: empty string returns -1");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "");
    int64_t cp = string_ops_str_ord(id, intern);
    wl_intern_free(intern);
    if (cp != -1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected -1, got %" PRId64, cp);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_str_ord_uses_first_codepoint(void)
{
    TEST("str_ord: returns codepoint of first character only");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    int64_t cp = string_ops_str_ord(id, intern);
    wl_intern_free(intern);
    if (cp != 'h') {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected %d ('h'), got %" PRId64, (int)'h',
            cp);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* to_upper / to_lower                                                      */
/* ======================================================================== */

static void
test_to_upper_ascii_lowercase(void)
{
    TEST("to_upper: ASCII a-z converted to A-Z");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    int64_t result = string_ops_to_upper(id, intern);
    if (result < 0) {
        FAIL("to_upper returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "HELLO") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"HELLO\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_to_upper_preserves_non_ascii(void)
{
    /* "caf" + U+00E9: only ASCII bytes are uppercased */
    TEST("to_upper: non-ASCII bytes are preserved unchanged (Phase 1)");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "caf\xC3\xA9");
    int64_t result = string_ops_to_upper(id, intern);
    if (result < 0) {
        FAIL("to_upper returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "CAF\xC3\xA9") != 0) {
        FAIL("expected ASCII uppercased, non-ASCII bytes unchanged");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_to_upper_empty(void)
{
    TEST("to_upper: empty string returns empty string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "");
    int64_t result = string_ops_to_upper(id, intern);
    if (result < 0) {
        FAIL("to_upper returned error on empty string");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "") != 0) {
        FAIL("expected empty string");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_to_lower_ascii_uppercase(void)
{
    TEST("to_lower: ASCII A-Z converted to a-z");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "HELLO");
    int64_t result = string_ops_to_lower(id, intern);
    if (result < 0) {
        FAIL("to_lower returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "hello") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"hello\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_to_lower_preserves_non_ascii(void)
{
    /* "CAF" + U+00E9: only ASCII bytes are lowercased */
    TEST("to_lower: non-ASCII bytes are preserved unchanged (Phase 1)");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "CAF\xC3\xA9");
    int64_t result = string_ops_to_lower(id, intern);
    if (result < 0) {
        FAIL("to_lower returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "caf\xC3\xA9") != 0) {
        FAIL("expected ASCII lowercased, non-ASCII bytes unchanged");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

/* ======================================================================== */
/* str_replace                                                              */
/* ======================================================================== */

static void
test_str_replace_single_occurrence(void)
{
    TEST("str_replace: replaces single occurrence");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello world");
    int64_t id_old = wl_intern_put(intern, "world");
    int64_t id_new = wl_intern_put(intern, "there");
    int64_t result = string_ops_str_replace(id, id_old, id_new, intern);
    if (result < 0) {
        FAIL("str_replace returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "hello there") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"hello there\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_str_replace_multiple_occurrences(void)
{
    TEST("str_replace: replaces all occurrences");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "aabbaa");
    int64_t id_old = wl_intern_put(intern, "aa");
    int64_t id_new = wl_intern_put(intern, "X");
    int64_t result = string_ops_str_replace(id, id_old, id_new, intern);
    if (result < 0) {
        FAIL("str_replace returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "XbbX") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"XbbX\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_str_replace_empty_needle(void)
{
    TEST("str_replace: empty needle returns original id unchanged");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    int64_t id_empty = wl_intern_put(intern, "");
    int64_t id_new = wl_intern_put(intern, "X");
    int64_t result = string_ops_str_replace(id, id_empty, id_new, intern);
    wl_intern_free(intern);
    if (result != id) {
        FAIL("expected original id for empty needle");
        return;
    }
    PASS();
}

static void
test_str_replace_no_match(void)
{
    TEST("str_replace: no match returns original id unchanged");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    int64_t id_old = wl_intern_put(intern, "xyz");
    int64_t id_new = wl_intern_put(intern, "abc");
    int64_t result = string_ops_str_replace(id, id_old, id_new, intern);
    wl_intern_free(intern);
    if (result != id) {
        FAIL("expected original id when no match");
        return;
    }
    PASS();
}

static void
test_str_replace_delete_occurrences(void)
{
    TEST("str_replace: replacing with empty string deletes occurrences");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello world");
    int64_t id_old = wl_intern_put(intern, " ");
    int64_t id_new = wl_intern_put(intern, "");
    int64_t result = string_ops_str_replace(id, id_old, id_new, intern);
    if (result < 0) {
        FAIL("str_replace returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "helloworld") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"helloworld\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

/* ======================================================================== */
/* trim                                                                     */
/* ======================================================================== */

static void
test_trim_leading_and_trailing(void)
{
    TEST("trim: removes both leading and trailing whitespace");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "  hello  ");
    int64_t result = string_ops_trim(id, intern);
    if (result < 0) {
        FAIL("trim returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "hello") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"hello\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_trim_no_whitespace(void)
{
    TEST("trim: no surrounding whitespace leaves string unchanged");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "hello");
    int64_t result = string_ops_trim(id, intern);
    if (result < 0) {
        FAIL("trim returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "hello") != 0) {
        FAIL("expected unchanged \"hello\"");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_trim_all_whitespace(void)
{
    TEST("trim: all-whitespace string becomes empty");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "   ");
    int64_t result = string_ops_trim(id, intern);
    if (result < 0) {
        FAIL("trim returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "") != 0) {
        FAIL("expected empty string after trimming all whitespace");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_trim_tabs_and_newlines(void)
{
    TEST("trim: strips tabs and newlines from both ends");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "\t\nhello\r\n");
    int64_t result = string_ops_trim(id, intern);
    if (result < 0) {
        FAIL("trim returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "hello") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"hello\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_trim_empty(void)
{
    TEST("trim: empty string stays empty");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "");
    int64_t result = string_ops_trim(id, intern);
    if (result < 0) {
        FAIL("trim returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "") != 0) {
        FAIL("expected empty string");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

/* ======================================================================== */
/* to_string                                                                */
/* ======================================================================== */

static void
test_to_string_positive(void)
{
    TEST("to_string: positive integer produces decimal string");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t result = string_ops_to_string(42, intern);
    if (result < 0) {
        FAIL("to_string returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "42") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"42\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_to_string_zero(void)
{
    TEST("to_string: zero produces \"0\"");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t result = string_ops_to_string(0, intern);
    if (result < 0) {
        FAIL("to_string returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "0") != 0) {
        FAIL("expected \"0\"");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_to_string_negative(void)
{
    TEST("to_string: negative integer includes leading minus sign");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t result = string_ops_to_string(-1, intern);
    if (result < 0) {
        FAIL("to_string returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "-1") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"-1\", got \"%s\"",
            s ? s : "(null)");
        FAIL(buf);
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

static void
test_to_string_large(void)
{
    TEST("to_string: large positive integer converts correctly");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t result = string_ops_to_string(1000000000LL, intern);
    if (result < 0) {
        FAIL("to_string returned error");
        wl_intern_free(intern);
        return;
    }
    const char *s = wl_intern_reverse(intern, result);
    if (!s || strcmp(s, "1000000000") != 0) {
        FAIL("expected \"1000000000\"");
        wl_intern_free(intern);
        return;
    }
    wl_intern_free(intern);
    PASS();
}

/* ======================================================================== */
/* to_number                                                                */
/* ======================================================================== */

static void
test_to_number_valid(void)
{
    TEST("to_number: valid decimal string returns integer value");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "42");
    int64_t result = string_ops_to_number(id, intern);
    wl_intern_free(intern);
    if (result != 42) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 42, got %" PRId64, result);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_to_number_negative(void)
{
    TEST("to_number: negative decimal string returns negative value");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "-7");
    int64_t result = string_ops_to_number(id, intern);
    wl_intern_free(intern);
    if (result != -7) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected -7, got %" PRId64, result);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_to_number_leading_zeros(void)
{
    TEST("to_number: leading zeros parsed as base-10");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "007");
    int64_t result = string_ops_to_number(id, intern);
    wl_intern_free(intern);
    if (result != 7) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 7, got %" PRId64, result);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_to_number_numeric_prefix(void)
{
    TEST("to_number: numeric prefix parsed, alphabetic suffix ignored");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "42abc");
    int64_t result = string_ops_to_number(id, intern);
    wl_intern_free(intern);
    if (result != 42) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 42 for \"42abc\", got %" PRId64,
            result);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_to_number_non_numeric(void)
{
    TEST("to_number: non-numeric string returns 0");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "abc");
    int64_t result = string_ops_to_number(id, intern);
    wl_intern_free(intern);
    if (result != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0 for \"abc\", got %" PRId64,
            result);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_to_number_empty(void)
{
    TEST("to_number: empty string returns 0");
    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed"); return;
    }
    int64_t id = wl_intern_put(intern, "");
    int64_t result = string_ops_to_number(id, intern);
    wl_intern_free(intern);
    if (result != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0 for empty string, got %" PRId64,
            result);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* NULL intern error handling for all functions                            */
/* ======================================================================== */

static void
test_null_intern_all_functions(void)
{
    TEST("null intern: all functions return error value");

    if (string_ops_strlen(0, NULL) != -1) {
        FAIL("strlen: expected -1");
        return;
    }
    if (string_ops_cat(0, 0, NULL) != -1) {
        FAIL("cat: expected -1");
        return;
    }
    if (string_ops_substr(0, 0, 0, NULL) != -1) {
        FAIL("substr: expected -1");
        return;
    }
    if (string_ops_contains(0, 0, NULL) != false) {
        FAIL("contains: expected false");
        return;
    }
    if (string_ops_str_prefix(0, 0, NULL) != false) {
        FAIL("str_prefix: expected false");
        return;
    }
    if (string_ops_str_suffix(0, 0, NULL) != false) {
        FAIL("str_suffix: expected false");
        return;
    }
    if (string_ops_str_ord(0, NULL) != -1) {
        FAIL("str_ord: expected -1");
        return;
    }
    if (string_ops_to_upper(0, NULL) != -1) {
        FAIL("to_upper: expected -1");
        return;
    }
    if (string_ops_to_lower(0, NULL) != -1) {
        FAIL("to_lower: expected -1");
        return;
    }
    if (string_ops_str_replace(0, 0, 0, NULL) != -1) {
        FAIL("str_replace: expected -1");
        return;
    }
    if (string_ops_trim(0, NULL) != -1) {
        FAIL("trim: expected -1");
        return;
    }
    if (string_ops_to_string(0, NULL) != -1) {
        FAIL("to_string: expected -1");
        return;
    }
    if (string_ops_to_number(0, NULL) != 0) {
        FAIL("to_number: expected 0");
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
    printf("=== String Operations Unit Tests (Issue #143) ===\n");

    printf("\n--- UTF-8 Utilities: utf8_strlen ---\n");
    test_utf8_strlen_null();
    test_utf8_strlen_empty();
    test_utf8_strlen_ascii();
    test_utf8_strlen_2byte();
    test_utf8_strlen_3byte();
    test_utf8_strlen_4byte();
    test_utf8_strlen_mixed();

    printf("\n--- UTF-8 Utilities: utf8_next ---\n");
    test_utf8_next_ascii();
    test_utf8_next_2byte();
    test_utf8_next_3byte();
    test_utf8_next_4byte();
    test_utf8_next_at_nul();

    printf("\n--- strlen ---\n");
    test_strlen_empty();
    test_strlen_ascii();
    test_strlen_multibyte();
    test_strlen_null_intern();

    printf("\n--- cat ---\n");
    test_cat_two_nonempty();
    test_cat_empty_plus_nonempty();
    test_cat_both_empty();
    test_cat_result_length();
    test_cat_null_intern();

    printf("\n--- substr ---\n");
    test_substr_full_string();
    test_substr_partial();
    test_substr_zero_length();
    test_substr_out_of_bounds_start();
    test_substr_negative_start();
    test_substr_length_exceeds_remaining();
    test_substr_multibyte_codepoint_boundary();

    printf("\n--- contains ---\n");
    test_contains_found();
    test_contains_not_found();
    test_contains_empty_needle();
    test_contains_empty_haystack();

    printf("\n--- str_prefix ---\n");
    test_str_prefix_match();
    test_str_prefix_no_match();
    test_str_prefix_empty_pattern();
    test_str_prefix_full_match();

    printf("\n--- str_suffix ---\n");
    test_str_suffix_match();
    test_str_suffix_no_match();
    test_str_suffix_empty_pattern();
    test_str_suffix_longer_than_string();

    printf("\n--- str_ord ---\n");
    test_str_ord_ascii();
    test_str_ord_2byte();
    test_str_ord_3byte();
    test_str_ord_4byte();
    test_str_ord_empty();
    test_str_ord_uses_first_codepoint();

    printf("\n--- to_upper / to_lower ---\n");
    test_to_upper_ascii_lowercase();
    test_to_upper_preserves_non_ascii();
    test_to_upper_empty();
    test_to_lower_ascii_uppercase();
    test_to_lower_preserves_non_ascii();

    printf("\n--- str_replace ---\n");
    test_str_replace_single_occurrence();
    test_str_replace_multiple_occurrences();
    test_str_replace_empty_needle();
    test_str_replace_no_match();
    test_str_replace_delete_occurrences();

    printf("\n--- trim ---\n");
    test_trim_leading_and_trailing();
    test_trim_no_whitespace();
    test_trim_all_whitespace();
    test_trim_tabs_and_newlines();
    test_trim_empty();

    printf("\n--- to_string ---\n");
    test_to_string_positive();
    test_to_string_zero();
    test_to_string_negative();
    test_to_string_large();

    printf("\n--- to_number ---\n");
    test_to_number_valid();
    test_to_number_negative();
    test_to_number_leading_zeros();
    test_to_number_numeric_prefix();
    test_to_number_non_numeric();
    test_to_number_empty();

    printf("\n--- Error Handling ---\n");
    test_null_intern_all_functions();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
