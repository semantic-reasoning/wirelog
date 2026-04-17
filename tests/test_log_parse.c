/*
 * tests/test_log_parse.c - WL_LOG env parser unit tests (Issue #287).
 *
 * TDD: these tests were written first against a stub implementation that
 * failed (returned -1 for everything); the real parser in wirelog/util/log.c
 * makes them pass. Bisecting to a single commit is preserved because the
 * tests and the parser land together here.
 *
 * The parser is pure: no process-env reads.
 */

#include "wirelog/util/log.h"

#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

static void
expect_eq_(uint8_t got, uint8_t want, const char *ctx)
{
    if (got != want) {
        fprintf(stderr, "FAIL: %s got=%u want=%u\n", ctx, (unsigned)got,
            (unsigned)want);
        assert(0);
    }
}

static void
test_empty(void)
{
    uint8_t t[WL_LOG_SEC__COUNT];
    memset(t, 0xAA, sizeof(t));
    int rc = wl_log_parse_spec("", t);
    assert(rc == 0);
    for (unsigned i = 0; i < WL_LOG_SEC__COUNT; ++i)
        expect_eq_(t[i], 0, "empty");

    rc = wl_log_parse_spec(NULL, t);
    assert(rc == 0);
    for (unsigned i = 0; i < WL_LOG_SEC__COUNT; ++i)
        expect_eq_(t[i], 0, "null");
}

static void
test_single(void)
{
    uint8_t t[WL_LOG_SEC__COUNT];
    int rc = wl_log_parse_spec("JOIN:4", t);
    assert(rc == 0);
    expect_eq_(t[WL_LOG_SEC_JOIN], 4, "JOIN:4");
    expect_eq_(t[WL_LOG_SEC_CONSOLIDATION], 0, "JOIN:4 non-JOIN");
}

static void
test_wildcard(void)
{
    uint8_t t[WL_LOG_SEC__COUNT];
    int rc = wl_log_parse_spec("*:2", t);
    assert(rc == 0);
    for (unsigned i = 0; i < WL_LOG_SEC__COUNT; ++i)
        expect_eq_(t[i], 2, "*:2");
}

static void
test_wildcard_then_override(void)
{
    uint8_t t[WL_LOG_SEC__COUNT];
    int rc = wl_log_parse_spec("*:2,JOIN:5", t);
    assert(rc == 0);
    expect_eq_(t[WL_LOG_SEC_JOIN], 5, "*:2,JOIN:5 join");
    expect_eq_(t[WL_LOG_SEC_CONSOLIDATION], 2, "*:2,JOIN:5 non-join");
}

static void
test_last_wins(void)
{
    uint8_t t[WL_LOG_SEC__COUNT];
    int rc = wl_log_parse_spec("JOIN:3,JOIN:5", t);
    assert(rc == 0);
    expect_eq_(t[WL_LOG_SEC_JOIN], 5, "last-wins");
}

static void
test_whitespace_and_case(void)
{
    uint8_t t[WL_LOG_SEC__COUNT];
    int rc = wl_log_parse_spec(" join : 4 ", t);
    assert(rc == 0);
    expect_eq_(t[WL_LOG_SEC_JOIN], 4, "whitespace+case");
}

static void
test_malformed_missing_colon(void)
{
    uint8_t t[WL_LOG_SEC__COUNT];
    memset(t, 0xAA, sizeof(t));
    int rc = wl_log_parse_spec("JOIN", t);
    assert(rc == -1);
    for (unsigned i = 0; i < WL_LOG_SEC__COUNT; ++i)
        expect_eq_(t[i], 0, "malformed missing-colon zeros");
}

static void
test_malformed_out_of_range(void)
{
    uint8_t t[WL_LOG_SEC__COUNT];
    int rc = wl_log_parse_spec("JOIN:9", t);
    assert(rc == -1);
    rc = wl_log_parse_spec("JOIN:a", t);
    assert(rc == -1);
}

static void
test_unknown_section_silently_skipped(void)
{
    uint8_t t[WL_LOG_SEC__COUNT];
    int rc = wl_log_parse_spec("NOSUCH:3", t);
    assert(rc == 0);
    for (unsigned i = 0; i < WL_LOG_SEC__COUNT; ++i)
        expect_eq_(t[i], 0, "unknown section ignored");

    rc = wl_log_parse_spec("NOSUCH:3,JOIN:2", t);
    assert(rc == 0);
    expect_eq_(t[WL_LOG_SEC_JOIN], 2, "unknown mixed with known");
    expect_eq_(t[WL_LOG_SEC_CONSOLIDATION], 0, "unknown mixed non-join");
}

static void
test_section_name_roundtrip(void)
{
    for (unsigned i = 0; i < WL_LOG_SEC__COUNT; ++i) {
        const char *n = wl_log_section_name((wl_log_section_t)i);
        assert(n && *n);
        wl_log_section_t back = wl_log_section_from_name(n);
        assert(back == (wl_log_section_t)i);
    }
    assert(wl_log_section_from_name("NOSUCH") == WL_LOG_SEC__COUNT);
    assert(wl_log_section_from_name(NULL) == WL_LOG_SEC__COUNT);
    assert(wl_log_section_from_name("") == WL_LOG_SEC__COUNT);
}

int
main(void)
{
    test_empty();
    test_single();
    test_wildcard();
    test_wildcard_then_override();
    test_last_wins();
    test_whitespace_and_case();
    test_malformed_missing_colon();
    test_malformed_out_of_range();
    test_unknown_section_silently_skipped();
    test_section_name_roundtrip();
    puts("test_log_parse OK");
    return 0;
}
