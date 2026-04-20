/*
 * wirelog/util/log_testhook.c - test-only emit capture.
 *
 * NEVER linked into libwirelog. Linked only into gating test binaries via
 * the wirelog_log_testhook_sources meson list. Enforced by
 * scripts/ci/check-no-testhook-in-libwirelog.sh.
 *
 * Test binaries that include this TU must NOT also link log_emit.c — the
 * C linker would reject the duplicate wl_log_emit definition. Each gating
 * test executable is built from {log.c, log_testhook.c} with log_emit.c
 * deliberately omitted.
 */

#include "wirelog/util/log.h"

#include <stdarg.h>
#include <stdio.h>

/* Public test-observable state. Tests read these to verify that emit was
 * (or was not) invoked. wl_log_test_last holds the most recent formatted
 * line; wl_log_test_count is the total emit count since the last reset. */
char wl_log_test_last[512];
int wl_log_test_count;

void
wl_log_test_reset(void)
{
    wl_log_test_last[0] = '\0';
    wl_log_test_count = 0;
}

WL_LOG_ATTR_COLD
void
wl_log_emit(wl_log_section_t sec, wl_log_level_t lvl,
    const char *file, int line, const char *fmt, ...)
{
    (void)sec;
    (void)lvl;
    (void)file;
    (void)line;

    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(wl_log_test_last, sizeof(wl_log_test_last), fmt, ap);
    va_end(ap);
    (void)n;
    ++wl_log_test_count;
}
