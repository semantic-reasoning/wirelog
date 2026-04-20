/*
 * wirelog/util/log_emit.c - wirelog Structured Logger emit path.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Production emit path: vsnprintf into a 512-byte stack buffer, single
 * fwrite to the cached sink. Kept in a dedicated TU so gating test binaries
 * can substitute log_testhook.c without duplicate-symbol errors.
 *
 * Hot-path properties:
 *   - __attribute__((cold)) nudges the compiler to keep the disabled branch
 *     straight. The caller (WL_LOG macro) guards entry; reaching this
 *     function is off the performance-sensitive path by construction.
 *   - No dynamic allocation; no mutex. FILE-internal locking via fwrite
 *     provides message atomicity on POSIX.
 *   - Not reentrant: wl_log_emit MUST NOT call WL_LOG.
 *   - Not async-signal-safe: vsnprintf and fwrite are not AS-safe.
 */

#include "wirelog/util/log.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

/* Provided by log.c. Declared here rather than in the public header so the
 * FILE* sink stays an implementation detail of the logger module. */
extern FILE *wl_log_sink_get_(void);

static const char *const wl_log_level_names_[WL_LOG_LEVEL_MAX + 1] = {
    [WL_LOG_NONE] = "NONE",
    [WL_LOG_ERROR] = "ERROR",
    [WL_LOG_WARN] = "WARN",
    [WL_LOG_INFO] = "INFO",
    [WL_LOG_DEBUG] = "DEBUG",
    [WL_LOG_TRACE] = "TRACE",
};

static const char *
level_name_(wl_log_level_t lvl)
{
    if ((unsigned)lvl > (unsigned)WL_LOG_LEVEL_MAX)
        return "?";
    return wl_log_level_names_[lvl];
}

WL_LOG_ATTR_COLD
void
wl_log_emit(wl_log_section_t sec, wl_log_level_t lvl,
    const char *file, int line, const char *fmt, ...)
{
    char buf[512];
    int off = snprintf(buf, sizeof(buf), "[%s][%s] %s:%d: ",
            level_name_(lvl), wl_log_section_name(sec),
            file ? file : "?", line);
    if (off < 0) {
        off = 0;
    }
    if ((size_t)off >= sizeof(buf)) {
        off = (int)sizeof(buf) - 1;
    }

    va_list ap;
    va_start(ap, fmt);
    int body = vsnprintf(buf + off, sizeof(buf) - (size_t)off, fmt, ap);
    va_end(ap);

    size_t total = (body < 0) ? (size_t)off : (size_t)off + (size_t)body;
    if (total >= sizeof(buf)) {
        /* Silent truncation with a "...\n" sentinel in the last 4 bytes so
         * readers can tell the line was cut. */
        total = sizeof(buf) - 1;
        memcpy(buf + sizeof(buf) - 5, "...\n", 4);
        buf[sizeof(buf) - 1] = '\0';
    } else if (total == 0 || buf[total - 1] != '\n') {
        if (total < sizeof(buf) - 1) {
            buf[total++] = '\n';
            buf[total] = '\0';
        }
    }

    FILE *sink = wl_log_sink_get_();
    (void)fwrite(buf, 1, total, sink);
}
