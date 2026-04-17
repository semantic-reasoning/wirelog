/*
 * wirelog/util/log_emit.c - wirelog Structured Logger emit path.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * In commit 1 this file holds a cold no-op body; commit 3 replaces with the
 * real vsnprintf + single-fwrite implementation. A separate TU makes it easy
 * for test binaries to link the testhook variant instead.
 */

#include "wirelog/util/log.h"

#include <stdarg.h>

__attribute__((cold))
void
wl_log_emit(wl_log_section_t sec, wl_log_level_t lvl,
    const char *file, int line, const char *fmt, ...)
{
    (void)sec; (void)lvl; (void)file; (void)line; (void)fmt;
    /* Real body lands in commit 3. */
}
