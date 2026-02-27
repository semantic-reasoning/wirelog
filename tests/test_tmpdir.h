/*
 * test_tmpdir.h - Cross-platform temp directory helper
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Provides test_tmpdir() for portable temp file paths across
 * Unix (/tmp) and Windows (%TEMP% / %TMP%).
 */

#ifndef WIRELOG_TEST_TMPDIR_H
#define WIRELOG_TEST_TMPDIR_H

#include <stdio.h>
#include <stdlib.h>

static const char *
test_tmpdir(void)
{
#ifdef _WIN32
    const char *d = getenv("TEMP");
    if (!d)
        d = getenv("TMP");
    if (!d)
        d = ".";
    return d;
#else
    return "/tmp";
#endif
}

/*
 * Build a temp file path: {tmpdir}/{filename} into caller-supplied buffer.
 */
static void
test_tmppath(char *buf, size_t bufsz, const char *filename)
{
    snprintf(buf, bufsz, "%s/%s", test_tmpdir(), filename);
}

#endif /* WIRELOG_TEST_TMPDIR_H */
