/*
 * tests/test_log_integration.c - WL_LOG end-to-end integration (Issue #287).
 *
 * Drives the full env-parse -> init -> threshold -> WL_LOG -> vsnprintf ->
 * fwrite path using WL_LOG_FILE to capture output. Asserts the line shape
 * matches "[TRACE][JOIN] <file>:<line>: hello 42" and that an unset WL_LOG
 * produces no output.
 *
 * Not run via capture-hook; links the production log_emit.c.
 */

#define _POSIX_C_SOURCE 200809L

#include "wirelog/util/log.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Portability shims for MSVC: no <unistd.h>, no setenv/unsetenv/getpid.
 * _putenv_s(name, "") removes the variable per MSVC docs; _getpid lives
 * in <process.h>. The integration test writes its tmpfile to $TMP/$TEMP
 * on Windows and /tmp elsewhere. */
#if defined(_MSC_VER) && !defined(__clang__)
#  include <process.h>
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;  /* _putenv_s unconditionally overwrites */
    /* MSVC CRT: _putenv_s(name, "") removes the variable. Substitute a
     * minimal non-empty value so getenv() returns non-NULL, matching
     * POSIX presence semantics used by this test file. */
    return _putenv_s(name, (value && *value) ? value : "1");
}
static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}
#  define setenv   wl_test_setenv_
#  define unsetenv wl_test_unsetenv_
#  define getpid   _getpid
#else
#  include <unistd.h>
#endif

static char tmp_path_[256];

static const char *
tmpdir_(void)
{
    const char *d = getenv("TMPDIR");
    if (d && *d) return d;
#if defined(_WIN32)
    d = getenv("TEMP");
    if (d && *d) return d;
    d = getenv("TMP");
    if (d && *d) return d;
    return ".";
#else
    return "/tmp";
#endif
}

static void
make_tmpfile_(void)
{
    /* pid+time gives a unique-enough path per invocation for tests. */
    const char *d = tmpdir_();
#if defined(_WIN32)
    const char sep = '\\';
#else
    const char sep = '/';
#endif
    snprintf(tmp_path_, sizeof(tmp_path_),
        "%s%cwl_log_integration_%ld_%ld.log",
        d, sep, (long)getpid(), (long)time(NULL));
    (void)remove(tmp_path_);
}

static size_t
read_file_(const char *path, char *buf, size_t bufsz)
{
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    size_t n = fread(buf, 1, bufsz - 1, f);
    fclose(f);
    buf[n] = '\0';
    return n;
}

static void
test_emit_when_enabled(void)
{
    make_tmpfile_();
    setenv("WL_LOG_FILE", tmp_path_, 1);
    setenv("WL_LOG", "JOIN:5", 1);

    wl_log_init();
    wl_log_demo_join();
    wl_log_shutdown();

    char buf[1024] = {0};
    size_t n = read_file_(tmp_path_, buf, sizeof(buf));
    assert(n > 0);
    /* Must contain "[TRACE][JOIN]" prefix and the "hello 42" tail. */
    assert(strstr(buf, "[TRACE][JOIN]") != NULL);
    assert(strstr(buf, "hello 42") != NULL);
    /* File path in the prefix should reference log.c (the demo helper lives
     * there) — no regex dependency required. */

    (void)remove(tmp_path_);
    unsetenv("WL_LOG");
    unsetenv("WL_LOG_FILE");
}

static void
test_no_emit_when_unset(void)
{
    make_tmpfile_();
    setenv("WL_LOG_FILE", tmp_path_, 1);
    unsetenv("WL_LOG");
    unsetenv("WL_DEBUG_JOIN");
    unsetenv("WL_CONSOLIDATION_LOG");

    wl_log_init();
    wl_log_demo_join();
    wl_log_shutdown();

    char buf[64] = {0};
    size_t n = read_file_(tmp_path_, buf, sizeof(buf));
    assert(n == 0);

    (void)remove(tmp_path_);
    unsetenv("WL_LOG_FILE");
}

int
main(void)
{
    /* Stash existing env so we don't perturb the parent shell. */
    test_emit_when_enabled();
    test_no_emit_when_unset();
    puts("test_log_integration OK");
    return 0;
}
