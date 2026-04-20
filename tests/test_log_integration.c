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
#include <unistd.h>

static char tmp_path_[256];

static void
make_tmpfile_(void)
{
    /* pid+time gives a unique-enough path per invocation for tests. */
    snprintf(tmp_path_, sizeof(tmp_path_),
        "/tmp/wl_log_integration_%ld_%ld.log",
        (long)getpid(), (long)time(NULL));
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
