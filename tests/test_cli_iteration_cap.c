/*
 * The CLI fails at the fixed-point iteration limit (#2345).
 *
 * wl_run_pipeline() used a raw snapshot or step, which return 0 at the
 * limit; a capped snapshot printed the partial model and the run succeeded.
 * Each mode must now fail and print no row or delta of the capped
 * evaluation.  The limit is lowered with the WL_SESSION_TEST_HOOKS override.
 */

#include "../wirelog/cli/driver.h"
#include "../wirelog/columnar/internal.h"

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

enum { LENGTH = 13 };

static void
set_limit(uint32_t limit)
{
    wl_columnar_eval_serial_test_iteration_limit = limit;
    wl_columnar_session_test_iteration_limit = limit;
}

/* Append to buf at *pos, treating truncation as failure.  snprintf returns
 * the length it would have written, so accumulating that return value lets
 * *pos run past the buffer once anything truncates. */
static bool
append_fmt(char *buf, size_t bufsize, size_t *pos, const char *fmt, ...)
{
    if (*pos >= bufsize)
        return false;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf + *pos, bufsize - *pos, fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= bufsize - *pos)
        return false;
    *pos += (size_t)n;
    return true;
}

/* Run the pipeline into a file under the test's working directory and
 * return the number of output lines naming p. */
static unsigned
run(const char *source, bool delta_mode, int *rc)
{
    const char *path = "test_cli_iteration_cap.out";
    FILE *out = fopen(path, "w+");
    CHECK(out);
    *rc = wl_run_pipeline(source, 1, delta_mode, false, 0, out);
    CHECK(fflush(out) == 0 && fseek(out, 0, SEEK_SET) == 0);
    char line[256];
    unsigned rows = 0;
    while (fgets(line, sizeof(line), out))
        if (strstr(line, "p("))
            rows++;
    fclose(out);
    remove(path);
    return rows;
}

/* Watch mode over a stdin file whose final line has no newline.  The
 * closure starts with 10 edges (complete under limit 3); the second watched
 * edge lengthens it past what limit 3 allows, so the step for the final
 * line fails, and the run must fail with it. */
static int
run_watch(const char *final_suffix)
{
    char source[2048];
    size_t n = 0;
    CHECK(append_fmt(source, sizeof(source), &n, "%s",
        ".decl seed(x:int64)\n.decl edge(x:int64,y:int64)\n"
        ".decl p(x:int64)\n.decl q(x:int64)\n"
        "p(X) :- seed(X).\nq(X) :- p(X).\n"
        "p(Y) :- q(X), edge(X,Y).\nseed(0).\n"));
    for (int i = 0; i < 10; i++)
        CHECK(append_fmt(source, sizeof(source), &n, "edge(%d,%d).\n", i,
            i + 1));
    const char *in_path = "test_cli_iteration_cap.in";
    FILE *in = fopen(in_path, "w");
    CHECK(in);
    fprintf(in, "edge 10 11\nedge 11 12%s", final_suffix);
    CHECK(fclose(in) == 0);
    CHECK(freopen(in_path, "r", stdin));
    FILE *out = fopen("test_cli_iteration_cap.out", "w");
    CHECK(out);
    set_limit(3);
    int rc = wl_run_pipeline(source, 1, false, true, 0, out);
    set_limit(0);
    fclose(out);
    remove("test_cli_iteration_cap.out");
    remove(in_path);
    return rc;
}

int
main(void)
{
    char source[2048];
    size_t n = 0;
    CHECK(append_fmt(source, sizeof(source), &n, "%s",
        ".decl seed(x:int64)\n.decl edge(x:int64,y:int64)\n"
        ".decl p(x:int64)\n.decl q(x:int64)\n"
        "p(X) :- seed(X).\nq(X) :- p(X).\n"
        "p(Y) :- q(X), edge(X,Y).\nseed(0).\n"));
    for (int i = 0; i < LENGTH; i++)
        CHECK(append_fmt(source, sizeof(source), &n, "edge(%d,%d).\n", i,
            i + 1));

    for (int delta = 0; delta < 2; delta++) {
        int rc = 0;
        set_limit(1);
        CHECK(run(source, delta, &rc) == 0);
        CHECK(rc != 0);
        set_limit(0);
        CHECK(run(source, delta, &rc) == LENGTH + 1);
        CHECK(rc == 0);
    }
    CHECK(run_watch("\n") != 0);
    CHECK(run_watch("") != 0);
    puts(
        "cli iteration cap: snapshot, delta and watch modes fail at the limit");
    return 0;
}
