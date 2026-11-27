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

int
main(void)
{
    char source[2048];
    int n = snprintf(source, sizeof(source), "%s",
            ".decl seed(x:int64)\n.decl edge(x:int64,y:int64)\n"
            ".decl p(x:int64)\n.decl q(x:int64)\n"
            "p(X) :- seed(X).\nq(X) :- p(X).\n"
            "p(Y) :- q(X), edge(X,Y).\nseed(0).\n");
    for (int i = 0; i < LENGTH; i++)
        n += snprintf(source + n, sizeof(source) - (size_t)n,
                "edge(%d,%d).\n", i, i + 1);
    CHECK(n > 0 && (size_t)n < sizeof(source));

    for (int delta = 0; delta < 2; delta++) {
        int rc = 0;
        set_limit(1);
        CHECK(run(source, delta, &rc) == 0);
        CHECK(rc != 0);
        set_limit(0);
        CHECK(run(source, delta, &rc) == LENGTH + 1);
        CHECK(rc == 0);
    }
    puts("cli iteration cap: snapshot and delta modes fail at the limit");
    return 0;
}
