/*
 * wirelog_session_step() reports a typed delta failure for the step that
 * failed, not for every later step (#2345).
 *
 * A typed delta is delivered through a layout that typed_emit() allocates.
 * When that allocation fails the step returns WIRELOG_ERR_EXEC.  The flag
 * recording it was never cleared by a later step, so every following
 * successful step also returned WIRELOG_ERR_EXEC.
 *
 * Linux only: the failure is injected with -Wl,--wrap=calloc.
 */

#include "../wirelog/wirelog-advanced.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

#ifdef __linux__
void *__real_calloc(size_t count, size_t size);

/* Armed from inside the typed callback: the next one-column layout array
 * typed_emit() allocates for the following delta fails. */
static bool fail_next_layout;
static unsigned failed_layouts;

void *
__wrap_calloc(size_t count, size_t size)
{
    if (fail_next_layout && count == 1 && size == sizeof(uint32_t)) {
        fail_next_layout = false;
        failed_layouts++;
        return NULL;
    }
    return __real_calloc(count, size);
}

static unsigned deltas;
static bool arm_on_first;

static void
typed_delta(const char *relation, const wirelog_typed_row_v1_t *row,
    int32_t diff, void *data)
{
    (void)relation;
    (void)row;
    (void)diff;
    (void)data;
    if (deltas++ == 0 && arm_on_first)
        fail_next_layout = true;
}

int
main(void)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl a(x:int64)\n.decl b(x:int64)\nb(X) :- a(X).\n", &err);
    CHECK(program && err == WIRELOG_OK);
    wirelog_session_t *s = NULL;
    CHECK(wirelog_session_create(program, WIRELOG_BACKEND_DEFAULT, 1, &s)
        == WIRELOG_OK && s);
    CHECK(wirelog_session_set_typed_delta_cb(s, typed_delta, NULL)
        == WIRELOG_OK);

    /* A step whose second typed delta cannot be delivered fails ... */
    int64_t rows[] = { 1, 2 };
    CHECK(wirelog_session_insert(s, "a", rows, 2, 1) == WIRELOG_OK);
    arm_on_first = true;
    CHECK(wirelog_session_step(s) == WIRELOG_ERR_EXEC);
    arm_on_first = false;
    /* The undeliverable delta suppressed its callback. */
    CHECK(failed_layouts == 1 && !fail_next_layout && deltas == 1);

    /* ... and the next step, whose deltas are all delivered, succeeds. */
    int64_t more = 3;
    CHECK(wirelog_session_insert(s, "a", &more, 1, 1) == WIRELOG_OK);
    deltas = 0;
    CHECK(wirelog_session_step(s) == WIRELOG_OK);
    CHECK(deltas >= 1 && failed_layouts == 1);

    wirelog_session_destroy(s);
    wirelog_program_free(program);
    puts("advanced typed step failure: the flag is per step");
    return 0;
}
#else
int
main(void)
{
    puts("SKIP: calloc wrapping needs GNU ld --wrap (Linux)");
    return 77;
}
#endif
