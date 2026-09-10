/* Public source-reader lifetime qualification (Issue #1497). */

#include "wirelog/wirelog-advanced.h"
#include "wirelog/wirelog-extension.h"
#include "wirelog/wirelog.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static const char *SOURCE =
    ".decl edge(x: int64, y: int64)\n"
    ".decl event(id: int64, payload: metadata/2 side)\n"
    ".decl reach(x: int64, y: int64)\n"
    ".decl seen(id: int64)\n"
    "reach(x, y) :- edge(x, y), @call(\"test.lifetime\", x).\n"
    "reach(x, z) :- reach(x, y), edge(y, z).\n"
    "seen(id) :- event(id, _).\n";

typedef struct {
    uint32_t reach_rows;
    uint32_t seen_rows;
} snapshot_counts_t;

static int evaluation_should_fail;

static int
lifetime_extension_invoke(const wirelog_extension_value_t *args,
    uint32_t nargs, wirelog_extension_value_t *result, void *user_data)
{
    (void)args;
    (void)nargs;
    (void)user_data;
    if (evaluation_should_fail)
        return 1;
    result->type = WIRELOG_EXTENSION_VALUE_BOOL;
    result->size = sizeof(uint8_t);
    result->as.bool_value = 1;
    return 0;
}

static void
count_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    snapshot_counts_t *counts = user_data;
    (void)row;
    (void)ncols;
    if (!relation || !counts)
        return;
    if (strcmp(relation, "reach") == 0)
        counts->reach_rows++;
    else if (strcmp(relation, "seen") == 0)
        counts->seen_rows++;
}

int
main(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(SOURCE, &error);
    wirelog_session_t *session = NULL;
    wirelog_extension_registry_t *registry = NULL;
    wirelog_extension_snapshot_t *extension_snapshot = NULL;
    wirelog_compound_arg_t args[2] = {
        { WIRELOG_TYPE_INT64, 7 },
        { WIRELOG_TYPE_INT64, 11 },
    };
    uint64_t handle = WIRELOG_COMPOUND_HANDLE_NULL;
    int64_t event_row[2];
    int64_t edge_rows[] = { 1, 2, 2, 3 };
    snapshot_counts_t snapshot = { 0 };
    int failures = 0;

    if (!program) {
        fprintf(stderr, "parse failed: %s\n", wirelog_error_string(error));
        return 1;
    }
    const uint32_t argument_types[] = { WIRELOG_EXTENSION_VALUE_INT64 };
    wirelog_extension_descriptor_t descriptor = {
        WIRELOG_EXTENSION_ABI_VERSION, sizeof(descriptor),
        "test.lifetime", 1, argument_types,
        WIRELOG_EXTENSION_VALUE_BOOL, lifetime_extension_invoke, NULL, NULL
    };
    registry = wirelog_extension_registry_create();
    if (!registry || wirelog_extension_register(registry, &descriptor) != 0) {
        fprintf(stderr, "extension setup failed\n");
        wirelog_program_free(program);
        if (registry)
            wirelog_extension_registry_destroy(registry);
        return 1;
    }
    extension_snapshot = wirelog_extension_snapshot_acquire(registry);
    if (!extension_snapshot
        || wirelog_session_create_with_snapshot(program,
            WIRELOG_BACKEND_COLUMNAR, 2, extension_snapshot, &session)
            != WIRELOG_OK || !session) {
        fprintf(stderr, "public session creation failed\n");
        wirelog_extension_snapshot_release(extension_snapshot);
        wirelog_extension_registry_destroy(registry);
        wirelog_program_free(program);
        return 1;
    }
    wirelog_extension_snapshot_release(extension_snapshot);
    extension_snapshot = NULL;
    wirelog_extension_unregister(registry, descriptor.name);

    if (wirelog_session_make_compound(session, "metadata", 2, args,
            &handle) != WIRELOG_OK
        || handle == WIRELOG_COMPOUND_HANDLE_NULL) {
        fprintf(stderr, "side-compound creation failed\n");
        failures++;
    }
    event_row[0] = 1;
    event_row[1] = (int64_t)handle;
    if (wirelog_session_insert(session, "event", event_row, 1, 2)
            != WIRELOG_OK
        || wirelog_session_insert(session, "edge", edge_rows, 2, 2)
            != WIRELOG_OK) {
        fprintf(stderr, "source insertion failed\n");
        failures++;
    }

    /* Fail inside the public evaluation after arrangement/worker setup, then
     * retry the same session successfully.  This is the operation-boundary
     * error cleanup qualification; the internal bundle tests cover rollback
     * counters and exact lease slots. */
    evaluation_should_fail = 1;
    if (wirelog_session_snapshot(session, count_rows, &snapshot)
            != WIRELOG_ERR_EXEC) {
        fprintf(stderr, "evaluation failure did not report WIRELOG_ERR_EXEC\n");
        failures++;
    }
    evaluation_should_fail = 0;
    snapshot = (snapshot_counts_t){ 0 };
    if (wirelog_session_snapshot(session, count_rows, &snapshot)
            != WIRELOG_OK
        || snapshot.reach_rows != 3 || snapshot.seen_rows != 1) {
        fprintf(stderr, "post-error evaluation failed (reach=%u seen=%u)\n",
            snapshot.reach_rows, snapshot.seen_rows);
        failures++;
    }

    wirelog_session_destroy(session);
    /* This ordering is the public lifetime contract: destroy is synchronous,
     * so the borrowed program can be freed immediately afterwards. */
    wirelog_program_free(program);
    if (wirelog_extension_registry_destroy(registry) != 0)
        failures++;
    return failures != 0;
}
