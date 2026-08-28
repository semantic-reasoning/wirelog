#include "session.h"
#include "wirelog/wirelog-extension.h"
#include <stdio.h>
#include <stdlib.h>

static int destroy_calls;

static int
invoke(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *user_data)
{
    (void)args;
    (void)nargs;
    (void)result;
    (void)user_data;
    return 0;
}

static void
destroy_extension(void *user_data)
{
    (void)user_data;
    destroy_calls++;
}

static int
fake_session_create(const wl_plan_t *plan, uint32_t num_workers,
    wl_session_t **out)
{
    (void)plan;
    (void)num_workers;
    *out = (wl_session_t *)calloc(1, sizeof(**out));
    return *out ? 0 : -1;
}

static void
fake_session_destroy(wl_session_t *session)
{
    free(session);
}

static int
check(int condition, const char *message)
{
    if (!condition)
        fprintf(stderr, "FAIL: %s\n", message);
    return condition ? 0 : 1;
}

int
main(void)
{
    const wl_compute_backend_t backend = {
        "test", fake_session_create, fake_session_destroy
    };
    const wl_compute_backend_t no_destroy_backend = {
        "no-destroy", fake_session_create, NULL
    };
    wirelog_extension_registry_t *registry;
    wirelog_extension_snapshot_t *snapshot;
    wirelog_extension_descriptor_t descriptor = {
        WIRELOG_EXTENSION_ABI_VERSION,
        sizeof(descriptor),
        "test.scalar",
        0,
        NULL,
        WIRELOG_EXTENSION_VALUE_INT64,
        invoke,
        NULL,
        destroy_extension
    };
    wl_session_t *session = NULL;
    int failures = 0;

    registry = wirelog_extension_registry_create();
    failures += check(registry != NULL, "registry create");
    failures += check(wirelog_extension_register(registry, &descriptor) == 0,
            "extension register");
    snapshot = wirelog_extension_snapshot_acquire(registry);
    failures += check(snapshot != NULL, "snapshot acquire");
    failures += check(wl_session_create_with_snapshot(&backend, NULL, 2,
            snapshot, &session) == 0, "session create with snapshot");
    failures += check(session->extension_snapshot == snapshot,
            "session stores snapshot");

    wirelog_extension_snapshot_retain(snapshot);
    wirelog_extension_snapshot_release(snapshot);
    wirelog_extension_snapshot_release(snapshot);
    failures += check(wirelog_extension_unregister(registry, "test.scalar")
            == 0, "unregister while session owns snapshot");
    failures += check(destroy_calls == 0,
            "entry remains alive while session owns snapshot");

    wl_session_destroy(session);
    failures += check(destroy_calls == 1,
            "entry destroyed after session teardown");

    failures += check(wirelog_extension_register(registry, &descriptor) == 0,
            "re-register for missing backend destroy");
    snapshot = wirelog_extension_snapshot_acquire(registry);
    failures += check(snapshot != NULL, "snapshot acquire without destroy");
    {
        wl_session_t no_destroy_session = {
            &no_destroy_backend, snapshot, true
        };
        failures += check(wirelog_extension_unregister(registry,
                "test.scalar") == 0, "unregister without backend destroy");
        wl_session_destroy(&no_destroy_session);
    }
    failures += check(destroy_calls == 2,
            "snapshot released without backend destroy");
    failures += check(wirelog_extension_registry_destroy(registry) == 0,
            "registry destroy");
    return failures != 0;
}
