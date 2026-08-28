#include "wirelog/wirelog-extension.h"
#include <stdio.h>
#include <string.h>

static int invoke(const wirelog_extension_value_t *a, uint32_t n,
    wirelog_extension_value_t *r, void *u) {
    (void)a; (void)n; (void)r; (void)u; return 0;
}
static int destroy_called;
static void destroy_reentrant(void *user_data)
{
    destroy_called++;
    (void)wirelog_extension_unregister(
        (wirelog_extension_registry_t *)user_data, "not-present");
}
static int check(int condition, const char *message) {
    if (!condition){
        fprintf(stderr, "FAIL: %s\n", message); return 1;
    }
    return 0;
}

int main(void)
{
    wirelog_extension_registry_t *r = wirelog_extension_registry_create();
    wirelog_extension_snapshot_t *s;
    wirelog_extension_snapshot_t *s2;
    const wirelog_extension_descriptor_t *d;
    uint32_t types[] = { WIRELOG_EXTENSION_VALUE_INT64 };
    char name[] = " Math::Sin ";
    wirelog_extension_descriptor_t desc = { WIRELOG_EXTENSION_ABI_VERSION,
                                            sizeof(desc), name, 1, types,
                                            WIRELOG_EXTENSION_VALUE_INT64,
                                            invoke, NULL, NULL };
    wirelog_extension_descriptor_t bad = desc;
    int failures = 0;
    failures += check(r != NULL, "create");
    bad.abi_version++;
    failures += check(wirelog_extension_register(r,
            &bad) == -1 && strstr(wirelog_extension_last_error(), "ABI"),
            "ABI validation");
    bad = desc;
    bad.size = (uint32_t)(offsetof(wirelog_extension_descriptor_t,
        result_type));
    failures += check(wirelog_extension_register(r,
            &bad) == -1 && strstr(wirelog_extension_last_error(), "size"),
            "size validation");
    {
        wirelog_extension_descriptor_t large = desc;
        large.arity = UINT32_MAX;
        large.destroy = destroy_reentrant;
        large.user_data = r;
        if ((uint64_t)UINT32_MAX
            > (uint64_t)(SIZE_MAX / sizeof(uint32_t))) {
            large.argument_types = types;
            failures += check(wirelog_extension_register(r, &large) == -1
                    && strcmp(wirelog_extension_last_error(),
                    "extension arity is too large") == 0
                    && destroy_called == 0, "arity overflow validation");
        } else {
            large.argument_types = NULL;
            failures += check(wirelog_extension_register(r, &large) == -1
                    && strcmp(wirelog_extension_last_error(),
                    "invalid descriptor") == 0
                    && destroy_called == 0, "large arity invalid descriptor");
        }
    }
    failures += check(wirelog_extension_register(r, &desc) == 0, "register");
    s = wirelog_extension_snapshot_acquire(r);
    s2 = wirelog_extension_snapshot_acquire(r);
    failures += check(wirelog_extension_snapshot_find(s, "Math::Sin") != NULL,
            "normalized lookup");
    failures += check(wirelog_extension_register(r,
            &desc) == -1 && strstr(wirelog_extension_last_error(), "duplicate"),
            "duplicate error");
    name[0] = 'X';
    failures += check(wirelog_extension_snapshot_pin(s, "Math::Sin", &d) == 0,
            "first pin");
    failures += check(wirelog_extension_snapshot_pin(s, "Math::Sin", NULL) == 0,
            "repeated pin");
    failures += check(wirelog_extension_snapshot_pin(s2, "Math::Sin",
            NULL) == 0, "second snapshot pin");
    failures += check(wirelog_extension_unregister(r, "math::sin") == -1,
            "case-sensitive names");
    failures += check(wirelog_extension_unregister(r,
            " Math::Sin ") == -1 && strstr(wirelog_extension_last_error(),
            "pinned"), "pinned unregister rejection");
    failures += check(wirelog_extension_snapshot_unpin(s2, d) == 0,
            "unpin own snapshot");
    failures += check(wirelog_extension_snapshot_unpin(s2,
            d) == -1 && strstr(wirelog_extension_last_error(), "this snapshot"),
            "cannot consume another snapshot pin");
    wirelog_extension_snapshot_release(s);
    failures += check(wirelog_extension_unregister(r, "Math::Sin") == 0,
            "unregister after release drops pins");
    wirelog_extension_snapshot_release(s2);
    {
        wirelog_extension_descriptor_t owned = { WIRELOG_EXTENSION_ABI_VERSION,
                                                 sizeof(owned), "Destroy.Me", 0,
                                                 NULL,
                                                 WIRELOG_EXTENSION_VALUE_INT64,
                                                 invoke, r, destroy_reentrant };
        failures += check(wirelog_extension_register(r, &owned) == 0,
                "destroy callback register");
        failures += check(wirelog_extension_register(r,
                &owned) == -1 && destroy_called == 0,
                "duplicate discard does not destroy");
        failures += check(wirelog_extension_unregister(r,
                "Destroy.Me") == 0 && destroy_called == 1,
                "destroy callback outside lock");
    }
    failures += check(wirelog_extension_registry_destroy(r) == 0, "destroy");
    return failures != 0;
}
