#include "wirelog/wirelog-extension.h"
#include <stdio.h>
#include <stdlib.h>
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
    desc.addon_abi_identity = UINT64_C(0x0123456789abcdef);
    desc.addon_abi_version = 7;
    failures += check(wirelog_extension_register(r, &desc) == 0,
            "metadata descriptor register");
    s = wirelog_extension_snapshot_acquire(r);
    d = wirelog_extension_snapshot_find(s, "Math::Sin");
    failures += check(d != NULL && d->addon_abi_identity
            == UINT64_C(0x0123456789abcdef) && d->addon_abi_version == 7,
            "metadata copied into snapshot");
    wirelog_extension_snapshot_release(s);
    failures += check(wirelog_extension_unregister(r, "Math::Sin") == 0,
            "metadata descriptor unregister");
    desc.addon_abi_identity = 0;
    desc.addon_abi_version = 0;
    {
        const size_t old_size = offsetof(wirelog_extension_descriptor_t,
                addon_abi_identity);
        unsigned char *old_storage = malloc(old_size);
        failures += check(old_storage != NULL, "legacy descriptor allocation");
        if (!old_storage) return failures != 0;
        memset(old_storage, 0, old_size);
        memcpy(old_storage, &desc, old_size);
        ((wirelog_extension_descriptor_t *)old_storage)->size =
            (uint32_t)old_size;
        failures += check(wirelog_extension_register(r,
                (const wirelog_extension_descriptor_t *)old_storage) == 0,
                "legacy descriptor register");
        s = wirelog_extension_snapshot_acquire(r);
        d = wirelog_extension_snapshot_find(s, "Math::Sin");
        failures += check(d != NULL && d->addon_abi_identity == 0
                && d->addon_abi_version == 0,
                "legacy metadata defaults");
        wirelog_extension_snapshot_release(s);
        failures += check(wirelog_extension_unregister(r, "Math::Sin") == 0,
                "legacy descriptor unregister");
        free(old_storage);
    }
    desc.addon_abi_identity = 1;
    desc.addon_abi_version = 0;
    failures += check(wirelog_extension_register(r, &desc) == -1
            && strstr(wirelog_extension_last_error(), "metadata"),
            "half-populated metadata rejected");
    desc.addon_abi_identity = 0;
    desc.addon_abi_version = 7;
    failures += check(wirelog_extension_register(r, &desc) == -1
            && strstr(wirelog_extension_last_error(), "metadata"),
            "half-populated metadata rejected 2");
    desc.addon_abi_version = 0;
    desc.size = (uint32_t)sizeof(desc) + 64;
    failures += check(wirelog_extension_register(r, &desc) == 0,
            "oversized descriptor register");
    s = wirelog_extension_snapshot_acquire(r);
    d = wirelog_extension_snapshot_find(s, "Math::Sin");
    failures += check(d != NULL && d->size == sizeof(*d),
            "oversized descriptor size is clamped");
    wirelog_extension_snapshot_release(s);
    failures += check(wirelog_extension_unregister(r, "Math::Sin") == 0,
            "oversized descriptor unregister");
    desc.size = sizeof(desc);
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
    bad = desc;
    bad.result_type = 99;
    failures += check(wirelog_extension_register(r, &bad) == -1
            && strcmp(wirelog_extension_last_error(),
            "invalid descriptor") == 0,
            "result type validation");
    bad = desc;
    types[0] = 99;
    failures += check(wirelog_extension_register(r, &bad) == -1
            && strcmp(wirelog_extension_last_error(),
            "invalid descriptor") == 0,
            "argument type validation");
    types[0] = WIRELOG_EXTENSION_VALUE_INT64;
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
