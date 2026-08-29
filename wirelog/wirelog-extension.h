/*
 * wirelog-extension.h - public scalar-function addon ABI
 */

/**
 * @file wirelog-extension.h
 * @brief Public ABI and lifecycle API for scalar-function addons.
 */
#ifndef WIRELOG_EXTENSION_H
#define WIRELOG_EXTENSION_H

#include "wirelog/wirelog-export.h"
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define WIRELOG_EXTENSION_ABI_VERSION 1u

typedef struct wirelog_extension_registry wirelog_extension_registry_t;
typedef struct wirelog_extension_snapshot wirelog_extension_snapshot_t;

/*
 * Snapshot handles are reference-counted and may be shared by retaining them.
 * The caller must keep a reference alive while using a handle and must not
 * release or use the same handle concurrently with its final release.
 */

typedef enum wirelog_extension_value_type {
    WIRELOG_EXTENSION_VALUE_INT64 = 1,
    WIRELOG_EXTENSION_VALUE_BOOL = 2,
    WIRELOG_EXTENSION_VALUE_STRING = 3
} wirelog_extension_value_type_t;

typedef struct wirelog_extension_value {
    uint32_t type;
    uint32_t size;
    union {
        int64_t int64_value;
        uint8_t bool_value;
        struct { const char *data; size_t length; } string_value;
    } as;
} wirelog_extension_value_t;

/* Callback arguments are borrowed for the duration of invoke().  The engine
 * copies accepted BOOL and INT64 results after invoke() returns; callbacks
 * must therefore not expect the engine to retain pointers or release memory.
 * STRING results are reserved for a future ownership-aware ABI and are
 * rejected by the current evaluator. */

typedef int (*wirelog_extension_scalar_fn)(
    const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *user_data);
typedef void (*wirelog_extension_destroy_fn)(void *user_data);

/* Callback capabilities. A zero policy is the legacy/unspecified contract;
 * runtime enforcement is added by the follow-up evaluator policy unit. */
#define WIRELOG_EXTENSION_CALLBACK_THREAD_SAFE  (1u << 0)
#define WIRELOG_EXTENSION_CALLBACK_DETERMINISTIC (1u << 1)
#define WIRELOG_EXTENSION_CALLBACK_PURE        (1u << 2)
#define WIRELOG_EXTENSION_CALLBACK_REENTRANT   (1u << 3)
#define WIRELOG_EXTENSION_CALLBACK_POLICY_KNOWN_MASK \
        (WIRELOG_EXTENSION_CALLBACK_THREAD_SAFE \
        | WIRELOG_EXTENSION_CALLBACK_DETERMINISTIC \
        | WIRELOG_EXTENSION_CALLBACK_PURE \
        | WIRELOG_EXTENSION_CALLBACK_REENTRANT)

/* size is the sizeof the struct known by the caller; fields beyond size are
 * ignored, allowing this descriptor to grow without breaking old addons. */
typedef struct wirelog_extension_descriptor {
    uint32_t abi_version;
    uint32_t size;
    const char *name;
    uint32_t arity;
    const uint32_t *argument_types;
    uint32_t result_type;
    wirelog_extension_scalar_fn invoke;
    void *user_data;
    wirelog_extension_destroy_fn destroy;
    /* Stable identity and version of the addon's callable ABI contract.
     * Both zero means legacy metadata is absent; exactly one zero is invalid.
     * These append-only fields are available only when covered by @size. */
    uint64_t addon_abi_identity;
    uint32_t addon_abi_version;
    /* Capability declarations; zero means legacy/unspecified. */
    uint32_t callback_policy;
} wirelog_extension_descriptor_t;

WIRELOG_API wirelog_extension_registry_t *
wirelog_extension_registry_create(void);
WIRELOG_API int
wirelog_extension_registry_destroy(wirelog_extension_registry_t *registry);
WIRELOG_API int
wirelog_extension_register(wirelog_extension_registry_t *registry,
    const wirelog_extension_descriptor_t *descriptor);
WIRELOG_API int
wirelog_extension_unregister(wirelog_extension_registry_t *registry,
    const char *name);
WIRELOG_API wirelog_extension_snapshot_t *
wirelog_extension_snapshot_acquire(wirelog_extension_registry_t *registry);
WIRELOG_API void
wirelog_extension_snapshot_retain(wirelog_extension_snapshot_t *snapshot);
WIRELOG_API void
wirelog_extension_snapshot_release(wirelog_extension_snapshot_t *snapshot);
WIRELOG_API const wirelog_extension_descriptor_t *
wirelog_extension_snapshot_find(const wirelog_extension_snapshot_t *snapshot,
    const char *name);
WIRELOG_API int
wirelog_extension_snapshot_pin(wirelog_extension_snapshot_t *snapshot,
    const char *name,
    const wirelog_extension_descriptor_t **out);
WIRELOG_API int
wirelog_extension_snapshot_unpin(wirelog_extension_snapshot_t *snapshot,
    const wirelog_extension_descriptor_t *descriptor);
WIRELOG_API const char *wirelog_extension_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_EXTENSION_H */
