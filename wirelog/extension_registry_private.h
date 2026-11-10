/* Internal registry representation shared by raw and controlled helpers. */
#ifndef WL_EXTENSION_REGISTRY_PRIVATE_H
#define WL_EXTENSION_REGISTRY_PRIVATE_H
#include "wirelog-extension.h"
#include "thread.h"
#include <stdbool.h>
#include <stddef.h>
typedef struct wl_extension_entry {
    wirelog_extension_descriptor_t descriptor;
    char *name;
    uint32_t *argument_types;
    size_t references;
    size_t pins;
    bool registered;
    wirelog_extension_registry_t *registry;
} wl_extension_entry_t;

struct wirelog_extension_registry {
    wl_mutex_t mutex;
    wl_extension_entry_t **entries;
    size_t count;
    size_t capacity;
    size_t active_snapshots;
    size_t active_leases;
};

struct wirelog_extension_snapshot {
    wirelog_extension_registry_t *registry;
    size_t references;
    wl_extension_entry_t **entries;
    size_t count;
    size_t *pins;
    bool *destroy_entries;
};

/* Fixed lookup outcomes only; no input-sized TLS diagnostic copy. */
typedef enum {
    WL_EXTENSION_REGISTRY_LOOKUP_OK,
    WL_EXTENSION_REGISTRY_LOOKUP_NULL,
    WL_EXTENSION_REGISTRY_LOOKUP_INVALID,
    WL_EXTENSION_REGISTRY_LOOKUP_MISSING
} wl_extension_registry_lookup_outcome_t;
void wl_extension_registry_lookup_error(
    wl_extension_registry_lookup_outcome_t outcome);
#endif
