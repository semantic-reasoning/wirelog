#include "wirelog/wirelog-extension.h"
#include "wirelog/thread.h"
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define WL_EXTENSION_ERROR_SIZE 256

#if defined(_MSC_VER)
static __declspec(thread) char wl_extension_error[WL_EXTENSION_ERROR_SIZE];
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
static _Thread_local char wl_extension_error[WL_EXTENSION_ERROR_SIZE];
#else
static __thread char wl_extension_error[WL_EXTENSION_ERROR_SIZE];
#endif

typedef struct wl_extension_entry {
    wirelog_extension_descriptor_t descriptor;
    char *name;
    uint32_t *argument_types;
    size_t references;
    size_t pins;
    bool registered;
} wl_extension_entry_t;

struct wirelog_extension_registry {
    mutex_t mutex;
    wl_extension_entry_t **entries;
    size_t count;
    size_t capacity;
    size_t active_snapshots;
};

struct wirelog_extension_snapshot {
    wirelog_extension_registry_t *registry;
    wl_extension_entry_t **entries;
    size_t count;
    size_t *pins;
    bool *destroy_entries;
};

static void
wl_extension_error_set(const char *message)
{
    if (!message) message = "";
    strncpy(wl_extension_error, message, sizeof(wl_extension_error) - 1);
    wl_extension_error[sizeof(wl_extension_error) - 1] = '\0';
}

static char *
wl_extension_name_copy(const char *name)
{
    const char *begin = name;
    const char *end;
    size_t length;
    char *copy;
    while (*begin == ' ' || *begin == '\t' || *begin == '\n' ||
        *begin == '\r') begin++;
    end = begin + strlen(begin);
    while (end > begin &&
        (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' ||
        end[-1] == '\r')) end--;
    length = (size_t)(end - begin);
    if (length == 0 || length > 255) return NULL;
    copy = malloc(length + 1);
    if (!copy) return NULL;
    memcpy(copy, begin, length);
    copy[length] = '\0';
    return copy;
}

static bool
wl_extension_entry_release_locked(wl_extension_entry_t *entry)
{
    return --entry->references == 0;
}

static void
wl_extension_entry_discard(wl_extension_entry_t *entry)
{
    free(entry->argument_types);
    free(entry->name);
    free(entry);
}

static void
wl_extension_entry_destroy(wl_extension_entry_t *entry)
{
    if (entry->descriptor.destroy)
        entry->descriptor.destroy(entry->descriptor.user_data);
    wl_extension_entry_discard(entry);
}

static wl_extension_entry_t *
wl_extension_entry_create(const wirelog_extension_descriptor_t *source)
{
    wl_extension_entry_t *entry = calloc(1, sizeof(*entry));
    size_t args_size;
    if (!entry) return NULL;
    entry->name = wl_extension_name_copy(source->name);
    if (!entry->name) {
        wl_extension_entry_discard(entry); return NULL;
    }
    args_size = (size_t)source->arity * sizeof(uint32_t);
    if (source->arity != 0) {
        entry->argument_types = malloc(args_size);
        if (!entry->argument_types) {
            wl_extension_entry_discard(entry); return NULL;
        }
        memcpy(entry->argument_types, source->argument_types, args_size);
    }
    entry->descriptor = *source;
    entry->descriptor.name = entry->name;
    entry->descriptor.argument_types = entry->argument_types;
    entry->references = 1;
    entry->registered = true;
    return entry;
}

wirelog_extension_registry_t *
wirelog_extension_registry_create(void)
{
    wirelog_extension_registry_t *registry = calloc(1, sizeof(*registry));
    if (!registry) {
        wl_extension_error_set("allocation failed"); return NULL;
    }
    if (mutex_init(&registry->mutex) != 0) {
        free(registry); wl_extension_error_set("mutex initialization failed");
        return NULL;
    }
    wl_extension_error_set(NULL);
    return registry;
}

int
wirelog_extension_registry_destroy(wirelog_extension_registry_t *registry)
{
    if (!registry) {
        wl_extension_error_set("registry is NULL"); return -1;
    }
    mutex_lock(&registry->mutex);
    if (registry->count != 0 || registry->active_snapshots != 0) {
        mutex_unlock(&registry->mutex);
        wl_extension_error_set("registry is not empty"); return -1;
    }
    free(registry->entries);
    mutex_unlock(&registry->mutex);
    mutex_destroy(&registry->mutex);
    free(registry);
    wl_extension_error_set(NULL);
    return 0;
}

int
wirelog_extension_register(wirelog_extension_registry_t *registry,
    const wirelog_extension_descriptor_t *source)
{
    wl_extension_entry_t *entry;
    size_t i;
    if (!registry || !source) {
        wl_extension_error_set("descriptor or registry is NULL"); return -1;
    }
    if (source->abi_version != WIRELOG_EXTENSION_ABI_VERSION) {
        wl_extension_error_set("ABI version mismatch"); return -1;
    }
    if (source->size < offsetof(wirelog_extension_descriptor_t,
        result_type) + sizeof(source->result_type)) {
        wl_extension_error_set("descriptor size is too small"); return -1;
    }
    {
        const uint64_t max_arity =
            (uint64_t)(SIZE_MAX / sizeof(uint32_t));
        if ((uint64_t)source->arity > max_arity) {
            wl_extension_error_set("extension arity is too large");
            return -1;
        }
    }
    {
        wirelog_extension_descriptor_t owned_source = { 0 };
        size_t copy_size = source->size <
            sizeof(owned_source) ? source->size : sizeof(owned_source);
        memcpy(&owned_source, source, copy_size);
        if (!owned_source.name || !owned_source.invoke ||
            (owned_source.arity != 0 && !owned_source.argument_types)) {
            wl_extension_error_set("invalid descriptor"); return -1;
        }
        entry = wl_extension_entry_create(&owned_source);
    }
    if (!entry) {
        wl_extension_error_set("invalid descriptor or allocation failed");
        return -1;
    }
    mutex_lock(&registry->mutex);
    for (i = 0; i < registry->count; i++) {
        if (strcmp(registry->entries[i]->name, entry->name) == 0) {
            mutex_unlock(&registry->mutex); wl_extension_entry_discard(entry);
            wl_extension_error_set("duplicate extension name"); return -1;
        }
    }
    if (registry->count == registry->capacity) {
        size_t capacity = registry->capacity ? registry->capacity * 2 : 8;
        wl_extension_entry_t **grown = realloc(registry->entries,
                capacity * sizeof(*grown));
        if (!grown) {
            mutex_unlock(&registry->mutex); wl_extension_entry_discard(entry);
            wl_extension_error_set("allocation failed"); return -1;
        }
        registry->entries = grown; registry->capacity = capacity;
    }
    registry->entries[registry->count++] = entry;
    mutex_unlock(&registry->mutex);
    wl_extension_error_set(NULL);
    return 0;
}

int
wirelog_extension_unregister(wirelog_extension_registry_t *registry,
    const char *name)
{
    char *normalized;
    size_t i;
    if (!registry || !name) {
        wl_extension_error_set("name or registry is NULL"); return -1;
    }
    normalized = wl_extension_name_copy(name);
    if (!normalized) {
        wl_extension_error_set("invalid extension name"); return -1;
    }
    mutex_lock(&registry->mutex);
    for (i = 0; i < registry->count;
        i++) if (strcmp(registry->entries[i]->name, normalized) == 0) break;
    free(normalized);
    if (i == registry->count) {
        mutex_unlock(&registry->mutex);
        wl_extension_error_set("extension not found"); return -1;
    }
    if (registry->entries[i]->pins != 0) {
        mutex_unlock(&registry->mutex);
        wl_extension_error_set("extension is pinned"); return -1;
    }
    { wl_extension_entry_t *entry = registry->entries[i];
      bool destroy;
      memmove(&registry->entries[i], &registry->entries[i + 1],
          (registry->count - i - 1) * sizeof(*registry->entries));
      registry->count--; entry->registered = false;
      destroy = wl_extension_entry_release_locked(entry);
      mutex_unlock(&registry->mutex);
      if (destroy) wl_extension_entry_destroy(entry);
      wl_extension_error_set(NULL); return 0; }
}

wirelog_extension_snapshot_t *
wirelog_extension_snapshot_acquire(wirelog_extension_registry_t *registry)
{
    wirelog_extension_snapshot_t *snapshot;
    size_t i;
    if (!registry) {
        wl_extension_error_set("registry is NULL"); return NULL;
    }
    snapshot = calloc(1, sizeof(*snapshot));
    if (!snapshot) {
        wl_extension_error_set("allocation failed"); return NULL;
    }
    mutex_lock(&registry->mutex);
    snapshot->entries = calloc(registry->count, sizeof(*snapshot->entries));
    if (registry->count && !snapshot->entries) {
        mutex_unlock(&registry->mutex); free(snapshot);
        wl_extension_error_set("allocation failed"); return NULL;
    }
    snapshot->pins = calloc(registry->count, sizeof(*snapshot->pins));
    if (registry->count && !snapshot->pins) {
        mutex_unlock(&registry->mutex); free(snapshot->entries); free(snapshot);
        wl_extension_error_set("allocation failed"); return NULL;
    }
    snapshot->destroy_entries = calloc(registry->count,
            sizeof(*snapshot->destroy_entries));
    if (registry->count && !snapshot->destroy_entries) {
        mutex_unlock(&registry->mutex);
        free(snapshot->pins);
        free(snapshot->entries);
        free(snapshot);
        wl_extension_error_set("allocation failed");
        return NULL;
    }
    snapshot->registry = registry; snapshot->count = registry->count;
    registry->active_snapshots++;
    for (i = 0; i < snapshot->count; i++) {
        snapshot->entries[i] = registry->entries[i];
        snapshot->entries[i]->references++;
    }
    mutex_unlock(&registry->mutex); wl_extension_error_set(NULL);
    return snapshot;
}

void
wirelog_extension_snapshot_release(wirelog_extension_snapshot_t *snapshot)
{
    size_t i;
    if (!snapshot) return;
    mutex_lock(&snapshot->registry->mutex);
    for (i = 0; i < snapshot->count; i++) {
        snapshot->entries[i]->pins -= snapshot->pins[i];
        snapshot->pins[i] = 0;
    }
    for (i = 0; i < snapshot->count; i++) {
        snapshot->destroy_entries[i] =
            wl_extension_entry_release_locked(snapshot->entries[i]);
    }
    snapshot->registry->active_snapshots--;
    mutex_unlock(&snapshot->registry->mutex);
    for (i = 0; i < snapshot->count; i++)
        if (snapshot->destroy_entries[i])
            wl_extension_entry_destroy(snapshot->entries[i]);
    free(snapshot->destroy_entries);
    free(snapshot->pins);
    free(snapshot->entries);
    free(snapshot);
    wl_extension_error_set(NULL);
}

const wirelog_extension_descriptor_t *
wirelog_extension_snapshot_find(const wirelog_extension_snapshot_t *snapshot,
    const char *name)
{
    char *normalized; size_t i;
    if (!snapshot || !name) {
        wl_extension_error_set("snapshot or name is NULL"); return NULL;
    }
    normalized = wl_extension_name_copy(name); if (!normalized) {
        wl_extension_error_set("invalid extension name"); return NULL;
    }
    for (i = 0; i < snapshot->count;
        i++) if (strcmp(snapshot->entries[i]->name, normalized) == 0) {
            free(normalized); wl_extension_error_set(NULL);
            return &snapshot->entries[i]->descriptor;
        }
    free(normalized); wl_extension_error_set("extension not found");
    return NULL;
}

int
wirelog_extension_snapshot_pin(wirelog_extension_snapshot_t *snapshot,
    const char *name, const wirelog_extension_descriptor_t **out)
{
    const wirelog_extension_descriptor_t *descriptor =
        wirelog_extension_snapshot_find(snapshot, name);
    size_t i;
    if (!descriptor) return -1;
    mutex_lock(&snapshot->registry->mutex);
    for (i = 0; i < snapshot->count;
        i++) if (&snapshot->entries[i]->descriptor == descriptor) {
            snapshot->entries[i]->pins++; snapshot->pins[i]++;
            if (out) *out = descriptor;
            mutex_unlock(&snapshot->registry->mutex);
            wl_extension_error_set(NULL); return 0;
        }
    mutex_unlock(&snapshot->registry->mutex);
    wl_extension_error_set("descriptor is not in snapshot"); return -1;
}

int
wirelog_extension_snapshot_unpin(wirelog_extension_snapshot_t *snapshot,
    const wirelog_extension_descriptor_t *descriptor)
{
    size_t i;
    if (!snapshot || !descriptor) {
        wl_extension_error_set("snapshot or descriptor is NULL"); return -1;
    }
    mutex_lock(&snapshot->registry->mutex);
    for (i = 0; i < snapshot->count;
        i++) if (&snapshot->entries[i]->descriptor == descriptor) {
            if (!snapshot->pins[i]) {
                mutex_unlock(&snapshot->registry->mutex);
                wl_extension_error_set(
                    "descriptor is not pinned by this snapshot"); return -1;
            }
            snapshot->pins[i]--; snapshot->entries[i]->pins--;
            mutex_unlock(&snapshot->registry->mutex);
            wl_extension_error_set(NULL); return 0;
        }
    mutex_unlock(&snapshot->registry->mutex);
    wl_extension_error_set("descriptor is not in snapshot"); return -1;
}

const char *wirelog_extension_last_error(void) {
    return wl_extension_error;
}
