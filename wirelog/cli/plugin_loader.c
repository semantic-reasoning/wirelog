/*
 * plugin_loader.c - CLI Plugin Loader Implementation (Issue #461)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Loads user adapter plugins via dlopen and bulk-registers them
 * with the I/O adapter registry.  Gated behind the io_plugin_dlopen
 * meson option; this file is only compiled when the option is enabled.
 */

#include "plugin_loader.h"
#include "wirelog/io/io_adapter.h"

#include <dlfcn.h>
#include <stdio.h>
#include <string.h>

/* ------------------------------------------------------------------------ */
/* Loaded plugin tracking                                                   */
/* ------------------------------------------------------------------------ */

#define MAX_PLUGINS 16

typedef struct {
    void *handle;
    const wl_io_adapter_t *const *adapters;
    uint32_t n_adapters;
} loaded_plugin_t;

static loaded_plugin_t s_plugins[MAX_PLUGINS];
static uint32_t s_plugin_count;

/* ------------------------------------------------------------------------ */
/* Public API                                                               */
/* ------------------------------------------------------------------------ */

int
wl_plugin_load(const char *path)
{
    if (!path) {
        fprintf(stderr, "error: plugin path is NULL\n");
        return -1;
    }

    if (s_plugin_count >= MAX_PLUGINS) {
        fprintf(stderr, "error: too many plugins loaded (max %d)\n",
            MAX_PLUGINS);
        return -1;
    }

    void *handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        fprintf(stderr, "error: cannot load plugin '%s': %s\n",
            path, dlerror());
        return -1;
    }

    /* Clear any stale dlerror */
    dlerror();

    wl_io_plugin_entry_fn entry =
        (wl_io_plugin_entry_fn)dlsym(handle, WL_IO_PLUGIN_ENTRY_SYMBOL);

    const char *err = dlerror();
    if (err || !entry) {
        fprintf(stderr,
            "error: plugin '%s' missing symbol '%s': %s\n",
            path, WL_IO_PLUGIN_ENTRY_SYMBOL,
            err ? err : "symbol resolved to NULL");
        dlclose(handle);
        return -1;
    }

    uint32_t n_out = 0;
    const wl_io_adapter_t *const *adapters =
        entry(&n_out, WL_IO_ABI_VERSION);

    if (!adapters) {
        fprintf(stderr,
            "error: plugin '%s' returned NULL "
            "(ABI version mismatch? host=%u)\n",
            path, (unsigned)WL_IO_ABI_VERSION);
        dlclose(handle);
        return -1;
    }

    if (n_out == 0) {
        fprintf(stderr, "warning: plugin '%s' returned 0 adapters\n", path);
        dlclose(handle);
        return 0;
    }

    /* Bulk-register all adapters from this plugin */
    uint32_t registered = 0;
    for (uint32_t i = 0; i < n_out; i++) {
        if (!adapters[i]) {
            fprintf(stderr,
                "error: plugin '%s' adapter[%u] is NULL\n",
                path, (unsigned)i);
            continue;
        }
        if (wl_io_register_adapter(adapters[i]) != 0) {
            fprintf(stderr,
                "error: plugin '%s' adapter[%u] (%s) "
                "registration failed: %s\n",
                path, (unsigned)i,
                adapters[i]->scheme ? adapters[i]->scheme : "(null)",
                wl_io_last_error());
            /* Fail fast: unregister what we registered and close */
            for (uint32_t j = 0; j < i; j++) {
                if (adapters[j] && adapters[j]->scheme) {
                    wl_io_unregister_adapter(adapters[j]->scheme);
                }
            }
            dlclose(handle);
            return -1;
        }
        registered++;
    }

    /* Track the loaded plugin for cleanup */
    s_plugins[s_plugin_count].handle = handle;
    s_plugins[s_plugin_count].adapters = adapters;
    s_plugins[s_plugin_count].n_adapters = registered;
    s_plugin_count++;

    return 0;
}

void
wl_plugin_unload_all(void)
{
    for (uint32_t i = 0; i < s_plugin_count; i++) {
        loaded_plugin_t *p = &s_plugins[i];

        /* Unregister all adapters from this plugin */
        for (uint32_t j = 0; j < p->n_adapters; j++) {
            if (p->adapters[j] && p->adapters[j]->scheme) {
                wl_io_unregister_adapter(p->adapters[j]->scheme);
            }
        }

        dlclose(p->handle);
        p->handle = NULL;
        p->adapters = NULL;
        p->n_adapters = 0;
    }

    s_plugin_count = 0;
}
