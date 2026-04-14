/*
 * plugin_loader.h - CLI Plugin Loader Interface (Issue #461)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#ifndef WIRELOG_CLI_PLUGIN_LOADER_H
#define WIRELOG_CLI_PLUGIN_LOADER_H

/*
 * Load adapter plugin(s) from a shared library.
 *
 * Opens the library at `path` via dlopen, resolves the
 * "wl_io_plugin_entry" symbol, validates the ABI version,
 * and bulk-registers all returned adapters.
 *
 * Returns 0 on success, -1 on error (message printed to stderr).
 * Returns -1 if any adapter fails registration (fail-fast).
 *
 * Thread safety: NOT thread-safe.  This is a CLI-only module;
 * call from a single thread before starting the evaluation pipeline.
 */
int wl_plugin_load(const char *path);

/*
 * Unload all previously loaded plugins.
 *
 * Unregisters all adapters that were registered via wl_plugin_load()
 * and closes the shared library handles.  Safe to call if no plugins
 * were loaded.
 *
 * Must be called only after all I/O activity has ceased (i.e., after
 * the evaluation pipeline has completed and all worker threads have
 * joined).  The adapter pointers returned by the plugin live in the
 * dlopen'd library and become invalid after dlclose.
 */
void wl_plugin_unload_all(void);

#endif /* WIRELOG_CLI_PLUGIN_LOADER_H */
