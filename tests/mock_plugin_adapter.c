/*
 * mock_plugin_adapter.c - Mock adapter plugin for test_plugin_loader
 *
 * Built as a shared library (libmock_plugin_adapter.so) that exports
 * the wl_io_plugin_entry symbol.  Used by test_plugin_loader.c to
 * exercise the plugin load + register + dispatch path.
 */

#include "wirelog/io/io_adapter.h"

#include <stddef.h>
#include <stdlib.h>

static int
mock_read(wl_io_ctx_t *ctx, int64_t **out_data, uint32_t *out_nrows,
    void *user_data)
{
    (void)ctx;
    (void)user_data;
    *out_data = NULL;
    *out_nrows = 0;
    return 0;
}

static const wl_io_adapter_t mock_adapter = {
    .abi_version = WL_IO_ABI_VERSION,
    .scheme = "mock_plugin",
    .description = "mock plugin adapter for testing",
    .read = mock_read,
    .validate = NULL,
    .user_data = NULL,
};

static const wl_io_adapter_t *const adapter_list[] = {&mock_adapter};

WL_IO_PLUGIN_EXPORT
const wl_io_adapter_t *const *
wl_io_plugin_entry(uint32_t *n_out, uint32_t abi_ver)
{
    if (abi_ver != WL_IO_ABI_VERSION) {
        *n_out = 0;
        return NULL;
    }
    *n_out = 1;
    return adapter_list;
}
